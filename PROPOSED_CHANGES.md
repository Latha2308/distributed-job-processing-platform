# Proposed Changes for Delayed Job Retry Implementation

## Summary
This document outlines the changes needed to implement atomic delayed job retry with exponential backoff while preserving the existing async Redis architecture.

## Key Design Decisions

1. **Async-only architecture**: Use asyncio background task, not threads
2. **Priority ordering fixed**: Change from `score = -priority` to `score = priority`
3. **Atomic transfers**: Lua script for delayed → main queue transfers
4. **Proper error handling**: Check enqueue return values, don't fall back to immediate execution
5. **Worker protection**: Verify job state before processing
6. **Test isolation**: Unique queue names per test

## Files to Modify

### 1. app/core/redis_client.py
**Changes:**
- Add import for `time` and logging
- Add `push_to_delayed_queue(job_data, execute_at, queue_name)` - async
- Add `transfer_ready_jobs_atomic(main_queue, delayed_queue)` - async with Lua script
- Add `delayed_queue_length(queue_name)` - async
- Add `get_delayed_queue_ready_count(queue_name)` - async
- Add `peek_delayed_queue(queue_name, limit)` - async
- **Fix priority ordering**: Change `push_to_queue` from `score = -priority` to `score = priority`
- **Fix priority ordering**: Change `pop_from_queue` comment to clarify lowest priority value (not lowest score)

**Lua Script Logic:**
```lua
-- Get jobs with execute_at <= current_time
local ready_jobs = redis.call('ZRANGEBYSCORE', delayed_queue, 0, current_time)

for each job:
  -- Parse job to extract priority
  local job_obj = cjson.decode(job_data)
  local priority = job_obj.priority or 0
  
  -- Remove metadata fields (priority, execute_at)
  job_obj.priority = nil
  job_obj.execute_at = nil
  
  -- Serialize clean job
  local clean_job = cjson.encode(job_obj)
  
  -- Add to main queue with priority as score
  local added = redis.call('ZADD', main_queue, priority, clean_job)
  
  -- Only remove from delayed if add succeeded
  if added == 1 then
    redis.call('ZREM', delayed_queue, job_data)
    transferred = transferred + 1
  end
```

**Note on duplicate handling**: ZADD with existing member updates score but returns 0. We only remove from delayed queue if ZADD returns 1 (new member), preventing duplicate transfers.

### 2. app/services/queue_service.py
**Changes:**
- Modify `__init__`: Add `queue_name` and `delayed_queue_name` parameters with defaults
- Modify `enqueue_job`: Add `execute_at: Optional[float] = None` parameter
- Add `_enqueue_redis_delayed(message, execute_at)` - async method
- Update `_enqueue_redis` to route to delayed queue if `execute_at` is provided
- Add `process_delayed_jobs()` - async method that calls `transfer_ready_jobs_atomic`
- **Error handling**: Log and return False if delayed enqueue fails

### 3. app/services/job_service.py
**Changes:**
- Modify `retry_job`:
  - Replace `datetime.utcnow()` with `datetime.now(timezone.utc)`
  - Calculate `execute_at` timestamp: `next_retry_at.timestamp()`
  - Pass `execute_at` to `enqueue_job`
  - **Check return value**: If enqueue fails, log error and raise exception (don't fall back)
- Import `timezone` from datetime
- Import `Integer` for type hints (if missing)

### 4. app/worker/worker.py
**Changes:**
- Add `delayed_processor_task: Optional[asyncio.Task] = None` to `__init__`
- Modify `start()`: Start delayed processor task
- Modify `stop()`: Cancel delayed processor task
- Add `_delayed_job_processor()` - async method:
  - Loop while running
  - Call `queue_service.process_delayed_jobs()`
  - Sleep for DELAYED_JOB_CHECK_INTERVAL (default 5 seconds)
- **Existing protection verified**: `_process_job` already checks job status before processing

### 5. app/worker/job_executor.py
**Changes:**
- Add `test_job` handler that:
  - Uses `datetime.now(timezone.utc)` for timestamps
  - Returns success result with timestamp
  - Can optionally fail based on payload flag

### 6. app/core/config.py
**Changes:**
- Add `DELAYED_JOB_CHECK_INTERVAL: int = 5` setting

### 7. tests/conftest.py
**Changes:**
- Add `test_queue_names` fixture: Returns unique queue names using `uuid.uuid4()`
- Add `redis_client_fixture` fixture: Returns async Redis client for tests
- Add `queue_service_fixture`: Returns QueueService with test queue names
- **Cleanup**: Fixtures clean up only test-specific queues, never touch "job_queue"

### 8. tests/test_job_processing.py (NEW FILE)
**Changes:**
- Create comprehensive integration tests:
  1. `test_enqueue_job_immediate` - Standard enqueue
  2. `test_enqueue_job_delayed` - Delayed enqueue
  3. `test_delayed_job_transfer_atomic` - Atomic transfer
  4. `test_delayed_job_not_ready` - Jobs not transferred early
  5. `test_retry_with_exponential_backoff` - Retry delay calculation
  6. `test_worker_processes_job` - **Invoke actual worker processing**
  7. `test_worker_skips_completed_job` - Reprocessing protection
  8. `test_worker_skips_dlq_job` - DLQ protection
  9. `test_concurrent_delayed_transfer` - Concurrent worker safety
  10. `test_transfer_preserves_priority` - Priority ordering

## Priority Ordering Verification

**Current (INCORRECT):**
```python
# Enqueue
score = -priority  # priority 0 → score 0, priority 5 → score -5

# Dequeue
zpopmin  # Gets -5 before 0 (WRONG! Higher priority value processed first)
```

**Fixed (CORRECT):**
```python
# Enqueue
score = priority  # priority 0 → score 0, priority 5 → score 5

# Dequeue  
zpopmin  # Gets 0 before 5 (CORRECT! Lower priority value = higher priority)
```

## Error Handling Strategy

**Delayed enqueue failure:**
```python
# In retry_job:
success = await self.queue_service.enqueue_job(
    job_id=job.id,
    job_type=job.job_type,
    priority=job.priority,
    execute_at=execute_at,
)

if not success:
    logger.error("delayed_retry_enqueue_failed", job_id=str(job_id))
    # Do NOT fall back to immediate execution
    # Raise exception so transaction rolls back
    raise RuntimeError(f"Failed to enqueue delayed retry for job {job_id}")
```

## Test Isolation Strategy

```python
# Each test gets unique queue names
test_id = str(uuid.uuid4())
main_queue = f"test_queue_{test_id}"
delayed_queue = f"test_queue_{test_id}_delayed"

# Cleanup only deletes test queues
await redis_client.delete(main_queue)
await redis_client.delete(delayed_queue)
# Never touches "job_queue" or "job_queue_delayed"
```

## Async Background Task Pattern

```python
# In Worker class
async def _delayed_job_processor(self):
    """Background task to process delayed jobs"""
    queue_service = QueueService(redis_client)
    
    while self.running:
        try:
            transferred = await queue_service.process_delayed_jobs()
            if transferred > 0:
                logger.info(f"Transferred {transferred} delayed jobs")
            await asyncio.sleep(settings.DELAYED_JOB_CHECK_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Delayed processor error: {e}")
            await asyncio.sleep(5)
```

## Questions for Approval

1. **Priority ordering fix**: Changing from `-priority` to `priority` will affect existing queued jobs. Should we document a migration step?

2. **DELAYED_JOB_CHECK_INTERVAL**: Default 5 seconds. Is this acceptable latency for retry processing?

3. **Error handling for enqueue failures**: Should we use a different strategy than raising an exception (e.g., set job to a new "RETRY_FAILED" status)?

4. **Test worker invocation**: Tests will need access to Worker instance. Should we create a worker fixture or instantiate in each test?

Please review and approve before I proceed with implementation.
