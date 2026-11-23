# Recovery Transaction Requirements

**Status:** Documentation only (Step 289A)
**Implementation:** Future steps (289B+)
**Purpose:** Define atomic transaction requirements for bounded QUEUED-job reconciliation

---

## Overview

This document specifies the transaction structure and requirements for the bounded QUEUED-job reconciliation system (Alternative 3 from Step 288A). The `last_recovery_attempt_at` column (added in Step 289A) provides the database foundation for throttling recovery attempts. This document defines how that column will be used in future implementation steps.

## Atomic Transaction Structure

The reconciliation process MUST execute within a single atomic PostgreSQL transaction to ensure consistency. The transaction performs the following steps:

### Phase 1: Lock Candidate Job
```sql
-- Acquire pessimistic lock on job row
SELECT id, status, created_at, last_recovery_attempt_at
FROM jobs
WHERE id = :job_id
FOR UPDATE NOWAIT;
```

**Purpose:** Prevent concurrent reconcilers or workers from processing the same job.

**Failure handling:** If lock cannot be acquired (NOWAIT fails), skip this job and move to next candidate.

### Phase 2: Revalidate Eligibility
```sql
-- Verify job is still QUEUED and eligible for recovery
SELECT id
FROM jobs
WHERE id = :job_id
AND status = 'QUEUED'
AND created_at < NOW() - INTERVAL '10 minutes'  -- age_threshold
AND (
    last_recovery_attempt_at IS NULL
    OR last_recovery_attempt_at < NOW() - INTERVAL '1 hour'  -- recovery_interval
);
```

**Purpose:** Confirm job hasn't been claimed by a worker or recently recovered since initial query.

**Failure handling:** If eligibility check fails, release lock (rollback) and move to next candidate.

### Phase 3: Check for Existing Unpublished Events
```sql
-- Check if unpublished job_queued_recovery event already exists
SELECT id
FROM job_outbox
WHERE job_id = :job_id
AND event_type = 'job_queued_recovery'
AND published = false;
```

**Purpose:** Prevent duplicate outbox entries for the same recovery attempt.

**Failure handling:** If unpublished event exists, skip outbox creation (job already in recovery pipeline). Update throttling timestamp and commit.

### Phase 4: Create Outbox Event
```sql
-- Create outbox event for recovery (only if no unpublished event exists)
INSERT INTO job_outbox (
    id,
    job_id,
    event_type,
    published,
    attempts,
    retry_attempt,
    created_at
)
VALUES (
    gen_random_uuid(),
    :job_id,
    'job_queued_recovery',  -- NOT 'job_created'
    false,
    0,
    NULL,  -- NULL for recovery events (not a retry)
    NOW()
);
```

**Purpose:** Create durable recovery event in outbox for publication to Redis.

**Event type:** `job_queued_recovery` is published to the main queue (not retry queue) and does NOT include `retry_attempt` in the message payload. This distinguishes recovery from normal job creation while maintaining dispatcher compatibility.

### Phase 5: Update Throttling Timestamp
```sql
-- Update last_recovery_attempt_at to throttle future attempts
UPDATE jobs
SET last_recovery_attempt_at = NOW()
WHERE id = :job_id;
```

**Purpose:** Record this recovery attempt to prevent excessive republication.

**Throttling semantics:**
- First recovery: Eligible after `age_threshold` (10 minutes) from job creation
- Subsequent recoveries: Eligible after `recovery_interval` (1 hour) from last attempt
- Maximum frequency: 24 recovery attempts per day per job (bounded, not infinite)

### Phase 6: Commit Transaction
```sql
COMMIT;
```

**Purpose:** Atomically commit outbox event and throttling timestamp together.

**Crash-safety:** If crash occurs before COMMIT, entire transaction is rolled back:
- No outbox event created
- No throttling timestamp updated
- Job remains eligible for recovery in next reconciliation cycle

## Transaction Invariants

The following invariants MUST be maintained:

1. **Atomicity:** Outbox event creation and throttling timestamp update MUST be atomic.
   - If outbox creation succeeds, timestamp update MUST succeed.
   - If either fails, both MUST be rolled back.

2. **Idempotency:** Reconciliation MUST NOT create duplicate outbox events for the same job.
   - Check for existing unpublished `job_queued_recovery` events before insertion.
   - Use `event_type` to distinguish recovery events from normal events.

3. **Throttling:** Recovery attempts MUST respect dual-window throttling.
   - First attempt: `created_at < NOW() - age_threshold`
   - Subsequent attempts: `last_recovery_attempt_at < NOW() - recovery_interval`

4. **Isolation:** Concurrent reconcilers MUST NOT process the same job.
   - Use `FOR UPDATE NOWAIT` to acquire exclusive lock.
   - Skip job if lock cannot be acquired immediately.

5. **Revalidation:** Eligibility MUST be rechecked after acquiring lock.
   - Status might have changed (QUEUED → PROCESSING).
   - Job might have been recovered by another reconciler.

6. **Bounded republication:** Recovery attempts MUST NOT be infinite.
   - Throttling limits frequency to 24 attempts/day.
   - Future DLQ migration (later step) will enforce absolute limits.

## Event Handling: `job_queued_recovery`

The `job_queued_recovery` event type is introduced to distinguish recovery from normal job creation:

### Message Format
```json
{
    "job_id": "uuid",
    "event_type": "job_queued_recovery",
    "job_type": "string",
    "priority": 5
}
```

**Note:** Unlike `job_created` events, `job_queued_recovery` does NOT include `retry_attempt` in the message payload. The absence of this field signals to the dispatcher that this is a recovery event (not a retry) and should be published to the main queue.

### Dispatcher Handling

The dispatcher MUST route `job_queued_recovery` events to the main job queue (not retry queue):

```python
# Pseudo-code for dispatcher routing logic
if event_type == "job_queued_recovery":
    queue_key = f"job_queue:{job_type}"  # Main queue
    # Do NOT include retry_attempt in message
elif event_type == "job_created" and retry_attempt is not None:
    queue_key = f"retry_queue:{job_type}"  # Retry queue
else:
    queue_key = f"job_queue:{job_type}"  # Main queue (normal job)
```

### Worker Handling

Workers do NOT need to distinguish `job_queued_recovery` from `job_created` events:
- Both event types represent jobs to be claimed and executed.
- Workers use the claim fence (status=QUEUED, worker_id=NULL) to prevent duplicate execution.
- Duplicate messages (from at-least-once republication) are safely ignored if job is already PROCESSING.

### Rationale

Using a separate event type (`job_queued_recovery`) instead of reusing `job_created` provides:
1. **Observability:** Recovery events are distinguishable in logs and metrics.
2. **Routing clarity:** Dispatcher can route recovery events without ambiguity.
3. **Future extensibility:** Recovery events can have different handling logic if needed.
4. **Audit trail:** Outbox table clearly shows which events were recovery attempts.

## Crash-Safety Guarantees

Under the atomic transaction structure, the following guarantees hold:

### Scenario 1: Crash Before Transaction Commit
- **State:** No outbox event, no throttling timestamp update.
- **Guarantee:** Job remains eligible for recovery in next cycle.
- **Outcome:** At-least-once delivery maintained.

### Scenario 2: Crash After Transaction Commit, Before Publication
- **State:** Outbox event created, throttling timestamp updated.
- **Guarantee:** Outbox publisher will eventually publish event (durable retry).
- **Outcome:** At-least-once delivery maintained.

### Scenario 3: Crash After Publication, Before Outbox Marked Published
- **State:** Event published to Redis, outbox still shows `published=false`.
- **Guarantee:** Outbox publisher will republish event (at-least-once).
- **Outcome:** Duplicate message sent to Redis, but worker claim fence prevents duplicate execution.

### Scenario 4: Concurrent Reconcilers
- **State:** Two reconcilers attempt to recover same job concurrently.
- **Guarantee:** `FOR UPDATE NOWAIT` ensures only one acquires lock.
- **Outcome:** One reconciler processes job, other skips and moves to next candidate.

### Scenario 5: Worker Claims Job During Reconciliation
- **State:** Worker claims job between initial query and transaction lock.
- **Guarantee:** Eligibility revalidation fails (status changed to PROCESSING).
- **Outcome:** Reconciler releases lock, no outbox event created.

## Implementation Phases (Future Steps)

**Step 289B:** Implement outbox deduplication logic
- Add `check_existing_unpublished_event()` function.
- Unit tests for deduplication scenarios.

**Step 289C:** Implement atomic recovery transaction
- Add `execute_recovery_transaction()` function.
- Integration tests for atomicity and rollback.

**Step 289D:** Implement reconciler service
- Add background reconciler with configurable interval.
- Concurrency tests with `FOR UPDATE SKIP LOCKED`.

**Step 289E:** Update dispatcher to handle `job_queued_recovery` events
- Add routing logic for recovery events.
- Integration tests for recovery message flow.

**Step 289F:** End-to-end validation
- Test complete recovery pipeline.
- Test all crash scenarios.
- Measure bounded republication behavior.

## Configuration Parameters

The following parameters control reconciliation behavior:

```python
# app/core/config.py (to be added in future steps)

# Age threshold: Jobs younger than this are not eligible for recovery
# Default: 10 minutes (600 seconds)
RECOVERY_AGE_THRESHOLD_SECONDS = 600

# Recovery interval: Minimum time between recovery attempts
# Default: 1 hour (3600 seconds)
RECOVERY_INTERVAL_SECONDS = 3600

# Reconciler poll interval: How often reconciler runs
# Default: 5 minutes (300 seconds)
RECONCILER_POLL_INTERVAL_SECONDS = 300

# Reconciler batch size: Maximum jobs to recover per cycle
# Default: 100 jobs
RECONCILER_BATCH_SIZE = 100
```

## Testing Requirements

The following tests MUST be implemented in future steps:

### Unit Tests
1. Eligibility query with NULL `last_recovery_attempt_at`
2. Eligibility query with recent `last_recovery_attempt_at` (should skip)
3. Eligibility query with old `last_recovery_attempt_at` (should include)
4. Outbox deduplication logic (existing unpublished event)
5. Throttling timestamp update logic

### Integration Tests
1. Full recovery transaction (lock, validate, create outbox, update timestamp, commit)
2. Transaction rollback on eligibility failure
3. Transaction rollback on lock acquisition failure
4. Concurrent reconcilers with `FOR UPDATE NOWAIT`
5. Worker claims job during reconciliation (eligibility revalidation)

### End-to-End Tests
1. Job stranded in QUEUED status is recovered
2. Recovered job is successfully dispatched to Redis
3. Worker successfully claims and processes recovered job
4. Duplicate recovery attempts are throttled
5. Bounded republication (no infinite loops)

### Crash Tests
1. Crash before transaction commit (job remains eligible)
2. Crash after transaction commit, before publication (outbox publisher retries)
3. Crash after publication, before marking published (duplicate message sent, claim fence prevents duplicate execution)

## References

- **Step 288A:** Crash-safety analysis and Alternative 3 design
- **Step 288B:** Hardened design with 8 concerns addressed
- **Step 288C:** Design invariants verification
- **Step 289A:** Database foundation (this migration)

---

**Important:** This is a design document only. Implementation will occur in Steps 289B-289F. Do NOT implement reconciliation logic, dispatcher changes, or worker changes until explicitly requested in those steps.
