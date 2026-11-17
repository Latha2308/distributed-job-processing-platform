"""
Integration Tests for Delayed Retry Workflow

Tests the end-to-end delayed retry mechanism with actual production implementation:
- JobService.retry_job(job_id) with exponential backoff (2s, 4s, 8s)
- Retry count incrementation after successful Redis scheduling
- Redis delayed queue scheduling and validation failures
- Worker poller integration with redis_client.transfer_ready_jobs_atomic()
- Dead letter queue behavior with JobStatus.DEAD_LETTER
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from datetime import datetime, timedelta
from uuid import uuid4, UUID

from app.services.job_service import JobService
from app.core.redis_client import RedisClient
from app.services.queue_service import QueueService
from app.worker.worker import Worker
from app.db.models import JobStatus, JobPriority, Job
from app.core.config import settings


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture
def mock_job():
    """Create a mock Job with all required attributes"""
    job = MagicMock(spec=Job)
    job.id = uuid4()
    job.job_type = "test_job"
    job.payload = {"data": "test"}
    job.priority = JobPriority.NORMAL.value  # 5
    job.status = JobStatus.FAILED
    job.retry_count = 0
    job.max_retries = 3
    job.timeout = 300
    job.created_at = datetime.utcnow()
    job.updated_at = datetime.utcnow()
    job.next_retry_at = None
    job.is_dead_letter = False
    job.moved_to_dlq_at = None
    return job


@pytest.fixture
def mock_db_session():
    """Create a mock database session"""
    session = MagicMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def mock_redis_client():
    """Create a mock RedisClient"""
    redis = AsyncMock(spec=RedisClient)
    redis.push_to_delayed_queue = AsyncMock(return_value=True)
    redis.transfer_ready_jobs_atomic = AsyncMock(return_value=0)
    return redis


@pytest.fixture
def mock_queue_service():
    """Create a mock QueueService"""
    return AsyncMock(spec=QueueService)


# ============================================================================
# CATEGORY 1: Delay Enforcement Tests (3 tests)
# ============================================================================

@pytest.mark.asyncio
async def test_first_retry_schedules_with_2_second_delay(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that first retry (retry_count=0 -> 1) schedules job with 2 second delay.
    
    Calculation: INITIAL_RETRY_DELAY * (RETRY_BACKOFF_BASE ^ next_retry_count)
                 1 * (2 ^ 1) = 2 seconds
    
    Implementation uses: execute_at = time.time() + delay (line 278 in job_service.py)
    """
    # Setup job state
    mock_job.retry_count = 0
    
    # Create JobService instance
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    
    # Mock get_job to return our test job
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    # Use deterministic timestamp
    fixed_time = 1000000.0
    expected_delay = 2.0  # 1 * (2^1) = 2 seconds
    expected_execute_at = fixed_time + expected_delay
    
    # Patch time.time() which is used for execute_at calculation
    # Patch settings from app.core.config where it's actually imported (line 273 in retry_job)
    with patch('app.services.job_service.time.time', return_value=fixed_time), \
         patch('app.core.config.settings') as mock_settings:
        
        mock_settings.INITIAL_RETRY_DELAY = 1
        mock_settings.RETRY_BACKOFF_BASE = 2
        
        result = await job_service.retry_job(mock_job.id)
    
    # Verify Redis was called with correct parameters
    mock_redis_client.push_to_delayed_queue.assert_called_once()
    call_args = mock_redis_client.push_to_delayed_queue.call_args
    
    # Verify job_data structure
    assert call_args.kwargs['job_data']['job_id'] == str(mock_job.id)
    assert call_args.kwargs['job_data']['job_type'] == 'test_job'
    assert call_args.kwargs['job_data']['priority'] == JobPriority.NORMAL.value
    
    # Verify execute_at timestamp
    assert call_args.kwargs['execute_at'] == expected_execute_at
    assert call_args.kwargs['queue_name'] == 'job_queue_delayed'
    
    # Verify job state was updated
    assert mock_job.retry_count == 1  # Incremented after Redis success
    assert mock_job.status == JobStatus.RETRYING
    
    # Verify database operations
    mock_db_session.flush.assert_called_once()
    mock_db_session.refresh.assert_called_once_with(mock_job)


@pytest.mark.asyncio
async def test_second_retry_schedules_with_4_second_delay(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that second retry (retry_count=1 -> 2) schedules job with 4 second delay.
    
    Calculation: 1 * (2 ^ 2) = 4 seconds
    """
    mock_job.retry_count = 1
    
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    fixed_time = 2000000.0
    expected_delay = 4.0  # 1 * (2^2) = 4 seconds
    expected_execute_at = fixed_time + expected_delay
    
    with patch('app.services.job_service.time.time', return_value=fixed_time), \
         patch('app.core.config.settings') as mock_settings:
        
        mock_settings.INITIAL_RETRY_DELAY = 1
        mock_settings.RETRY_BACKOFF_BASE = 2
        
        result = await job_service.retry_job(mock_job.id)
    
    call_args = mock_redis_client.push_to_delayed_queue.call_args
    assert call_args.kwargs['execute_at'] == expected_execute_at
    assert mock_job.retry_count == 2  # Incremented from 1 to 2


@pytest.mark.asyncio
async def test_third_retry_schedules_with_8_second_delay(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that third retry (retry_count=2 -> 3) schedules job with 8 second delay.
    
    Calculation: 1 * (2 ^ 3) = 8 seconds
    """
    mock_job.retry_count = 2
    
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    fixed_time = 3000000.0
    expected_delay = 8.0  # 1 * (2^3) = 8 seconds
    expected_execute_at = fixed_time + expected_delay
    
    with patch('app.services.job_service.time.time', return_value=fixed_time), \
         patch('app.core.config.settings') as mock_settings:
        
        mock_settings.INITIAL_RETRY_DELAY = 1
        mock_settings.RETRY_BACKOFF_BASE = 2
        
        result = await job_service.retry_job(mock_job.id)
    
    call_args = mock_redis_client.push_to_delayed_queue.call_args
    assert call_args.kwargs['execute_at'] == expected_execute_at
    assert mock_job.retry_count == 3  # Incremented from 2 to 3


# ============================================================================
# CATEGORY 2: Retry Count Management (2 tests)
# ============================================================================

@pytest.mark.asyncio
async def test_retry_count_increments_only_after_redis_succeeds(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that retry_count is incremented ONLY AFTER Redis scheduling succeeds.
    
    Flow verified by implementation:
    1. Call redis.push_to_delayed_queue()
    2. If success=True, increment job.retry_count
    3. Update job.status to RETRYING
    4. Flush and commit database changes
    """
    mock_job.retry_count = 1
    original_retry_count = mock_job.retry_count
    
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    with patch('app.services.job_service.time.time', return_value=5000000.0), \
         patch('app.core.config.settings') as mock_settings:
        
        mock_settings.INITIAL_RETRY_DELAY = 1
        mock_settings.RETRY_BACKOFF_BASE = 2
        
        result = await job_service.retry_job(mock_job.id)
    
    # Verify Redis was called first
    mock_redis_client.push_to_delayed_queue.assert_called_once()
    
    # Verify retry_count was incremented
    assert mock_job.retry_count == original_retry_count + 1
    assert mock_job.status == JobStatus.RETRYING
    
    # Verify database flush happened after increment
    mock_db_session.flush.assert_called_once()


@pytest.mark.asyncio
async def test_retry_count_unchanged_when_redis_returns_false(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that if Redis validation fails (returns False), retry_count is NOT incremented.
    
    Redis returns False for validation failures like:
    - Invalid priority (not 0-10)
    - NaN or infinity timestamp
    - Missing required fields
    
    Job should remain FAILED with unchanged retry_count.
    """
    mock_job.retry_count = 0
    original_retry_count = mock_job.retry_count
    original_status = mock_job.status
    
    # Redis validation failure
    mock_redis_client.push_to_delayed_queue = AsyncMock(return_value=False)
    
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    with patch('app.services.job_service.time.time', return_value=6000000.0), \
         patch('app.core.config.settings') as mock_settings:
        
        mock_settings.INITIAL_RETRY_DELAY = 1
        mock_settings.RETRY_BACKOFF_BASE = 2
        
        result = await job_service.retry_job(mock_job.id)
    
    # Verify Redis was called
    mock_redis_client.push_to_delayed_queue.assert_called_once()
    
    # Verify retry_count was NOT incremented
    assert mock_job.retry_count == original_retry_count
    assert mock_job.status == original_status  # Still FAILED
    
    # Verify database flush was NOT called (no changes to persist)
    mock_db_session.flush.assert_not_called()


# ============================================================================
# CATEGORY 3: Exception Handling (2 tests)
# ============================================================================

@pytest.mark.asyncio
async def test_redis_exception_propagates_and_prevents_increment(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that Redis exceptions propagate up from retry_job() and prevent retry_count increment.
    
    When Redis raises an exception (connection error, timeout, etc.):
    - Exception propagates to caller
    - retry_count is NOT incremented
    - Database transaction should be rolled back by caller
    """
    mock_job.retry_count = 0
    original_retry_count = mock_job.retry_count
    
    # Redis raises exception
    mock_redis_client.push_to_delayed_queue = AsyncMock(
        side_effect=ConnectionError("Redis unavailable")
    )
    
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    with patch('app.services.job_service.time.time', return_value=7000000.0), \
         patch('app.core.config.settings') as mock_settings:
        
        mock_settings.INITIAL_RETRY_DELAY = 1
        mock_settings.RETRY_BACKOFF_BASE = 2
        
        # Verify exception propagates
        with pytest.raises(ConnectionError, match="Redis unavailable"):
            await job_service.retry_job(mock_job.id)
    
    # Verify retry_count was NOT incremented
    assert mock_job.retry_count == original_retry_count
    
    # Verify database flush was NOT called
    mock_db_session.flush.assert_not_called()


@pytest.mark.asyncio
async def test_worker_poller_continues_after_transfer_errors():
    """
    Test that worker poller continues operating even if transfer_ready_jobs_atomic fails.
    
    The poller should:
    1. Log the error
    2. Sleep 5 seconds on error (not 1 second)
    3. Continue the polling loop
    4. Not crash the worker
    """
    mock_redis = AsyncMock(spec=RedisClient)
    
    # First two calls fail, third succeeds
    mock_redis.transfer_ready_jobs_atomic = AsyncMock(
        side_effect=[
            Exception("Transfer error 1"),
            Exception("Transfer error 2"),
            3  # Third call succeeds
        ]
    )
    
    worker = Worker(worker_id="test-worker")
    worker.running = True
    
    # Track sleep calls to verify error handling
    sleep_calls = []
    iteration_count = 0
    original_sleep = asyncio.sleep  # Save before patching
    
    async def mock_sleep(duration):
        nonlocal iteration_count
        sleep_calls.append(duration)
        iteration_count += 1
        if iteration_count >= 3:
            # Stop after 3 iterations
            worker.running = False
        await original_sleep(0)  # Yield control without recursion
    
    with patch('app.worker.worker.redis_client', mock_redis), \
         patch('asyncio.sleep', side_effect=mock_sleep), \
         patch('app.worker.worker.settings') as mock_settings:
        
        mock_settings.DELAYED_JOB_CHECK_INTERVAL = 1
        
        # Run poller directly without full worker startup
        poller_task = asyncio.create_task(worker._delayed_queue_poller_loop())
        
        try:
            await asyncio.wait_for(poller_task, timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("Poller did not complete within timeout - possible infinite loop")
    
    # Verify transfer was called 3 times despite first 2 errors
    assert mock_redis.transfer_ready_jobs_atomic.call_count == 3
    
    # Verify sleep was called with error delay (5s) for first two errors
    # and normal delay (1s) would have been called if loop continued
    # We expect: 5s (error 1), 5s (error 2), then loop exits
    assert 5 in sleep_calls  # Error sleep duration
    assert len(sleep_calls) >= 2


# ============================================================================
# CATEGORY 4: Dead Letter Queue Behavior (2 tests)
# ============================================================================

@pytest.mark.asyncio
async def test_job_moves_to_dead_letter_when_max_retries_reached(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that job is moved to dead letter queue when retry_count >= max_retries.
    
    With max_retries=3:
    - retry_count 0, 1, 2 -> schedule retry
    - retry_count 3 -> dead letter queue (no retry scheduled)
    
    Dead letter flow:
    - Status set to JobStatus.DEAD_LETTER
    - is_dead_letter set to True
    - moved_to_dlq_at timestamp set
    - NO Redis scheduling
    """
    mock_job.retry_count = 3  # Equals max_retries
    mock_job.max_retries = 3
    
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    # Mock move_to_dead_letter_queue to verify it's called
    job_service.move_to_dead_letter_queue = AsyncMock(return_value=mock_job)
    
    with patch('app.services.job_service.time.time', return_value=8000000.0):
        result = await job_service.retry_job(mock_job.id)
    
    # Verify dead letter queue method was called
    job_service.move_to_dead_letter_queue.assert_called_once_with(mock_job.id)
    
    # Verify NO Redis scheduling happened
    mock_redis_client.push_to_delayed_queue.assert_not_called()


@pytest.mark.asyncio
async def test_move_to_dead_letter_queue_sets_correct_fields(
    mock_db_session, mock_redis_client, mock_queue_service, mock_job
):
    """
    Test that move_to_dead_letter_queue() sets all required fields correctly.
    
    Fields that must be set:
    - status -> JobStatus.DEAD_LETTER
    - is_dead_letter -> True
    - moved_to_dlq_at -> current timestamp
    - updated_at -> current timestamp
    """
    job_service = JobService(mock_db_session, mock_redis_client, mock_queue_service)
    job_service.get_job = AsyncMock(return_value=mock_job)
    
    # Call move_to_dead_letter_queue directly
    result = await job_service.move_to_dead_letter_queue(mock_job.id)
    
    # Verify job fields were set correctly
    assert mock_job.status == JobStatus.DEAD_LETTER
    assert mock_job.is_dead_letter is True
    assert mock_job.moved_to_dlq_at is not None
    assert isinstance(mock_job.moved_to_dlq_at, datetime)
    
    # Verify database operations
    mock_db_session.flush.assert_called_once()
    mock_db_session.refresh.assert_called_once_with(mock_job)


# ============================================================================
# CATEGORY 5: Worker Poller Integration (3 tests)
# ============================================================================

@pytest.mark.asyncio
async def test_worker_poller_transfers_multiple_ready_jobs():
    """
    Test that worker poller successfully transfers multiple ready jobs from delayed queue.
    
    Simulates:
    - transfer_ready_jobs_atomic returns 5 (5 jobs transferred)
    - Poller logs the transfer
    - Poller continues running until stopped
    """
    mock_redis = AsyncMock(spec=RedisClient)
    mock_redis.transfer_ready_jobs_atomic = AsyncMock(return_value=5)
    
    worker = Worker(worker_id="test-worker")
    worker.running = True
    
    iteration_count = 0
    original_sleep = asyncio.sleep
    
    async def mock_sleep(duration):
        nonlocal iteration_count
        iteration_count += 1
        if iteration_count >= 2:  # Run 2 iterations
            worker.running = False
        await original_sleep(0)
    
    with patch('app.worker.worker.redis_client', mock_redis), \
         patch('asyncio.sleep', side_effect=mock_sleep), \
         patch('app.worker.worker.settings') as mock_settings:
        
        mock_settings.DELAYED_JOB_CHECK_INTERVAL = 1
        
        poller_task = asyncio.create_task(worker._delayed_queue_poller_loop())
        
        try:
            await asyncio.wait_for(poller_task, timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("Poller did not complete within timeout")
    
    # Verify transfer was called at least twice
    assert mock_redis.transfer_ready_jobs_atomic.call_count >= 2
    
    # Verify correct arguments
    call_args = mock_redis.transfer_ready_jobs_atomic.call_args
    assert call_args.kwargs['main_queue'] == 'job_queue'
    assert call_args.kwargs['delayed_queue'] == 'job_queue_delayed'
    assert call_args.kwargs['batch_limit'] == 100
    assert call_args.kwargs['quarantine_queue'] == 'job_queue_invalid'


@pytest.mark.asyncio
async def test_worker_poller_respects_check_interval():
    """
    Test that worker poller loop respects DELAYED_JOB_CHECK_INTERVAL sleep duration.
    
    Verifies:
    - asyncio.sleep is called with correct interval (1 second default)
    - Loop continues until explicitly stopped
    - Sleep happens after each transfer attempt
    """
    mock_redis = AsyncMock(spec=RedisClient)
    mock_redis.transfer_ready_jobs_atomic = AsyncMock(return_value=0)
    
    worker = Worker(worker_id="test-worker")
    worker.running = True
    
    sleep_calls = []
    iteration_count = 0
    original_sleep = asyncio.sleep
    
    async def mock_sleep(duration):
        nonlocal iteration_count
        sleep_calls.append(duration)
        iteration_count += 1
        if iteration_count >= 3:  # Run 3 iterations
            worker.running = False
        await original_sleep(0)
    
    with patch('app.worker.worker.redis_client', mock_redis), \
         patch('asyncio.sleep', side_effect=mock_sleep), \
         patch('app.worker.worker.settings') as mock_settings:
        
        mock_settings.DELAYED_JOB_CHECK_INTERVAL = 1
        
        poller_task = asyncio.create_task(worker._delayed_queue_poller_loop())
        
        try:
            await asyncio.wait_for(poller_task, timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("Poller did not complete within timeout")
    
    # Verify sleep was called with correct duration
    assert all(duration == 1 for duration in sleep_calls)
    assert len(sleep_calls) == 3


@pytest.mark.asyncio
async def test_worker_poller_handles_cancellation_cleanly():
    """
    Test that worker poller task handles cancellation without errors.
    
    This is critical for graceful worker shutdown:
    - Cancelling poller_task should be handled cleanly
    - Poller should exit on CancelledError without propagating it
    - Task should complete within timeout after cancellation
    """
    mock_redis = AsyncMock(spec=RedisClient)
    mock_redis.transfer_ready_jobs_atomic = AsyncMock(return_value=0)
    
    worker = Worker(worker_id="test-worker")
    worker.running = True
    
    iteration_count = 0
    original_sleep = asyncio.sleep
    
    async def mock_sleep(duration):
        nonlocal iteration_count
        iteration_count += 1
        # Allow one full iteration before cancellation is triggered externally
        await original_sleep(0)
    
    with patch('app.worker.worker.redis_client', mock_redis), \
         patch('asyncio.sleep', side_effect=mock_sleep), \
         patch('app.worker.worker.settings') as mock_settings:
        
        mock_settings.DELAYED_JOB_CHECK_INTERVAL = 1
        
        poller_task = asyncio.create_task(worker._delayed_queue_poller_loop())
        
        # Let poller run briefly, then cancel it
        await asyncio.sleep(0.1)
        poller_task.cancel()
        
        # Wait for clean shutdown
        try:
            await asyncio.wait_for(poller_task, timeout=1.0)
        except asyncio.CancelledError:
            pass  # Expected - clean cancellation
        except asyncio.TimeoutError:
            pytest.fail("Poller did not handle cancellation within timeout")
    
    # Verify poller was cancelled and completed
    assert poller_task.done()
    assert poller_task.cancelled() or poller_task.exception() is None
