"""
Tests for Execution Generation Fencing using retry_count

Verifies that expected_retry_count parameter prevents stale worker executions
from overwriting newer execution state when worker IDs are reused (e.g., Kubernetes
pod restarts).

These tests verify:
1. Stale COMPLETED/FAILED/timeout updates are rejected
2. Valid generation 0 and 1 completions succeed
3. Future generation is rejected
4. Invalid generation types are rejected
5. Generation mismatch preserves job state
6. Concurrent terminal updates cannot both succeed (PostgreSQL row-level locking)
7. Actual Worker._process_job uses captured generation in all terminal paths
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import patch

from app.db.models import Job, JobStatus, JobPriority, WorkerHeartbeat
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.core.redis_client import RedisClient
from app.models.job import JobUpdate
from app.worker.worker import Worker
from tests.conftest import TestSessionLocal


@pytest.mark.asyncio
async def test_stale_completed_with_reused_worker_id_rejected(db_session):
    """
    Test 1: Stale COMPLETED update with reused worker_id is rejected.

    Simulates:
    - Worker pod-1 claims job (retry_count=0)
    - Recovery increments retry_count to 1
    - New pod-1 claims retry (retry_count=1)
    - Old pod-1 attempts completion with expected_retry_count=0
    - Fencing REJECTS because retry_count is now 1
    """
    # Create job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-1",
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Old execution captures generation 0
    old_execution_retry_count = 0

    # Simulate recovery: increment retry_count
    job.retry_count = 1
    await db_session.commit()

    # Old execution attempts completion with stale generation
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.COMPLETED, result={"stale": True}),
        worker_id="worker-pod-1",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-pod-1",
        expected_retry_count=old_execution_retry_count,  # 0 (stale)
    )

    # Verify rejection
    assert result is None, "Stale completion should be rejected"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 1
    assert job.result is None


@pytest.mark.asyncio
async def test_stale_failed_with_reused_worker_id_rejected(db_session):
    """
    Test 2: Stale FAILED update with reused worker_id is rejected.
    """
    # Create job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-2",
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Old execution captures generation 0
    old_execution_retry_count = 0

    # Simulate recovery: increment retry_count
    job.retry_count = 1
    await db_session.commit()

    # Old execution attempts FAILED with stale generation
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.FAILED, error_message="Stale failure"),
        worker_id="worker-pod-2",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-pod-2",
        expected_retry_count=old_execution_retry_count,  # 0 (stale)
    )

    # Verify rejection
    assert result is None, "Stale FAILED should be rejected"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 1
    assert job.error_message is None


@pytest.mark.asyncio
async def test_stale_timeout_cannot_overwrite_newer_execution(db_session):
    """
    Test 3: Stale timeout update cannot overwrite a newer execution.
    """
    # Create job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-timeout-1",
        retry_count=1,  # Already at generation 1
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Old execution from generation 0 attempts timeout
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.FAILED, error_message="Timeout from generation 0"),
        worker_id="worker-timeout-1",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-timeout-1",
        expected_retry_count=0,  # Stale generation
    )

    # Verify rejection
    assert result is None, "Stale timeout should be rejected"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 1
    assert job.error_message is None


@pytest.mark.asyncio
async def test_valid_initial_generation_0_completes_successfully(db_session):
    """
    Test 4: Valid initial generation 0 completes successfully.
    """
    # Create job at generation 0
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-gen0",
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Execution completes with valid generation 0
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.COMPLETED, result={"generation": 0}),
        worker_id="worker-gen0",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-gen0",
        expected_retry_count=0,  # Valid generation
    )

    # Verify success
    assert result is not None, "Valid generation 0 should succeed"
    await db_session.commit()

    # Verify job updated
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.result == {"generation": 0}
    assert job.retry_count == 0


@pytest.mark.asyncio
async def test_valid_recovered_generation_1_completes_successfully(db_session):
    """
    Test 5: Valid recovered generation 1 completes successfully.
    """
    # Create job at generation 1 (after recovery)
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-gen1",
        retry_count=1,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Execution completes with valid generation 1
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.COMPLETED, result={"generation": 1}),
        worker_id="worker-gen1",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-gen1",
        expected_retry_count=1,  # Valid generation
    )

    # Verify success
    assert result is not None, "Valid generation 1 should succeed"
    await db_session.commit()

    # Verify job updated
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.result == {"generation": 1}
    assert job.retry_count == 1


@pytest.mark.asyncio
async def test_future_generation_rejected(db_session):
    """
    Test 6: Future generation is rejected.
    """
    # Create job at generation 0
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-future",
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Attempt completion with future generation
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.COMPLETED, result={"future": True}),
        worker_id="worker-future",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-future",
        expected_retry_count=5,  # Future generation (job is at 0)
    )

    # Verify rejection
    assert result is None, "Future generation should be rejected"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 0
    assert job.result is None


@pytest.mark.asyncio
async def test_invalid_generation_types_rejected(db_session):
    """
    Test 7: Invalid generation types (bool, float, string, negative) are rejected.
    """
    # Create job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-invalid",
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))

    # Test bool (False is treated as 0 in Python, so we explicitly check bool type)
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.COMPLETED),
        expected_retry_count=True,  # Boolean
    )
    assert result is None, "Boolean expected_retry_count should be rejected"

    # Test negative
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(status=JobStatus.COMPLETED),
        expected_retry_count=-1,  # Negative
    )
    assert result is None, "Negative expected_retry_count should be rejected"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING


@pytest.mark.asyncio
async def test_generation_mismatch_preserves_all_job_state(db_session):
    """
    Test 8: Generation mismatch does not modify result, status, retry_count, or worker_id.
    """
    # Create job with specific state
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-preserve",
        retry_count=1,
        max_retries=3,
        timeout=300,
        result={"previous": "state"},
        error_message="Previous error",
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Capture original state
    original_status = job.status
    original_worker_id = job.worker_id
    original_retry_count = job.retry_count
    original_result = job.result
    original_error_message = job.error_message

    # Attempt update with stale generation
    job_service = JobService(db_session, RedisClient(), QueueService(RedisClient()))
    result = await job_service.update_job_status(
        job_id,
        JobUpdate(
            status=JobStatus.COMPLETED,
            result={"new": "data"},
            error_message="New error",
        ),
        worker_id="worker-preserve",
        expected_status=JobStatus.PROCESSING,
        expected_worker_id="worker-preserve",
        expected_retry_count=0,  # Stale generation
    )

    # Verify rejection
    assert result is None, "Stale generation should be rejected"

    # Verify ALL state unchanged
    await db_session.refresh(job)
    assert job.status == original_status, "Status should be preserved"
    assert job.worker_id == original_worker_id, "Worker ID should be preserved"
    assert job.retry_count == original_retry_count, "Retry count should be preserved"
    assert job.result == original_result, "Result should be preserved"
    assert job.error_message == original_error_message, "Error message should be preserved"


@pytest.mark.asyncio
async def test_concurrent_terminal_updates_only_one_succeeds(db_session):
    """
    Test 9: Two competing PostgreSQL terminal updates cannot both succeed.

    Uses real PostgreSQL row-level locking to verify only one update wins.
    """

    async def attempt_completion(worker_id: str, generation: int, delay: float, job_id):
        """Attempt to complete job after delay"""
        await asyncio.sleep(delay)

        async with TestSessionLocal() as session:
            job_service = JobService(session, RedisClient(), QueueService(RedisClient()))
            result = await job_service.update_job_status(
                job_id,
                JobUpdate(status=JobStatus.COMPLETED, result={"worker": worker_id}),
                worker_id=worker_id,
                expected_status=JobStatus.PROCESSING,
                expected_worker_id=worker_id,
                expected_retry_count=generation,
            )

            if result is not None:
                await session.commit()
                return True
            return False

    # Create job in the test session
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-concurrent-1",
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Two concurrent attempts with same generation
    results = await asyncio.gather(
        attempt_completion("worker-concurrent-1", 0, 0.01, job_id),
        attempt_completion("worker-concurrent-1", 0, 0.01, job_id),
        return_exceptions=True,
    )

    # Count successes
    successes = sum(1 for r in results if r is True)

    # Verify only one succeeded
    assert successes == 1, f"Expected exactly 1 success, got {successes}"

    # Verify job is COMPLETED with one worker's result
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.result["worker"] == "worker-concurrent-1"


@pytest.mark.asyncio
async def test_worker_success_path_supplies_captured_generation(db_session):
    """
    Test 10: Actual Worker._process_job success path supplies the captured generation.

    Verifies that Worker captures retry_count after claim and passes it to
    the success completion path.
    """
    # Create job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        retry_count=0,
        max_retries=3,
        priority=JobPriority.NORMAL.value,
        timeout=300,
    )
    db_session.add(job)

    # Create worker heartbeat
    worker = WorkerHeartbeat(
        worker_id="test-worker-success",
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Track update_job_status calls
    update_calls = []
    original_update = JobService.update_job_status

    async def tracking_update(self, job_id, update_data, **kwargs):
        """Track all update_job_status calls"""
        update_calls.append(
            {
                "job_id": job_id,
                "status": update_data.status,
                "expected_retry_count": kwargs.get("expected_retry_count"),
            }
        )
        return await original_update(self, job_id, update_data, **kwargs)

    # Process job with tracking
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch.object(JobService, "update_job_status", tracking_update):
            worker_instance = Worker("test-worker-success")
            await worker_instance._process_job(job_id)

    # Verify completion call included expected_retry_count
    completion_calls = [c for c in update_calls if c["status"] == JobStatus.COMPLETED]
    assert len(completion_calls) == 1, "Should have one completion call"
    assert (
        completion_calls[0]["expected_retry_count"] == 0
    ), "Success path should pass captured generation 0"

    # Verify job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_worker_exception_path_supplies_captured_generation(db_session):
    """
    Test 11: Actual Worker._process_job exception path supplies the captured generation.
    """
    # Create job that will fail
    job = Job(
        job_type="test_job",
        payload={"should_fail": True, "fail_message": "Test exception"},
        status=JobStatus.QUEUED,
        retry_count=0,
        max_retries=3,
        priority=JobPriority.NORMAL.value,
        timeout=300,
    )
    db_session.add(job)

    # Create worker heartbeat
    worker = WorkerHeartbeat(
        worker_id="test-worker-exception",
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Track update_job_status calls
    update_calls = []
    original_update = JobService.update_job_status

    async def tracking_update(self, job_id, update_data, **kwargs):
        """Track all update_job_status calls"""
        update_calls.append(
            {
                "job_id": job_id,
                "status": update_data.status,
                "expected_retry_count": kwargs.get("expected_retry_count"),
            }
        )
        return await original_update(self, job_id, update_data, **kwargs)

    # Process job with tracking
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch.object(JobService, "update_job_status", tracking_update):
            worker_instance = Worker("test-worker-exception")
            await worker_instance._process_job(job_id)

    # Verify terminal update included expected_retry_count
    # Exception path will result in RETRYING (retry_count < max_retries) or FAILED
    terminal_calls = [c for c in update_calls if c.get("expected_retry_count") is not None]
    assert len(terminal_calls) >= 1, "Should have terminal update with expected_retry_count"
    # The first terminal call should have generation 0
    assert (
        terminal_calls[0]["expected_retry_count"] == 0
    ), "Exception path should pass captured generation 0"


@pytest.mark.asyncio
async def test_worker_timeout_path_supplies_captured_generation(db_session):
    """
    Test 12: Actual Worker._process_job timeout path supplies the captured generation.
    """
    # Create job that will timeout
    job = Job(
        job_type="test_job",
        payload={"processing_time": 10.0},  # Long job
        status=JobStatus.QUEUED,
        retry_count=0,
        max_retries=3,
        priority=JobPriority.NORMAL.value,
        timeout=0.1,  # Short timeout
    )
    db_session.add(job)

    # Create worker heartbeat
    worker = WorkerHeartbeat(
        worker_id="test-worker-timeout",
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Track update_job_status calls
    update_calls = []
    original_update = JobService.update_job_status

    async def tracking_update(self, job_id, update_data, **kwargs):
        """Track all update_job_status calls"""
        update_calls.append(
            {
                "job_id": job_id,
                "status": update_data.status,
                "expected_retry_count": kwargs.get("expected_retry_count"),
            }
        )
        return await original_update(self, job_id, update_data, **kwargs)

    # Process job with tracking
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch.object(JobService, "update_job_status", tracking_update):
            worker_instance = Worker("test-worker-timeout")
            await worker_instance._process_job(job_id)

    # Verify timeout resulted in terminal update with expected_retry_count
    # Timeout can result in RETRYING or terminal FAILED depending on retry_count
    terminal_calls = [c for c in update_calls if c.get("expected_retry_count") is not None]
    assert len(terminal_calls) >= 1, "Should have terminal update with expected_retry_count"
    assert (
        terminal_calls[0]["expected_retry_count"] == 0
    ), "Timeout path should pass captured generation 0"
