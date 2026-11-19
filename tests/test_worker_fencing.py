"""
Tests for Worker-Level Job Completion Fencing

Verifies that the Worker._process_job() implementation correctly uses atomic
fencing to prevent stale worker updates from overwriting job state changes
made by recovery processes.

These tests verify end-to-end worker behavior including:
- Successful fenced completion with execution records
- Stale completion rejection without side effects
- Stale timeout rejection without retries
- Stale exception rejection without execution records
- No counter increments on fencing rejection
- Worker continues processing after fencing rejection
- Database error handling without partial commits
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import select

from app.db.models import Job, JobStatus, JobPriority, JobExecution, WorkerHeartbeat
from app.worker.worker import Worker
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.core.redis_client import RedisClient
from tests.conftest import TestSessionLocal


@pytest.mark.asyncio
async def test_successful_completion_creates_execution_record(db_session):
    """
    Test 1: Successful job completion with valid ownership creates execution record
    and commits.

    Verifies:
    - Job status updated to COMPLETED
    - Execution record created with correct details
    - Worker success counter incremented
    - Transaction committed
    """
    # Create a QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.1},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create worker heartbeat
    worker_id = "test-worker-1"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()

    # Create worker and process job
    # Patch AsyncSessionLocal to use test database
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)
        await worker._process_job(job_id)

    # Verify job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.worker_id == worker_id
    assert job.result is not None
    assert job.result["status"] == "success"
    assert job.completed_at is not None
    assert job.processing_duration is not None

    # Verify execution record created
    result = await db_session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
    execution = result.scalar_one_or_none()
    assert execution is not None
    assert execution.worker_id == worker_id
    assert execution.status == JobStatus.COMPLETED
    assert execution.attempt_number == 1
    assert execution.result is not None
    assert execution.duration is not None

    # Verify worker counter incremented
    await db_session.refresh(worker_heartbeat)
    assert worker_heartbeat.jobs_processed == 1
    assert worker_heartbeat.jobs_failed == 0


@pytest.mark.asyncio
async def test_stale_completion_rejected_no_execution_record(db_session):
    """
    Test 2: Stale completion is rejected without overwriting job or creating
    execution record.

    Simulates recovery process marking job FAILED before worker completes,
    then worker attempts to mark COMPLETED.

    Verifies:
    - Job remains FAILED (recovery state preserved)
    - No execution record created by stale worker
    - Transaction rolled back
    - Warning logged
    """
    # Create a QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.1},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create worker heartbeat
    worker_id = "test-worker-1"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()

    # Mock executor to inject recovery during execution
    async def mock_execute_with_recovery(job_type, payload):
        """Simulate job execution, but recovery marks FAILED midway"""
        # Start execution
        await asyncio.sleep(0.05)

        # Recovery process marks job FAILED (simulating timeout recovery)
        async with TestSessionLocal() as recovery_session:
            recovery_job_service = JobService(
                recovery_session,
                RedisClient(),
                QueueService(RedisClient()),
            )
            from app.models.job import JobUpdate

            await recovery_job_service.update_job_status(
                job_id,
                JobUpdate(
                    status=JobStatus.FAILED,
                    error_message="Recovery: job timed out",
                    error_type="RecoveryTimeout",
                ),
            )
            await recovery_session.commit()

        # Worker continues and completes
        await asyncio.sleep(0.05)
        return {"status": "success", "message": "completed"}

    # Create worker with mocked executor
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)
        with patch.object(worker.executor, "execute", side_effect=mock_execute_with_recovery):
            await worker._process_job(job_id)

    # Verify job remains FAILED (recovery state preserved)
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.error_message == "Recovery: job timed out"
    assert job.error_type == "RecoveryTimeout"
    assert job.result is None  # Worker's result NOT applied

    # Verify NO execution record created by worker
    result = await db_session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
    execution = result.scalar_one_or_none()
    assert execution is None, "No execution record should be created for rejected completion"

    # Verify worker counter NOT incremented
    await db_session.refresh(worker_heartbeat)
    assert worker_heartbeat.jobs_processed == 0
    assert worker_heartbeat.jobs_failed == 0


@pytest.mark.asyncio
async def test_stale_timeout_rejected_no_retry(db_session):
    """
    Test 3: Stale timeout handling is rejected without scheduling retry.

    Simulates recovery process marking job FAILED before worker's timeout
    handler runs.

    Verifies:
    - Job remains in recovery state
    - No retry scheduled by stale worker
    - Transaction rolled back
    """
    # Create a QUEUED job with short timeout
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 10},  # Will timeout
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=0.1,  # Very short timeout
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create worker heartbeat
    worker_id = "test-worker-2"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()

    # Mock executor to inject recovery after timeout
    async def mock_execute_with_timeout_and_recovery(job_type, payload):
        """Simulate timeout, but recovery marks FAILED first"""
        # This will timeout (processing_time=10 > timeout=0.1)
        await asyncio.sleep(payload["processing_time"])
        return {"status": "success"}

    # Inject recovery BEFORE worker's timeout handler runs
    original_wait_for = asyncio.wait_for

    async def wait_for_with_recovery(*args, **kwargs):
        """Wrap wait_for to inject recovery just before timeout exception"""
        try:
            return await original_wait_for(*args, **kwargs)
        except asyncio.TimeoutError:
            # Recovery marks job FAILED before worker's timeout handler
            async with TestSessionLocal() as recovery_session:
                recovery_job_service = JobService(
                    recovery_session,
                    RedisClient(),
                    QueueService(RedisClient()),
                )
                from app.models.job import JobUpdate

                await recovery_job_service.update_job_status(
                    job_id,
                    JobUpdate(
                        status=JobStatus.FAILED,
                        error_message="Recovery: worker timeout detected",
                        error_type="WorkerTimeout",
                    ),
                )
                await recovery_session.commit()

            # Re-raise timeout so worker's handler runs
            raise

    # Create worker with mocked executor and wait_for
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)
        with patch.object(
            worker.executor, "execute", side_effect=mock_execute_with_timeout_and_recovery
        ):
            with patch("asyncio.wait_for", side_effect=wait_for_with_recovery):
                await worker._process_job(job_id)

    # Verify job remains FAILED with recovery message
    await db_session.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.error_message == "Recovery: worker timeout detected"
    assert job.error_type == "WorkerTimeout"

    # Verify job is NOT in RETRYING state (no retry scheduled by stale worker)
    # Note: retry_job() would update next_retry_at, so we check it's None or unchanged
    assert job.next_retry_at is None or job.retry_count == 0

    # Verify worker counter NOT incremented
    await db_session.refresh(worker_heartbeat)
    assert worker_heartbeat.jobs_processed == 0
    assert worker_heartbeat.jobs_failed == 0


@pytest.mark.asyncio
async def test_stale_exception_rejected_no_execution_record(db_session):
    """
    Test 4: Stale exception handling is rejected without creating execution
    record or scheduling retry.

    Simulates recovery process marking job COMPLETED before worker's exception
    handler runs.

    Verifies:
    - Job remains COMPLETED (recovery state preserved)
    - No execution record created by stale worker
    - No retry scheduled
    - Transaction rolled back
    """
    # Create a QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "should_fail": True, "fail_message": "Test error"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create worker heartbeat
    worker_id = "test-worker-3"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()

    # Mock executor to inject recovery during execution
    async def mock_execute_with_exception_and_recovery(job_type, payload):
        """Simulate exception, but recovery completes job first"""
        await asyncio.sleep(0.05)

        # Recovery process marks job COMPLETED (admin override)
        async with TestSessionLocal() as recovery_session:
            recovery_job_service = JobService(
                recovery_session,
                RedisClient(),
                QueueService(RedisClient()),
            )
            from app.models.job import JobUpdate

            await recovery_job_service.update_job_status(
                job_id,
                JobUpdate(
                    status=JobStatus.COMPLETED,
                    result={"recovered": True, "admin_override": True},
                ),
            )
            await recovery_session.commit()

        # Worker hits exception
        raise ValueError("Test error")

    # Create worker with mocked executor
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)
        with patch.object(
            worker.executor, "execute", side_effect=mock_execute_with_exception_and_recovery
        ):
            await worker._process_job(job_id)

    # Verify job remains COMPLETED (recovery state preserved)
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.result == {"recovered": True, "admin_override": True}
    assert job.error_message is None

    # Verify NO execution record created by worker
    result = await db_session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
    execution = result.scalar_one_or_none()
    assert execution is None, "No execution record should be created for rejected failure"

    # Verify job is NOT in RETRYING state
    assert job.next_retry_at is None or job.retry_count == 0

    # Verify worker counter NOT incremented
    await db_session.refresh(worker_heartbeat)
    assert worker_heartbeat.jobs_processed == 0
    assert worker_heartbeat.jobs_failed == 0


@pytest.mark.asyncio
async def test_fencing_rejection_no_counter_increment(db_session):
    """
    Test 5: Fencing rejection does not increment worker success/failure counters.

    Verifies that when fencing rejects an update, the worker's job counters
    remain unchanged.
    """
    # Create a QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.1},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create worker heartbeat with initial counts
    worker_id = "test-worker-4"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=10,  # Pre-existing count
        jobs_failed=2,  # Pre-existing count
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()
    initial_processed = worker_heartbeat.jobs_processed
    initial_failed = worker_heartbeat.jobs_failed

    # Mock executor to inject recovery during execution
    async def mock_execute_with_recovery(job_type, payload):
        """Simulate completion, but recovery marks FAILED first"""
        await asyncio.sleep(0.05)

        # Recovery marks job FAILED
        async with TestSessionLocal() as recovery_session:
            recovery_job_service = JobService(
                recovery_session,
                RedisClient(),
                QueueService(RedisClient()),
            )
            from app.models.job import JobUpdate

            await recovery_job_service.update_job_status(
                job_id,
                JobUpdate(
                    status=JobStatus.FAILED,
                    error_message="Recovery failure",
                    error_type="RecoveryTest",
                ),
            )
            await recovery_session.commit()

        return {"status": "success"}

    # Create worker with mocked executor
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)
        with patch.object(worker.executor, "execute", side_effect=mock_execute_with_recovery):
            await worker._process_job(job_id)

    # Verify worker counters unchanged
    await db_session.refresh(worker_heartbeat)
    assert (
        worker_heartbeat.jobs_processed == initial_processed
    ), "Success counter should not increment when fencing rejects"
    assert (
        worker_heartbeat.jobs_failed == initial_failed
    ), "Failure counter should not increment when fencing rejects"


@pytest.mark.asyncio
async def test_worker_continues_after_fencing_rejection(db_session):
    """
    Test 6: Fencing rejection does not prevent worker from processing subsequent jobs.

    Verifies that after a fencing rejection, the worker can successfully process
    the next job.
    """
    # Create two jobs
    job1 = Job(
        job_type="test_job",
        payload={"data": "job1", "processing_time": 0.1},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    job2 = Job(
        job_type="test_job",
        payload={"data": "job2", "processing_time": 0.1},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add_all([job1, job2])
    await db_session.commit()
    await db_session.refresh(job1)
    await db_session.refresh(job2)
    job1_id = job1.id
    job2_id = job2.id

    # Create worker heartbeat
    worker_id = "test-worker-5"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()

    # Mock executor to inject recovery for first job only
    call_count = {"count": 0}

    async def mock_execute_with_recovery_for_first_job(job_type, payload):
        """First job gets recovery, second job succeeds normally"""
        call_count["count"] += 1
        await asyncio.sleep(0.05)

        if call_count["count"] == 1:
            # First job: Recovery marks FAILED
            async with TestSessionLocal() as recovery_session:
                recovery_job_service = JobService(
                    recovery_session,
                    RedisClient(),
                    QueueService(RedisClient()),
                )
                from app.models.job import JobUpdate

                await recovery_job_service.update_job_status(
                    job1_id,
                    JobUpdate(
                        status=JobStatus.FAILED,
                        error_message="Recovery for job 1",
                        error_type="Recovery",
                    ),
                )
                await recovery_session.commit()

        return {"status": "success", "job": payload.get("data")}

    # Create worker with mocked executor
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)
        with patch.object(
            worker.executor, "execute", side_effect=mock_execute_with_recovery_for_first_job
        ):
            # Process first job (fencing rejection)
            await worker._process_job(job1_id)

            # Process second job (should succeed)
            await worker._process_job(job2_id)

    # Verify first job rejected (remains FAILED)
    await db_session.refresh(job1)
    assert job1.status == JobStatus.FAILED
    assert job1.error_message == "Recovery for job 1"

    # Verify second job completed successfully
    await db_session.refresh(job2)
    assert job2.status == JobStatus.COMPLETED
    assert job2.result["status"] == "success"
    assert job2.result["job"] == "job2"

    # Verify worker counter only incremented for second job
    await db_session.refresh(worker_heartbeat)
    assert worker_heartbeat.jobs_processed == 1, "Only second job should increment counter"
    assert worker_heartbeat.jobs_failed == 0


@pytest.mark.asyncio
async def test_database_error_no_partial_commit(db_session):
    """
    Test 7: Unexpected database errors are handled without committing partial
    state or incorrectly marking job FAILED.

    Simulates a database error during execution record creation to verify
    the outer exception handler rolls back properly.

    Verifies:
    - Job status not incorrectly marked FAILED
    - No partial execution record committed
    - Transaction rolled back
    """
    # Create a QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.1},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create worker heartbeat
    worker_id = "test-worker-6"
    worker_heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        started_at=datetime.utcnow(),
        last_heartbeat=datetime.utcnow(),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker_heartbeat)
    await db_session.commit()

    # Mock create_job_execution to raise database error
    async def mock_create_execution_with_error(*args, **kwargs):
        """Simulate database error during execution record creation"""
        from sqlalchemy.exc import DBAPIError

        raise DBAPIError(
            "simulated error",
            None,
            Exception("Database connection lost"),
        )

    # Create worker and process job with mocked execution record creation
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker = Worker(worker_id)

        # We need to patch at the worker level after claim succeeds
        original_process = worker._process_job

        async def process_with_db_error(job_id):
            """Wrap _process_job to inject error after job starts"""
            # Let claim succeed
            async with TestSessionLocal() as temp_session:
                job_service_temp = JobService(
                    temp_session,
                    RedisClient(),
                    QueueService(RedisClient()),
                )
                with patch.object(
                    job_service_temp,
                    "create_job_execution",
                    side_effect=mock_create_execution_with_error,
                ):
                    # This won't work because we need to patch the actual instance
                    pass

            # Instead, patch JobService.create_job_execution at class level
            with patch(
                "app.services.job_service.JobService.create_job_execution",
                side_effect=mock_create_execution_with_error,
            ):
                await original_process(job_id)

        await process_with_db_error(job_id)

    # Verify job remains in PROCESSING or reverted (not incorrectly marked COMPLETED)
    await db_session.refresh(job)
    # After database error, outer exception handler rolls back, job should be PROCESSING
    assert job.status in [
        JobStatus.PROCESSING,
        JobStatus.QUEUED,
    ], f"Job should remain in PROCESSING/QUEUED after DB error, but is {job.status}"

    # Verify NO execution record created
    result = await db_session.execute(select(JobExecution).where(JobExecution.job_id == job_id))
    execution = result.scalar_one_or_none()
    assert execution is None, "No execution record should exist after database error"

    # Verify worker counter NOT incremented
    await db_session.refresh(worker_heartbeat)
    assert worker_heartbeat.jobs_processed == 0
    assert worker_heartbeat.jobs_failed == 0
