"""Tests for automatic stuck-job recovery service

Tests cover:
1. Stuck job detection with timeout + safety buffer
2. Atomic recovery claiming with FOR UPDATE SKIP LOCKED
3. Worker heartbeat verification
4. Max retries handling and dead letter queue
5. Outbox event creation for retry
6. Batch recovery operations
7. Race condition handling (concurrent recovery attempts)
"""

from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models import Job, JobOutbox, JobStatus, WorkerHeartbeat
from app.services.job_recovery_service import JobRecoveryService


@pytest.fixture
def recovery_service(db_session: AsyncSession):
    """Create job recovery service"""
    return JobRecoveryService(db_session)


@pytest.fixture
def recovery_service_with_factory(db_session: AsyncSession):
    """Create job recovery service with session factory for batch operations"""
    from tests.conftest import TestSessionLocal

    return JobRecoveryService(db_session, session_factory=TestSessionLocal)


@pytest.fixture
async def stuck_job_default_timeout(db_session: AsyncSession) -> Job:
    """
    Create a stuck job with default timeout (300s) started 320s ago.

    Stuck because: 320s > (300s timeout + 10s safety buffer)
    """
    job = Job(
        id=uuid4(),
        job_type="stuck_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-dead-1",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=settings.JOB_TIMEOUT,  # 300s default
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    return job


@pytest.fixture
async def stuck_job_custom_timeout(db_session: AsyncSession) -> Job:
    """
    Create a stuck job with custom timeout (60s) started 75s ago.

    Stuck because: 75s > (60s timeout + 10s safety buffer)
    """
    job = Job(
        id=uuid4(),
        job_type="custom_timeout_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-dead-2",
        started_at=datetime.utcnow() - timedelta(seconds=75),
        timeout=60,  # Custom timeout
        retry_count=1,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    return job


@pytest.fixture
async def not_stuck_job_within_timeout(db_session: AsyncSession) -> Job:
    """
    Create a job that appears stuck but is within timeout + safety buffer.

    Not stuck because: 250s < (300s timeout + 10s safety buffer)
    """
    job = Job(
        id=uuid4(),
        job_type="not_stuck_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-active-1",
        started_at=datetime.utcnow() - timedelta(seconds=250),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    return job


@pytest.fixture
async def not_stuck_job_safety_buffer(db_session: AsyncSession) -> Job:
    """
    Create a job within safety buffer window (timeout passed but buffer not).

    Not stuck because: 305s < (300s timeout + 10s safety buffer)
    """
    job = Job(
        id=uuid4(),
        job_type="safety_buffer_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-active-2",
        started_at=datetime.utcnow() - timedelta(seconds=305),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    return job


@pytest.fixture
async def dead_worker_heartbeat(db_session: AsyncSession) -> WorkerHeartbeat:
    """Create a dead worker heartbeat (last_heartbeat > 60s ago)"""
    heartbeat = WorkerHeartbeat(
        worker_id="worker-dead-1",
        is_active=True,
        jobs_processed=10,
        jobs_failed=0,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),  # 2 minutes ago (dead)
    )
    db_session.add(heartbeat)
    await db_session.commit()
    await db_session.refresh(heartbeat)
    return heartbeat


@pytest.fixture
async def alive_worker_heartbeat(db_session: AsyncSession) -> WorkerHeartbeat:
    """Create an alive worker heartbeat (last_heartbeat < 60s ago)"""
    heartbeat = WorkerHeartbeat(
        worker_id="worker-active-1",
        is_active=True,
        jobs_processed=5,
        jobs_failed=0,
        started_at=datetime.utcnow() - timedelta(minutes=5),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=30),  # 30 seconds ago (alive)
    )
    db_session.add(heartbeat)
    await db_session.commit()
    await db_session.refresh(heartbeat)
    return heartbeat


# ============================================================================
# Stuck Job Detection Tests
# ============================================================================


@pytest.mark.asyncio
async def test_detect_stuck_job_default_timeout(
    recovery_service: JobRecoveryService, stuck_job_default_timeout: Job
):
    """Test detection of stuck job with default timeout"""
    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)

    assert len(stuck_ids) == 1
    assert stuck_ids[0] == stuck_job_default_timeout.id


@pytest.mark.asyncio
async def test_detect_stuck_job_custom_timeout(
    recovery_service: JobRecoveryService, stuck_job_custom_timeout: Job
):
    """Test detection of stuck job with custom timeout"""
    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)

    assert len(stuck_ids) == 1
    assert stuck_ids[0] == stuck_job_custom_timeout.id


@pytest.mark.asyncio
async def test_detect_multiple_stuck_jobs(
    recovery_service: JobRecoveryService,
    stuck_job_default_timeout: Job,
    stuck_job_custom_timeout: Job,
):
    """Test detection of multiple stuck jobs"""
    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)

    assert len(stuck_ids) == 2
    assert stuck_job_default_timeout.id in stuck_ids
    assert stuck_job_custom_timeout.id in stuck_ids


@pytest.mark.asyncio
async def test_detect_respects_batch_size(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that detection respects batch_size parameter"""
    # Create 5 stuck jobs
    for i in range(5):
        job = Job(
            id=uuid4(),
            job_type=f"stuck_task_{i}",
            payload={"data": f"test_{i}"},
            status=JobStatus.PROCESSING,
            worker_id=f"worker-{i}",
            started_at=datetime.utcnow() - timedelta(seconds=320),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        db_session.add(job)
    await db_session.commit()

    # Request only 3
    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=3)
    assert len(stuck_ids) == 3


@pytest.mark.asyncio
async def test_detect_no_stuck_jobs_within_timeout(
    recovery_service: JobRecoveryService, not_stuck_job_within_timeout: Job
):
    """Test that jobs within timeout are not detected"""
    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert len(stuck_ids) == 0


@pytest.mark.asyncio
async def test_detect_no_stuck_jobs_in_safety_buffer(
    recovery_service: JobRecoveryService, not_stuck_job_safety_buffer: Job
):
    """Test that jobs in safety buffer window are not detected"""
    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert len(stuck_ids) == 0


@pytest.mark.asyncio
async def test_detect_only_processing_status(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that only PROCESSING jobs are detected"""
    # Create jobs in various states, all timed out
    for status in [JobStatus.PENDING, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.RETRYING]:
        job = Job(
            id=uuid4(),
            job_type=f"task_{status.value}",
            payload={"data": "test"},
            status=status,
            worker_id="worker-1",
            started_at=datetime.utcnow() - timedelta(seconds=320),
            timeout=300,
        )
        db_session.add(job)
    await db_session.commit()

    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert len(stuck_ids) == 0


@pytest.mark.asyncio
async def test_detect_requires_worker_id(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that jobs without worker_id are not detected"""
    job = Job(
        id=uuid4(),
        job_type="orphan_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id=None,  # No worker assigned
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()

    stuck_ids = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert len(stuck_ids) == 0


# ============================================================================
# Worker Heartbeat Verification Tests
# ============================================================================


@pytest.mark.asyncio
async def test_is_worker_alive_fresh_heartbeat(
    recovery_service: JobRecoveryService, alive_worker_heartbeat: WorkerHeartbeat
):
    """Test worker is considered alive with fresh heartbeat"""
    is_alive = await recovery_service._is_worker_alive(alive_worker_heartbeat.worker_id)
    assert is_alive is True


@pytest.mark.asyncio
async def test_is_worker_alive_stale_heartbeat(
    recovery_service: JobRecoveryService, dead_worker_heartbeat: WorkerHeartbeat
):
    """Test worker is considered dead with stale heartbeat"""
    is_alive = await recovery_service._is_worker_alive(dead_worker_heartbeat.worker_id)
    assert is_alive is False


@pytest.mark.asyncio
async def test_is_worker_alive_missing_heartbeat(recovery_service: JobRecoveryService):
    """Test missing worker heartbeat is considered dead"""
    is_alive = await recovery_service._is_worker_alive("nonexistent-worker")
    assert is_alive is False


# ============================================================================
# Atomic Recovery Tests
# ============================================================================


@pytest.mark.asyncio
async def test_recover_stuck_job_success(
    db_session: AsyncSession,
    recovery_service: JobRecoveryService,
    stuck_job_default_timeout: Job,
    dead_worker_heartbeat: WorkerHeartbeat,
):
    """Test successful recovery of stuck job transitions to RETRYING"""
    initial_retry_count = stuck_job_default_timeout.retry_count or 0

    success = await recovery_service.recover_stuck_job(stuck_job_default_timeout.id)
    assert success is True

    # Commit the recovery transaction (caller responsibility)
    await db_session.commit()

    # Verify job transitioned to RETRYING (not FAILED)
    await db_session.refresh(stuck_job_default_timeout)
    assert stuck_job_default_timeout.status == JobStatus.RETRYING
    assert stuck_job_default_timeout.error_type == "StuckJobRecovery"
    assert "stuck in PROCESSING" in stuck_job_default_timeout.error_message
    assert stuck_job_default_timeout.completed_at is not None

    # Verify retry_count incremented exactly once
    assert stuck_job_default_timeout.retry_count == initial_retry_count + 1

    # Verify next_retry_at calculated and set
    assert stuck_job_default_timeout.next_retry_at is not None
    # Verify exponential backoff: delay = INITIAL_DELAY * (BACKOFF_BASE ** retry_count)
    expected_delay = settings.INITIAL_RETRY_DELAY * (
        settings.RETRY_BACKOFF_BASE ** (initial_retry_count + 1)
    )
    # next_retry_at should be close to now + expected_delay (within 5 seconds)
    time_until_retry = (stuck_job_default_timeout.next_retry_at - datetime.utcnow()).total_seconds()
    assert abs(time_until_retry - expected_delay) < 5

    # Verify outbox event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == stuck_job_default_timeout.id)
    result = await db_session.execute(stmt)
    outbox_event = result.scalar_one_or_none()

    assert outbox_event is not None
    assert outbox_event.event_type == "job_recovered"
    assert outbox_event.published is False
    assert outbox_event.attempts == 0


@pytest.mark.asyncio
async def test_recover_proceeds_despite_worker_heartbeat(
    db_session: AsyncSession,
    recovery_service: JobRecoveryService,
    stuck_job_default_timeout: Job,
    alive_worker_heartbeat: WorkerHeartbeat,
):
    """Test recovery proceeds based on job timeout, regardless of worker heartbeat.

    FIX: Recovery decision is based on job timeout (authoritative contract),
    not worker heartbeat status. A fresh heartbeat does not prevent recovery
    of a legitimately timed-out job.

    Rationale: Worker can be hung/deadlocked but still heartbeating, or
    heartbeat can be from a previous execution generation.
    """
    # Update job to use alive worker
    stuck_job_default_timeout.worker_id = alive_worker_heartbeat.worker_id
    await db_session.commit()

    success = await recovery_service.recover_stuck_job(stuck_job_default_timeout.id)
    assert success is True, "Recovery should proceed based on job timeout"

    # Verify job transitioned to RETRYING
    await db_session.refresh(stuck_job_default_timeout)
    assert stuck_job_default_timeout.status == JobStatus.RETRYING


@pytest.mark.asyncio
async def test_recover_skips_if_already_claimed(
    db_session: AsyncSession, recovery_service: JobRecoveryService, stuck_job_default_timeout: Job
):
    """Test recovery skips if job already changed status"""
    # Simulate another recovery process already claimed and updated the job
    stuck_job_default_timeout.status = JobStatus.COMPLETED
    await db_session.commit()

    success = await recovery_service.recover_stuck_job(stuck_job_default_timeout.id)
    assert success is False


@pytest.mark.asyncio
async def test_recover_moves_to_dlq_if_max_retries_exceeded(
    db_session: AsyncSession,
    recovery_service: JobRecoveryService,
    stuck_job_default_timeout: Job,
    dead_worker_heartbeat: WorkerHeartbeat,
):
    """Test recovery moves job to DLQ if max retries exceeded"""
    # Set retry_count >= max_retries
    stuck_job_default_timeout.retry_count = 3
    stuck_job_default_timeout.max_retries = 3
    await db_session.commit()

    success = await recovery_service.recover_stuck_job(stuck_job_default_timeout.id)
    assert success is True

    # Commit the recovery transaction (caller responsibility)
    await db_session.commit()

    # Verify job moved to DEAD_LETTER
    await db_session.refresh(stuck_job_default_timeout)
    assert stuck_job_default_timeout.status == JobStatus.DEAD_LETTER
    assert stuck_job_default_timeout.is_dead_letter is True
    assert stuck_job_default_timeout.moved_to_dlq_at is not None
    assert stuck_job_default_timeout.error_type == "StuckJobRecoveryMaxRetriesExceeded"

    # Verify NO outbox event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == stuck_job_default_timeout.id)
    result = await db_session.execute(stmt)
    outbox_event = result.scalar_one_or_none()
    assert outbox_event is None


@pytest.mark.asyncio
async def test_recover_handles_nonexistent_job(recovery_service: JobRecoveryService):
    """Test recovery handles nonexistent job gracefully"""
    fake_job_id = uuid4()
    success = await recovery_service.recover_stuck_job(fake_job_id)
    assert success is False


# ============================================================================
# Batch Recovery Tests
# ============================================================================


@pytest.mark.asyncio
async def test_recover_batch_success(
    db_session: AsyncSession,
    recovery_service_with_factory: JobRecoveryService,
    stuck_job_default_timeout: Job,
    stuck_job_custom_timeout: Job,
):
    """Test batch recovery of multiple stuck jobs with session factory"""
    # Create dead worker heartbeats for both jobs
    for worker_id in [stuck_job_default_timeout.worker_id, stuck_job_custom_timeout.worker_id]:
        heartbeat = WorkerHeartbeat(
            worker_id=worker_id,
            is_active=True,
            started_at=datetime.utcnow() - timedelta(minutes=10),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
        )
        db_session.add(heartbeat)
    await db_session.commit()

    detected, recovered = await recovery_service_with_factory.recover_stuck_jobs_batch(
        batch_size=10
    )

    assert detected == 2
    assert recovered == 2

    # Verify both jobs recovered (each committed in its own transaction)
    await db_session.refresh(stuck_job_default_timeout)
    await db_session.refresh(stuck_job_custom_timeout)
    assert stuck_job_default_timeout.status == JobStatus.RETRYING
    assert stuck_job_custom_timeout.status == JobStatus.RETRYING


@pytest.mark.asyncio
async def test_recover_batch_all_jobs_recovered(
    db_session: AsyncSession,
    recovery_service_with_factory: JobRecoveryService,
    stuck_job_default_timeout: Job,
    stuck_job_custom_timeout: Job,
):
    """Test batch recovery when all jobs are legitimately timed out.

    FIX: Recovery proceeds based on job timeout, not worker heartbeat status.
    Both jobs should be recovered even if workers appear alive.
    """
    # Create dead worker for first job
    dead_heartbeat = WorkerHeartbeat(
        worker_id=stuck_job_default_timeout.worker_id,
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),  # Stale
    )
    db_session.add(dead_heartbeat)

    # Create alive worker for second job (but job still timed out)
    alive_heartbeat = WorkerHeartbeat(
        worker_id=stuck_job_custom_timeout.worker_id,
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=5),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=30),  # Fresh
    )
    db_session.add(alive_heartbeat)
    await db_session.commit()

    detected, recovered = await recovery_service_with_factory.recover_stuck_jobs_batch(
        batch_size=10
    )

    assert detected == 2
    assert recovered == 2, "Both jobs should be recovered (timeout exceeded)"

    # Verify both jobs recovered
    await db_session.refresh(stuck_job_default_timeout)
    await db_session.refresh(stuck_job_custom_timeout)
    assert stuck_job_default_timeout.status == JobStatus.RETRYING
    assert stuck_job_custom_timeout.status == JobStatus.RETRYING


@pytest.mark.asyncio
async def test_recover_batch_no_stuck_jobs(
    recovery_service_with_factory: JobRecoveryService,
):
    """Test batch recovery when no stuck jobs exist"""
    detected, recovered = await recovery_service_with_factory.recover_stuck_jobs_batch(
        batch_size=10
    )

    assert detected == 0
    assert recovered == 0


@pytest.mark.asyncio
async def test_recover_batch_requires_session_factory(recovery_service: JobRecoveryService):
    """Test batch recovery raises RuntimeError without session_factory"""
    with pytest.raises(RuntimeError, match="requires session_factory"):
        await recovery_service.recover_stuck_jobs_batch(batch_size=10)


@pytest.mark.asyncio
async def test_recover_batch_respects_batch_size(
    db_session: AsyncSession, recovery_service_with_factory: JobRecoveryService
):
    """Test batch recovery respects batch_size limit"""
    # Create 5 stuck jobs with dead workers
    for i in range(5):
        job = Job(
            id=uuid4(),
            job_type=f"stuck_task_{i}",
            payload={"data": f"test_{i}"},
            status=JobStatus.PROCESSING,
            worker_id=f"worker-batch-{i}",
            started_at=datetime.utcnow() - timedelta(seconds=320),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        db_session.add(job)

        heartbeat = WorkerHeartbeat(
            worker_id=f"worker-batch-{i}",
            is_active=True,
            started_at=datetime.utcnow() - timedelta(minutes=10),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
        )
        db_session.add(heartbeat)
    await db_session.commit()

    # Request only 3
    detected, recovered = await recovery_service_with_factory.recover_stuck_jobs_batch(batch_size=3)

    assert detected == 3
    assert recovered == 3


# ============================================================================
# Batch Transaction Isolation Tests
# ============================================================================


@pytest.mark.asyncio
async def test_batch_recovery_does_not_commit_unrelated_changes(db_session: AsyncSession):
    """Test batch recovery with session_factory cannot commit unrelated caller changes

    This proves that batch recovery with session_factory uses independent
    transactions per job and never touches the caller's session.
    """
    from tests.conftest import TestSessionLocal

    # Create stuck job
    job = Job(
        id=uuid4(),
        job_type="batch_isolation_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-batch-isolation",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-batch-isolation",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()
    job_id = job.id

    # Caller makes UNRELATED change in same session
    unrelated_job = Job(
        id=uuid4(),
        job_type="unrelated_batch_change",
        payload={"unrelated": "data"},
        status=JobStatus.PENDING,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(unrelated_job)
    unrelated_job_id = unrelated_job.id

    # Batch recovery with session_factory (creates independent session per job)
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    assert detected == 1
    assert recovered == 1

    # Verify recovery completed in its own transaction
    async with TestSessionLocal() as verify_session:
        recovered_job = await verify_session.execute(select(Job).where(Job.id == job_id))
        job_persisted = recovered_job.scalar_one()
        assert job_persisted.status == JobStatus.RETRYING

    # CRITICAL: Verify unrelated change NOT committed by batch recovery
    async with TestSessionLocal() as verify_session:
        unrelated_result = await verify_session.execute(
            select(Job).where(Job.id == unrelated_job_id)
        )
        unrelated_persisted = unrelated_result.scalar_one_or_none()
        assert unrelated_persisted is None  # NOT committed

    # Caller can still commit or rollback the unrelated change
    await db_session.rollback()

    # Verify unrelated change rolled back (recovery already committed independently)
    async with TestSessionLocal() as verify_session:
        unrelated_result = await verify_session.execute(
            select(Job).where(Job.id == unrelated_job_id)
        )
        assert unrelated_result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_batch_recovery_per_job_durability(db_session: AsyncSession):
    """Test each job recovery commits independently and is durable immediately"""
    from tests.conftest import TestSessionLocal

    # Create 3 stuck jobs
    job_ids = []
    for i in range(3):
        job = Job(
            id=uuid4(),
            job_type=f"durability_test_{i}",
            payload={"data": f"test_{i}"},
            status=JobStatus.PROCESSING,
            worker_id=f"worker-durability-{i}",
            started_at=datetime.utcnow() - timedelta(seconds=320),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        db_session.add(job)
        job_ids.append(job.id)

        heartbeat = WorkerHeartbeat(
            worker_id=f"worker-durability-{i}",
            is_active=True,
            started_at=datetime.utcnow() - timedelta(minutes=10),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
        )
        db_session.add(heartbeat)
    await db_session.commit()

    # Batch recovery with session_factory
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    assert detected == 3
    assert recovered == 3

    # Verify ALL jobs persisted (each committed independently)
    async with TestSessionLocal() as verify_session:
        for job_id in job_ids:
            result = await verify_session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one()
            assert job.status == JobStatus.RETRYING
            assert job.retry_count == 1

            # Verify outbox event created
            outbox_result = await verify_session.execute(
                select(JobOutbox).where(JobOutbox.job_id == job_id)
            )
            outbox = outbox_result.scalar_one()
            assert outbox.event_type == "job_recovered"


@pytest.mark.asyncio
async def test_batch_recovery_per_job_failure_isolation(db_session: AsyncSession):
    """Test one failing job does not prevent other jobs from being recovered"""
    from tests.conftest import TestSessionLocal

    # Create 3 stuck jobs - middle one will fail
    job_ids = []
    for i in range(3):
        job = Job(
            id=uuid4(),
            job_type=f"failure_isolation_test_{i}",
            payload={"data": f"test_{i}"},
            status=JobStatus.PROCESSING,
            worker_id=f"worker-isolation-{i}",
            started_at=datetime.utcnow() - timedelta(seconds=320),
            timeout=300,
            retry_count=0 if i != 1 else 10,  # Middle job exceeds max_retries
            max_retries=3,
        )
        db_session.add(job)
        job_ids.append(job.id)

        heartbeat = WorkerHeartbeat(
            worker_id=f"worker-isolation-{i}",
            is_active=True,
            started_at=datetime.utcnow() - timedelta(minutes=10),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
        )
        db_session.add(heartbeat)
    await db_session.commit()

    # Batch recovery with session_factory
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    assert detected == 3
    assert recovered == 3  # All 3 "recovered" (2 RETRYING, 1 DLQ)

    # Verify first and third jobs recovered, middle job moved to DLQ
    async with TestSessionLocal() as verify_session:
        # First job: RETRYING
        result = await verify_session.execute(select(Job).where(Job.id == job_ids[0]))
        job0 = result.scalar_one()
        assert job0.status == JobStatus.RETRYING

        # Middle job: DEAD_LETTER
        result = await verify_session.execute(select(Job).where(Job.id == job_ids[1]))
        job1 = result.scalar_one()
        assert job1.status == JobStatus.DEAD_LETTER

        # Third job: RETRYING
        result = await verify_session.execute(select(Job).where(Job.id == job_ids[2]))
        job2 = result.scalar_one()
        assert job2.status == JobStatus.RETRYING


@pytest.mark.asyncio
async def test_batch_recovery_failed_job_rollback(db_session: AsyncSession):
    """Test failed recovery rolls back both job changes and outbox creation"""
    from tests.conftest import TestSessionLocal
    from unittest.mock import AsyncMock, patch

    # Create stuck job
    job = Job(
        id=uuid4(),
        job_type="rollback_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-rollback",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    job_id = job.id

    heartbeat = WorkerHeartbeat(
        worker_id="worker-rollback",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # Mock flush to raise exception during recovery
    with patch("sqlalchemy.ext.asyncio.AsyncSession.flush", new_callable=AsyncMock) as mock_flush:
        mock_flush.side_effect = RuntimeError("Simulated flush failure")

        recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
        detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)

        # Recovery attempted but failed
        assert detected == 1
        assert recovered == 0

    # Verify job state unchanged (rollback succeeded)
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job_unchanged = result.scalar_one()
        assert job_unchanged.status == JobStatus.PROCESSING
        assert job_unchanged.retry_count == 0

        # Verify NO outbox event created
        outbox_result = await verify_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        assert outbox_result.scalar_one_or_none() is None


@pytest.mark.asyncio
async def test_batch_recovery_concurrent_no_double_recovery(db_session: AsyncSession):
    """Test two concurrent batch recoveries cannot recover the same job twice"""
    from tests.conftest import TestSessionLocal
    import asyncio

    # Create stuck job
    job = Job(
        id=uuid4(),
        job_type="concurrent_batch_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-concurrent-batch",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    job_id = job.id

    heartbeat = WorkerHeartbeat(
        worker_id="worker-concurrent-batch",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # Two concurrent batch recoveries
    async def batch_recovery(batch_id: int):
        async with TestSessionLocal() as session:
            service = JobRecoveryService(session, session_factory=TestSessionLocal)
            detected, recovered = await service.recover_stuck_jobs_batch(batch_size=10)
            return (batch_id, detected, recovered)

    results = await asyncio.gather(batch_recovery(1), batch_recovery(2))

    # One should succeed, one should detect but not recover (already claimed)
    total_detected = sum(r[1] for r in results)
    total_recovered = sum(r[2] for r in results)

    assert total_detected >= 1  # At least one detected
    assert total_recovered == 1  # Exactly one recovered

    # Verify job recovered exactly once
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        final_job = result.scalar_one()
        assert final_job.status == JobStatus.RETRYING
        assert final_job.retry_count == 1

        # Verify exactly one outbox event
        outbox_result = await verify_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        outbox_events = outbox_result.scalars().all()
        assert len(outbox_events) == 1


# ============================================================================
# Durable Transaction Tests
# ============================================================================


@pytest.mark.asyncio
async def test_recovery_atomic_job_and_outbox_persistence(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that job state and outbox event are persisted atomically in single transaction

    Uses independent PostgreSQL session to verify changes are only visible after commit.
    """
    # Create stuck job with dead worker
    job = Job(
        id=uuid4(),
        job_type="atomic_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-atomic",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-atomic",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()
    job_id = job.id

    # Recover job (flushes but does NOT commit)
    success = await recovery_service.recover_stuck_job(job_id)
    assert success is True

    # Use independent session to verify changes are NOT visible before commit
    # Import TestSessionLocal for test database
    from tests.conftest import TestSessionLocal

    async with TestSessionLocal() as independent_session:
        job_result = await independent_session.execute(select(Job).where(Job.id == job_id))
        independent_job = job_result.scalar_one()

        # Changes not visible in independent session before commit
        assert independent_job.status == JobStatus.PROCESSING
        assert independent_job.retry_count == 0

        # Outbox event not visible in independent session before commit
        outbox_result = await independent_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        independent_outbox = outbox_result.scalar_one_or_none()
        assert independent_outbox is None

    # Commit the recovery transaction (caller responsibility)
    await db_session.commit()

    # Now verify changes are visible in independent session after commit
    from tests.conftest import TestSessionLocal

    async with TestSessionLocal() as independent_session:
        job_result = await independent_session.execute(select(Job).where(Job.id == job_id))
        persisted_job = job_result.scalar_one()

        assert persisted_job.status == JobStatus.RETRYING
        assert persisted_job.retry_count == 1
        assert persisted_job.next_retry_at is not None

        # Verify outbox event persisted atomically with job
        outbox_result = await independent_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        persisted_outbox = outbox_result.scalar_one()

        assert persisted_outbox.event_type == "job_recovered"
        assert persisted_outbox.published is False


@pytest.mark.asyncio
async def test_recovery_rollback_on_database_error(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that recovery rolls back cleanly on database error"""
    # Create stuck job
    job = Job(
        id=uuid4(),
        job_type="rollback_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-rollback",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-rollback",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # Mock flush to raise exception
    original_flush = recovery_service.db.flush

    async def mock_flush_error():
        raise RuntimeError("Simulated database flush failure")

    recovery_service.db.flush = mock_flush_error

    # Attempt recovery - should raise exception
    try:
        await recovery_service.recover_stuck_job(job.id)
        assert False, "Expected exception not raised"
    except RuntimeError as e:
        assert "Simulated database flush failure" in str(e)
    finally:
        recovery_service.db.flush = original_flush

    # Caller rolls back the transaction
    await db_session.rollback()

    # Verify job state unchanged (rollback succeeded)
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 0
    assert job.next_retry_at is None

    # Verify no outbox event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    outbox_event = result.scalar_one_or_none()
    assert outbox_event is None


@pytest.mark.asyncio
async def test_recovery_cannot_commit_unrelated_changes(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that recovery service cannot commit unrelated caller changes

    This is the KEY test proving transaction ownership is fixed.
    Recovery service only flushes changes within the transaction.
    Caller controls commit/rollback boundary.
    """
    # Setup: Create stuck job with dead worker
    job = Job(
        id=uuid4(),
        job_type="transaction_isolation_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-isolation",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-isolation",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()
    job_id = job.id

    # Caller makes UNRELATED change in same transaction (simulates real-world scenario)
    unrelated_job = Job(
        id=uuid4(),
        job_type="unrelated_change",
        payload={"unrelated": "data"},
        status=JobStatus.PENDING,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(unrelated_job)
    unrelated_job_id = unrelated_job.id

    # Recovery service performs recovery (flushes but does NOT commit)
    success = await recovery_service.recover_stuck_job(job_id)
    assert success is True

    # CRITICAL TEST: Verify unrelated change is NOT visible to independent session
    # If recovery service committed, unrelated_job would be visible
    from tests.conftest import TestSessionLocal

    async with TestSessionLocal() as independent_session:
        # Recovery changes not visible (not committed)
        recovered_job_result = await independent_session.execute(
            select(Job).where(Job.id == job_id)
        )
        recovered_job = recovered_job_result.scalar_one()
        assert recovered_job.status == JobStatus.PROCESSING  # NOT RETRYING yet
        assert recovered_job.retry_count == 0  # NOT incremented yet

        # Unrelated change not visible (not committed)
        unrelated_result = await independent_session.execute(
            select(Job).where(Job.id == unrelated_job_id)
        )
        unrelated_in_independent = unrelated_result.scalar_one_or_none()
        assert unrelated_in_independent is None  # NOT committed by recovery service

    # Caller decides to ROLLBACK instead of commit (simulating business logic failure)
    await db_session.rollback()

    # Verify BOTH recovery changes AND unrelated changes are rolled back
    async with TestSessionLocal() as independent_session:
        # Recovery changes rolled back
        recovered_job_result = await independent_session.execute(
            select(Job).where(Job.id == job_id)
        )
        recovered_job = recovered_job_result.scalar_one()
        assert recovered_job.status == JobStatus.PROCESSING
        assert recovered_job.retry_count == 0

        # Unrelated change rolled back
        unrelated_result = await independent_session.execute(
            select(Job).where(Job.id == unrelated_job_id)
        )
        unrelated_in_independent = unrelated_result.scalar_one_or_none()
        assert unrelated_in_independent is None

        # No outbox event persisted
        outbox_result = await independent_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        outbox_event = outbox_result.scalar_one_or_none()
        assert outbox_event is None

    # Success case: Caller commits both recovery and unrelated changes together
    # Re-do recovery in new transaction
    async with TestSessionLocal() as commit_session:
        # Create unrelated change again
        unrelated_job2 = Job(
            id=uuid4(),
            job_type="unrelated_change_2",
            payload={"unrelated": "data"},
            status=JobStatus.PENDING,
            retry_count=0,
            max_retries=3,
        )
        commit_session.add(unrelated_job2)
        unrelated_job2_id = unrelated_job2.id

        # Perform recovery
        recovery2 = JobRecoveryService(commit_session)
        success2 = await recovery2.recover_stuck_job(job_id)
        assert success2 is True

        # Caller commits BOTH recovery and unrelated changes
        await commit_session.commit()

    # Verify BOTH changes persisted after caller commit
    async with TestSessionLocal() as verify_session:
        # Recovery changes persisted
        recovered_job_result = await verify_session.execute(select(Job).where(Job.id == job_id))
        recovered_job = recovered_job_result.scalar_one()
        assert recovered_job.status == JobStatus.RETRYING
        assert recovered_job.retry_count == 1

        # Unrelated change persisted
        unrelated_result = await verify_session.execute(
            select(Job).where(Job.id == unrelated_job2_id)
        )
        unrelated_persisted = unrelated_result.scalar_one()
        assert unrelated_persisted.job_type == "unrelated_change_2"

        # Outbox event persisted
        outbox_result = await verify_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        outbox_event = outbox_result.scalar_one()
        assert outbox_event.event_type == "job_recovered"


@pytest.mark.asyncio
async def test_recovery_concurrent_attempts_one_winner(db_session: AsyncSession):
    """Test concurrent recovery attempts with real PostgreSQL transactions"""
    from tests.conftest import TestSessionLocal

    # Use unique IDs to avoid conflicts with other tests
    job_id = uuid4()
    worker_id = f"worker-concurrent-{uuid4()}"

    # Create stuck job using the test database session
    job = Job(
        id=job_id,
        job_type="concurrent_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id=worker_id,
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # Create two separate recovery services with separate sessions
    async def attempt_recovery(attempt_id: int):
        async with TestSessionLocal() as session:
            service = JobRecoveryService(session)
            try:
                result = await service.recover_stuck_job(job_id)
                if result:
                    # Commit the recovery (each concurrent attempt tries to commit)
                    await session.commit()
                return (attempt_id, result)
            except Exception:
                await session.rollback()
                return (attempt_id, False)

    # Run both recoveries concurrently
    import asyncio

    results = await asyncio.gather(attempt_recovery(1), attempt_recovery(2))

    # Verify exactly one succeeded
    successes = [r for r in results if r[1] is True]
    failures = [r for r in results if r[1] is False]

    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}. Results: {results}"
    assert len(failures) == 1, f"Expected 1 failure, got {len(failures)}. Results: {results}"

    # Verify job state (only one recovery applied)
    async with TestSessionLocal() as check_session:
        job_result = await check_session.execute(select(Job).where(Job.id == job_id))
        final_job = job_result.scalar_one()

        assert final_job.status == JobStatus.RETRYING
        assert final_job.retry_count == 1

        # Verify exactly one outbox event created
        outbox_result = await check_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        outbox_events = outbox_result.scalars().all()
        assert len(outbox_events) == 1


@pytest.mark.asyncio
async def test_recovery_retry_count_correctness(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test retry_count incremented correctly for multiple recoveries"""
    # Create job with retry_count=1 (already retried once)
    job = Job(
        id=uuid4(),
        job_type="retry_count_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-retry-count",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=1,  # Already at retry attempt 1
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-retry-count",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # Recover job
    success = await recovery_service.recover_stuck_job(job.id)
    assert success is True

    # Commit the recovery transaction (caller responsibility)
    await db_session.commit()

    # Verify retry_count incremented from 1 to 2
    await db_session.refresh(job)
    assert job.retry_count == 2

    # Verify exponential backoff for retry_count=2
    # delay = INITIAL_DELAY * (BACKOFF_BASE ** retry_count) = 1 * (2 ** 2) = 4 seconds
    expected_delay = settings.INITIAL_RETRY_DELAY * (settings.RETRY_BACKOFF_BASE**2)
    assert expected_delay == 4

    time_until_retry = (job.next_retry_at - datetime.utcnow()).total_seconds()
    assert abs(time_until_retry - expected_delay) < 5


@pytest.mark.asyncio
async def test_recovery_next_retry_at_correctness(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test next_retry_at calculated correctly with exponential backoff"""
    # Test cases: retry_count -> expected delay
    test_cases = [
        (0, 2),  # 1 * 2^1 = 2 seconds
        (1, 4),  # 1 * 2^2 = 4 seconds
        (2, 8),  # 1 * 2^3 = 8 seconds
    ]

    for initial_retry_count, expected_delay in test_cases:
        job = Job(
            id=uuid4(),
            job_type=f"backoff_test_{initial_retry_count}",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id=f"worker-backoff-{initial_retry_count}",
            started_at=datetime.utcnow() - timedelta(seconds=320),
            timeout=300,
            retry_count=initial_retry_count,
            max_retries=3,
        )
        db_session.add(job)

        heartbeat = WorkerHeartbeat(
            worker_id=f"worker-backoff-{initial_retry_count}",
            is_active=True,
            started_at=datetime.utcnow() - timedelta(minutes=10),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
        )
        db_session.add(heartbeat)
    await db_session.commit()

    # Recover all jobs
    for initial_retry_count, expected_delay in test_cases:
        job_stmt = select(Job).where(Job.job_type == f"backoff_test_{initial_retry_count}")
        result = await db_session.execute(job_stmt)
        job = result.scalar_one()

        success = await recovery_service.recover_stuck_job(job.id)
        assert success is True

        # Commit each recovery transaction
        await db_session.commit()

        await db_session.refresh(job)
        time_until_retry = (job.next_retry_at - datetime.utcnow()).total_seconds()
        assert abs(time_until_retry - expected_delay) < 5, (
            f"retry_count={initial_retry_count}: expected delay {expected_delay}s, "
            f"got {time_until_retry:.1f}s"
        )


@pytest.mark.asyncio
async def test_recovery_no_duplicate_outbox_events(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test that recovery creates exactly one outbox event per job"""
    # Create stuck job
    job = Job(
        id=uuid4(),
        job_type="no_duplicate_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-no-dup",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-no-dup",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # First recovery
    success = await recovery_service.recover_stuck_job(job.id)
    assert success is True

    # Commit the recovery transaction
    await db_session.commit()

    # Verify one outbox event
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    outbox_events = result.scalars().all()
    assert len(outbox_events) == 1

    # Attempt second recovery (should fail - status no longer PROCESSING)
    success2 = await recovery_service.recover_stuck_job(job.id)
    assert success2 is False

    # Verify still only one outbox event
    result2 = await db_session.execute(stmt)
    outbox_events2 = result2.scalars().all()
    assert len(outbox_events2) == 1


# ============================================================================
# Retry Attempt Metadata Tests
# ============================================================================


@pytest.mark.asyncio
async def test_recovery_event_stores_immutable_retry_attempt(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test recovery event stores retry_attempt matching job's retry_count"""
    # Create stuck job at retry_count=0
    job = Job(
        id=uuid4(),
        job_type="retry_attempt_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-retry-attempt",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-retry-attempt",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    # Recover job
    success = await recovery_service.recover_stuck_job(job.id)
    assert success is True

    # Commit the recovery transaction (caller responsibility)
    await db_session.commit()

    # Verify job transitioned to retry_count=1
    await db_session.refresh(job)
    assert job.retry_count == 1

    # Verify outbox event has matching retry_attempt
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    outbox_event = result.scalar_one()

    assert outbox_event.event_type == "job_recovered"
    assert outbox_event.retry_attempt == 1  # Matches job.retry_count
    assert outbox_event.published is False


@pytest.mark.asyncio
async def test_recovery_successive_attempts_distinct_retry_attempt(db_session: AsyncSession):
    """Test two successive recoveries create events with distinct retry_attempt values"""

    job_id = uuid4()
    worker_id = f"worker-successive-{uuid4()}"

    # First recovery: retry_count 0 -> 1
    job = Job(
        id=job_id,
        job_type="successive_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id=worker_id,
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id=worker_id,
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()

    recovery1 = JobRecoveryService(db_session)
    success1 = await recovery1.recover_stuck_job(job_id)
    assert success1 is True

    # Commit the recovery transaction
    await db_session.commit()

    # Simulate job processed and stuck again
    # Manually reset job to PROCESSING (simulating it got stuck again)
    stmt = select(Job).where(Job.id == job_id)
    result = await db_session.execute(stmt)
    job = result.scalar_one()

    job.status = JobStatus.PROCESSING
    job.started_at = datetime.utcnow() - timedelta(seconds=320)
    await db_session.commit()

    # Second recovery: retry_count 1 -> 2
    recovery2 = JobRecoveryService(db_session)
    success2 = await recovery2.recover_stuck_job(job_id)
    assert success2 is True

    # Commit the recovery transaction
    await db_session.commit()

    # Verify two distinct outbox events with different retry_attempt
    stmt = select(JobOutbox).where(JobOutbox.job_id == job_id).order_by(JobOutbox.created_at)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 2

    # First event: retry_attempt=1
    assert events[0].event_type == "job_recovered"
    assert events[0].retry_attempt == 1

    # Second event: retry_attempt=2
    assert events[1].event_type == "job_recovered"
    assert events[1].retry_attempt == 2

    # Verify job is at retry_count=2
    job_result = await db_session.execute(select(Job).where(Job.id == job_id))
    final_job = job_result.scalar_one()
    assert final_job.retry_count == 2


@pytest.mark.asyncio
async def test_job_created_event_allows_null_retry_attempt(db_session: AsyncSession):
    """Test job_created events work with NULL retry_attempt"""
    from app.services.job_service import JobService
    from app.services.queue_service import QueueService
    from app.core.redis_client import RedisClient
    from app.models.job import JobCreate
    from unittest.mock import MagicMock

    # Create job with job_created event (normal job creation)
    mock_redis = MagicMock(spec=RedisClient)
    mock_queue = MagicMock(spec=QueueService)
    job_service = JobService(db_session, mock_redis, mock_queue)

    job_data = JobCreate(job_type="test_task", payload={"data": "test"}, priority=5)

    job = await job_service.create_job(job_data)
    await db_session.commit()

    # Verify job_created event has NULL retry_attempt
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    outbox_event = result.scalar_one()

    assert outbox_event.event_type == "job_created"
    assert outbox_event.retry_attempt is None  # NULL for job_created events


@pytest.mark.asyncio
async def test_recovery_rollback_does_not_persist_event(
    db_session: AsyncSession, recovery_service: JobRecoveryService
):
    """Test failed recovery transaction doesn't persist job or outbox event"""
    # Create stuck job
    job = Job(
        id=uuid4(),
        job_type="rollback_event_test",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-rollback-event",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        timeout=300,
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    heartbeat = WorkerHeartbeat(
        worker_id="worker-rollback-event",
        is_active=True,
        started_at=datetime.utcnow() - timedelta(minutes=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=120),
    )
    db_session.add(heartbeat)
    await db_session.commit()
    job_id = job.id

    # Mock flush to raise exception (simulating database error during flush)
    original_flush = recovery_service.db.flush

    async def mock_flush_error():
        raise RuntimeError("Simulated database flush failure")

    recovery_service.db.flush = mock_flush_error

    # Attempt recovery - should raise exception
    try:
        await recovery_service.recover_stuck_job(job_id)
        assert False, "Expected exception not raised"
    except RuntimeError as e:
        assert "Simulated database flush failure" in str(e)
    finally:
        recovery_service.db.flush = original_flush

    # Rollback the transaction (caller responsibility)
    await db_session.rollback()

    # Verify job state unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 0

    # Verify no outbox event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job_id)
    result = await db_session.execute(stmt)
    outbox_event = result.scalar_one_or_none()
    assert outbox_event is None


@pytest.mark.asyncio
async def test_recovery_schema_matches_model(db_session: AsyncSession):
    """Test PostgreSQL schema matches SQLAlchemy model for retry_attempt"""
    from sqlalchemy import text

    # Query actual PostgreSQL schema
    result = await db_session.execute(
        text(
            """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'job_outbox'
        AND column_name = 'retry_attempt'
    """
        )
    )
    row = result.fetchone()

    # Verify column exists in database
    assert row is not None, "retry_attempt column not found in database"
    assert row[0] == "retry_attempt"
    assert row[1] == "integer"
    assert row[2] == "YES"  # Nullable

    # Verify index exists
    index_result = await db_session.execute(
        text(
            """
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = 'job_outbox'
        AND indexname = 'idx_outbox_job_retry'
    """
        )
    )
    index_row = index_result.fetchone()
    assert index_row is not None, "idx_outbox_job_retry index not found"
    assert index_row[0] == "idx_outbox_job_retry"
