"""
Tests for Recovery Eligibility Verification After Lock Acquisition

Verifies that recover_stuck_job() independently verifies timeout eligibility
after acquiring the PostgreSQL row lock, not relying solely on detection results.

Critical vulnerability: Between detection and recovery, job state can change:
- started_at refreshed (worker reclaimed job)
- timeout increased (admin intervention)
- Job still processing normally (false positive in detection)

These tests verify that recovery MUST recheck eligibility using the locked row
before transitioning to RETRYING and creating outbox events.
"""

import pytest
from datetime import datetime, timedelta

from sqlalchemy import select

from app.db.models import Job, JobStatus, JobOutbox, WorkerHeartbeat
from app.services.job_recovery_service import JobRecoveryService
from tests.conftest import TestSessionLocal


@pytest.mark.asyncio
async def test_direct_recovery_call_for_healthy_job_rejects(db_session):
    """
    Test 1: Direct recovery call for a healthy PROCESSING job that has NOT timed out.

    CRITICAL: If someone calls recover_stuck_job() directly with a healthy job ID,
    recovery MUST reject it. Cannot assume detection already validated timeout.
    """
    # Create a healthy PROCESSING job (started 30 seconds ago, timeout=300s)
    job = Job(
        job_type="healthy_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-healthy",
        started_at=datetime.utcnow() - timedelta(seconds=30),  # Fresh
        retry_count=0,
        max_retries=3,
        timeout=300,  # 5 minutes
    )
    db_session.add(job)

    worker = WorkerHeartbeat(
        worker_id="worker-healthy",
        started_at=datetime.utcnow() - timedelta(seconds=30),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=10),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Direct recovery call WITHOUT detection
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    recovered = await recovery_service.recover_stuck_job(job_id)

    # CRITICAL: Must reject healthy job
    assert not recovered, "Recovery must reject job that has not timed out"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING, "Job status should be unchanged"
    assert job.retry_count == 0, "Retry count should be unchanged"

    # Verify no outbox event created
    result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
    outbox = result.scalar_one_or_none()
    assert outbox is None, "No outbox event should be created for rejected recovery"


@pytest.mark.asyncio
async def test_direct_recovery_call_for_timed_out_job_succeeds(db_session):
    """
    Test 2: Direct recovery call for a genuinely timed-out PROCESSING job.

    Verifies that recovery accepts legitimately timed-out jobs.
    """
    # Create a timed-out job (started 320s ago, timeout=300s, buffer=10s)
    job = Job(
        job_type="timed_out_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-timeout",
        started_at=datetime.utcnow() - timedelta(seconds=320),  # Timed out
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    worker = WorkerHeartbeat(
        worker_id="worker-timeout",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=70),  # Stale
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Direct recovery call
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    recovered = await recovery_service.recover_stuck_job(job_id)

    # Should succeed
    assert recovered, "Recovery should succeed for legitimately timed-out job"

    # Verify job transitioned
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING, "Job should be RETRYING"
    assert job.retry_count == 1, "Retry count should be incremented"

    # Verify outbox event created
    result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
    outbox = result.scalar_one_or_none()
    assert outbox is not None, "Outbox event should be created"
    assert outbox.published is False


@pytest.mark.asyncio
async def test_job_started_at_refreshed_before_recovery(db_session):
    """
    Test 3: Job detected as stale, then started_at refreshed before recovery acquires lock.

    Scenario:
    1. Job started 320s ago (timed out)
    2. Detection marks it as stuck
    3. Worker reclaims job, updates started_at to NOW
    4. Recovery attempts to lock job

    CRITICAL: Recovery must recheck timeout using the NEW started_at value.
    """
    # Create job that appears timed out
    job = Job(
        job_type="refreshed_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-refresh",
        started_at=datetime.utcnow() - timedelta(seconds=320),  # Initially timed out
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Detection would mark this as stuck
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs, "Job should be detected as stuck initially"

    # BEFORE recovery, worker refreshes started_at
    job.started_at = datetime.utcnow() - timedelta(seconds=30)  # Fresh now
    await db_session.commit()

    # Recovery attempts to process
    recovered = await recovery_service.recover_stuck_job(job_id)

    # CRITICAL: Must reject because job is no longer timed out
    assert not recovered, "Recovery must reject job with refreshed started_at"

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING, "Job should remain PROCESSING"
    assert job.retry_count == 0, "Retry count unchanged"

    # Verify no outbox event
    result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
    outbox = result.scalar_one_or_none()
    assert outbox is None, "No outbox event for rejected recovery"


@pytest.mark.asyncio
async def test_job_timeout_increased_before_recovery(db_session):
    """
    Test 4: Job detected as stale, then timeout increased before recovery.

    Scenario:
    1. Job started 320s ago with timeout=300s (timed out)
    2. Detection marks it as stuck
    3. Admin increases timeout to 600s (job now healthy)
    4. Recovery attempts to process

    CRITICAL: Recovery must recheck using the NEW timeout value.
    """
    # Create job that initially timed out
    job = Job(
        job_type="timeout_increased",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-timeout-change",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        retry_count=0,
        max_retries=3,
        timeout=300,  # Initially 5 minutes
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Detection marks as stuck
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs

    # Admin increases timeout
    job.timeout = 600  # Now 10 minutes (job is healthy)
    await db_session.commit()

    # Recovery attempts to process
    recovered = await recovery_service.recover_stuck_job(job_id)

    # CRITICAL: Must reject with new timeout
    assert not recovered, "Recovery must reject job with increased timeout"

    # Verify unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.retry_count == 0

    # Verify no outbox
    result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
    outbox = result.scalar_one_or_none()
    assert outbox is None


@pytest.mark.asyncio
async def test_job_completed_before_recovery(db_session):
    """
    Test 5: Job changes to COMPLETED before recovery acquires lock.
    """
    # Create timed-out job
    job = Job(
        job_type="completed_before_recovery",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-complete",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Detection marks as stuck
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs

    # Worker completes job
    job.status = JobStatus.COMPLETED
    job.completed_at = datetime.utcnow()
    await db_session.commit()

    # Recovery attempts
    recovered = await recovery_service.recover_stuck_job(job_id)

    # Should skip (SELECT FOR UPDATE SKIP LOCKED won't match)
    assert not recovered, "Recovery should skip COMPLETED job"

    # Verify unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_job_already_retrying_before_recovery(db_session):
    """
    Test 6: Job changes to RETRYING before recovery (another recovery won).
    """
    # Create timed-out job
    job = Job(
        job_type="already_retrying",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-retry",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Another recovery transitions to RETRYING
    job.status = JobStatus.RETRYING
    job.retry_count = 1
    await db_session.commit()

    # This recovery attempts
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    recovered = await recovery_service.recover_stuck_job(job_id)

    # Should skip (SELECT FOR UPDATE won't match status=PROCESSING)
    assert not recovered

    # Verify unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.retry_count == 1


@pytest.mark.asyncio
async def test_concurrent_recovery_only_one_succeeds(db_session):
    """
    Test 7: Two recovery runners compete for the same timed-out job.

    Verifies SELECT FOR UPDATE SKIP LOCKED ensures only one recovery wins.
    """
    # Create timed-out job
    job = Job(
        job_type="concurrent_recovery",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-concurrent",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Two concurrent recovery attempts
    async def attempt_recovery():
        async with TestSessionLocal() as session:
            recovery_service = JobRecoveryService(session, session_factory=TestSessionLocal)
            success = await recovery_service.recover_stuck_job(job_id)
            if success:
                await session.commit()
                return True
            return False

    import asyncio

    results = await asyncio.gather(attempt_recovery(), attempt_recovery(), return_exceptions=True)

    # Only one should succeed
    successes = sum(1 for r in results if r is True)
    assert successes == 1, f"Expected exactly 1 success, got {successes}"

    # Verify job recovered
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.retry_count == 1


@pytest.mark.asyncio
async def test_fresh_heartbeat_does_not_block_timed_out_job(db_session):
    """
    Test 8: Fresh worker heartbeat does not block recovery of genuinely timed-out job.

    This is the Step 286 fix - heartbeat should NOT block recovery.
    """
    # Create timed-out job with fresh heartbeat
    job = Job(
        job_type="heartbeat_no_block",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-heartbeat",
        started_at=datetime.utcnow() - timedelta(seconds=320),  # Timed out
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Fresh heartbeat
    worker = WorkerHeartbeat(
        worker_id="worker-heartbeat",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=25),  # Fresh
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Recovery should succeed
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    recovered = await recovery_service.recover_stuck_job(job_id)

    assert recovered, "Recovery should succeed despite fresh heartbeat"

    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING


@pytest.mark.asyncio
async def test_rejected_recovery_no_side_effects(db_session):
    """
    Test 9: Rejected recovery creates no outbox event and does not increment retry_count.
    """
    # Create healthy job
    job = Job(
        job_type="rejected_no_effects",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-no-effects",
        started_at=datetime.utcnow() - timedelta(seconds=30),  # Healthy
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Recovery rejects
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    recovered = await recovery_service.recover_stuck_job(job_id)

    assert not recovered

    # Verify NO side effects
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING, "Status unchanged"
    assert job.retry_count == 0, "Retry count unchanged"
    assert job.next_retry_at is None, "next_retry_at unchanged"
    assert job.error_type is None, "error_type unchanged"
    assert job.error_message is None, "error_message unchanged"

    # Verify no outbox event
    result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
    outbox = result.scalar_one_or_none()
    assert outbox is None, "No outbox event created"


@pytest.mark.asyncio
async def test_successful_recovery_creates_exactly_one_outbox_event(db_session):
    """
    Test 10: Recovery of eligible job creates exactly one durable outbox event.
    """
    # Create timed-out job
    job = Job(
        job_type="successful_recovery",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-success",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Recovery succeeds
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    recovered = await recovery_service.recover_stuck_job(job_id)

    assert recovered
    await db_session.commit()

    # Verify job updated
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.retry_count == 1

    # Verify exactly one outbox event
    result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
    outbox_events = result.scalars().all()
    assert len(outbox_events) == 1, "Exactly one outbox event"
    assert outbox_events[0].retry_attempt == 1
    assert outbox_events[0].published is False
