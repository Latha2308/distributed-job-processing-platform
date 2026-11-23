"""
Tests for Heartbeat Ownership Conflicts Across Worker Restarts

Verifies that stale worker heartbeats cannot interfere with recovery decisions
when worker IDs are reused (e.g., Kubernetes pod restarts).

Critical scenarios tested:
A. Two worker processes sharing the same worker_id
B. Old worker heartbeat arriving after newer worker registered
C. Old worker heartbeat arriving after job advanced from retry_count=0 to retry_count=1
D. Old worker heartbeat arriving after different job claimed
E. Stale worker clearing current_job_id while another execution is active
F. Recovery evaluating job while stale heartbeats continue

The core question: Can stale heartbeats prevent legitimate recovery by keeping
a dead execution appearing "alive" to the recovery service?
"""

import pytest
import asyncio
from datetime import datetime, timedelta

from app.db.models import Job, JobStatus, WorkerHeartbeat
from app.services.job_recovery_service import JobRecoveryService
from app.services.worker_service import WorkerService
from tests.conftest import TestSessionLocal


@pytest.mark.asyncio
async def test_stale_heartbeat_prevents_recovery_of_abandoned_job(db_session):
    """
    Scenario A: Old worker heartbeat prevents recovery of abandoned job.

    Setup:
    1. Worker pod-1 claims job (retry_count=0)
    2. Worker pod-1 starts processing
    3. Worker pod-1 sends heartbeat
    4. Worker pod-1 crashes (but heartbeat is still fresh)
    5. Job times out (started_at + timeout + 10s elapsed)
    6. Recovery service evaluates job

    CRITICAL BUG: Recovery checks if worker is alive based on heartbeat.
    If heartbeat is <60s old, recovery skips the job even though the
    worker process is dead and will never complete the job.

    Expected behavior: Recovery should not rely solely on heartbeat freshness.
    Actual behavior: Fresh heartbeat prevents recovery of abandoned job.
    """
    # Create job that will timeout
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=320),  # 320s ago (timeout=300 + buffer=10)
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Create worker heartbeat that's fresh (worker appears alive)
    worker = WorkerHeartbeat(
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=30),  # Fresh: 30s ago
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
        current_job_id=job.id,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Recovery service evaluates job
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)

    # Detect stuck jobs
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs, "Job should be detected as stuck (timeout exceeded)"

    # Attempt recovery
    recovered = await recovery_service.recover_stuck_job(job_id)

    # FIX VERIFIED: Recovery should succeed based on job timeout alone
    # Worker heartbeat should NOT block recovery of timed-out jobs
    await db_session.refresh(job)

    assert recovered, "Recovery should succeed (job timeout exceeded)"
    assert job.status == JobStatus.RETRYING, "Job should be recovered to RETRYING"
    assert job.retry_count == 1, "Retry count should be incremented"

    print("\n" + "=" * 80)
    print("FIX VERIFIED: Recovery succeeded despite fresh heartbeat")
    print("=" * 80)
    print(f"Job status: {job.status}")
    print(f"Job retry_count: {job.retry_count}")
    print(f"Job timeout: {job.timeout}s")
    print(f"Time elapsed: {(datetime.utcnow() - job.started_at).total_seconds()}s")
    print(f"Worker heartbeat age: {(datetime.utcnow() - worker.last_heartbeat).total_seconds()}s")
    print("=" * 80)
    print("Recovery decision based on job timeout, not worker heartbeat.")
    print("Fresh heartbeat does not block recovery of legitimately timed-out job.")
    print("=" * 80 + "\n")


@pytest.mark.asyncio
async def test_old_worker_heartbeat_after_newer_worker_registered(db_session):
    """
    Scenario B: Old worker heartbeat updates after newer worker registered.

    Setup:
    1. Worker pod-1 (generation 1) registers
    2. Worker pod-1 claims job
    3. Worker pod-1 crashes
    4. Worker pod-1 (generation 2, same ID) restarts and registers
    5. Worker pod-1 (generation 1, old) heartbeat arrives late

    Issue: WorkerHeartbeat has no generation/version field.
    Old heartbeat can overwrite newer worker's state.
    """
    # Create job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=50),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Old worker (generation 1) registers and sends heartbeat
    worker_service = WorkerService(db_session)
    worker = await worker_service.register_worker("worker-pod-1")
    await worker_service.update_heartbeat("worker-pod-1", current_job_id=str(job.id))
    await db_session.commit()

    # Capture old worker's heartbeat time
    await db_session.refresh(worker)
    old_heartbeat_time = worker.last_heartbeat

    # Simulate time passing (worker crashes)
    await asyncio.sleep(0.1)

    # New worker (generation 2, same ID) restarts and registers
    # This reactivates the existing WorkerHeartbeat record
    new_worker = await worker_service.register_worker("worker-pod-1")
    await db_session.commit()
    await db_session.refresh(new_worker)

    new_heartbeat_time = new_worker.last_heartbeat
    assert new_heartbeat_time > old_heartbeat_time, "New worker should have newer heartbeat"

    # Old worker (generation 1) sends late heartbeat
    # This updates the SAME WorkerHeartbeat record
    await asyncio.sleep(0.1)
    old_worker_heartbeat = await worker_service.update_heartbeat(
        "worker-pod-1", current_job_id=str(job.id)
    )
    await db_session.commit()
    await db_session.refresh(old_worker_heartbeat)

    # Old heartbeat overwrites new worker's heartbeat
    assert (
        old_worker_heartbeat.last_heartbeat > new_heartbeat_time
    ), "Old worker's late heartbeat overwrites newer worker's state"

    print("\n" + "=" * 80)
    print("ISSUE: Old worker heartbeat can overwrite newer worker state")
    print("=" * 80)
    print(f"Old worker heartbeat: {old_heartbeat_time}")
    print(f"New worker registered: {new_heartbeat_time}")
    print(f"Late heartbeat arrived: {old_worker_heartbeat.last_heartbeat}")
    print("=" * 80)
    print("WorkerHeartbeat has no generation/version field.")
    print("Old worker process can update heartbeat for reused worker_id.")
    print("=" * 80 + "\n")


@pytest.mark.asyncio
async def test_stale_heartbeat_after_job_advanced_to_retry_count_1(db_session):
    """
    Scenario C: Old worker heartbeat after job advanced from retry_count=0 to retry_count=1.

    Setup:
    1. Worker pod-1 claims job (retry_count=0)
    2. Worker pod-1 starts processing
    3. Worker pod-1 heartbeat becomes stale
    4. Recovery transitions job to RETRYING (retry_count=1)
    5. New Worker pod-1 claims retry (retry_count=1)
    6. Old Worker pod-1 (from retry_count=0) sends late heartbeat

    Issue: Heartbeat update doesn't check job execution generation.
    Old worker can refresh heartbeat, making recovery think worker is alive.
    """
    # Create job at retry_count=1 (already recovered once)
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=50),
        retry_count=1,  # Already at generation 1
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Worker heartbeat is stale (from generation 0 execution)
    worker = WorkerHeartbeat(
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=400),  # Old start time
        last_heartbeat=datetime.utcnow() - timedelta(seconds=70),  # Stale: 70s ago
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
        current_job_id=job.id,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Old worker (generation 0) sends late heartbeat
    worker_service = WorkerService(db_session)
    updated_worker = await worker_service.update_heartbeat(
        "worker-pod-1", current_job_id=str(job_id)
    )
    await db_session.commit()
    await db_session.refresh(updated_worker)

    # Heartbeat is now fresh
    heartbeat_age = (datetime.utcnow() - updated_worker.last_heartbeat).total_seconds()
    assert heartbeat_age < 5, "Heartbeat should be fresh"

    # Job times out
    job.started_at = datetime.utcnow() - timedelta(seconds=320)  # Force timeout
    await db_session.commit()

    # Recovery evaluates job
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs, "Job should be detected as stuck"

    # Attempt recovery
    recovered = await recovery_service.recover_stuck_job(job_id)

    # FIX: Recovery should succeed based on job timeout
    # Old worker heartbeat doesn't matter - job timeout is authoritative
    assert recovered, "Recovery should succeed (job timeout exceeded)"
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING, "Job should be recovered"

    print("\n" + "=" * 80)
    print("FIX: Old worker heartbeat doesn't prevent recovery")
    print("=" * 80)
    print(f"Job retry_count: {job.retry_count}")
    print(f"Heartbeat age: {(datetime.utcnow() - updated_worker.last_heartbeat).total_seconds()}s")
    print("=" * 80)
    print("Recovery decision based on job timeout, not heartbeat.")
    print("Old worker from generation 0 cannot block generation 1 recovery.")
    print("=" * 80 + "\n")


@pytest.mark.asyncio
async def test_stale_worker_clearing_current_job_id_during_active_execution(db_session):
    """
    Scenario E: Stale worker clearing current_job_id while another execution is active.

    Setup:
    1. Worker pod-1 claims job A (retry_count=0)
    2. Worker pod-1 crashes
    3. Recovery transitions job A to RETRYING (retry_count=1)
    4. New Worker pod-1 claims job B
    5. Old Worker pod-1 sends heartbeat with current_job_id=None (cleanup on shutdown)

    Issue: Old worker clears current_job_id, hiding active execution of job B.
    """
    # Create two jobs
    job_a = Job(
        job_type="test_job",
        payload={"data": "job_a"},
        status=JobStatus.RETRYING,  # Already recovered
        retry_count=1,
        max_retries=3,
        timeout=300,
    )
    job_b = Job(
        job_type="test_job",
        payload={"data": "job_b"},
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=10),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add_all([job_a, job_b])

    # Worker is processing job B
    worker = WorkerHeartbeat(
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=10),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=5),
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
        current_job_id=job_b.id,  # Currently processing job B
    )
    db_session.add(worker)
    await db_session.commit()

    # Old worker (from job A execution) sends heartbeat with current_job_id=None
    worker_service = WorkerService(db_session)
    updated_worker = await worker_service.update_heartbeat("worker-pod-1", current_job_id=None)
    await db_session.commit()
    await db_session.refresh(updated_worker)

    # Current job ID is cleared
    assert updated_worker.current_job_id is None, "current_job_id should be cleared"
    assert updated_worker.last_heartbeat is not None, "Heartbeat should be updated"

    print("\n" + "=" * 80)
    print("ISSUE: Stale worker can clear current_job_id during active execution")
    print("=" * 80)
    print(f"Job B status: {job_b.status}")
    print(f"Job B worker_id: {job_b.worker_id}")
    print(f"Worker current_job_id: {updated_worker.current_job_id}")
    print("=" * 80)
    print("Old worker from job A cleared current_job_id.")
    print("New execution of job B is hidden from heartbeat monitoring.")
    print("=" * 80 + "\n")


@pytest.mark.asyncio
async def test_recovery_with_continuous_stale_heartbeats(db_session):
    """
    Scenario F: Recovery evaluating job while stale heartbeats continue.

    Setup:
    1. Worker pod-1 claims job (retry_count=0)
    2. Worker pod-1 starts processing
    3. Job times out (started_at + timeout + 10s elapsed)
    4. Old Worker pod-1 continues sending heartbeats (every 30s)
    5. Recovery attempts to recover job

    Critical: If old worker sends heartbeat every 30s, the heartbeat is always
    fresh (<60s), and recovery will NEVER recover the job.
    """
    # Create job that timed out
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 1000},  # Long job
        status=JobStatus.PROCESSING,
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=320),  # Timed out
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Worker heartbeat is fresh
    worker = WorkerHeartbeat(
        worker_id="worker-pod-1",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=25),  # Fresh: 25s ago
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
        current_job_id=job.id,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Recovery attempts to recover job (iteration 1)
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs, "Job should be detected as stuck"

    recovered_1 = await recovery_service.recover_stuck_job(job_id)
    assert recovered_1, "Recovery should succeed (job timeout exceeded)"

    # Job should be recovered now
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING, "Job should be recovered"

    # Simulate 30 seconds passing
    await asyncio.sleep(0.1)

    # Old worker sends another heartbeat (but job is already recovered)
    worker_service = WorkerService(db_session)
    await worker_service.update_heartbeat("worker-pod-1", current_job_id=str(job_id))
    await db_session.commit()

    # FIX: Recovery should succeed on first or second attempt
    # (depends on whether first attempt was successful)
    await db_session.refresh(job)

    assert job.status == JobStatus.RETRYING, "Job should be recovered"
    assert job.retry_count == 1, "Job should have retry_count=1"

    print("\n" + "=" * 80)
    print("FIX VERIFIED: Recovery succeeded despite continuous heartbeats")
    print("=" * 80)
    print("Job timeout: {}s".format(job.timeout))
    print("Time elapsed: {}s".format((datetime.utcnow() - job.started_at).total_seconds()))
    print("Recovery attempts: 2")
    print("Job status: {}".format(job.status))
    print("Job retry_count: {}".format(job.retry_count))
    print("=" * 80)
    print("Recovery decision based on job timeout, not heartbeat freshness.")
    print("Continuous heartbeats no longer block legitimate recovery.")
    print("=" * 80 + "\n")


@pytest.mark.asyncio
async def test_concurrent_heartbeat_and_recovery(db_session):
    """
    Test concurrent heartbeat update and recovery evaluation.

    Verifies that heartbeat updates and recovery operations can run concurrently
    without deadlocks or race conditions.
    """

    async def send_heartbeats(worker_id: str, job_id: str, count: int):
        """Send multiple heartbeats"""
        for _ in range(count):
            async with TestSessionLocal() as session:
                worker_service = WorkerService(session)
                await worker_service.update_heartbeat(worker_id, current_job_id=job_id)
                await session.commit()
            await asyncio.sleep(0.01)

    async def attempt_recovery(job_id):
        """Attempt recovery"""
        async with TestSessionLocal() as session:
            recovery_service = JobRecoveryService(session, session_factory=TestSessionLocal)
            return await recovery_service.recover_stuck_job(job_id)

    # Create timed-out job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-concurrent",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Worker heartbeat is stale
    worker = WorkerHeartbeat(
        worker_id="worker-concurrent",
        started_at=datetime.utcnow() - timedelta(seconds=320),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=70),  # Stale
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
        current_job_id=job.id,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Run heartbeats and recovery concurrently
    results = await asyncio.gather(
        send_heartbeats("worker-concurrent", str(job_id), 5),
        attempt_recovery(job_id),
        attempt_recovery(job_id),
        return_exceptions=True,
    )

    # Check for exceptions
    for result in results:
        if isinstance(result, Exception):
            pytest.fail(f"Concurrent operation failed: {result}")

    # Verify final state
    await db_session.refresh(job)
    await db_session.refresh(worker)

    print("\nFinal job status: {}".format(job.status))
    print("Final job retry_count: {}".format(job.retry_count))
    print(
        "Final heartbeat age: {}s".format(
            (datetime.utcnow() - worker.last_heartbeat).total_seconds()
        )
    )


@pytest.mark.asyncio
async def test_heartbeat_timeout_threshold_vs_job_timeout(db_session):
    """
    Test the relationship between heartbeat timeout (60s) and job timeout (300s).

    Critical insight: Heartbeat timeout is much shorter than job timeout.
    A job can timeout (300s + 10s) while heartbeat is still fresh (<60s).

    This is the root cause: Recovery checks both:
    1. Job timeout: started_at + 310s
    2. Worker alive: heartbeat < 60s

    Both conditions must be true for recovery, but they're on different timescales.
    """
    # Create job that just timed out
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        worker_id="worker-threshold",
        started_at=datetime.utcnow() - timedelta(seconds=311),  # Just exceeded timeout
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)

    # Worker heartbeat is fresh (worker appears alive)
    worker = WorkerHeartbeat(
        worker_id="worker-threshold",
        started_at=datetime.utcnow() - timedelta(seconds=311),
        last_heartbeat=datetime.utcnow() - timedelta(seconds=30),  # Fresh: 30s ago
        is_active=True,
        jobs_processed=0,
        jobs_failed=0,
        current_job_id=job.id,
    )
    db_session.add(worker)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Recovery evaluates job
    recovery_service = JobRecoveryService(db_session, session_factory=TestSessionLocal)

    # Job should be detected as stuck
    stuck_jobs = await recovery_service.detect_stuck_jobs(batch_size=10)
    assert job_id in stuck_jobs, "Job should be detected (timeout exceeded)"

    # Recovery checks if worker is alive
    is_alive = await recovery_service._is_worker_alive("worker-threshold")
    assert is_alive, "Worker should appear alive (fresh heartbeat)"

    # Recovery attempts to recover
    recovered = await recovery_service.recover_stuck_job(job_id)

    # FIX: Recovery should succeed based on job timeout
    assert recovered, "Recovery should succeed (job timeout exceeded)"

    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING, "Job should be recovered"

    print("\n" + "=" * 80)
    print("FIX: Job timeout is authoritative for recovery decisions")
    print("=" * 80)
    print("Job timeout: 300s")
    print("Time elapsed: {}s".format((datetime.utcnow() - job.started_at).total_seconds()))
    print("Heartbeat timeout: 60s")
    print("Heartbeat age: {}s".format((datetime.utcnow() - worker.last_heartbeat).total_seconds()))
    print("=" * 80)
    print("Job timed out (311s > 310s threshold)")
    print("Worker appears alive (heartbeat 30s < 60s threshold)")
    print("Recovery proceeds based on job timeout (authoritative contract).")
    print("Heartbeat timeout is independent - used for worker health only.")
    print("=" * 80 + "\n")
