"""
Tests for Atomic Job Claiming

Verifies that the claim_job() method prevents race conditions and ensures
only one worker can claim a job at a time.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import select

from app.db.models import Job, JobStatus, JobPriority
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.core.redis_client import RedisClient


@pytest.mark.asyncio
async def test_claim_queued_job_success(db_session):
    """Test successfully claiming a QUEUED job"""
    # Create a queued job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Claim the job
    claimed_job = await job_service.claim_job(job.id, "worker-1")
    await db_session.commit()

    # Verify claim succeeded
    assert claimed_job is not None
    assert claimed_job.id == job.id
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.worker_id == "worker-1"
    assert claimed_job.started_at is not None
    assert claimed_job.queue_wait_time is not None
    assert claimed_job.queue_wait_time >= 0


@pytest.mark.asyncio
async def test_claim_retrying_job_when_ready(db_session):
    """Test claiming a RETRYING job when next_retry_at has passed"""
    # Create a retrying job with next_retry_at in the past
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Claim the job with valid retry_attempt=1 (matches retry_count)
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=1)
    await db_session.commit()

    # Verify claim succeeded
    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.worker_id == "worker-1"
    assert claimed_job.retry_count == 1  # Preserved
    assert claimed_job.worker_id == "worker-1"
    assert claimed_job.retry_count == 1  # Preserved


@pytest.mark.asyncio
async def test_cannot_claim_retrying_job_too_early(db_session):
    """Test that RETRYING job cannot be claimed before next_retry_at"""
    # Create a retrying job with next_retry_at in the future
    future_time = datetime.utcnow() + timedelta(seconds=60)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=future_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim the job
    claimed_job = await job_service.claim_job(job.id, "worker-1")

    # Verify claim failed
    assert claimed_job is None

    # Verify job status unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_cannot_claim_processing_job(db_session):
    """Test that a PROCESSING job cannot be claimed by another worker"""
    # Create a job already in processing
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,
        priority=JobPriority.NORMAL.value,
        worker_id="worker-1",
        started_at=datetime.utcnow(),
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim by another worker
    claimed_job = await job_service.claim_job(job.id, "worker-2")

    # Verify claim failed
    assert claimed_job is None

    # Verify job still owned by worker-1
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.worker_id == "worker-1"


@pytest.mark.asyncio
async def test_cannot_claim_completed_job(db_session):
    """Test that a COMPLETED job cannot be claimed"""
    # Create a completed job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.COMPLETED,
        priority=JobPriority.NORMAL.value,
        worker_id="worker-1",
        completed_at=datetime.utcnow(),
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim
    claimed_job = await job_service.claim_job(job.id, "worker-2")

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_cannot_claim_dead_letter_job(db_session):
    """Test that a DEAD_LETTER job cannot be claimed"""
    # Create a dead letter job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.DEAD_LETTER,
        priority=JobPriority.NORMAL.value,
        is_dead_letter=True,
        moved_to_dlq_at=datetime.utcnow(),
        retry_count=3,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim
    claimed_job = await job_service.claim_job(job.id, "worker-1")

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.DEAD_LETTER


@pytest.mark.asyncio
async def test_concurrent_workers_only_one_claims_job(db_session):
    """
    Test that when two workers attempt to claim the same job concurrently,
    only one succeeds.

    This is the critical race condition test - uses two independent database sessions.
    Each worker commits immediately after attempting to claim.
    """
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Create a queued job using the test session
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create two independent sessions to simulate concurrent workers
    TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    session1 = SessionFactory()
    session2 = SessionFactory()

    async def worker_claim_and_commit(session, worker_id):
        """Worker attempts to claim and immediately commits/rolls back"""
        try:
            mock_redis = RedisClient()
            job_service = JobService(session, mock_redis, QueueService(mock_redis))

            # Attempt to claim
            claimed_job = await job_service.claim_job(job_id, worker_id)

            if claimed_job:
                # Successful claim - commit immediately
                await session.commit()
                return claimed_job
            else:
                # Failed claim - rollback
                await session.rollback()
                return None
        except Exception:
            # Unexpected error - clean up and re-raise to fail the test
            await session.rollback()
            raise

    try:
        # Run both workers concurrently with timeout
        results = await asyncio.wait_for(
            asyncio.gather(
                worker_claim_and_commit(session1, "worker-1"),
                worker_claim_and_commit(session2, "worker-2"),
                return_exceptions=False,
            ),
            timeout=5.0,  # 5 second timeout to prevent hanging
        )

        # Verify exactly one claim succeeded
        successful_claims = [r for r in results if r is not None]
        failed_claims = [r for r in results if r is None]

        assert (
            len(successful_claims) == 1
        ), f"Exactly one worker should claim the job, got {len(successful_claims)}"
        assert (
            len(failed_claims) == 1
        ), f"Exactly one worker should fail to claim, got {len(failed_claims)}"

        # Verify the successful claim
        claimed_job = successful_claims[0]
        assert claimed_job.status == JobStatus.PROCESSING
        assert claimed_job.worker_id in ["worker-1", "worker-2"]

        # Verify database state using original session
        db_session.expire_all()  # Synchronous method - do not await
        result = await db_session.execute(select(Job).where(Job.id == job_id))
        db_job = result.scalar_one()
        assert db_job.status == JobStatus.PROCESSING
        assert db_job.worker_id == claimed_job.worker_id

    except asyncio.TimeoutError:
        pytest.fail("Test timed out - possible deadlock in concurrent claim")
    finally:
        # Clean up sessions
        await session1.close()
        await session2.close()
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_claims_on_multiple_jobs(db_session):
    """
    Test that multiple workers can claim different jobs simultaneously without conflict.
    Each worker uses its own independent AsyncSession.
    """
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Create multiple queued jobs
    jobs = []
    for i in range(5):
        job = Job(
            job_type="test_job",
            payload={"data": f"test-{i}"},
            status=JobStatus.QUEUED,
            priority=JobPriority.NORMAL.value,
            max_retries=3,
            timeout=300,
        )
        db_session.add(job)
        jobs.append(job)

    await db_session.commit()
    for job in jobs:
        await db_session.refresh(job)

    # Create independent sessions for each worker
    TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    sessions = [SessionFactory() for _ in range(5)]

    async def worker_claim_job(session, job_id, worker_id):
        """Each worker claims with its own session"""
        try:
            mock_redis = RedisClient()
            job_service = JobService(session, mock_redis, QueueService(mock_redis))

            claimed_job = await job_service.claim_job(job_id, worker_id)

            if claimed_job:
                await session.commit()
                return claimed_job
            else:
                await session.rollback()
                return None
        except Exception:
            # Unexpected error - clean up and re-raise to fail the test
            await session.rollback()
            raise

    try:
        # Each worker tries to claim a different job with its own session
        tasks = [
            worker_claim_job(sessions[0], jobs[0].id, "worker-1"),
            worker_claim_job(sessions[1], jobs[1].id, "worker-2"),
            worker_claim_job(sessions[2], jobs[2].id, "worker-3"),
            worker_claim_job(sessions[3], jobs[3].id, "worker-4"),
            worker_claim_job(sessions[4], jobs[4].id, "worker-5"),
        ]

        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)  # 5 second timeout

        # Verify all claims succeeded
        assert all(r is not None for r in results)
        assert len(results) == 5

        # Verify each job claimed by different worker
        worker_ids = {r.worker_id for r in results}
        assert len(worker_ids) == 5  # All different workers

    except asyncio.TimeoutError:
        pytest.fail("Test timed out - possible deadlock in concurrent claims")
    finally:
        # Clean up all sessions
        for session in sessions:
            await session.close()
        await engine.dispose()


@pytest.mark.asyncio
async def test_claim_nonexistent_job(db_session):
    """Test claiming a job that doesn't exist returns None"""
    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim non-existent job
    nonexistent_id = uuid4()
    claimed_job = await job_service.claim_job(nonexistent_id, "worker-1")

    # Verify claim failed
    assert claimed_job is None


@pytest.mark.asyncio
async def test_claim_sets_queue_wait_time(db_session):
    """Test that claiming a job calculates and sets queue_wait_time"""
    # Create a job that's been queued for a while
    past_time = datetime.utcnow() - timedelta(seconds=30)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        created_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Claim the job
    claimed_job = await job_service.claim_job(job.id, "worker-1")
    await db_session.commit()

    # Verify queue_wait_time was set and is reasonable
    assert claimed_job is not None
    assert claimed_job.queue_wait_time is not None
    assert claimed_job.queue_wait_time >= 25  # At least 25 seconds
    assert (
        claimed_job.queue_wait_time <= 35
    )  # At most 35 seconds (accounting for test execution time)


@pytest.mark.asyncio
async def test_claim_preserves_job_metadata(db_session):
    """Test that claiming preserves existing job metadata like retry_count and payload"""
    # Create a job with specific metadata
    job = Job(
        job_type="test_job",
        payload={"important": "data", "count": 42},
        status=JobStatus.QUEUED,
        priority=JobPriority.HIGH.value,
        retry_count=0,
        max_retries=5,
        timeout=600,
        idempotency_key="unique-key-123",
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Claim the job
    claimed_job = await job_service.claim_job(job.id, "worker-1")
    await db_session.commit()

    # Verify all metadata preserved
    assert claimed_job is not None
    assert claimed_job.job_type == "test_job"
    assert claimed_job.payload == {"important": "data", "count": 42}
    assert claimed_job.priority == JobPriority.HIGH.value
    assert claimed_job.retry_count == 0
    assert claimed_job.max_retries == 5
    assert claimed_job.timeout == 600
    assert claimed_job.idempotency_key == "unique-key-123"


# ============================================================================
# Retry Attempt Validation Tests
# ============================================================================


@pytest.mark.asyncio
async def test_claim_recovered_job_with_valid_retry_attempt(db_session):
    """
    Test that a recovered job with valid retry_attempt can be claimed.

    Scenario: Recovery dispatcher publishes retry_attempt=2 for a job at retry_count=2.
    Expected: Worker can claim the job atomically.
    """
    # Create a retrying job with retry_count=2, ready to retry
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=2,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Claim with matching retry_attempt=2
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=2)
    await db_session.commit()

    # Verify claim succeeded
    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.worker_id == "worker-1"
    assert claimed_job.retry_count == 2  # Preserved
    assert claimed_job.started_at is not None


@pytest.mark.asyncio
async def test_claim_recovered_job_duplicate_retry_attempt_fails(db_session):
    """
    Test that duplicate retry_attempt messages are rejected.

    Scenario:
    1. Recovery publishes retry_attempt=1 (Redis message 1)
    2. Worker-1 claims and processes, increments retry_count to 2
    3. Crash before outbox commit, Redis message 1 redelivered
    4. Worker-2 receives retry_attempt=1, but job is now at retry_count=2

    Expected: Claim with retry_attempt=1 fails (stale).
    """
    # Create job at retry_count=2 (first attempt already completed)
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=2,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with stale retry_attempt=1
    claimed_job = await job_service.claim_job(job.id, "worker-2", retry_attempt=1)

    # Verify claim failed (stale retry_attempt)
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None
    assert job.retry_count == 2


@pytest.mark.asyncio
async def test_claim_recovered_job_stale_retry_attempt_fails(db_session):
    """
    Test that stale retry_attempt messages from old recovery runs are rejected.

    Scenario: Job has progressed from retry_count=1 to retry_count=3,
    but a stale Redis message with retry_attempt=1 arrives.

    Expected: Claim fails atomically.
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=3,
        next_retry_at=past_time,
        max_retries=5,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with very stale retry_attempt=1
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=1)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.retry_count == 3
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_claim_recovered_job_future_retry_attempt_fails(db_session):
    """
    Test that future retry_attempt values are rejected.

    Scenario: Job is at retry_count=1, but message claims retry_attempt=3.
    Expected: Claim fails (invalid future attempt).
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=5,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with future retry_attempt=3
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=3)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.retry_count == 1
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_claim_recovered_job_before_next_retry_at_fails(db_session):
    """
    Test that retry_attempt cannot be claimed before next_retry_at.

    Scenario: Job at retry_count=1, retry_attempt=1, but next_retry_at is 60s in future.
    Expected: Claim fails (not ready yet).
    """
    future_time = datetime.utcnow() + timedelta(seconds=60)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=future_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with matching retry_attempt=1 but too early
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=1)

    # Verify claim failed (not ready yet)
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None
    assert job.retry_count == 1


@pytest.mark.asyncio
async def test_claim_recovered_job_concurrent_workers_one_winner(db_session):
    """
    Test that two workers racing to claim the same retry_attempt produce exactly one winner.

    Scenario: Recovery publishes retry_attempt=2 twice (duplicate message).
    Worker-1 and Worker-2 both receive retry_attempt=2 and race to claim.

    Expected: Exactly one worker claims successfully (atomic UPDATE).
    """
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Create a retrying job at retry_count=2
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=2,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create two independent sessions
    TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    session1 = SessionFactory()
    session2 = SessionFactory()

    async def worker_claim_with_retry_attempt(session, worker_id, retry_attempt):
        """Worker attempts to claim with retry_attempt and commits"""
        try:
            mock_redis = RedisClient()
            job_service = JobService(session, mock_redis, QueueService(mock_redis))

            claimed_job = await job_service.claim_job(job_id, worker_id, retry_attempt)

            if claimed_job:
                await session.commit()
                return claimed_job
            else:
                await session.rollback()
                return None
        except Exception:
            await session.rollback()
            raise

    try:
        # Both workers race to claim retry_attempt=2
        results = await asyncio.wait_for(
            asyncio.gather(
                worker_claim_with_retry_attempt(session1, "worker-1", retry_attempt=2),
                worker_claim_with_retry_attempt(session2, "worker-2", retry_attempt=2),
                return_exceptions=False,
            ),
            timeout=5.0,
        )

        # Verify exactly one claim succeeded
        successful_claims = [r for r in results if r is not None]
        failed_claims = [r for r in results if r is None]

        assert (
            len(successful_claims) == 1
        ), f"Exactly one worker should claim retry_attempt=2, got {len(successful_claims)}"
        assert len(failed_claims) == 1

        # Verify the winner
        claimed_job = successful_claims[0]
        assert claimed_job.status == JobStatus.PROCESSING
        assert claimed_job.worker_id in ["worker-1", "worker-2"]
        assert claimed_job.retry_count == 2

        # Verify database state
        db_session.expire_all()
        result = await db_session.execute(select(Job).where(Job.id == job_id))
        db_job = result.scalar_one()
        assert db_job.status == JobStatus.PROCESSING
        assert db_job.worker_id == claimed_job.worker_id
        assert db_job.retry_count == 2

    except asyncio.TimeoutError:
        pytest.fail("Test timed out - possible deadlock in concurrent retry claim")
    finally:
        await session1.close()
        await session2.close()
        await engine.dispose()


@pytest.mark.asyncio
async def test_claim_initial_job_still_works_without_retry_attempt(db_session):
    """
    Test backward compatibility: initial QUEUED jobs can still be claimed without retry_attempt.

    Scenario: Normal job submission (non-recovery), no retry_attempt in message.
    Expected: Worker can claim without providing retry_attempt parameter.
    """
    # Create a fresh queued job (initial submission)
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Claim WITHOUT retry_attempt (backward compatible)
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=None)
    await db_session.commit()

    # Verify claim succeeded
    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.worker_id == "worker-1"
    assert claimed_job.retry_count == 0


@pytest.mark.asyncio
async def test_worker_fencing_prevents_old_completion(db_session):
    """
    Test that a slow worker from an old retry attempt cannot overwrite a newer attempt's work.

    Scenario:
    1. Worker-1 claims retry_attempt=1, starts processing (slow)
    2. Job times out, dispatcher increments retry_count to 2
    3. Worker-2 claims retry_attempt=2, completes successfully
    4. Worker-1 finally finishes and tries to update job

    Expected: Worker-1's completion is rejected (retry_count mismatch prevents update).

    Note: This test verifies the claim step. The actual completion fencing
    is handled by mark_job_completed() which should also validate retry_count.
    """
    # Create job at retry_count=2 (newer attempt)
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=2,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Worker-1 tries to claim with old retry_attempt=1 (should fail)
    old_claim = await job_service.claim_job(job.id, "worker-1", retry_attempt=1)
    assert old_claim is None  # Old attempt cannot claim newer job

    # Worker-2 claims with current retry_attempt=2 (should succeed)
    new_claim = await job_service.claim_job(job.id, "worker-2", retry_attempt=2)
    await db_session.commit()

    assert new_claim is not None
    assert new_claim.status == JobStatus.PROCESSING
    assert new_claim.worker_id == "worker-2"
    assert new_claim.retry_count == 2

    # Verify database state: only worker-2 owns the job
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.worker_id == "worker-2"
    assert job.retry_count == 2


# ============================================================================
# Bypass Prevention Tests
# ============================================================================


@pytest.mark.asyncio
async def test_retrying_job_without_retry_attempt_fails(db_session):
    """
    Test that RETRYING jobs cannot be claimed without retry_attempt.

    This closes the bypass vulnerability where missing retry_attempt
    allowed RETRYING jobs to be claimed without validation.

    Scenario: RETRYING job with retry_count=1, but claim with retry_attempt=None.
    Expected: Claim fails (RETRYING requires explicit retry_attempt).
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim RETRYING job WITHOUT retry_attempt (bypass attempt)
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=None)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None
    assert job.retry_count == 1


@pytest.mark.asyncio
async def test_queued_job_with_retry_attempt_fails(db_session):
    """
    Test that QUEUED jobs cannot be claimed with retry_attempt.

    Scenario: Initial QUEUED job, but claim provides retry_attempt=1.
    Expected: Claim fails (QUEUED jobs are initial attempts, not retries).
    """
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim QUEUED job WITH retry_attempt (invalid)
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=1)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED
    assert job.worker_id is None
    assert job.retry_count == 0


@pytest.mark.asyncio
async def test_claim_with_boolean_retry_attempt_fails(db_session):
    """
    Test that boolean retry_attempt values are rejected.

    Scenario: RETRYING job, but claim with retry_attempt=True.
    Expected: Claim fails immediately (type validation rejects bool).
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with boolean retry_attempt
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=True)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_claim_with_string_retry_attempt_fails(db_session):
    """
    Test that string retry_attempt values are rejected.

    Scenario: RETRYING job, but claim with retry_attempt="1".
    Expected: Claim fails immediately (type validation rejects str).
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with string retry_attempt
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt="1")

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_claim_with_float_retry_attempt_fails(db_session):
    """
    Test that float retry_attempt values are rejected.

    Scenario: RETRYING job, but claim with retry_attempt=1.0.
    Expected: Claim fails immediately (type validation rejects float).
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with float retry_attempt
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=1.0)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_claim_with_negative_retry_attempt_fails(db_session):
    """
    Test that negative retry_attempt values are rejected.

    Scenario: RETRYING job, but claim with retry_attempt=-1.
    Expected: Claim fails immediately (value validation rejects negative).
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with negative retry_attempt
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=-1)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_claim_with_zero_retry_attempt_fails(db_session):
    """
    Test that zero retry_attempt values are rejected.

    Scenario: RETRYING job at retry_count=1, but claim with retry_attempt=0.
    Expected: Claim fails immediately (value validation rejects zero).

    Note: Retry attempts start at 1 (first retry), not 0.
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Attempt to claim with zero retry_attempt
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=0)

    # Verify claim failed
    assert claimed_job is None

    # Verify job unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_malformed_retry_attempt_does_not_change_job_state(db_session):
    """
    Test that malformed retry_attempt values are rejected without side effects.

    Scenario: Multiple malformed claim attempts should not modify job state.
    Expected: Job remains RETRYING and can be claimed with valid retry_attempt.
    """
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=2,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    original_updated_at = job.updated_at

    # Create service
    mock_redis = RedisClient()
    queue_service = QueueService(mock_redis)
    job_service = JobService(db_session, mock_redis, queue_service)

    # Try multiple malformed values
    malformed_values = [None, True, False, "2", 2.0, -1, 0, 999]

    for bad_value in malformed_values:
        result = await job_service.claim_job(job.id, "worker-1", retry_attempt=bad_value)
        assert result is None, f"Claim should fail for retry_attempt={bad_value}"

    # Verify job state unchanged after all failed attempts
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None
    assert job.retry_count == 2
    assert job.updated_at == original_updated_at  # No database writes

    # Verify job can still be claimed with valid retry_attempt
    claimed_job = await job_service.claim_job(job.id, "worker-1", retry_attempt=2)
    await db_session.commit()

    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
    assert claimed_job.worker_id == "worker-1"
    assert claimed_job.retry_count == 2


@pytest.mark.asyncio
async def test_concurrent_bypass_attempts_all_fail(db_session):
    """
    Test that concurrent workers attempting to bypass with retry_attempt=None all fail.

    Scenario: Two workers race to claim RETRYING job without retry_attempt.
    Expected: Both fail (no winner, bypass closed).
    """
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Create a retrying job at retry_count=1
    past_time = datetime.utcnow() - timedelta(seconds=10)

    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        next_retry_at=past_time,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Create two independent sessions
    TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    session1 = SessionFactory()
    session2 = SessionFactory()

    async def worker_bypass_attempt(session, worker_id):
        """Worker attempts to bypass with retry_attempt=None"""
        try:
            mock_redis = RedisClient()
            job_service = JobService(session, mock_redis, QueueService(mock_redis))

            # Attempt bypass with retry_attempt=None
            claimed_job = await job_service.claim_job(job_id, worker_id, retry_attempt=None)

            if claimed_job:
                await session.commit()
                return claimed_job
            else:
                await session.rollback()
                return None
        except Exception:
            await session.rollback()
            raise

    try:
        # Both workers try to bypass concurrently
        results = await asyncio.wait_for(
            asyncio.gather(
                worker_bypass_attempt(session1, "worker-1"),
                worker_bypass_attempt(session2, "worker-2"),
                return_exceptions=False,
            ),
            timeout=5.0,
        )

        # Verify BOTH failed (bypass closed)
        assert all(r is None for r in results), "Both bypass attempts should fail"

        # Verify job still RETRYING and unclaimed
        db_session.expire_all()
        result = await db_session.execute(select(Job).where(Job.id == job_id))
        db_job = result.scalar_one()
        assert db_job.status == JobStatus.RETRYING
        assert db_job.worker_id is None
        assert db_job.retry_count == 1

    except asyncio.TimeoutError:
        pytest.fail("Test timed out - possible deadlock")
    finally:
        await session1.close()
        await session2.close()
        await engine.dispose()
