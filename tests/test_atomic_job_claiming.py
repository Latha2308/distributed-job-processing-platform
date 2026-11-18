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

    # Claim the job
    claimed_job = await job_service.claim_job(job.id, "worker-1")
    await db_session.commit()

    # Verify claim succeeded
    assert claimed_job is not None
    assert claimed_job.status == JobStatus.PROCESSING
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
