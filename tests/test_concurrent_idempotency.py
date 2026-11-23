"""
Concurrent idempotency tests for job creation.

These tests verify that the SQLAlchemy savepoint-based exception handling
correctly resolves race conditions when multiple requests with the same
idempotency key arrive simultaneously.

Important: These tests use actual PostgreSQL database connections and
transactions to exercise real concurrency scenarios and unique constraint
violations.

Table lifecycle: Each test uses a function-scoped fixture to create tables
before the test and drop them after, ensuring all operations (setup, concurrent
execution, verification, teardown) occur within the same event-loop lifecycle.

WARNING: These tests must NOT be run in parallel with other tests that use
the db_session fixture or otherwise manipulate the jobprocessing_test database,
as both will create/drop tables and could interfere with each other.

Run these tests sequentially: pytest tests/test_concurrent_idempotency.py
"""

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select
from unittest.mock import MagicMock, AsyncMock

from app.db.base import Base
from app.db.models import Job, JobOutbox, JobStatus
from app.models.job import JobCreate
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.core.redis_client import RedisClient
from tests.conftest import test_engine, TestSessionLocal


@pytest_asyncio.fixture
async def setup_test_tables():
    """
    Function-scoped fixture to create test tables before each test and drop after.

    This ensures that database setup, concurrent operations, verification, and
    teardown all run within the same test's event-loop lifecycle, which is
    required when using asyncio_mode = auto.

    WARNING: These tests must NOT be run in parallel with other tests that use
    the db_session fixture or otherwise manipulate the jobprocessing_test database,
    as both will create/drop tables and could interfere with each other.

    Run these tests sequentially: pytest tests/test_concurrent_idempotency.py
    """
    # Create all tables before this test
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    # Drop all tables after this test completes
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing"""
    return MagicMock(spec=RedisClient)


@pytest.fixture
def mock_queue_service(mock_redis):
    """Mock queue service for testing"""
    queue_service = MagicMock(spec=QueueService)
    queue_service.enqueue_job = AsyncMock()
    return queue_service


@pytest.mark.asyncio
async def test_concurrent_create_with_same_idempotency_key(
    setup_test_tables, mock_redis, mock_queue_service
):
    """
    Test that 5 concurrent job creation requests with the same idempotency key
    all succeed and return the same job.

    This test exercises the actual PostgreSQL unique constraint collision
    handling using SQLAlchemy savepoints (nested transactions).

    Uses asyncio.Barrier to synchronize concurrent tasks so they all attempt
    job creation at exactly the same time, maximizing the chance of
    hitting the race condition.

    Expected behavior:
    - All 5 requests succeed (no HTTP 500 / no raised exceptions)
    - All 5 requests return the same job ID
    - Exactly 1 job record exists in the database
    - Exactly 1 outbox event exists for that job

    This regression test validates the fix for the concurrent idempotency bug
    where 4 out of 5 requests would fail with IntegrityError.
    """
    # Generate a unique idempotency key for this test run
    idempotency_key = f"concurrent-test-{uuid4()}"

    # Prepare job data
    job_data_dict = {
        "job_type": "concurrent_task",
        "payload": {"test": "concurrent_data"},
        "priority": 5,
        "idempotency_key": idempotency_key,
        "max_retries": 1,
        "timeout": 10,
    }

    # Use a Barrier for precise synchronization
    # All 5 tasks must reach the barrier before any proceed
    num_tasks = 5
    barrier = asyncio.Barrier(num_tasks)

    async def create_job_in_separate_session(task_id: int):
        """
        Create a job using an independent database session and transaction.

        Each concurrent task gets its own AsyncSession from the connection pool,
        simulating separate API requests that arrive simultaneously.

        Args:
            task_id: Identifier for this task (for logging/debugging)
        """
        try:
            # Wait at barrier until all tasks are ready
            await asyncio.wait_for(barrier.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return Exception(f"Task {task_id} timed out waiting at barrier")

        # Create independent session from the test connection pool
        async with TestSessionLocal() as session:
            try:
                # Create service instance with this session
                job_service = JobService(session, mock_redis, mock_queue_service)

                # Create job with idempotency key
                job_data = JobCreate(**job_data_dict)
                job = await job_service.create_job(job_data)

                # Commit this transaction
                await session.commit()

                return job

            except Exception as e:
                # Rollback on any error
                await session.rollback()
                # Return the exception so we can verify what failed
                return e

    # Launch 5 concurrent job creation tasks
    # Each task has its own session and will race to insert the same idempotency key
    tasks = [create_job_in_separate_session(i) for i in range(num_tasks)]

    # Wait for all tasks to complete with timeout
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Concurrent job creation timed out after 15 seconds")

    # Verify ALL tasks succeeded (no exceptions)
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert (
        len(exceptions) == 0
    ), f"Expected all tasks to succeed, but {len(exceptions)} failed: {exceptions}"

    # Verify all are Job objects
    jobs = [r for r in results if isinstance(r, Job)]
    assert len(jobs) == num_tasks, f"Expected {num_tasks} Job objects, got {len(jobs)}"

    # Verify all returned the same job ID
    job_ids = {job.id for job in jobs}
    assert len(job_ids) == 1, f"Expected 1 unique job ID, got {len(job_ids)}: {job_ids}"

    # Get the single job ID
    created_job_id = list(job_ids)[0]

    # Verify database state using a separate verification session
    async with TestSessionLocal() as verify_session:
        # Query for jobs with this idempotency key
        job_result = await verify_session.execute(
            select(Job).where(Job.idempotency_key == idempotency_key)
        )
        persisted_jobs = list(job_result.scalars().all())

        assert (
            len(persisted_jobs) == 1
        ), f"Expected exactly 1 job in database, found {len(persisted_jobs)}"

        persisted_job = persisted_jobs[0]
        assert (
            persisted_job.id == created_job_id
        ), f"Database job ID {persisted_job.id} doesn't match returned ID {created_job_id}"
        assert persisted_job.idempotency_key == idempotency_key
        assert persisted_job.status == JobStatus.QUEUED

        # Query for outbox events for this job
        outbox_result = await verify_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == created_job_id)
        )
        outbox_events = list(outbox_result.scalars().all())

        assert (
            len(outbox_events) == 1
        ), f"Expected exactly 1 outbox event, found {len(outbox_events)}"

        outbox_event = outbox_events[0]
        assert outbox_event.job_id == created_job_id
        assert outbox_event.event_type == "job_created"
        assert outbox_event.published is False


@pytest.mark.asyncio
async def test_concurrent_create_with_different_keys(
    setup_test_tables, mock_redis, mock_queue_service
):
    """
    Test that 5 concurrent requests with different idempotency keys
    create 5 separate jobs successfully.

    This verifies that the concurrency handling doesn't interfere with
    legitimate concurrent creation of different jobs.
    """
    # Generate 5 unique idempotency keys
    keys = [f"unique-key-{uuid4()}" for _ in range(5)]
    num_tasks = len(keys)

    # Use barrier for synchronization
    barrier = asyncio.Barrier(num_tasks)

    async def create_job_with_key(idempotency_key: str, task_id: int):
        """Create a job with a specific idempotency key"""
        try:
            await asyncio.wait_for(barrier.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return Exception(f"Task {task_id} timed out at barrier")

        async with TestSessionLocal() as session:
            try:
                job_service = JobService(session, mock_redis, mock_queue_service)

                job_data = JobCreate(
                    job_type="different_keys_task",
                    payload={"key": idempotency_key},
                    priority=5,
                    idempotency_key=idempotency_key,
                )

                job = await job_service.create_job(job_data)
                await session.commit()

                return job

            except Exception as e:
                await session.rollback()
                return e

    # Create 5 concurrent tasks with different keys
    tasks = [create_job_with_key(key, i) for i, key in enumerate(keys)]

    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Concurrent job creation with different keys timed out")

    # Verify all succeeded
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0, f"Expected all tasks to succeed, but {len(exceptions)} failed"

    jobs = [r for r in results if isinstance(r, Job)]
    assert len(jobs) == num_tasks

    # Verify all have different IDs
    job_ids = {job.id for job in jobs}
    assert len(job_ids) == num_tasks, "Expected 5 unique job IDs for 5 different idempotency keys"

    # Verify database has 5 jobs
    async with TestSessionLocal() as verify_session:
        for key in keys:
            job_result = await verify_session.execute(select(Job).where(Job.idempotency_key == key))
            jobs_for_key = list(job_result.scalars().all())
            assert (
                len(jobs_for_key) == 1
            ), f"Expected 1 job for key {key}, found {len(jobs_for_key)}"


@pytest.mark.asyncio
async def test_sequential_then_concurrent_with_same_key(
    setup_test_tables, mock_redis, mock_queue_service
):
    """
    Test mixed sequential and concurrent requests.

    First request creates the job sequentially, then 3 concurrent requests
    should all return the existing job.
    """
    idempotency_key = f"mixed-test-{uuid4()}"

    # Create first job sequentially
    async with TestSessionLocal() as session:
        job_service = JobService(session, mock_redis, mock_queue_service)

        job_data = JobCreate(
            job_type="mixed_task",
            payload={"phase": "sequential"},
            priority=5,
            idempotency_key=idempotency_key,
        )

        first_job = await job_service.create_job(job_data)
        await session.commit()
        first_job_id = first_job.id

    # Now launch 3 concurrent requests with the same key
    num_concurrent = 3
    barrier = asyncio.Barrier(num_concurrent)

    async def create_duplicate(task_id: int):
        try:
            await asyncio.wait_for(barrier.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return Exception(f"Task {task_id} timed out at barrier")

        async with TestSessionLocal() as session:
            try:
                job_service = JobService(session, mock_redis, mock_queue_service)

                job_data = JobCreate(
                    job_type="mixed_task",
                    payload={"phase": "concurrent"},
                    priority=5,
                    idempotency_key=idempotency_key,
                )

                job = await job_service.create_job(job_data)
                await session.commit()

                return job

            except Exception as e:
                await session.rollback()
                return e

    tasks = [create_duplicate(i) for i in range(num_concurrent)]

    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Sequential then concurrent test timed out")

    # Verify all succeeded
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0

    jobs = [r for r in results if isinstance(r, Job)]
    assert len(jobs) == num_concurrent

    # Verify all returned the same job ID as the first sequential request
    for job in jobs:
        assert (
            job.id == first_job_id
        ), "Concurrent requests should return the same job as sequential request"

    # Verify database still has only 1 job and 1 outbox event
    async with TestSessionLocal() as verify_session:
        job_result = await verify_session.execute(
            select(Job).where(Job.idempotency_key == idempotency_key)
        )
        jobs_in_db = list(job_result.scalars().all())
        assert len(jobs_in_db) == 1

        outbox_result = await verify_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == first_job_id)
        )
        outbox_events = list(outbox_result.scalars().all())
        assert len(outbox_events) == 1


@pytest.mark.asyncio
async def test_concurrent_without_idempotency_key(
    setup_test_tables, mock_redis, mock_queue_service
):
    """
    Test that concurrent requests WITHOUT idempotency keys create separate jobs.

    This verifies that the idempotency handling doesn't affect normal job creation.
    """
    num_tasks = 3
    barrier = asyncio.Barrier(num_tasks)

    async def create_job_without_key(task_id: int):
        try:
            await asyncio.wait_for(barrier.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            return Exception(f"Task {task_id} timed out at barrier")

        async with TestSessionLocal() as session:
            try:
                job_service = JobService(session, mock_redis, mock_queue_service)

                job_data = JobCreate(
                    job_type="no_key_task",
                    payload={"timestamp": str(uuid4())},
                    priority=5,
                    # No idempotency_key
                )

                job = await job_service.create_job(job_data)
                await session.commit()

                return job

            except Exception as e:
                await session.rollback()
                return e

    # Create 3 concurrent jobs without idempotency keys
    tasks = [create_job_without_key(i) for i in range(num_tasks)]

    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15.0)
    except asyncio.TimeoutError:
        pytest.fail("Concurrent creation without idempotency keys timed out")

    # Verify all succeeded
    exceptions = [r for r in results if isinstance(r, Exception)]
    assert len(exceptions) == 0

    jobs = [r for r in results if isinstance(r, Job)]
    assert len(jobs) == num_tasks

    # Verify all have different IDs (since no idempotency key)
    job_ids = {job.id for job in jobs}
    assert (
        len(job_ids) == num_tasks
    ), "Expected 3 different jobs when no idempotency key is provided"

    # Verify each has its own outbox event
    async with TestSessionLocal() as verify_session:
        for job_id in job_ids:
            outbox_result = await verify_session.execute(
                select(JobOutbox).where(JobOutbox.job_id == job_id)
            )
            outbox_events = list(outbox_result.scalars().all())
            assert len(outbox_events) == 1, f"Expected 1 outbox event for job {job_id}"
