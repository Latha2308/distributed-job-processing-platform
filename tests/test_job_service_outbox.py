"""Tests for job service with transactional outbox integration"""

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobOutbox, JobStatus, JobPriority
from app.models.job import JobCreate
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.core.redis_client import RedisClient
from unittest.mock import MagicMock, AsyncMock


@pytest.fixture
def mock_redis():
    """Mock Redis client"""
    return MagicMock(spec=RedisClient)


@pytest.fixture
def mock_queue_service(mock_redis):
    """Mock queue service"""
    queue_service = MagicMock(spec=QueueService)
    queue_service.enqueue_job = AsyncMock()
    return queue_service


@pytest.fixture
def job_service(db_session: AsyncSession, mock_redis, mock_queue_service):
    """Create job service with mocked dependencies"""
    return JobService(db_session, mock_redis, mock_queue_service)


@pytest.mark.asyncio
async def test_job_and_outbox_committed_together(db_session: AsyncSession, job_service: JobService):
    """
    Test that job and outbox event are created in the same transaction.

    Verifies atomic commit: both records exist after commit.
    """
    job_data = JobCreate(
        job_type="test_task",
        payload={"data": "test"},
        priority=5,
    )

    # Create job
    job = await job_service.create_job(job_data)
    assert job.id is not None
    assert job.status == JobStatus.QUEUED

    # Commit transaction
    await db_session.commit()

    # Verify job exists
    job_result = await db_session.execute(select(Job).where(Job.id == job.id))
    persisted_job = job_result.scalar_one_or_none()
    assert persisted_job is not None
    assert persisted_job.job_type == "test_task"

    # Verify outbox event exists for this job
    outbox_result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job.id))
    outbox_event = outbox_result.scalar_one_or_none()
    assert outbox_event is not None
    assert outbox_event.job_id == job.id
    assert outbox_event.event_type == "job_created"
    assert outbox_event.published is False
    assert outbox_event.attempts == 0


@pytest.mark.asyncio
async def test_rollback_persists_nothing(db_session: AsyncSession, job_service: JobService):
    """
    Test that rollback leaves neither job nor outbox event persisted.

    Verifies transaction atomicity on failure.
    """
    job_data = JobCreate(
        job_type="rollback_task",
        payload={"data": "test"},
        priority=5,
    )

    job_id = None

    try:
        # Create job
        job = await job_service.create_job(job_data)
        job_id = job.id

        # Simulate error before commit
        raise RuntimeError("Simulated transaction failure")
    except RuntimeError:
        await db_session.rollback()

    # Verify job does NOT exist
    if job_id:
        job_result = await db_session.execute(select(Job).where(Job.id == job_id))
        persisted_job = job_result.scalar_one_or_none()
        assert persisted_job is None

        # Verify outbox event does NOT exist
        outbox_result = await db_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job_id)
        )
        outbox_event = outbox_result.scalar_one_or_none()
        assert outbox_event is None


@pytest.mark.asyncio
async def test_idempotent_request_no_duplicate_outbox(
    db_session: AsyncSession, job_service: JobService
):
    """
    Test that idempotent requests return existing job without creating duplicate events.

    Verifies:
    - Same job returned for duplicate idempotency key
    - Only ONE outbox event created for the job
    """
    idempotency_key = f"unique-key-{uuid4()}"

    job_data = JobCreate(
        job_type="idempotent_task",
        payload={"data": "test"},
        priority=5,
        idempotency_key=idempotency_key,
    )

    # First request - creates job and outbox event
    job1 = await job_service.create_job(job_data)
    await db_session.commit()

    # Count outbox events for this job
    count_result1 = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job1.id))
    events_after_first = list(count_result1.scalars().all())
    assert len(events_after_first) == 1

    # Second request with same idempotency key
    job2 = await job_service.create_job(job_data)
    await db_session.commit()

    # Verify same job returned
    assert job1.id == job2.id

    # Verify still only ONE outbox event (no duplicate)
    count_result2 = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job1.id))
    events_after_second = list(count_result2.scalars().all())
    assert len(events_after_second) == 1

    # Verify event attributes
    event = events_after_second[0]
    assert event.event_type == "job_created"
    assert event.published is False


@pytest.mark.asyncio
async def test_critical_priority_preserved(db_session: AsyncSession, job_service: JobService):
    """
    Test that priority 0 (CRITICAL) is preserved in both job and outbox.

    Verifies the dispatcher will receive correct priority for publishing.
    """
    job_data = JobCreate(
        job_type="critical_task",
        payload={"action": "urgent"},
        priority=JobPriority.CRITICAL.value,  # 0
    )

    # Create job
    job = await job_service.create_job(job_data)
    await db_session.commit()

    # Verify job priority
    assert job.priority == 0

    # Verify outbox event exists
    outbox_result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job.id))
    outbox_event = outbox_result.scalar_one_or_none()
    assert outbox_event is not None

    # Verify job details accessible for dispatcher
    job_result = await db_session.execute(select(Job).where(Job.id == job.id))
    persisted_job = job_result.scalar_one_or_none()
    assert persisted_job.priority == 0


@pytest.mark.asyncio
async def test_default_priority_job_with_outbox(db_session: AsyncSession, job_service: JobService):
    """
    Test that jobs using default priority work with outbox pattern.

    When priority is omitted, JobCreate uses its default value.
    Dispatcher will use the job's actual priority when publishing.
    """
    job_data = JobCreate(
        job_type="default_priority_task",
        payload={"data": "test"},
        # priority omitted - uses JobCreate default
    )

    # Create job
    job = await job_service.create_job(job_data)
    await db_session.commit()

    # Verify job has a priority (JobCreate default)
    assert job.priority is not None

    # Verify outbox event exists
    outbox_result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job.id))
    outbox_event = outbox_result.scalar_one_or_none()
    assert outbox_event is not None
    assert outbox_event.event_type == "job_created"
    assert outbox_event.published is False


@pytest.mark.asyncio
async def test_multiple_jobs_multiple_outbox_events(
    db_session: AsyncSession, job_service: JobService
):
    """
    Test that multiple jobs each get their own outbox event.

    Verifies one-to-one relationship between jobs and outbox events.
    """
    jobs_data = [
        JobCreate(job_type=f"task_{i}", payload={"index": i}, priority=i) for i in range(3)
    ]

    created_jobs = []
    for job_data in jobs_data:
        job = await job_service.create_job(job_data)
        created_jobs.append(job)

    await db_session.commit()

    # Verify each job has exactly one outbox event
    for job in created_jobs:
        outbox_result = await db_session.execute(
            select(JobOutbox).where(JobOutbox.job_id == job.id)
        )
        events = list(outbox_result.scalars().all())
        assert len(events) == 1
        assert events[0].event_type == "job_created"
        assert events[0].published is False


@pytest.mark.asyncio
async def test_job_status_is_queued(db_session: AsyncSession, job_service: JobService):
    """
    Test that newly created jobs have QUEUED status.

    Maintains API contract even though Redis enqueueing is now asynchronous.
    """
    job_data = JobCreate(
        job_type="status_test",
        payload={"data": "test"},
        priority=5,
    )

    job = await job_service.create_job(job_data)
    await db_session.commit()

    # Verify status is QUEUED (API contract)
    assert job.status == JobStatus.QUEUED

    # Refresh from database
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED


@pytest.mark.asyncio
async def test_outbox_event_fields_correct(db_session: AsyncSession, job_service: JobService):
    """
    Test that outbox event has correct initial field values.

    Verifies dispatcher will process the event correctly.
    """
    job_data = JobCreate(
        job_type="field_test",
        payload={"data": "test"},
        priority=5,
    )

    job = await job_service.create_job(job_data)
    await db_session.commit()

    # Get outbox event
    outbox_result = await db_session.execute(select(JobOutbox).where(JobOutbox.job_id == job.id))
    event = outbox_result.scalar_one()

    # Verify all fields
    assert event.id is not None  # UUID generated
    assert event.job_id == job.id
    assert event.event_type == "job_created"
    assert event.published is False
    assert event.attempts == 0
    assert event.created_at is not None
    assert event.published_at is None
    assert event.last_error is None


@pytest.mark.asyncio
async def test_no_redis_enqueue_on_create(
    db_session: AsyncSession, job_service: JobService, mock_queue_service
):
    """
    Test that Redis is NOT called during job creation.

    Verifies that direct enqueueing has been removed and dispatcher
    will handle publication asynchronously.
    """
    job_data = JobCreate(
        job_type="no_redis_test",
        payload={"data": "test"},
        priority=5,
    )

    # Create job
    job = await job_service.create_job(job_data)
    await db_session.commit()

    # Job created successfully
    assert job.id is not None

    # Explicitly verify queue_service.enqueue_job was NOT called
    mock_queue_service.enqueue_job.assert_not_called()
