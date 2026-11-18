"""Tests for outbox dispatcher

Test Database Isolation Strategy:
====================================
PROBLEM: OutboxDispatcher imports AsyncSessionLocal from app.core.database,
which connects to the production database URL from settings.

SOLUTION: Patch AsyncSessionLocal with TestSessionLocal before each test.
This ensures all dispatcher sessions use the isolated test database
(jobprocessing_test) rather than the development database.

Database Configuration:
- Test database: postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test
- Dev database: From settings.DATABASE_URL (never touched by tests)
- TestSessionLocal from conftest.py creates sessions for test database
- All tests patch app.services.outbox_dispatcher.AsyncSessionLocal with TestSessionLocal

Verification:
- db_session fixture creates/drops tables in test database only
- Dispatcher sessions patched to use TestSessionLocal
- Each test is isolated - tables dropped after test completes
"""

import asyncio
from datetime import datetime
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobOutbox, JobStatus, JobPriority
from app.services.outbox_dispatcher import OutboxDispatcher
from app.services.queue_service import QueueService

# Import test session factory from conftest
from tests.conftest import TestSessionLocal


@pytest.fixture
def mock_queue_service():
    """
    Mock queue service for testing.

    Non-async fixture since we're just creating mock objects.
    """
    queue_service = MagicMock(spec=QueueService)
    queue_service.enqueue_job = AsyncMock(return_value=True)
    return queue_service


@pytest.fixture
def dispatcher(mock_queue_service):
    """
    Create dispatcher instance with test database session.

    Non-async fixture since we're just instantiating the dispatcher.
    The dispatcher will be patched to use TestSessionLocal in each test.
    """
    return OutboxDispatcher(
        queue_service=mock_queue_service,
        batch_size=10,
        poll_interval_seconds=0.1,
        max_attempts=5,
    )


@pytest.mark.asyncio
async def test_publish_critical_priority_job(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that priority 0 (CRITICAL) is preserved"""
    # Patch dispatcher to use test database
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        # Create a critical priority job
        job = Job(
            id=uuid4(),
            job_type="critical_task",
            payload={"action": "emergency"},
            priority=JobPriority.CRITICAL.value,  # 0
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        # Create outbox event
        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        # Process the event
        processed = await dispatcher.process_batch()

        assert processed == 1
        mock_queue_service.enqueue_job.assert_called_once_with(
            job_id=job.id,
            job_type="critical_task",
            priority=0,  # Priority 0 must be preserved
        )

        # Verify event is marked as published
        await db_session.refresh(event)
        assert event.published is True
        assert event.published_at is not None
        assert event.last_error is None


@pytest.mark.asyncio
async def test_publish_job_with_null_priority(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that null priority defaults to 5"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="normal_task",
            payload={"action": "process"},
            priority=None,  # Nullable priority
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        processed = await dispatcher.process_batch()

        assert processed == 1
        mock_queue_service.enqueue_job.assert_called_once_with(
            job_id=job.id,
            job_type="normal_task",
            priority=5,  # Defaults to 5 when None
        )


@pytest.mark.asyncio
async def test_redis_enqueue_failure_retries_event(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that Redis failures leave event unpublished for retry"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="retry_task",
            payload={"action": "test"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        # Simulate Redis failure
        mock_queue_service.enqueue_job.return_value = False

        processed = await dispatcher.process_batch()

        # Event not successfully published
        assert processed == 0

        # Verify event is NOT published and error is recorded
        await db_session.refresh(event)
        assert event.published is False
        assert event.published_at is None
        assert event.last_error == "Redis enqueue failed"
        assert event.attempts == 1

        # Second attempt should also fail
        processed = await dispatcher.process_batch()
        assert processed == 0

        await db_session.refresh(event)
        assert event.attempts == 2
        assert event.published is False


@pytest.mark.asyncio
async def test_missing_job_leaves_event_unpublished(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that missing job leaves event unpublished for investigation"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        non_existent_job_id = uuid4()

        event = JobOutbox(
            id=uuid4(),
            job_id=non_existent_job_id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        processed = await dispatcher.process_batch()

        # Event not successfully published
        assert processed == 0

        # Verify event is NOT published and error is recorded
        await db_session.refresh(event)
        assert event.published is False
        assert event.published_at is None
        assert f"Job {non_existent_job_id} not found" in event.last_error
        assert event.attempts == 1

        # Event should remain available for investigation
        stmt = select(JobOutbox).where(JobOutbox.id == event.id)
        result = await db_session.execute(stmt)
        found_event = result.scalar_one_or_none()
        assert found_event is not None
        assert found_event.published is False


@pytest.mark.asyncio
async def test_exhausted_events_excluded_from_queue(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that exhausted events don't block newer events"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job1 = Job(
            id=uuid4(),
            job_type="exhausted_task",
            payload={"action": "old"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        job2 = Job(
            id=uuid4(),
            job_type="new_task",
            payload={"action": "new"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add_all([job1, job2])

        # Exhausted event (attempts >= max_attempts)
        exhausted_event = JobOutbox(
            id=uuid4(),
            job_id=job1.id,
            event_type="job_created",
            published=False,
            attempts=5,  # At max_attempts
        )

        # New event
        new_event = JobOutbox(
            id=uuid4(),
            job_id=job2.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )

        db_session.add_all([exhausted_event, new_event])
        await db_session.commit()

        # Process batch should skip exhausted event and process new event
        processed = await dispatcher.process_batch()

        assert processed == 1

        # New event should be published
        await db_session.refresh(new_event)
        assert new_event.published is True

        # Exhausted event remains unchanged (excluded from query)
        await db_session.refresh(exhausted_event)
        assert exhausted_event.published is False
        assert exhausted_event.attempts == 5


@pytest.mark.asyncio
async def test_manual_recovery_with_bypass(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test manual recovery of exhausted events with bypass flag"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="recovered_task",
            payload={"action": "recover"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        # Exhausted event
        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=5,  # At max_attempts
            last_error="Previous failures",
        )
        db_session.add(event)
        await db_session.commit()

        # Without bypass, should fail (max_attempts check blocks processing)
        success = await dispatcher.process_single_event(event.id, bypass_max_attempts=False)
        assert success is False

        await db_session.refresh(event)
        assert event.published is False
        # Without bypass, max_attempts blocks _publish_event, attempts stays 5
        assert event.attempts == 5

        # With bypass, should succeed
        mock_queue_service.enqueue_job.reset_mock()
        success = await dispatcher.process_single_event(event.id, bypass_max_attempts=True)
        assert success is True

        await db_session.refresh(event)
        assert event.published is True
        assert event.published_at is not None
        assert event.last_error is None
        # With bypass, one attempt increments: 5 + 1 = 6
        assert event.attempts == 6


@pytest.mark.asyncio
async def test_concurrent_dispatchers_with_row_locking(
    db_session: AsyncSession, mock_queue_service
):
    """
    Test that multiple dispatchers with overlapping transactions don't duplicate process.

    Uses separate sessions with FOR UPDATE SKIP LOCKED to verify actual
    PostgreSQL row-locking behavior.
    """
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="concurrent_task",
            payload={"action": "test"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        # Track which dispatcher processed the event
        call_tracker = []

        async def track_enqueue_call(dispatcher_id: int):
            """Mock that tracks which dispatcher called enqueue_job"""

            async def tracked_enqueue(*args, **kwargs):
                call_tracker.append(dispatcher_id)
                # Add small delay to ensure transactions overlap
                await asyncio.sleep(0.01)
                return True

            return tracked_enqueue

        # Create two dispatchers with separate mocks
        mock_service_1 = MagicMock(spec=QueueService)
        mock_service_1.enqueue_job = AsyncMock(side_effect=await track_enqueue_call(1))
        dispatcher1 = OutboxDispatcher(queue_service=mock_service_1, batch_size=10)

        mock_service_2 = MagicMock(spec=QueueService)
        mock_service_2.enqueue_job = AsyncMock(side_effect=await track_enqueue_call(2))
        dispatcher2 = OutboxDispatcher(queue_service=mock_service_2, batch_size=10)

        # Run both concurrently
        results = await asyncio.gather(
            dispatcher1.process_batch(),
            dispatcher2.process_batch(),
        )

        # Exactly one should process (SKIP LOCKED prevents duplication)
        total_processed = sum(results)
        assert total_processed == 1
        assert len(call_tracker) == 1

        # Verify event is published exactly once
        await db_session.refresh(event)
        assert event.published is True
        assert event.attempts == 1


@pytest.mark.asyncio
async def test_database_commit_failure_after_redis_success(
    db_session: AsyncSession, mock_queue_service
):
    """
    Test commit failure after Redis success causes event to remain unpublished.

    Demonstrates at-least-once delivery:
    - Redis publication succeeds
    - Database commit fails, causing rollback
    - Event remains unpublished with attempts=0 (rollback undoes changes)
    - Retry will cause duplicate Redis publication
    """
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="commit_failure_task",
            payload={"action": "test"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        # Create dispatcher
        dispatcher = OutboxDispatcher(queue_service=mock_queue_service, batch_size=10)

        # Mock commit to fail
        async def failing_commit(self):
            """Simulate database commit failure"""
            raise RuntimeError("Database connection lost during commit")

        with pytest.raises(RuntimeError, match="Database connection lost"):
            with patch.object(AsyncSession, "commit", new=failing_commit):
                await dispatcher.process_batch()

        # Redis was called successfully
        mock_queue_service.enqueue_job.assert_called_once()

        # After rollback, event remains unchanged
        await db_session.refresh(event)
        assert event.published is False
        assert event.attempts == 0  # Rollback undoes the increment

        # Retry causes duplicate Redis publication
        mock_queue_service.enqueue_job.reset_mock()
        processed = await dispatcher.process_batch()

        assert processed == 1
        assert mock_queue_service.enqueue_job.call_count == 1

        # After successful retry
        await db_session.refresh(event)
        assert event.published is True
        assert event.attempts == 1  # First successful attempt


@pytest.mark.asyncio
async def test_multiple_events_batch_processing(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test batch processing of multiple events"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        jobs = []
        events = []

        for i in range(5):
            job = Job(
                id=uuid4(),
                job_type=f"batch_task_{i}",
                payload={"index": i},
                priority=i,
                status=JobStatus.QUEUED,
            )
            jobs.append(job)

            event = JobOutbox(
                id=uuid4(),
                job_id=job.id,
                event_type="job_created",
                published=False,
                attempts=0,
            )
            events.append(event)

        db_session.add_all(jobs)
        db_session.add_all(events)
        await db_session.commit()

        processed = await dispatcher.process_batch()

        assert processed == 5

        for event in events:
            await db_session.refresh(event)
            assert event.published is True
            assert event.attempts == 1


@pytest.mark.asyncio
async def test_exception_during_publish_records_error(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that exceptions during publication are recorded"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="exception_task",
            payload={"action": "test"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=False,
            attempts=0,
        )
        db_session.add(event)
        await db_session.commit()

        # Simulate exception
        mock_queue_service.enqueue_job.side_effect = RuntimeError("Redis connection lost")

        processed = await dispatcher.process_batch()

        assert processed == 0

        await db_session.refresh(event)
        assert event.published is False
        assert "RuntimeError: Redis connection lost" in event.last_error
        assert event.attempts == 1


@pytest.mark.asyncio
async def test_already_published_event_skipped(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that already published events are skipped"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job = Job(
            id=uuid4(),
            job_type="already_published_task",
            payload={"action": "test"},
            priority=5,
            status=JobStatus.QUEUED,
        )
        db_session.add(job)

        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_created",
            published=True,  # Already published
            published_at=datetime.utcnow(),
            attempts=1,
        )
        db_session.add(event)
        await db_session.commit()

        success = await dispatcher.process_single_event(event.id, bypass_max_attempts=False)
        assert success is True

        mock_queue_service.enqueue_job.assert_not_called()


@pytest.mark.asyncio
async def test_process_single_event_not_found(dispatcher):
    """Test process_single_event with non-existent event ID"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        non_existent_id = uuid4()

        success = await dispatcher.process_single_event(non_existent_id, bypass_max_attempts=False)
        assert success is False
