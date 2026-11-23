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
from datetime import datetime, timedelta
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

        # Verify event is NOT published, has backoff set, and error is recorded
        await db_session.refresh(event)
        assert event.published is False
        assert event.published_at is None
        assert event.last_error == "Redis publication failed"
        assert event.attempts == 1
        assert event.next_publication_attempt_at is not None  # Backoff set

        # Second attempt should NOT process (backoff not expired yet)
        processed = await dispatcher.process_batch()
        assert processed == 0

        # Clear backoff to make it eligible again
        event.next_publication_attempt_at = None
        await db_session.commit()

        # Now it should be processed again
        processed = await dispatcher.process_batch()
        assert processed == 0

        await db_session.refresh(event)
        assert event.attempts == 2
        assert event.published is False


@pytest.mark.asyncio
async def test_missing_job_leaves_event_unpublished(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that missing job is marked as permanent failure"""
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

        # Event NOT successfully published (reconciliation required)
        assert processed == 0

        # Verify event is marked for reconciliation (not published)
        await db_session.refresh(event)
        assert event.published is False  # NOT marked published (never delivered to Redis)
        assert event.published_at is None
        assert event.reconciliation_required is True  # Marked for manual review
        assert f"Job {non_existent_job_id} not found" in event.last_error
        assert event.attempts == 1

        # Event should remain available for audit/reconciliation
        stmt = select(JobOutbox).where(JobOutbox.id == event.id)
        result = await db_session.execute(stmt)
        found_event = result.scalar_one_or_none()
        assert found_event is not None
        assert found_event.reconciliation_required is True  # Needs reconciliation


@pytest.mark.asyncio
async def test_exhausted_events_excluded_from_queue(
    db_session: AsyncSession, dispatcher, mock_queue_service
):
    """Test that events in backoff don't block newer events"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        job1 = Job(
            id=uuid4(),
            job_type="backoff_task",
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

        # Event in backoff (next_publication_attempt_at in future)
        backoff_event = JobOutbox(
            id=uuid4(),
            job_id=job1.id,
            event_type="job_created",
            published=False,
            attempts=5,
            next_publication_attempt_at=datetime.utcnow() + timedelta(seconds=60),  # In backoff
        )

        # New event (eligible immediately)
        new_event = JobOutbox(
            id=uuid4(),
            job_id=job2.id,
            event_type="job_created",
            published=False,
            attempts=0,
            next_publication_attempt_at=None,  # Eligible
        )

        db_session.add_all([backoff_event, new_event])
        await db_session.commit()

        # Process batch should skip backoff event and process new event
        processed = await dispatcher.process_batch()

        assert processed == 1

        # New event should be published
        await db_session.refresh(new_event)
        assert new_event.published is True

        # Backoff event remains unchanged (excluded from query due to future backoff)
        await db_session.refresh(backoff_event)
        assert backoff_event.published is False
        assert backoff_event.attempts == 5


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
            attempts=5,
            next_publication_attempt_at=datetime.utcnow() + timedelta(seconds=60),  # In backoff
            last_error="Previous failures",
        )
        db_session.add(event)
        await db_session.commit()

        # Without bypass, should still process but might fail due to other reasons
        # process_single_event doesn't check eligibility, just locks the event
        success = await dispatcher.process_single_event(event.id, bypass_backoff=False)
        # Should succeed since Redis will work
        assert success is True

        await db_session.refresh(event)
        assert event.published is True
        assert event.attempts == 6  # Incremented


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

        success = await dispatcher.process_single_event(event.id, bypass_backoff=False)
        assert success is True

        mock_queue_service.enqueue_job.assert_not_called()


@pytest.mark.asyncio
async def test_process_single_event_not_found(dispatcher):
    """Test process_single_event with non-existent event ID"""
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        non_existent_id = uuid4()

        success = await dispatcher.process_single_event(non_existent_id, bypass_backoff=False)
        assert success is False


# ============================================================================
# Durable Recovery Event Tests
# ============================================================================


@pytest.mark.asyncio
async def test_recovery_event_publishes_with_persisted_state(
    db_session: AsyncSession, mock_queue_service
):
    """Test job_recovered event uses persisted recovery state"""
    # Create job in RETRYING status with persisted recovery state
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=1,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    # Create recovery event with matching retry_attempt
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock Redis delayed queue
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        assert processed == 1

    # Verify Redis call used persisted next_retry_at
    mock_redis.push_to_delayed_queue.assert_called_once()
    call_args = mock_redis.push_to_delayed_queue.call_args
    assert call_args[1]["execute_at"] == job.next_retry_at.timestamp()

    # Verify job_data includes retry_attempt for deduplication
    job_data = call_args[1]["job_data"]
    assert job_data["job_id"] == str(job.id)
    assert job_data["retry_attempt"] == 1

    # Verify job state unchanged
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.retry_count == 1
    assert job.next_retry_at == datetime(2026, 11, 20, 10, 0, 0)

    # Verify event marked published
    await db_session.refresh(event)
    assert event.published is True


@pytest.mark.asyncio
async def test_recovery_event_stale_retry_attempt_not_published(
    db_session: AsyncSession, mock_queue_service
):
    """Test stale recovery event (mismatched retry_attempt) not published to Redis"""
    # Create job at retry_count=2
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=2,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    # Create stale event with retry_attempt=1 (job is now at 2)
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,  # Stale
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        assert processed == 0  # NOT successfully published

    # Verify Redis NOT called
    mock_redis.push_to_delayed_queue.assert_not_called()

    # Verify event marked for reconciliation (not published - never delivered)
    await db_session.refresh(event)
    assert event.published is False  # NOT published (never reached Redis)
    assert event.reconciliation_required is True  # Marked for manual review
    assert "Stale event" in event.last_error


@pytest.mark.asyncio
async def test_recovery_event_null_retry_attempt_rejected(
    db_session: AsyncSession, mock_queue_service
):
    """Test recovery event with NULL retry_attempt (legacy) rejected"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=1,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    # Create legacy event without retry_attempt
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=None,  # Legacy - NULL
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        assert processed == 0  # NOT successfully published

    # Verify Redis NOT called
    mock_redis.push_to_delayed_queue.assert_not_called()

    # Verify event marked for reconciliation (not published - never delivered)
    await db_session.refresh(event)
    assert event.published is False  # NOT published (never reached Redis)
    assert event.reconciliation_required is True  # Marked for manual review
    assert "NULL retry_attempt" in event.last_error


@pytest.mark.asyncio
async def test_recovery_event_job_not_retrying_rejected(
    db_session: AsyncSession, mock_queue_service
):
    """Test recovery event rejected if job not in RETRYING status"""
    # Job in PROCESSING status (not RETRYING)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,  # Wrong status
        retry_count=1,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        assert processed == 0

    # Verify Redis NOT called
    mock_redis.push_to_delayed_queue.assert_not_called()

    # Verify event NOT marked published
    await db_session.refresh(event)
    assert event.published is False
    assert "expected RETRYING" in event.last_error


@pytest.mark.asyncio
async def test_recovery_event_redis_failure_preserves_retry_intent(
    db_session: AsyncSession, mock_queue_service
):
    """Test Redis failure leaves event unpublished for retry"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=1,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock Redis failure
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=False)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        assert processed == 0

    # Verify event NOT marked published and eligible for retry
    await db_session.refresh(event)
    assert event.published is False
    assert event.attempts == 1  # Attempt incremented


@pytest.mark.asyncio
async def test_recovery_event_duplicate_publication_updates_same_member(
    db_session: AsyncSession, mock_queue_service
):
    """Test repeated publication of same event updates same Redis member"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=1,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # First publication
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        await dispatcher.process_batch()

    first_call = mock_redis.push_to_delayed_queue.call_args

    # Reset event to unpublished (simulate crash after Redis but before commit)
    await db_session.refresh(event)
    event.published = False
    event.attempts = 0
    await db_session.commit()

    # Second publication (duplicate)
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        await dispatcher.process_batch()

    second_call = mock_redis.push_to_delayed_queue.call_args

    # Verify both calls have identical job_data (same member)
    assert first_call[1]["job_data"] == second_call[1]["job_data"]
    assert first_call[1]["job_data"]["retry_attempt"] == 1
    assert first_call[1]["execute_at"] == second_call[1]["execute_at"]


@pytest.mark.asyncio
async def test_recovery_event_different_retry_attempts_different_members(
    db_session: AsyncSession, mock_queue_service
):
    """Test different retry attempts create different Redis members"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=2,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    # Create events for retry_attempt 1 and 2
    event1 = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,
        published=False,
        attempts=0,
    )
    event2 = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=2,
        published=False,
        attempts=0,
    )
    db_session.add(event1)
    db_session.add(event2)
    await db_session.commit()

    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        # event1 is stale (retry_attempt=1, job.retry_count=2), marked for reconciliation
        # event2 matches, published to Redis
        assert processed == 1  # Only event2 successfully published

    # Verify event1 (stale) NOT published to Redis
    # Verify event2 (current) published to Redis
    assert mock_redis.push_to_delayed_queue.call_count == 1

    # Get the call for event2
    call_args = mock_redis.push_to_delayed_queue.call_args
    assert call_args[1]["job_data"]["retry_attempt"] == 2

    # Verify event1 marked for reconciliation
    await db_session.refresh(event1)
    assert event1.published is False
    assert event1.reconciliation_required is True

    # Verify event2 successfully published
    await db_session.refresh(event2)
    assert event2.published is True
    assert event2.reconciliation_required is False


@pytest.mark.asyncio
async def test_job_created_events_still_work(db_session: AsyncSession, mock_queue_service):
    """Test existing job_created events continue working normally"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.PENDING,
        priority=5,
    )
    db_session.add(job)

    # job_created event (no retry_attempt)
    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        retry_attempt=None,  # NULL for job_created
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()
        assert processed == 1

    # Verify enqueued to main queue
    mock_queue_service.enqueue_job.assert_called_once_with(
        job_id=job.id,
        job_type=job.job_type,
        priority=5,
    )

    # Verify event marked published
    await db_session.refresh(event)
    assert event.published is True


# ============================================================================
# Exponential Backoff and Retry Exhaustion Tests
# ============================================================================


@pytest.mark.asyncio
async def test_redis_failure_sets_exponential_backoff(db_session: AsyncSession, mock_queue_service):
    """Test that Redis failure sets exponential backoff, not hard limit"""
    # Create job and event
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock Redis failure
    mock_queue_service.enqueue_job = AsyncMock(return_value=False)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify not published
    assert processed == 0

    # Verify backoff set
    await db_session.refresh(event)
    assert event.published is False
    assert event.attempts == 1
    assert event.next_publication_attempt_at is not None
    assert event.last_error == "Redis publication failed"

    # Verify backoff is approximately 1 second (first attempt)
    backoff_seconds = (event.next_publication_attempt_at - datetime.utcnow()).total_seconds()
    assert 0.5 < backoff_seconds < 2.0  # Allow some tolerance


@pytest.mark.asyncio
async def test_event_remains_eligible_after_many_failures(
    db_session: AsyncSession, mock_queue_service
):
    """Test that events remain eligible even after more than 5 failures"""
    # Create job and event
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add(job)

    # Create event with 10 attempts already (would be exhausted under old max_attempts=5)
    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=10,
        next_publication_attempt_at=None,  # Eligible now
    )
    db_session.add(event)
    await db_session.commit()

    # Mock Redis failure
    mock_queue_service.enqueue_job = AsyncMock(return_value=False)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify event was processed (not excluded due to attempt count)
    assert processed == 0

    # Verify attempt incremented and backoff set
    await db_session.refresh(event)
    assert event.attempts == 11  # Incremented
    assert event.next_publication_attempt_at is not None
    assert event.published is False  # Still eligible for future retry


@pytest.mark.asyncio
async def test_backoff_increases_and_is_capped(db_session: AsyncSession, mock_queue_service):
    """Test that backoff doubles each time and caps at 5 minutes"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock Redis failure
    mock_queue_service.enqueue_job = AsyncMock(return_value=False)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # Test first 3 failures
    expected_backoffs = [(1, 0.5, 3), (2, 1, 4), (4, 2, 8)]  # (expected, min, max) with tolerance

    for i, (expected, min_val, max_val) in enumerate(expected_backoffs):
        # Make event eligible
        event.next_publication_attempt_at = None
        await db_session.commit()

        with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
            await dispatcher.process_batch()

        await db_session.refresh(event)
        actual_backoff = (event.next_publication_attempt_at - datetime.utcnow()).total_seconds()

        # Wide tolerance for test execution time variability
        assert (
            min_val <= actual_backoff <= max_val
        ), f"Attempt {i + 1}: expected ~{expected}s (range {min_val}-{max_val}), got {actual_backoff}s"

    # Test that backoff caps at 300 seconds (5 minutes)
    # After 9 attempts: 2^9 = 512 seconds, but should cap at 300
    event.attempts = 9
    event.next_publication_attempt_at = None
    await db_session.commit()

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        await dispatcher.process_batch()

    await db_session.refresh(event)
    actual_backoff = (event.next_publication_attempt_at - datetime.utcnow()).total_seconds()

    # Should be capped at 300 seconds
    assert 299 < actual_backoff < 301


@pytest.mark.asyncio
async def test_event_retried_after_redis_recovery(db_session: AsyncSession, mock_queue_service):
    """Test that event is retried after Redis recovers"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add(job)

    # Event with backoff in the past (ready for retry)
    past_time = datetime.utcnow() - timedelta(seconds=10)
    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=3,
        next_publication_attempt_at=past_time,  # Backoff expired
        last_error="Redis publication failed",
    )
    db_session.add(event)
    await db_session.commit()

    # Mock Redis success (recovered)
    mock_queue_service.enqueue_job = AsyncMock(return_value=True)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify successful publication
    assert processed == 1

    await db_session.refresh(event)
    assert event.published is True
    assert event.next_publication_attempt_at is None  # Backoff cleared
    assert event.last_error is None  # Error cleared


@pytest.mark.asyncio
async def test_invalid_legacy_event_marked_permanent_failure(
    db_session: AsyncSession, mock_queue_service
):
    """Test that legacy recovery event with NULL retry_attempt is marked for reconciliation"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=1,
        next_retry_at=datetime.utcnow() + timedelta(seconds=60),
        priority=5,
    )
    db_session.add(job)

    # Legacy recovery event without retry_attempt
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=None,  # NULL - legacy event
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify marked for reconciliation (NOT published - never delivered)
    assert processed == 0

    await db_session.refresh(event)
    assert event.published is False  # NOT published
    assert event.reconciliation_required is True  # Marked for manual review
    assert event.last_error == "job_recovered event has NULL retry_attempt (legacy event)"
    # next_publication_attempt_at is cleared when reconciliation_required is set


@pytest.mark.asyncio
async def test_concurrent_dispatchers_no_duplicate_publication(
    db_session: AsyncSession, mock_queue_service
):
    """Test that concurrent dispatchers don't publish the same event twice"""
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Create job and event
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Create two independent sessions
    TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    # Track which dispatcher published
    published_by = []

    async def dispatcher_process(dispatcher_id):
        """Simulates a dispatcher instance"""
        mock_service = MagicMock(spec=QueueService)
        mock_service.enqueue_job = AsyncMock(return_value=True)

        dispatcher = OutboxDispatcher(mock_service, batch_size=10)

        # Override AsyncSessionLocal to use test database
        with patch("app.services.outbox_dispatcher.AsyncSessionLocal", SessionFactory):
            processed = await dispatcher.process_batch()
            if processed > 0:
                published_by.append(dispatcher_id)
            return processed

    try:
        # Run two dispatchers concurrently
        results = await asyncio.gather(
            dispatcher_process("dispatcher-1"),
            dispatcher_process("dispatcher-2"),
            return_exceptions=False,
        )

        # Verify exactly one dispatcher published the event
        total_processed = sum(results)
        assert total_processed == 1, f"Expected 1 publication, got {total_processed}"
        assert len(published_by) == 1, f"Expected 1 dispatcher to publish, got {len(published_by)}"

        # Verify event is published
        await db_session.refresh(event)
        assert event.published is True
        assert event.attempts == 1

    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_job_created_publication_still_works(db_session: AsyncSession, mock_queue_service):
    """Test that standard job_created events still work correctly"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=3,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock successful enqueue
    mock_queue_service.enqueue_job = AsyncMock(return_value=True)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    assert processed == 1

    # Verify enqueued correctly
    mock_queue_service.enqueue_job.assert_called_once_with(
        job_id=job.id,
        job_type="test_task",
        priority=3,
    )

    # Verify event published
    await db_session.refresh(event)
    assert event.published is True
    assert event.last_error is None


@pytest.mark.asyncio
async def test_job_recovered_retry_attempt_unchanged(db_session: AsyncSession, mock_queue_service):
    """Test that job_recovered event preserves immutable retry_attempt"""
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=2,
        next_retry_at=datetime(2026, 11, 20, 10, 0, 0),
        priority=5,
    )
    db_session.add(job)

    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=2,  # Immutable identifier
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    original_retry_attempt = event.retry_attempt

    # Mock Redis
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    assert processed == 1

    # Verify retry_attempt unchanged
    await db_session.refresh(event)
    assert event.retry_attempt == original_retry_attempt
    assert event.published is True

    # Verify Redis received correct retry_attempt
    call_args = mock_redis.push_to_delayed_queue.call_args
    assert call_args[1]["job_data"]["retry_attempt"] == 2
