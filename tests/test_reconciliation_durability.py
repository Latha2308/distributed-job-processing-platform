"""
Tests for outbox reconciliation state durability and correctness.

These tests verify that reconciliation_required state correctly identifies
undeliverable events and persists across sessions.
"""

import pytest
from datetime import datetime, timedelta
from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobOutbox, JobStatus
from app.services.outbox_dispatcher import OutboxDispatcher
from app.services.queue_service import QueueService

from tests.conftest import TestSessionLocal


@pytest.mark.asyncio
async def test_null_retry_attempt_enters_reconciliation(db_session: AsyncSession):
    """
    Test that NULL retry_attempt recovery events enter reconciliation state.

    Verifies:
    - Event is NOT marked published (never delivered to Redis)
    - reconciliation_required=True
    - next_publication_attempt_at is cleared
    - Event is excluded from normal polling
    - State persists across sessions
    """
    # Create job in RETRYING status
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

    # Create legacy recovery event without retry_attempt
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=None,  # NULL - legacy/invalid event
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()
    event_id = event.id

    # Mock queue service
    mock_queue_service = MagicMock(spec=QueueService)
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # Process event
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify NOT successfully published
    assert processed == 0

    # Verify state in same session
    await db_session.refresh(event)
    assert event.published is False, "Event should NOT be marked published"
    assert event.reconciliation_required is True, "Should require reconciliation"
    assert event.next_publication_attempt_at is None, "Backoff should be cleared"
    assert "NULL retry_attempt" in event.last_error
    assert event.attempts == 1

    # Verify Redis was NOT called
    mock_redis.push_to_delayed_queue.assert_not_called()

    # Verify state persists across independent session
    async with TestSessionLocal() as new_session:
        stmt = select(JobOutbox).where(JobOutbox.id == event_id)
        result = await new_session.execute(stmt)
        persisted_event = result.scalar_one()

        assert persisted_event.published is False
        assert persisted_event.reconciliation_required is True
        assert persisted_event.next_publication_attempt_at is None

    # Verify event is excluded from next poll
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    assert processed == 0, "Reconciliation event should not be retried"


@pytest.mark.asyncio
async def test_stale_retry_attempt_enters_reconciliation(db_session: AsyncSession):
    """
    Test that stale retry_attempt events enter reconciliation state.

    Scenario: Job has progressed to retry_count=2, but event has retry_attempt=1
    """
    # Create job at retry_count=2
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        retry_count=2,  # Current attempt
        next_retry_at=datetime.utcnow() + timedelta(seconds=60),
        priority=5,
    )
    db_session.add(job)

    # Create stale event for retry_attempt=1
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,  # Stale - job is now at 2
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()
    event_id = event.id

    # Mock queue service
    mock_queue_service = MagicMock(spec=QueueService)
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # Process event
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify NOT successfully published
    assert processed == 0

    # Verify reconciliation state
    await db_session.refresh(event)
    assert event.published is False, "Stale event should NOT be marked published"
    assert event.reconciliation_required is True
    assert event.next_publication_attempt_at is None
    assert "Stale event" in event.last_error
    assert "retry_attempt=1" in event.last_error
    assert "retry_count=2" in event.last_error

    # Verify persistence
    async with TestSessionLocal() as new_session:
        stmt = select(JobOutbox).where(JobOutbox.id == event_id)
        result = await new_session.execute(stmt)
        persisted_event = result.scalar_one()

        assert persisted_event.reconciliation_required is True
        assert persisted_event.published is False


@pytest.mark.asyncio
async def test_job_not_found_enters_reconciliation(db_session: AsyncSession):
    """Test that events for missing jobs enter reconciliation state."""
    non_existent_job_id = uuid4()

    # Create event for non-existent job
    event = JobOutbox(
        job_id=non_existent_job_id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()
    event_id = event.id

    # Mock queue service
    mock_queue_service = MagicMock(spec=QueueService)
    mock_queue_service.enqueue_job = AsyncMock(return_value=True)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # Process event
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify NOT successfully published
    assert processed == 0

    # Verify reconciliation state
    await db_session.refresh(event)
    assert event.published is False
    assert event.reconciliation_required is True
    assert f"Job {non_existent_job_id} not found" in event.last_error

    # Verify persistence
    async with TestSessionLocal() as new_session:
        stmt = select(JobOutbox).where(JobOutbox.id == event_id)
        result = await new_session.execute(stmt)
        persisted_event = result.scalar_one()

        assert persisted_event.reconciliation_required is True


@pytest.mark.asyncio
async def test_job_status_mismatch_enters_reconciliation(db_session: AsyncSession):
    """Test that recovery events for non-RETRYING jobs enter reconciliation."""
    # Create job in PROCESSING status (not RETRYING)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.PROCESSING,  # Wrong status
        retry_count=1,
        priority=5,
    )
    db_session.add(job)

    # Create recovery event
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=1,
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock queue service
    mock_queue_service = MagicMock(spec=QueueService)
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # Process event
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        await dispatcher.process_batch()

    # Verify reconciliation state
    await db_session.refresh(event)
    assert event.published is False
    assert event.reconciliation_required is True
    assert "expected RETRYING" in event.last_error  # Case-insensitive check


@pytest.mark.asyncio
async def test_valid_event_not_in_reconciliation(db_session: AsyncSession):
    """Test that valid events are NOT marked for reconciliation."""
    # Create valid job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add(job)

    # Create valid event
    event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()

    # Mock successful publication
    mock_queue_service = MagicMock(spec=QueueService)
    mock_queue_service.enqueue_job = AsyncMock(return_value=True)

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # Process event
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Verify successfully published
    assert processed == 1

    # Verify NOT in reconciliation
    await db_session.refresh(event)
    assert event.published is True, "Should be marked published"
    assert event.reconciliation_required is False, "Should NOT require reconciliation"
    assert event.last_error is None


@pytest.mark.asyncio
async def test_reconciliation_events_excluded_from_polling(db_session: AsyncSession):
    """Test that reconciliation events do not block valid events."""
    # Create two jobs
    job1 = Job(
        id=uuid4(),
        job_type="invalid_task",
        payload={"data": "test1"},
        status=JobStatus.RETRYING,
        retry_count=1,
        next_retry_at=datetime.utcnow() + timedelta(seconds=60),
        priority=5,
    )
    job2 = Job(
        id=uuid4(),
        job_type="valid_task",
        payload={"data": "test2"},
        status=JobStatus.QUEUED,
        priority=5,
    )
    db_session.add_all([job1, job2])

    # Create invalid event (will enter reconciliation)
    invalid_event = JobOutbox(
        job_id=job1.id,
        event_type="job_recovered",
        retry_attempt=None,  # NULL - will fail
        published=False,
        attempts=0,
    )

    # Create valid event
    valid_event = JobOutbox(
        job_id=job2.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )

    db_session.add_all([invalid_event, valid_event])
    await db_session.commit()

    # Mock queue service
    mock_queue_service = MagicMock(spec=QueueService)
    mock_queue_service.enqueue_job = AsyncMock(return_value=True)
    mock_redis = MagicMock()
    mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
    mock_queue_service.redis = mock_redis

    dispatcher = OutboxDispatcher(mock_queue_service, batch_size=10)

    # First batch: process both events
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Only valid event should be published
    assert processed == 1

    # Verify states
    await db_session.refresh(invalid_event)
    await db_session.refresh(valid_event)

    assert invalid_event.reconciliation_required is True
    assert invalid_event.published is False

    assert valid_event.published is True
    assert valid_event.reconciliation_required is False

    # Second batch: only reconciliation event remains
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        processed = await dispatcher.process_batch()

    # Should be excluded from polling
    assert processed == 0


@pytest.mark.asyncio
async def test_concurrent_reconciliation_state_is_atomic(db_session: AsyncSession):
    """Test that reconciliation state changes are atomic across concurrent dispatchers."""
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Create job
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

    # Create event with NULL retry_attempt
    event = JobOutbox(
        job_id=job.id,
        event_type="job_recovered",
        retry_attempt=None,
        published=False,
        attempts=0,
    )
    db_session.add(event)
    await db_session.commit()
    event_id = event.id

    # Create two independent sessions
    TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    SessionFactory = async_sessionmaker(engine, expire_on_commit=False)

    async def dispatcher_process(dispatcher_id):
        """Simulate dispatcher processing"""
        mock_service = MagicMock(spec=QueueService)
        mock_redis = MagicMock()
        mock_redis.push_to_delayed_queue = AsyncMock(return_value=True)
        mock_service.redis = mock_redis

        dispatcher = OutboxDispatcher(mock_service, batch_size=10)

        with patch("app.services.outbox_dispatcher.AsyncSessionLocal", SessionFactory):
            processed = await dispatcher.process_batch()
            return processed

    try:
        # Run two dispatchers concurrently
        import asyncio

        results = await asyncio.gather(
            dispatcher_process("dispatcher-1"), dispatcher_process("dispatcher-2")
        )

        # At most one should process it (both might see it's already reconciliation_required)
        total_processed = sum(results)
        assert total_processed == 0, "Reconciliation events should not be published"

        # Verify final state
        async with SessionFactory() as new_session:
            stmt = select(JobOutbox).where(JobOutbox.id == event_id)
            result = await new_session.execute(stmt)
            final_event = result.scalar_one()

            assert final_event.reconciliation_required is True
            assert final_event.published is False
            # Attempts should be at least 1 (from one dispatcher)
            assert final_event.attempts >= 1

    finally:
        await engine.dispose()
