"""Tests for QUEUED-job reconciliation (Step 289B)

Tests cover:
1. Old QUEUED job creates one recovery outbox event
2. Fresh QUEUED job is ignored
3. PROCESSING, RETRYING, COMPLETED, and FAILED jobs are ignored
4. Existing unpublished event prevents duplicate recovery intent
5. Failed publication awaiting retry does not create another event
6. Published event permits bounded republication after throttle
7. Two concurrent reconcilers cannot create duplicate events
8. Failure before transaction commit leaves neither timestamp nor event
9. Successful commit persists both timestamp and event
10. Repeated scans respect the throttle
11. reconciliation_required does not permanently block eligible QUEUED job
12. Batch-size limits are enforced
"""

from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobOutbox, JobStatus
from app.services.job_recovery_service import JobRecoveryService


@pytest.fixture
def reconciliation_service(db_session: AsyncSession):
    """Create job recovery service for reconciliation"""
    return JobRecoveryService(db_session)


@pytest.fixture
def reconciliation_service_with_factory(db_session: AsyncSession):
    """Create job recovery service with session factory for batch operations"""
    from tests.conftest import TestSessionLocal

    return JobRecoveryService(db_session, session_factory=TestSessionLocal)


# Test 1: Old QUEUED job creates one recovery outbox event
@pytest.mark.asyncio
async def test_old_queued_job_creates_recovery_event(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that an old QUEUED job creates a job_queued_recovery event"""
    # Create a QUEUED job that is 15 minutes old (> 10 min threshold)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()

    # Reconcile the job
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True

    # Verify outbox event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    event = events[0]
    assert event.event_type == "job_queued_recovery"
    assert event.retry_attempt is None  # NULL for recovery events
    assert event.published is False
    assert event.attempts == 0

    # Verify throttling timestamp updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is not None
    assert job.status == JobStatus.QUEUED  # Status unchanged
    assert job.retry_count == 0  # Retry count unchanged


# Test 2: Fresh QUEUED job is ignored
@pytest.mark.asyncio
async def test_fresh_queued_job_ignored(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that a fresh QUEUED job (< 10 min) is not reconciled"""
    # Create a QUEUED job that is only 5 minutes old (< 10 min threshold)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=5),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()

    # Attempt to reconcile the job
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is False

    # Verify no outbox event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 0

    # Verify throttling timestamp NOT updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is None


# Test 3: Non-QUEUED jobs are ignored
@pytest.mark.asyncio
async def test_non_queued_jobs_ignored(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that PROCESSING, RETRYING, COMPLETED, and FAILED jobs are not reconciled"""
    # Create jobs in various non-QUEUED statuses, all old enough
    statuses = [JobStatus.PROCESSING, JobStatus.RETRYING, JobStatus.COMPLETED, JobStatus.FAILED]

    job_ids = []
    for status in statuses:
        job = Job(
            id=uuid4(),
            job_type="test_task",
            payload={"data": "test"},
            status=status,
            created_at=datetime.utcnow() - timedelta(minutes=15),
            retry_count=0,
            max_retries=3,
        )
        db_session.add(job)
        job_ids.append(job.id)

    await db_session.commit()

    # Attempt to reconcile each job
    for job_id in job_ids:
        success = await reconciliation_service.reconcile_queued_job(job_id)
        await db_session.commit()
        assert success is False

    # Verify no outbox events created
    for job_id in job_ids:
        stmt = select(JobOutbox).where(JobOutbox.job_id == job_id)
        result = await db_session.execute(stmt)
        events = result.scalars().all()
        assert len(events) == 0


# Test 4: Existing unpublished event prevents duplicate
@pytest.mark.asyncio
async def test_existing_unpublished_event_prevents_duplicate(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that an existing unpublished event prevents duplicate event creation"""
    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create an existing unpublished job_queued_recovery event
    existing_event = JobOutbox(
        job_id=job.id,
        event_type="job_queued_recovery",
        retry_attempt=None,
        published=False,
        attempts=0,
    )
    db_session.add(existing_event)
    await db_session.commit()

    # Attempt to reconcile the job
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert (
        success is True
    )  # Returns True because reconciliation completed (but skipped event creation)

    # Verify NO new event created (still only 1 event)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].id == existing_event.id  # Same event

    # Verify throttling timestamp WAS updated (even though no event created)
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is not None


# Test 5: Failed publication awaiting retry does not create another event
@pytest.mark.asyncio
async def test_failed_publication_awaiting_retry_no_duplicate(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that a failed publication awaiting retry does not create another event"""
    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create an existing unpublished event that has failed publication and is in backoff
    existing_event = JobOutbox(
        job_id=job.id,
        event_type="job_queued_recovery",
        retry_attempt=None,
        published=False,
        attempts=3,  # Failed 3 times
        next_publication_attempt_at=datetime.utcnow() + timedelta(minutes=5),  # Backoff not expired
        last_error="ConnectionError: Redis unavailable",
    )
    db_session.add(existing_event)
    await db_session.commit()

    # Attempt to reconcile the job
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True

    # Verify NO new event created (still only 1 event)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].id == existing_event.id

    # Verify throttling timestamp WAS updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is not None


# Test 6: Published event permits bounded republication after throttle
@pytest.mark.asyncio
async def test_published_event_permits_republication_after_throttle(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that a published event allows a new event after throttle expires"""
    # Create a QUEUED job with old last_recovery_attempt_at (> 1 hour ago)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(hours=3),
        last_recovery_attempt_at=datetime.utcnow() - timedelta(hours=2),  # Throttle expired
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create an existing PUBLISHED event (old recovery attempt)
    existing_event = JobOutbox(
        job_id=job.id,
        event_type="job_queued_recovery",
        retry_attempt=None,
        published=True,  # Already published
        published_at=datetime.utcnow() - timedelta(hours=2),
        attempts=1,
    )
    db_session.add(existing_event)
    await db_session.commit()

    # Reconcile the job (throttle has expired)
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True

    # Verify NEW event created (2 events total now)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id).order_by(JobOutbox.created_at)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 2
    assert events[0].id == existing_event.id  # Old event
    assert events[1].event_type == "job_queued_recovery"  # New event
    assert events[1].published is False

    # Verify throttling timestamp updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at > datetime.utcnow() - timedelta(seconds=10)


# Test 7: Published event within throttle does NOT permit republication
@pytest.mark.asyncio
async def test_published_event_within_throttle_no_republication(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that a recently published event does not allow republication within throttle"""
    # Create a QUEUED job with recent last_recovery_attempt_at (< 1 hour ago)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(hours=2),
        last_recovery_attempt_at=datetime.utcnow() - timedelta(minutes=30),  # Throttle NOT expired
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create an existing PUBLISHED event (recent recovery attempt)
    existing_event = JobOutbox(
        job_id=job.id,
        event_type="job_queued_recovery",
        retry_attempt=None,
        published=True,
        published_at=datetime.utcnow() - timedelta(minutes=30),
        attempts=1,
    )
    db_session.add(existing_event)
    await db_session.commit()

    # Attempt to reconcile the job (throttle has NOT expired)
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is False  # Not eligible due to throttle

    # Verify NO new event created (still only 1 event)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].id == existing_event.id


# Test 8: Two concurrent reconcilers cannot create duplicate events
@pytest.mark.asyncio
async def test_concurrent_reconcilers_no_duplicate_events(db_session: AsyncSession):
    """Test that FOR UPDATE SKIP LOCKED prevents concurrent reconcilers from creating duplicates"""
    from tests.conftest import TestSessionLocal

    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()

    # Simulate two concurrent reconcilers
    async with TestSessionLocal() as session1:
        async with TestSessionLocal() as session2:
            service1 = JobRecoveryService(session1)
            service2 = JobRecoveryService(session2)

            # Both attempt to reconcile the same job concurrently
            result1 = await service1.reconcile_queued_job(job.id)
            result2 = await service2.reconcile_queued_job(job.id)

            # Commit both sessions
            await session1.commit()
            await session2.commit()

            # Exactly one should succeed
            assert result1 != result2  # One True, one False

    # Verify only ONE event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].event_type == "job_queued_recovery"


# Test 9: Failure before commit leaves neither timestamp nor event
@pytest.mark.asyncio
async def test_rollback_leaves_no_timestamp_or_event(db_session: AsyncSession):
    """Test that rollback before commit leaves neither timestamp nor event"""
    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()

    # Create a new session for reconciliation
    from tests.conftest import TestSessionLocal

    async with TestSessionLocal() as test_session:
        service = JobRecoveryService(test_session)

        # Reconcile the job (but don't commit)
        success = await service.reconcile_queued_job(job.id)
        assert success is True

        # Verify event created in this session (before commit)
        stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
        result = await test_session.execute(stmt)
        events = result.scalars().all()
        assert len(events) == 1

        # ROLLBACK instead of commit
        await test_session.rollback()

    # Verify NO event persisted
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()
    assert len(events) == 0

    # Verify timestamp NOT persisted
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is None


# Test 10: Successful commit persists both timestamp and event
@pytest.mark.asyncio
async def test_commit_persists_timestamp_and_event(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that successful commit persists both timestamp and event atomically"""
    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()

    # Reconcile and commit
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True

    # Verify event persisted
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()
    assert len(events) == 1

    # Verify timestamp persisted
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is not None


# Test 11: Repeated scans respect the throttle
@pytest.mark.asyncio
async def test_repeated_scans_respect_throttle(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that repeated reconciliation attempts respect the throttle"""
    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)
    await db_session.commit()

    # First reconciliation
    success1 = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()
    assert success1 is True

    # Second reconciliation immediately (should be throttled)
    success2 = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()
    assert success2 is False  # Throttled

    # Verify only ONE event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()
    assert len(events) == 1


# Test 12: reconciliation_required does not permanently block
@pytest.mark.asyncio
async def test_reconciliation_required_does_not_block(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that reconciliation_required events do not permanently block recovery"""
    # Create a QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create an existing event marked reconciliation_required (invalid event)
    invalid_event = JobOutbox(
        job_id=job.id,
        event_type="job_queued_recovery",
        retry_attempt=None,
        published=False,
        reconciliation_required=True,  # Marked as invalid
        last_error="Job not found at time of publication",
        attempts=5,
    )
    db_session.add(invalid_event)
    await db_session.commit()

    # Reconcile the job (should create NEW event despite invalid event existing)
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True

    # Verify NEW event created (2 events total)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id).order_by(JobOutbox.created_at)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 2
    assert events[0].reconciliation_required is True  # Old invalid event
    assert events[1].reconciliation_required is False  # New valid event
    assert events[1].published is False


# Test 13: Batch reconciliation with limits
@pytest.mark.asyncio
async def test_batch_reconciliation_respects_limits(
    db_session: AsyncSession, reconciliation_service_with_factory: JobRecoveryService
):
    """Test that batch reconciliation respects batch size limits"""
    # Create 10 QUEUED jobs (all eligible)
    job_ids = []
    for i in range(10):
        job = Job(
            id=uuid4(),
            job_type="test_task",
            payload={"data": f"test_{i}"},
            status=JobStatus.QUEUED,
            created_at=datetime.utcnow() - timedelta(minutes=15),
            retry_count=0,
            max_retries=3,
        )
        db_session.add(job)
        job_ids.append(job.id)

    await db_session.commit()

    # Reconcile with batch_size=5
    detected, reconciled = await reconciliation_service_with_factory.reconcile_queued_jobs_batch(
        batch_size=5
    )

    assert detected == 5  # Only 5 detected
    assert reconciled == 5  # All 5 reconciled

    # Verify exactly 5 events created
    stmt = select(JobOutbox)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 5

    # Second batch should reconcile remaining 5
    detected2, reconciled2 = await reconciliation_service_with_factory.reconcile_queued_jobs_batch(
        batch_size=5
    )

    assert detected2 == 5
    assert reconciled2 == 5

    # Verify total 10 events created
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 10


# Test 14: Detection query ordering (oldest first)
@pytest.mark.asyncio
async def test_detection_orders_oldest_first(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that detection query returns oldest QUEUED jobs first"""
    # Create 3 QUEUED jobs with different ages
    oldest_job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "oldest"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=30),
    )
    middle_job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "middle"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=20),
    )
    newest_job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "newest"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
    )

    db_session.add(newest_job)  # Add in reverse order
    db_session.add(middle_job)
    db_session.add(oldest_job)
    await db_session.commit()

    # Detect with batch_size=3
    detected_ids = await reconciliation_service.detect_queued_jobs(batch_size=3)

    # Verify oldest job is first
    assert len(detected_ids) == 3
    assert detected_ids[0] == oldest_job.id
    assert detected_ids[1] == middle_job.id
    assert detected_ids[2] == newest_job.id


# ============================================================================
# Integration Tests: job_created Deduplication (Step 289B.1)
# ============================================================================


# Test 15: Pending job_created prevents recovery publication
@pytest.mark.asyncio
async def test_pending_job_created_prevents_recovery(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that unpublished job_created event prevents duplicate recovery event"""
    # Create QUEUED job (old enough for recovery)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create unpublished job_created event (initial creation, not yet published)
    job_created_event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        retry_attempt=None,
        published=False,
        attempts=0,
    )
    db_session.add(job_created_event)
    await db_session.commit()

    # Attempt reconciliation
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True  # Completed (but skipped event creation)

    # Verify NO new event created (still only job_created)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].event_type == "job_created"
    assert events[0].id == job_created_event.id

    # Verify throttling timestamp WAS updated (even though no event created)
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is not None


# Test 16: Failed job_created with retry path prevents duplicate
@pytest.mark.asyncio
async def test_failed_job_created_in_backoff_prevents_duplicate(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that failed job_created awaiting retry prevents duplicate recovery event"""
    # Create QUEUED job
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create failed job_created in exponential backoff
    job_created_event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        retry_attempt=None,
        published=False,
        attempts=3,  # Failed 3 times
        next_publication_attempt_at=datetime.utcnow() + timedelta(minutes=5),  # Backoff not expired
        last_error="ConnectionError: Redis unavailable",
    )
    db_session.add(job_created_event)
    await db_session.commit()

    # Attempt reconciliation
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True  # Completed (but skipped event creation)

    # Verify NO new event created
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].id == job_created_event.id

    # Verify throttling timestamp WAS updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at is not None


# Test 17: Published job_created does not permanently block recovery
@pytest.mark.asyncio
async def test_published_job_created_allows_recovery_after_throttle(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that published job_created allows recovery event after throttle expires"""
    # Create QUEUED job with old last_recovery_attempt_at (throttle expired)
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(hours=3),
        last_recovery_attempt_at=datetime.utcnow() - timedelta(hours=2),  # Throttle expired
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Create PUBLISHED job_created event (old)
    job_created_event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        retry_attempt=None,
        published=True,  # Already published
        published_at=datetime.utcnow() - timedelta(hours=2),
        attempts=1,
    )
    db_session.add(job_created_event)
    await db_session.commit()

    # Attempt reconciliation (throttle expired)
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert success is True

    # Verify NEW event created (2 events total)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id).order_by(JobOutbox.created_at)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 2
    assert events[0].id == job_created_event.id  # Old published event
    assert events[1].event_type == "job_queued_recovery"  # New recovery event
    assert events[1].published is False

    # Verify throttling timestamp updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at > datetime.utcnow() - timedelta(seconds=10)


# Test 18: Concurrent reconcilers with existing job_created (no duplicate)
@pytest.mark.asyncio
async def test_concurrent_reconcilers_with_job_created_no_duplicate(db_session: AsyncSession):
    """Test that concurrent reconcilers cannot create duplicates even with existing job_created"""
    from tests.conftest import TestSessionLocal

    # Create QUEUED job with unpublished job_created
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(minutes=15),
        retry_count=0,
        max_retries=3,
    )
    job_created_event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=False,
        attempts=0,
    )
    db_session.add(job)
    db_session.add(job_created_event)
    await db_session.commit()

    # Two concurrent reconcilers
    async with TestSessionLocal() as session1:
        async with TestSessionLocal() as session2:
            service1 = JobRecoveryService(session1)
            service2 = JobRecoveryService(session2)

            # Both attempt reconciliation
            result1 = await service1.reconcile_queued_job(job.id)
            result2 = await service2.reconcile_queued_job(job.id)

            await session1.commit()
            await session2.commit()

            # Both should skip event creation (existing job_created found)
            # At least one should succeed
            assert result1 is True or result2 is True

    # Verify still only ONE event (job_created)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 1
    assert events[0].event_type == "job_created"


# Test 19: Mixed event types - job_created published, job_queued_recovery unpublished
@pytest.mark.asyncio
async def test_mixed_events_job_queued_recovery_prevents_duplicate(
    db_session: AsyncSession, reconciliation_service: JobRecoveryService
):
    """Test that existing unpublished job_queued_recovery prevents duplicate even with published job_created"""
    # Create QUEUED job with expired throttle
    job = Job(
        id=uuid4(),
        job_type="test_task",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        created_at=datetime.utcnow() - timedelta(hours=3),
        last_recovery_attempt_at=datetime.utcnow()
        - timedelta(hours=2),  # Throttle expired (> 1 hour ago)
        retry_count=0,
        max_retries=3,
    )
    db_session.add(job)

    # Old published job_created
    job_created_event = JobOutbox(
        job_id=job.id,
        event_type="job_created",
        published=True,
        published_at=datetime.utcnow() - timedelta(hours=2),
    )
    db_session.add(job_created_event)

    # Recent unpublished job_queued_recovery (from previous reconciliation)
    job_recovery_event = JobOutbox(
        job_id=job.id,
        event_type="job_queued_recovery",
        published=False,
        attempts=0,
        created_at=datetime.utcnow() - timedelta(minutes=5),
    )
    db_session.add(job_recovery_event)
    await db_session.commit()

    # Attempt reconciliation
    success = await reconciliation_service.reconcile_queued_job(job.id)
    await db_session.commit()

    assert (
        success is True
    )  # Completed (but skipped due to existing unpublished job_queued_recovery)

    # Verify NO new event created (still 2 events)
    stmt = select(JobOutbox).where(JobOutbox.job_id == job.id)
    result = await db_session.execute(stmt)
    events = result.scalars().all()

    assert len(events) == 2  # job_created (published) + job_queued_recovery (unpublished)

    # Verify throttling timestamp updated
    await db_session.refresh(job)
    assert job.last_recovery_attempt_at > datetime.utcnow() - timedelta(seconds=10)
