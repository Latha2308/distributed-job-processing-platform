"""Integration tests for stuck-job recovery with real PostgreSQL and Redis.

These tests verify the recovery service works end-to-end with actual database
transactions. Each test manages its own sessions independently to avoid lock
contention. Bounded timeouts (lock_timeout=5000ms, statement_timeout=30000ms)
ensure tests fail fast rather than hanging indefinitely.

PostgreSQL → Redis Integration Tests:
These tests verify the full recovery dispatch pipeline:
1. Recovery service creates RETRYING jobs + outbox events in PostgreSQL
2. OutboxDispatcher publishes events to Redis delayed queue
3. Delayed queue promotion moves jobs to ready queue
4. All components use Redis DB 15 (isolated from development DB 0)
"""

import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from sqlalchemy import select

from app.core.redis_client import RedisClient
from app.db.base import Base
from app.db.models import Job, JobOutbox, JobStatus
from app.services.job_recovery_service import JobRecoveryService
from app.services.outbox_dispatcher import OutboxDispatcher
from app.services.queue_service import QueueService
from tests.conftest import TEST_REDIS_URL, TestSessionLocal, test_engine


@pytest.fixture
async def setup_teardown_db():
    """Setup and teardown database tables for integration tests.

    Creates all tables before test and drops them after. Uses independent
    connection to avoid session lifecycle conflicts with test operations.
    """
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield

    # Cleanup: Ensure all test sessions are closed before dropping tables
    await test_engine.dispose()

    # Recreate engine for teardown to avoid connection pool issues
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def redis_client():
    """Create isolated Redis client for testing.

    Uses Redis DB 15 to ensure complete isolation from production/dev Redis DB 0.
    Cleans only test-specific keys to preserve unrelated data.
    """
    client = await aioredis.from_url(TEST_REDIS_URL, decode_responses=True)

    # Verify we're using the correct isolated database
    # The connection_kwargs should have db=15 from TEST_REDIS_URL
    current_db = client.connection_pool.connection_kwargs.get("db", 0)
    assert current_db == 15, f"Expected Redis DB 15, got DB {current_db}"

    # Clean only job-related keys before test
    test_keys = ["job_queue", "job_queue_delayed"]
    for key in test_keys:
        await client.delete(key)

    yield client

    # Clean only job-related keys after test
    for key in test_keys:
        await client.delete(key)
    await client.aclose()


@pytest.mark.asyncio
async def test_recovery_creates_job_outbox_event_atomically(setup_teardown_db, redis_client):
    """Test that recovery creates job and outbox event atomically.

    Verifies:
    - Stuck job is detected and recovered
    - Job status transitions to RETRYING
    - Retry count increments
    - Outbox event is created with correct metadata
    - All changes are atomic (commit together or rollback together)
    """

    # Setup: Create stuck job with independent session
    job_id = None
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="integration_task",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-dead",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id
        original_retry_count = job.retry_count
    # Setup session is now closed, releasing any locks

    # Action: Run recovery in independent session
    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session, session_factory=TestSessionLocal)
        detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)
    # Recovery session is now closed

    assert detected == 1
    assert recovered == 1

    # Verify: Check job and outbox state in independent session
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == original_retry_count + 1

        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        outbox_event = result.scalar_one()

        assert outbox_event.event_type == "job_recovered"
        assert outbox_event.published is False
        assert outbox_event.retry_attempt == original_retry_count + 1


@pytest.mark.asyncio
async def test_two_recovery_runners_create_exactly_one_outbox_event(
    setup_teardown_db, redis_client
):
    """Test concurrent recovery runners don't create duplicate events.

    Verifies:
    - Two concurrent recovery attempts for the same job
    - At most one recovery succeeds (SELECT FOR UPDATE prevents duplicates)
    - Exactly one outbox event is created
    - No partial state corruption from race conditions
    """

    # Setup: Create stuck job with independent session
    job_id = None
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="race_task",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-dead",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id
    # Setup session closed

    # Action: Two concurrent recoveries in independent sessions
    async def recover():
        try:
            async with TestSessionLocal() as session:
                recovery_service = JobRecoveryService(session, session_factory=TestSessionLocal)
                return await recovery_service.recover_stuck_jobs_batch(batch_size=10)
        except Exception:
            # Expected: One recovery may fail due to lock contention
            return (0, 0)

    results = await asyncio.gather(recover(), recover(), return_exceptions=True)

    successful_recoveries = sum(r[1] for r in results if isinstance(r, tuple))
    assert successful_recoveries <= 1, "At most one recovery should succeed"

    # Verify: Exactly one outbox event in independent session
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        outbox_events = result.scalars().all()
        assert len(outbox_events) == 1, "Exactly one outbox event should exist"


@pytest.mark.asyncio
async def test_transaction_rollback_prevents_partial_recovery(setup_teardown_db, redis_client):
    """Test rollback prevents partial recovery state.

    Verifies:
    - Transaction rollback on error preserves original state
    - No partial updates to job status
    - No orphaned outbox events
    - Database atomicity guarantees hold under failure
    """

    # Setup: Create stuck job with independent session
    job_id = None
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="rollback_task",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-dead",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id
        original_retry_count = job.retry_count
    # Setup session closed

    # Action: Force rollback by raising exception before commit
    try:
        async with TestSessionLocal() as recovery_session:
            result = await recovery_session.execute(select(Job).where(Job.id == job_id))
            recovering_job = result.scalar_one()
            recovering_job.status = JobStatus.RETRYING
            recovering_job.retry_count += 1

            outbox = JobOutbox(
                id=uuid4(),
                job_id=job_id,
                event_type="job_recovered",
                retry_attempt=1,
                published=False,
            )
            recovery_session.add(outbox)
            # Raise before commit - triggers automatic rollback
            raise RuntimeError("Simulated failure")
    except RuntimeError:
        pass  # Expected - session rollback is automatic on exception

    # Verify: State unchanged in independent session
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.PROCESSING, "Job status should be unchanged"
        assert job.retry_count == original_retry_count, "Retry count should be unchanged"

        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        outbox_events = result.scalars().all()
        assert len(outbox_events) == 0, "No outbox events should exist after rollback"


@pytest.mark.asyncio
async def test_lock_timeout_produces_bounded_failure(setup_teardown_db, redis_client):
    """Regression test: Lock contention produces bounded failure, not indefinite hang.

    Verifies:
    - lock_timeout=5000ms setting is active
    - Competing sessions fail fast with DBAPIError when locks are held
    - No indefinite hangs on blocked operations
    - Test completes in bounded time (<10 seconds)

    This test prevents regression to the Step 279 issue where stale connections
    caused indefinite hangs.
    """
    from sqlalchemy.exc import DBAPIError

    job_id = None

    # Setup: Create a job and hold an open transaction with lock
    async with TestSessionLocal() as lock_holder_session:
        job = Job(
            id=uuid4(),
            job_type="lock_test_task",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-1",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        lock_holder_session.add(job)
        await lock_holder_session.commit()
        job_id = job.id

        # Acquire exclusive row lock with SELECT FOR UPDATE
        await lock_holder_session.execute(select(Job).where(Job.id == job_id).with_for_update())

        # Now try to update the same row from a competing session
        # This should fail fast due to lock_timeout, not hang indefinitely
        try:
            async with TestSessionLocal() as competing_session:
                # This should hit lock_timeout and raise DBAPIError
                result = await competing_session.execute(
                    select(Job).where(Job.id == job_id).with_for_update()
                )
                competing_job = result.scalar_one()
                competing_job.status = JobStatus.RETRYING
                await competing_session.commit()

                # Should not reach here
                pytest.fail("Expected DBAPIError due to lock timeout")

        except DBAPIError as e:
            # Expected: lock_timeout causes bounded failure
            error_str = str(e).lower()
            assert "lock" in error_str and (
                "timeout" in error_str or "not available" in error_str
            ), f"Expected lock timeout error, got: {e}"

    # Verify: Original session's lock is released after context exit
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        assert job.status == JobStatus.PROCESSING, "Job should remain in original state"


# ========== PostgreSQL → Redis Integration Tests ==========


@pytest_asyncio.fixture
async def test_redis_client():
    """Create test Redis client explicitly configured for DB 15.

    This fixture provides a RedisClient instance configured for Redis DB 15,
    ensuring complete isolation from development Redis DB 0.
    """
    client = RedisClient()

    # Override the connection URL to use test database 15
    import redis.asyncio as aioredis
    from redis.asyncio import ConnectionPool

    test_pool = ConnectionPool.from_url(
        TEST_REDIS_URL,
        max_connections=10,
        decode_responses=True,
    )

    # Replace the client's connection pool
    redis_instance = aioredis.Redis(connection_pool=test_pool)
    client._client = redis_instance

    # Verify we're using Redis DB 15
    db_num = test_pool.connection_kwargs.get("db", 0)
    assert db_num == 15, f"Expected Redis DB 15, got DB {db_num}"

    # Clean up before test
    await redis_instance.flushdb()

    yield client

    # Clean up after test
    await redis_instance.flushdb()
    await redis_instance.aclose()
    await test_pool.disconnect()


@pytest.mark.asyncio
async def test_full_recovery_to_redis_dispatch(setup_teardown_db, test_redis_client):
    """Test full PostgreSQL → Redis recovery dispatch pipeline.

    Verifies:
    a. Stuck job is recovered in PostgreSQL (status=RETRYING, retry_count=1)
    b. Unpublished outbox event created with retry_attempt=1
    c. OutboxDispatcher publishes to Redis delayed queue
    d. Event marked published=True after Redis confirmation
    e. Redis payload contains job_id and retry_attempt=1
    f. Delayed queue score matches next_retry_at
    g. Promotion moves job to ready queue with retry_attempt preserved
    """
    job_id = None
    next_retry_at = None

    # Step 1: Create stuck job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="dispatch_test_task",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-dead",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
            priority=5,
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    # Step 2: Recover job through JobRecoveryService
    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session, session_factory=TestSessionLocal)
        detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    assert detected == 1
    assert recovered == 1

    # Step 3: Verify recovery state in PostgreSQL
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.next_retry_at is not None
        next_retry_at = job.next_retry_at

        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        outbox_event = result.scalar_one()

        assert outbox_event.event_type == "job_recovered"
        assert outbox_event.published is False
        assert outbox_event.retry_attempt == 1

    # Step 4: Dispatch outbox event to Redis
    queue_service = QueueService(redis=test_redis_client)

    # Patch AsyncSessionLocal to use test database
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 1

    # Step 5: Verify event marked as published in PostgreSQL
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()

        assert event.published is True
        assert event.published_at is not None
        assert event.reconciliation_required is False
        assert event.last_error is None

        # Verify job state unchanged (dispatcher doesn't modify job)
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        assert job.retry_count == 1
        assert job.next_retry_at == next_retry_at

    # Step 6: Verify Redis delayed queue contains the job
    redis_client_raw = await test_redis_client.get_client()
    delayed_jobs = await redis_client_raw.zrange("job_queue_delayed", 0, -1, withscores=True)

    assert len(delayed_jobs) == 1, "Expected one job in delayed queue"

    job_json, score = delayed_jobs[0]
    job_data = json.loads(job_json)

    assert job_data["job_id"] == str(job_id)
    assert job_data["job_type"] == "dispatch_test_task"
    assert job_data["priority"] == 5
    assert job_data["retry_attempt"] == 1

    # Verify score matches next_retry_at
    expected_score = next_retry_at.timestamp()
    assert abs(score - expected_score) < 1, "Score should match next_retry_at timestamp"

    # Step 7: Promote delayed job to ready queue
    promoted = await test_redis_client.transfer_ready_jobs_atomic(
        main_queue="job_queue",
        delayed_queue="job_queue_delayed",
        batch_limit=10,
    )

    # If job is not ready yet, wait and try again
    if promoted == 0:
        # Force promotion by directly manipulating the score
        await redis_client_raw.zadd("job_queue_delayed", {job_json: 0})
        promoted = await test_redis_client.transfer_ready_jobs_atomic()

    assert promoted == 1, "Expected one job promoted to ready queue"

    # Step 8: Verify ready queue contains job with retry_attempt preserved
    ready_jobs = await redis_client_raw.zrange("job_queue", 0, -1, withscores=True)

    assert len(ready_jobs) == 1, "Expected one job in ready queue"

    ready_job_json, priority_score = ready_jobs[0]
    ready_job_data = json.loads(ready_job_json)

    assert ready_job_data["job_id"] == str(job_id)
    assert ready_job_data["job_type"] == "dispatch_test_task"
    assert ready_job_data["priority"] == 5
    assert ready_job_data["retry_attempt"] == 1, "retry_attempt must be preserved through promotion"
    assert priority_score == 5, "Priority score should be used in ready queue"


@pytest.mark.asyncio
async def test_redis_failure_prevents_outbox_publication(setup_teardown_db, test_redis_client):
    """Test transient Redis failure handling.

    Verifies:
    - Simulated Redis publication failure leaves event unpublished
    - next_publication_attempt_at is set for exponential backoff
    - retry_count remains unchanged (no double increment)
    - After Redis recovery, same event publishes with same retry_attempt
    - Event marked published after successful Redis delivery
    """
    job_id = None

    # Step 1: Create and recover stuck job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="redis_failure_test",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-dead",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
            priority=5,
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session, session_factory=TestSessionLocal)
        await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    # Step 2: Simulate Redis failure by making push_to_delayed_queue return False

    # Create a mock that will fail once then succeed
    call_count = 0

    async def mock_push_to_delayed_queue(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return False  # First call fails
        return await original_push(*args, **kwargs)  # Subsequent calls succeed

    original_push = test_redis_client.push_to_delayed_queue
    test_redis_client.push_to_delayed_queue = mock_push_to_delayed_queue

    # Step 3: Attempt dispatch with Redis failure
    queue_service = QueueService(redis=test_redis_client)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 0, "No events should be published due to Redis failure"

    # Step 4: Verify event remains unpublished with backoff scheduled
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()

        assert event.published is False, "Event should remain unpublished"
        assert event.next_publication_attempt_at is not None, "Backoff should be scheduled"
        assert event.attempts == 1, "Attempt counter should increment"
        assert event.reconciliation_required is False, "Should remain retryable"

        # Verify job state unchanged
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        assert job.retry_count == 1, "Retry count should not double-increment"

    # Step 5: Restore Redis and bypass backoff by clearing next_publication_attempt_at
    async with TestSessionLocal() as bypass_session:
        result = await bypass_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()
        event.next_publication_attempt_at = None  # Make immediately eligible
        await bypass_session.commit()

    # Step 6: Retry dispatch (should succeed now)
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 1, "Event should publish successfully after Redis recovery"

    # Step 7: Verify event is now published
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()

        assert event.published is True, "Event should be published after recovery"
        assert event.published_at is not None
        assert event.reconciliation_required is False
        assert event.attempts == 2, "Attempt counter reflects both attempts"

    # Step 8: Verify Redis contains the job with correct retry_attempt
    redis_client_raw = await test_redis_client.get_client()
    delayed_jobs = await redis_client_raw.zrange("job_queue_delayed", 0, -1)

    assert len(delayed_jobs) == 1
    job_data = json.loads(delayed_jobs[0])
    assert job_data["retry_attempt"] == 1, "Same retry_attempt should be published after recovery"


@pytest.mark.asyncio
async def test_stale_recovery_event_marked_for_reconciliation(setup_teardown_db, test_redis_client):
    """Test stale recovery event handling.

    Verifies:
    - Event with outdated retry_attempt is detected
    - No Redis message published for stale event
    - Event marked reconciliation_required=True
    - Event remains published=False (never delivered to Redis)
    - Diagnostic error recorded in last_error
    """
    job_id = None

    # Step 1: Create job in RETRYING state with retry_count=2
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="stale_event_test",
            payload={"data": "test"},
            status=JobStatus.RETRYING,
            retry_count=2,  # Current retry count
            max_retries=3,
            priority=5,
            next_retry_at=datetime.utcnow() + timedelta(seconds=60),
        )
        setup_session.add(job)

        # Create stale outbox event with retry_attempt=1 (older than current retry_count=2)
        stale_event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_recovered",
            retry_attempt=1,  # Stale - job is now at retry_count=2
            published=False,
            attempts=0,
        )
        setup_session.add(stale_event)
        await setup_session.commit()
        job_id = job.id

    # Step 2: Attempt to dispatch stale event
    queue_service = QueueService(redis=test_redis_client)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 0, "Stale event should not be successfully published"

    # Step 3: Verify event marked for reconciliation
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()

        assert event.published is False, "Stale event should NOT be marked published"
        assert event.reconciliation_required is True, "Should be marked for reconciliation"
        assert event.next_publication_attempt_at is None, "No automatic retries"
        assert event.last_error is not None, "Diagnostic error should be recorded"
        assert "stale" in event.last_error.lower(), "Error should mention stale event"
        assert "retry_attempt=1" in event.last_error, "Error should include event retry_attempt"
        assert "retry_count=2" in event.last_error, "Error should include current retry_count"

    # Step 4: Verify no Redis message published
    redis_client_raw = await test_redis_client.get_client()
    delayed_jobs = await redis_client_raw.zrange("job_queue_delayed", 0, -1)

    assert len(delayed_jobs) == 0, "No Redis message should be published for stale event"


@pytest.mark.asyncio
async def test_duplicate_publication_idempotency(setup_teardown_db, test_redis_client):
    """Test duplicate publication handling.

    Verifies:
    - Already-published event is not reprocessed
    - No additional Redis messages created
    - Normal sequential operation (not testing crash-recovery scenarios)

    Note: This does NOT guarantee exactly-once delivery under concurrent
    Redis/DB crash conditions. At-least-once delivery is acceptable.
    """
    job_id = None

    # Step 1: Create and recover job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="duplicate_test",
            payload={"data": "test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-dead",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
            priority=5,
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session, session_factory=TestSessionLocal)
        await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    # Step 2: Dispatch once
    queue_service = QueueService(redis=test_redis_client)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 1

    # Step 3: Verify event is published
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()
        assert event.published is True

    # Step 4: Count Redis messages
    redis_client_raw = await test_redis_client.get_client()
    delayed_count_before = await redis_client_raw.zcard("job_queue_delayed")

    assert delayed_count_before == 1, "Should have one delayed message"

    # Step 5: Attempt to dispatch again (should skip already-published event)
    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 0, "Should not process already-published events"

    # Step 6: Verify no additional Redis messages
    delayed_count_after = await redis_client_raw.zcard("job_queue_delayed")

    assert (
        delayed_count_after == delayed_count_before
    ), "No additional messages should be created for already-published events"


@pytest.mark.asyncio
async def test_null_retry_attempt_marked_for_reconciliation(setup_teardown_db, test_redis_client):
    """Test NULL retry_attempt handling (legacy event).

    Verifies:
    - Event with NULL retry_attempt cannot be published
    - Marked reconciliation_required=True
    - Remains published=False (never delivered)
    - Diagnostic error recorded
    """
    job_id = None

    # Step 1: Create job and outbox event with NULL retry_attempt
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="null_retry_test",
            payload={"data": "test"},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            next_retry_at=datetime.utcnow() + timedelta(seconds=60),
        )
        setup_session.add(job)

        # Create event with NULL retry_attempt (legacy event)
        event = JobOutbox(
            id=uuid4(),
            job_id=job.id,
            event_type="job_recovered",
            retry_attempt=None,  # NULL - cannot be published safely
            published=False,
            attempts=0,
        )
        setup_session.add(event)
        await setup_session.commit()
        job_id = job.id

    # Step 2: Attempt dispatch
    queue_service = QueueService(redis=test_redis_client)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 0

    # Step 3: Verify reconciliation marker
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(JobOutbox).where(JobOutbox.job_id == job_id))
        event = result.scalar_one()

        assert event.published is False
        assert event.reconciliation_required is True
        assert event.next_publication_attempt_at is None
        assert event.last_error is not None
        assert "null retry_attempt" in event.last_error.lower()

    # Step 4: Verify no Redis message
    redis_client_raw = await test_redis_client.get_client()
    delayed_jobs = await redis_client_raw.zrange("job_queue_delayed", 0, -1)

    assert len(delayed_jobs) == 0


# ========== End-to-End Worker Retry Claiming Tests ==========


@pytest.mark.asyncio
async def test_full_recovery_to_worker_execution(setup_teardown_db, test_redis_client):
    """Test complete recovery → Redis → worker claim → execution pipeline.

    Verifies:
    a. Worker A claims and starts job
    b. Heartbeat goes stale
    c. Recovery service detects and recovers job (status=RETRYING, retry_count=1)
    d. Dispatcher publishes to Redis delayed queue
    e. Delayed job promoted to ready queue
    f. Worker B reads message with retry_attempt=1
    g. Worker B successfully claims with retry_attempt=1
    h. Worker B executes job to COMPLETED
    i. Final state: COMPLETED, retry_count=1, owned by worker B

    This is the primary success path exercising actual worker code.
    """
    from app.worker.worker import Worker
    from app.db.models import WorkerHeartbeat

    job_id = None
    worker_a_id = "worker-a-stale"
    worker_b_id = "worker-b-recovery"

    # Step 1: Create QUEUED job and worker A heartbeat
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "test", "processing_time": 0.01},
            status=JobStatus.QUEUED,
            priority=5,
            timeout=300,
            retry_count=0,
            max_retries=3,
        )
        setup_session.add(job)

        worker_a = WorkerHeartbeat(
            worker_id=worker_a_id,
            started_at=datetime.utcnow() - timedelta(seconds=500),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=500),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        setup_session.add(worker_a)
        await setup_session.commit()
        job_id = job.id

    # Step 2: Simulate worker A claiming the job
    async with TestSessionLocal() as claim_session:
        result = await claim_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        job.status = JobStatus.PROCESSING
        job.worker_id = worker_a_id
        job.started_at = datetime.utcnow() - timedelta(seconds=400)
        await claim_session.commit()

    # Step 3: Recover the stuck job
    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session, session_factory=TestSessionLocal)
        detected, recovered = await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    assert detected == 1
    assert recovered == 1

    # Step 3b: Make the job immediately claimable by setting next_retry_at to past
    async with TestSessionLocal() as adjust_session:
        result = await adjust_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        job.next_retry_at = datetime.utcnow() - timedelta(seconds=10)  # Make ready now
        await adjust_session.commit()

    # Step 4: Verify recovery state
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.next_retry_at is not None

    # Step 5: Dispatch recovery event to Redis
    queue_service = QueueService(redis=test_redis_client)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        processed = await dispatcher.process_batch()

    assert processed == 1

    # Step 6: Promote delayed job to ready queue (force immediate promotion)
    redis_client_raw = await test_redis_client.get_client()
    delayed_jobs = await redis_client_raw.zrange("job_queue_delayed", 0, -1)
    assert len(delayed_jobs) == 1

    # Force immediate promotion by setting score to 0
    job_json = delayed_jobs[0]
    await redis_client_raw.zadd("job_queue_delayed", {job_json: 0})

    promoted = await test_redis_client.transfer_ready_jobs_atomic()
    assert promoted == 1

    # Step 7: Verify ready queue has the job with retry_attempt=1
    ready_jobs = await redis_client_raw.zrange("job_queue", 0, -1)
    assert len(ready_jobs) == 1
    ready_job_data = json.loads(ready_jobs[0])
    assert ready_job_data["retry_attempt"] == 1

    # Step 8: Worker B processes the job using actual worker code
    # Create worker B heartbeat
    async with TestSessionLocal() as worker_session:
        worker_b = WorkerHeartbeat(
            worker_id=worker_b_id,
            started_at=datetime.utcnow(),
            last_heartbeat=datetime.utcnow(),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        worker_session.add(worker_b)
        await worker_session.commit()

    # Patch AsyncSessionLocal and redis_client for worker
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch("app.worker.worker.redis_client", test_redis_client):
            worker = Worker(worker_b_id)
            # Process the specific job with retry_attempt=1
            await worker._process_job(job_id, retry_attempt=1)

    # Step 9: Verify final state
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.COMPLETED, "Job should be completed by worker B"
        assert job.retry_count == 1, "Retry count should remain 1"
        assert job.worker_id == worker_b_id, "Job should be owned by worker B"
        assert job.completed_at is not None
        assert job.result is not None

        # Verify worker B stats updated
        result = await verify_session.execute(
            select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker_b_id)
        )
        worker_b_heartbeat = result.scalar_one()
        assert worker_b_heartbeat.jobs_processed == 1


@pytest.mark.asyncio
async def test_stale_retry_attempt_cannot_claim(setup_teardown_db, test_redis_client):
    """Test retry_attempt=0 cannot claim job at retry_count=1.

    Verifies:
    - Job is RETRYING with retry_count=1
    - Worker attempts claim with retry_attempt=0 (stale)
    - Claim fails (returns None)
    - Job state unchanged
    - No execution occurs
    """
    from app.services.job_service import JobService

    job_id = None

    # Create job in RETRYING state with retry_count=1
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="stale_test",
            payload={"data": "test"},
            status=JobStatus.RETRYING,
            retry_count=1,  # Current retry count
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),  # Ready now
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    # Attempt to claim with stale retry_attempt=0
    async with TestSessionLocal() as claim_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim_session, test_redis_client, queue_service)

        claimed_job = await job_service.claim_job(
            job_id=job_id, worker_id="worker-stale", retry_attempt=0  # Stale
        )

    assert claimed_job is None, "Stale retry_attempt should not claim job"

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.worker_id is None


@pytest.mark.asyncio
async def test_future_retry_attempt_cannot_claim(setup_teardown_db, test_redis_client):
    """Test retry_attempt=2 cannot claim job at retry_count=1.

    Verifies:
    - Job is RETRYING with retry_count=1
    - Worker attempts claim with retry_attempt=2 (future)
    - Claim fails
    - Job state unchanged
    """
    from app.services.job_service import JobService

    job_id = None

    # Create job in RETRYING state with retry_count=1
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="future_test",
            payload={"data": "test"},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    # Attempt to claim with future retry_attempt=2
    async with TestSessionLocal() as claim_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim_session, test_redis_client, queue_service)

        claimed_job = await job_service.claim_job(
            job_id=job_id, worker_id="worker-future", retry_attempt=2  # Future
        )

    assert claimed_job is None

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.worker_id is None


@pytest.mark.asyncio
async def test_missing_retry_attempt_cannot_claim_retrying_job(
    setup_teardown_db, test_redis_client
):
    """Test None retry_attempt cannot claim RETRYING job.

    Verifies:
    - Job is RETRYING with retry_count=1
    - Worker attempts claim with retry_attempt=None
    - Claim fails (RETRYING jobs require retry_attempt)
    - Job state unchanged
    """
    from app.services.job_service import JobService

    job_id = None

    # Create job in RETRYING state
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="missing_attempt_test",
            payload={"data": "test"},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    # Attempt to claim without retry_attempt
    async with TestSessionLocal() as claim_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim_session, test_redis_client, queue_service)

        claimed_job = await job_service.claim_job(
            job_id=job_id, worker_id="worker-no-attempt", retry_attempt=None
        )

    assert claimed_job is None, "Missing retry_attempt should not claim RETRYING job"

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.worker_id is None


@pytest.mark.asyncio
async def test_malformed_retry_attempt_rejected(setup_teardown_db, test_redis_client):
    """Test malformed retry_attempt values rejected.

    Verifies:
    - Boolean retry_attempt rejected
    - String retry_attempt rejected
    - Float retry_attempt rejected
    - Negative retry_attempt rejected
    - Zero retry_attempt rejected
    - Job state unchanged for all cases
    """
    from app.services.job_service import JobService

    job_id = None

    # Create job in RETRYING state
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="malformed_test",
            payload={"data": "test"},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    malformed_values = [
        True,  # Boolean
        "1",  # String
        1.0,  # Float
        -1,  # Negative
        0,  # Zero
    ]

    for malformed_value in malformed_values:
        async with TestSessionLocal() as claim_session:
            queue_service = QueueService(redis=test_redis_client)
            job_service = JobService(claim_session, test_redis_client, queue_service)

            claimed_job = await job_service.claim_job(
                job_id=job_id, worker_id="worker-malformed", retry_attempt=malformed_value
            )

        assert claimed_job is None, f"Malformed value {malformed_value} should be rejected"

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.worker_id is None


@pytest.mark.asyncio
async def test_duplicate_retry_messages_single_execution(setup_teardown_db, test_redis_client):
    """Test duplicate retry_attempt=1 messages produce only one execution.

    Verifies:
    - Two workers race to claim same job with retry_attempt=1
    - Only one claim succeeds
    - Only one execution occurs
    - Second worker gets None from claim_job
    """
    from app.worker.worker import Worker
    from app.db.models import WorkerHeartbeat, JobExecution

    job_id = None
    worker_1_id = "worker-race-1"
    worker_2_id = "worker-race-2"

    # Create RETRYING job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "test", "processing_time": 0.01},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),
        )
        setup_session.add(job)

        # Create worker heartbeats
        for worker_id in [worker_1_id, worker_2_id]:
            worker = WorkerHeartbeat(
                worker_id=worker_id,
                started_at=datetime.utcnow(),
                last_heartbeat=datetime.utcnow(),
                is_active=True,
                jobs_processed=0,
                jobs_failed=0,
            )
            setup_session.add(worker)

        await setup_session.commit()
        job_id = job.id

    # Race condition: Two workers try to process same job simultaneously
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch("app.worker.worker.redis_client", test_redis_client):
            worker_1 = Worker(worker_1_id)
            worker_2 = Worker(worker_2_id)

            # Execute both claims concurrently
            await asyncio.gather(
                worker_1._process_job(job_id, retry_attempt=1),
                worker_2._process_job(job_id, retry_attempt=1),
                return_exceptions=True,
            )

    # Verify exactly one execution
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.COMPLETED
        assert job.retry_count == 1

        # Check which worker won
        assert job.worker_id in [worker_1_id, worker_2_id]
        winning_worker = job.worker_id

        # Verify only one execution record
        result = await verify_session.execute(
            select(JobExecution).where(JobExecution.job_id == job_id)
        )
        executions = result.scalars().all()
        assert len(executions) == 1
        assert executions[0].worker_id == winning_worker

        # Verify only winning worker incremented counter
        result = await verify_session.execute(
            select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == winning_worker)
        )
        winning_heartbeat = result.scalar_one()
        assert winning_heartbeat.jobs_processed == 1

        # Verify losing worker did not increment counter
        losing_worker = worker_2_id if winning_worker == worker_1_id else worker_1_id
        result = await verify_session.execute(
            select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == losing_worker)
        )
        losing_heartbeat = result.scalar_one()
        assert losing_heartbeat.jobs_processed == 0


@pytest.mark.asyncio
async def test_completed_job_cannot_be_claimed_again(setup_teardown_db, test_redis_client):
    """Test already-COMPLETED job cannot be claimed.

    Verifies:
    - Job is COMPLETED
    - Worker attempts claim
    - Claim fails
    - Job state unchanged
    """
    from app.services.job_service import JobService

    job_id = None

    # Create COMPLETED job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="completed_test",
            payload={"data": "test"},
            status=JobStatus.COMPLETED,
            retry_count=0,
            max_retries=3,
            priority=5,
            timeout=300,
            worker_id="worker-original",
            completed_at=datetime.utcnow(),
            result={"status": "success"},
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    # Attempt to claim completed job
    async with TestSessionLocal() as claim_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim_session, test_redis_client, queue_service)

        claimed_job = await job_service.claim_job(
            job_id=job_id, worker_id="worker-late", retry_attempt=None
        )

    assert claimed_job is None

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.COMPLETED
        assert job.worker_id == "worker-original"


@pytest.mark.asyncio
async def test_stale_worker_cannot_complete_recovered_job(setup_teardown_db, test_redis_client):
    """Test stale original worker cannot complete recovered job.

    Verifies:
    - Job recovered by worker B (status=PROCESSING, worker_id=worker-b)
    - Original worker A attempts completion with outdated ownership
    - Completion fencing rejects stale update
    - Job state unchanged by stale worker
    - No execution record created by stale worker
    """
    from app.db.models import WorkerHeartbeat, JobExecution
    from app.services.job_service import JobService

    job_id = None
    worker_a_id = "worker-a-stale-complete"
    worker_b_id = "worker-b-claimed"

    # Create job PROCESSING by worker B (after recovery)
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "test", "processing_time": 0.01},
            status=JobStatus.PROCESSING,
            worker_id=worker_b_id,  # Currently owned by worker B
            started_at=datetime.utcnow(),
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
        )
        setup_session.add(job)

        # Create both worker heartbeats
        for worker_id in [worker_a_id, worker_b_id]:
            worker = WorkerHeartbeat(
                worker_id=worker_id,
                started_at=datetime.utcnow(),
                last_heartbeat=datetime.utcnow(),
                is_active=True,
                jobs_processed=0,
                jobs_failed=0,
            )
            setup_session.add(worker)

        await setup_session.commit()
        job_id = job.id

    # Worker A (stale) attempts completion using fenced update
    async with TestSessionLocal() as stale_session:
        from app.models.job import JobUpdate

        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(stale_session, test_redis_client, queue_service)

        # Attempt fenced update with stale worker_id
        updated_job = await job_service.update_job_status(
            job_id,
            JobUpdate(status=JobStatus.COMPLETED, result={"status": "stale"}),
            worker_id=worker_a_id,  # Stale worker
            expected_status=JobStatus.PROCESSING,
            expected_worker_id=worker_a_id,  # Expects to own it
        )

        # Should be rejected
        assert updated_job is None, "Stale worker update should be fenced"

        # Rollback to prevent any changes
        await stale_session.rollback()

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.PROCESSING, "Status should not change"
        assert job.worker_id == worker_b_id, "Ownership should not change"
        assert job.completed_at is None, "Should not be marked complete"

        # Verify no execution record from stale worker
        result = await verify_session.execute(
            select(JobExecution).where(
                JobExecution.job_id == job_id, JobExecution.worker_id == worker_a_id
            )
        )
        stale_executions = result.scalars().all()
        assert len(stale_executions) == 0, "No execution record from stale worker"


# ========== Part A: Real Queue Consumption Verification ==========


@pytest.mark.asyncio
async def test_real_queue_message_consumption_with_retry_attempt(
    setup_teardown_db, test_redis_client
):
    """Test actual Redis message consumption extracts retry_attempt correctly.

    Verifies the complete queue → worker → claim boundary:
    - Recovery creates job with retry_count=1
    - Dispatcher publishes to Redis with retry_attempt=1
    - Worker reads actual Redis message (not manual construction)
    - retry_attempt extracted from message reaches claim_job unchanged
    - Job completes successfully

    This is the critical parsing boundary test.
    """
    from app.worker.worker import Worker
    from app.db.models import WorkerHeartbeat
    from app.services.queue_service import QueueService

    job_id = None
    worker_id = "worker-queue-test"

    # Step 1: Create and recover a stuck job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "queue_test", "processing_time": 0.01},
            status=JobStatus.PROCESSING,
            worker_id="worker-stale",
            started_at=datetime.utcnow() - timedelta(seconds=400),
            timeout=300,
            retry_count=0,
            max_retries=3,
            priority=5,
        )
        setup_session.add(job)

        # Create stale worker
        stale_worker = WorkerHeartbeat(
            worker_id="worker-stale",
            started_at=datetime.utcnow() - timedelta(seconds=500),
            last_heartbeat=datetime.utcnow() - timedelta(seconds=500),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        setup_session.add(stale_worker)
        await setup_session.commit()
        job_id = job.id

    # Step 2: Recover the job
    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session, session_factory=TestSessionLocal)
        await recovery_service.recover_stuck_jobs_batch(batch_size=10)

    # Make job immediately claimable
    async with TestSessionLocal() as adjust_session:
        result = await adjust_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        job.next_retry_at = datetime.utcnow() - timedelta(seconds=10)
        await adjust_session.commit()

    # Step 3: Dispatch to Redis
    queue_service = QueueService(redis=test_redis_client)

    with patch("app.services.outbox_dispatcher.AsyncSessionLocal", TestSessionLocal):
        dispatcher = OutboxDispatcher(queue_service=queue_service, batch_size=10)
        await dispatcher.process_batch()

    # Step 4: Promote to ready queue
    redis_client_raw = await test_redis_client.get_client()
    delayed_jobs = await redis_client_raw.zrange("job_queue_delayed", 0, -1)
    assert len(delayed_jobs) == 1

    # Force promotion
    job_json = delayed_jobs[0]
    await redis_client_raw.zadd("job_queue_delayed", {job_json: 0})
    await test_redis_client.transfer_ready_jobs_atomic()

    # Verify the message structure in ready queue
    ready_jobs = await redis_client_raw.zrange("job_queue", 0, -1)
    assert len(ready_jobs) == 1
    message_in_queue = json.loads(ready_jobs[0])
    assert message_in_queue["retry_attempt"] == 1, "Message must have retry_attempt=1"

    # Step 5: Create worker and consume actual queue message
    async with TestSessionLocal() as worker_session:
        worker = WorkerHeartbeat(
            worker_id=worker_id,
            started_at=datetime.utcnow(),
            last_heartbeat=datetime.utcnow(),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        worker_session.add(worker)
        await worker_session.commit()

    # Step 6: Process using actual queue dequeue (not manual job_id passing)
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch("app.worker.worker.redis_client", test_redis_client):
            worker_instance = Worker(worker_id)

            # Simulate single iteration of _processing_loop
            queue_service = QueueService(test_redis_client)
            message = await queue_service.dequeue_job()

            assert message is not None, "Message should be dequeued"
            assert message["job_id"] == str(job_id)
            assert message["retry_attempt"] == 1, "retry_attempt must be in dequeued message"

            # Extract values exactly as _processing_loop does
            dequeued_job_id = UUID(message["job_id"])
            dequeued_retry_attempt = message.get("retry_attempt")

            # Process with extracted values
            await worker_instance._process_job(dequeued_job_id, dequeued_retry_attempt)

    # Step 7: Verify successful completion
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.COMPLETED
        assert job.retry_count == 1
        assert job.worker_id == worker_id


@pytest.mark.asyncio
async def test_malformed_queue_messages_rejected(setup_teardown_db, test_redis_client):
    """Test malformed Redis messages cannot execute jobs.

    Verifies:
    - Missing retry_attempt for RETRYING job: claim fails
    - String retry_attempt: claim fails
    - Boolean retry_attempt: claim fails
    - Incorrect retry_attempt value: claim fails
    - Invalid job_id: handled gracefully

    None of these should execute the job handler or modify job state.
    """
    from app.services.queue_service import QueueService

    job_id = None

    # Create RETRYING job
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "malformed_test"},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),
        )
        setup_session.add(job)
        await setup_session.commit()
        job_id = job.id

    # Test cases: malformed messages
    test_cases = [
        {"job_id": str(job_id)},  # Missing retry_attempt
        {"job_id": str(job_id), "retry_attempt": "1"},  # String
        {"job_id": str(job_id), "retry_attempt": True},  # Boolean
        {"job_id": str(job_id), "retry_attempt": 0},  # Stale
        {"job_id": str(job_id), "retry_attempt": 2},  # Future
        {"job_id": "invalid-uuid", "retry_attempt": 1},  # Invalid UUID
    ]

    for test_message in test_cases:
        # Push malformed message to queue
        redis_client_raw = await test_redis_client.get_client()
        await redis_client_raw.zadd("job_queue", {json.dumps(test_message): 5})

        # Attempt to consume
        with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
            with patch("app.worker.worker.redis_client", test_redis_client):
                from app.worker.worker import Worker

                worker = Worker("worker-malformed-test")

                # Simulate single message processing
                queue_service = QueueService(test_redis_client)
                message = await queue_service.dequeue_job()

                if message and "job_id" in message:
                    try:
                        dequeued_job_id = UUID(message["job_id"])
                        dequeued_retry_attempt = message.get("retry_attempt")
                        await worker._process_job(dequeued_job_id, dequeued_retry_attempt)
                    except (ValueError, AttributeError):
                        # Invalid UUID is expected to fail gracefully
                        pass

    # Verify job state unchanged
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING, "Status should be unchanged"
        assert job.retry_count == 1, "Retry count should be unchanged"
        assert job.worker_id is None, "Should not be claimed"


# ========== Part B: Worker Restart and Ownership Audit ==========


@pytest.mark.asyncio
async def test_reused_worker_id_cannot_bypass_claim_fencing(setup_teardown_db, test_redis_client):
    """Test reused worker_id cannot bypass claim fencing.

    Critical security test for worker ID reuse (e.g., Kubernetes pod restart).

    Scenario:
    1. Worker A (id="worker-pod-1") starts job
    2. Worker A loses heartbeat, job recovered
    3. Worker A process terminates
    4. NEW Worker A (same id="worker-pod-1") starts
    5. Original job already claimed by Worker B
    6. New Worker A attempts to claim with stale retry_attempt
    7. Claim MUST fail (retry_count has moved forward)

    Verifies: worker_id alone is NOT sufficient - retry_attempt fencing protects.
    """
    from app.services.job_service import JobService

    job_id = None
    reused_worker_id = "worker-pod-1"  # Same ID across restarts

    # Phase 1: Original worker A starts job
    async with TestSessionLocal() as phase1_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "reuse_test"},
            status=JobStatus.QUEUED,
            retry_count=0,
            max_retries=3,
            priority=5,
            timeout=300,
        )
        phase1_session.add(job)
        await phase1_session.commit()
        job_id = job.id

    # Original worker A claims initial job
    async with TestSessionLocal() as claim1_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim1_session, test_redis_client, queue_service)

        claimed = await job_service.claim_job(
            job_id=job_id,
            worker_id=reused_worker_id,  # Original worker A
            retry_attempt=None,  # Initial claim
        )
        assert claimed is not None
        await claim1_session.commit()

    # Simulate worker A going stale, job recovered to retry_count=1
    async with TestSessionLocal() as recovery_session:
        result = await recovery_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        job.status = JobStatus.RETRYING
        job.retry_count = 1  # Incremented
        job.next_retry_at = datetime.utcnow() - timedelta(seconds=10)
        job.worker_id = None  # Cleared by recovery
        await recovery_session.commit()

    # Phase 2: NEW worker A starts (same ID after restart)
    # Attempts to claim with stale retry_attempt=0 (from before restart)
    async with TestSessionLocal() as claim2_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim2_session, test_redis_client, queue_service)

        claimed = await job_service.claim_job(
            job_id=job_id,
            worker_id=reused_worker_id,  # SAME worker_id
            retry_attempt=0,  # Stale - from before recovery
        )

        # MUST FAIL: retry_count is now 1, not 0
        assert claimed is None, "Reused worker_id with stale retry_attempt must fail"

    # Verify job state protected
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.RETRYING
        assert job.retry_count == 1
        assert job.worker_id is None


@pytest.mark.asyncio
async def test_reused_worker_id_heartbeat_vulnerability(setup_teardown_db, test_redis_client):
    """VULNERABILITY TEST: Reused worker_id can update heartbeat for job it doesn't own.

    Demonstrates a potential race condition:
    1. Worker A (id="worker-pod-1") claims job X (retry_count=1)
    2. Worker A loses heartbeat, job X recovered to retry_count=2
    3. Worker B claims job X with retry_attempt=2
    4. NEW Worker A restarts (same id="worker-pod-1")
    5. New Worker A has self.current_job_id=X from before restart (if preserved)
    6. New Worker A's heartbeat loop updates current_job_id=X
    7. This creates misleading heartbeat data

    Analysis:
    - Heartbeat updates do NOT validate job ownership
    - worker_service.update_heartbeat() does not check job.worker_id
    - This is a REPORTING issue, not a SECURITY issue
    - Job completion is still protected by expected_worker_id fencing
    - Heartbeat data may be stale/misleading across restarts

    Recommendation: Document that current_job_id in heartbeats is advisory only.
    Recovery decisions MUST use job.status and job.started_at, not heartbeat data.
    """
    from app.db.models import WorkerHeartbeat
    from app.services.worker_service import WorkerService

    job_id = uuid4()
    reused_worker_id = "worker-pod-1"

    # Setup: Job claimed by Worker B
    async with TestSessionLocal() as setup_session:
        job = Job(
            id=job_id,
            job_type="test_job",
            payload={"data": "heartbeat_test"},
            status=JobStatus.PROCESSING,
            worker_id="worker-b",  # Owned by Worker B
            started_at=datetime.utcnow(),
            retry_count=2,
            max_retries=3,
            priority=5,
            timeout=300,
        )
        setup_session.add(job)

        # Worker A heartbeat (reused ID, different process)
        worker_a_heartbeat = WorkerHeartbeat(
            worker_id=reused_worker_id,
            started_at=datetime.utcnow(),
            last_heartbeat=datetime.utcnow(),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        setup_session.add(worker_a_heartbeat)
        await setup_session.commit()

    # Action: Worker A (reused ID) updates heartbeat with job it doesn't own
    async with TestSessionLocal() as heartbeat_session:
        worker_service = WorkerService(heartbeat_session)

        # This succeeds without validation!
        await worker_service.update_heartbeat(
            worker_id=reused_worker_id, current_job_id=str(job_id)
        )
        await heartbeat_session.commit()

    # Verify: Heartbeat shows job_id but worker doesn't own it
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(
            select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == reused_worker_id)
        )
        heartbeat = result.scalar_one()

        assert heartbeat.current_job_id == job_id, "Heartbeat updated with job ID"

        # But job is owned by Worker B!
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.worker_id == "worker-b", "Job still owned by Worker B"

    # VULNERABILITY FINDING:
    # Heartbeat data is NOT authoritative for ownership.
    # This is a monitoring/observability issue, not a correctness issue.
    # Recovery MUST use job.status and job.started_at, not heartbeat.current_job_id.


@pytest.mark.asyncio
async def test_completion_fencing_protects_against_reused_worker_id(
    setup_teardown_db, test_redis_client
):
    """Test completion fencing protects even with reused worker_id.

    Scenario:
    1. Worker A (id="worker-pod-1") claims job with retry_attempt=1
    2. Job starts executing
    3. Worker A loses heartbeat, job recovered to retry_attempt=2
    4. Worker B claims with retry_attempt=2
    5. Original Worker A (still running, stale execution) attempts completion
    6. Completion MUST be fenced by expected_worker_id check

    Even though Worker A has the "correct" worker_id, the job was reassigned.
    The expected_worker_id fencing must still reject it.
    """
    from app.db.models import WorkerHeartbeat
    from app.services.job_service import JobService
    from app.models.job import JobUpdate

    job_id = None
    worker_id = "worker-pod-1"

    # Phase 1: Worker A claims and starts job
    async with TestSessionLocal() as phase1_session:
        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "completion_fence_test"},
            status=JobStatus.RETRYING,
            retry_count=1,
            max_retries=3,
            priority=5,
            timeout=300,
            next_retry_at=datetime.utcnow() - timedelta(seconds=10),
        )
        phase1_session.add(job)

        worker_heartbeat = WorkerHeartbeat(
            worker_id=worker_id,
            started_at=datetime.utcnow(),
            last_heartbeat=datetime.utcnow(),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        phase1_session.add(worker_heartbeat)
        await phase1_session.commit()
        job_id = job.id

    # Worker A claims
    async with TestSessionLocal() as claim_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim_session, test_redis_client, queue_service)

        claimed = await job_service.claim_job(job_id=job_id, worker_id=worker_id, retry_attempt=1)
        assert claimed is not None
        await claim_session.commit()

    # Phase 2: Job recovered and reclaimed by Worker B (different worker)
    async with TestSessionLocal() as reassign_session:
        result = await reassign_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        job.status = JobStatus.PROCESSING
        job.worker_id = "worker-b"  # Reassigned to Worker B
        await reassign_session.commit()

    # Phase 3: Stale Worker A (original execution) attempts completion
    async with TestSessionLocal() as completion_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(completion_session, test_redis_client, queue_service)

        # Attempt fenced completion
        updated = await job_service.update_job_status(
            job_id,
            JobUpdate(status=JobStatus.COMPLETED, result={"status": "stale"}),
            worker_id=worker_id,  # Original worker (same ID)
            expected_status=JobStatus.PROCESSING,
            expected_worker_id=worker_id,  # Expects to own it
        )

        # MUST FAIL: Job is now owned by Worker B
        assert updated is None, "Completion fencing must reject stale worker"

    # Verify job still owned by Worker B
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.PROCESSING
        assert job.worker_id == "worker-b"
        assert job.completed_at is None


# ========== CRITICAL REGRESSION: Same Worker ID Stale Completion ==========


@pytest.mark.asyncio
async def test_REGRESSION_reused_worker_id_stale_completion_overwrites_state(
    setup_teardown_db, test_redis_client
):
    """REGRESSION TEST: Stale execution is REJECTED when worker_id is reused.

    This test VERIFIES THE FIX for execution-generation fencing using retry_count.

    VULNERABILITY SEQUENCE (NOW FIXED):
    1. Worker A (id="worker-pod-1") claims initial job (retry_count=0)
    2. Worker A starts execution (long-running)
    3. Worker A heartbeat goes stale
    4. Recovery service marks job RETRYING (retry_count=1)
    5. NEW Worker A (same id="worker-pod-1") claims retry (retry_count=1)
    6. New Worker A starts fresh execution
    7. ORIGINAL Worker A (stale execution from step 2) completes
    8. Stale completion attempts fenced update:
       WHERE status=PROCESSING AND worker_id="worker-pod-1" AND retry_count=0
    9. FIX: Fencing REJECTS stale completion because:
       - Job IS PROCESSING (new execution) ✓
       - Job worker_id IS "worker-pod-1" (new execution) ✓
       - Job retry_count is 1 (new execution) ✗ (expected 0 from stale)
    10. Stale result is REJECTED, new execution can complete safely

    ROOT CAUSE (FIXED):
    - Previously: Fencing only checked expected_status and expected_worker_id
    - Now: Fencing also checks expected_retry_count (execution generation)
    - Old execution (retry_count=0) CANNOT complete job at retry_count=1

    EXPECTED BEHAVIOR (NOW IMPLEMENTED):
    - Completion fences on retry_count as execution generation marker
    - Old execution (retry_count=0) is REJECTED when job is at retry_count=1
    - New execution can complete successfully after stale rejection

    VERIFIED BEHAVIOR:
    - Stale completion is REJECTED (update returns None)
    - Job remains PROCESSING with new execution's state
    - New execution can complete successfully
    """
    from app.services.job_service import JobService
    from app.models.job import JobUpdate

    job_id = None
    reused_worker_id = "worker-pod-1"

    # Phase 1: Initial execution starts
    async with TestSessionLocal() as phase1_session:
        from app.db.models import WorkerHeartbeat

        job = Job(
            id=uuid4(),
            job_type="test_job",
            payload={"data": "regression_test", "processing_time": 10.0},  # Long job
            status=JobStatus.QUEUED,
            retry_count=0,
            max_retries=3,
            priority=5,
            timeout=300,
        )
        phase1_session.add(job)

        worker = WorkerHeartbeat(
            worker_id=reused_worker_id,
            started_at=datetime.utcnow(),
            last_heartbeat=datetime.utcnow(),
            is_active=True,
            jobs_processed=0,
            jobs_failed=0,
        )
        phase1_session.add(worker)
        await phase1_session.commit()
        job_id = job.id

    # Worker A claims initial job
    async with TestSessionLocal() as claim1_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim1_session, test_redis_client, queue_service)

        claimed = await job_service.claim_job(
            job_id=job_id,
            worker_id=reused_worker_id,
            retry_attempt=None,  # Initial claim
        )
        assert claimed is not None
        assert claimed.retry_count == 0
        await claim1_session.commit()

    original_execution_retry_count = 0  # Original execution is at retry_count=0

    # Phase 2: Simulate job going stale, recovery
    async with TestSessionLocal() as recovery_session:
        result = await recovery_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        job.status = JobStatus.RETRYING
        job.retry_count = 1  # Incremented by recovery
        job.next_retry_at = datetime.utcnow() - timedelta(seconds=10)
        job.worker_id = None  # Cleared by recovery
        await recovery_session.commit()

    # Phase 3: NEW Worker A (restarted, same ID) claims retry
    async with TestSessionLocal() as claim2_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(claim2_session, test_redis_client, queue_service)

        claimed = await job_service.claim_job(
            job_id=job_id,
            worker_id=reused_worker_id,  # SAME worker_id
            retry_attempt=1,  # New retry attempt
        )
        assert claimed is not None
        assert claimed.retry_count == 1
        await claim2_session.commit()

    new_execution_retry_count = 1  # New execution is at retry_count=1

    # Verify new execution state
    async with TestSessionLocal() as verify_session:
        result = await verify_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()
        assert job.status == JobStatus.PROCESSING
        assert job.worker_id == reused_worker_id
        assert job.retry_count == 1

    # Phase 4: ORIGINAL stale execution (from Phase 1) attempts completion
    # This simulates the old execution finally finishing after recovery
    async with TestSessionLocal() as stale_completion_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(stale_completion_session, test_redis_client, queue_service)

        # Stale execution attempts fenced completion with:
        # - expected_status=PROCESSING ✓ (matches new execution)
        # - expected_worker_id=reused_worker_id ✓ (matches new execution)
        # - expected_retry_count=0 ✗ (does NOT match new execution at retry_count=1)

        stale_result = {"status": "stale_execution", "retry_count": original_execution_retry_count}

        updated = await job_service.update_job_status(
            job_id,
            JobUpdate(status=JobStatus.COMPLETED, result=stale_result),
            worker_id=reused_worker_id,
            expected_status=JobStatus.PROCESSING,
            expected_worker_id=reused_worker_id,
            expected_retry_count=original_execution_retry_count,  # 0 (stale generation)
        )

        # FIX VERIFICATION: Stale completion should be REJECTED
        assert updated is None, (
            "Stale completion should be REJECTED by retry_count fencing. "
            f"Old execution (retry_count={original_execution_retry_count}) "
            f"cannot complete job at retry_count={new_execution_retry_count}."
        )

        # Verify job state UNCHANGED after stale rejection
        result = await stale_completion_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.PROCESSING, "Job should remain PROCESSING"
        assert job.worker_id == reused_worker_id, "worker_id should remain unchanged"
        assert (
            job.retry_count == new_execution_retry_count
        ), "retry_count should remain at new generation"
        assert job.result is None, "Stale result should NOT be written"

    # Phase 5: NEW execution completes successfully
    async with TestSessionLocal() as new_completion_session:
        queue_service = QueueService(redis=test_redis_client)
        job_service = JobService(new_completion_session, test_redis_client, queue_service)

        # New execution completes with correct generation
        new_result = {"status": "new_execution", "retry_count": new_execution_retry_count}

        updated = await job_service.update_job_status(
            job_id,
            JobUpdate(status=JobStatus.COMPLETED, result=new_result),
            worker_id=reused_worker_id,
            expected_status=JobStatus.PROCESSING,
            expected_worker_id=reused_worker_id,
            expected_retry_count=new_execution_retry_count,  # 1 (current generation)
        )

        assert updated is not None, "New execution completion should succeed"
        await new_completion_session.commit()

        # Verify new execution's result is persisted
        result = await new_completion_session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one()

        assert job.status == JobStatus.COMPLETED, "Job should be COMPLETED"
        assert job.result == new_result, "New execution's result should be persisted"
        assert (
            job.result["retry_count"] == new_execution_retry_count
        ), "Result should be from new execution"
        assert (
            job.retry_count == new_execution_retry_count
        ), "retry_count should remain at new generation"
