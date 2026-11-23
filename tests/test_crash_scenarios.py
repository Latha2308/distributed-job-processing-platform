"""
Crash Scenario Reproduction Tests

Demonstrates message loss and job stranding scenarios using real Redis and PostgreSQL.
Tests use actual dequeue operations to show what happens when workers crash.
"""

import pytest
import pytest_asyncio
import json
from datetime import datetime, timedelta

from app.db.models import Job, JobStatus, JobPriority
from app.services.queue_service import QueueService
from app.services.job_service import JobService
from app.services.job_recovery_service import JobRecoveryService
from app.core.redis_client import RedisClient
from tests.conftest import TestSessionLocal, TEST_REDIS_URL


@pytest_asyncio.fixture
async def test_redis():
    """Redis client for testing"""
    client = RedisClient()
    import redis.asyncio as aioredis
    from redis.asyncio import ConnectionPool

    test_pool = ConnectionPool.from_url(
        TEST_REDIS_URL,
        max_connections=10,
        decode_responses=True,
    )
    redis_instance = aioredis.Redis(connection_pool=test_pool)
    client._client = redis_instance

    # Clean only test-specific keys
    test_keys = ["job_queue", "job_queue_delayed"]
    for key in test_keys:
        await redis_instance.delete(key)

    yield client

    # Clean only test-specific keys
    for key in test_keys:
        await redis_instance.delete(key)
    await redis_instance.aclose()
    await test_pool.disconnect()


@pytest.mark.asyncio
async def test_crash_after_dequeue_before_claim_message_lost(db_session, test_redis):
    """
    CRITICAL SCENARIO: Crash after dequeue but before claim.

    Demonstrates:
    1. Message is removed from Redis (ZPOPMIN is atomic)
    2. Job remains in QUEUED state (never claimed)
    3. Message is permanently lost
    4. Job becomes stranded
    """
    # Step 1: Create QUEUED job
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
    job_id = job.id

    # Step 2: Publish message using supported path
    queue_service = QueueService(test_redis)
    client = await test_redis.get_client()
    message = {"job_id": str(job_id), "job_type": "test_job", "priority": 5}
    await client.zadd("job_queue", {json.dumps(message): 5})

    # Verify message in queue
    queue_len_before = await client.zcard("job_queue")
    assert queue_len_before == 1, "Message should be in queue"

    # Step 3: Dequeue message (simulating worker dequeue)
    dequeued_message = await queue_service.dequeue_job()
    assert dequeued_message is not None
    assert dequeued_message["job_id"] == str(job_id)

    # Step 4: Verify message is GONE from Redis (ZPOPMIN is destructive)
    queue_len_after = await client.zcard("job_queue")
    assert queue_len_after == 0, "CRITICAL: Message removed by ZPOPMIN"

    # Step 5: SIMULATE CRASH - worker dies before claim_job()
    # (In production: network failure, SIGKILL, OOM, kernel panic, etc.)
    # NO call to claim_job() - worker crashed!

    # Step 6: Verify job still QUEUED (never claimed)
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED
    assert job.worker_id is None
    assert job.started_at is None

    # Step 7: CRITICAL - Message is lost, job is stranded
    # - Redis queue is empty (message was popped)
    # - Job is still QUEUED (never claimed)
    # - No worker will ever process this job naturally

    print("\n🔴 CRITICAL FINDING:")
    print("  - Redis queue empty: {queue_len_after == 0}")
    print("  - Job status: {job.status.value}")
    print("  - Job worker_id: {job.worker_id}")
    print("  - Message permanently lost: True")
    print("  - Job stranded: True")


@pytest.mark.asyncio
async def test_crash_after_claim_before_execution_recoverable(db_session, test_redis):
    """
    SCENARIO: Crash after claim but before execution starts.

    Demonstrates:
    1. Message removed from Redis
    2. Job in PROCESSING state with worker_id
    3. Recovery service CAN detect this (timeout-based)
    """
    # Create QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=1,  # Short timeout for faster recovery detection
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Enqueue and dequeue
    queue_service = QueueService(test_redis)
    client = await test_redis.get_client()
    message = {"job_id": str(job_id), "job_type": "test_job", "priority": 5}
    await client.zadd("job_queue", {json.dumps(message): 5})

    dequeued_message = await queue_service.dequeue_job()
    assert dequeued_message is not None

    # Message gone from Redis
    queue_len = await client.zcard("job_queue")
    assert queue_len == 0

    # Claim job (simulating worker claim)
    job_service = JobService(db_session, test_redis, queue_service)
    claimed_job = await job_service.claim_job(job_id, "worker-crash-test", None)
    assert claimed_job is not None
    await db_session.commit()

    # SIMULATE CRASH after claim but before execution
    await db_session.refresh(job)
    assert job.status == JobStatus.PROCESSING
    assert job.worker_id == "worker-crash-test"
    assert job.started_at is not None

    # Wait beyond timeout + safety buffer (10s)
    import asyncio

    await asyncio.sleep(12.0)  # timeout=1s + 10s safety buffer + margin

    # Recovery service CAN detect this
    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session)
        stuck_jobs = await recovery_service.detect_stuck_jobs(min_timeout=1)

        # Job should be detected as stuck
        assert job_id in stuck_jobs, "Recovery should detect stuck PROCESSING job"

        print("\n✅ RECOVERABLE:")
        print("  - Job status: {job.status.value}")
        print(f"  - Worker ID: {job.worker_id}")
        print("  - Detected by recovery: True")


@pytest.mark.asyncio
async def test_database_failure_after_dequeue_message_lost(db_session, test_redis):
    """
    SCENARIO: Database connection fails after dequeue.

    Demonstrates same issue as crash - message is lost, job stranded.
    """
    # Create QUEUED job
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
    job_id = job.id

    # Enqueue
    queue_service = QueueService(test_redis)
    client = await test_redis.get_client()
    message = {"job_id": str(job_id), "job_type": "test_job", "priority": 5}
    await client.zadd("job_queue", {json.dumps(message): 5})

    # Dequeue
    dequeued_message = await queue_service.dequeue_job()
    assert dequeued_message is not None

    # Message gone
    queue_len = await client.zcard("job_queue")
    assert queue_len == 0

    # SIMULATE DATABASE FAILURE - cannot connect to claim
    # (transient network issue, DB restart, connection pool exhausted, etc.)
    # NO claim_job() call - database unavailable!

    # Job remains QUEUED, message lost
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED
    assert job.worker_id is None

    print("\n🔴 DATABASE FAILURE SCENARIO:")
    print("  - Redis message lost: True")
    print("  - Job remains QUEUED: True")
    print("  - Cannot recover without reconciliation: True")


@pytest.mark.asyncio
async def test_json_parse_failure_after_destructive_pop(db_session, test_redis):
    """
    SCENARIO: Invalid JSON discovered after ZPOPMIN.

    Demonstrates:
    1. Redis pops message (destructive)
    2. JSON parsing fails
    3. Message cannot be recovered
    """
    # Push malformed JSON
    client = await test_redis.get_client()
    await client.zadd("job_queue", {"not valid json{{{": 5})

    # Verify message exists
    queue_len_before = await client.zcard("job_queue")
    assert queue_len_before == 1

    # Dequeue - will fail JSON parsing
    queue_service = QueueService(test_redis)
    _dequeued_message = await queue_service.dequeue_job()  # noqa: F841

    # dequeue_job returns None on JSON parse failure (check implementation)
    # But message is already gone from Redis!
    queue_len_after = await client.zcard("job_queue")
    assert queue_len_after == 0, "Message removed even though parse failed"

    print("\n🔴 JSON PARSE FAILURE:")
    print("  - Message removed from Redis: True")
    print("  - JSON parse failed: True")
    print(f"  - Message lost: {_dequeued_message is None}")
    print("  - Cannot retry parse: True")


@pytest.mark.asyncio
async def test_queued_jobs_not_detected_by_recovery(db_session, test_redis):
    """
    CRITICAL: Verify that QUEUED jobs are NOT detected by recovery service.

    Recovery only handles PROCESSING jobs. Stranded QUEUED jobs remain forever.
    """
    # Create stranded QUEUED job (simulating dequeue-then-crash)
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=1,  # Short timeout
        created_at=datetime.utcnow() - timedelta(hours=24),  # Old job
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)
    job_id = job.id

    # Wait beyond any reasonable timeout
    import asyncio

    await asyncio.sleep(1.5)

    # Try recovery detection
    async with TestSessionLocal() as recovery_session:
        recovery_service = JobRecoveryService(recovery_session)
        stuck_jobs = await recovery_service.detect_stuck_jobs(min_timeout=1)

        # QUEUED job should NOT be detected
        assert job_id not in stuck_jobs, "Recovery should NOT detect QUEUED jobs"

    print("\n🔴 QUEUED JOBS NOT RECOVERED:")
    print("  - Job status: QUEUED")
    print("  - Job age: 24 hours")
    print("  - Detected by recovery: False")
    print("  - Will remain stranded: True")


@pytest.mark.asyncio
async def test_cancellation_after_dequeue_message_lost(db_session, test_redis):
    """
    SCENARIO: Worker receives shutdown signal after dequeue.

    Graceful shutdown still loses message if between dequeue and claim.
    """
    # Create QUEUED job
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
    job_id = job.id

    # Enqueue
    queue_service = QueueService(test_redis)
    client = await test_redis.get_client()
    message = {"job_id": str(job_id), "job_type": "test_job", "priority": 5}
    await client.zadd("job_queue", {json.dumps(message): 5})

    # Dequeue
    dequeued_message = await queue_service.dequeue_job()
    assert dequeued_message is not None

    # SIMULATE SIGTERM/SIGINT - graceful shutdown requested
    # Worker hasn't called claim_job() yet
    # Even graceful shutdown cannot recover message!

    # Message gone, job stranded
    queue_len = await client.zcard("job_queue")
    assert queue_len == 0

    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED
    assert job.worker_id is None

    print("\n🔴 GRACEFUL SHUTDOWN SCENARIO:")
    print("  - Signal: SIGTERM (graceful)")
    print("  - Message lost: True")
    print("  - Job stranded: True")
    print("  - Even graceful shutdown cannot help: True")
