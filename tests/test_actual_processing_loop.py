"""
Tests for the ACTUAL Worker._processing_loop() implementation.

These tests invoke the real production _processing_loop(), not manual dequeue + _process_job().
Uses monkeypatching to inject test Redis client into the worker module.
"""

import pytest
import pytest_asyncio
import asyncio
import json
from datetime import datetime, timedelta

from sqlalchemy import select

from app.db.models import Job, JobStatus, JobPriority, JobExecution
from app.worker.worker import Worker
from app.core.redis_client import RedisClient
from tests.conftest import TestSessionLocal, TEST_REDIS_URL


@pytest_asyncio.fixture
async def test_redis():
    """Redis client for testing on DB 15"""
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


async def enqueue_message(
    test_redis, job_id: str, job_type: str, priority: int, retry_attempt=None
):
    """Helper to enqueue a message with optional retry_attempt"""
    client = await test_redis.get_client()
    message = {
        "job_id": job_id,
        "job_type": job_type,
        "priority": priority,
    }
    if retry_attempt is not None:
        message["retry_attempt"] = retry_attempt
    await client.zadd("job_queue", {json.dumps(message): priority})


async def run_worker_until_job_completed(
    worker, db_session, job, test_redis, monkeypatch, timeout_seconds=10
):
    """
    Run actual worker _processing_loop() until job completes or timeout.

    Uses monkeypatch to inject test Redis client into worker module.
    """
    import app.worker.worker
    from unittest.mock import patch

    # Monkeypatch the module-level redis_client used by Worker._processing_loop()
    monkeypatch.setattr(app.worker.worker, "redis_client", test_redis)

    loop_task = None
    try:
        with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
            worker.running = True
            # Create task for actual _processing_loop()
            loop_task = asyncio.create_task(worker._processing_loop())

            # Poll job status until completed or timeout
            for _ in range(timeout_seconds * 10):  # Poll every 0.1s
                await asyncio.sleep(0.1)
                await db_session.refresh(job)
                if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.DEAD_LETTER):
                    break
    finally:
        # Always stop worker and clean up task
        worker.running = False
        if loop_task:
            loop_task.cancel()
            try:
                await loop_task
            except asyncio.CancelledError:
                pass


@pytest.mark.asyncio
async def test_real_loop_processes_queued_job(db_session, test_redis, monkeypatch):
    """
    Test 1: Real _processing_loop() processes a QUEUED job to completion.
    """
    # Create QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Enqueue message
    await enqueue_message(test_redis, str(job.id), "test_job", 5, retry_attempt=None)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-1")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.worker_id == "test-loop-worker-1"
    assert job.result is not None


@pytest.mark.asyncio
async def test_real_loop_processes_retrying_job(db_session, test_redis, monkeypatch):
    """
    Test 2: Real _processing_loop() processes a due RETRYING job with correct retry_attempt.
    """
    # Create RETRYING job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=1,
        max_retries=3,
        timeout=300,
        next_retry_at=datetime.utcnow() - timedelta(seconds=1),
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Enqueue with matching retry_attempt
    await enqueue_message(test_redis, str(job.id), "test_job", 5, retry_attempt=1)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-2")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.retry_count == 1


@pytest.mark.asyncio
async def test_real_loop_handles_malformed_uuid(db_session, test_redis, monkeypatch):
    """
    Test 3: Real _processing_loop() handles malformed UUID without terminating.
    """
    # Enqueue invalid UUID message
    await enqueue_message(test_redis, "not-a-valid-uuid", "test_job", 3, retry_attempt=None)

    # Enqueue valid job after invalid one
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    await enqueue_message(test_redis, str(job.id), "test_job", 7, retry_attempt=None)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-3")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify valid job completed despite invalid UUID
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_real_loop_handles_missing_job_id(db_session, test_redis, monkeypatch):
    """
    Test 4: Real _processing_loop() handles missing job_id without terminating.
    """
    # Push message without job_id
    client = await test_redis.get_client()
    message = json.dumps({"job_type": "test_job", "priority": 5})
    await client.zadd("job_queue", {message: 3})

    # Enqueue valid job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    await enqueue_message(test_redis, str(job.id), "test_job", 7, retry_attempt=None)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-4")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify valid job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_real_loop_handles_malformed_json(db_session, test_redis, monkeypatch):
    """
    Test 5: Real _processing_loop() handles malformed JSON according to production behavior.

    Production behavior: dequeue_job() returns None on JSON parse failure.
    Loop continues to next iteration.
    """
    # Push malformed JSON
    client = await test_redis.get_client()
    await client.zadd("job_queue", {"invalid json{{{": 3})

    # Enqueue valid job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    await enqueue_message(test_redis, str(job.id), "test_job", 7, retry_attempt=None)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-5")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify valid job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_real_loop_processes_valid_after_invalid(db_session, test_redis, monkeypatch):
    """
    Test 6: Real _processing_loop() processes valid message after invalid message.
    """
    # Push multiple invalid messages
    client = await test_redis.get_client()
    await client.zadd("job_queue", {"not json": 2})
    await client.zadd("job_queue", {json.dumps({"job_id": "bad-uuid"}): 3})

    # Enqueue valid job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    await enqueue_message(test_redis, str(job.id), "test_job", 8, retry_attempt=None)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-6")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify valid job completed
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_real_loop_prevents_duplicate_execution(db_session, test_redis, monkeypatch):
    """
    Test 7: Real _processing_loop() prevents duplicate execution from duplicate messages.
    """
    # Create QUEUED job
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 0.05},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Enqueue same job twice
    await enqueue_message(test_redis, str(job.id), "test_job", 5, retry_attempt=None)
    await enqueue_message(test_redis, str(job.id), "test_job", 5, retry_attempt=None)

    # Run real _processing_loop()
    worker = Worker("test-loop-worker-7")
    await run_worker_until_job_completed(worker, db_session, job, test_redis, monkeypatch)

    # Verify job completed once
    await db_session.refresh(job)
    assert job.status == JobStatus.COMPLETED

    # Verify only ONE execution record
    result = await db_session.execute(select(JobExecution).where(JobExecution.job_id == job.id))
    executions = result.scalars().all()
    assert len(executions) == 1


@pytest.mark.asyncio
async def test_real_loop_rejects_stale_retry_attempt(db_session, test_redis, monkeypatch):
    """
    Test 8: Real _processing_loop() rejects stale retry_attempt (cannot claim newer generation).
    """
    # Create RETRYING job at generation 2
    job = Job(
        job_type="test_job",
        payload={"data": "test"},
        status=JobStatus.RETRYING,
        priority=JobPriority.NORMAL.value,
        retry_count=2,
        max_retries=3,
        timeout=300,
        next_retry_at=datetime.utcnow() - timedelta(seconds=1),
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Enqueue with stale retry_attempt=1 (current is 2)
    await enqueue_message(test_redis, str(job.id), "test_job", 5, retry_attempt=1)

    # Run real _processing_loop() briefly (job should not be claimed)
    import app.worker.worker
    from unittest.mock import patch

    monkeypatch.setattr(app.worker.worker, "redis_client", test_redis)

    worker = Worker("test-loop-worker-8")
    loop_task = None
    try:
        with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
            worker.running = True
            loop_task = asyncio.create_task(worker._processing_loop())

            # Wait a bit for worker to process message
            await asyncio.sleep(2)
    finally:
        worker.running = False
        if loop_task:
            loop_task.cancel()
            try:
                await loop_task
            except asyncio.CancelledError:
                pass

    # Verify job NOT executed (still RETRYING, not claimed)
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING
    assert job.worker_id is None


@pytest.mark.asyncio
async def test_real_loop_cancellation(db_session, test_redis, monkeypatch):
    """
    Test 9: Real _processing_loop() handles cancellation predictably.
    """
    import app.worker.worker
    from unittest.mock import patch

    monkeypatch.setattr(app.worker.worker, "redis_client", test_redis)

    # Create job that won't complete quickly
    job = Job(
        job_type="test_job",
        payload={"data": "test", "processing_time": 10.0},
        status=JobStatus.QUEUED,
        priority=JobPriority.NORMAL.value,
        retry_count=0,
        max_retries=3,
        timeout=300,
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    await enqueue_message(test_redis, str(job.id), "test_job", 5, retry_attempt=None)

    # Start loop and cancel it
    worker = Worker("test-loop-worker-9")
    loop_task = None
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        worker.running = True
        loop_task = asyncio.create_task(worker._processing_loop())

        # Let it start
        await asyncio.sleep(0.5)

        # Cancel
        worker.running = False
        loop_task.cancel()

        # Should exit cleanly
        try:
            await loop_task
        except asyncio.CancelledError:
            pass  # Expected

    # Verify task is done
    assert loop_task.done()


@pytest.mark.asyncio
async def test_real_loop_continues_when_queue_empty(db_session, test_redis, monkeypatch):
    """
    Test 10: Real _processing_loop() continues polling when queue is empty.

    Verifies the loop doesn't exit and continues to poll for new jobs.
    """
    import app.worker.worker
    from unittest.mock import patch

    monkeypatch.setattr(app.worker.worker, "redis_client", test_redis)

    # Track sleep calls to verify polling behavior
    sleep_calls = []
    original_sleep = asyncio.sleep

    async def mock_sleep(delay):
        sleep_calls.append(delay)
        if len(sleep_calls) >= 3:  # Stop after 3 polls
            raise asyncio.CancelledError()
        await original_sleep(0.01)  # Small actual sleep

    worker = Worker("test-loop-worker-10")
    with patch("app.worker.worker.AsyncSessionLocal", TestSessionLocal):
        with patch("asyncio.sleep", side_effect=mock_sleep):
            worker.running = True
            try:
                await worker._processing_loop()
            except asyncio.CancelledError:
                pass  # Expected from mock_sleep

    # Verify sleep was called (loop didn't exit on empty queue)
    assert len(sleep_calls) >= 2
    # Verify sleep duration is 1 second (production behavior)
    assert all(delay == 1 for delay in sleep_calls)
