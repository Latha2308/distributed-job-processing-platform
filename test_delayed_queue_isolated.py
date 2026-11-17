"""
Isolated tests for delayed queue functionality.

SAFETY:
- Uses unique test-specific queue names (test_main_*, test_delayed_*, test_quarantine_*)
- Cleanup deletes ONLY test-specific queues (via try/finally)
- Does NOT modify production queues (job_queue, job_queue_delayed, job_queue_invalid)
- Does NOT modify PostgreSQL data
- Creates a fresh connection pool and RedisClient per test to avoid event loop conflicts
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool

from app.core.config import settings
from app.core.redis_client import RedisClient


def normalize_redis_value(value):
    """Normalize Redis value to string (handles both bytes and str)"""
    if isinstance(value, bytes):
        return value.decode('utf-8')
    return value


class TestDelayedQueueIsolated:
    """Isolated tests for delayed queue with unique test queues"""

    @pytest_asyncio.fixture
    async def redis_test_client(self):
        """Create fresh test Redis client with test-specific connection pool (one per test)"""
        # Generate unique queue names for this test BEFORE try/finally
        session_id = str(uuid.uuid4())[:8]
        test_main_queue = f"test_main_queue_{session_id}"
        test_delayed_queue = f"test_delayed_queue_{session_id}"
        test_quarantine_queue = f"test_quarantine_queue_{session_id}"
        
        # Create a fresh connection pool for THIS TEST ONLY
        # This pool will be bound to this test's event loop
        test_pool = ConnectionPool.from_url(
            str(settings.REDIS_URL),
            max_connections=10,
            decode_responses=True,
        )
        
        # Create a fresh RedisClient instance for this test
        test_redis_client = RedisClient()
        
        # Inject the test-specific pool by creating the Redis client directly
        # This bypasses the module-level shared redis_pool
        test_redis_client._client = aioredis.Redis(connection_pool=test_pool)
        
        # Get the raw client for direct Redis operations
        raw_client = await test_redis_client.get_client()
        
        try:
            # Ensure test queues are empty before starting
            await raw_client.delete(test_main_queue)
            await raw_client.delete(test_delayed_queue)
            await raw_client.delete(test_quarantine_queue)
            
            yield {
                "client": test_redis_client,  # RedisClient with test-specific pool
                "raw_client": raw_client,  # Direct Redis client for raw operations
                "main_queue": test_main_queue,
                "delayed_queue": test_delayed_queue,
                "quarantine_queue": test_quarantine_queue,
                "test_pool": test_pool,  # Keep reference for cleanup
            }
        finally:
            # Cleanup: Delete ONLY test-specific queues (even if tests fail)
            try:
                await raw_client.delete(test_main_queue)
                await raw_client.delete(test_delayed_queue)
                await raw_client.delete(test_quarantine_queue)
            finally:
                # Close the test Redis client to release connections
                await test_redis_client.close()
                # Close the test-specific connection pool before loop closes
                await test_pool.aclose()

    @pytest.mark.asyncio
    async def test_push_to_delayed_queue_valid(self, redis_test_client):
        """Test pushing valid job to delayed queue"""
        client = redis_test_client["client"]
        delayed_queue = redis_test_client["delayed_queue"]
        
        job_data = {
            "job_id": str(uuid.uuid4()),
            "job_type": "test_job",
            "priority": 5,
            "payload": {"test": "data"}
        }
        
        execute_at = time.time() + 10  # 10 seconds in future
        
        result = await client.push_to_delayed_queue(job_data, execute_at, delayed_queue)
        
        assert result is True, "Should successfully add valid job"
        
        # Verify job is in delayed queue
        length = await client.delayed_queue_length(delayed_queue)
        assert length == 1, "Delayed queue should contain 1 job"

    @pytest.mark.asyncio
    async def test_push_to_delayed_queue_invalid_priority(self, redis_test_client):
        """Test rejection of invalid priority values"""
        client = redis_test_client["client"]
        delayed_queue = redis_test_client["delayed_queue"]
        
        execute_at = time.time() + 10
        
        # Test boolean priority (rejected)
        job_bool = {
            "job_id": str(uuid.uuid4()),
            "job_type": "test_job",
            "priority": True  # Invalid: boolean
        }
        result = await client.push_to_delayed_queue(job_bool, execute_at, delayed_queue)
        assert result is False, "Should reject boolean priority"
        
        # Test out-of-range priority
        job_out_of_range = {
            "job_id": str(uuid.uuid4()),
            "job_type": "test_job",
            "priority": 15  # Invalid: > 10
        }
        result = await client.push_to_delayed_queue(job_out_of_range, execute_at, delayed_queue)
        assert result is False, "Should reject priority > 10"
        
        # Test negative priority
        job_negative = {
            "job_id": str(uuid.uuid4()),
            "job_type": "test_job",
            "priority": -1  # Invalid: < 0
        }
        result = await client.push_to_delayed_queue(job_negative, execute_at, delayed_queue)
        assert result is False, "Should reject negative priority"
        
        # Verify nothing was added
        length = await client.delayed_queue_length(delayed_queue)
        assert length == 0, "No jobs should be added for invalid priorities"

    @pytest.mark.asyncio
    async def test_push_to_delayed_queue_invalid_execute_at(self, redis_test_client):
        """Test rejection of NaN and infinity for execute_at"""
        client = redis_test_client["client"]
        delayed_queue = redis_test_client["delayed_queue"]
        
        job_data = {
            "job_id": str(uuid.uuid4()),
            "job_type": "test_job",
            "priority": 5
        }
        
        # Test NaN
        result = await client.push_to_delayed_queue(job_data, float('nan'), delayed_queue)
        assert result is False, "Should reject NaN execute_at"
        
        # Test positive infinity
        result = await client.push_to_delayed_queue(job_data, float('inf'), delayed_queue)
        assert result is False, "Should reject +infinity execute_at"
        
        # Test negative infinity
        result = await client.push_to_delayed_queue(job_data, float('-inf'), delayed_queue)
        assert result is False, "Should reject -infinity execute_at"
        
        # Verify nothing was added
        length = await client.delayed_queue_length(delayed_queue)
        assert length == 0, "No jobs should be added for invalid execute_at"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_malformed_json(self, redis_test_client):
        """Test Lua quarantines malformed JSON"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add malformed JSON directly to delayed queue
        malformed_payloads = [
            '{invalid json}',  # Syntax error
            '{"job_id": "test", "unclosed":',  # Incomplete
            'not json at all',  # Plain text
        ]
        
        for payload in malformed_payloads:
            await raw_client.zadd(delayed_queue, {payload: current_time - 1})
        
        # Transfer should quarantine all malformed JSON
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 0, "Should not transfer malformed JSON"
        
        # Verify all malformed jobs are quarantined
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 3, "All 3 malformed JSON payloads should be quarantined"
        
        # Verify each malformed payload is in quarantine (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        for payload in malformed_payloads:
            assert payload in quarantined_items, f"Malformed payload '{payload[:20]}...' should be in quarantine"
        
        # Verify delayed queue is empty
        delayed_len = await raw_client.zcard(delayed_queue)
        assert delayed_len == 0, "Delayed queue should be empty after quarantine"
        
        # Verify main queue is empty
        main_len = await raw_client.zcard(main_queue)
        assert main_len == 0, "Main queue should be empty (no valid transfers)"
        
        # Verify malformed payloads are NOT in main or delayed queues (normalize bytes/str)
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        delayed_items_raw = await raw_client.zrange(delayed_queue, 0, -1)
        delayed_items = [normalize_redis_value(item) for item in delayed_items_raw]
        for payload in malformed_payloads:
            assert payload not in main_items, f"Malformed payload should NOT be in main queue"
            assert payload not in delayed_items, f"Malformed payload should NOT be in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_empty_array(self, redis_test_client):
        """Test Lua quarantines empty arrays []"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add empty array (valid JSON but invalid job structure)
        empty_array = json.dumps([])
        await raw_client.zadd(delayed_queue, {empty_array: current_time - 1})
        
        # Transfer should quarantine empty array
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 0, "Should not transfer empty array"
        
        # Verify quarantined
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 1, "Empty array should be quarantined"
        
        # Verify payload in quarantine is the empty array (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        assert empty_array in quarantined_items, "Quarantined item should be []"
        
        # Verify NOT in main or delayed queues (normalize bytes/str)
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        delayed_items_raw = await raw_client.zrange(delayed_queue, 0, -1)
        delayed_items = [normalize_redis_value(item) for item in delayed_items_raw]
        assert empty_array not in main_items, "Empty array should NOT be in main queue"
        assert empty_array not in delayed_items, "Empty array should NOT be in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_empty_object(self, redis_test_client):
        """Test Lua quarantines empty objects {}"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add empty object (valid JSON but invalid job structure)
        empty_object = json.dumps({})
        await raw_client.zadd(delayed_queue, {empty_object: current_time - 1})
        
        # Transfer should quarantine empty object
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 0, "Should not transfer empty object"
        
        # Verify quarantined
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 1, "Empty object should be quarantined"
        
        # Verify payload in quarantine is the empty object (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        assert empty_object in quarantined_items, "Quarantined item should be {}"
        
        # Verify NOT in main or delayed queues (normalize bytes/str)
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        delayed_items_raw = await raw_client.zrange(delayed_queue, 0, -1)
        delayed_items = [normalize_redis_value(item) for item in delayed_items_raw]
        assert empty_object not in main_items, "Empty object should NOT be in main queue"
        assert empty_object not in delayed_items, "Empty object should NOT be in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_non_empty_array(self, redis_test_client):
        """Test Lua quarantines non-empty arrays"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add non-empty arrays
        arrays = [
            json.dumps(["item1", "item2"]),
            json.dumps([1, 2, 3]),
            json.dumps([{"nested": "object"}]),
        ]
        
        for array in arrays:
            await raw_client.zadd(delayed_queue, {array: current_time - 1})
        
        # Transfer should quarantine all arrays
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 0, "Should not transfer arrays"
        
        # Verify all quarantined
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 3, "All 3 arrays should be quarantined"
        
        # Verify each array is in quarantine (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        for array in arrays:
            assert array in quarantined_items, f"Array {array[:20]}... should be in quarantine"
        
        # Verify arrays are NOT in main or delayed queues (normalize bytes/str)
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        delayed_items_raw = await raw_client.zrange(delayed_queue, 0, -1)
        delayed_items = [normalize_redis_value(item) for item in delayed_items_raw]
        for array in arrays:
            assert array not in main_items, f"Array should NOT be in main queue"
            assert array not in delayed_items, f"Array should NOT be in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_missing_fields(self, redis_test_client):
        """Test Lua quarantines jobs with missing required fields"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Jobs with missing fields
        invalid_jobs = [
            json.dumps({"job_type": "test", "priority": 5}),  # Missing job_id
            json.dumps({"job_id": "123", "priority": 5}),  # Missing job_type
            json.dumps({"job_id": "123", "job_type": "test"}),  # Missing priority
            json.dumps({"job_id": "", "job_type": "test", "priority": 5}),  # Empty job_id
            json.dumps({"job_id": "123", "job_type": "", "priority": 5}),  # Empty job_type
        ]
        
        for job in invalid_jobs:
            await raw_client.zadd(delayed_queue, {job: current_time - 1})
        
        # Transfer should quarantine all
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 0, "Should not transfer jobs with missing fields"
        
        # Verify all quarantined
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 5, "All 5 invalid jobs should be quarantined"
        
        # Verify each invalid job is in quarantine (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        for job in invalid_jobs:
            assert job in quarantined_items, f"Invalid job {job[:30]}... should be in quarantine"
        
        # Verify invalid jobs are NOT in main or delayed queues (normalize bytes/str)
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        delayed_items_raw = await raw_client.zrange(delayed_queue, 0, -1)
        delayed_items = [normalize_redis_value(item) for item in delayed_items_raw]
        for job in invalid_jobs:
            assert job not in main_items, f"Invalid job should NOT be in main queue"
            assert job not in delayed_items, f"Invalid job should NOT be in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_invalid_priority_in_lua(self, redis_test_client):
        """Test Lua quarantines jobs with invalid priority values"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Jobs with invalid priorities (that made it past Python validation somehow)
        invalid_jobs = [
            json.dumps({"job_id": "1", "job_type": "test", "priority": 15}),  # > 10
            json.dumps({"job_id": "2", "job_type": "test", "priority": -1}),  # < 0
            json.dumps({"job_id": "3", "job_type": "test", "priority": 5.5}),  # Not integer
            json.dumps({"job_id": "4", "job_type": "test", "priority": "5"}),  # String, not number
        ]
        
        for job in invalid_jobs:
            await raw_client.zadd(delayed_queue, {job: current_time - 1})
        
        # Transfer should quarantine all
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 0, "Should not transfer jobs with invalid priorities"
        
        # Verify all quarantined
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 4, "All 4 jobs with invalid priorities should be quarantined"
        
        # Verify each invalid job is in quarantine (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        for job in invalid_jobs:
            assert job in quarantined_items, f"Invalid priority job {job[:30]}... should be in quarantine"
        
        # Verify invalid jobs are NOT in main or delayed queues (normalize bytes/str)
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        delayed_items_raw = await raw_client.zrange(delayed_queue, 0, -1)
        delayed_items = [normalize_redis_value(item) for item in delayed_items_raw]
        for job in invalid_jobs:
            assert job not in main_items, f"Invalid priority job should NOT be in main queue"
            assert job not in delayed_items, f"Invalid priority job should NOT be in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_ready_jobs_atomic_valid_jobs(self, redis_test_client):
        """Test successful transfer of valid jobs"""
        client = redis_test_client["client"]
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add 3 valid jobs (all ready to execute)
        jobs = [
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "test_job_1",
                "priority": 0,  # CRITICAL
                "execute_at": current_time - 10
            },
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "test_job_2",
                "priority": 5,  # MEDIUM
                "execute_at": current_time - 5
            },
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "test_job_3",
                "priority": 10,  # BACKGROUND
                "execute_at": current_time - 1
            },
        ]
        
        for job in jobs:
            result = await client.push_to_delayed_queue(
                job,
                job["execute_at"],
                delayed_queue
            )
            assert result is True, f"Should successfully add job {job['job_id']}"
        
        # Transfer jobs
        transferred = await client.transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 3, "Should transfer all 3 valid jobs"
        
        # Verify main queue has 3 jobs
        main_len = await raw_client.zcard(main_queue)
        assert main_len == 3, "Main queue should contain 3 jobs"
        
        # Verify delayed queue is empty
        delayed_len = await raw_client.zcard(delayed_queue)
        assert delayed_len == 0, "Delayed queue should be empty after transfer"
        
        # Verify quarantine is empty
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 0, "Quarantine should be empty (no invalid jobs)"
        
        # Verify execute_at field is removed in main queue (normalize bytes/str)
        transferred_jobs_raw = await raw_client.zrange(main_queue, 0, -1)
        for job_json_raw in transferred_jobs_raw:
            job_json = normalize_redis_value(job_json_raw)
            job_data = json.loads(job_json)
            assert "execute_at" not in job_data, "execute_at should be removed in main queue"
            assert "job_id" in job_data, "job_id should be preserved"
            assert "job_type" in job_data, "job_type should be preserved"
            assert "priority" in job_data, "priority should be preserved"

    @pytest.mark.asyncio
    async def test_transfer_respects_batch_limit(self, redis_test_client):
        """Test batch_limit is enforced"""
        client = redis_test_client["client"]
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add 10 valid jobs
        for i in range(10):
            job = {
                "job_id": str(uuid.uuid4()),
                "job_type": f"test_job_{i}",
                "priority": 5,
                "execute_at": current_time - 1
            }
            await client.push_to_delayed_queue(job, job["execute_at"], delayed_queue)
        
        # Transfer with batch_limit=5
        transferred = await client.transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=5,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 5, "Should transfer exactly 5 jobs (batch limit)"
        
        # Verify 5 remaining in delayed queue
        delayed_len = await raw_client.zcard(delayed_queue)
        assert delayed_len == 5, "5 jobs should remain in delayed queue"

    @pytest.mark.asyncio
    async def test_transfer_only_ready_jobs(self, redis_test_client):
        """Test only jobs with execute_at <= current_time are transferred"""
        client = redis_test_client["client"]
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add 2 ready jobs and 2 future jobs
        ready_jobs = [
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "ready_1",
                "priority": 5,
                "execute_at": current_time - 10  # Past
            },
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "ready_2",
                "priority": 5,
                "execute_at": current_time - 1  # Past
            },
        ]
        
        future_jobs = [
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "future_1",
                "priority": 5,
                "execute_at": current_time + 10  # Future
            },
            {
                "job_id": str(uuid.uuid4()),
                "job_type": "future_2",
                "priority": 5,
                "execute_at": current_time + 100  # Future
            },
        ]
        
        for job in ready_jobs + future_jobs:
            await client.push_to_delayed_queue(job, job["execute_at"], delayed_queue)
        
        # Transfer jobs
        transferred = await client.transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 2, "Should transfer only 2 ready jobs"
        
        # Verify 2 jobs remain in delayed queue
        delayed_len = await raw_client.zcard(delayed_queue)
        assert delayed_len == 2, "2 future jobs should remain in delayed queue"

    @pytest.mark.asyncio
    async def test_priority_ordering_in_main_queue(self, redis_test_client):
        """Test jobs are ordered by priority in main queue (0=highest, 10=lowest)"""
        client = redis_test_client["client"]
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add jobs with different priorities (all ready)
        jobs = [
            {"job_id": "background", "job_type": "test", "priority": 10, "execute_at": current_time - 1},
            {"job_id": "critical", "job_type": "test", "priority": 0, "execute_at": current_time - 1},
            {"job_id": "medium", "job_type": "test", "priority": 5, "execute_at": current_time - 1},
        ]
        
        for job in jobs:
            await client.push_to_delayed_queue(job, job["execute_at"], delayed_queue)
        
        # Transfer all jobs
        transferred = await client.transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 3, "Should transfer all 3 jobs"
        
        # Pop jobs in order and verify priority ordering (normalize bytes/str)
        # First pop should be priority 0 (CRITICAL)
        first_raw = await raw_client.zpopmin(main_queue, count=1)
        first_data = json.loads(normalize_redis_value(first_raw[0][0]))
        assert first_data["job_id"] == "critical", "First job should be CRITICAL (priority 0)"
        assert first_raw[0][1] == 0, "First job score should be 0"
        
        # Second pop should be priority 5 (MEDIUM)
        second_raw = await raw_client.zpopmin(main_queue, count=1)
        second_data = json.loads(normalize_redis_value(second_raw[0][0]))
        assert second_data["job_id"] == "medium", "Second job should be MEDIUM (priority 5)"
        assert second_raw[0][1] == 5, "Second job score should be 5"
        
        # Third pop should be priority 10 (BACKGROUND)
        third_raw = await raw_client.zpopmin(main_queue, count=1)
        third_data = json.loads(normalize_redis_value(third_raw[0][0]))
        assert third_data["job_id"] == "background", "Third job should be BACKGROUND (priority 10)"
        assert third_raw[0][1] == 10, "Third job score should be 10"

    @pytest.mark.asyncio
    async def test_mixed_valid_and_invalid_jobs(self, redis_test_client):
        """Test transfer handles mix of valid and invalid jobs correctly"""
        raw_client = redis_test_client["raw_client"]
        main_queue = redis_test_client["main_queue"]
        delayed_queue = redis_test_client["delayed_queue"]
        quarantine_queue = redis_test_client["quarantine_queue"]
        
        current_time = time.time()
        
        # Add 2 valid jobs
        valid_jobs = [
            json.dumps({
                "job_id": str(uuid.uuid4()),
                "job_type": "valid_1",
                "priority": 5
            }),
            json.dumps({
                "job_id": str(uuid.uuid4()),
                "job_type": "valid_2",
                "priority": 3
            }),
        ]
        
        # Add 3 invalid jobs
        invalid_jobs = [
            json.dumps([]),  # Empty array
            json.dumps({}),  # Empty object
            '{invalid}',  # Malformed JSON
        ]
        
        # Add all to delayed queue
        for job in valid_jobs + invalid_jobs:
            await raw_client.zadd(delayed_queue, {job: current_time - 1})
        
        # Transfer
        transferred = await redis_test_client["client"].transfer_ready_jobs_atomic(
            main_queue=main_queue,
            delayed_queue=delayed_queue,
            batch_limit=100,
            quarantine_queue=quarantine_queue
        )
        
        assert transferred == 2, "Should transfer 2 valid jobs"
        
        # Verify main queue has 2 jobs
        main_len = await raw_client.zcard(main_queue)
        assert main_len == 2, "Main queue should have 2 valid jobs"
        
        # Verify quarantine has 3 jobs
        quarantine_len = await raw_client.zcard(quarantine_queue)
        assert quarantine_len == 3, "Quarantine should have 3 invalid jobs"
        
        # Verify each invalid job is in quarantine and NOT in main/delayed (normalize bytes/str)
        quarantined_items_raw = await raw_client.zrange(quarantine_queue, 0, -1)
        quarantined_items = [normalize_redis_value(item) for item in quarantined_items_raw]
        main_items_raw = await raw_client.zrange(main_queue, 0, -1)
        main_items = [normalize_redis_value(item) for item in main_items_raw]
        for invalid_job in invalid_jobs:
            assert invalid_job in quarantined_items, f"Invalid job should be in quarantine"
            assert invalid_job not in main_items, f"Invalid job should NOT be in main queue"
        
        # Verify delayed queue is empty
        delayed_len = await raw_client.zcard(delayed_queue)
        assert delayed_len == 0, "Delayed queue should be empty"

    @pytest.mark.asyncio
    async def test_delayed_queue_helper_methods(self, redis_test_client):
        """Test delayed queue helper methods (length, ready_count, peek)"""
        client = redis_test_client["client"]
        delayed_queue = redis_test_client["delayed_queue"]
        
        current_time = time.time()
        
        # Add 2 ready jobs and 2 future jobs
        jobs = [
            {"job_id": "ready_1", "job_type": "test", "priority": 5, "execute_at": current_time - 10},
            {"job_id": "ready_2", "job_type": "test", "priority": 5, "execute_at": current_time - 1},
            {"job_id": "future_1", "job_type": "test", "priority": 5, "execute_at": current_time + 10},
            {"job_id": "future_2", "job_type": "test", "priority": 5, "execute_at": current_time + 100},
        ]
        
        for job in jobs:
            await client.push_to_delayed_queue(job, job["execute_at"], delayed_queue)
        
        # Test delayed_queue_length
        total_length = await client.delayed_queue_length(delayed_queue)
        assert total_length == 4, "Should have 4 total jobs in delayed queue"
        
        # Test get_delayed_queue_ready_count
        ready_count = await client.get_delayed_queue_ready_count(delayed_queue)
        assert ready_count == 2, "Should have 2 ready jobs"
        
        # Test peek_delayed_queue
        peeked_jobs = await client.peek_delayed_queue(delayed_queue, limit=4)
        assert len(peeked_jobs) == 4, "Should peek 4 jobs"
        
        # Verify peeked jobs have correct structure
        for job_data, execute_at in peeked_jobs:
            assert "job_id" in job_data, "Peeked job should have job_id"
            assert "execute_at" in job_data, "Peeked job should have execute_at"
            assert isinstance(execute_at, (int, float)), "execute_at score should be numeric"
        
        # Verify peek doesn't remove jobs
        length_after_peek = await client.delayed_queue_length(delayed_queue)
        assert length_after_peek == 4, "Peek should not remove jobs"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
