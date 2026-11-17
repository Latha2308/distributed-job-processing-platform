"""Redis client configuration"""

import json
import time
from typing import Any, Optional

import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool

from app.core.config import settings
from app.core.logging_config import setup_logging

logger = setup_logging()

# Create connection pool
redis_pool = ConnectionPool.from_url(
    str(settings.REDIS_URL),
    max_connections=settings.REDIS_POOL_SIZE,
    decode_responses=True,
)

class RedisClient:
    """Redis client wrapper"""

    def __init__(self):
        self._client: Optional[aioredis.Redis] = None

    async def get_client(self) -> aioredis.Redis:
        """Get or create Redis client"""
        if self._client is None:
            self._client = aioredis.Redis(connection_pool=redis_pool)
        return self._client

    async def close(self):
        """Close Redis connection"""
        if self._client:
            await self._client.aclose()  # Use aclose() instead of deprecated close()

    async def set_json(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """Store JSON data in Redis"""
        client = await self.get_client()
        data = json.dumps(value)
        if expire:
            return await client.setex(key, expire, data)
        return await client.set(key, data)

    async def get_json(self, key: str) -> Optional[Any]:
        """Retrieve JSON data from Redis"""
        client = await self.get_client()
        data = await client.get(key)
        if data:
            return json.loads(data)
        return None

    async def delete(self, key: str) -> bool:
        """Delete key from Redis"""
        client = await self.get_client()
        return await client.delete(key) > 0

    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        client = await self.get_client()
        return await client.exists(key) > 0

    async def push_to_queue(self, queue_name: str, item: Any, priority: int = 0) -> bool:
        """
        Push item to priority queue.

        Priority convention: Lower number = higher priority (0=highest, 10=lowest)
        FIXED: Changed from score=-priority to score=priority so zpopmin gets priority 0 before 10.
        """
        client = await self.get_client()
        data = json.dumps(item)
        # FIXED: Use priority directly as score (was -priority which caused reverse ordering)
        # ZADD returns number of elements added (0 if element existed and was updated)
        # Both 0 and 1 indicate success
        return await client.zadd(queue_name, {data: priority}) >= 0

    async def pop_from_queue(self, queue_name: str) -> Optional[Any]:
        """Pop item from priority queue"""
        client = await self.get_client()
        # Get item with highest priority (lowest score)
        items = await client.zpopmin(queue_name, count=1)
        if items:
            data, _ = items[0]
            return json.loads(data)
        return None

    async def queue_length(self, queue_name: str) -> int:
        """Get queue length"""
        client = await self.get_client()
        return await client.zcard(queue_name)

    # ========== DELAYED QUEUE METHODS ==========

    async def push_to_delayed_queue(
        self,
        job_data: dict,
        execute_at: float,
        queue_name: str = "job_queue_delayed"
    ) -> bool:
        """
        Push a job to the delayed queue with execute_at timestamp as score.

        Args:
            job_data: Job payload (must include job_id, job_type, priority as integer 0-10)
            execute_at: Unix timestamp when job should be executed (rejects NaN/infinity)
            queue_name: Name of the delayed queue

        Returns:
            True if added successfully, False on validation failure
        """
        # Validate required fields
        required_fields = ["job_id", "job_type", "priority"]
        missing_fields = [f for f in required_fields if f not in job_data]
        if missing_fields:
            logger.error(
                "push_to_delayed_queue_validation_failed",
                missing_fields=missing_fields,
                job_data_keys=list(job_data.keys())
            )
            return False

        # Validate types and ranges
        try:
            priority = job_data["priority"]

            # Reject boolean values for priority (bool is subclass of int in Python)
            if isinstance(priority, bool):
                logger.error(
                    "priority_is_boolean",
                    priority=priority,
                    required="integer 0-10"
                )
                return False

            # Validate priority is integer in range 0-10
            if not isinstance(priority, int) or priority < 0 or priority > 10:
                logger.error(
                    "invalid_priority",
                    priority=priority,
                    priority_type=type(priority).__name__,
                    valid_range="0-10 (integer)"
                )
                return False

            # Validate execute_at and reject NaN/infinity
            execute_at_time = float(execute_at)

            # Reject NaN
            if execute_at_time != execute_at_time:  # NaN check (NaN != NaN)
                logger.error("execute_at_is_nan", execute_at=execute_at)
                return False

            # Reject infinity
            if execute_at_time == float('inf') or execute_at_time == float('-inf'):
                logger.error("execute_at_is_infinity", execute_at=execute_at)
                return False

            # Warn if in the past (but allow it)
            if execute_at_time < time.time():
                logger.warning(
                    "execute_at_in_past",
                    execute_at=execute_at_time,
                    current_time=time.time()
                )

        except (ValueError, TypeError) as e:
            logger.error(
                "invalid_numeric_field",
                error=str(e),
                priority=job_data.get("priority"),
                execute_at=execute_at
            )
            return False

        try:
            client = await self.get_client()
            job_json = json.dumps(job_data)
            result = await client.zadd(queue_name, {job_json: execute_at_time})

            logger.debug(
                "job_added_to_delayed_queue",
                job_id=job_data["job_id"],
                execute_at=execute_at_time,
                delay_seconds=round(execute_at_time - time.time(), 2),
                queue_name=queue_name
            )

            return result >= 0  # 0 or 1 both indicate success

        except Exception as e:
            logger.error(
                "failed_to_push_delayed_queue",
                error=str(e),
                error_type=type(e).__name__,
                job_id=job_data.get("job_id"),
                queue_name=queue_name
            )
            return False

    async def transfer_ready_jobs_atomic(
        self,
        main_queue: str = "job_queue",
        delayed_queue: str = "job_queue_delayed",
        batch_limit: int = 100,
        quarantine_queue: str = "job_queue_invalid"
    ) -> int:
        """
        Atomically transfer ready jobs from delayed queue to main queue using Lua.

        IMPORTANT: Redis Lua scripts execute atomically (no interleaving) but do NOT
        automatically roll back writes if an error occurs. Partial transfers may occur
        if script fails mid-execution.

        DUPLICATE PREVENTION:
        - ZADD prevents duplicate serialized JSON strings in queue
        - Does NOT guarantee uniqueness by job_id
        - Database-level atomic job claiming (UPDATE WHERE status=eligible) is required
          to prevent concurrent workers from processing the same job_id

        SAFETY GUARANTEES:
        - Validates ALL key types before any mutations
        - Validates each job is a JSON object with required fields
        - Quarantines empty objects {}, empty arrays [], and malformed JSON
        - Enforces priority as integer 0-10 in Lua
        - Quarantines invalid jobs to configurable quarantine queue
        - Bounded batch processing (enforced max 100 jobs per call)
        - Safe operation ordering within script

        Args:
            main_queue: Main priority queue name
            delayed_queue: Delayed queue name
            batch_limit: Requested batch size (capped at 100, rejects invalid values)
            quarantine_queue: Queue for invalid jobs (default: job_queue_invalid)

        Returns:
            Number of jobs successfully transferred

        Raises:
            RuntimeError: If Lua script fails. Partial transfers may have occurred.
        """
        current_time = time.time()

        # Validate and enforce batch_limit
        if isinstance(batch_limit, bool) or not isinstance(batch_limit, int) or batch_limit < 1:
            logger.error(
                "invalid_batch_limit",
                batch_limit=batch_limit,
                batch_limit_type=type(batch_limit).__name__,
                required="integer >= 1"
            )
            raise ValueError(f"batch_limit must be integer >= 1, got {batch_limit}")

        # Enforce maximum batch size of 100
        enforced_limit = min(batch_limit, 100)

        # Lua script with validation BEFORE mutations
        lua_script = """
        local delayed_queue = KEYS[1]
        local main_queue = KEYS[2]
        local quarantine_queue = KEYS[3]
        local current_time = tonumber(ARGV[1])
        local batch_limit = tonumber(ARGV[2])

        -- VALIDATE ALL key types BEFORE any mutations
        local delayed_type = redis.call('TYPE', delayed_queue)['ok']
        if delayed_type ~= 'zset' and delayed_type ~= 'none' then
            error('Invalid delayed_queue type: ' .. delayed_type .. ' (expected zset or none)')
        end

        local main_type = redis.call('TYPE', main_queue)['ok']
        if main_type ~= 'zset' and main_type ~= 'none' then
            error('Invalid main_queue type: ' .. main_type .. ' (expected zset or none)')
        end

        local quarantine_type = redis.call('TYPE', quarantine_queue)['ok']
        if quarantine_type ~= 'zset' and quarantine_type ~= 'none' then
            error('Invalid quarantine_queue type: ' .. quarantine_type .. ' (expected zset or none)')
        end

        -- Get jobs ready to execute (execute_at <= current_time), limited by batch
        local ready_jobs = redis.call('ZRANGEBYSCORE', delayed_queue, 0, current_time, 'LIMIT', 0, batch_limit)
        local transferred = 0
        local quarantined = 0

        for _, job_data in ipairs(ready_jobs) do
            -- Validate JSON structure
            local decode_success, job = pcall(cjson.decode, job_data)

            if not decode_success then
                -- Invalid JSON - quarantine for investigation
                redis.call('ZADD', quarantine_queue, current_time, job_data)
                redis.call('ZREM', delayed_queue, job_data)
                quarantined = quarantined + 1
            -- Validate decoded JSON is a non-empty object with required fields
            elseif type(job) ~= 'table' then
                -- Not a table (primitives, null) - quarantine
                redis.call('ZADD', quarantine_queue, current_time, job_data)
                redis.call('ZREM', delayed_queue, job_data)
                quarantined = quarantined + 1
            elseif type(job.job_id) ~= 'string' or job.job_id == '' then
                -- Missing, non-string, or empty job_id - quarantine
                -- This also rejects empty objects {} (no job_id field)
                -- This also rejects arrays (job_id will be nil or non-string)
                redis.call('ZADD', quarantine_queue, current_time, job_data)
                redis.call('ZREM', delayed_queue, job_data)
                quarantined = quarantined + 1
            elseif type(job.job_type) ~= 'string' or job.job_type == '' then
                -- Missing, non-string, or empty job_type - quarantine
                redis.call('ZADD', quarantine_queue, current_time, job_data)
                redis.call('ZREM', delayed_queue, job_data)
                quarantined = quarantined + 1
            elseif type(job.priority) ~= 'number' then
                -- Missing or non-numeric priority - quarantine
                redis.call('ZADD', quarantine_queue, current_time, job_data)
                redis.call('ZREM', delayed_queue, job_data)
                quarantined = quarantined + 1
            else
                -- Validate priority is integer in range 0-10
                local priority = job.priority
                if priority ~= math.floor(priority) or priority < 0 or priority > 10 then
                    -- Invalid priority - quarantine
                    redis.call('ZADD', quarantine_queue, current_time, job_data)
                    redis.call('ZREM', delayed_queue, job_data)
                    quarantined = quarantined + 1
                else
                    -- Valid job - transfer to main queue

                    -- Remove metadata fields not needed in main queue
                    job.execute_at = nil

                    -- Re-encode cleaned job
                    local encode_success, clean_job = pcall(cjson.encode, job)

                    if not encode_success then
                        -- Encoding failed - quarantine
                        redis.call('ZADD', quarantine_queue, current_time, job_data)
                        redis.call('ZREM', delayed_queue, job_data)
                        quarantined = quarantined + 1
                    else
                        -- SAFE ORDERING:
                        -- 1. Add to main queue (ZADD is idempotent)
                        -- 2. Remove from delayed queue
                        -- Note: If error occurs between these steps, partial transfer may occur
                        redis.call('ZADD', main_queue, priority, clean_job)
                        redis.call('ZREM', delayed_queue, job_data)
                        transferred = transferred + 1
                    end
                end
            end
        end

        return {transferred, quarantined}
        """

        try:
            client = await self.get_client()

            # Execute Lua script atomically on Redis server
            result = await client.eval(
                lua_script,
                3,  # number of keys
                delayed_queue,
                main_queue,
                quarantine_queue,
                current_time,
                enforced_limit
            )

            transferred, quarantined = result

            if transferred > 0:
                logger.info(
                    "delayed_jobs_transferred",
                    count=transferred,
                    from_queue=delayed_queue,
                    to_queue=main_queue
                )

            if quarantined > 0:
                logger.warning(
                    "invalid_jobs_quarantined",
                    count=quarantined,
                    quarantine_queue=quarantine_queue,
                    action_required="Investigate quarantined jobs"
                )

            return int(transferred)

        except Exception as e:
            logger.error(
                "delayed_transfer_failed",
                error=str(e),
                error_type=type(e).__name__,
                delayed_queue=delayed_queue,
                main_queue=main_queue,
                impact="Partial transfers may have occurred before error"
            )
            # Raise error - do NOT fall back to unsafe Python implementation
            raise RuntimeError(f"Atomic delayed job transfer failed: {e}") from e

    async def delayed_queue_length(self, queue_name: str = "job_queue_delayed") -> int:
        """Get total count of jobs in delayed queue"""
        client = await self.get_client()
        return await client.zcard(queue_name)

    async def get_delayed_queue_ready_count(self, queue_name: str = "job_queue_delayed") -> int:
        """
        Get count of jobs ready to execute now (execute_at <= current_time).

        Useful for monitoring delayed job processing lag.
        """
        client = await self.get_client()
        current_time = time.time()
        return await client.zcount(queue_name, 0, current_time)

    async def peek_delayed_queue(
        self,
        queue_name: str = "job_queue_delayed",
        limit: int = 10
    ) -> list:
        """
        Peek at jobs in the delayed queue without removing them.

        Returns:
            List of (job_data, execute_at) tuples
        """
        try:
            client = await self.get_client()
            jobs_with_scores = await client.zrange(queue_name, 0, limit - 1, withscores=True)

            result = []
            for job_json, execute_at in jobs_with_scores:
                try:
                    job_data = json.loads(job_json)
                    result.append((job_data, execute_at))
                except json.JSONDecodeError:
                    logger.debug(
                        "invalid_json_during_peek",
                        job_json_preview=job_json[:100]
                    )
                    continue

            return result

        except Exception as e:
            logger.error("peek_delayed_queue_failed", error=str(e), queue_name=queue_name)
            return []

    async def verify_redis_connectivity(self) -> dict:
        """
        Verify Redis connectivity and check queue state.

        Should be called on worker startup to validate:
        - Redis is accessible
        - Queue types are correct (zset)
        - Quarantine queue status

        Returns:
            Dict with connectivity status and queue stats
        """
        try:
            client = await self.get_client()

            # Test connectivity
            await client.ping()

            # Get queue stats
            main_queue_len = await client.zcard("job_queue")
            delayed_queue_len = await client.zcard("job_queue_delayed")
            quarantine_len = await client.zcard("job_queue_invalid")

            # Check key types
            main_queue_type = await client.type("job_queue")
            delayed_queue_type = await client.type("job_queue_delayed")

            stats = {
                "connected": True,
                "main_queue_length": main_queue_len,
                "delayed_queue_length": delayed_queue_len,
                "quarantine_queue_length": quarantine_len,
                "main_queue_type": main_queue_type,
                "delayed_queue_type": delayed_queue_type,
            }

            logger.info(
                "redis_connectivity_verified",
                **stats,
                redis_url=str(settings.REDIS_URL).split("@")[-1]  # Hide credentials
            )

            # Warn about unexpected states
            if main_queue_type not in ["zset", "none"]:
                logger.error(
                    "unexpected_main_queue_type",
                    expected="zset or none",
                    actual=main_queue_type,
                    action_required="Clear or migrate job_queue"
                )

            if quarantine_len > 0:
                logger.warning(
                    "quarantine_queue_not_empty",
                    count=quarantine_len,
                    action_required="Investigate job_queue_invalid"
                )

            return stats

        except Exception as e:
            logger.error(
                "redis_connectivity_failed",
                error=str(e),
                error_type=type(e).__name__,
                redis_url=str(settings.REDIS_URL).split("@")[-1]
            )
            return {"connected": False, "error": str(e)}

# Global Redis client instance
redis_client = RedisClient()

async def get_redis() -> RedisClient:
    """Dependency for getting Redis client"""
    return redis_client
