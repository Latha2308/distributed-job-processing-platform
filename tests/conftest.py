"""Pytest configuration and fixtures"""

import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.db.base import Base
from app.core.database import get_db
from app.main import app

# Test database URL - ONLY use jobprocessing_test database
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"

# Test Redis URL - ONLY use database 15 to isolate from development (database 0)
TEST_REDIS_URL = "redis://localhost:6379/15"

# Create test engine with bounded lock timeouts to prevent indefinite hangs
# These settings apply only to test connections, not production
test_engine = create_async_engine(
    TEST_DATABASE_URL,
    echo=False,
    connect_args={
        # Prevent indefinite lock waits - fail fast if blocked
        "server_settings": {
            "lock_timeout": "5000",  # 5 seconds max wait for locks
            "statement_timeout": "30000",  # 30 seconds max per statement
        }
    },
)

TestSessionLocal = async_sessionmaker(
    test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session"""
    # Create tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create session
    async with TestSessionLocal() as session:
        yield session

    # Drop tables
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    """Create a test Redis client isolated to database 15.

    CRITICAL: This fixture MUST use Redis DB 15, never DB 0 (development).
    Verify connection is to correct database before any operations.

    ISOLATION STRATEGY:
    - Uses unique job IDs (uuid4) to identify test-owned data
    - Removes only test-owned sorted-set members from shared queues
    - Preserves unrelated Redis data in DB 15
    - Does NOT use flushdb() to avoid destroying unrelated test data
    """
    client = await aioredis.from_url(TEST_REDIS_URL, decode_responses=True)

    # Verify we're connected to the correct database
    db_num = client.connection_pool.connection_kwargs.get("db", 0)
    if db_num != 15:
        await client.aclose()
        raise RuntimeError(
            f"Redis client connected to wrong database: expected 15, got {db_num}. "
            f"Test isolation compromised!"
        )

    # No pre-test cleanup needed - tests use unique job IDs and clean up after themselves

    yield client

    # No post-test cleanup - tests remove their own messages using unique job IDs
    # This preserves unrelated Redis data from concurrent tests
    await client.aclose()


@pytest_asyncio.fixture
async def redis_cleanup_tracker(redis_client):
    """
    Track test-owned job IDs and guarantee cleanup even on test failure.

    Usage in test:
        async def test_something(redis_cleanup_tracker):
            job_id = uuid4()
            redis_cleanup_tracker.track(job_id)  # Register for cleanup
            # ... test code that might fail ...
            # Cleanup happens automatically in finally block
    """
    tracked_job_ids = []

    class CleanupTracker:
        def track(self, job_id):
            """Register a job ID for guaranteed cleanup"""
            tracked_job_ids.append(str(job_id))

    tracker = CleanupTracker()

    try:
        yield tracker
    finally:
        # Guaranteed cleanup even if test fails
        if tracked_job_ids:
            import json

            # Clean up from main queue
            try:
                main_queue_messages = await redis_client.zrange("job_queue", 0, -1, withscores=True)
                for message_data, _ in main_queue_messages:
                    try:
                        message = json.loads(message_data)
                        if message.get("job_id") in tracked_job_ids:
                            await redis_client.zrem("job_queue", message_data)
                    except (json.JSONDecodeError, KeyError):
                        pass  # Skip malformed messages
            except Exception:
                pass  # Don't fail cleanup if queue doesn't exist

            # Clean up from delayed queue
            try:
                delayed_queue_messages = await redis_client.zrange(
                    "job_queue_delayed", 0, -1, withscores=True
                )
                for message_data, _ in delayed_queue_messages:
                    try:
                        message = json.loads(message_data)
                        if message.get("job_id") in tracked_job_ids:
                            await redis_client.zrem("job_queue_delayed", message_data)
                    except (json.JSONDecodeError, KeyError):
                        pass  # Skip malformed messages
            except Exception:
                pass  # Don't fail cleanup if queue doesn't exist


@pytest_asyncio.fixture
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Create a test client"""

    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def sample_job_data():
    """Sample job data for testing"""
    return {
        "job_type": "data_processing",
        "payload": {"data": [1, 2, 3, 4, 5]},
        "priority": 5,
    }
