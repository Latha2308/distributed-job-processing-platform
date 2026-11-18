"""Tests for Redis client connection cleanup."""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from app.core.redis_client import RedisClient


@pytest_asyncio.fixture
async def redis_client():
    """Create a fresh Redis client for testing."""
    client = RedisClient()
    yield client
    # Cleanup after test
    try:
        await client.close()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_redis_client_close_is_idempotent():
    """Test that close() can be called multiple times safely."""
    client = RedisClient()

    # Mock the pool and client
    with (
        patch("app.core.redis_client.redis_pool") as mock_pool,
        patch("app.core.redis_client.aioredis.Redis") as mock_redis_class,
    ):
        mock_redis_instance = AsyncMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_pool.disconnect = AsyncMock()

        # Initialize client
        await client.get_client()

        # First close
        await client.close()
        mock_redis_instance.aclose.assert_called_once()
        mock_pool.disconnect.assert_called_once()

        # Second close should not raise error
        await client.close()
        # aclose should not be called again (client is None)
        mock_redis_instance.aclose.assert_called_once()
        # Pool disconnect will be called again
        assert mock_pool.disconnect.call_count == 2


@pytest.mark.asyncio
async def test_redis_client_close_resets_client_to_none():
    """Test that close() resets _client to None."""
    client = RedisClient()

    with (
        patch("app.core.redis_client.redis_pool") as mock_pool,
        patch("app.core.redis_client.aioredis.Redis") as mock_redis_class,
    ):
        mock_redis_instance = AsyncMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_pool.disconnect = AsyncMock()

        # Initialize client
        await client.get_client()
        assert client._client is not None

        # Close
        await client.close()
        assert client._client is None


@pytest.mark.asyncio
async def test_redis_client_close_handles_client_aclose_error():
    """Test that close() handles errors from client.aclose() gracefully."""
    client = RedisClient()

    with (
        patch("app.core.redis_client.redis_pool") as mock_pool,
        patch("app.core.redis_client.aioredis.Redis") as mock_redis_class,
    ):
        mock_redis_instance = AsyncMock()
        mock_redis_instance.aclose.side_effect = RuntimeError("Connection error")
        mock_redis_class.return_value = mock_redis_instance
        mock_pool.disconnect = AsyncMock()

        # Initialize client
        await client.get_client()

        # Close should handle error and still reset client
        await client.close()
        assert client._client is None
        # Pool disconnect should still be called
        mock_pool.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_redis_client_close_handles_pool_disconnect_error():
    """Test that close() handles errors from pool.disconnect() gracefully."""
    client = RedisClient()

    with (
        patch("app.core.redis_client.redis_pool") as mock_pool,
        patch("app.core.redis_client.aioredis.Redis") as mock_redis_class,
    ):
        mock_redis_instance = AsyncMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_pool.disconnect = AsyncMock(side_effect=RuntimeError("Pool error"))

        # Initialize client
        await client.get_client()

        # Close should handle error without raising
        await client.close()
        assert client._client is None
        mock_redis_instance.aclose.assert_called_once()


@pytest.mark.asyncio
async def test_redis_client_close_with_no_client_initialized():
    """Test that close() works safely when no client was initialized."""
    client = RedisClient()

    with patch("app.core.redis_client.redis_pool") as mock_pool:
        mock_pool.disconnect = AsyncMock()

        # Close without initializing client
        await client.close()

        # Pool disconnect should still be called
        mock_pool.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_redis_client_can_reconnect_after_close():
    """Test that a client can get a new connection after close()."""
    client = RedisClient()

    with (
        patch("app.core.redis_client.redis_pool") as mock_pool,
        patch("app.core.redis_client.aioredis.Redis") as mock_redis_class,
    ):
        mock_redis_instance1 = AsyncMock()
        mock_redis_instance2 = AsyncMock()
        mock_redis_class.side_effect = [mock_redis_instance1, mock_redis_instance2]
        mock_pool.disconnect = AsyncMock()

        # First connection
        client1 = await client.get_client()
        assert client1 == mock_redis_instance1

        # Close
        await client.close()
        assert client._client is None

        # Reconnect - should create new client instance
        client2 = await client.get_client()
        assert client2 == mock_redis_instance2
        assert client._client is not None


@pytest.mark.asyncio
async def test_redis_client_close_calls_pool_disconnect():
    """Test that close() explicitly calls pool.disconnect()."""
    client = RedisClient()

    with (
        patch("app.core.redis_client.redis_pool") as mock_pool,
        patch("app.core.redis_client.aioredis.Redis") as mock_redis_class,
    ):
        mock_redis_instance = AsyncMock()
        mock_redis_class.return_value = mock_redis_instance
        mock_pool.disconnect = AsyncMock()

        # Initialize and close
        await client.get_client()
        await client.close()

        # Verify both client and pool were cleaned up
        mock_redis_instance.aclose.assert_called_once()
        mock_pool.disconnect.assert_called_once()
