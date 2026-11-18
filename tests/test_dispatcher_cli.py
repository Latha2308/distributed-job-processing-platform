"""Tests for the dispatcher CLI and runner."""

import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.dispatcher.dispatcher_cli import DispatcherRunner


@pytest.fixture
def mock_queue_service():
    """Mock QueueService."""
    service = MagicMock()
    service.enqueue_job = AsyncMock()
    return service


@pytest.fixture
def mock_dispatcher():
    """Mock OutboxDispatcher."""
    dispatcher = AsyncMock()
    dispatcher.start = AsyncMock()
    dispatcher.stop = AsyncMock()
    return dispatcher


@pytest.mark.asyncio
async def test_dispatcher_runner_initialization():
    """Test DispatcherRunner initialization."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=20,
        poll_interval=2.0,
        max_attempts=3,
        shutdown_timeout=10.0,
    )

    assert runner.dispatcher_id == "test-dispatcher"
    assert runner.batch_size == 20
    assert runner.poll_interval == 2.0
    assert runner.max_attempts == 3
    assert runner.shutdown_timeout == 10.0
    assert runner.dispatcher is None
    assert runner.queue_service is None
    assert not runner.shutdown_event.is_set()
    assert runner.dispatcher_task is None
    assert runner.exit_code == 0


@pytest.mark.asyncio
async def test_dispatcher_runner_starts_and_runs(mock_queue_service, mock_dispatcher):
    """Test dispatcher runner starts successfully and runs."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Track stop() calls
    stop_called = False

    async def mock_stop():
        nonlocal stop_called
        stop_called = True

    mock_dispatcher.stop = mock_stop

    # Mock the dispatcher to run until stop() is called
    async def mock_start():
        while not stop_called:
            await asyncio.sleep(0.05)

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ) as mock_dispatcher_class,
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for startup
        await asyncio.sleep(0.1)

        # Signal shutdown
        runner.shutdown_event.set()

        # Wait for completion
        exit_code = await run_task

    # Verify successful run
    assert exit_code == 0
    assert runner.dispatcher is not None
    assert runner.queue_service is not None

    # Verify OutboxDispatcher was initialized with correct parameters
    mock_dispatcher_class.assert_called_once_with(
        queue_service=mock_queue_service,
        batch_size=10,
        poll_interval_seconds=1.0,
        max_attempts=5,
    )

    # Verify cleanup
    assert stop_called
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_handles_sigterm(mock_queue_service, mock_dispatcher):
    """Test dispatcher runner handles SIGTERM gracefully."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Track stop() calls
    stop_called = False

    async def mock_stop():
        nonlocal stop_called
        stop_called = True

    mock_dispatcher.stop = mock_stop

    # Mock dispatcher to run until stop() is called
    async def mock_start():
        while not stop_called:
            await asyncio.sleep(0.05)

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for dispatcher to start
        await asyncio.sleep(0.2)

        # Simulate SIGTERM by calling the signal handler directly
        runner._handle_signal(signal.SIGTERM)

        # Wait for runner to complete
        exit_code = await run_task

    # Verify graceful shutdown
    assert exit_code == 0
    assert stop_called
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_handles_sigint(mock_queue_service, mock_dispatcher):
    """Test dispatcher runner handles SIGINT gracefully."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Track stop() calls
    stop_called = False

    async def mock_stop():
        nonlocal stop_called
        stop_called = True

    mock_dispatcher.stop = mock_stop

    # Mock dispatcher to run until stop() is called
    async def mock_start():
        while not stop_called:
            await asyncio.sleep(0.05)

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for dispatcher to start
        await asyncio.sleep(0.2)

        # Simulate SIGINT by calling the signal handler directly
        runner._handle_signal(signal.SIGINT)

        # Wait for runner to complete
        exit_code = await run_task

    # Verify graceful shutdown
    assert exit_code == 0
    assert stop_called
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_handles_dispatcher_exception(mock_queue_service, mock_dispatcher):
    """Test dispatcher runner handles exceptions in dispatcher.start()."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Mock dispatcher to raise error
    mock_dispatcher.start.side_effect = RuntimeError("Dispatcher failed")

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()
        exit_code = await runner.run()

    # Verify failure detected and cleanup happened
    assert exit_code == 1
    mock_dispatcher.stop.assert_called_once()
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_detects_unexpected_exit(mock_queue_service, mock_dispatcher):
    """Test dispatcher runner detects when dispatcher.start() exits unexpectedly."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Mock dispatcher to exit without error and without shutdown signal
    async def mock_start():
        await asyncio.sleep(0.1)
        # Exit without setting shutdown_event - this is unexpected

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()
        exit_code = await runner.run()

    # Verify unexpected exit detected
    assert exit_code == 1
    mock_dispatcher.stop.assert_called_once()
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_cleanup_bounded_timeout_on_stop(
    mock_queue_service, mock_dispatcher
):
    """Test dispatcher runner applies bounded timeout when dispatcher.stop() hangs."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=0.5,
    )

    # Mock dispatcher.stop() to hang longer than timeout
    async def slow_stop():
        await asyncio.sleep(10)  # Longer than shutdown_timeout

    mock_dispatcher.stop = slow_stop

    # Mock dispatcher.start() to exit immediately on shutdown signal
    async def mock_start():
        await runner.shutdown_event.wait()

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for startup
        await asyncio.sleep(0.1)

        # Signal shutdown
        runner.shutdown_event.set()

        # Wait for cleanup timeout
        exit_code = await run_task

    # Verify timeout triggered and exit code set
    assert exit_code == 1
    # Redis close should still be attempted in force_cleanup
    mock_redis.close.assert_called()


@pytest.mark.asyncio
async def test_dispatcher_runner_does_not_cancel_task_during_graceful_shutdown(
    mock_queue_service, mock_dispatcher
):
    """Test dispatcher runner does not cancel task during successful graceful shutdown."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Track dispatcher lifecycle events
    stop_called = False
    task_cancelled = False

    # Mock dispatcher.stop() to record when it's called
    async def mock_stop():
        nonlocal stop_called
        stop_called = True

    mock_dispatcher.stop = mock_stop

    # Mock dispatcher to stay running until stop() is explicitly called
    async def mock_start():
        try:
            # Keep running until stop() is called
            while not stop_called:
                await asyncio.sleep(0.05)
            # Simulate completing after stop() is called
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            nonlocal task_cancelled
            task_cancelled = True
            raise

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for startup
        await asyncio.sleep(0.1)

        # Verify dispatcher is running and stop() hasn't been called yet
        assert not stop_called
        assert not task_cancelled

        # Signal shutdown
        runner.shutdown_event.set()

        # Wait for graceful completion
        exit_code = await run_task

    # Verify sequence: stop() was called, task completed without cancellation
    assert stop_called
    assert not task_cancelled
    assert exit_code == 0
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_cancels_task_only_on_timeout(mock_queue_service, mock_dispatcher):
    """Test dispatcher runner cancels task only when cleanup times out."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=0.3,
    )

    # Mock dispatcher to hang during stop
    async def slow_stop():
        await asyncio.sleep(10)

    mock_dispatcher.stop = slow_stop

    # Mock dispatcher to run indefinitely
    async def mock_start():
        try:
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            # Should be cancelled during force cleanup
            raise

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for startup
        await asyncio.sleep(0.1)

        # Signal shutdown
        runner.shutdown_event.set()

        # Wait for cleanup timeout and force cleanup
        exit_code = await run_task

    # Verify timeout occurred and task was cancelled
    assert exit_code == 1
    assert runner.dispatcher_task.done()
    mock_redis.close.assert_called()


@pytest.mark.asyncio
async def test_dispatcher_runner_handles_queue_service_initialization_failure():
    """Test dispatcher runner handles QueueService initialization failure."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Mock QueueService to raise error on initialization
    with (
        patch(
            "app.dispatcher.dispatcher_cli.QueueService",
            side_effect=RuntimeError("Redis connection failed"),
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()
        exit_code = await runner.run()

    # Verify failure detected
    assert exit_code == 1
    mock_redis.close.assert_called_once()


@pytest.mark.asyncio
async def test_dispatcher_runner_shutdown_event_coordination(mock_queue_service, mock_dispatcher):
    """Test shutdown_event properly coordinates graceful shutdown."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    start_called = False
    stop_called = False
    shutdown_observed = asyncio.Event()

    async def mock_stop():
        nonlocal stop_called
        stop_called = True

    mock_dispatcher.stop = mock_stop

    async def mock_start():
        nonlocal start_called
        start_called = True
        # Wait until shutdown event is set
        await runner.shutdown_event.wait()
        # Signal that we observed the shutdown event
        shutdown_observed.set()
        # Continue running until stop() is called
        while not stop_called:
            await asyncio.sleep(0.05)

    mock_dispatcher.start = mock_start

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for dispatcher to start
        await asyncio.sleep(0.2)
        assert start_called

        # Signal shutdown
        runner.shutdown_event.set()

        # Wait for mock_start to observe the shutdown event
        await asyncio.wait_for(shutdown_observed.wait(), timeout=1.0)

        # Wait for runner to complete
        exit_code = await run_task

    # Verify shutdown event was observed, stop was called, and exit was clean
    assert shutdown_observed.is_set()
    assert stop_called
    assert exit_code == 0


@pytest.mark.asyncio
async def test_dispatcher_runner_passes_all_parameters(mock_queue_service):
    """Test dispatcher runner passes all parameters to OutboxDispatcher."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=25,
        poll_interval=3.5,
        max_attempts=7,
        shutdown_timeout=5.0,
    )

    # Track stop calls
    stop_called = False

    async def mock_stop():
        nonlocal stop_called
        stop_called = True

    # Mock dispatcher to signal shutdown immediately
    async def mock_start():
        while not stop_called:
            await asyncio.sleep(0.05)

    mock_dispatcher = AsyncMock()
    mock_dispatcher.start = mock_start
    mock_dispatcher.stop = mock_stop

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ) as mock_dispatcher_class,
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start and stop
        run_task = asyncio.create_task(runner.run())
        await asyncio.sleep(0.1)
        runner.shutdown_event.set()
        exit_code = await run_task

    # Verify OutboxDispatcher was initialized with correct parameters
    assert exit_code == 0
    mock_dispatcher_class.assert_called_once_with(
        queue_service=mock_queue_service,
        batch_size=25,
        poll_interval_seconds=3.5,
        max_attempts=7,
    )


@pytest.mark.asyncio
async def test_dispatcher_runner_stop_error_sets_exit_code(mock_queue_service, mock_dispatcher):
    """Test that errors during dispatcher.stop() set non-zero exit code."""
    runner = DispatcherRunner(
        dispatcher_id="test-dispatcher",
        batch_size=10,
        poll_interval=1.0,
        max_attempts=5,
        shutdown_timeout=5.0,
    )

    # Mock dispatcher.start() to wait for shutdown
    async def mock_start():
        await runner.shutdown_event.wait()

    mock_dispatcher.start = mock_start

    # Mock dispatcher.stop() to raise error
    mock_dispatcher.stop.side_effect = RuntimeError("Stop failed")

    with (
        patch("app.dispatcher.dispatcher_cli.QueueService", return_value=mock_queue_service),
        patch(
            "app.dispatcher.dispatcher_cli.OutboxDispatcher",
            return_value=mock_dispatcher,
        ),
        patch("app.dispatcher.dispatcher_cli.redis_client") as mock_redis,
    ):
        mock_redis.close = AsyncMock()

        # Start and signal shutdown
        run_task = asyncio.create_task(runner.run())
        await asyncio.sleep(0.1)
        runner.shutdown_event.set()
        exit_code = await run_task

    # Verify error during stop sets exit code
    assert exit_code == 1
    mock_redis.close.assert_called_once()
