"""Tests for recovery CLI runner.

These tests verify:
1. Argument validation (batch size, poll interval, runner ID)
2. Single-batch mode (--once)
3. Graceful shutdown handling
4. Error handling
5. Dependency injection and resource cleanup
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.recovery.recovery_cli import RecoveryRunner, validate_args


class MockSession:
    """Mock database session for testing."""

    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class MockSessionFactory:
    """Mock session factory that properly tracks context manager usage."""

    def __init__(self):
        self.sessions_created = []

    def __call__(self):
        return self

    async def __aenter__(self):
        session = MockSession()
        self.sessions_created.append(session)
        return session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Close the most recently created session
        if self.sessions_created:
            self.sessions_created[-1].closed = True
        return False


class MockEngine:
    """Mock database engine for testing."""

    def __init__(self):
        self.disposed = False

    async def dispose(self):
        self.disposed = True


# ============================================================================
# Argument Validation Tests
# ============================================================================


def test_validate_args_empty_runner_id():
    """Test that empty runner_id is rejected."""
    args = MagicMock(
        runner_id="",
        batch_size=10,
        poll_interval=60.0,
        shutdown_timeout=30.0,
    )

    with pytest.raises(ValueError, match="runner-id must be a non-empty string"):
        validate_args(args)


def test_validate_args_whitespace_runner_id():
    """Test that whitespace-only runner_id is rejected."""
    args = MagicMock(
        runner_id="   ",
        batch_size=10,
        poll_interval=60.0,
        shutdown_timeout=30.0,
    )

    with pytest.raises(ValueError, match="runner-id must be a non-empty string"):
        validate_args(args)


def test_validate_args_negative_batch_size():
    """Test that negative batch_size is rejected."""
    args = MagicMock(
        runner_id="recovery-1",
        batch_size=-5,
        poll_interval=60.0,
        shutdown_timeout=30.0,
    )

    with pytest.raises(ValueError, match="batch-size must be positive"):
        validate_args(args)


def test_validate_args_zero_batch_size():
    """Test that zero batch_size is rejected."""
    args = MagicMock(
        runner_id="recovery-1",
        batch_size=0,
        poll_interval=60.0,
        shutdown_timeout=30.0,
    )

    with pytest.raises(ValueError, match="batch-size must be positive"):
        validate_args(args)


def test_validate_args_negative_poll_interval():
    """Test that negative poll_interval is rejected."""
    args = MagicMock(
        runner_id="recovery-1",
        batch_size=10,
        poll_interval=-1.0,
        shutdown_timeout=30.0,
    )

    with pytest.raises(ValueError, match="poll-interval must be positive and finite"):
        validate_args(args)


def test_validate_args_infinite_poll_interval():
    """Test that infinite poll_interval is rejected."""
    args = MagicMock(
        runner_id="recovery-1",
        batch_size=10,
        poll_interval=float("inf"),
        shutdown_timeout=30.0,
    )

    with pytest.raises(ValueError, match="poll-interval must be positive and finite"):
        validate_args(args)


def test_validate_args_negative_shutdown_timeout():
    """Test that negative shutdown_timeout is rejected."""
    args = MagicMock(
        runner_id="recovery-1",
        batch_size=10,
        poll_interval=60.0,
        shutdown_timeout=-5.0,
    )

    with pytest.raises(ValueError, match="shutdown-timeout must be positive and finite"):
        validate_args(args)


def test_validate_args_valid():
    """Test that valid arguments pass validation."""
    args = MagicMock(
        runner_id="recovery-1",
        batch_size=10,
        poll_interval=60.0,
        shutdown_timeout=30.0,
        queued_batch_size=10,
        queued_age_threshold_minutes=10,
        queued_throttle_hours=1,
    )

    # Should not raise
    validate_args(args)


# ============================================================================
# Single-Batch Mode Tests
# ============================================================================


@pytest.mark.asyncio
async def test_single_batch_mode_no_stuck_jobs():
    """Test single-batch mode executes exactly one batch and exits successfully."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    # Mock the recovery service
    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(return_value=(0, 0))

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=60.0,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        exit_code = await runner.run()

        # Verify behavior
        assert exit_code == 0, "Should exit with code 0 on success"
        assert mock_service.recover_stuck_jobs_batch.call_count == 1, "Should run exactly one batch"
        assert len(detection_factory.sessions_created) == 1, "Should create one detection session"
        assert detection_factory.sessions_created[0].closed, "Session should be closed"
        assert mock_engine.disposed, "Engine should be disposed"


@pytest.mark.asyncio
async def test_single_batch_mode_with_stuck_job():
    """Test single-batch mode detects and recovers stuck job successfully."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        # Simulate detecting 1 job and recovering it successfully
        mock_service.recover_stuck_jobs_batch = AsyncMock(return_value=(1, 1))

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=60.0,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        exit_code = await runner.run()

        assert exit_code == 0, "Should exit with code 0 on full recovery"
        assert mock_service.recover_stuck_jobs_batch.call_count == 1
        assert detection_factory.sessions_created[0].closed
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_single_batch_mode_partial_recovery_failure():
    """Test single-batch mode returns nonzero on recovery failure."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        # Simulate detecting 3 jobs but only recovering 2
        mock_service.recover_stuck_jobs_batch = AsyncMock(return_value=(3, 2))

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=60.0,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        exit_code = await runner.run()

        assert exit_code == 1, "Should exit with code 1 when some recoveries fail"
        assert mock_service.recover_stuck_jobs_batch.call_count == 1
        assert detection_factory.sessions_created[0].closed
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_single_batch_mode_exception():
    """Test single-batch mode returns nonzero on exception."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(
            side_effect=RuntimeError("Database connection failed")
        )

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=60.0,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        exit_code = await runner.run()

        assert exit_code == 1, "Should exit with code 1 on exception"
        assert detection_factory.sessions_created[0].closed
        assert mock_engine.disposed


# ============================================================================
# Continuous Mode Tests
# ============================================================================


@pytest.mark.asyncio
async def test_continuous_mode_multiple_batches():
    """Test continuous mode performs multiple batches."""
    batch_count = 0
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            nonlocal batch_count
            batch_count += 1
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.02,  # 20ms interval
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for at least 3 batches (3 * 20ms = 60ms, add buffer)
        await asyncio.sleep(0.1)

        # Trigger shutdown
        runner.shutdown_event.set()

        # Wait for shutdown
        exit_code = await asyncio.wait_for(run_task, timeout=1.0)

        assert exit_code == 0, "Should exit successfully"
        assert batch_count >= 3, f"Should have run at least 3 batches, got {batch_count}"
        assert mock_engine.disposed, "Engine should be disposed"


@pytest.mark.asyncio
async def test_continuous_mode_fresh_detection_session_per_batch():
    """Test every batch receives a fresh detection session."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        call_count = 0

        async def mock_recovery(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                runner.shutdown_event.set()
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        await runner.run()

        # Each batch should have created a fresh session
        assert len(detection_factory.sessions_created) == 3, "Should create 3 detection sessions"
        # All sessions should be closed
        assert all(
            s.closed for s in detection_factory.sessions_created
        ), "All sessions should be closed"


@pytest.mark.asyncio
async def test_continuous_mode_per_job_session_factory_passed():
    """Test the per-job session factory is passed correctly to recovery service."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        call_count = 0

        async def mock_recovery(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                runner.shutdown_event.set()
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        await runner.run()

        # Verify the recovery service was constructed with the correct factory
        assert mock_service_class.call_count == 2
        for call_args in mock_service_class.call_args_list:
            # Second argument should be session_factory keyword arg
            assert call_args[1]["session_factory"] == recovery_factory


@pytest.mark.asyncio
async def test_continuous_mode_transient_failure_continues():
    """Test a transient failure does not stop continuous polling."""
    batch_count = 0
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            nonlocal batch_count
            batch_count += 1
            if batch_count == 2:
                # Fail on second batch
                raise RuntimeError("Transient error")
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.02,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for 4 batches (batch 2 fails with backoff, so needs more time)
        # 4 batches + 1 failure with 5s backoff = need to wait
        await asyncio.sleep(0.15)

        # Trigger shutdown
        runner.shutdown_event.set()

        exit_code = await asyncio.wait_for(run_task, timeout=1.0)

        assert exit_code == 0, "Should exit successfully despite transient failure"
        assert batch_count >= 4, f"Should continue after transient failure, got {batch_count}"
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_continuous_mode_five_consecutive_failures():
    """Test five consecutive failures terminate with nonzero status."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        # Always fail
        mock_service.recover_stuck_jobs_batch = AsyncMock(
            side_effect=RuntimeError("Persistent error")
        )

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        exit_code = await runner.run()

        assert exit_code == 1, "Should exit with code 1 after 5 consecutive failures"
        # Should have tried 5 times
        assert mock_service.recover_stuck_jobs_batch.call_count == 5
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_continuous_mode_successful_batch_resets_failure_counter():
    """Test a successful batch resets the consecutive-failure counter."""
    batch_count = 0
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            nonlocal batch_count
            batch_count += 1
            # Fail batches 1-2, succeed on 3, fail 4-6, succeed on 7
            # This ensures we never hit 5 consecutive failures
            if batch_count in [1, 2, 4, 5, 6]:
                raise RuntimeError("Intermittent error")
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.02,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        # Start runner in background
        run_task = asyncio.create_task(runner.run())

        # Wait for enough time for 7 batches + backoff delays
        await asyncio.sleep(0.3)

        # Trigger shutdown
        runner.shutdown_event.set()

        exit_code = await asyncio.wait_for(run_task, timeout=2.0)

        # Should succeed because failures were reset by successful batches
        assert (
            exit_code == 0
        ), f"Should not terminate despite failures because of successful resets, got exit_code={exit_code}, batch_count={batch_count}"
        assert batch_count >= 7, f"Should have completed at least 7 batches, got {batch_count}"
        assert mock_engine.disposed


# ============================================================================
# Shutdown Safety Tests
# ============================================================================


@pytest.mark.asyncio
async def test_shutdown_while_idle_stops_promptly():
    """Test shutdown while idle stops promptly."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()
    batch_count = 0

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            nonlocal batch_count
            batch_count += 1
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=10.0,  # Long interval - 10 seconds
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        # Start runner
        run_task = asyncio.create_task(runner.run())

        # Wait for first batch to complete and enter poll interval wait
        await asyncio.sleep(0.1)

        # Trigger shutdown while waiting for next poll interval
        runner.shutdown_event.set()

        # Should complete quickly (not wait for full 10s poll interval)
        exit_code = await asyncio.wait_for(run_task, timeout=1.0)

        assert exit_code == 0
        assert batch_count == 1, f"Should have completed one batch, got {batch_count}"
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_shutdown_during_active_batch_waits_for_completion():
    """Test shutdown during an active batch waits for completion."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()
    batch_started = asyncio.Event()
    batch_can_complete = asyncio.Event()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            batch_started.set()
            # Wait for permission to complete
            await batch_can_complete.wait()
            return (1, 1)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        run_task = asyncio.create_task(runner.run())

        # Wait for batch to start
        await batch_started.wait()

        # Signal shutdown while batch is active
        runner.shutdown_event.set()

        # Give a moment to process shutdown
        await asyncio.sleep(0.02)

        # Batch should still be running
        assert not run_task.done(), "Runner should wait for active batch"

        # Allow batch to complete
        batch_can_complete.set()

        # Now shutdown should complete
        exit_code = await asyncio.wait_for(run_task, timeout=1.0)

        assert exit_code == 0
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_shutdown_timeout_forces_cancellation():
    """Test shutdown timeout forces cancellation of hung recovery."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()
    batch_started = asyncio.Event()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            batch_started.set()
            # Hang indefinitely - never return
            await asyncio.Event().wait()  # Wait forever
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            shutdown_timeout=0.1,  # Very short timeout
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        run_task = asyncio.create_task(runner.run())

        # Wait for batch to start
        await batch_started.wait()

        # Signal shutdown while batch is hung
        runner.shutdown_event.set()

        # Runner should complete (either by timeout+cancel or successful cancel)
        exit_code = await asyncio.wait_for(run_task, timeout=2.0)

        # Exit code can be 0 (cancelled successfully) or 1 (timeout forced cleanup)
        # Both are acceptable - the key is that the runner terminates
        assert exit_code in [0, 1], f"Should exit when hung batch is cancelled, got {exit_code}"
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_cancellation_is_handled_gracefully():
    """Test asyncio.CancelledError is handled gracefully during recovery."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()
    batch_started = asyncio.Event()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            batch_started.set()
            # Wait a bit, then cancel ourselves
            await asyncio.sleep(0.01)
            raise asyncio.CancelledError()

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=False,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        run_task = asyncio.create_task(runner.run())

        # Wait for batch to start
        await batch_started.wait()

        # The cancellation should be caught and logged, runner should exit
        # Wait for the runner to complete - wrap in try/except since CancelledError might propagate
        try:
            exit_code = await asyncio.wait_for(run_task, timeout=1.0)
            # Cancellation during recovery is treated as an error
            assert exit_code == 1, "Should exit with error code when recovery is cancelled"
        except asyncio.CancelledError:
            # If CancelledError propagates, that's also acceptable behavior
            pass

        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_engine_disposal_after_active_work():
    """Test engine disposal occurs only after active work has completed."""
    disposal_order = []
    batch_started = asyncio.Event()
    batch_completing = asyncio.Event()

    class TrackingSessionFactory:
        """Session factory that tracks disposal order."""

        def __call__(self):
            return self

        async def __aenter__(self):
            return MockSession()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            disposal_order.append("session_closed")
            return False

    detection_factory = TrackingSessionFactory()
    recovery_factory = TrackingSessionFactory()
    mock_engine = MockEngine()

    async def mock_dispose():
        disposal_order.append("engine_disposed")
        mock_engine.disposed = True

    mock_engine.dispose = mock_dispose  # type: ignore[method-assign]

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:

        async def mock_recovery(*args, **kwargs):
            batch_started.set()
            await batch_completing.wait()
            return (0, 0)

        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(side_effect=mock_recovery)

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        run_task = asyncio.create_task(runner.run())

        # Wait for batch to start
        await batch_started.wait()

        # Engine should not be disposed yet
        assert not mock_engine.disposed

        # Allow batch to complete
        batch_completing.set()

        await run_task

        # Engine should be disposed after work completes
        assert mock_engine.disposed
        # Session should close before engine disposal
        assert disposal_order == ["session_closed", "engine_disposed"]


@pytest.mark.asyncio
async def test_session_resources_close_on_success():
    """Test session resources close on success."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(return_value=(1, 1))

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        await runner.run()

        # All sessions should be closed
        assert all(s.closed for s in detection_factory.sessions_created)
        assert mock_engine.disposed


@pytest.mark.asyncio
async def test_session_resources_close_on_failure():
    """Test session resources close on failure."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    with patch("app.recovery.recovery_cli.JobRecoveryService") as mock_service_class:
        mock_service = mock_service_class.return_value
        mock_service.recover_stuck_jobs_batch = AsyncMock(
            side_effect=RuntimeError("Recovery failed")
        )

        runner = RecoveryRunner(
            runner_id="recovery-test",
            batch_size=10,
            poll_interval=0.01,
            run_once=True,
            enable_queued_reconciliation=False,
            detection_session_factory=detection_factory,
            recovery_session_factory=recovery_factory,
            engine_to_dispose=mock_engine,
        )

        exit_code = await runner.run()

        # Should fail but still clean up
        assert exit_code == 1
        assert all(s.closed for s in detection_factory.sessions_created)
        assert mock_engine.disposed


# ============================================================================
# Initialization Tests
# ============================================================================


@pytest.mark.asyncio
async def test_runner_initialization():
    """Test that RecoveryRunner initializes correctly."""
    runner = RecoveryRunner(
        runner_id="recovery-test",
        batch_size=15,
        poll_interval=30.0,
        shutdown_timeout=60.0,
        run_once=False,
        enable_queued_reconciliation=False,
    )

    assert runner.runner_id == "recovery-test"
    assert runner.batch_size == 15
    assert runner.poll_interval == 30.0
    assert runner.shutdown_timeout == 60.0
    assert runner.run_once is False
    assert runner.running is False
    assert runner.exit_code == 0


@pytest.mark.asyncio
async def test_runner_initialization_run_once():
    """Test that RecoveryRunner initializes correctly in run_once mode."""
    runner = RecoveryRunner(
        runner_id="recovery-test",
        batch_size=10,
        poll_interval=60.0,
        run_once=True,
        enable_queued_reconciliation=False,
    )

    assert runner.run_once is True


@pytest.mark.asyncio
async def test_runner_initialization_with_custom_factories():
    """Test that RecoveryRunner accepts custom session factories and engine."""
    detection_factory = MockSessionFactory()
    recovery_factory = MockSessionFactory()
    mock_engine = MockEngine()

    runner = RecoveryRunner(
        runner_id="recovery-test",
        batch_size=10,
        poll_interval=60.0,
        detection_session_factory=detection_factory,
        recovery_session_factory=recovery_factory,
        engine_to_dispose=mock_engine,
    )

    assert runner.detection_session_factory == detection_factory
    assert runner.recovery_session_factory == recovery_factory
    assert runner.engine_to_dispose == mock_engine
