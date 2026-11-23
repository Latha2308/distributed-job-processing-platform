"""Standalone stuck-job recovery CLI process.

This module provides a standalone process that continuously polls for stuck jobs
and recovers them using atomic database transactions. It follows the worker pattern
with proper async signal handling and graceful shutdown.

Usage:
    # Continuous mode
    python -m app.recovery.recovery_cli --runner-id recovery-1 --batch-size 10 --poll-interval 60.0

    # Single batch mode (for testing or cron jobs)
    python -m app.recovery.recovery_cli --runner-id recovery-1 --once
"""

import argparse
import asyncio
import logging
import signal
import sys
from typing import Any, Callable, Optional

from app.core.config import settings
from app.core.database import AsyncSessionLocal, engine
from app.services.job_recovery_service import JobRecoveryService

logger = logging.getLogger(__name__)


class RecoveryRunner:
    """Manages the lifecycle of the recovery runner with graceful shutdown."""

    def __init__(
        self,
        runner_id: str,
        batch_size: int,
        poll_interval: float,
        shutdown_timeout: float = 30.0,
        run_once: bool = False,
        enable_queued_reconciliation: bool = True,
        queued_batch_size: int = 10,
        queued_age_threshold_minutes: int = 10,
        queued_throttle_hours: int = 1,
        detection_session_factory: Optional[Callable[[], Any]] = None,
        recovery_session_factory: Optional[Callable[[], Any]] = None,
        engine_to_dispose: Optional[Any] = None,
    ):
        """Initialize the recovery runner.

        Args:
            runner_id: Unique identifier for this recovery runner instance
            batch_size: Maximum number of stuck PROCESSING jobs to process per batch
            poll_interval: Seconds between recovery batch attempts
            shutdown_timeout: Maximum seconds to wait for graceful shutdown
            run_once: If True, run exactly one batch and exit
            enable_queued_reconciliation: Enable QUEUED job reconciliation (default: True)
            queued_batch_size: Maximum number of QUEUED jobs to reconcile per batch
            queued_age_threshold_minutes: Minimum age for QUEUED jobs to be eligible for first recovery
            queued_throttle_hours: Minimum hours between QUEUED recovery attempts
            detection_session_factory: Factory for detection sessions (default: AsyncSessionLocal)
            recovery_session_factory: Factory for per-job recovery sessions (default: AsyncSessionLocal)
            engine_to_dispose: Database engine to dispose on cleanup (default: app.core.database.engine)
        """
        self.runner_id = runner_id
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.shutdown_timeout = shutdown_timeout
        self.run_once = run_once
        self.enable_queued_reconciliation = enable_queued_reconciliation
        self.queued_batch_size = queued_batch_size
        self.queued_age_threshold_minutes = queued_age_threshold_minutes
        self.queued_throttle_hours = queued_throttle_hours
        self.shutdown_event = asyncio.Event()
        self.recovery_task: Optional[asyncio.Task] = None
        self.exit_code = 0
        self.running = False

        # Dependency injection for testing
        self.detection_session_factory = detection_session_factory or AsyncSessionLocal
        self.recovery_session_factory = recovery_session_factory or AsyncSessionLocal
        self.engine_to_dispose = engine_to_dispose if engine_to_dispose is not None else engine

    async def run(self) -> int:
        """Run the recovery runner with graceful shutdown handling.

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        loop = asyncio.get_running_loop()

        # Register signal handlers within the event loop (skip if run_once)
        if not self.run_once:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._handle_signal, sig)

        mode = "single-batch" if self.run_once else "continuous"
        queued_status = "enabled" if self.enable_queued_reconciliation else "disabled"
        logger.info(
            f"Starting recovery runner: {self.runner_id} "
            f"(mode={mode}, batch_size={self.batch_size}, poll_interval={self.poll_interval}s, "
            f"queued_reconciliation={queued_status})"
        )

        try:
            if self.run_once:
                # Single batch mode - run once and exit
                await self._run_single_batch()
            else:
                # Continuous mode - run until shutdown signal
                self.running = True
                self.recovery_task = asyncio.create_task(
                    self._run_recovery_loop(), name="recovery-task"
                )

                logger.info(f"Recovery runner {self.runner_id} started successfully")

                # Monitor recovery task and wait for shutdown signal
                await self._monitor_until_shutdown()

        except Exception as e:
            logger.error(f"Recovery runner {self.runner_id} failed: {e}", exc_info=True)
            self.exit_code = 1
        finally:
            await self._cleanup()

        return self.exit_code

    async def _run_single_batch(self) -> None:
        """Run exactly one recovery batch and exit."""
        try:
            # Create fresh detection session for this batch
            async with self.detection_session_factory() as detection_session:
                recovery_service = JobRecoveryService(
                    detection_session,
                    session_factory=self.recovery_session_factory,
                    queued_age_threshold_minutes=self.queued_age_threshold_minutes,
                    queued_throttle_hours=self.queued_throttle_hours,
                )

                # Run PROCESSING recovery batch
                detected, recovered = await recovery_service.recover_stuck_jobs_batch(
                    batch_size=self.batch_size
                )

                logger.info(
                    f"processing_recovery_batch_completed: runner_id={self.runner_id}, "
                    f"detected={detected}, recovered={recovered}, failed={detected - recovered}"
                )

                if detected > recovered:
                    # Some jobs failed to recover
                    logger.warning(
                        f"processing_recovery_batch_partial_failure: runner_id={self.runner_id}, "
                        f"detected={detected}, recovered={recovered}, failed={detected - recovered}"
                    )
                    self.exit_code = 1

                # Run QUEUED reconciliation batch if enabled
                if self.enable_queued_reconciliation:
                    queued_detected, queued_reconciled = (
                        await recovery_service.reconcile_queued_jobs_batch(
                            batch_size=self.queued_batch_size,
                        )
                    )

                    logger.info(
                        f"queued_reconciliation_batch_completed: runner_id={self.runner_id}, "
                        f"detected={queued_detected}, reconciled={queued_reconciled}, "
                        f"failed={queued_detected - queued_reconciled}"
                    )

                    if queued_detected > queued_reconciled:
                        # Some QUEUED jobs failed to reconcile
                        logger.warning(
                            f"queued_reconciliation_batch_partial_failure: runner_id={self.runner_id}, "
                            f"detected={queued_detected}, reconciled={queued_reconciled}, "
                            f"failed={queued_detected - queued_reconciled}"
                        )
                        self.exit_code = 1

        except Exception as e:
            logger.error(
                f"recovery_batch_failed: runner_id={self.runner_id}, "
                f"error={str(e)}, error_type={type(e).__name__}",
                exc_info=True,
            )
            self.exit_code = 1

    async def _run_recovery_loop(self) -> None:
        """Run the continuous recovery loop."""
        consecutive_failures = 0
        max_consecutive_failures = 5

        while self.running:
            try:
                # Create fresh detection session for each iteration
                async with self.detection_session_factory() as detection_session:
                    recovery_service = JobRecoveryService(
                        detection_session,
                        session_factory=self.recovery_session_factory,
                        queued_age_threshold_minutes=self.queued_age_threshold_minutes,
                        queued_throttle_hours=self.queued_throttle_hours,
                    )

                    # Run PROCESSING recovery batch
                    detected, recovered = await recovery_service.recover_stuck_jobs_batch(
                        batch_size=self.batch_size
                    )

                    logger.info(
                        f"processing_recovery_batch_completed: runner_id={self.runner_id}, "
                        f"detected={detected}, recovered={recovered}, failed={detected - recovered}"
                    )

                    # Run QUEUED reconciliation batch if enabled
                    if self.enable_queued_reconciliation:
                        queued_detected, queued_reconciled = (
                            await recovery_service.reconcile_queued_jobs_batch(
                                batch_size=self.queued_batch_size,
                            )
                        )

                        logger.info(
                            f"queued_reconciliation_batch_completed: runner_id={self.runner_id}, "
                            f"detected={queued_detected}, reconciled={queued_reconciled}, "
                            f"failed={queued_detected - queued_reconciled}"
                        )

                    # Reset failure counter on success
                    consecutive_failures = 0

            except asyncio.CancelledError:
                # Shutdown requested - propagate cancellation
                logger.info(f"recovery_loop_cancelled: runner_id={self.runner_id}")
                raise

            except Exception as e:
                consecutive_failures += 1
                logger.error(
                    f"recovery_batch_failed: runner_id={self.runner_id}, "
                    f"error={str(e)}, error_type={type(e).__name__}, "
                    f"consecutive_failures={consecutive_failures}",
                    exc_info=True,
                )

                # Check if we've exceeded max consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(
                        f"recovery_max_failures_exceeded: runner_id={self.runner_id}, "
                        f"consecutive_failures={consecutive_failures}, "
                        f"max_consecutive_failures={max_consecutive_failures}"
                    )
                    self.exit_code = 1
                    self.shutdown_event.set()
                    break

                # Backoff on repeated failures (up to poll_interval)
                backoff_delay = min(5.0 * consecutive_failures, self.poll_interval)
                logger.info(
                    f"recovery_backoff: runner_id={self.runner_id}, "
                    f"consecutive_failures={consecutive_failures}, "
                    f"backoff_seconds={backoff_delay}"
                )
                await asyncio.sleep(backoff_delay)
                continue

            # Wait for next poll interval or shutdown signal
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self.poll_interval)
                # Shutdown signal received
                break
            except asyncio.TimeoutError:
                # Poll interval elapsed, continue to next iteration
                continue

    async def _monitor_until_shutdown(self) -> None:
        """Monitor recovery task and wait for shutdown signal."""
        if self.recovery_task is None:
            logger.error("Recovery task not initialized")
            self.exit_code = 1
            return

        # Wait for either shutdown signal or recovery task completion
        shutdown_wait = asyncio.create_task(self.shutdown_event.wait())

        done, pending = await asyncio.wait(
            [shutdown_wait, self.recovery_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Determine what completed first
        if self.recovery_task in done:
            # Recovery task finished before shutdown signal completed its wait
            # Cancel the shutdown waiter since we no longer need it
            shutdown_wait.cancel()
            try:
                await shutdown_wait
            except asyncio.CancelledError:
                pass

            # Check if recovery task failed or exited unexpectedly
            try:
                # This will re-raise any exception from the recovery task
                await self.recovery_task
                # If we get here, recovery exited without error
                # This is only unexpected if shutdown was not triggered
                if self.exit_code == 0 and not self.shutdown_event.is_set():
                    logger.warning(
                        f"recovery_task_exited_unexpectedly (runner_id={self.runner_id})"
                    )
                    self.exit_code = 1
            except Exception:
                # Exception already logged in _run_recovery_loop
                self.exit_code = 1
        else:
            # Shutdown signal received first - this is the normal graceful shutdown path
            # Cancel the shutdown waiter since it completed
            # DO NOT cancel recovery_task - let _cleanup() stop it gracefully
            shutdown_wait.cancel()
            try:
                await shutdown_wait
            except asyncio.CancelledError:
                pass

    def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signals.

        Args:
            sig: The signal received
        """
        logger.info(
            f"Received signal {sig.name}, initiating graceful shutdown... (runner_id={self.runner_id})"
        )
        self.running = False
        self.shutdown_event.set()

    async def _cleanup(self) -> None:
        """Clean up resources and wait for graceful shutdown with timeout."""
        logger.info(f"Shutting down recovery runner {self.runner_id}...")

        try:
            # Apply bounded timeout to entire cleanup sequence
            await asyncio.wait_for(self._cleanup_internal(), timeout=self.shutdown_timeout)
            logger.info(f"Recovery runner {self.runner_id} shutdown complete")
        except asyncio.TimeoutError:
            logger.error(
                f"Shutdown did not complete within {self.shutdown_timeout}s timeout, forcing cleanup (runner_id={self.runner_id})"
            )
            self.exit_code = 1
            await self._force_cleanup()

    async def _cleanup_internal(self) -> None:
        """Internal cleanup logic without timeout."""
        # Stop the recovery loop
        self.running = False

        # Wait for recovery task to complete if it exists
        if self.recovery_task and not self.recovery_task.done():
            try:
                await self.recovery_task
                logger.info(f"Recovery task completed (runner_id={self.runner_id})")
            except asyncio.CancelledError:
                logger.info(f"Recovery task was cancelled (runner_id={self.runner_id})")
            except Exception as e:
                # Exception already logged in _run_recovery_loop
                logger.debug(
                    f"Recovery task exception during cleanup: {e} (runner_id={self.runner_id})"
                )

        # Dispose of database engine to release all connections
        try:
            await self.engine_to_dispose.dispose()
            logger.info(f"Database engine disposed (runner_id={self.runner_id})")
        except Exception as e:
            logger.error(
                f"Error disposing database engine: {e} (runner_id={self.runner_id})",
                exc_info=True,
            )

    async def _force_cleanup(self) -> None:
        """Force cleanup by cancelling remaining tasks."""
        if self.recovery_task and not self.recovery_task.done():
            logger.warning(f"Cancelling recovery task (runner_id={self.runner_id})")
            self.recovery_task.cancel()
            try:
                await self.recovery_task
            except asyncio.CancelledError:
                logger.info(f"Recovery task cancelled (runner_id={self.runner_id})")
            except Exception as e:
                logger.error(f"Error during forced cleanup: {e} (runner_id={self.runner_id})")

        # Still try to dispose engine
        try:
            await self.engine_to_dispose.dispose()
        except Exception:
            pass  # Best effort


def validate_args(args: argparse.Namespace) -> None:
    """Validate command-line arguments.

    Args:
        args: Parsed arguments

    Raises:
        ValueError: If any argument is invalid
    """
    if not args.runner_id or not args.runner_id.strip():
        raise ValueError("--runner-id must be a non-empty string")

    if args.batch_size <= 0:
        raise ValueError(f"--batch-size must be positive, got {args.batch_size}")

    if args.poll_interval <= 0 or not float("inf") > args.poll_interval:
        raise ValueError(f"--poll-interval must be positive and finite, got {args.poll_interval}")

    if args.shutdown_timeout <= 0 or not float("inf") > args.shutdown_timeout:
        raise ValueError(
            f"--shutdown-timeout must be positive and finite, got {args.shutdown_timeout}"
        )

    # Validate QUEUED reconciliation arguments
    if args.queued_batch_size <= 0:
        raise ValueError(f"--queued-batch-size must be positive, got {args.queued_batch_size}")

    if args.queued_age_threshold_minutes <= 0:
        raise ValueError(
            f"--queued-age-threshold-minutes must be positive, got {args.queued_age_threshold_minutes}"
        )

    if args.queued_throttle_hours <= 0:
        raise ValueError(
            f"--queued-throttle-hours must be positive, got {args.queued_throttle_hours}"
        )


def main() -> None:
    """Main entry point for the recovery CLI."""
    parser = argparse.ArgumentParser(
        description="Run the stuck-job recovery process",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Continuous recovery with defaults (PROCESSING + QUEUED)
  python -m app.recovery.recovery_cli --runner-id recovery-1

  # Custom batch size and interval
  python -m app.recovery.recovery_cli --runner-id recovery-1 --batch-size 20 --poll-interval 30.0

  # Disable QUEUED reconciliation (PROCESSING only)
  python -m app.recovery.recovery_cli --runner-id recovery-1 --no-queued-reconciliation

  # Single batch mode (for testing)
  python -m app.recovery.recovery_cli --runner-id recovery-test --once
        """,
    )

    parser.add_argument(
        "--runner-id",
        type=str,
        required=True,
        help="Unique identifier for this recovery runner instance",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.JOB_RECOVERY_BATCH_SIZE,
        help=f"Maximum number of stuck PROCESSING jobs to process per batch (default: {settings.JOB_RECOVERY_BATCH_SIZE})",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=settings.JOB_RECOVERY_INTERVAL,
        help=f"Seconds between recovery batch attempts (default: {settings.JOB_RECOVERY_INTERVAL})",
    )
    parser.add_argument(
        "--shutdown-timeout",
        type=float,
        default=30.0,
        help="Maximum seconds to wait for graceful shutdown (default: 30.0)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one recovery batch and exit (useful for testing or cron jobs)",
    )

    # QUEUED reconciliation arguments (Step 289D)
    parser.add_argument(
        "--no-queued-reconciliation",
        action="store_true",
        help="Disable QUEUED job reconciliation (default: enabled)",
    )
    parser.add_argument(
        "--queued-batch-size",
        type=int,
        default=settings.QUEUED_RECONCILIATION_BATCH_SIZE,
        help=f"Maximum number of QUEUED jobs to reconcile per batch (default: {settings.QUEUED_RECONCILIATION_BATCH_SIZE})",
    )
    parser.add_argument(
        "--queued-age-threshold-minutes",
        type=int,
        default=settings.QUEUED_RECONCILIATION_AGE_THRESHOLD_MINUTES,
        help=f"Minimum age (minutes) for QUEUED jobs to be eligible for first recovery (default: {settings.QUEUED_RECONCILIATION_AGE_THRESHOLD_MINUTES})",
    )
    parser.add_argument(
        "--queued-throttle-hours",
        type=int,
        default=settings.QUEUED_RECONCILIATION_THROTTLE_HOURS,
        help=f"Minimum hours between QUEUED recovery attempts (default: {settings.QUEUED_RECONCILIATION_THROTTLE_HOURS})",
    )

    args = parser.parse_args()

    # Validate arguments before starting
    try:
        validate_args(args)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Run the recovery runner
    runner = RecoveryRunner(
        runner_id=args.runner_id,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        shutdown_timeout=args.shutdown_timeout,
        run_once=args.once,
        enable_queued_reconciliation=not args.no_queued_reconciliation,
        queued_batch_size=args.queued_batch_size,
        queued_age_threshold_minutes=args.queued_age_threshold_minutes,
        queued_throttle_hours=args.queued_throttle_hours,
    )

    try:
        exit_code = asyncio.run(runner.run())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
