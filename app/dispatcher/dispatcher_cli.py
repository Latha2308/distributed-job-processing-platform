"""Standalone outbox dispatcher CLI process.

This module provides a standalone process that continuously polls the job_outbox
table and dispatches events to Redis. It follows the worker pattern with proper
async signal handling and graceful shutdown.

Usage:
    python -m app.dispatcher.dispatcher_cli --dispatcher-id dispatcher-1 --batch-size 10 --poll-interval 5.0 --max-attempts 5
"""

import argparse
import asyncio
import logging
import signal
import sys
from typing import Optional


from app.core.redis_client import redis_client
from app.services.outbox_dispatcher import OutboxDispatcher
from app.services.queue_service import QueueService

logger = logging.getLogger(__name__)


class DispatcherRunner:
    """Manages the lifecycle of the outbox dispatcher with graceful shutdown."""

    def __init__(
        self,
        dispatcher_id: str,
        batch_size: int = 10,
        poll_interval: float = 5.0,
        max_attempts: int = 5,
        shutdown_timeout: float = 30.0,
    ):
        """Initialize the dispatcher runner.

        Args:
            dispatcher_id: Unique identifier for this dispatcher instance
            batch_size: Number of events to process per batch
            poll_interval: Seconds between polling attempts
            max_attempts: Maximum publication attempts before manual review required
            shutdown_timeout: Maximum seconds to wait for graceful shutdown
        """
        self.dispatcher_id = dispatcher_id
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.shutdown_timeout = shutdown_timeout
        self.dispatcher: Optional[OutboxDispatcher] = None
        self.queue_service: Optional[QueueService] = None
        self.shutdown_event = asyncio.Event()
        self.dispatcher_task: Optional[asyncio.Task] = None
        self.exit_code = 0

    async def run(self) -> int:
        """Run the dispatcher with graceful shutdown handling.

        Returns:
            Exit code (0 for success, non-zero for failure)
        """
        loop = asyncio.get_running_loop()

        # Register signal handlers within the event loop
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal, sig)

        logger.info(
            f"Starting outbox dispatcher: {self.dispatcher_id} "
            f"(batch_size={self.batch_size}, poll_interval={self.poll_interval}s, "
            f"max_attempts={self.max_attempts})"
        )

        try:
            # Initialize queue service with Redis client
            self.queue_service = QueueService(redis_client)

            # Initialize dispatcher
            self.dispatcher = OutboxDispatcher(
                queue_service=self.queue_service,
                batch_size=self.batch_size,
                poll_interval_seconds=self.poll_interval,
                max_attempts=self.max_attempts,
            )

            # Start dispatcher in background task
            self.dispatcher_task = asyncio.create_task(
                self._run_dispatcher(), name="dispatcher-task"
            )

            logger.info(f"Dispatcher {self.dispatcher_id} started successfully")

            # Monitor dispatcher task and wait for shutdown signal
            await self._monitor_until_shutdown()

        except Exception as e:
            logger.error(f"Dispatcher {self.dispatcher_id} failed: {e}", exc_info=True)
            self.exit_code = 1
        finally:
            await self._cleanup()

        return self.exit_code

    async def _run_dispatcher(self) -> None:
        """Run the dispatcher's start() method."""
        if self.dispatcher is None:
            logger.error("Dispatcher not initialized")
            self.exit_code = 1
            self.shutdown_event.set()
            return

        try:
            await self.dispatcher.start()
            # If start() exits normally (e.g., after stop() is called), it's expected
            logger.info("Dispatcher start() completed")
        except Exception as e:
            logger.error(f"Dispatcher task failed with exception: {e}", exc_info=True)
            # Signal shutdown on unexpected error
            self.exit_code = 1
            self.shutdown_event.set()
            raise

    async def _monitor_until_shutdown(self) -> None:
        """Monitor dispatcher task and wait for shutdown signal."""
        if self.dispatcher_task is None:
            logger.error("Dispatcher task not initialized")
            self.exit_code = 1
            return

        # Wait for either shutdown signal or dispatcher task completion
        shutdown_wait = asyncio.create_task(self.shutdown_event.wait())

        done, pending = await asyncio.wait(
            [shutdown_wait, self.dispatcher_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Determine what completed first
        if self.dispatcher_task in done:
            # Dispatcher task finished before shutdown signal - unexpected
            # Cancel the shutdown waiter since we no longer need it
            shutdown_wait.cancel()
            try:
                await shutdown_wait
            except asyncio.CancelledError:
                pass

            # Check if dispatcher task failed or exited unexpectedly
            try:
                # This will re-raise any exception from the dispatcher task
                await self.dispatcher_task
                # If we get here, dispatcher exited without error but before shutdown signal
                logger.warning("Dispatcher task exited unexpectedly without error")
                self.exit_code = 1
            except Exception:
                # Exception already logged in _run_dispatcher
                self.exit_code = 1
        else:
            # Shutdown signal received first - this is the normal graceful shutdown path
            # Cancel the shutdown waiter since it completed
            # DO NOT cancel dispatcher_task - let _cleanup() stop it gracefully
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
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
        self.shutdown_event.set()

    async def _cleanup(self) -> None:
        """Clean up resources and wait for graceful shutdown with timeout."""
        logger.info(f"Shutting down dispatcher {self.dispatcher_id}...")

        try:
            # Apply bounded timeout to entire cleanup sequence
            await asyncio.wait_for(self._cleanup_internal(), timeout=self.shutdown_timeout)
            logger.info(f"Dispatcher {self.dispatcher_id} shutdown complete")
        except asyncio.TimeoutError:
            logger.error(
                f"Shutdown did not complete within {self.shutdown_timeout}s timeout, "
                "forcing cleanup"
            )
            self.exit_code = 1
            await self._force_cleanup()

    async def _cleanup_internal(self) -> None:
        """Internal cleanup logic without timeout."""
        # Stop the dispatcher
        if self.dispatcher:
            try:
                await self.dispatcher.stop()
                logger.info("Dispatcher stopped")
            except Exception as e:
                logger.error(f"Error stopping dispatcher: {e}", exc_info=True)
                self.exit_code = 1

        # Wait for dispatcher task to complete
        if self.dispatcher_task and not self.dispatcher_task.done():
            try:
                await self.dispatcher_task
                logger.info("Dispatcher task completed")
            except asyncio.CancelledError:
                logger.info("Dispatcher task was cancelled")
            except Exception as e:
                # Exception already logged in _run_dispatcher
                logger.debug(f"Dispatcher task exception during cleanup: {e}")

        # Clean up Redis connection
        try:
            await redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}", exc_info=True)

    async def _force_cleanup(self) -> None:
        """Force cleanup by cancelling remaining tasks."""
        if self.dispatcher_task and not self.dispatcher_task.done():
            logger.warning("Cancelling dispatcher task")
            self.dispatcher_task.cancel()
            try:
                await self.dispatcher_task
            except asyncio.CancelledError:
                logger.info("Dispatcher task cancelled")
            except Exception as e:
                logger.error(f"Error during forced cleanup: {e}")

        # Still try to close Redis
        try:
            await redis_client.close()
        except Exception:
            pass  # Best effort


def main() -> None:
    """Main entry point for the dispatcher CLI."""
    parser = argparse.ArgumentParser(description="Run the outbox dispatcher process")
    parser.add_argument(
        "--dispatcher-id",
        type=str,
        required=True,
        help="Unique identifier for this dispatcher instance",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of events to process per batch (default: 10)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds between polling attempts (default: 5.0)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Maximum publication attempts before manual review required (default: 5)",
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

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Run the dispatcher
    runner = DispatcherRunner(
        dispatcher_id=args.dispatcher_id,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        max_attempts=args.max_attempts,
        shutdown_timeout=args.shutdown_timeout,
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
