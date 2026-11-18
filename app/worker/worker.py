"""Worker implementation for job processing"""

import asyncio
import os
import signal
import sys
from datetime import datetime
from typing import Optional
from uuid import UUID

import click

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.logging_config import setup_logging
from app.core.redis_client import redis_client
from app.db.models import JobStatus
from app.models.job import JobUpdate
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.services.worker_service import WorkerService
from app.worker.job_executor import JobExecutor

logger = setup_logging()


class Worker:
    """Worker for processing jobs from the queue"""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.running = False
        self.current_job_id: Optional[UUID] = None
        self.executor = JobExecutor()
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.poller_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the worker"""
        self.running = True

        logger.info(
            "worker_starting",
            worker_id=self.worker_id,
            prefetch_count=settings.WORKER_PREFETCH_COUNT,
        )

        # Register worker
        async with AsyncSessionLocal() as db:
            worker_service = WorkerService(db)
            await worker_service.register_worker(
                self.worker_id,
                worker_metadata={
                    "hostname": os.getenv("HOSTNAME", "unknown"),
                    "pid": os.getpid(),
                },
            )
            await db.commit()

        # Start heartbeat task
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Start delayed queue poller task
        self.poller_task = asyncio.create_task(self._delayed_queue_poller_loop())

        # Start processing loop
        try:
            await self._processing_loop()
        finally:
            await self.stop()

    async def stop(self):
        """Stop the worker gracefully"""
        logger.info("worker_stopping", worker_id=self.worker_id)
        self.running = False

        # Cancel heartbeat
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

        # Cancel delayed queue poller
        if self.poller_task:
            self.poller_task.cancel()
            try:
                await self.poller_task
            except asyncio.CancelledError:
                pass

        # Deactivate worker
        async with AsyncSessionLocal() as db:
            worker_service = WorkerService(db)
            await worker_service.deactivate_worker(self.worker_id)
            await db.commit()

        # Close Redis connection
        await redis_client.close()

        logger.info("worker_stopped", worker_id=self.worker_id)

    async def _processing_loop(self):
        """Main processing loop"""
        queue_service = QueueService(redis_client)

        while self.running:
            try:
                # Dequeue next job
                message = await queue_service.dequeue_job()

                if not message:
                    # No jobs available, wait before retrying
                    await asyncio.sleep(1)
                    continue

                job_id = UUID(message["job_id"])

                # Process the job
                await self._process_job(job_id)

            except asyncio.CancelledError:
                logger.info("worker_cancelled", worker_id=self.worker_id)
                break

            except Exception as e:
                logger.error(
                    "processing_loop_error",
                    worker_id=self.worker_id,
                    error=str(e),
                )
                await asyncio.sleep(1)

    async def _process_job(self, job_id: UUID):
        """Process a single job"""
        self.current_job_id = job_id
        start_time = datetime.utcnow()

        async with AsyncSessionLocal() as db:
            try:
                # Initialize services
                queue_service = QueueService(redis_client)
                job_service = JobService(db, redis_client, queue_service)
                worker_service = WorkerService(db)

                # Atomically claim the job for this worker
                # This prevents race conditions where multiple workers process the same job
                job = await job_service.claim_job(job_id, self.worker_id)

                if not job:
                    # Job claim failed - either not in eligible state or already claimed
                    logger.info(
                        "job_claim_failed_skipping",
                        job_id=str(job_id),
                        worker_id=self.worker_id,
                        reason="Job not eligible or already claimed by another worker",
                    )
                    return

                # Commit the claim before executing the job
                # This ensures the job is marked as PROCESSING in the database
                await db.commit()

                logger.info(
                    "job_processing_started",
                    job_id=str(job_id),
                    job_type=job.job_type,
                    worker_id=self.worker_id,
                )

                # Execute the job with timeout
                try:
                    result = await asyncio.wait_for(
                        self.executor.execute(job.job_type, job.payload),
                        timeout=job.timeout,
                    )

                    # Job completed successfully
                    duration = (datetime.utcnow() - start_time).total_seconds()

                    await job_service.update_job_status(
                        job_id,
                        JobUpdate(
                            status=JobStatus.COMPLETED,
                            result=result,
                        ),
                    )

                    await job_service.create_job_execution(
                        job_id=job_id,
                        worker_id=self.worker_id,
                        status=JobStatus.COMPLETED,
                        attempt_number=(job.retry_count or 0) + 1,
                        result=result,
                        duration=duration,
                    )

                    await worker_service.increment_job_count(
                        self.worker_id,
                        success=True,
                    )

                    await db.commit()

                    logger.info(
                        "job_completed",
                        job_id=str(job_id),
                        duration=duration,
                        worker_id=self.worker_id,
                    )

                except asyncio.TimeoutError:
                    # Job timed out
                    logger.warning(
                        "job_timeout",
                        job_id=str(job_id),
                        timeout=job.timeout,
                    )

                    await job_service.update_job_status(
                        job_id,
                        JobUpdate(
                            status=JobStatus.FAILED,
                            error_message=f"Job exceeded timeout of {job.timeout} seconds",
                            error_type="TimeoutError",
                        ),
                    )
                    await db.commit()

                    # Retry job
                    await job_service.retry_job(job_id)
                    await db.commit()

                except Exception as e:
                    # Job failed
                    error_type = type(e).__name__
                    error_message = str(e)

                    logger.error(
                        "job_failed",
                        job_id=str(job_id),
                        error_type=error_type,
                        error=error_message,
                    )

                    duration = (datetime.utcnow() - start_time).total_seconds()

                    await job_service.update_job_status(
                        job_id,
                        JobUpdate(
                            status=JobStatus.FAILED,
                            error_message=error_message,
                            error_type=error_type,
                        ),
                    )

                    await job_service.create_job_execution(
                        job_id=job_id,
                        worker_id=self.worker_id,
                        status=JobStatus.FAILED,
                        attempt_number=(job.retry_count or 0) + 1,
                        error_message=error_message,
                        error_type=error_type,
                        duration=duration,
                    )

                    await worker_service.increment_job_count(
                        self.worker_id,
                        success=False,
                    )

                    await db.commit()

                    # Retry job
                    await job_service.retry_job(job_id)
                    await db.commit()

            except Exception as e:
                logger.error(
                    "job_processing_error",
                    job_id=str(job_id),
                    error=str(e),
                )
                await db.rollback()

            finally:
                self.current_job_id = None

    async def _heartbeat_loop(self):
        """Send periodic heartbeat"""
        while self.running:
            try:
                async with AsyncSessionLocal() as db:
                    worker_service = WorkerService(db)
                    await worker_service.update_heartbeat(
                        self.worker_id,
                        current_job_id=str(self.current_job_id) if self.current_job_id else None,
                    )
                    await db.commit()

                await asyncio.sleep(settings.WS_HEARTBEAT_INTERVAL)

            except asyncio.CancelledError:
                break

            except Exception as e:
                logger.error(
                    "heartbeat_error",
                    worker_id=self.worker_id,
                    error=str(e),
                )
                await asyncio.sleep(5)

    async def _delayed_queue_poller_loop(self):
        """
        Poll delayed queue and transfer ready jobs to main queue.

        Runs every DELAYED_JOB_CHECK_INTERVAL seconds (default 1s).
        Transfers jobs from job_queue_delayed to job_queue when execute_at <= now.

        IMPORTANT LIMITATIONS:
        - Does NOT guarantee exactly-once transfer (Redis/DB not atomic)
        - Invalid jobs quarantined to job_queue_invalid (requires manual investigation)
        - Does NOT prevent processing loop from working if poller fails
        """
        logger.info(
            "delayed_queue_poller_starting",
            worker_id=self.worker_id,
            interval_seconds=settings.DELAYED_JOB_CHECK_INTERVAL,
        )

        while self.running:
            try:
                transferred = await redis_client.transfer_ready_jobs_atomic(
                    main_queue="job_queue",
                    delayed_queue="job_queue_delayed",
                    batch_limit=100,
                    quarantine_queue="job_queue_invalid",
                )

                if transferred > 0:
                    logger.info(
                        "delayed_jobs_transferred",
                        count=transferred,
                        worker_id=self.worker_id,
                    )

                await asyncio.sleep(settings.DELAYED_JOB_CHECK_INTERVAL)

            except asyncio.CancelledError:
                logger.info(
                    "delayed_queue_poller_cancelled",
                    worker_id=self.worker_id,
                )
                break

            except Exception as e:
                logger.error(
                    "delayed_queue_poller_error",
                    worker_id=self.worker_id,
                    error=str(e),
                    error_type=type(e).__name__,
                    impact="Delayed retries may be postponed. Job processing continues normally.",
                )
                # Sleep longer on error to avoid tight error loop
                await asyncio.sleep(5)

        logger.info(
            "delayed_queue_poller_stopped",
            worker_id=self.worker_id,
        )


# Global worker instance
worker_instance: Optional[Worker] = None


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global worker_instance
    if worker_instance:
        logger.info("shutdown_signal_received", signal=signum)
        asyncio.create_task(worker_instance.stop())


@click.command()
@click.option(
    "--worker-id", default=os.getenv("WORKER_ID", f"worker-{os.getpid()}"), help="Unique worker ID"
)
def main(worker_id: str):
    """Start the worker"""
    global worker_instance

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start worker
    worker_instance = Worker(worker_id)

    try:
        asyncio.run(worker_instance.start())
    except KeyboardInterrupt:
        logger.info("worker_interrupted")
    except Exception as e:
        logger.error("worker_error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
