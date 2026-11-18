"""Job service for business logic"""

import time
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID

from sqlalchemy import select, func, and_, or_, Integer, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging_config import setup_logging
from app.core.metrics import (
    job_submissions,
    job_completions,
    job_processing_duration,
    queue_depth,
    job_retries,
    job_failures,
)
from app.core.redis_client import RedisClient
from app.db.models import Job, JobExecution, JobStatus
from app.models.job import JobCreate, JobUpdate, JobStats, JobMetrics
from app.services.queue_service import QueueService

logger = setup_logging()


class JobService:
    """Service for job management"""

    def __init__(
        self,
        db: AsyncSession,
        redis: RedisClient,
        queue_service: QueueService,
    ):
        self.db = db
        self.redis = redis
        self.queue_service = queue_service

    async def create_job(self, job_data: JobCreate) -> Job:
        """
        Create a new job and enqueue it.

        Args:
            job_data: Job creation data

        Returns:
            Created job

        Raises:
            ValueError: If idempotency key already exists
        """
        # Check idempotency
        if job_data.idempotency_key:
            existing = await self._get_job_by_idempotency_key(job_data.idempotency_key)
            if existing:
                logger.info(
                    "job_already_exists",
                    idempotency_key=job_data.idempotency_key,
                    job_id=str(existing.id),
                )
                return existing

        # Create job record
        job = Job(
            job_type=job_data.job_type,
            payload=job_data.payload,
            priority=job_data.priority,
            idempotency_key=job_data.idempotency_key,
            timeout=job_data.timeout,
            max_retries=job_data.max_retries,
            status=JobStatus.QUEUED,
        )

        self.db.add(job)
        await self.db.flush()
        await self.db.refresh(job)

        # Enqueue job
        await self.queue_service.enqueue_job(
            job_id=job.id,
            job_type=job.job_type,
            priority=job.priority if job.priority is not None else 0,
        )

        # Update metrics
        job_submissions.labels(job_type=job.job_type, priority=str(job.priority)).inc()

        # Update queue depth metric
        depth = await self.queue_service.get_queue_depth()
        queue_depth.labels(priority=str(job.priority)).set(depth)

        logger.info(
            "job_created",
            job_id=str(job.id),
            job_type=job.job_type,
            priority=job.priority,
        )

        return job

    async def get_job(self, job_id: UUID) -> Optional[Job]:
        """Get job by ID"""
        result = await self.db.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()

    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        job_type: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[List[Job], int]:
        """
        List jobs with optional filters.

        Args:
            status: Filter by status
            job_type: Filter by job type
            page: Page number (1-indexed)
            page_size: Items per page

        Returns:
            Tuple of (jobs list, total count)
        """
        # Build query
        query = select(Job)

        if status:
            query = query.where(Job.status == status)
        if job_type:
            query = query.where(Job.job_type == job_type)

        # Get total count
        count_query = select(func.count()).select_from(Job)
        if status:
            count_query = count_query.where(Job.status == status)
        if job_type:
            count_query = count_query.where(Job.job_type == job_type)

        count_result = await self.db.execute(count_query)
        total = count_result.scalar_one()

        # Apply pagination
        query = query.order_by(Job.created_at.desc())
        query = query.offset((page - 1) * page_size).limit(page_size)

        # Execute query
        result = await self.db.execute(query)
        jobs = result.scalars().all()

        return list(jobs), total

    async def claim_job(
        self,
        job_id: UUID,
        worker_id: str,
    ) -> Optional[Job]:
        """
        Atomically claim a job for processing using conditional UPDATE.

        Uses a single database UPDATE with RETURNING to ensure only one worker
        can claim a job. The UPDATE only succeeds if the job is in an eligible state.

        Eligible states:
        - QUEUED: Jobs ready for first execution
        - RETRYING: Jobs scheduled for retry where next_retry_at <= now

        Ineligible states:
        - PROCESSING: Already being worked on
        - COMPLETED: Already finished
        - FAILED: Requires manual intervention or retry scheduling
        - DEAD_LETTER: Permanently failed

        Args:
            job_id: ID of job to claim
            worker_id: ID of worker attempting to claim

        Returns:
            Job object if successfully claimed, None if claim failed

        Raises:
            None - returns None on failure rather than raising exceptions
        """
        now = datetime.utcnow()

        # Build atomic UPDATE statement with conditions
        stmt = (
            update(Job)
            .where(
                and_(
                    Job.id == job_id,
                    # Allow QUEUED jobs
                    or_(
                        Job.status == JobStatus.QUEUED,
                        # Allow RETRYING jobs only if retry time has arrived
                        and_(
                            Job.status == JobStatus.RETRYING,
                            Job.next_retry_at.isnot(None),
                            Job.next_retry_at <= now,
                        ),
                    ),
                )
            )
            .values(
                status=JobStatus.PROCESSING,
                worker_id=worker_id,
                started_at=now,
                updated_at=now,
                # Calculate queue wait time for QUEUED jobs
                queue_wait_time=func.EXTRACT("epoch", now - Job.created_at),
            )
            .returning(Job)
        )

        # Execute atomic update
        result = await self.db.execute(stmt)
        claimed_job = result.scalar_one_or_none()

        if claimed_job:
            # Refresh to get all attributes
            await self.db.refresh(claimed_job)

            logger.info(
                "job_claimed",
                job_id=str(job_id),
                worker_id=worker_id,
                previous_status="queued_or_retrying",
                retry_count=claimed_job.retry_count,
            )

            return claimed_job
        else:
            # Claim failed - job not in eligible state or already claimed
            logger.debug(
                "job_claim_failed",
                job_id=str(job_id),
                worker_id=worker_id,
                reason="Job not in eligible state or already claimed by another worker",
            )
            return None

    def _handle_completed_status(self, job: Job, update_data: JobUpdate) -> None:
        """Handle job completion status update."""
        job.completed_at = datetime.utcnow()
        if job.started_at:
            duration = (job.completed_at - job.started_at).total_seconds()
            job.processing_duration = duration

            # Update metrics
            job_processing_duration.labels(job_type=job.job_type).observe(duration)

        job_completions.labels(job_type=job.job_type, status="completed").inc()

    def _handle_failed_status(self, job: Job, update_data: JobUpdate) -> None:
        """Handle job failure status update."""
        job.completed_at = datetime.utcnow()
        job_completions.labels(job_type=job.job_type, status="failed").inc()

        if update_data.error_type:
            job_failures.labels(job_type=job.job_type, error_type=update_data.error_type).inc()

    def _handle_processing_status(self, job: Job, worker_id: Optional[str]) -> None:
        """Handle job processing status update."""
        job.started_at = datetime.utcnow()
        job.worker_id = worker_id

        # Calculate queue wait time
        if job.created_at:
            wait_time = (job.started_at - job.created_at).total_seconds()
            job.queue_wait_time = wait_time

    async def update_job_status(
        self,
        job_id: UUID,
        update_data: JobUpdate,
        worker_id: Optional[str] = None,
    ) -> Optional[Job]:
        """Update job status"""
        job = await self.get_job(job_id)
        if not job:
            return None

        if update_data.status:
            job.status = update_data.status

            # Delegate to status-specific handlers
            if update_data.status == JobStatus.COMPLETED:
                self._handle_completed_status(job, update_data)
            elif update_data.status == JobStatus.FAILED:
                self._handle_failed_status(job, update_data)
            elif update_data.status == JobStatus.PROCESSING:
                self._handle_processing_status(job, worker_id)

        if update_data.result is not None:
            job.result = update_data.result

        if update_data.error_message:
            job.error_message = update_data.error_message

        if update_data.error_type:
            job.error_type = update_data.error_type

        job.updated_at = datetime.utcnow()

        await self.db.flush()
        await self.db.refresh(job)

        logger.info(
            "job_updated",
            job_id=str(job.id),
            status=job.status,
            worker_id=worker_id,
        )

        return job

    async def retry_job(self, job_id: UUID) -> Optional[Job]:
        """
        Schedule a failed job for retry with exponential backoff delay.

        Uses Redis delayed queue to enforce retry delays (2s, 4s, 8s with default config).

        IMPORTANT LIMITATIONS:
        - Redis and PostgreSQL are NOT atomic together
        - If Redis succeeds but DB commit fails: job may be in delayed queue without RETRYING status
        - Worker status check is defensive, NOT atomic job claiming
        - No automatic recovery for stuck jobs (requires manual intervention)
        - Poller only transfers Redis entries, does NOT update database status

        Args:
            job_id: Job ID to retry

        Returns:
            Updated job or None if job doesn't exist or exceeded max retries

        Raises:
            Exception: If Redis operation fails (triggers caller rollback)
        """
        job = await self.get_job(job_id)
        if not job:
            return None

        # Check if job has exceeded max retries
        if (job.retry_count or 0) >= (job.max_retries or 0):
            # Move to dead letter queue
            await self.move_to_dead_letter_queue(job_id)
            return job

        # Calculate exponential backoff delay for NEXT retry attempt
        # Since retry_count is incremented AFTER Redis scheduling,
        # we calculate delay for (retry_count + 1)
        # Example: retry_count=0 → delay=2s, retry_count=1 → delay=4s, retry_count=2 → delay=8s
        from app.core.config import settings

        next_retry_count = (job.retry_count or 0) + 1
        delay = settings.INITIAL_RETRY_DELAY * (settings.RETRY_BACKOFF_BASE**next_retry_count)
        next_retry_at = datetime.utcnow() + timedelta(seconds=delay)
        execute_at = time.time() + delay

        # Schedule retry in Redis delayed queue
        # CRITICAL: Do this BEFORE incrementing retry_count
        # If Redis fails, we don't consume a retry attempt
        try:
            success = await self.redis.push_to_delayed_queue(
                job_data={
                    "job_id": str(job.id),
                    "job_type": job.job_type,
                    "priority": job.priority,
                },
                execute_at=execute_at,
                queue_name="job_queue_delayed",
            )

            if not success:
                # Redis validation failure (invalid priority, NaN timestamp, etc.)
                # Job stays FAILED, retry NOT consumed
                logger.error(
                    "retry_scheduling_validation_failed",
                    job_id=str(job.id),
                    retry_count=job.retry_count,
                    next_retry_count=next_retry_count,
                    delay_seconds=delay,
                    next_retry_at=next_retry_at.isoformat(),
                    action_required="Job remains FAILED. Manual retry required or investigate validation failure.",
                )
                return job

        except Exception as e:
            # Redis network error, connection timeout, etc.
            # Exception propagates to caller (worker) which will rollback transaction
            logger.error(
                "retry_scheduling_redis_error",
                job_id=str(job.id),
                retry_count=job.retry_count,
                next_retry_count=next_retry_count,
                delay_seconds=delay,
                error=str(e),
                error_type=type(e).__name__,
                action_required="Redis operation failed. Transaction will be rolled back. Job remains FAILED.",
            )
            raise

        # Redis scheduling successful - now update database
        job.retry_count = (job.retry_count or 0) + 1
        job.status = JobStatus.RETRYING
        job.next_retry_at = next_retry_at
        job.updated_at = datetime.utcnow()

        await self.db.flush()
        await self.db.refresh(job)

        # Update metrics
        job_retries.labels(job_type=job.job_type).inc()

        logger.info(
            "job_scheduled_for_retry",
            job_id=str(job.id),
            retry_count=job.retry_count,
            next_retry_at=next_retry_at.isoformat(),
            delay_seconds=delay,
        )

        return job

    async def move_to_dead_letter_queue(self, job_id: UUID) -> Optional[Job]:
        """Move job to dead letter queue"""
        job = await self.get_job(job_id)
        if not job:
            return None

        job.status = JobStatus.DEAD_LETTER
        job.is_dead_letter = True
        job.moved_to_dlq_at = datetime.utcnow()
        job.updated_at = datetime.utcnow()

        await self.db.flush()
        await self.db.refresh(job)

        logger.warning(
            "job_moved_to_dlq",
            job_id=str(job.id),
            job_type=job.job_type,
            retry_count=job.retry_count,
        )

        return job

    async def get_job_stats(self) -> JobStats:
        """Get job statistics"""
        # Count by status
        status_counts = await self.db.execute(
            select(Job.status, func.count(Job.id)).group_by(Job.status)
        )

        counts = {status: count for status, count in status_counts.all()}
        total = sum(counts.values())

        # Calculate averages
        avg_query = await self.db.execute(
            select(
                func.avg(Job.processing_duration),
                func.avg(Job.queue_wait_time),
            ).where(Job.status == JobStatus.COMPLETED)
        )
        avg_processing, avg_wait = avg_query.one()

        # Calculate rates
        completed = counts.get(JobStatus.COMPLETED, 0)
        failed = counts.get(JobStatus.FAILED, 0)
        dead_letter = counts.get(JobStatus.DEAD_LETTER, 0)

        total_finished = completed + failed + dead_letter
        success_rate = (completed / total_finished * 100) if total_finished > 0 else 0
        failure_rate = ((failed + dead_letter) / total_finished * 100) if total_finished > 0 else 0

        return JobStats(
            total_jobs=total,
            pending=counts.get(JobStatus.PENDING, 0),
            queued=counts.get(JobStatus.QUEUED, 0),
            processing=counts.get(JobStatus.PROCESSING, 0),
            completed=completed,
            failed=failed,
            retrying=counts.get(JobStatus.RETRYING, 0),
            dead_letter=dead_letter,
            avg_processing_duration=float(avg_processing) if avg_processing else None,
            avg_queue_wait_time=float(avg_wait) if avg_wait else None,
            success_rate=round(success_rate, 2),
            failure_rate=round(failure_rate, 2),
        )

    async def get_job_metrics_by_type(self) -> List[JobMetrics]:
        """Get metrics grouped by job type"""
        result = await self.db.execute(
            select(
                Job.job_type,
                func.count(Job.id).label("total"),
                func.sum(func.cast(Job.status == JobStatus.COMPLETED, Integer)).label("completed"),
                func.sum(func.cast(Job.status == JobStatus.FAILED, Integer)).label("failed"),
                func.avg(Job.processing_duration).label("avg_duration"),
            ).group_by(Job.job_type)
        )

        metrics = []
        for row in result.all():
            total = row.total or 0
            completed = row.completed or 0
            failed = row.failed or 0

            success_rate = (completed / total * 100) if total > 0 else 0

            metrics.append(
                JobMetrics(
                    job_type=row.job_type,
                    total=total,
                    completed=completed,
                    failed=failed,
                    avg_duration=float(row.avg_duration) if row.avg_duration else None,
                    success_rate=round(success_rate, 2),
                )
            )

        return metrics

    async def _get_job_by_idempotency_key(self, idempotency_key: str) -> Optional[Job]:
        """Get job by idempotency key"""
        result = await self.db.execute(select(Job).where(Job.idempotency_key == idempotency_key))
        return result.scalar_one_or_none()

    async def create_job_execution(
        self,
        job_id: UUID,
        worker_id: str,
        status: JobStatus,
        attempt_number: int,
        result: Optional[dict] = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
        duration: Optional[float] = None,
    ) -> JobExecution:
        """Create a job execution record"""
        execution = JobExecution(
            job_id=job_id,
            worker_id=worker_id,
            status=status,
            attempt_number=attempt_number,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow() if duration else None,
            duration=duration,
            result=result,
            error_message=error_message,
            error_type=error_type,
        )

        self.db.add(execution)
        await self.db.flush()
        await self.db.refresh(execution)

        return execution
