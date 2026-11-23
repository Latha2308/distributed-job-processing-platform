"""Job service for business logic"""

import time
from datetime import datetime, timedelta
from typing import Any, List, Optional
from uuid import UUID

from asyncpg.exceptions import UniqueViolationError as AsyncpgUniqueViolationError
from sqlalchemy import select, func, and_, Integer, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging_config import setup_logging
from app.core.metrics import (
    job_submissions,
    job_completions,
    job_processing_duration,
    job_retries,
    job_failures,
)
from app.core.redis_client import RedisClient
from app.db.models import Job, JobExecution, JobStatus, JobOutbox
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

    def _is_idempotency_key_collision(self, e: IntegrityError, idempotency_key: str) -> bool:
        """
        Determine if IntegrityError is an idempotency key collision.

        Traverses the exception chain to find the asyncpg UniqueViolationError
        and validates that it's specifically for the ix_jobs_idempotency_key constraint.

        Uses bounded queue-based traversal with cycle detection to handle
        SQLAlchemy's exception wrapping: IntegrityError -> dialect wrapper -> asyncpg exception.

        Args:
            e: The IntegrityError to classify
            idempotency_key: The idempotency key for structured logging

        Returns:
            True if this is an idempotency key collision, False otherwise
        """
        # Traverse the exception chain to find the asyncpg UniqueViolationError
        # SQLAlchemy wraps exceptions: IntegrityError -> dialect wrapper -> asyncpg exception
        # Use bounded traversal with cycle detection to find the actual asyncpg exception
        asyncpg_exception = None
        visited: set[int] = set()
        to_visit = []

        # Start from e.orig if it exists
        if hasattr(e, "orig") and e.orig is not None:
            to_visit.append(e.orig)

        # Bounded traversal: maximum 5 exception objects
        max_depth = 5
        while to_visit and len(visited) < max_depth:
            current = to_visit.pop(0)

            # Avoid cycles
            exc_id = id(current)
            if exc_id in visited:
                continue
            visited.add(exc_id)

            # Check if this is the asyncpg UniqueViolationError
            if isinstance(current, AsyncpgUniqueViolationError):
                asyncpg_exception = current
                break

            # Follow __cause__ chain
            if hasattr(current, "__cause__") and current.__cause__ is not None:
                to_visit.append(current.__cause__)

            # Follow __context__ chain
            if hasattr(current, "__context__") and current.__context__ is not None:
                to_visit.append(current.__context__)

        # If we didn't find asyncpg UniqueViolationError, this is a different error
        if asyncpg_exception is None:
            logger.error(
                "integrity_error_without_unique_violation",
                error=str(e),
                orig_type=type(e.orig).__name__ if hasattr(e, "orig") and e.orig else None,
                visited_count=len(visited),
            )
            return False

        # Verify this is a PostgreSQL unique constraint violation (SQLSTATE 23505)
        sqlstate = getattr(asyncpg_exception, "sqlstate", None)
        if sqlstate != "23505":
            logger.error(
                "unique_violation_with_unexpected_sqlstate",
                sqlstate=sqlstate,
                expected="23505",
            )
            return False

        # Verify this is specifically the idempotency_key constraint
        constraint_name = getattr(asyncpg_exception, "constraint_name", None)
        if constraint_name != "ix_jobs_idempotency_key":
            logger.error(
                "unique_violation_for_different_constraint",
                constraint=constraint_name,
                expected="ix_jobs_idempotency_key",
                idempotency_key=idempotency_key,
            )
            return False

        # Confirmed: This is a concurrent idempotency key collision
        logger.info(
            "concurrent_idempotency_collision_detected",
            idempotency_key=idempotency_key,
            constraint=constraint_name,
            sqlstate=sqlstate,
        )

        return True

    async def create_job(self, job_data: JobCreate) -> Job:
        """
        Create a new job with transactional outbox pattern.

        Creates both Job and JobOutbox records in the same transaction.
        The outbox dispatcher will asynchronously publish to Redis.

        Handles concurrent requests with duplicate idempotency keys by using
        SQLAlchemy savepoints (nested transactions) to isolate insertion attempts.
        When a concurrent duplicate is detected (UniqueViolationError), the savepoint
        is rolled back and the existing job is returned.

        Transaction is managed by the caller (get_db dependency).

        Args:
            job_data: Job creation data

        Returns:
            Created or existing job

        Raises:
            IntegrityError: If a non-idempotency constraint is violated
        """
        # Fast path: Check idempotency - return existing job WITHOUT creating new outbox event
        # This handles sequential duplicates efficiently without entering a savepoint
        if job_data.idempotency_key:
            existing = await self._get_job_by_idempotency_key(job_data.idempotency_key)
            if existing:
                logger.info(
                    "job_already_exists_sequential",
                    idempotency_key=job_data.idempotency_key,
                    job_id=str(existing.id),
                )
                return existing

        # Use a savepoint (nested transaction) to isolate the insertion attempt
        # The try/except is OUTSIDE to allow the savepoint to fully roll back
        # before we query for the existing job
        try:
            async with self.db.begin_nested():  # Creates PostgreSQL SAVEPOINT
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

                # Create outbox event in same nested transaction
                outbox_event = JobOutbox(
                    job_id=job.id,
                    event_type="job_created",
                    published=False,
                    attempts=0,
                )

                self.db.add(outbox_event)
                await self.db.flush()
                await self.db.refresh(outbox_event)

                # Update metrics
                job_submissions.labels(job_type=job.job_type, priority=str(job.priority)).inc()

                logger.info(
                    "job_created_with_outbox",
                    job_id=str(job.id),
                    outbox_event_id=str(outbox_event.id),
                    job_type=job.job_type,
                    priority=job.priority,
                )

                return job
                # SAVEPOINT commits here if no exception raised

        except IntegrityError as e:
            # Savepoint has been FULLY rolled back by the context manager exit
            # Both job and outbox records are discarded
            # Outer transaction remains valid for querying

            # Only handle idempotency key collisions when an idempotency key was provided
            # If no idempotency key, this is a different constraint violation
            if job_data.idempotency_key is None:
                raise

            # Classify the exception using extracted helper method
            if not self._is_idempotency_key_collision(e, job_data.idempotency_key):
                # Not an idempotency collision - re-raise the original exception
                raise

            # Query for the existing job that won the race
            # This is safe because the savepoint has fully rolled back
            existing = await self._get_job_by_idempotency_key(job_data.idempotency_key)

            if not existing:
                # Edge case: constraint violated but no existing job found
                # This could happen if the winning job was deleted immediately
                logger.error(
                    "concurrent_collision_but_no_existing_job",
                    idempotency_key=job_data.idempotency_key,
                    constraint="ix_jobs_idempotency_key",
                )
                raise

            logger.info(
                "concurrent_duplicate_resolved",
                idempotency_key=job_data.idempotency_key,
                existing_job_id=str(existing.id),
            )

            return existing

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
        retry_attempt: Optional[int] = None,
    ) -> Optional[Job]:
        """
        Atomically claim a job for processing using conditional UPDATE.

        Uses a single database UPDATE with RETURNING to ensure only one worker
        can claim a job. The UPDATE only succeeds if the job is in an eligible state.

        Eligible states:
        - QUEUED: Jobs ready for first execution (retry_attempt must be None)
        - RETRYING: Jobs scheduled for retry where:
          * retry_attempt must be provided and valid
          * retry_attempt matches job.retry_count (prevents duplicate execution)
          * next_retry_at <= now

        Ineligible states:
        - PROCESSING: Already being worked on
        - COMPLETED: Already finished
        - FAILED: Requires manual intervention or retry scheduling
        - DEAD_LETTER: Permanently failed

        Retry Attempt Validation:
        - RETRYING jobs REQUIRE valid retry_attempt (prevents bypass)
        - retry_attempt must be a positive integer matching job.retry_count
        - QUEUED jobs MUST NOT have retry_attempt (initial execution only)
        - Malformed values (bool, str, float, negative, zero) are rejected

        Args:
            job_id: ID of job to claim
            worker_id: ID of worker attempting to claim
            retry_attempt: Expected retry attempt number (required for RETRYING jobs)

        Returns:
            Job object if successfully claimed, None if claim failed

        Raises:
            None - returns None on failure rather than raising exceptions
        """
        # Validate retry_attempt type and value before building SQL
        if retry_attempt is not None:
            # Reject malformed values - do not silently convert to None
            if not isinstance(retry_attempt, int) or isinstance(retry_attempt, bool):
                logger.warning(
                    "job_claim_failed_invalid_retry_attempt_type",
                    job_id=str(job_id),
                    worker_id=worker_id,
                    retry_attempt=retry_attempt,
                    retry_attempt_type=type(retry_attempt).__name__,
                )
                return None

            # Reject invalid integer values
            if retry_attempt <= 0:
                logger.warning(
                    "job_claim_failed_invalid_retry_attempt_value",
                    job_id=str(job_id),
                    worker_id=worker_id,
                    retry_attempt=retry_attempt,
                )
                return None

        now = datetime.utcnow()

        # Build conditions based on whether this is a retry or initial attempt
        conditions = [Job.id == job_id]

        if retry_attempt is not None:
            # Claiming a recovered retry attempt
            # RETRYING jobs require explicit retry_attempt validation
            conditions.append(
                and_(
                    Job.status == JobStatus.RETRYING,
                    Job.retry_count == retry_attempt,  # Must match exactly
                    Job.next_retry_at.isnot(None),
                    Job.next_retry_at <= now,
                )
            )
        else:
            # Claiming initial job - ONLY QUEUED status allowed
            # RETRYING jobs MUST provide retry_attempt (no bypass)
            conditions.append(Job.status == JobStatus.QUEUED)

        # Build atomic UPDATE statement with conditions
        stmt = (
            update(Job)
            .where(and_(*conditions))
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
                retry_attempt=retry_attempt,
            )

            return claimed_job
        else:
            # Claim failed - job not in eligible state or already claimed
            logger.debug(
                "job_claim_failed",
                job_id=str(job_id),
                worker_id=worker_id,
                retry_attempt=retry_attempt,
                reason="Job not in eligible state, already claimed, or stale retry_attempt",
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

    def _validate_expected_retry_count(
        self, job_id: UUID, expected_retry_count: Optional[int]
    ) -> bool:
        """
        Validate expected_retry_count parameter for execution-generation fencing.

        Args:
            job_id: Job ID for logging
            expected_retry_count: Expected retry count value to validate

        Returns:
            True if valid, False if invalid (logs warning)
        """
        if expected_retry_count is None:
            return True

        if not isinstance(expected_retry_count, int) or isinstance(expected_retry_count, bool):
            logger.warning(
                "invalid_expected_retry_count_type",
                job_id=str(job_id),
                expected_retry_count=expected_retry_count,
                expected_retry_count_type=type(expected_retry_count).__name__,
            )
            return False

        if expected_retry_count < 0:
            logger.warning(
                "invalid_expected_retry_count_value",
                job_id=str(job_id),
                expected_retry_count=expected_retry_count,
            )
            return False

        return True

    def _should_use_fenced_update(
        self,
        expected_status: Optional[JobStatus],
        expected_worker_id: Optional[str],
        expected_retry_count: Optional[int],
    ) -> bool:
        """Check if fenced atomic update should be used."""
        return (
            expected_status is not None
            or expected_worker_id is not None
            or expected_retry_count is not None
        )

    async def _update_job_unfenced(
        self, job_id: UUID, update_data: JobUpdate, worker_id: Optional[str]
    ) -> Optional[Job]:
        """
        Update job using read-then-write pattern (backward compatible).

        Used when no fencing parameters are provided.
        """
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

    async def update_job_status(
        self,
        job_id: UUID,
        update_data: JobUpdate,
        worker_id: Optional[str] = None,
        expected_status: Optional[JobStatus] = None,
        expected_worker_id: Optional[str] = None,
        expected_retry_count: Optional[int] = None,
    ) -> Optional[Job]:
        """
        Update job status with optional atomic fencing.

        When fencing parameters are provided (expected_status, expected_worker_id,
        or expected_retry_count), uses atomic conditional UPDATE to prevent stale
        workers from overwriting job state that has changed (e.g., by recovery process).

        When fencing parameters are omitted, uses existing read-then-write pattern
        for backward compatibility.

        Args:
            job_id: Job ID to update
            update_data: Status update data
            worker_id: Worker performing update (for logging)
            expected_status: If provided, only update if job has this status (atomic fencing)
            expected_worker_id: If provided, only update if job owned by this worker (atomic fencing)
            expected_retry_count: If provided, only update if job is at this execution generation (atomic fencing)

        Returns:
            Updated job or None if job not found or fencing check failed
        """
        # Validate expected_retry_count if provided
        if not self._validate_expected_retry_count(job_id, expected_retry_count):
            return None

        # FENCED PATH: Use atomic conditional UPDATE
        if self._should_use_fenced_update(
            expected_status, expected_worker_id, expected_retry_count
        ):
            return await self._update_job_status_atomic(
                job_id,
                update_data,
                worker_id,
                expected_status,
                expected_worker_id,
                expected_retry_count,
            )

        # UNFENCED PATH: Existing read-then-write pattern (backward compatible)
        return await self._update_job_unfenced(job_id, update_data, worker_id)

    async def _update_job_status_atomic(
        self,
        job_id: UUID,
        update_data: JobUpdate,
        worker_id: Optional[str],
        expected_status: Optional[JobStatus],
        expected_worker_id: Optional[str],
        expected_retry_count: Optional[int],
    ) -> Optional[Job]:
        """
        Atomically update job status with fencing conditions.

        Uses single conditional UPDATE statement similar to claim_job() pattern.
        Returns None if fencing conditions not met (job state changed or worker lost ownership).

        This prevents stale workers from overwriting job state that has changed,
        for example by recovery process marking job FAILED after worker timeout,
        or when worker ID is reused and execution generation has advanced.

        Args:
            job_id: Job ID to update
            update_data: Status update data
            worker_id: Worker performing update (for logging)
            expected_status: Required current status (None = don't check)
            expected_worker_id: Required current worker_id (None = don't check)
            expected_retry_count: Required current retry_count/generation (None = don't check)

        Returns:
            Updated job or None if conditions not met
        """
        now = datetime.utcnow()

        # Build WHERE conditions for atomic UPDATE
        conditions = self._build_fencing_conditions(
            job_id, expected_status, expected_worker_id, expected_retry_count
        )

        # Build VALUES for UPDATE
        values = self._build_update_values(now, update_data, worker_id, expected_worker_id)

        # Execute atomic conditional UPDATE
        stmt = update(Job).where(and_(*conditions)).values(**values).returning(Job)

        result = await self.db.execute(stmt)
        updated_job = result.scalar_one_or_none()

        if not updated_job:
            # Fencing conditions not met - job state changed or worker lost ownership
            logger.warning(
                "job_update_fencing_failed",
                job_id=str(job_id),
                expected_status=expected_status.value if expected_status else None,
                expected_worker_id=expected_worker_id,
                expected_retry_count=expected_retry_count,
                actual_worker_id=worker_id,
                reason="Job status changed, worker ownership lost, or execution generation advanced",
            )
            return None

        # Refresh to get all attributes
        await self.db.refresh(updated_job)

        # Post-update: Calculate processing_duration and emit metrics
        await self._handle_post_update_metrics(now, update_data, updated_job)

        logger.info(
            "job_updated_atomic",
            job_id=str(job_id),
            status=updated_job.status.value,
            worker_id=worker_id,
            fenced=True,
            expected_retry_count=expected_retry_count,
        )

        return updated_job

    def _build_fencing_conditions(
        self,
        job_id: UUID,
        expected_status: Optional[JobStatus],
        expected_worker_id: Optional[str],
        expected_retry_count: Optional[int],
    ) -> list:
        """Build WHERE conditions for atomic fenced UPDATE."""
        conditions = [Job.id == job_id]

        if expected_status is not None:
            conditions.append(Job.status == expected_status)

        if expected_worker_id is not None:
            conditions.append(Job.worker_id == expected_worker_id)

        if expected_retry_count is not None:
            conditions.append(Job.retry_count == expected_retry_count)

        return conditions

    def _build_update_values(
        self,
        now: datetime,
        update_data: JobUpdate,
        worker_id: Optional[str],
        expected_worker_id: Optional[str],
    ) -> dict[str, Any]:
        """Build VALUES dict for atomic UPDATE statement."""
        values: dict[str, Any] = {"updated_at": now}

        if update_data.status:
            values["status"] = update_data.status
            self._add_status_specific_values(
                values, update_data.status, now, worker_id, expected_worker_id
            )

        if update_data.result is not None:
            values["result"] = update_data.result

        if update_data.error_message:
            values["error_message"] = update_data.error_message

        if update_data.error_type:
            values["error_type"] = update_data.error_type

        return values

    def _add_status_specific_values(
        self,
        values: dict[str, Any],
        status: JobStatus,
        now: datetime,
        worker_id: Optional[str],
        expected_worker_id: Optional[str],
    ) -> None:
        """Add status-specific fields to UPDATE values dict."""
        if status == JobStatus.COMPLETED:
            values["completed_at"] = now
            # Note: processing_duration calculated after UPDATE using started_at
        elif status == JobStatus.FAILED:
            values["completed_at"] = now
        elif status == JobStatus.PROCESSING:
            values["started_at"] = now
            if expected_worker_id is None and worker_id is not None:
                # Only set worker_id if not already being used for fencing
                values["worker_id"] = worker_id

    async def _handle_post_update_metrics(
        self,
        now: datetime,
        update_data: JobUpdate,
        updated_job: Job,
    ) -> None:
        """Calculate durations and emit metrics after successful fenced update."""
        if update_data.status == JobStatus.COMPLETED:
            await self._handle_completed_metrics(now, updated_job)
        elif update_data.status == JobStatus.FAILED:
            self._handle_failed_metrics(update_data, updated_job)
        elif update_data.status == JobStatus.PROCESSING:
            await self._handle_processing_metrics(now, updated_job)

    async def _handle_completed_metrics(self, now: datetime, job: Job) -> None:
        """Handle processing duration and completion metrics."""
        if job.started_at:
            duration = (now - job.started_at).total_seconds()
            job.processing_duration = duration
            await self.db.flush()

            # Update metrics
            job_processing_duration.labels(job_type=job.job_type).observe(duration)

        job_completions.labels(job_type=job.job_type, status="completed").inc()

    def _handle_failed_metrics(self, update_data: JobUpdate, job: Job) -> None:
        """Handle failure metrics."""
        job_completions.labels(job_type=job.job_type, status="failed").inc()

        if update_data.error_type:
            job_failures.labels(
                job_type=job.job_type,
                error_type=update_data.error_type,
            ).inc()

    async def _handle_processing_metrics(self, now: datetime, job: Job) -> None:
        """Handle queue wait time calculation."""
        if job.created_at:
            wait_time = (now - job.created_at).total_seconds()
            job.queue_wait_time = wait_time
            await self.db.flush()

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
                    "retry_attempt": next_retry_count,
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
