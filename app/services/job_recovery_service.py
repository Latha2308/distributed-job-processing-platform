"""Service for automatic stuck-job recovery

Provides two recovery mechanisms:

1. PROCESSING-Job Recovery (existing):
   - Detects jobs stuck in PROCESSING status due to worker crashes
   - Transitions to RETRYING with incremented retry_count
   - Creates job_recovered outbox event for retry queue

2. QUEUED-Job Reconciliation (Step 289B):
   - Detects jobs stuck in QUEUED status due to message loss
   - Job status remains QUEUED (unchanged)
   - Creates job_queued_recovery outbox event for main queue
   - Throttles recovery attempts to prevent unbounded republication

Architecture:
1. Detect stuck jobs: SQL query with timeout/age threshold + safety buffer
2. Atomic claim: SELECT ... FOR UPDATE SKIP LOCKED
3. Verify eligibility under lock (status, age, throttle)
4. Check for existing unpublished recovery events (deduplication)
5. Create outbox event (if no existing event) + update throttling timestamp
6. FLUSH (persist within caller's transaction)
7. Caller COMMIT (durable recovery intent in PostgreSQL)
8. Outbox dispatcher publishes to Redis (separate transaction)

Transaction Ownership:
- Single recovery (recover_stuck_job, reconcile_queued_job): Caller owns transaction
  - Service uses flush() to persist within caller's transaction
  - Caller must commit to make changes durable
  - Service never commits or rolls back caller's session

- Batch recovery (recover_stuck_jobs_batch, reconcile_queued_jobs_batch): Requires session_factory
  - Creates independent session per job for per-job isolation
  - Each job commits/rolls back its own transaction
  - Failed jobs do not affect other jobs
  - Detection query does not hold locks during processing

PROCESSING recovery transitions jobs to RETRYING and persists retry parameters.
QUEUED reconciliation creates recovery events without changing job status.
The outbox dispatcher handles Redis publishing in separate transactions.
"""

from datetime import datetime, timedelta
from typing import Callable, List, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging_config import setup_logging
from app.db.models import Job, JobOutbox, JobStatus, WorkerHeartbeat
from app.services.worker_service import WorkerService

logger = setup_logging()


class JobRecoveryService:
    """Service for automatic stuck-job recovery and QUEUED-job reconciliation"""

    # Safety buffer to avoid premature recovery (grace period beyond timeout)
    SAFETY_BUFFER = timedelta(seconds=10)

    # Default batch size for recovery operations
    DEFAULT_BATCH_SIZE = 10

    # QUEUED-job reconciliation thresholds (Step 289B)
    # Initial recovery threshold - jobs younger than this are not eligible for first recovery
    QUEUED_AGE_THRESHOLD = timedelta(minutes=10)  # 10 minutes

    # Subsequent recovery interval - minimum time between recovery attempts
    QUEUED_RECOVERY_INTERVAL = timedelta(hours=1)  # 1 hour (max 24 attempts/day)

    def __init__(
        self,
        db: AsyncSession,
        session_factory: Optional[Callable[[], AsyncSession]] = None,
        queued_age_threshold_minutes: Optional[int] = None,
        queued_throttle_hours: Optional[int] = None,
    ):
        """Initialize recovery service

        Args:
            db: Database session for single recovery operations (recover_stuck_job)
            session_factory: Optional session factory for batch operations.
                Required for recover_stuck_jobs_batch() to provide per-job isolation.
                Not used by recover_stuck_job() which uses the provided db session.
            queued_age_threshold_minutes: Override for QUEUED_AGE_THRESHOLD (default: 10 minutes)
            queued_throttle_hours: Override for QUEUED_RECOVERY_INTERVAL (default: 1 hour)
        """
        self.db = db
        self.session_factory = session_factory
        self.worker_service = WorkerService(db)

        # Allow configuration override for QUEUED reconciliation thresholds
        if queued_age_threshold_minutes is not None:
            self.QUEUED_AGE_THRESHOLD = timedelta(minutes=queued_age_threshold_minutes)
        if queued_throttle_hours is not None:
            self.QUEUED_RECOVERY_INTERVAL = timedelta(hours=queued_throttle_hours)

    async def detect_stuck_jobs(
        self, batch_size: int = DEFAULT_BATCH_SIZE, min_timeout: Optional[int] = None
    ) -> List[UUID]:
        """Detect jobs stuck in PROCESSING status beyond their timeout

        A job is considered stuck if:
        1. Status is PROCESSING
        2. Time elapsed since started_at > (job.timeout + safety_buffer)
        3. Has a worker_id assigned

        Note: This uses a simple approach where we check the default timeout
        as a minimum threshold. Jobs with custom timeouts that are longer than
        the default will be checked in subsequent batches when they exceed
        their individual timeout + safety buffer.

        Args:
            batch_size: Maximum number of stuck jobs to return
            min_timeout: Minimum timeout threshold (defaults to JOB_TIMEOUT)

        Returns:
            List of stuck job IDs
        """
        if min_timeout is None:
            min_timeout = settings.JOB_TIMEOUT

        safety_buffer_seconds = int(self.SAFETY_BUFFER.total_seconds())

        # Get candidate stuck jobs (PROCESSING with started_at and worker_id)
        # Then filter by individual timeout in Python
        stmt = (
            select(Job)
            .where(Job.status == JobStatus.PROCESSING)
            .where(Job.started_at.isnot(None))
            .where(Job.worker_id.isnot(None))
            .order_by(Job.started_at.asc())
            .limit(batch_size * 2)  # Fetch extra to account for filtering
        )

        result = await self.db.execute(stmt)
        candidate_jobs = result.scalars().all()

        stuck_job_ids: List[UUID] = []
        now = datetime.utcnow()

        for job in candidate_jobs:
            if len(stuck_job_ids) >= batch_size:
                break

            # Skip if started_at is None (should not happen due to query filter)
            if job.started_at is None:
                continue

            # Calculate timeout threshold for this specific job
            job_timeout = job.timeout if job.timeout is not None else min_timeout
            timeout_threshold = timedelta(seconds=(job_timeout + safety_buffer_seconds))

            # Check if job exceeded its timeout + safety buffer
            time_elapsed = now - job.started_at
            if time_elapsed > timeout_threshold:
                stuck_job_ids.append(job.id)

        if stuck_job_ids:
            logger.warning(
                "detected_stuck_jobs",
                count=len(stuck_job_ids),
                job_ids=[str(jid) for jid in stuck_job_ids],
            )

        return stuck_job_ids

    async def recover_stuck_job(self, job_id: UUID) -> bool:
        """Recover a single stuck job with atomic claiming

        Uses SELECT ... FOR UPDATE SKIP LOCKED to atomically claim the job,
        then transitions to RETRYING with calculated retry parameters.

        Transaction ownership:
        - Caller owns the transaction and controls commit/rollback
        - This method uses flush() to persist changes within the transaction
        - Caller must commit after successful recovery to make changes durable
        - Caller should rollback on exceptions to undo partial changes

        Transaction phases:
        1. Atomic claim with FOR UPDATE SKIP LOCKED
        2. Verify worker is dead (heartbeat check)
        3. Check max_retries limit
        4. Transition to RETRYING + increment retry_count + calculate next_retry_at
        5. Create job_recovered outbox event
        6. FLUSH (persist within caller's transaction)

        Retry intent becomes durable only when caller commits. The job state
        (RETRYING, retry_count, next_retry_at) and outbox event are atomic
        within the transaction boundary.

        Args:
            job_id: Job ID to recover

        Returns:
            True if recovered successfully, False if skipped (already claimed, max retries, etc)

        Raises:
            Exception: Database errors propagate to caller for transaction rollback
        """
        try:
            # Phase 1: Atomic claim with row-level lock
            stmt = (
                select(Job)
                .where(Job.id == job_id)
                .where(Job.status == JobStatus.PROCESSING)
                .with_for_update(skip_locked=True)
            )
            result = await self.db.execute(stmt)
            job = result.scalar_one_or_none()

            if not job:
                # Job already claimed by another recovery worker or status changed
                logger.info("recovery_skipped_not_found_or_claimed", job_id=str(job_id))
                return False

            # Phase 2: Verify timeout eligibility using the locked row
            # CRITICAL: Cannot assume detection results remain valid after lock acquisition.
            # Between detection and recovery, the job could have been:
            # - Refreshed with new started_at (worker reclaimed)
            # - Updated with longer timeout (admin intervention)
            # - Still processing normally (false positive)
            #
            # Must independently verify timeout using the LOCKED row's current state.
            if not self._is_job_timeout_exceeded(job):
                logger.info(
                    "recovery_skipped_not_timed_out",
                    job_id=str(job_id),
                    started_at=job.started_at.isoformat() if job.started_at else None,
                    timeout=job.timeout,
                    worker_id=job.worker_id,
                )
                return False

            # Phase 3: Check max_retries limit
            current_retry_count = job.retry_count or 0
            max_retries = job.max_retries or settings.MAX_RETRIES

            if current_retry_count >= max_retries:
                # Exceeded max retries - move to dead letter queue
                logger.warning(
                    "recovery_exceeded_max_retries",
                    job_id=str(job_id),
                    retry_count=current_retry_count,
                    max_retries=max_retries,
                )

                # Mark as DEAD_LETTER without creating outbox event
                job.status = JobStatus.DEAD_LETTER
                job.is_dead_letter = True
                job.moved_to_dlq_at = datetime.utcnow()
                job.error_type = "StuckJobRecoveryMaxRetriesExceeded"
                job.error_message = (
                    f"Job stuck in PROCESSING status beyond timeout. "
                    f"Max retries ({max_retries}) exceeded."
                )
                job.completed_at = datetime.utcnow()
                job.updated_at = datetime.utcnow()

                await self.db.flush()

                logger.info(
                    "recovery_moved_to_dlq", job_id=str(job_id), retry_count=current_retry_count
                )
                return True

            # Phase 4: Transition to RETRYING and create outbox event (atomic)
            # Calculate retry parameters ONCE for durability
            next_retry_count = current_retry_count + 1
            delay = settings.INITIAL_RETRY_DELAY * (settings.RETRY_BACKOFF_BASE**next_retry_count)
            next_retry_at = datetime.utcnow() + timedelta(seconds=delay)

            # Transition directly to RETRYING (durable retry intent)
            job.status = JobStatus.RETRYING
            job.retry_count = next_retry_count
            job.next_retry_at = next_retry_at
            job.error_type = "StuckJobRecovery"
            job.error_message = (
                f"Job stuck in PROCESSING status beyond timeout. "
                f"Worker {job.worker_id or 'unknown'} failed to complete job. "
                f"Automatic recovery initiated. Retry attempt {next_retry_count}."
            )
            job.completed_at = datetime.utcnow()
            job.updated_at = datetime.utcnow()

            # Create outbox event for Redis publication
            # Store immutable retry_attempt identifier for this specific recovery
            outbox_event = JobOutbox(
                job_id=job_id,
                event_type="job_recovered",
                retry_attempt=next_retry_count,  # Immutable: identifies this specific retry attempt
                published=False,
                attempts=0,
            )
            self.db.add(outbox_event)

            # FLUSH - persist changes within transaction
            # Caller controls commit/rollback boundary
            await self.db.flush()

            logger.info(
                "recovery_completed",
                job_id=str(job_id),
                worker_id=job.worker_id,
                retry_count=next_retry_count,
                next_retry_at=next_retry_at.isoformat(),
                delay_seconds=delay,
                started_at=job.started_at.isoformat() if job.started_at else None,
                error_type=job.error_type,
            )

            return True

        except Exception as e:
            # Do not commit or rollback - let caller handle transaction boundary
            logger.error(
                "recovery_failed", job_id=str(job_id), error=str(e), error_type=type(e).__name__
            )
            raise

    def _is_job_timeout_exceeded(self, job: Job) -> bool:
        """Check if job has exceeded its timeout plus safety buffer.

        This is the definitive timeout eligibility check using the locked job row.
        Must be called AFTER acquiring the row lock to ensure we're checking
        the current state, not stale detection results.

        Args:
            job: Locked job instance with current database values

        Returns:
            True if job timeout exceeded, False otherwise
        """
        # Verify required fields
        if not job.started_at:
            logger.debug(
                "job_missing_started_at",
                job_id=str(job.id),
                status=job.status,
            )
            return False

        # Calculate timeout threshold
        job_timeout = job.timeout if job.timeout is not None else settings.JOB_TIMEOUT
        safety_buffer_seconds = int(self.SAFETY_BUFFER.total_seconds())
        timeout_threshold_seconds = job_timeout + safety_buffer_seconds

        # Check if elapsed time exceeds threshold
        now = datetime.utcnow()
        elapsed_time = now - job.started_at
        elapsed_seconds = elapsed_time.total_seconds()

        is_timed_out = elapsed_seconds > timeout_threshold_seconds

        if not is_timed_out:
            logger.debug(
                "job_not_timed_out",
                job_id=str(job.id),
                elapsed_seconds=int(elapsed_seconds),
                timeout_threshold=timeout_threshold_seconds,
                remaining_seconds=int(timeout_threshold_seconds - elapsed_seconds),
            )

        return is_timed_out

    async def _is_worker_alive(self, worker_id: str) -> bool:
        """Check if worker is still alive based on heartbeat

        Args:
            worker_id: Worker ID to check

        Returns:
            True if worker heartbeat is fresh, False if stale or missing
        """
        stmt = select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker_id)
        result = await self.db.execute(stmt)
        heartbeat = result.scalar_one_or_none()

        if not heartbeat:
            logger.debug("worker_heartbeat_missing", worker_id=worker_id)
            return False

        # Check if heartbeat is within timeout threshold
        time_since_heartbeat = datetime.utcnow() - heartbeat.last_heartbeat
        is_alive = time_since_heartbeat < WorkerService.HEARTBEAT_TIMEOUT

        logger.debug(
            "worker_heartbeat_check",
            worker_id=worker_id,
            last_heartbeat=heartbeat.last_heartbeat.isoformat(),
            seconds_ago=int(time_since_heartbeat.total_seconds()),
            is_alive=is_alive,
        )

        return is_alive

    async def _recover_job_in_transaction(self, job_id: UUID) -> bool:
        """Recover a single job in its own dedicated transaction

        Helper for batch recovery. Creates a new session, recovers the job,
        and commits or rolls back the transaction.

        Args:
            job_id: Job ID to recover

        Returns:
            True if recovered successfully, False otherwise
        """
        if not self.session_factory:
            raise RuntimeError(
                "_recover_job_in_transaction requires session_factory. "
                "Use recover_stuck_job() for caller-owned transactions."
            )

        async with self.session_factory() as job_session:
            try:
                recovery_service = JobRecoveryService(job_session)
                success = await recovery_service.recover_stuck_job(job_id)
                if success:
                    await job_session.commit()
                    return True
                return False
            except Exception as e:
                await job_session.rollback()
                logger.error(
                    "recovery_job_failed",
                    job_id=str(job_id),
                    error=str(e),
                    error_type=type(e).__name__,
                )
                return False

    async def recover_stuck_jobs_batch(
        self, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> tuple[int, int]:
        """Detect and recover a batch of stuck jobs

        This is the main entry point for periodic recovery tasks.
        Requires session_factory to provide per-job transaction isolation.

        Each job recovery:
        - Gets its own dedicated session/transaction
        - Acquires SELECT FOR UPDATE SKIP LOCKED
        - Commits or rolls back independently
        - Does not affect other jobs in the batch

        The detection query uses self.db for read-only access and does not
        hold locks during recovery processing.

        Args:
            batch_size: Maximum number of jobs to recover in this batch

        Returns:
            Tuple of (detected_count, recovered_count)

        Raises:
            RuntimeError: If session_factory not provided
        """
        if not self.session_factory:
            raise RuntimeError(
                "recover_stuck_jobs_batch requires session_factory for per-job isolation. "
                "Provide session_factory in constructor or use recover_stuck_job() directly."
            )

        # Detection uses self.db for read-only query (no locks held)
        stuck_job_ids = await self.detect_stuck_jobs(batch_size=batch_size)

        if not stuck_job_ids:
            return (0, 0)

        # Process each job in its own transaction
        recovered_count = 0
        for job_id in stuck_job_ids:
            success = await self._recover_job_in_transaction(job_id)
            if success:
                recovered_count += 1

        logger.info(
            "recovery_batch_completed",
            detected=len(stuck_job_ids),
            recovered=recovered_count,
            failed=len(stuck_job_ids) - recovered_count,
        )

        return (len(stuck_job_ids), recovered_count)

    # -------------------------------------------------------------------------
    # QUEUED-Job Reconciliation (Step 289B)
    # -------------------------------------------------------------------------

    async def detect_queued_jobs(self, batch_size: int = DEFAULT_BATCH_SIZE) -> List[UUID]:
        """Detect jobs stuck in QUEUED status beyond initial recovery threshold

        A job is eligible for reconciliation if:
        1. Status is QUEUED
        2. created_at > QUEUED_AGE_THRESHOLD (10 minutes old)
        3. last_recovery_attempt_at is NULL (never recovered) OR
           last_recovery_attempt_at < NOW() - QUEUED_RECOVERY_INTERVAL (throttle expired)

        This identifies jobs that may never have been delivered to Redis or whose
        Redis message was lost due to crash, eviction, or race condition.

        Args:
            batch_size: Maximum number of QUEUED jobs to return

        Returns:
            List of QUEUED job IDs eligible for reconciliation
        """
        now = datetime.utcnow()
        age_threshold = now - self.QUEUED_AGE_THRESHOLD
        recovery_threshold = now - self.QUEUED_RECOVERY_INTERVAL

        stmt = (
            select(Job.id)
            .where(Job.status == JobStatus.QUEUED)
            .where(Job.created_at < age_threshold)
            .where(
                (Job.last_recovery_attempt_at.is_(None))
                | (Job.last_recovery_attempt_at < recovery_threshold)
            )
            .order_by(Job.created_at.asc())
            .limit(batch_size)
        )

        result = await self.db.execute(stmt)
        queued_job_ids = [row[0] for row in result.all()]

        if queued_job_ids:
            logger.info(
                "detected_queued_jobs_for_reconciliation",
                count=len(queued_job_ids),
                job_ids=[str(jid) for jid in queued_job_ids],
            )

        return queued_job_ids

    async def reconcile_queued_job(self, job_id: UUID) -> bool:
        """Reconcile a single QUEUED job with atomic claiming and outbox deduplication

        Creates a durable recovery event (job_queued_recovery) in the outbox for jobs
        that may never have been delivered to Redis or whose message was lost.

        Transaction ownership:
        - Caller owns the transaction and controls commit/rollback
        - This method uses flush() to persist changes within the transaction
        - Caller must commit after successful reconciliation to make changes durable
        - Caller should rollback on exceptions to undo partial changes

        Transaction phases:
        1. Atomic claim with FOR UPDATE SKIP LOCKED
        2. Revalidate eligibility (status, age, throttle)
        3. Check for existing unpublished job_queued_recovery events (deduplication)
        4. If existing event found: update throttling timestamp only, skip event creation
        5. If no existing event: create job_queued_recovery outbox event
        6. Update last_recovery_attempt_at throttling timestamp
        7. FLUSH (persist within caller's transaction)

        Deduplication predicate (see docs/QUEUED_RECONCILIATION_DEDUPLICATION.md):
        - Checks for unpublished, valid job_queued_recovery events
        - Skips creation if found (avoids duplicate messages)
        - Published events permit bounded republication after throttle
        - reconciliation_required events do not block (invalid event, not invalid job)

        The job status remains QUEUED (unchanged). The job retry_count remains unchanged.
        This is NOT a retry - this is recovery from potential message loss.

        Args:
            job_id: Job ID to reconcile

        Returns:
            True if reconciliation completed (event created or skipped due to existing event)
            False if skipped (already claimed, not eligible, status changed, etc)

        Raises:
            Exception: Database errors propagate to caller for transaction rollback
        """
        try:
            # Phase 1: Atomic claim with row-level lock
            stmt = (
                select(Job)
                .where(Job.id == job_id)
                .where(Job.status == JobStatus.QUEUED)
                .with_for_update(skip_locked=True)
            )
            result = await self.db.execute(stmt)
            job = result.scalar_one_or_none()

            if not job:
                # Job already claimed by another reconciler, worker, or status changed
                logger.debug("reconciliation_skipped_not_found_or_claimed", job_id=str(job_id))
                return False

            # Phase 2: Revalidate eligibility using the locked row
            # CRITICAL: Between detection and lock acquisition, the job could have been:
            # - Claimed by a worker (status changed to PROCESSING)
            # - Reconciled by another reconciler (last_recovery_attempt_at updated)
            # - Updated with different created_at (unlikely but possible)
            #
            # Must independently verify eligibility using the LOCKED row's current state.
            if not self._is_queued_job_eligible(job):
                logger.debug(
                    "reconciliation_skipped_not_eligible",
                    job_id=str(job_id),
                    created_at=job.created_at.isoformat() if job.created_at else None,
                    last_recovery_attempt_at=(
                        job.last_recovery_attempt_at.isoformat()
                        if job.last_recovery_attempt_at
                        else None
                    ),
                )
                return False

            # Phase 3: Check for existing unpublished events (deduplication)
            # Predicate: event_type IN ('job_created', 'job_queued_recovery')
            #            AND published=false AND reconciliation_required=false
            #
            # CRITICAL: Must check BOTH job_created AND job_queued_recovery events.
            # Both represent outstanding delivery intent for a QUEUED job:
            # - job_created: Initial creation event (publishes to main queue)
            # - job_queued_recovery: Recovery event (publishes to main queue)
            #
            # This prevents duplicate events when:
            # - Unpublished job_created awaiting first publication
            # - Unpublished job_created in retry backoff after failed publication
            # - Unpublished job_queued_recovery awaiting first publication
            # - Unpublished job_queued_recovery in retry backoff after failed publication
            #
            # This DOES NOT prevent new events when:
            # - Event already published (published=true) - bounded republication allowed
            # - Event marked reconciliation_required (invalid event, not invalid job)
            #
            # Note: job_recovered events are NOT checked because:
            # - job_recovered has retry_attempt set (not NULL)
            # - job_recovered publishes to RETRY queue (delayed), not main queue
            # - job_recovered is for RETRYING jobs, not QUEUED jobs
            existing_event_stmt = (
                select(JobOutbox.id)
                .where(JobOutbox.job_id == job_id)
                .where(JobOutbox.event_type.in_(["job_created", "job_queued_recovery"]))
                .where(JobOutbox.published == False)  # noqa: E712
                .where(JobOutbox.reconciliation_required == False)  # noqa: E712
            )
            existing_event_result = await self.db.execute(existing_event_stmt)
            existing_event = existing_event_result.scalar_one_or_none()

            if existing_event:
                # Existing unpublished event found - skip event creation
                # Update throttling timestamp to prevent immediate re-reconciliation
                logger.info(
                    "reconciliation_skipped_existing_event",
                    job_id=str(job_id),
                    existing_event_id=str(existing_event),
                )

                job.last_recovery_attempt_at = datetime.utcnow()
                job.updated_at = datetime.utcnow()
                await self.db.flush()
                return True

            # Phase 4: Create job_queued_recovery outbox event
            # event_type: 'job_queued_recovery' (not 'job_created' or 'job_recovered')
            # retry_attempt: NULL (this is NOT a retry, it's recovery from potential message loss)
            # published: false (awaiting publication by outbox dispatcher)
            outbox_event = JobOutbox(
                job_id=job_id,
                event_type="job_queued_recovery",
                retry_attempt=None,  # NULL - not a retry
                published=False,
                attempts=0,
            )
            self.db.add(outbox_event)

            # Phase 5: Update throttling timestamp
            # Records this reconciliation attempt to enforce QUEUED_RECOVERY_INTERVAL
            # Prevents excessive republication (max 24 attempts/day)
            job.last_recovery_attempt_at = datetime.utcnow()
            job.updated_at = datetime.utcnow()

            # FLUSH - persist changes within transaction
            # Caller controls commit/rollback boundary
            await self.db.flush()

            logger.info(
                "reconciliation_completed",
                job_id=str(job_id),
                event_type="job_queued_recovery",
                created_at=job.created_at.isoformat() if job.created_at else None,
                last_recovery_attempt_at=(
                    job.last_recovery_attempt_at.isoformat()
                    if job.last_recovery_attempt_at
                    else None
                ),
            )

            return True

        except Exception as e:
            # Do not commit or rollback - let caller handle transaction boundary
            logger.error(
                "reconciliation_failed",
                job_id=str(job_id),
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def _is_queued_job_eligible(self, job: Job) -> bool:
        """Check if QUEUED job is eligible for reconciliation

        A job is eligible if:
        1. Status is QUEUED (should already be filtered, but double-check)
        2. created_at > QUEUED_AGE_THRESHOLD (old enough for first recovery)
        3. last_recovery_attempt_at is NULL (never recovered) OR
           last_recovery_attempt_at < NOW() - QUEUED_RECOVERY_INTERVAL (throttle expired)

        This is the definitive eligibility check using the locked job row.
        Must be called AFTER acquiring the row lock to ensure we're checking
        the current state, not stale detection results.

        Args:
            job: Locked job instance with current database values

        Returns:
            True if job is eligible for reconciliation, False otherwise
        """
        # Verify status (should already be QUEUED, but double-check)
        if job.status != JobStatus.QUEUED:
            logger.debug(
                "job_not_queued",
                job_id=str(job.id),
                status=job.status.value if job.status else None,
            )
            return False

        # Check age threshold (first recovery eligibility)
        now = datetime.utcnow()
        age_threshold = now - self.QUEUED_AGE_THRESHOLD

        if job.created_at >= age_threshold:
            logger.debug(
                "job_too_recent",
                job_id=str(job.id),
                created_at=job.created_at.isoformat() if job.created_at else None,
                age_threshold=age_threshold.isoformat(),
            )
            return False

        # Check throttling (subsequent recovery eligibility)
        if job.last_recovery_attempt_at is not None:
            recovery_threshold = now - self.QUEUED_RECOVERY_INTERVAL
            if job.last_recovery_attempt_at >= recovery_threshold:
                logger.debug(
                    "job_recovery_throttled",
                    job_id=str(job.id),
                    last_recovery_attempt_at=job.last_recovery_attempt_at.isoformat(),
                    recovery_threshold=recovery_threshold.isoformat(),
                )
                return False

        return True

    async def _reconcile_queued_job_in_transaction(self, job_id: UUID) -> bool:
        """Reconcile a single QUEUED job in its own dedicated transaction

        Helper for batch reconciliation. Creates a new session, reconciles the job,
        and commits or rolls back the transaction.

        Args:
            job_id: Job ID to reconcile

        Returns:
            True if reconciled successfully, False otherwise
        """
        if not self.session_factory:
            raise RuntimeError(
                "_reconcile_queued_job_in_transaction requires session_factory. "
                "Use reconcile_queued_job() for caller-owned transactions."
            )

        async with self.session_factory() as job_session:
            try:
                # Create service with same threshold configuration as parent
                recovery_service = JobRecoveryService(
                    job_session,
                    queued_age_threshold_minutes=(
                        int(self.QUEUED_AGE_THRESHOLD.total_seconds() / 60)
                        if self.QUEUED_AGE_THRESHOLD != timedelta(minutes=10)
                        else None
                    ),
                    queued_throttle_hours=(
                        int(self.QUEUED_RECOVERY_INTERVAL.total_seconds() / 3600)
                        if self.QUEUED_RECOVERY_INTERVAL != timedelta(hours=1)
                        else None
                    ),
                )
                success = await recovery_service.reconcile_queued_job(job_id)
                if success:
                    await job_session.commit()
                    return True
                return False
            except Exception as e:
                await job_session.rollback()
                logger.error(
                    "reconciliation_job_failed",
                    job_id=str(job_id),
                    error=str(e),
                    error_type=type(e).__name__,
                )
                return False

    async def reconcile_queued_jobs_batch(
        self, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> tuple[int, int]:
        """Detect and reconcile a batch of QUEUED jobs

        This is the main entry point for periodic QUEUED-job reconciliation tasks.
        Requires session_factory to provide per-job transaction isolation.

        Each job reconciliation:
        - Gets its own dedicated session/transaction
        - Acquires SELECT FOR UPDATE SKIP LOCKED
        - Commits or rolls back independently
        - Does not affect other jobs in the batch

        The detection query uses self.db for read-only access and does not
        hold locks during reconciliation processing.

        Args:
            batch_size: Maximum number of QUEUED jobs to reconcile in this batch

        Returns:
            Tuple of (detected_count, reconciled_count)

        Raises:
            RuntimeError: If session_factory not provided
        """
        if not self.session_factory:
            raise RuntimeError(
                "reconcile_queued_jobs_batch requires session_factory for per-job isolation. "
                "Provide session_factory in constructor or use reconcile_queued_job() directly."
            )

        # Detection uses self.db for read-only query (no locks held)
        queued_job_ids = await self.detect_queued_jobs(batch_size=batch_size)

        if not queued_job_ids:
            return (0, 0)

        # Process each job in its own transaction
        reconciled_count = 0
        for job_id in queued_job_ids:
            success = await self._reconcile_queued_job_in_transaction(job_id)
            if success:
                reconciled_count += 1

        logger.info(
            "reconciliation_batch_completed",
            detected=len(queued_job_ids),
            reconciled=reconciled_count,
            failed=len(queued_job_ids) - reconciled_count,
        )

        return (len(queued_job_ids), reconciled_count)
