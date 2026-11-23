"""Outbox dispatcher for reliable event publishing with PostgreSQL row locking"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.core.logging_config import setup_logging
from app.db.models import Job, JobOutbox, JobStatus
from app.services.queue_service import QueueService

logger = setup_logging()


# Exponential backoff constants
INITIAL_BACKOFF_SECONDS = 1  # Start with 1 second
MAX_BACKOFF_SECONDS = 300  # Cap at 5 minutes
BACKOFF_MULTIPLIER = 2  # Double each time


class OutboxDispatcher:
    """
    Dispatcher for publishing unpublished events from the outbox table.

    Uses PostgreSQL FOR UPDATE SKIP LOCKED to safely coordinate multiple
    dispatcher instances without deadlocks or duplicate processing.

    Event Types Supported:
    - job_created: Initial job creation → main queue
    - job_queued_recovery: QUEUED-job reconciliation → main queue (Step 289C)
    - job_recovered: PROCESSING-job recovery → delayed retry queue

    Reliability Guarantees:
    - At-least-once delivery: Events are published at least once
    - Crash safety: A crash after Redis publication but before marking the
      event as published will cause duplicate publication on restart
    - Does NOT guarantee exactly-once delivery
    - Transient failures use exponential backoff with durable scheduling
    - No hard attempt limit - events remain retryable indefinitely

    Exponential Backoff:
    - First failure: retry after 1 second
    - Second failure: retry after 2 seconds
    - Third failure: retry after 4 seconds
    - Continues doubling up to maximum of 5 minutes
    - Backoff persisted in next_publication_attempt_at for durability
    - Prevents tight retry loops during Redis outages

    Failure Classification:
    1. Transient failures (Redis unavailable):
       - Use exponential backoff
       - Event remains eligible for retry (published=False, reconciliation_required=False)
       - Example: ConnectionError, TimeoutError

    2. Permanent undeliverable events (mark reconciliation_required=True):
       - Event excluded from automatic retry
       - Preserved for manual review with diagnostic information
       - Never marked published=True (never delivered to Redis)
       - Examples: NULL retry_attempt, stale retry_attempt, job not found, state mismatch

    3. Successfully delivered (mark published=True):
       - Redis confirmed receipt
       - Event marked published with timestamp
       - Only this state means "delivered to Redis"

    Recovery Mechanism:
    - Failed events remain in outbox with error details
    - Exponential backoff prevents resource exhaustion during transient failures
    - Reconciliation events excluded from polling (won't retry automatically)
    - Operators can monitor reconciliation_required=True for manual review
    - Manual intervention possible via process_single_event(bypass=True)
    """

    def __init__(
        self,
        queue_service: QueueService,
        batch_size: int = 10,
        poll_interval_seconds: float = 1.0,
    ):
        """
        Initialize the outbox dispatcher.

        Args:
            queue_service: Queue service for publishing events
            batch_size: Number of events to process per batch
            poll_interval_seconds: Time to wait between polls when no events ready
        """
        self.queue_service = queue_service
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self._running = False

    async def start(self) -> None:
        """Start the dispatcher loop"""
        self._running = True
        logger.info("outbox_dispatcher_started", batch_size=self.batch_size)

        try:
            while self._running:
                try:
                    processed = await self.process_batch()
                    if processed == 0:
                        # No events to process, wait before next poll
                        await asyncio.sleep(self.poll_interval_seconds)
                    else:
                        # Events processed, continue immediately
                        logger.debug("outbox_batch_processed", count=processed)
                except Exception as e:
                    logger.error("outbox_batch_error", error=str(e), error_type=type(e).__name__)
                    await asyncio.sleep(self.poll_interval_seconds)
        finally:
            logger.info("outbox_dispatcher_stopped")

    async def stop(self) -> None:
        """Stop the dispatcher loop"""
        self._running = False

    async def process_batch(self) -> int:
        """
        Process a batch of unpublished events.

        Uses FOR UPDATE SKIP LOCKED to claim events safely across multiple
        dispatcher instances.

        Returns:
            Number of events successfully published in this batch
        """
        async with AsyncSessionLocal() as session:
            try:
                # Fetch and lock unpublished events (excludes exhausted events)
                events = await self._fetch_and_lock_events(session)

                if not events:
                    return 0

                # Process each event
                processed_count = 0
                for event in events:
                    success = await self._publish_event(session, event, bypass_backoff=False)
                    if success:
                        processed_count += 1

                # Commit all changes (published flags and error updates)
                # NOTE: If this commit fails after Redis publication, events
                # remain unpublished and will be retried (duplicate publication)
                await session.commit()
                return processed_count

            except Exception as e:
                await session.rollback()
                logger.error("outbox_batch_transaction_failed", error=str(e))
                raise

    async def _fetch_and_lock_events(self, session: AsyncSession) -> List[JobOutbox]:
        """
        Fetch unpublished events eligible for publication with FOR UPDATE SKIP LOCKED.

        An event is eligible if:
        - published = False
        - reconciliation_required = False
        - next_publication_attempt_at IS NULL (immediate) OR
          next_publication_attempt_at <= now (backoff expired)

        This ensures that:
        - Only one dispatcher claims each event
        - No deadlocks occur between concurrent dispatchers
        - Events in exponential backoff are not retried prematurely
        - Events requiring reconciliation are excluded from automatic retry
        - No hard limit on attempts - retryable events remain eligible indefinitely

        Args:
            session: Database session

        Returns:
            List of locked outbox events eligible for publication
        """
        now = datetime.utcnow()

        stmt = (
            select(JobOutbox)
            .where(JobOutbox.published == False)  # noqa: E712
            .where(JobOutbox.reconciliation_required == False)  # noqa: E712
            .where(
                or_(
                    JobOutbox.next_publication_attempt_at.is_(None),
                    JobOutbox.next_publication_attempt_at <= now,
                )
            )
            .order_by(JobOutbox.created_at)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )

        result = await session.execute(stmt)
        events = list(result.scalars().all())

        if events:
            logger.debug("outbox_events_locked", count=len(events))

        return events

    def _calculate_next_attempt_time(self, current_attempts: int) -> datetime:
        """
        Calculate next publication attempt time using bounded exponential backoff.

        Backoff formula: min(INITIAL * (MULTIPLIER ^ (attempts - 1)), MAX)

        The formula uses (attempts - 1) because this is called AFTER incrementing attempts.
        This ensures:
        - First failure (attempts=1): 2^0 = 1 second
        - Second failure (attempts=2): 2^1 = 2 seconds
        - Third failure (attempts=3): 2^2 = 4 seconds
        - Fourth failure (attempts=4): 2^3 = 8 seconds
        - Continues doubling until capped at 300 seconds (5 minutes)

        Args:
            current_attempts: Number of attempts already made (including current)

        Returns:
            Timestamp for next publication attempt
        """
        backoff_seconds = min(
            INITIAL_BACKOFF_SECONDS * (BACKOFF_MULTIPLIER ** (current_attempts - 1)),
            MAX_BACKOFF_SECONDS,
        )
        return datetime.utcnow() + timedelta(seconds=backoff_seconds)

    async def _publish_event(
        self, session: AsyncSession, event: JobOutbox, bypass_backoff: bool = False
    ) -> bool:
        """
        Publish a single event to the queue.

        Routes events based on event_type:
        - job_created: Publish to main queue immediately
        - job_recovered: Publish to delayed queue with job's next_retry_at

        Failure Handling:
        - Transient failures (Redis unavailable): Set exponential backoff
        - Permanent failures (validation): Mark published with error
        - Reconciliation issues: Leave unpublished for investigation

        On failure, increments attempts and schedules next retry using
        exponential backoff. Events remain retryable indefinitely - no
        hard attempt limit.

        Args:
            session: Database session
            event: Outbox event to publish
            bypass_backoff: If True, ignore next_publication_attempt_at
                          (for manual recovery only)

        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Increment attempt counter
            event.attempts += 1

            # Fetch job details with FOR UPDATE to prevent race conditions
            job = await self._get_job_details_for_update(session, event.job_id)
            if not job:
                # Job not found - mark for reconciliation (not published)
                # This prevents infinite retries while preserving diagnostic information
                error_msg = f"Job {event.job_id} not found"
                event.last_error = error_msg
                event.reconciliation_required = True
                event.next_publication_attempt_at = None  # No more retries
                await session.flush()

                logger.error(
                    "outbox_job_not_found_reconciliation",
                    event_id=str(event.id),
                    job_id=str(event.job_id),
                    attempt=event.attempts,
                )
                return False  # Not successfully published

            # Route based on event type
            if event.event_type == "job_recovered":
                success = await self._publish_recovery_event(session, event, job)
            elif event.event_type == "job_queued_recovery":
                success = await self._publish_queued_recovery_event(session, event, job)
            else:
                # Default: job_created and any other event types
                success = await self._publish_standard_event(session, event, job)

            if success:
                # Mark as published
                # For stale recovery events, error is already set - don't clear it
                # For normal publications, clear the error
                was_already_published = event.published
                event.published = True
                event.published_at = datetime.utcnow()
                event.next_publication_attempt_at = None  # Clear backoff

                # Clear error only if this wasn't already published
                # (stale events are marked published by inner method with error preserved)
                if not was_already_published:
                    event.last_error = None

                await session.flush()

                logger.info(
                    "outbox_event_published",
                    event_id=str(event.id),
                    job_id=str(event.job_id),
                    job_type=job.job_type,
                    event_type=event.event_type,
                    attempt=event.attempts,
                )
                return True
            else:
                # Publication failed - use exponential backoff for retry
                # (unless this was already marked for reconciliation by inner method)
                if not event.published and not event.reconciliation_required:
                    next_attempt = self._calculate_next_attempt_time(event.attempts)
                    event.next_publication_attempt_at = next_attempt

                    if not event.last_error:
                        event.last_error = "Redis publication failed"

                    await session.flush()

                    logger.warning(
                        "outbox_publish_failed_scheduled_retry",
                        event_id=str(event.id),
                        job_id=str(event.job_id),
                        event_type=event.event_type,
                        attempt=event.attempts,
                        next_attempt=next_attempt.isoformat(),
                        backoff_seconds=(next_attempt - datetime.utcnow()).total_seconds(),
                    )
                return False

        except Exception as e:
            # Unexpected error - use exponential backoff for retry
            event.last_error = f"{type(e).__name__}: {str(e)}"
            next_attempt = self._calculate_next_attempt_time(event.attempts)
            event.next_publication_attempt_at = next_attempt
            await session.flush()

            logger.error(
                "outbox_publish_exception_scheduled_retry",
                event_id=str(event.id),
                job_id=str(event.job_id),
                event_type=event.event_type,
                error=str(e),
                error_type=type(e).__name__,
                attempt=event.attempts,
                next_attempt=next_attempt.isoformat(),
            )
            return False

    async def _publish_standard_event(
        self, session: AsyncSession, event: JobOutbox, job: Job
    ) -> bool:
        """
        Publish a standard job_created event to the main queue.

        Args:
            session: Database session
            event: Outbox event
            job: Job details

        Returns:
            True if published successfully
        """
        # Preserve priority 0 (CRITICAL) - use explicit None check
        priority = job.priority if job.priority is not None else 5

        # Publish to main queue via QueueService
        success = await self.queue_service.enqueue_job(
            job_id=job.id,
            job_type=job.job_type,
            priority=priority,
        )

        if not success:
            logger.warning(
                "standard_event_enqueue_failed",
                event_id=str(event.id),
                job_id=str(job.id),
                priority=priority,
            )

        return success

    async def _publish_queued_recovery_event(
        self, session: AsyncSession, event: JobOutbox, job: Job
    ) -> bool:
        """
        Publish a job_queued_recovery event to the main queue.

        This event type is created by the QUEUED-job reconciler (Step 289B) when
        a job remains in QUEUED status beyond the recovery threshold, indicating
        potential Redis message loss.

        Validation Requirements:
        1. Job must still exist
        2. Job must still be in QUEUED status
        3. event.retry_attempt must be NULL (not a retry, recovery from message loss)

        Message Format:
        - Same as job_created (no retry_attempt field)
        - Published to main queue (not retry queue)
        - Worker uses standard claim fence (status=QUEUED, worker_id=NULL)

        Undeliverable Events (mark reconciliation_required=True):
        - Job not in QUEUED status (already claimed, completed, or failed)
        - event.retry_attempt is not NULL (invalid state)

        These events are excluded from automatic retry but preserved for manual review.
        They are NOT marked published=True since they never reached Redis.

        Args:
            session: Database session
            event: Outbox event (job_queued_recovery)
            job: Job details

        Returns:
            True if published successfully to Redis, False otherwise
        """
        # Validate retry_attempt is NULL (required for QUEUED recovery)
        if event.retry_attempt is not None:
            error_msg = (
                f"job_queued_recovery event has non-NULL retry_attempt={event.retry_attempt}"
            )
            event.last_error = error_msg
            # Mark for reconciliation (not published - never delivered to Redis)
            event.reconciliation_required = True
            event.next_publication_attempt_at = None  # No more automatic retries
            await session.flush()

            logger.error(
                "queued_recovery_event_invalid_retry_attempt_reconciliation",
                event_id=str(event.id),
                job_id=str(job.id),
                retry_attempt=event.retry_attempt,
            )
            return False  # Not successfully published

        # Validate job is still in QUEUED status
        # If job was already claimed by a worker, no need to publish recovery message
        if job.status != JobStatus.QUEUED:
            error_msg = f"Job status is {job.status.value}, expected QUEUED"
            event.last_error = error_msg
            # Mark for reconciliation - job no longer needs recovery
            # This is informational, not an error - job was claimed naturally
            event.reconciliation_required = True
            event.next_publication_attempt_at = None  # No more automatic retries
            await session.flush()

            logger.info(
                "queued_recovery_event_job_not_queued_skipped",
                event_id=str(event.id),
                job_id=str(job.id),
                job_status=job.status.value,
                message="Job already claimed or completed, recovery not needed",
            )
            return False  # Not successfully published (but not an error)

        # Preserve priority 0 (CRITICAL) - use explicit None check
        priority = job.priority if job.priority is not None else 5

        # Publish to main queue via QueueService
        # Message format same as job_created (no retry_attempt field)
        success = await self.queue_service.enqueue_job(
            job_id=job.id,
            job_type=job.job_type,
            priority=priority,
        )

        if not success:
            # Transient Redis failure - will retry with backoff
            logger.warning(
                "queued_recovery_event_enqueue_failed",
                event_id=str(event.id),
                job_id=str(job.id),
                priority=priority,
            )
            return False

        # Do NOT modify job state - job remains QUEUED
        # Worker will claim it using standard fence (status=QUEUED, worker_id=NULL)

        logger.info(
            "queued_recovery_event_published",
            event_id=str(event.id),
            job_id=str(job.id),
            job_status=job.status.value,
            priority=priority,
        )

        return True

    async def _publish_recovery_event(
        self, session: AsyncSession, event: JobOutbox, job: Job
    ) -> bool:
        """
        Publish a job_recovered event to the delayed queue.

        Uses the persisted recovery state from the recovery service transaction:
        - job.status must be RETRYING
        - job.next_retry_at determines the Redis schedule
        - event.retry_attempt must match job.retry_count (detects stale events)

        Does NOT modify job state - recovery service already set:
        - status = RETRYING
        - retry_count (incremented)
        - next_retry_at (calculated)

        Redis publication uses (job_id, retry_attempt) as the member for
        deterministic deduplication. Multiple publications of the same event
        update the same sorted set member, not create duplicates.

        Undeliverable Events (mark reconciliation_required=True):
        - NULL retry_attempt (legacy event, cannot be published safely)
        - Stale retry_attempt (newer attempt exists, this is superseded)
        - Job status not RETRYING (state inconsistency)
        - Job next_retry_at is NULL (missing scheduling data)

        These events are excluded from automatic retry but preserved for manual review.
        They are NOT marked published=True since they never reached Redis.

        Args:
            session: Database session
            event: Outbox event with retry_attempt
            job: Job details

        Returns:
            True if published successfully to Redis, False otherwise
        """
        # Validate retry_attempt is present (required for recovery events)
        if event.retry_attempt is None:
            error_msg = "job_recovered event has NULL retry_attempt (legacy event)"
            event.last_error = error_msg
            # Mark for reconciliation (not published - never delivered to Redis)
            # Prevents infinite retries while preserving diagnostic information
            event.reconciliation_required = True
            event.next_publication_attempt_at = None  # No more automatic retries
            await session.flush()

            logger.error(
                "recovery_event_null_retry_attempt_reconciliation",
                event_id=str(event.id),
                job_id=str(job.id),
                event_type=event.event_type,
                created_at=event.created_at.isoformat() if event.created_at else None,
            )
            return False  # Not successfully published

        # Validate job is in RETRYING status
        if job.status != JobStatus.RETRYING:
            error_msg = f"Job status is {job.status.value}, expected RETRYING"
            event.last_error = error_msg
            # Mark for reconciliation - state inconsistency needs investigation
            event.reconciliation_required = True
            event.next_publication_attempt_at = None  # No more automatic retries
            await session.flush()

            logger.warning(
                "recovery_event_job_not_retrying_reconciliation",
                event_id=str(event.id),
                job_id=str(job.id),
                retry_attempt=event.retry_attempt,
                job_status=job.status.value,
                job_retry_count=job.retry_count,
            )
            return False  # Not successfully published

        # Detect stale events: retry_attempt must match current retry_count
        current_retry_count = job.retry_count or 0
        if event.retry_attempt != current_retry_count:
            error_msg = (
                f"Stale event: retry_attempt={event.retry_attempt} "
                f"but current retry_count={current_retry_count}"
            )
            event.last_error = error_msg
            # Mark for reconciliation - this event is superseded
            # The current retry_attempt event is the valid one
            event.reconciliation_required = True
            event.next_publication_attempt_at = None  # No more retries
            await session.flush()

            logger.warning(
                "recovery_event_stale_reconciliation",
                event_id=str(event.id),
                job_id=str(job.id),
                event_retry_attempt=event.retry_attempt,
                job_retry_count=current_retry_count,
                job_status=job.status.value,
            )
            return False  # Not successfully published

        # Validate next_retry_at is set
        if job.next_retry_at is None:
            error_msg = "Job next_retry_at is NULL"
            event.last_error = error_msg
            # Mark for reconciliation - missing scheduling data
            event.reconciliation_required = True
            event.next_publication_attempt_at = None  # No more retries
            await session.flush()

            logger.error(
                "recovery_event_no_next_retry_reconciliation",
                event_id=str(event.id),
                job_id=str(job.id),
                retry_attempt=event.retry_attempt,
            )
            return False  # Not successfully published

        # Use persisted next_retry_at for Redis score
        execute_at = job.next_retry_at.timestamp()

        # Preserve priority 0 (CRITICAL) - use explicit None check
        priority = job.priority if job.priority is not None else 5

        # Create deterministic Redis member using job_id and retry_attempt
        # This ensures multiple publications update the same member, not create duplicates
        job_data = {
            "job_id": str(job.id),
            "job_type": job.job_type,
            "priority": priority,
            "retry_attempt": event.retry_attempt,
        }

        # Publish to Redis delayed queue
        success = await self.queue_service.redis.push_to_delayed_queue(
            job_data=job_data,
            execute_at=execute_at,
        )

        if not success:
            # Transient Redis failure - will retry with backoff
            logger.warning(
                "recovery_event_delayed_queue_failed",
                event_id=str(event.id),
                job_id=str(job.id),
                retry_attempt=event.retry_attempt,
                next_retry_at=job.next_retry_at.isoformat(),
            )
            return False

        # Do NOT modify job state - recovery service already did that
        # Just mark the outbox event as published

        logger.info(
            "recovery_event_published",
            event_id=str(event.id),
            job_id=str(job.id),
            retry_attempt=event.retry_attempt,
            retry_count=current_retry_count,
            next_retry_at=job.next_retry_at.isoformat(),
        )

        return True

    async def _get_job_details(self, session: AsyncSession, job_id: UUID) -> Optional[Job]:
        """
        Fetch job details needed for publication (without locking).

        Args:
            session: Database session
            job_id: Job ID

        Returns:
            Job object or None if not found
        """
        stmt = select(Job).where(Job.id == job_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def _get_job_details_for_update(
        self, session: AsyncSession, job_id: UUID
    ) -> Optional[Job]:
        """
        Fetch job details with FOR UPDATE lock for recovery events.

        This prevents race conditions when updating job status during recovery.

        Args:
            session: Database session
            job_id: Job ID

        Returns:
            Job object or None if not found
        """
        stmt = select(Job).where(Job.id == job_id).with_for_update()
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def process_single_event(self, event_id: UUID, bypass_backoff: bool = False) -> bool:
        """
        Process a single event by ID (useful for testing and manual retry).

        Can bypass exponential backoff for manual operator intervention.

        Args:
            event_id: Outbox event ID
            bypass_backoff: If True, ignore next_publication_attempt_at
                          (use only for manual operator intervention)

        Returns:
            True if published successfully
        """
        async with AsyncSessionLocal() as session:
            try:
                # Fetch and lock the specific event
                stmt = (
                    select(JobOutbox)
                    .where(JobOutbox.id == event_id)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                event = result.scalar_one_or_none()

                if not event:
                    logger.warning("outbox_event_not_found", event_id=str(event_id))
                    return False

                if event.published:
                    logger.info("outbox_event_already_published", event_id=str(event_id))
                    return True

                # Pass bypass flag to _publish_event
                success = await self._publish_event(session, event, bypass_backoff)
                await session.commit()
                return success

            except Exception as e:
                await session.rollback()
                logger.error(
                    "outbox_single_event_failed",
                    event_id=str(event_id),
                    error=str(e),
                )
                return False
