"""Outbox dispatcher for reliable event publishing with PostgreSQL row locking"""

import asyncio
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal
from app.core.logging_config import setup_logging
from app.db.models import Job, JobOutbox
from app.services.queue_service import QueueService

logger = setup_logging()


class OutboxDispatcher:
    """
    Dispatcher for publishing unpublished events from the outbox table.

    Uses PostgreSQL FOR UPDATE SKIP LOCKED to safely coordinate multiple
    dispatcher instances without deadlocks or duplicate processing.

    Reliability Guarantees:
    - At-least-once delivery: Events are published at least once
    - Crash safety: A crash after Redis publication but before marking the
      event as published will cause duplicate publication on restart
    - Does NOT guarantee exactly-once delivery

    Recovery Mechanism:
    - Events exceeding max_attempts are excluded from automatic processing
    - Failed events remain in the outbox with error details for manual review
    - Operators can manually retry exhausted events using
      process_single_event(event_id, bypass_max_attempts=True)
    - This prevents silent data loss for critical events

    Failure Scenarios:
    1. Redis enqueue fails: Event stays unpublished, will retry
    2. Database commit fails after Redis success: Event stays unpublished,
       may cause duplicate publication on retry (at-least-once delivery)
    3. Crash after Redis success, before commit: Same as #2
    4. Referenced job missing: Event stays unpublished for investigation
    """

    def __init__(
        self,
        queue_service: QueueService,
        batch_size: int = 10,
        poll_interval_seconds: float = 1.0,
        max_attempts: int = 5,
    ):
        """
        Initialize the outbox dispatcher.

        Args:
            queue_service: Queue service for publishing events
            batch_size: Number of events to process per batch
            poll_interval_seconds: Time to wait between polls
            max_attempts: Maximum publication attempts before excluding from
                        automatic processing (manual review required)
        """
        self.queue_service = queue_service
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.max_attempts = max_attempts
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
                    success = await self._publish_event(session, event, bypass_max_attempts=False)
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
        Fetch unpublished events with FOR UPDATE SKIP LOCKED.

        This ensures that:
        - Only one dispatcher claims each event
        - No deadlocks occur between concurrent dispatchers
        - Events with attempts >= max_attempts are excluded to prevent blocking
          the queue (they remain stored for manual investigation)

        Args:
            session: Database session

        Returns:
            List of locked outbox events with attempts < max_attempts
        """
        stmt = (
            select(JobOutbox)
            .where(JobOutbox.published == False)  # noqa: E712
            .where(JobOutbox.attempts < self.max_attempts)
            .order_by(JobOutbox.created_at)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )

        result = await session.execute(stmt)
        events = list(result.scalars().all())

        if events:
            logger.debug("outbox_events_locked", count=len(events))

        return events

    async def _publish_event(
        self, session: AsyncSession, event: JobOutbox, bypass_max_attempts: bool = False
    ) -> bool:
        """
        Publish a single event to the queue.

        Fetches the job details, publishes to Redis, and marks as published
        only if Redis confirms success. On failure, increments attempts and
        records the error.

        Events that exceed max_attempts are skipped unless bypass is enabled
        for manual operator recovery.

        Args:
            session: Database session
            event: Outbox event to publish
            bypass_max_attempts: If True, ignore max_attempts limit
                                (for manual recovery only)

        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Check if max attempts exceeded (unless bypassed for manual recovery)
            if not bypass_max_attempts and event.attempts >= self.max_attempts:
                logger.warning(
                    "outbox_max_attempts_exceeded",
                    event_id=str(event.id),
                    job_id=str(event.job_id),
                    attempts=event.attempts,
                    max_attempts=self.max_attempts,
                    last_error=event.last_error,
                )
                # Do NOT mark as published - leave for manual review
                return False

            # Increment attempt counter
            event.attempts += 1

            # Fetch job details
            job = await self._get_job_details(session, event.job_id)
            if not job:
                # Job not found - DO NOT mark as published
                # Leave for manual investigation
                error_msg = f"Job {event.job_id} not found"
                event.last_error = error_msg
                await session.flush()

                logger.error(
                    "outbox_job_not_found",
                    event_id=str(event.id),
                    job_id=str(event.job_id),
                    attempt=event.attempts,
                )
                return False

            # Preserve priority 0 (CRITICAL) - use explicit None check
            priority = job.priority if job.priority is not None else 5

            # Publish to queue via QueueService
            success = await self.queue_service.enqueue_job(
                job_id=job.id,
                job_type=job.job_type,
                priority=priority,
            )

            if success:
                # Mark as published
                event.published = True
                event.published_at = datetime.utcnow()
                event.last_error = None
                await session.flush()

                logger.info(
                    "outbox_event_published",
                    event_id=str(event.id),
                    job_id=str(event.job_id),
                    job_type=job.job_type,
                    priority=priority,
                    attempt=event.attempts,
                )
                return True
            else:
                # Redis publication failed - event stays unpublished
                event.last_error = "Redis enqueue failed"
                await session.flush()

                logger.warning(
                    "outbox_publish_failed",
                    event_id=str(event.id),
                    job_id=str(event.job_id),
                    priority=priority,
                    attempt=event.attempts,
                    max_attempts=self.max_attempts,
                )
                return False

        except Exception as e:
            # Unexpected error during publication
            event.last_error = f"{type(e).__name__}: {str(e)}"
            await session.flush()

            logger.error(
                "outbox_publish_exception",
                event_id=str(event.id),
                job_id=str(event.job_id),
                error=str(e),
                error_type=type(e).__name__,
                attempt=event.attempts,
            )
            return False

    async def _get_job_details(self, session: AsyncSession, job_id: UUID) -> Optional[Job]:
        """
        Fetch job details needed for publication.

        Args:
            session: Database session
            job_id: Job ID

        Returns:
            Job object or None if not found
        """
        stmt = select(Job).where(Job.id == job_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    async def process_single_event(self, event_id: UUID, bypass_max_attempts: bool = False) -> bool:
        """
        Process a single event by ID (useful for testing and manual retry).

        Respects the retry policy unless explicitly bypassed by an operator
        for manual recovery.

        Args:
            event_id: Outbox event ID
            bypass_max_attempts: If True, ignore max_attempts limit
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
                success = await self._publish_event(session, event, bypass_max_attempts)
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
