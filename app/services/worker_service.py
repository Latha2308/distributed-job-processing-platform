"""Worker service for managing worker heartbeats"""

from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging_config import setup_logging
from app.core.metrics import active_workers
from app.db.models import WorkerHeartbeat
from app.models.worker import WorkerMetrics

logger = setup_logging()


class WorkerService:
    """Service for worker management"""

    HEARTBEAT_TIMEOUT = timedelta(seconds=60)

    def __init__(self, db: AsyncSession):
        self.db = db

    async def register_worker(
        self, worker_id: str, worker_metadata: Optional[dict] = None
    ) -> WorkerHeartbeat:
        """Register a new worker"""
        # Check if worker already exists
        existing = await self.get_worker(worker_id)

        if existing:
            # Reactivate existing worker
            existing.is_active = True
            existing.last_heartbeat = datetime.utcnow()
            existing.worker_metadata = worker_metadata
            await self.db.flush()
            await self.db.refresh(existing)
            logger.info("worker_reactivated", worker_id=worker_id)
            return existing

        # Create new worker
        worker = WorkerHeartbeat(
            worker_id=worker_id,
            is_active=True,
            started_at=datetime.utcnow(),
            last_heartbeat=datetime.utcnow(),
            worker_metadata=worker_metadata,
        )

        self.db.add(worker)
        await self.db.flush()
        await self.db.refresh(worker)

        # Update metrics
        await self._update_worker_metrics()

        logger.info("worker_registered", worker_id=worker_id)
        return worker

    async def update_heartbeat(
        self, worker_id: str, current_job_id: Optional[str] = None
    ) -> Optional[WorkerHeartbeat]:
        """Update worker heartbeat"""
        worker = await self.get_worker(worker_id)
        if not worker:
            return None

        worker.last_heartbeat = datetime.utcnow()
        worker.is_active = True

        if current_job_id:
            worker.current_job_id = UUID(current_job_id)
        else:
            worker.current_job_id = None

        await self.db.flush()
        await self.db.refresh(worker)

        logger.debug("worker_heartbeat", worker_id=worker_id)
        return worker

    async def deactivate_worker(self, worker_id: str) -> Optional[WorkerHeartbeat]:
        """Deactivate a worker"""
        worker = await self.get_worker(worker_id)
        if not worker:
            return None

        worker.is_active = False
        worker.current_job_id = None

        await self.db.flush()
        await self.db.refresh(worker)

        # Update metrics
        await self._update_worker_metrics()

        logger.info("worker_deactivated", worker_id=worker_id)
        return worker

    async def increment_job_count(
        self, worker_id: str, success: bool = True
    ) -> Optional[WorkerHeartbeat]:
        """Increment worker job count"""
        worker = await self.get_worker(worker_id)
        if not worker:
            return None

        if success:
            worker.jobs_processed = (worker.jobs_processed or 0) + 1
        else:
            worker.jobs_failed = (worker.jobs_failed or 0) + 1

        await self.db.flush()
        await self.db.refresh(worker)

        return worker

    async def get_worker(self, worker_id: str) -> Optional[WorkerHeartbeat]:
        """Get worker by ID"""
        result = await self.db.execute(
            select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker_id)
        )
        return result.scalar_one_or_none()

    async def list_workers(self, active_only: bool = False) -> List[WorkerHeartbeat]:
        """List all workers"""
        query = select(WorkerHeartbeat)

        if active_only:
            query = query.where(WorkerHeartbeat.is_active.is_(True))

        query = query.order_by(WorkerHeartbeat.last_heartbeat.desc())

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_worker_metrics(self) -> WorkerMetrics:
        """Get worker metrics"""
        workers = await self.list_workers()

        active = sum(1 for w in workers if w.is_active)
        inactive = len(workers) - active

        total_processed = sum((w.jobs_processed or 0) for w in workers)
        total_failed = sum((w.jobs_failed or 0) for w in workers)

        avg_jobs = total_processed / len(workers) if workers else 0

        return WorkerMetrics(
            total_workers=len(workers),
            active_workers=active,
            inactive_workers=inactive,
            total_jobs_processed=total_processed,
            total_jobs_failed=total_failed,
            average_jobs_per_worker=round(avg_jobs, 2),
        )

    async def mark_stale_workers_inactive(self) -> int:
        """Mark workers with stale heartbeats as inactive"""
        threshold = datetime.utcnow() - self.HEARTBEAT_TIMEOUT

        result = await self.db.execute(
            select(WorkerHeartbeat).where(
                and_(
                    WorkerHeartbeat.is_active.is_(True), WorkerHeartbeat.last_heartbeat < threshold
                )
            )
        )

        stale_workers = result.scalars().all()
        count = 0

        for worker in stale_workers:
            worker.is_active = False
            worker.current_job_id = None
            count += 1

            logger.warning(
                "worker_marked_stale",
                worker_id=worker.worker_id,
                last_heartbeat=worker.last_heartbeat.isoformat(),
            )

        if count > 0:
            await self.db.flush()
            await self._update_worker_metrics()

        return count

    async def _update_worker_metrics(self):
        """Update Prometheus worker metrics"""
        metrics = await self.get_worker_metrics()
        active_workers.set(metrics.active_workers)
