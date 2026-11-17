"""Worker endpoints"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.worker import WorkerStatus, WorkerListResponse, WorkerMetrics
from app.services.worker_service import WorkerService

router = APIRouter()


def get_worker_service(db: AsyncSession = Depends(get_db)) -> WorkerService:
    """Dependency for WorkerService"""
    return WorkerService(db)


@router.get(
    "/",
    response_model=WorkerListResponse,
    summary="List all workers"
)
async def list_workers(
    active_only: bool = False,
    service: WorkerService = Depends(get_worker_service),
):
    """
    List all workers.
    
    - **active_only**: Only return active workers
    """
    workers = await service.list_workers(active_only=active_only)
    
    active_count = sum(1 for w in workers if w.is_active)
    inactive_count = len(workers) - active_count
    
    return WorkerListResponse(
        workers=[WorkerStatus.model_validate(w) for w in workers],
        total_active=active_count,
        total_inactive=inactive_count,
    )


@router.get(
    "/{worker_id}",
    response_model=WorkerStatus,
    summary="Get worker by ID"
)
async def get_worker(
    worker_id: str,
    service: WorkerService = Depends(get_worker_service),
):
    """Get worker details by ID"""
    worker = await service.get_worker(worker_id)
    if not worker:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker {worker_id} not found"
        )
    return WorkerStatus.model_validate(worker)


@router.get(
    "/metrics/overview",
    response_model=WorkerMetrics,
    summary="Get worker metrics"
)
async def get_worker_metrics(
    service: WorkerService = Depends(get_worker_service),
):
    """Get worker metrics and statistics"""
    return await service.get_worker_metrics()


@router.post(
    "/cleanup",
    summary="Mark stale workers as inactive"
)
async def cleanup_stale_workers(
    service: WorkerService = Depends(get_worker_service),
):
    """Mark workers with stale heartbeats as inactive"""
    count = await service.mark_stale_workers_inactive()
    return {
        "message": f"Marked {count} stale workers as inactive",
        "count": count
    }
