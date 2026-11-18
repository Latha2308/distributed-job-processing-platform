"""Monitoring endpoints"""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis_client import get_redis, RedisClient
from app.services.job_service import JobService
from app.services.queue_service import QueueService
from app.services.worker_service import WorkerService

router = APIRouter()


@router.get("/dashboard", summary="Get monitoring dashboard data")
async def get_dashboard(
    db: AsyncSession = Depends(get_db),
    redis: RedisClient = Depends(get_redis),
):
    """
    Get comprehensive monitoring dashboard data including:
    - Job statistics
    - Worker metrics
    - Queue depth
    - System health
    """
    # Initialize services
    queue_service = QueueService(redis)
    job_service = JobService(db, redis, queue_service)
    worker_service = WorkerService(db)

    # Gather metrics
    job_stats = await job_service.get_job_stats()
    job_metrics = await job_service.get_job_metrics_by_type()
    worker_metrics = await worker_service.get_worker_metrics()
    queue_depth = await queue_service.get_queue_depth()

    return {
        "job_stats": job_stats,
        "job_metrics_by_type": job_metrics,
        "worker_metrics": worker_metrics,
        "queue_depth": queue_depth,
        "system_health": {
            "status": "healthy" if worker_metrics.active_workers > 0 else "degraded",
            "active_workers": worker_metrics.active_workers,
            "queue_depth": queue_depth,
        },
    }


@router.get("/health", summary="System health check")
async def health_check(
    db: AsyncSession = Depends(get_db),
    redis: RedisClient = Depends(get_redis),
):
    """
    Check system health including:
    - Database connectivity
    - Redis connectivity
    - Active workers
    """
    try:
        # Check database
        await db.execute(text("SELECT 1"))
        db_healthy = True
    except Exception:
        db_healthy = False

    try:
        # Check Redis
        client = await redis.get_client()
        await client.ping()
        redis_healthy = True
    except Exception:
        redis_healthy = False

    # Check workers
    worker_service = WorkerService(db)
    workers = await worker_service.list_workers(active_only=True)
    workers_healthy = len(workers) > 0

    overall_healthy = db_healthy and redis_healthy and workers_healthy

    return {
        "status": "healthy" if overall_healthy else "unhealthy",
        "checks": {
            "database": "up" if db_healthy else "down",
            "redis": "up" if redis_healthy else "down",
            "workers": {
                "status": "up" if workers_healthy else "down",
                "active_count": len(workers),
            },
        },
    }


@router.get("/queue", summary="Queue status")
async def get_queue_status(
    redis: RedisClient = Depends(get_redis),
):
    """Get current queue status and depth"""
    queue_service = QueueService(redis)
    depth = await queue_service.get_queue_depth()

    return {
        "queue_depth": depth,
        "queue_type": "sqs" if queue_service.use_sqs else "redis",
    }
