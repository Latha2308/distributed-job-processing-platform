"""Job endpoints"""

import math
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.redis_client import get_redis, RedisClient
from app.db.models import JobStatus
from app.models.job import (
    JobCreate,
    JobResponse,
    JobUpdate,
    JobListResponse,
    JobStats,
    JobMetrics,
)
from app.services.job_service import JobService
from app.services.queue_service import QueueService

router = APIRouter()


def get_job_service(
    db: AsyncSession = Depends(get_db),
    redis: RedisClient = Depends(get_redis),
) -> JobService:
    """Dependency for JobService"""
    queue_service = QueueService(redis)
    return JobService(db, redis, queue_service)


@router.post(
    "/",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new job"
)
async def create_job(
    job_data: JobCreate,
    service: JobService = Depends(get_job_service),
):
    """
    Create a new job and enqueue it for processing.
    
    - **job_type**: Type of job to execute
    - **payload**: Job data as JSON
    - **priority**: Priority level (0=highest, 10=lowest)
    - **idempotency_key**: Optional unique key to prevent duplicate execution
    - **timeout**: Job timeout in seconds
    - **max_retries**: Maximum number of retry attempts
    """
    job = await service.create_job(job_data)
    return JobResponse.model_validate(job)


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job by ID"
)
async def get_job(
    job_id: UUID,
    service: JobService = Depends(get_job_service),
):
    """Get job details by ID"""
    job = await service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    return JobResponse.model_validate(job)


@router.get(
    "/",
    response_model=JobListResponse,
    summary="List jobs"
)
async def list_jobs(
    status_filter: Optional[JobStatus] = Query(None, alias="status"),
    job_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    service: JobService = Depends(get_job_service),
):
    """
    List jobs with optional filters and pagination.
    
    - **status**: Filter by job status
    - **job_type**: Filter by job type
    - **page**: Page number (starts at 1)
    - **page_size**: Number of items per page (max 100)
    """
    jobs, total = await service.list_jobs(
        status=status_filter,
        job_type=job_type,
        page=page,
        page_size=page_size,
    )
    
    return JobListResponse(
        items=[JobResponse.model_validate(job) for job in jobs],
        total=total,
        page=page,
        page_size=page_size,
        pages=math.ceil(total / page_size) if total > 0 else 0,
    )


@router.patch(
    "/{job_id}",
    response_model=JobResponse,
    summary="Update job status"
)
async def update_job(
    job_id: UUID,
    update_data: JobUpdate,
    service: JobService = Depends(get_job_service),
):
    """Update job status and result"""
    job = await service.update_job_status(job_id, update_data)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    return JobResponse.model_validate(job)


@router.post(
    "/{job_id}/retry",
    response_model=JobResponse,
    summary="Retry a failed job"
)
async def retry_job(
    job_id: UUID,
    service: JobService = Depends(get_job_service),
):
    """
    Retry a failed job.
    
    If the job has reached max retries, it will be moved to the dead letter queue.
    """
    job = await service.retry_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    return JobResponse.model_validate(job)


@router.get(
    "/stats/overview",
    response_model=JobStats,
    summary="Get job statistics"
)
async def get_job_stats(
    service: JobService = Depends(get_job_service),
):
    """Get overall job statistics"""
    return await service.get_job_stats()


@router.get(
    "/metrics/by-type",
    response_model=list[JobMetrics],
    summary="Get metrics by job type"
)
async def get_metrics_by_type(
    service: JobService = Depends(get_job_service),
):
    """Get metrics grouped by job type"""
    return await service.get_job_metrics_by_type()
