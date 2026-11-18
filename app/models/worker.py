"""Pydantic models for workers"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class WorkerStatus(BaseModel):
    """Schema for worker status"""

    model_config = ConfigDict(from_attributes=True)

    worker_id: str
    is_active: bool
    current_job_id: Optional[UUID] = None
    jobs_processed: int
    jobs_failed: int
    started_at: datetime
    last_heartbeat: datetime
    worker_metadata: Optional[Dict[str, Any]] = None


class WorkerListResponse(BaseModel):
    """Schema for list of workers"""

    workers: list[WorkerStatus]
    total_active: int
    total_inactive: int


class WorkerMetrics(BaseModel):
    """Schema for worker metrics"""

    total_workers: int
    active_workers: int
    inactive_workers: int
    total_jobs_processed: int
    total_jobs_failed: int
    average_jobs_per_worker: float
