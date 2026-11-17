"""Pydantic models for jobs"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict

from app.db.models import JobStatus, JobPriority


class JobCreate(BaseModel):
    """Schema for creating a new job"""
    
    job_type: str = Field(..., min_length=1, max_length=100)
    payload: Dict[str, Any] = Field(..., description="Job payload data")
    priority: int = Field(
        default=JobPriority.NORMAL.value,
        ge=0,
        le=10,
        description="Job priority (0=highest, 10=lowest)"
    )
    idempotency_key: Optional[str] = Field(
        None,
        max_length=255,
        description="Unique key to prevent duplicate job execution"
    )
    timeout: int = Field(
        default=300,
        ge=1,
        le=3600,
        description="Job timeout in seconds"
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retry attempts"
    )


class JobResponse(BaseModel):
    """Schema for job response"""
    
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    job_type: str
    payload: Dict[str, Any]
    priority: int
    status: JobStatus
    idempotency_key: Optional[str] = None
    
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    
    retry_count: int
    max_retries: int
    next_retry_at: Optional[datetime] = None
    
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    processing_duration: Optional[float] = None
    queue_wait_time: Optional[float] = None
    
    created_at: datetime
    updated_at: datetime
    
    timeout: int
    is_dead_letter: bool
    moved_to_dlq_at: Optional[datetime] = None


class JobUpdate(BaseModel):
    """Schema for updating job status"""
    
    status: Optional[JobStatus] = None
    result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None


class JobListResponse(BaseModel):
    """Schema for paginated job list"""
    
    items: list[JobResponse]
    total: int
    page: int
    page_size: int
    pages: int


class JobStats(BaseModel):
    """Schema for job statistics"""
    
    total_jobs: int
    pending: int
    queued: int
    processing: int
    completed: int
    failed: int
    retrying: int
    dead_letter: int
    
    avg_processing_duration: Optional[float] = None
    avg_queue_wait_time: Optional[float] = None
    
    success_rate: float
    failure_rate: float


class JobMetrics(BaseModel):
    """Schema for job metrics"""
    
    job_type: str
    total: int
    completed: int
    failed: int
    avg_duration: Optional[float] = None
    success_rate: float
