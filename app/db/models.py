"""Database models"""

import enum
from datetime import datetime
from typing import Optional
from uuid import uuid4

from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    Enum,
    JSON,
    Text,
    Float,
    Boolean,
    Index,
)
from sqlalchemy.dialects.postgresql import UUID

from app.core.database import Base


class JobStatus(str, enum.Enum):
    """Job status enumeration"""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


class JobPriority(int, enum.Enum):
    """Job priority levels (lower number = higher priority)"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 5
    LOW = 8
    BACKGROUND = 10


class Job(Base):
    """Job model for tracking job execution"""
    
    __tablename__ = "jobs"
    
    # Primary fields
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4, index=True)
    job_type = Column(String(100), nullable=False, index=True)
    payload = Column(JSON, nullable=False)
    priority = Column(Integer, default=JobPriority.NORMAL.value, index=True)
    
    # Status tracking
    status = Column(
        Enum(JobStatus),
        default=JobStatus.PENDING,
        nullable=False,
        index=True
    )
    
    # Idempotency
    idempotency_key = Column(String(255), unique=True, nullable=True, index=True)
    
    # Execution tracking
    result = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    error_type = Column(String(100), nullable=True)
    
    # Retry mechanism
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    next_retry_at = Column(DateTime, nullable=True)
    
    # Worker tracking
    worker_id = Column(String(100), nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Performance metrics
    processing_duration = Column(Float, nullable=True)  # seconds
    queue_wait_time = Column(Float, nullable=True)  # seconds
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )
    
    # Timeout
    timeout = Column(Integer, default=300)  # seconds
    
    # Dead letter queue
    is_dead_letter = Column(Boolean, default=False, index=True)
    moved_to_dlq_at = Column(DateTime, nullable=True)
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_job_status_priority', 'status', 'priority'),
        Index('idx_job_type_status', 'job_type', 'status'),
        Index('idx_created_at', 'created_at'),
        Index('idx_next_retry', 'next_retry_at'),
    )
    
    def __repr__(self) -> str:
        return f"<Job(id={self.id}, type={self.job_type}, status={self.status})>"


class JobExecution(Base):
    """Job execution history for audit trail"""
    
    __tablename__ = "job_executions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    
    # Execution details
    worker_id = Column(String(100), nullable=False)
    status = Column(Enum(JobStatus), nullable=False)
    
    # Timing
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    duration = Column(Float, nullable=True)  # seconds
    
    # Results
    result = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    error_type = Column(String(100), nullable=True)
    
    # Context
    attempt_number = Column(Integer, nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        Index('idx_job_execution_job_id', 'job_id'),
        Index('idx_job_execution_status', 'status'),
    )
    
    def __repr__(self) -> str:
        return f"<JobExecution(id={self.id}, job_id={self.job_id}, status={self.status})>"


class WorkerHeartbeat(Base):
    """Worker heartbeat for tracking worker health"""
    
    __tablename__ = "worker_heartbeats"
    
    worker_id = Column(String(100), primary_key=True)
    
    # Status
    is_active = Column(Boolean, default=True)
    current_job_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Capacity
    jobs_processed = Column(Integer, default=0)
    jobs_failed = Column(Integer, default=0)
    
    # Timing
    started_at = Column(DateTime, nullable=False)
    last_heartbeat = Column(DateTime, nullable=False)
    
    # Metadata (using worker_metadata as Python attribute to avoid SQLAlchemy reserved name)
    worker_metadata = Column("metadata", JSON, nullable=True)
    
    __table_args__ = (
        Index('idx_worker_active', 'is_active'),
        Index('idx_worker_heartbeat', 'last_heartbeat'),
    )
    
    def __repr__(self) -> str:
        return f"<WorkerHeartbeat(worker_id={self.worker_id}, active={self.is_active})>"
