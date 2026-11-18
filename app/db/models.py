"""Database models"""

import enum
from datetime import datetime
from typing import Optional
from uuid import UUID as PythonUUID, uuid4

from sqlalchemy import (
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
from sqlalchemy.orm import Mapped, mapped_column

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
    id: Mapped[PythonUUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid4, index=True
    )
    job_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    priority: Mapped[Optional[int]] = mapped_column(
        Integer, default=JobPriority.NORMAL.value, index=True
    )

    # Status tracking
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus), default=JobStatus.PENDING, nullable=False, index=True
    )

    # Idempotency
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(255), unique=True, index=True)

    # Execution tracking
    result: Mapped[Optional[dict]] = mapped_column(JSON)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    error_type: Mapped[Optional[str]] = mapped_column(String(100))

    # Retry mechanism
    retry_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    max_retries: Mapped[Optional[int]] = mapped_column(Integer, default=3)
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Worker tracking
    worker_id: Mapped[Optional[str]] = mapped_column(String(100))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Performance metrics
    processing_duration: Mapped[Optional[float]] = mapped_column(Float)  # seconds
    queue_wait_time: Mapped[Optional[float]] = mapped_column(Float)  # seconds

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    # Timeout
    timeout: Mapped[Optional[int]] = mapped_column(Integer, default=300)  # seconds

    # Dead letter queue
    is_dead_letter: Mapped[Optional[bool]] = mapped_column(Boolean, default=False, index=True)
    moved_to_dlq_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Indexes for performance
    __table_args__ = (
        Index("idx_job_status_priority", "status", "priority"),
        Index("idx_job_type_status", "job_type", "status"),
        Index("idx_created_at", "created_at"),
        Index("idx_next_retry", "next_retry_at"),
    )

    def __repr__(self) -> str:
        return f"<Job(id={self.id}, type={self.job_type}, status={self.status})>"


class JobExecution(Base):
    """Job execution history for audit trail"""

    __tablename__ = "job_executions"

    id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    job_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)

    # Execution details
    worker_id: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), nullable=False)

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    duration: Mapped[Optional[float]] = mapped_column(Float)  # seconds

    # Results
    result: Mapped[Optional[dict]] = mapped_column(JSON)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    error_type: Mapped[Optional[str]] = mapped_column(String(100))

    # Context
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("idx_job_execution_job_id", "job_id"),
        Index("idx_job_execution_status", "status"),
    )

    def __repr__(self) -> str:
        return f"<JobExecution(id={self.id}, job_id={self.job_id}, status={self.status})>"


class WorkerHeartbeat(Base):
    """Worker heartbeat for tracking worker health"""

    __tablename__ = "worker_heartbeats"

    worker_id: Mapped[str] = mapped_column(String(100), primary_key=True)

    # Status
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)
    current_job_id: Mapped[Optional[PythonUUID]] = mapped_column(UUID(as_uuid=True))

    # Capacity
    jobs_processed: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    jobs_failed: Mapped[Optional[int]] = mapped_column(Integer, default=0)

    # Timing
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Metadata (using worker_metadata as Python attribute to avoid SQLAlchemy reserved name)
    worker_metadata: Mapped[Optional[dict]] = mapped_column("metadata", JSON)

    __table_args__ = (
        Index("idx_worker_active", "is_active"),
        Index("idx_worker_heartbeat", "last_heartbeat"),
    )

    def __repr__(self) -> str:
        return f"<WorkerHeartbeat(worker_id={self.worker_id}, active={self.is_active})>"


class JobOutbox(Base):
    """Transactional outbox for reliable job event publishing"""

    __tablename__ = "job_outbox"

    # Primary key
    id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)

    # Job reference
    job_id: Mapped[PythonUUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)

    # Event tracking
    event_type: Mapped[str] = mapped_column(String(50), default="job_created", nullable=False)
    published: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Error tracking
    last_error: Mapped[Optional[str]] = mapped_column(Text)

    # Composite index for efficient polling
    __table_args__ = (Index("idx_outbox_published_created", "published", "created_at"),)

    def __repr__(self) -> str:
        return f"<JobOutbox(id={self.id}, job_id={self.job_id}, event_type={self.event_type}, published={self.published})>"
