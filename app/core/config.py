"""Application configuration management"""

from functools import lru_cache
from typing import Optional

from pydantic import Field, PostgresDsn, RedisDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings"""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # Application
    APP_NAME: str = "Distributed Job Processing Platform"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"

    # API
    API_V1_PREFIX: str = "/api/v1"
    SECRET_KEY: str = Field(..., min_length=32)
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # Database
    DATABASE_URL: PostgresDsn
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10
    DB_ECHO: bool = False

    # Redis
    REDIS_URL: RedisDsn
    REDIS_POOL_SIZE: int = 10

    # AWS SQS
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    SQS_QUEUE_URL: Optional[str] = None
    SQS_DLQ_URL: Optional[str] = None
    USE_SQS: bool = False  # Enable SQS when configured

    # Worker Configuration
    WORKER_COUNT: int = 3
    WORKER_PREFETCH_COUNT: int = 5
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_BASE: int = 2
    INITIAL_RETRY_DELAY: int = 1
    DELAYED_JOB_CHECK_INTERVAL: int = 1  # Check delayed queue every 1 second

    # Rate Limiting
    RATE_LIMIT_PER_MINUTE: int = 100

    # Monitoring
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9090

    # WebSocket
    WS_HEARTBEAT_INTERVAL: int = 30

    # Job Configuration
    JOB_TIMEOUT: int = 300  # 5 minutes
    MAX_JOB_AGE_DAYS: int = 30

    # Job Recovery Configuration (PROCESSING jobs)
    JOB_RECOVERY_INTERVAL: int = 60  # Check for stuck jobs every 60 seconds
    JOB_RECOVERY_BATCH_SIZE: int = 10  # Process up to 10 stuck jobs per batch
    JOB_RECOVERY_SAFETY_BUFFER: int = 10  # Grace period beyond timeout (seconds)

    # QUEUED Job Reconciliation Configuration (Step 289D)
    QUEUED_RECONCILIATION_ENABLED: bool = True  # Enable/disable QUEUED reconciliation
    QUEUED_RECONCILIATION_BATCH_SIZE: int = 10  # Process up to 10 QUEUED jobs per batch
    QUEUED_RECONCILIATION_AGE_THRESHOLD_MINUTES: int = 10  # Min age for first recovery
    QUEUED_RECONCILIATION_THROTTLE_HOURS: int = 1  # Min interval between recovery attempts

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"

    @property
    def database_url_sync(self) -> str:
        """Get synchronous database URL for Alembic"""
        return str(self.DATABASE_URL).replace("+asyncpg", "")


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


settings = get_settings()
