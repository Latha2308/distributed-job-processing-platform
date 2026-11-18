# Distributed Job Processing & Execution Platform

A locally tested distributed job processing platform demonstrating transactional outbox pattern, priority queuing, and distributed worker coordination. Built to showcase distributed systems architecture and microservices design patterns.

<p align="center">
  <a href="#overview">Overview</a> •
  <a href="#features">Features</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#usage">Usage</a> •
  <a href="#testing">Testing</a>
</p>

---

## Overview

### The Problem

Modern applications need to process background tasks asynchronously without blocking user requests. A common approach is to save a job record to the database, then enqueue a message to a broker like Redis. However, this creates a reliability problem:

- If Redis fails after the database commit, the job is saved but never executed
- If the database fails after the Redis enqueue, the job may execute against inconsistent state
- Network partitions between the API and Redis can cause similar issues

### The Solution: Transactional Outbox Pattern

This platform uses the **Transactional Outbox Pattern** to reduce the risk of lost jobs by persisting the delivery intent in PostgreSQL:

1. **Atomic Write**: Job record and outbox event are committed in a single database transaction
2. **Async Dispatch**: A standalone dispatcher polls the outbox table and publishes events to Redis
3. **Retry Logic**: Failed publications are retried with error tracking
4. **Recovery**: Unpublished events survive crashes and can be retried manually if needed

**Important**: This provides **at-least-once delivery**, meaning jobs may be delivered multiple times in failure scenarios. Applications must implement idempotency to handle duplicate executions safely.

### Real-World Use Cases

- **E-commerce**: Process order confirmations with audit trail and failure recovery
- **Data Pipelines**: Run heavy ETL jobs asynchronously without blocking API responses
- **Media Processing**: Transcode videos and images with automatic retry on failure
- **Notifications**: Send emails and push notifications at scale with durable queueing
- **Analytics**: Process events for reporting without risking data loss during outages

### Key Design Decisions

**Why PostgreSQL Outbox Instead of Direct Queueing?**
- **Atomicity**: Job creation and outbox event happen in a single transaction
- **Durability**: Events survive Redis failures, network partitions, and application crashes
- **Auditability**: Complete event history in the database for debugging and compliance
- **Tradeoff**: Adds latency (polling interval + database round-trip) in exchange for durability

**Why Multiple Workers?**
- **Parallel Processing**: Multiple workers process jobs concurrently
- **Fault Isolation**: Worker crashes don't block job submission or other workers
- **Scaling**: Adding workers can improve throughput depending on workload characteristics and infrastructure limits
- **Tradeoff**: More complex orchestration and resource management

## Features

### Core Capabilities (Implemented and Tested)

- **RESTful API** - Submit and manage jobs through FastAPI endpoints
- **Transactional Outbox Pattern** - Persist delivery intent in PostgreSQL for durability
- **Priority Queue** - Process critical jobs first (0-10 priority levels)
- **Distributed Workers** - Multiple concurrent workers for parallel processing
- **Automatic Retry** - Configurable retry attempts with error tracking
- **Idempotency Keys** - Prevent duplicate job creation with client-provided keys
- **Dead Letter Queue** - Isolate exhausted jobs for manual review
- **Job Timeout Protection** - Cancel jobs that exceed configured timeouts
- **Real-time Updates** - WebSocket support for live job status notifications
- **Prometheus Metrics** - Monitor job throughput, latency, and failures
- **Structured Logging** - Contextual JSON logs for debugging and audit

### Planned Features (Not Yet Implemented)

- **Rate Limiting** - Protect API from abuse
- **JWT Authentication** - Secure API access

### Technical Highlights

- **Transactional Outbox** with PostgreSQL FOR UPDATE SKIP LOCKED for concurrent dispatcher instances
- **At-Least-Once Delivery** with idempotency safeguards to handle duplicate execution
- **Async/Await** throughout using Python's asyncio
- **Type Safety** with Pydantic models and Python type hints
- **Database Connection Pooling** for optimized PostgreSQL operations
- **Structured Logging** with contextual information for tracing
- **Docker Compose** orchestration for local development and testing
- **Comprehensive Test Suite** with 60 passing tests (as previously verified)

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Backend** | Python 3.11+ | Core application language |
| **API Framework** | FastAPI 0.115+ | High-performance async REST API |
| **Database** | PostgreSQL 15 | Job persistence and outbox table |
| **ORM** | SQLAlchemy 2.0 (async) | Database operations with async support |
| **Queue** | Redis 7 | Priority queue for job distribution |
| **Monitoring** | Prometheus | Metrics collection |
| **Containerization** | Docker & Docker Compose | Local development and testing |
| **Testing** | Pytest, pytest-asyncio | Automated testing framework |
| **Migrations** | Alembic | Database schema versioning |

**Note on AWS**: The codebase includes AWS SQS integration code, but production deployment to AWS ECS has not been validated.

## Architecture

### System Diagram

```mermaid
graph TB
    Client[Client Application]
    API[FastAPI Server<br/>Job Validation & Creation]
    DB[(PostgreSQL<br/>Jobs + Outbox)]
    Dispatcher[Outbox Dispatcher<br/>Polls & Publishes]
    Queue[Redis Priority Queue<br/>Job Messages]
    W1[Worker 1]
    W2[Worker 2]
    W3[Worker 3]
    Prom[Prometheus<br/>Metrics]
    
    Client -->|POST /jobs| API
    API -->|Atomic Insert| DB
    DB -.->|Poll Outbox| Dispatcher
    Dispatcher -->|Publish Event| Queue
    Dispatcher -->|Mark Published| DB
    Queue -->|Dequeue| W1
    Queue -->|Dequeue| W2
    Queue -->|Dequeue| W3
    W1 -->|Update Status| DB
    W2 -->|Update Status| DB
    W3 -->|Update Status| DB
    API -->|Export| Prom
    W1 -->|Export| Prom
    W2 -->|Export| Prom
    W3 -->|Export| Prom
    Dispatcher -->|Export| Prom
    
    style API fill:#4CAF50
    style Dispatcher fill:#FF9800
    style Queue fill:#2196F3
    style DB fill:#9C27B0
    style W1 fill:#607D8B
    style W2 fill:#607D8B
    style W3 fill:#607D8B
    style Prom fill:#F44336
```

### Data Flow

1. **Job Submission**
   - Client sends POST request to `/api/v1/jobs/`
   - API validates request and checks idempotency key
   - Job record and JobOutbox event committed atomically to PostgreSQL
   - API returns job ID and status "queued" to client

2. **Event Dispatch (Asynchronous)**
   - Dispatcher continuously polls the `job_outbox` table for unpublished events
   - Uses PostgreSQL `FOR UPDATE SKIP LOCKED` to claim events safely
   - Publishes job details to Redis priority queue
   - Marks event as published in the outbox table
   - On failure, increments attempt counter and logs error for retry

3. **Job Processing**
   - Workers poll Redis priority queue (lower priority number = higher precedence)
   - Worker dequeues next job and updates status to "processing" in PostgreSQL
   - Executes job handler with timeout protection
   - Updates status to "completed" or "failed" with result/error details
   - Failed jobs are retried up to `max_retries`; exhausted jobs move to dead letter status

4. **Monitoring & Observability**
   - Prometheus scrapes `/metrics` endpoints from API, dispatcher, and workers
   - WebSocket connections provide real-time job status updates
   - Structured logs capture events, errors, and performance data

### Transactional Outbox Pattern Details

**Why This Pattern?**

Traditional approach (API directly enqueues to Redis):
- If Redis is down during job creation → job saved to DB but never executed
- If DB commit fails after Redis enqueue → job executes against inconsistent state

Outbox pattern approach:
- Job and outbox event are written in a single database transaction (atomic)
- Separate dispatcher process reads outbox and publishes to Redis
- If dispatcher fails, unpublished events remain in the database
- Restoring the dispatcher resumes publishing from where it left off

**Delivery Semantics:**

This implementation provides **at-least-once delivery**:
- Jobs are published at least once to Redis
- In failure scenarios (dispatcher crash after Redis publish but before marking as published), jobs may be published multiple times
- Applications must implement idempotency to handle duplicate executions

**Concurrency Safety:**

- Multiple dispatcher instances can run concurrently
- PostgreSQL `FOR UPDATE SKIP LOCKED` ensures each event is claimed by only one dispatcher
- No distributed locks or coordination required

**Failure Recovery:**

- Events with `attempts < max_attempts` are automatically retried
- Events exceeding `max_attempts` remain in the outbox with error details
- Operators can manually retry exhausted events if needed

## Project Structure

```
distributed-job-processing-platform/
├── app/
│   ├── api/v1/              # API endpoints
│   │   ├── endpoints/
│   │   │   ├── jobs.py      # Job CRUD operations
│   │   │   ├── workers.py   # Worker management
│   │   │   ├── monitoring.py # Health and dashboard
│   │   │   └── websocket.py # Real-time updates
│   │   └── router.py        # API router configuration
│   ├── core/                # Core configuration
│   │   ├── config.py        # Settings management
│   │   ├── database.py      # Database connection pooling
│   │   ├── redis_client.py  # Redis client with connection cleanup
│   │   ├── metrics.py       # Prometheus metrics
│   │   └── logging_config.py # Structured logging
│   ├── db/                  # Database layer
│   │   └── models.py        # SQLAlchemy models (Job, JobOutbox, Worker, etc.)
│   ├── dispatcher/          # Outbox dispatcher
│   │   ├── __init__.py
│   │   └── dispatcher_cli.py # Standalone dispatcher process
│   ├── models/              # Pydantic schemas
│   │   ├── job.py           # Job request/response models
│   │   └── worker.py        # Worker models
│   ├── services/            # Business logic
│   │   ├── job_service.py   # Job management with outbox
│   │   ├── queue_service.py # Queue operations (Redis/SQS)
│   │   ├── worker_service.py # Worker management
│   │   └── outbox_dispatcher.py # Outbox polling and publishing
│   ├── worker/              # Worker implementation
│   │   ├── worker.py        # Worker process
│   │   └── job_executor.py  # Job type handlers
│   └── main.py              # FastAPI application entry point
├── tests/                   # Test suite (60 passing tests)
│   ├── conftest.py          # Pytest fixtures
│   ├── test_jobs.py         # Job endpoint tests
│   ├── test_outbox_dispatcher.py  # Dispatcher unit tests
│   ├── test_job_service_outbox.py # Outbox integration tests
│   ├── test_dispatcher_cli.py     # CLI lifecycle tests
│   └── test_redis_cleanup.py      # Redis connection cleanup tests
├── docker/                  # Docker configurations
│   ├── Dockerfile.api       # API server image
│   └── Dockerfile.worker    # Worker image (used by workers and dispatcher)
├── alembic/                 # Database migrations
│   └── versions/
│       └── *_add_job_outbox_table.py  # Outbox table migration
├── scripts/                 # Deployment and utility scripts
├── monitoring/              # Monitoring configuration
│   └── prometheus.yml       # Prometheus scrape config
├── docker-compose.yml       # Local development (8 services)
├── requirements.txt         # Python dependencies
├── .env.example             # Environment template
└── README.md                # This file
```

## Installation

### Prerequisites

- **Python 3.11 or higher** (tested with Python 3.11, compatible with Python 3.13)
- **Docker and Docker Compose** (for infrastructure and local testing)
- **PostgreSQL 15** (if running manually)
- **Redis 7** (if running manually)
- **Git**

### Quick Start (Docker Compose - Recommended)

1. **Clone the repository**
   ```bash
   git clone https://github.com/Latha2308/distributed-job-processing-platform.git
   cd distributed-job-processing-platform
   ```

2. **Configure environment**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` and set a secure SECRET_KEY (minimum 32 characters):
   ```bash
   SECRET_KEY=your-super-secret-key-must-be-at-least-32-characters-long
   ```

3. **Start all services**
   ```bash
   docker compose up -d
   ```
   
   This starts **8 services**:
   - **postgres**: PostgreSQL database (port 5432)
   - **redis**: Redis queue and cache (port 6379)
   - **api**: FastAPI server (port 8000)
   - **dispatcher**: Outbox dispatcher process
   - **worker1, worker2, worker3**: Three worker instances
   - **prometheus**: Metrics collection (port 9090)

4. **Verify services are running**
   ```bash
   docker compose ps
   curl http://localhost:8000/health
   ```

5. **View logs**
   ```bash
   # All services
   docker compose logs -f

   # Specific service
   docker compose logs -f dispatcher
   docker compose logs -f worker1
   ```

6. **Access the application**
   - **API Documentation**: http://localhost:8000/docs
   - **Health Check**: http://localhost:8000/health
   - **Metrics**: http://localhost:8000/metrics
   - **Prometheus**: http://localhost:9090

7. **Run database migrations**
   ```bash
   docker compose exec api alembic upgrade head
   ```

### Manual Setup (Development)

For local development without Docker:

1. **Create virtual environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start infrastructure services**
   ```bash
   docker compose up -d postgres redis prometheus
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

6. **Start API server**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

7. **Start outbox dispatcher** (in separate terminal)
   ```bash
   python -m app.dispatcher.dispatcher_cli \
     --dispatcher-id dispatcher-01 \
     --batch-size 10 \
     --poll-interval 5.0 \
     --max-attempts 5 \
     --shutdown-timeout 30.0 \
     --log-level INFO
   ```

8. **Start workers** (in separate terminals)
   ```bash
   # Terminal 1
   python -m app.worker.worker --worker-id worker-1
   
   # Terminal 2
   python -m app.worker.worker --worker-id worker-2
   
   # Terminal 3
   python -m app.worker.worker --worker-id worker-3
   ```

### Environment Variables

Key configuration options in `.env`:

```bash
# Application
SECRET_KEY=your-secret-key-min-32-characters  # REQUIRED
DEBUG=True
ENVIRONMENT=development

# Database
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing

# Redis
REDIS_URL=redis://localhost:6379/0

# Workers
MAX_RETRIES=3
RETRY_BACKOFF_BASE=2
JOB_TIMEOUT=300

# Rate Limiting (planned feature)
RATE_LIMIT_PER_MINUTE=100

# AWS (Optional - not production-validated)
USE_SQS=False
AWS_REGION=us-east-1
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/account/queue
```

## Usage

### End-to-End Example

Here's a complete example demonstrating job submission, status checking, and the full processing flow:

#### 1. Create a Job

```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "data_processing",
    "payload": {"data": [1, 2, 3, 4, 5]},
    "priority": 5,
    "max_retries": 3,
    "timeout": 300
  }'
```

**Response:**
```json
{
  "id": "a3b4c5d6-e7f8-9012-3456-789abcdef012",
  "job_type": "data_processing",
  "payload": {"data": [1, 2, 3, 4, 5]},
  "priority": 5,
  "status": "queued",
  "created_at": "2024-01-15T10:30:00Z",
  "max_retries": 3,
  "retry_count": 0,
  "timeout": 300
}
```

#### 2. Check Job Status

```bash
curl http://localhost:8000/api/v1/jobs/a3b4c5d6-e7f8-9012-3456-789abcdef012
```

**Example Response (Processing):**
```json
{
  "id": "a3b4c5d6-e7f8-9012-3456-789abcdef012",
  "status": "processing",
  "updated_at": "2024-01-15T10:30:02Z"
}
```

**Example Response (Completed):**
```json
{
  "id": "a3b4c5d6-e7f8-9012-3456-789abcdef012",
  "status": "completed",
  "result": {
    "status": "success",
    "processed_count": 5,
    "message": "Processed 5 items"
  },
  "completed_at": "2024-01-15T10:30:05Z"
}
```

#### 3. Behind the Scenes

This is what happens internally:

1. **API** receives POST, validates request
2. **API** commits Job + JobOutbox to PostgreSQL atomically (transaction)
3. **API** returns job ID immediately (status: "queued")
4. **Dispatcher** polls outbox table, finds new event
5. **Dispatcher** publishes job to Redis priority queue
6. **Dispatcher** marks outbox event as published
7. **Worker** dequeues job from Redis
8. **Worker** updates status to "processing" in PostgreSQL
9. **Worker** executes job handler (processes data)
10. **Worker** updates status to "completed" with results

**Docker Compose Integration Test Result:**
In local testing, worker-2 successfully completed a data_processing job containing five items, validating the complete outbox → dispatcher → Redis → worker flow.

### Submitting Jobs

#### Create a Job with Priority

```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "data_processing",
    "payload": {"data": [1, 2, 3, 4, 5]},
    "priority": 5,
    "max_retries": 3,
    "timeout": 300
  }'
```

#### Create Job with Idempotency

```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "email_notification",
    "payload": {
      "recipient": "user@example.com",
      "subject": "Welcome"
    },
    "idempotency_key": "unique-key-123",
    "priority": 1
  }'
```

Submitting the same `idempotency_key` again returns the original job without creating a new outbox event.

### Checking Job Status

```bash
# Get specific job
curl http://localhost:8000/api/v1/jobs/a3b4c5d6-e7f8-9012-3456-789abcdef012

# List all jobs
curl http://localhost:8000/api/v1/jobs/

# Filter by status
curl "http://localhost:8000/api/v1/jobs/?status=completed"

# Filter by type and paginate
curl "http://localhost:8000/api/v1/jobs/?job_type=data_processing&page=1&page_size=20"
```

### Retrying Failed Jobs

```bash
curl -X POST http://localhost:8000/api/v1/jobs/a3b4c5d6-e7f8-9012-3456-789abcdef012/retry
```

### Monitoring

#### Get Job Statistics

```bash
curl http://localhost:8000/api/v1/jobs/stats/overview
```

**Example Response:**
```json
{
  "total_jobs": 1000,
  "pending": 10,
  "queued": 50,
  "processing": 5,
  "completed": 900,
  "failed": 30,
  "dead_letter": 5,
  "avg_processing_duration": 2.5,
  "success_rate": 96.77
}
```

#### Get Worker Status

```bash
curl http://localhost:8000/api/v1/workers/
```

#### Get Dashboard Data

```bash
curl http://localhost:8000/api/v1/monitoring/dashboard
```

### Supported Job Types

The platform comes with built-in handlers for:

1. **`data_processing`** - Generic data processing tasks
   ```json
   {"job_type": "data_processing", "payload": {"data": [...]}}
   ```

2. **`email_notification`** - Send email notifications
   ```json
   {"job_type": "email_notification", "payload": {"recipient": "user@example.com", "subject": "Hello"}}
   ```

3. **`report_generation`** - Generate reports
   ```json
   {"job_type": "report_generation", "payload": {"report_type": "sales", "start_date": "2024-01-01"}}
   ```

4. **`data_sync`** - Synchronize data between systems
   ```json
   {"job_type": "data_sync", "payload": {"source": "db_a", "destination": "db_b"}}
   ```

5. **`image_processing`** - Process images
   ```json
   {"job_type": "image_processing", "payload": {"image_url": "...", "operations": ["resize"]}}
   ```

6. **`backup`** - Run backup operations
   ```json
   {"job_type": "backup", "payload": {"resource": "database", "backup_location": "s3://..."}}
   ```

**Adding custom job types:**

Edit `app/worker/job_executor.py` and add your handler:

```python
async def _execute_your_custom_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
    # Your implementation
    return {"status": "success", "result": "..."}

# Register in __init__
self.handlers = {
    ...
    "your_custom_job": self._execute_your_custom_job,
}
```

### Real-Time Updates with WebSocket

Connect to WebSocket for live job status updates:

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/jobs/JOB_ID');

ws.onmessage = (event) => {
  const update = JSON.parse(event.data);
  console.log('Job status:', update.status);
};

// Keep connection alive
setInterval(() => ws.send('ping'), 30000);

// Refresh job status manually
ws.send('refresh');
```

## API Documentation

### Interactive API Documentation

Once the application is running, access the interactive documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Key Endpoints

#### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/jobs/` | Create a new job |
| GET | `/api/v1/jobs/{job_id}` | Get job by ID |
| GET | `/api/v1/jobs/` | List jobs (with filters) |
| PATCH | `/api/v1/jobs/{job_id}` | Update job status |
| POST | `/api/v1/jobs/{job_id}/retry` | Retry a failed job |
| GET | `/api/v1/jobs/stats/overview` | Get job statistics |
| GET | `/api/v1/jobs/metrics/by-type` | Get metrics by job type |

#### Workers

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/workers/` | List all workers |
| GET | `/api/v1/workers/{worker_id}` | Get worker by ID |
| GET | `/api/v1/workers/metrics/overview` | Get worker metrics |
| POST | `/api/v1/workers/cleanup` | Mark stale workers inactive |

#### Monitoring

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/monitoring/dashboard` | Get dashboard data |
| GET | `/api/v1/monitoring/health` | System health check |
| GET | `/api/v1/monitoring/queue` | Queue status |
| GET | `/metrics` | Prometheus metrics |

#### WebSocket

| Endpoint | Description |
|----------|-------------|
| WS `/api/v1/ws/jobs/{job_id}` | Real-time job updates |
| WS `/api/v1/ws/dashboard` | Real-time dashboard updates |

## Testing

### Test Suite Overview

The project includes comprehensive unit and integration tests covering the outbox pattern, dispatcher lifecycle, worker coordination, and API endpoints.

**Test Results (as previously verified before final documentation updates):**
- **60 passing tests** across multiple test modules
- **100% pass rate** with proper database isolation
- **MyPy**: 0 type errors
- **Black**: Code formatted consistently
- **Flake8**: 0 linting issues

**Note**: The 60 test count reflects results from an earlier verification run. The test suite includes comprehensive coverage of the outbox pattern, dispatcher lifecycle, API endpoints, and worker coordination.

**Test Categories:**

1. **Outbox Dispatcher Tests** (`test_outbox_dispatcher.py`)
   - Batch processing with PostgreSQL row locking
   - Retry logic and max attempts handling
   - Concurrent dispatcher coordination
   - Manual recovery for exhausted events

2. **Job Service Integration** (`test_job_service_outbox.py`)
   - Atomic job + outbox creation
   - Idempotency key handling
   - Priority preservation
   - Transaction rollback behavior

3. **Dispatcher CLI Tests** (`test_dispatcher_cli.py`)
   - Process lifecycle management
   - Signal handling (SIGINT/SIGTERM)
   - Graceful shutdown with timeout
   - Error recovery

4. **Redis Cleanup Tests** (`test_redis_cleanup.py`)
   - Connection pool cleanup
   - Idempotent close operations
   - Error handling during shutdown

5. **API Endpoint Tests** (existing test suite)
   - Job creation and retrieval
   - Status updates
   - Pagination and filtering
   - Error handling

### Running Tests

#### Run All Tests

```bash
pytest
```

#### Run with Coverage

```bash
pytest --cov=app --cov-report=html --cov-report=term
```

View coverage report:
```bash
open htmlcov/index.html
```

#### Run Specific Test Modules

```bash
# Outbox dispatcher tests
pytest tests/test_outbox_dispatcher.py -v

# Job service integration tests
pytest tests/test_job_service_outbox.py -v

# Dispatcher CLI tests
pytest tests/test_dispatcher_cli.py -v

# All tests with verbose output
pytest -v
```

### Test Configuration

Tests use a separate test database with full isolation:

```python
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
```

Key testing patterns:
- **Database fixtures**: Clean database state for each test
- **Mock dependencies**: Isolate units under test
- **Async test support**: Full pytest-asyncio integration
- **Type safety**: Tests verified with MyPy

### Docker Integration Test

A successful end-to-end integration test was conducted using Docker Compose:

**Test Scenario:**
1. Started all 8 services (postgres, redis, api, dispatcher, worker1-3, prometheus)
2. Submitted a `data_processing` job with 5 items via API
3. Verified job + outbox event committed atomically to PostgreSQL
4. Observed dispatcher poll and publish event to Redis
5. Worker-2 dequeued and successfully completed the job
6. Verified final status and results in PostgreSQL

**Result:** Full transactional outbox → dispatcher → Redis → worker flow validated.

## Failure Handling and Recovery

### Automatic Retries

Jobs that fail during execution are automatically retried based on the `max_retries` configuration:

```json
{
  "job_type": "data_processing",
  "payload": {...},
  "max_retries": 3
}
```

**Retry Behavior:**
- Each failure increments `retry_count`
- Jobs are re-enqueued to Redis for another attempt
- After exhausting `max_retries`, jobs move to `dead_letter` status
- Error details are logged for debugging

**Configuration:**
- `MAX_RETRIES`: Maximum retry attempts per job (default: 3)
- `RETRY_BACKOFF_BASE`: Backoff multiplier for worker sleep between attempts (default: 2)
- `INITIAL_RETRY_DELAY`: Initial delay in seconds (default: 1)

### Outbox Dispatcher Retries

The outbox dispatcher has separate retry logic for publishing events:

- **max_attempts**: Maximum publication attempts for an outbox event (default: 5)
- **On failure**: Increments `attempts` counter and logs error
- **Exhausted events**: Events with `attempts >= max_attempts` are excluded from automatic processing
- **Manual recovery**: Operators can retry exhausted events using `process_single_event(event_id, bypass_max_attempts=True)`

### Job Timeouts

Jobs that exceed their configured timeout are cancelled:

```python
{
  "job_type": "long_running_task",
  "timeout": 300  # 5 minutes
}
```

Timeout behavior:
- Worker monitors execution time
- Job cancelled if timeout exceeded
- Status updated to "failed" with timeout error
- Job can be retried if retries remain

### Dead Letter Queue

Jobs that permanently fail are moved to dead letter status for manual review:

```bash
# List dead letter jobs
curl "http://localhost:8000/api/v1/jobs/?status=dead_letter"

# Retry a specific job
curl -X POST http://localhost:8000/api/v1/jobs/{job_id}/retry
```

### Failure Scenarios and Behavior

| Scenario | Behavior |
|----------|----------|
| Redis unavailable during job creation | Job + outbox event saved to PostgreSQL; dispatcher publishes when Redis recovers |
| Dispatcher crashes after publishing to Redis | Outbox event remains unpublished; may cause duplicate publication on restart (at-least-once delivery) |
| Worker crashes during job execution | Job remains in "processing" state; can be reassigned or retried manually |
| Database connection failure | Transaction rolls back; job not created; API returns error to client |
| Job handler raises exception | Job marked as "failed"; retried up to max_retries; moved to dead letter if exhausted |

## Monitoring and Metrics

### Prometheus Metrics

Access Prometheus UI at http://localhost:9090

**Available Metrics:**

- `job_submissions_total` - Total job submissions by type and priority
- `job_completions_total` - Total completions by type and status
- `job_processing_duration_seconds` - Job processing time histogram
- `job_queue_depth` - Current queue depth by priority
- `active_workers` - Number of active workers
- `job_retries_total` - Total retry attempts by type
- `job_failures_total` - Total failures by type and error
- `api_requests_total` - API request count by method, endpoint, and status
- `api_request_duration_seconds` - API response time histogram

**Example Queries:**

```promql
# Job completion rate (per second)
rate(job_completions_total[5m])

# Average processing time
avg(job_processing_duration_seconds)

# Success rate percentage
(sum(job_completions_total{status="completed"}) / sum(job_completions_total)) * 100

# Queue backlog
sum(job_queue_depth)
```

### Application Logs

View logs from all services:

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f api
docker compose logs -f dispatcher
docker compose logs -f worker1

# Last 100 lines
docker compose logs --tail=100 api
```

Logs are structured JSON for easy parsing:

```json
{
  "event": "job_created_with_outbox",
  "job_id": "a3b4c5d6-e7f8-9012-3456-789abcdef012",
  "job_type": "data_processing",
  "priority": 5,
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "info"
}
```

## Deployment

### Local Development (Docker Compose)

The recommended approach for local development and testing:

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Rebuild and restart
docker compose up -d --build

# Remove volumes (clean slate)
docker compose down -v
```

### AWS ECS (Not Production-Validated)

The codebase includes AWS-related configuration, but production deployment has not been validated. If deploying to AWS, consider:

**Infrastructure Components:**
- **Compute**: ECS Fargate or EC2 for API, dispatcher, and workers
- **Database**: RDS PostgreSQL with Multi-AZ for high availability
- **Cache/Queue**: ElastiCache Redis with cluster mode
- **Alternative Queue**: SQS integration code exists but is untested
- **Load Balancer**: Application Load Balancer for API
- **Monitoring**: CloudWatch for logs and metrics

**Deployment Steps (Untested):**
1. Set up VPC, subnets, and security groups
2. Create RDS PostgreSQL instance
3. Create ElastiCache Redis cluster
4. Build and push Docker images to ECR
5. Create ECS task definitions for API, dispatcher, and workers
6. Create ECS services with appropriate scaling policies
7. Configure ALB and target groups
8. Set up CloudWatch alarms and dashboards
9. Run database migrations
10. Test thoroughly before serving production traffic

**Important**: Conduct comprehensive testing in a staging environment before production deployment.

## Troubleshooting

### Common Issues

#### Docker containers won't start

```bash
# Check logs
docker compose logs

# Check service status
docker compose ps

# Rebuild images
docker compose down
docker compose up -d --build
```

#### Database connection errors

```bash
# Verify PostgreSQL is running
docker compose ps postgres

# Check PostgreSQL logs
docker compose logs postgres

# Test connection
docker compose exec postgres psql -U postgres -c "SELECT version();"

# Verify DATABASE_URL in .env matches docker-compose configuration
```

#### Dispatcher not publishing events

```bash
# Check dispatcher logs
docker compose logs dispatcher

# Verify outbox events exist
docker compose exec postgres psql -U postgres -d jobprocessing \
  -c "SELECT * FROM job_outbox WHERE published = false LIMIT 10;"

# Check Redis connectivity
docker compose exec redis redis-cli ping

# Restart dispatcher
docker compose restart dispatcher
```

#### Workers not processing jobs

```bash
# Check worker logs
docker compose logs worker1 worker2 worker3

# Verify Redis queue depth
docker compose exec redis redis-cli ZCARD job_queue

# Check Redis connectivity from worker
docker compose exec worker1 python -c "import redis; r = redis.from_url('redis://redis:6379/0'); print(r.ping())"

# Restart workers
docker compose restart worker1 worker2 worker3
```

#### Port conflicts

If ports 5432, 6379, 8000, or 9090 are already in use:

```bash
# Find process using port
lsof -i :8000

# Kill process
kill -9 <PID>

# Or change ports in docker-compose.yml
```

#### "Event loop is closed" error on shutdown

This was a known issue that has been fixed. Ensure you have the latest code:

```bash
git pull origin main
docker compose down
docker compose up -d --build
```

#### Jobs stuck in "processing" status

This indicates a worker crashed during execution:

```bash
# Check worker logs for errors
docker compose logs worker1 worker2 worker3

# Manually update job status (if needed)
docker compose exec postgres psql -U postgres -d jobprocessing \
  -c "UPDATE jobs SET status = 'failed' WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '10 minutes';"

# Restart workers
docker compose restart worker1 worker2 worker3
```

### Getting Help

- **Documentation**: Review this README
- **Issues**: Open an issue on [GitHub](https://github.com/Latha2308/distributed-job-processing-platform/issues)
- **Logs**: Check application logs for detailed error messages
- **Tests**: Run tests to verify functionality: `pytest -v`

## Performance Considerations

### Observed Behavior (Local Docker Testing)

Based on local Docker Compose testing with the sample workload:

- **Job Submission**: API responds in milliseconds after database commit
- **Event Dispatch**: Dispatcher polls every 5 seconds by default (configurable)
- **Job Processing**: Varies by job type and complexity
- **End-to-End Latency**: Typically a few seconds from submission to completion for simple jobs

**Note**: These observations are from local development testing, not production load testing. Actual performance will vary based on:
- Hardware resources (CPU, memory, disk I/O)
- Database and Redis performance characteristics
- Network latency
- Job handler complexity
- Concurrent load

### Scaling Considerations

The architecture supports horizontal scaling of workers and dispatchers:

**Worker Scaling:**
- Add more worker instances to increase concurrent job processing capacity
- Each worker independently polls Redis and processes jobs
- No coordination overhead between workers

**Dispatcher Scaling:**
- Multiple dispatcher instances can run concurrently
- PostgreSQL `FOR UPDATE SKIP LOCKED` prevents duplicate event processing
- Useful for high-throughput environments or fault tolerance

**Bottlenecks to Consider:**
- PostgreSQL connection pool limits
- Redis memory and throughput
- Dispatcher polling interval vs. event volume
- Job handler execution time

### Illustrative Scaling Estimates

The following table provides **rough estimates** based on design assumptions, not measured benchmarks:

| Scenario | API Instances | Workers | Dispatchers | Database | Redis |
|----------|---------------|---------|-------------|----------|-------|
| Low (< 1K jobs/min) | 1-2 | 3-5 | 1 | db.t3.small | cache.t3.micro |
| Medium (1K-10K jobs/min) | 2-3 | 10-20 | 1-2 | db.t3.medium | cache.t3.small |
| High (10K-100K jobs/min) | 5-10 | 50-100 | 2-5 | db.r5.large | cache.r5.large |

**Important**: These are illustrative estimates. Actual capacity depends on job complexity, infrastructure performance, and system configuration. Conduct load testing with your specific workload before production deployment.

### Recommendations for Production

Before deploying to production at scale:

1. **Load Testing**: Simulate expected workload to identify bottlenecks
2. **Database Tuning**: Optimize PostgreSQL for your workload (connection pools, indexes, query plans)
3. **Redis Sizing**: Ensure Redis can handle queue depth during peak load
4. **Monitoring**: Set up alerts for queue depth, processing latency, and error rates
5. **Capacity Planning**: Measure actual throughput and plan for growth
6. **Dispatcher Tuning**: Adjust `poll_interval` and `batch_size` based on event volume

## Project Status

This project demonstrates distributed systems architecture and microservices design patterns. The implementation has been developed and tested locally using Docker Compose.

### Implemented and Tested

**Core Functionality:**
- Job submission and management via REST API
- Transactional outbox pattern with PostgreSQL
- Standalone outbox dispatcher with polling and retry logic
- Priority-based Redis queue
- Distributed worker coordination
- Automatic retry mechanism for failed jobs
- Dead letter queue for exhausted jobs
- Job timeout protection
- Idempotency support with client-provided keys
- WebSocket real-time status updates
- Prometheus metrics collection
- Structured logging throughout

**Development Infrastructure:**
- Docker Compose orchestration with 8 services
- PostgreSQL with connection pooling
- Redis for queue and caching
- Alembic database migrations
- Comprehensive test suite (60 passing tests as previously verified)

**Successful Integration Testing:**
- End-to-end flow validated with Docker Compose
- Job creation → outbox event → dispatcher → Redis → worker execution
- Worker-2 completed sample data_processing job with 5 items

### Not Production-Validated

The following components exist in the codebase but have **not been validated in production environments**:

- AWS ECS deployment
- AWS RDS and ElastiCache integration
- AWS SQS queue integration
- Production load testing
- High-availability configuration
- Multi-region deployment

### Planned Features (Not Yet Implemented)

- API rate limiting
- JWT-based authentication and authorization
- Enhanced monitoring dashboards
- Automated job reassignment for crashed workers
- Job scheduling and delayed execution enhancements
- Bulk job operations

### Known Limitations

- **At-least-once delivery**: Jobs may be processed multiple times in failure scenarios; applications must handle idempotency
- **Manual worker recovery**: Crashed workers do not automatically reassign in-progress jobs
- **No built-in authentication**: API endpoints are currently unprotected
- **Limited observability**: No centralized log aggregation or APM integration
- **Single database**: No read replicas or database sharding

### Recommendations Before Production Deployment

1. **Security**: Implement authentication, authorization, and API rate limiting
2. **Load Testing**: Conduct thorough load and stress testing with realistic workloads
3. **High Availability**: Set up database replication, Redis clustering, and multi-AZ deployment
4. **Monitoring**: Integrate with APM tools (Datadog, New Relic) and log aggregation (ELK, Splunk)
5. **Disaster Recovery**: Implement automated backups, point-in-time recovery, and failover procedures
6. **Capacity Planning**: Profile actual workload and right-size infrastructure
7. **Security Audit**: Review code for vulnerabilities, secrets management, and compliance requirements
8. **Documentation**: Create runbooks for operations, troubleshooting, and incident response

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Run linters (`black app tests && flake8 app tests`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Latha**
- GitHub: [@Latha2308](https://github.com/Latha2308)
- Repository: [distributed-job-processing-platform](https://github.com/Latha2308/distributed-job-processing-platform)

## Acknowledgments

This project was built to demonstrate:
- Distributed systems architecture with transactional outbox pattern
- Microservices design patterns and inter-service communication
- Fault-tolerant system design with at-least-once delivery semantics
- Production-ready Python development practices
- Cloud-native application development principles

The implementation showcases practical engineering tradeoffs, including the decision to prioritize durability over latency by using a PostgreSQL-backed outbox instead of direct message broker integration.

Special thanks to the open-source community for the excellent tools and libraries that made this project possible.

---

<p align="center">
  <strong>⭐ If you find this project useful for learning distributed systems, please consider giving it a star on GitHub! ⭐</strong>
</p>
