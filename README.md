# Distributed Job Processing & Execution Platform

<p align="center">
  <em>A scalable platform designed for production environments for background job processing with fault tolerance, priority queuing, and real-time monitoring</em>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#api-documentation">API</a> •
  <a href="#testing">Testing</a>
</p>

---

## Overview

This platform solves the problem of efficiently processing high volumes of background tasks in distributed systems. When an application receives thousands of concurrent job requests, processing them synchronously creates performance bottlenecks and poor user experience.

**Real-world use cases:**
- **E-commerce**: Processing bulk order confirmations and inventory updates
- **Data Analytics**: Running heavy data processing jobs without blocking API requests
- **Media Processing**: Transcoding videos and images asynchronously
- **Notifications**: Sending emails and push notifications at scale
- **ETL Pipelines**: Extracting, transforming, and loading data across systems

## Features

### Core Capabilities

- ✅ **RESTful API** - Submit and manage jobs through FastAPI endpoints
- ✅ **Priority Queue** - Process critical jobs first (0-10 priority levels)
- ✅ **Horizontal Scaling** - Multiple concurrent workers for parallel processing
- ✅ **Fault Tolerance** - Automatic retry with exponential backoff
- ✅ **Idempotency** - Prevent duplicate job execution with idempotency keys
- ✅ **Dead Letter Queue** - Isolate permanently failed jobs for manual review
- ✅ **Job Timeout Protection** - Automatic cancellation of long-running jobs
- ✅ **Real-time Updates** - WebSocket support for live job status notifications
- ✅ **Comprehensive Monitoring** - Prometheus metrics and monitoring endpoints
- 🚧 **Rate Limiting** - (Planned) Protect API from abuse
- 🚧 **Authentication** - (Planned) JWT-based API authentication

### Technical Highlights

- **Async/Await** throughout for maximum performance
- **Type Safety** with Pydantic models and Python type hints
- **Database Pooling** for optimized PostgreSQL connections
- **Structured Logging** with contextual information
- **Docker Ready** with multi-service orchestration
- **Cloud Native** with AWS infrastructure support (SQS integration ready, not tested in production)
- **Test Framework** with pytest and pytest-asyncio

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Backend** | Python 3.11+ | Core application language |
| **API Framework** | FastAPI 0.115+ | High-performance async REST API |
| **Database** | PostgreSQL 15 | Job persistence and history |
| **ORM** | SQLAlchemy 2.0 (async) | Database operations |
| **Queue** | Redis 7 / AWS SQS | Job queue with priority support |
| **Cache** | Redis | Caching and session management |
| **Monitoring** | Prometheus | Metrics collection and alerting |
| **Containerization** | Docker & Docker Compose | Local development and deployment |
| **Cloud** | AWS (ECS, RDS, ElastiCache, SQS) | Production infrastructure |
| **Testing** | Pytest, pytest-asyncio | Automated testing |
| **CI/CD** | GitHub Actions | Continuous integration and deployment |

## Architecture

```mermaid
graph TB
    Client[Client Application]
    API[FastAPI Server<br/>Authentication · Validation · Rate Limiting]
    Queue[Redis / AWS SQS<br/>Priority Queue]
    DB[(PostgreSQL<br/>Job Records · History)]
    W1[Worker 1]
    W2[Worker 2]
    W3[Worker 3]
    Monitor[Monitoring Dashboard<br/>Prometheus · WebSocket]
    
    Client -->|Submit Job| API
    API -->|Enqueue| Queue
    API -->|Save| DB
    Queue -->|Dequeue| W1
    Queue -->|Dequeue| W2
    Queue -->|Dequeue| W3
    W1 -->|Update Status| DB
    W2 -->|Update Status| DB
    W3 -->|Update Status| DB
    DB -->|Metrics| Monitor
    API -->|Real-time Updates| Monitor
    Monitor -->|WebSocket| Client
    
    style API fill:#4CAF50
    style Queue fill:#2196F3
    style DB fill:#FF9800
    style W1 fill:#9C27B0
    style W2 fill:#9C27B0
    style W3 fill:#9C27B0
    style Monitor fill:#F44336
```

### System Flow

1. **Job Submission**
   - Client sends POST request to `/api/v1/jobs/`
   - API validates request and checks idempotency
   - Job record created in PostgreSQL
   - Job message enqueued in Redis/SQS with priority
   - Job ID returned to client

2. **Job Processing**
   - Worker continuously polls queue for jobs
   - Worker dequeues next highest-priority job
   - Updates job status to "processing" in database
   - Executes job with timeout protection
   - Updates final status (completed/failed)
   - Sends real-time update via WebSocket

3. **Failure Handling**
   - Job fails → Check retry count
   - If under max retries → Re-enqueue with exponential backoff
   - If max retries exceeded → Move to dead letter queue
   - All failures logged with error details

4. **Worker Health**
   - Workers send periodic heartbeats
   - Monitoring marks stale workers inactive
   - Jobs from crashed workers can be reassigned

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
│   │   ├── database.py      # Database connection
│   │   ├── redis_client.py  # Redis client
│   │   ├── metrics.py       # Prometheus metrics
│   │   └── logging_config.py # Structured logging
│   ├── db/                  # Database layer
│   │   └── models.py        # SQLAlchemy models
│   ├── models/              # Pydantic schemas
│   │   ├── job.py           # Job request/response models
│   │   └── worker.py        # Worker models
│   ├── services/            # Business logic
│   │   ├── job_service.py   # Job management
│   │   ├── queue_service.py # Queue operations
│   │   └── worker_service.py # Worker management
│   ├── worker/              # Worker implementation
│   │   ├── worker.py        # Worker process
│   │   └── job_executor.py  # Job type handlers
│   └── main.py              # Application entry point
├── tests/                   # Test suite
│   ├── conftest.py          # Pytest fixtures
│   └── test_jobs.py         # Job endpoint tests
├── docker/                  # Docker configurations
│   ├── Dockerfile.api       # API server image
│   └── Dockerfile.worker    # Worker image
├── alembic/                 # Database migrations
├── scripts/                 # Deployment scripts
├── monitoring/              # Monitoring configuration
├── .github/workflows/       # CI/CD pipelines
├── docker-compose.yml       # Local development setup
├── requirements.txt         # Python dependencies
├── .env.example             # Environment template
└── README.md                # This file
```

## Installation

### Prerequisites

- **Python 3.11 or higher** (tested with Python 3.11, compatible with Python 3.13)
- **Docker and Docker Compose** (for infrastructure)
- **Git**

### Quick Start (Docker - Recommended)

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
   docker-compose up -d
   ```
   
   This starts:
   - PostgreSQL (port 5432)
   - Redis (port 6379)
   - API Server (port 8000)
   - 3 Worker instances
   - Prometheus (port 9090)

4. **Verify services are running**
   ```bash
   docker-compose ps
   curl http://localhost:8000/health
   ```

5. **Access the application**
   - **API Docs**: http://localhost:8000/docs
   - **Health Check**: http://localhost:8000/health
   - **Metrics**: http://localhost:8000/metrics
   - **Prometheus**: http://localhost:9090

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
   docker-compose up -d postgres redis
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

7. **Start workers** (in separate terminals)
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

# Rate Limiting
RATE_LIMIT_PER_MINUTE=100

# AWS (Optional - for production)
USE_SQS=False
AWS_REGION=us-east-1
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/account/queue
```

## Usage

### Submitting Jobs

#### Create a Job

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
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "job_type": "data_processing",
  "payload": {"data": [1, 2, 3, 4, 5]},
  "priority": 5,
  "status": "queued",
  "created_at": "2024-01-15T10:30:00Z",
  "max_retries": 3,
  "retry_count": 0
}
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

Submitting the same `idempotency_key` again returns the original job.

### Checking Job Status

```bash
# Get specific job
curl http://localhost:8000/api/v1/jobs/123e4567-e89b-12d3-a456-426614174000

# List all jobs
curl http://localhost:8000/api/v1/jobs/

# Filter by status
curl "http://localhost:8000/api/v1/jobs/?status=completed"

# Filter by type and paginate
curl "http://localhost:8000/api/v1/jobs/?job_type=data_processing&page=1&page_size=20"
```

### Retrying Failed Jobs

```bash
curl -X POST http://localhost:8000/api/v1/jobs/123e4567-e89b-12d3-a456-426614174000/retry
```

### Monitoring

#### Get Job Statistics

```bash
curl http://localhost:8000/api/v1/jobs/stats/overview
```

**Response:**
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

### Run All Tests

```bash
pytest
```

### Run with Coverage

```bash
pytest --cov=app --cov-report=html --cov-report=term
```

View coverage report:
```bash
open htmlcov/index.html
```

### Run Specific Tests

```bash
# Test jobs endpoints
pytest tests/test_jobs.py

# Test with verbose output
pytest -v

# Test specific function
pytest tests/test_jobs.py::test_create_job
```

### Test Configuration

Tests use a separate test database and are fully isolated. Configuration in `tests/conftest.py`:

```python
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobprocessing_test"
```

### Example Test

```python
@pytest.mark.asyncio
async def test_create_job(client: AsyncClient, sample_job_data):
    response = await client.post("/api/v1/jobs/", json=sample_job_data)
    
    assert response.status_code == 201
    data = response.json()
    assert data["job_type"] == sample_job_data["job_type"]
    assert data["status"] == "queued"
```

## Error Handling and Retry Mechanism

### Automatic Retries

Jobs that fail are automatically retried with exponential backoff:

```
Attempt 1: Fails → Wait 1 second → Retry
Attempt 2: Fails → Wait 2 seconds → Retry
Attempt 3: Fails → Wait 4 seconds → Retry
Attempt 4: Fails → Move to Dead Letter Queue
```

**Configuration:**
- `MAX_RETRIES`: Maximum retry attempts (default: 3)
- `RETRY_BACKOFF_BASE`: Backoff multiplier (default: 2)
- `INITIAL_RETRY_DELAY`: Initial delay in seconds (default: 1)

Formula: `delay = INITIAL_RETRY_DELAY * (RETRY_BACKOFF_BASE ^ retry_count)`

### Job Timeouts

Jobs that exceed their timeout are cancelled and retried:

```python
{
  "job_type": "long_running_task",
  "timeout": 300  # 5 minutes
}
```

### Dead Letter Queue

Jobs that fail permanently (exceeded max retries) are moved to the dead letter queue for manual review:

```bash
# List dead letter jobs
curl "http://localhost:8000/api/v1/jobs/?status=dead_letter"
```

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
- `api_requests_total` - API request count by endpoint
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
docker-compose logs -f

# Specific service
docker-compose logs -f api
docker-compose logs -f worker1

# Last 100 lines
docker-compose logs --tail=100 api
```

Logs are structured JSON for easy parsing:

```json
{
  "event": "job_created",
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "job_type": "data_processing",
  "priority": 5,
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "info"
}
```

## Deployment

### Docker Compose (Development)

Already covered in [Installation](#installation).

### AWS ECS (Production)

1. **Set up AWS resources**
   ```bash
   ./scripts/setup_aws.sh
   ```

2. **Build and push Docker images**
   ```bash
   ./scripts/deploy.sh
   ```

3. **Update environment variables** in AWS Systems Manager Parameter Store or ECS Task Definitions

4. **Deploy services**
   ```bash
   aws ecs update-service --cluster job-processing --service api-service --force-new-deployment
   aws ecs update-service --cluster job-processing --service worker-service --force-new-deployment
   ```

### Environment-Specific Configuration

Create environment-specific configs:

- `.env.development` - Local development
- `.env.staging` - Staging environment
- `.env.production` - Production (use AWS Parameter Store instead)

## Troubleshooting

### Common Issues

#### Docker containers won't start

```bash
# Check logs
docker-compose logs

# Rebuild images
docker-compose down
docker-compose up -d --build
```

#### Database connection errors

```bash
# Verify PostgreSQL is running
docker-compose ps postgres

# Check connection
docker-compose exec postgres psql -U postgres -c "SELECT version();"

# View PostgreSQL logs
docker-compose logs postgres
```

#### Workers not processing jobs

```bash
# Check worker logs
docker-compose logs worker1 worker2 worker3

# Verify Redis connection
docker-compose exec redis redis-cli ping

# Check queue depth
docker-compose exec redis redis-cli ZCARD job_queue
```

#### Port conflicts

If ports 8000, 5432, or 6379 are in use:

```bash
# Find process using port
lsof -i :8000

# Kill process
kill -9 <PID>

# Or change ports in docker-compose.yml
```

#### Import errors after dependency installation

```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install --force-reinstall -r requirements.txt

# Verify installation
pip list | grep fastapi
```

### Getting Help

- **Documentation**: Review this README and [ARCHITECTURE.md](ARCHITECTURE.md)
- **Issues**: Open an issue on [GitHub](https://github.com/Latha2308/distributed-job-processing-platform/issues)
- **Logs**: Check application logs for detailed error messages
- **Tests**: Run tests to verify functionality: `pytest -v`

## Performance

### Performance Characteristics

Performance characteristics (estimated based on design, not formally benchmarked):

- **Job Submission**: Sub-second per request
- **Job Processing**: Varies by job type (2-5 seconds average for sample jobs)
- **Database Queries**: Optimized with connection pooling and async I/O
- **API Response Time**: Fast async I/O for concurrent request handling
- **Throughput**: Scales with number of workers deployed

*Note: Actual performance depends on hardware, job complexity, and system load. Formal benchmarking recommended before production deployment.*

### Scaling Recommendations

| Load | API Instances | Workers | Database | Redis |
|------|---------------|---------|----------|-------|
| Low (< 1K jobs/min) | 1 | 3 | db.t3.small | cache.t3.micro |
| Medium (1K-10K jobs/min) | 2-3 | 10-20 | db.t3.medium | cache.t3.small |
| High (10K-100K jobs/min) | 5-10 | 50-100 | db.r5.large | cache.r5.large |
| Very High (> 100K jobs/min) | 10+ | 100+ | db.r5.xlarge+ | cache.r5.xlarge+ |

## Project Status

This project demonstrates distributed systems architecture and microservices design patterns. The core functionality is implemented and working, with some features planned for future releases.

**✅ Implemented and Working:**
- Job submission and processing via REST API
- Priority-based queue scheduling
- Retry mechanism with exponential backoff
- Worker scaling and concurrent processing
- Database persistence with PostgreSQL
- Prometheus metrics collection
- WebSocket real-time updates
- Idempotency support
- Dead letter queue for failed jobs
- Job timeout protection
- Structured logging

**🚧 Planned Features:**
- Rate limiting implementation
- Authentication and authorization (JWT)
- Enhanced worker recovery with automatic job reassignment
- Production load testing
- AWS SQS production deployment validation
- Comprehensive integration test suite

**📋 Recommended Before Production Deployment:**
1. Implement authentication layer for API security
2. Add rate limiting to prevent abuse
3. Complete comprehensive integration and load tests
4. Conduct security audit
5. Set up production monitoring and alerting
6. Perform load testing and capacity planning
7. Configure production-grade database backups
8. Set up log aggregation and analysis

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

Built to demonstrate:
- Distributed systems architecture
- Microservices design patterns
- Fault-tolerant system design
- Production-ready Python development
- Cloud-native application development

Special thanks to the open-source community for the excellent tools and libraries that made this project possible.

---

<p align="center">
  <strong>⭐ If you find this project useful, please consider giving it a star on GitHub! ⭐</strong>
</p>
