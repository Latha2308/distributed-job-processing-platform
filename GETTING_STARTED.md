# Getting Started Guide

This guide will help you set up and run the Distributed Job Processing Platform on your local machine.

## Prerequisites

Ensure you have the following installed:
- Python 3.11 or higher
- Docker and Docker Compose
- Git

## Quick Start (5 minutes)

### 1. Clone the repository
```bash
git clone https://github.com/Latha2308/distributed-job-processing-platform.git
cd distributed-job-processing-platform
```

### 2. Set up environment
```bash
cp .env.example .env
```

Edit `.env` and set the `SECRET_KEY`:
```bash
SECRET_KEY=your-super-secret-key-change-this-please
```

### 3. Start infrastructure with Docker
```bash
docker-compose up -d
```

This starts:
- PostgreSQL database
- Redis cache
- API server (port 8000)
- 3 Worker instances
- Prometheus (port 9090)

### 4. Verify everything is running
```bash
docker-compose ps
```

All services should show "Up" status.

### 5. Access the application

- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Metrics**: http://localhost:8000/metrics
- **Prometheus**: http://localhost:9090

## Testing the System

### Submit a job via API

```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "data_processing",
    "payload": {"data": [1, 2, 3, 4, 5]},
    "priority": 5
  }'
```

Response:
```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "job_type": "data_processing",
  "status": "queued",
  ...
}
```

### Check job status

```bash
curl http://localhost:8000/api/v1/jobs/123e4567-e89b-12d3-a456-426614174000
```

### View all jobs

```bash
curl http://localhost:8000/api/v1/jobs/
```

### Get job statistics

```bash
curl http://localhost:8000/api/v1/jobs/stats/overview
```

### Check worker status

```bash
curl http://localhost:8000/api/v1/workers/
```

### View monitoring dashboard

```bash
curl http://localhost:8000/api/v1/monitoring/dashboard
```

## Development Setup (Manual)

If you prefer to run services individually:

### 1. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start PostgreSQL and Redis
```bash
docker-compose up -d postgres redis
```

### 4. Run database migrations
```bash
alembic upgrade head
```

### 5. Start API server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 6. Start workers (in separate terminals)
```bash
# Terminal 1
python -m app.worker.worker --worker-id worker-1

# Terminal 2
python -m app.worker.worker --worker-id worker-2

# Terminal 3
python -m app.worker.worker --worker-id worker-3
```

## Using the Makefile

We provide a Makefile for common tasks:

```bash
# Install dependencies
make install

# Set up development environment
make dev-setup

# Run tests
make test

# Run linters
make lint

# Format code
make format

# Start API server
make run-api

# Start worker
make run-worker

# Start Docker services
make docker-up

# Stop Docker services
make docker-down

# View Docker logs
make docker-logs

# Run database migrations
make migrate

# Clean temporary files
make clean
```

## Available Job Types

The platform supports the following job types out of the box:

1. **data_processing**
   ```json
   {
     "job_type": "data_processing",
     "payload": {"data": [1, 2, 3]}
   }
   ```

2. **email_notification**
   ```json
   {
     "job_type": "email_notification",
     "payload": {
       "recipient": "user@example.com",
       "subject": "Hello"
     }
   }
   ```

3. **report_generation**
   ```json
   {
     "job_type": "report_generation",
     "payload": {
       "report_type": "sales",
       "start_date": "2024-01-01",
       "end_date": "2024-01-31"
     }
   }
   ```

4. **data_sync**
   ```json
   {
     "job_type": "data_sync",
     "payload": {
       "source": "database_a",
       "destination": "database_b"
     }
   }
   ```

5. **image_processing**
   ```json
   {
     "job_type": "image_processing",
     "payload": {
       "image_url": "https://example.com/image.jpg",
       "operations": ["resize", "watermark"]
     }
   }
   ```

6. **backup**
   ```json
   {
     "job_type": "backup",
     "payload": {
       "resource": "database",
       "backup_location": "s3://backups/"
     }
   }
   ```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_jobs.py

# Run tests in verbose mode
pytest -v
```

View coverage report:
```bash
open htmlcov/index.html
```

## Monitoring

### Prometheus Metrics

Visit http://localhost:9090 to access Prometheus UI.

Example queries:
- `job_submissions_total` - Total job submissions
- `job_completions_total` - Total job completions
- `rate(job_completions_total[5m])` - Job completion rate
- `job_processing_duration_seconds_bucket` - Processing time distribution
- `active_workers` - Number of active workers

### Application Logs

View logs from all services:
```bash
docker-compose logs -f
```

View logs from specific service:
```bash
docker-compose logs -f api
docker-compose logs -f worker1
```

## WebSocket Real-Time Updates

Connect to WebSocket for real-time job updates:

```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/jobs/JOB_ID');

ws.onmessage = (event) => {
  const update = JSON.parse(event.data);
  console.log('Job update:', update);
};

// Send ping to keep connection alive
setInterval(() => ws.send('ping'), 30000);
```

## Troubleshooting

### Containers won't start

```bash
# Check Docker is running
docker info

# Remove old containers and volumes
docker-compose down -v

# Rebuild images
docker-compose up -d --build
```

### Database connection errors

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# View PostgreSQL logs
docker-compose logs postgres

# Connect to database manually
docker-compose exec postgres psql -U postgres -d jobprocessing
```

### Workers not processing jobs

```bash
# Check worker logs
docker-compose logs worker1 worker2 worker3

# Check Redis queue
docker-compose exec redis redis-cli
> ZRANGE job_queue 0 -1
```

### Port already in use

If port 8000 or 5432 is already in use:

```bash
# Find process using port
lsof -i :8000

# Kill the process
kill -9 <PID>

# Or change port in docker-compose.yml
```

## Next Steps

1. **Read the Architecture**: Check out [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system design
2. **Explore the API**: Visit http://localhost:8000/docs for interactive API documentation
3. **Add Custom Job Types**: See [CONTRIBUTING.md](CONTRIBUTING.md) for how to add new job handlers
4. **Deploy to AWS**: Follow [deployment instructions](#) for production deployment

## Getting Help

- **Issues**: Open an issue on [GitHub](https://github.com/Latha2308/distributed-job-processing-platform/issues)
- **Documentation**: Check [README.md](README.md) for more details
- **Architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md) for system design

## Common Commands Cheat Sheet

```bash
# Development
make install          # Install dependencies
make test            # Run tests
make lint            # Check code quality
make format          # Format code

# Running
make docker-up       # Start all services
make docker-down     # Stop all services
make docker-logs     # View logs
make run-api         # Run API locally
make run-worker      # Run worker locally

# Database
make migrate         # Apply migrations
make migration       # Create new migration

# Cleanup
make clean           # Remove temporary files
docker-compose down -v  # Remove everything including volumes
```

Happy coding! 🚀
