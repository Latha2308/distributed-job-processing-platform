# Quick Start Guide

Get the Distributed Job Processing Platform running in 5 minutes.

## Prerequisites

- Python 3.11+ (you have 3.13.0 ✓)
- Docker and Docker Compose
- Git

## Setup Steps

### 1. Install Dependencies

```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt

# Verify installation
python -c "from app.main import app; print('✓ Installation successful')"
```

### 2. Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env and set SECRET_KEY (minimum 32 characters)
# Example: SECRET_KEY=my-super-secret-key-for-development-min-32-chars
```

### 3. Start Infrastructure

```bash
# Start PostgreSQL and Redis
docker-compose up -d postgres redis

# Wait for services to be ready (about 10 seconds)
sleep 10

# Verify services are running
docker-compose ps
```

### 4. Initialize Database

```bash
# Run migrations
alembic upgrade head

# Verify database connection
python -c "from app.core.database import engine; print('✓ Database ready')"
```

### 5. Start Application

**Option A: All services with Docker (Recommended)**
```bash
docker-compose up -d
```

**Option B: Manual (for development)**

Terminal 1 - API Server:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Terminal 2 - Worker 1:
```bash
python -m app.worker.worker --worker-id worker-1
```

Terminal 3 - Worker 2:
```bash
python -m app.worker.worker --worker-id worker-2
```

### 6. Verify It's Working

```bash
# Check health
curl http://localhost:8000/health

# Expected response:
# {"status":"healthy","version":"1.0.0"}

# View API docs
open http://localhost:8000/docs
```

### 7. Submit Your First Job

```bash
curl -X POST http://localhost:8000/api/v1/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "data_processing",
    "payload": {"data": [1, 2, 3, 4, 5]},
    "priority": 5
  }'
```

You should see a JSON response with the created job including a `job_id`.

### 8. Check Job Status

```bash
# Replace JOB_ID with the ID from step 7
curl http://localhost:8000/api/v1/jobs/JOB_ID

# Or list all jobs
curl http://localhost:8000/api/v1/jobs/
```

## Next Steps

- Read the [full README](README.md) for detailed documentation
- Explore the [API docs](http://localhost:8000/docs)
- Review [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system design
- Run tests: `pytest -v`

## Common Commands

```bash
# View logs
docker-compose logs -f

# Stop all services
docker-compose down

# Restart services
docker-compose restart

# Run tests
pytest

# Check code quality
black app tests
flake8 app tests
```

## Troubleshooting

### Port already in use
```bash
# Check what's using port 8000
lsof -i :8000

# Use different port
uvicorn app.main:app --port 8001
```

### Database connection failed
```bash
# Restart PostgreSQL
docker-compose restart postgres

# Check logs
docker-compose logs postgres
```

### Import errors
```bash
# Reinstall dependencies
pip install --force-reinstall -r requirements.txt
```

## Need Help?

Open an issue on [GitHub](https://github.com/Latha2308/distributed-job-processing-platform/issues)
