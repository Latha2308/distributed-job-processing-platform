# Architecture Documentation

## System Overview

The Distributed Job Processing & Execution Platform is designed to handle high-volume background job processing with fault tolerance, retry mechanisms, and real-time monitoring.

## Core Components

### 1. FastAPI Application (API Server)
- **Purpose**: REST API for job submission and management
- **Responsibilities**:
  - Accept job submissions
  - Provide job status queries
  - Expose monitoring endpoints
  - WebSocket connections for real-time updates
  - Rate limiting and authentication

### 2. PostgreSQL Database
- **Purpose**: Persistent storage for job records and execution history
- **Tables**:
  - `jobs`: Main job records with status, payload, and metadata
  - `job_executions`: Audit trail of all job execution attempts
  - `worker_heartbeats`: Worker health and status tracking

### 3. Redis / AWS SQS
- **Purpose**: Job queue with priority support
- **Features**:
  - Priority-based job ordering (0=highest, 10=lowest)
  - Atomic dequeue operations
  - Dead letter queue for failed jobs
  - Configurable: Use Redis for development, SQS for production

### 4. Worker Processes
- **Purpose**: Execute jobs from the queue
- **Responsibilities**:
  - Poll queue for available jobs
  - Execute job handlers based on job type
  - Update job status in database
  - Send heartbeats for health monitoring
  - Handle retries with exponential backoff

### 5. Monitoring System
- **Prometheus**: Metrics collection
- **Custom Dashboard**: Real-time monitoring UI
- **Metrics Tracked**:
  - Job submission rate
  - Job completion rate
  - Processing duration
  - Queue depth
  - Worker health
  - Error rates by type

## Data Flow

```
1. Job Submission
   Client → API → PostgreSQL (create record)
                → Redis/SQS (enqueue)
                → Return Job ID

2. Job Processing
   Worker → Redis/SQS (dequeue)
         → PostgreSQL (update status: processing)
         → Execute Job Handler
         → PostgreSQL (update status: completed/failed)
         → WebSocket (notify subscribers)

3. Job Retry (on failure)
   Worker → Check retry_count < max_retries
         → Calculate backoff delay
         → PostgreSQL (update retry info)
         → Redis/SQS (re-enqueue with delay)
   
   OR
   
   Worker → Move to Dead Letter Queue (if max retries exceeded)
```

## Key Design Decisions

### 1. Idempotency
- Jobs can specify an `idempotency_key`
- Duplicate submissions return the original job
- Prevents duplicate processing of the same logical operation

### 2. Priority Scheduling
- Jobs have priority levels 0-10
- Lower numbers = higher priority
- Priority queue ensures critical jobs process first

### 3. Retry Mechanism
- Configurable max retries (default: 3)
- Exponential backoff: delay = initial_delay * (base ^ retry_count)
- Failed jobs track error type and message
- Jobs exceeding max retries move to dead letter queue

### 4. Worker Recovery
- Workers send periodic heartbeats
- Stale workers are marked inactive
- Jobs assigned to crashed workers can be reassigned

### 5. Job Timeout
- Each job has a configurable timeout
- Workers cancel jobs exceeding timeout
- Timed out jobs are retried or moved to DLQ

## Scalability Considerations

### Horizontal Scaling
- **API**: Multiple API instances behind load balancer
- **Workers**: Scale worker count based on queue depth
- **Database**: Connection pooling, read replicas for queries
- **Queue**: SQS supports unlimited scalability

### Performance Optimizations
- Database indexes on frequently queried fields
- Redis connection pooling
- Async I/O throughout the stack
- Batch operations where applicable

### Monitoring & Alerting
- Prometheus metrics exposed on `/metrics`
- Custom dashboard on `/monitoring`
- Alert on:
  - Queue depth exceeds threshold
  - Worker count below minimum
  - Job failure rate spikes
  - Processing duration increases

## Security

### API Security
- JWT-based authentication (implementation ready)
- Rate limiting per client
- Input validation on all endpoints
- CORS configuration

### Data Security
- Environment variables for secrets
- PostgreSQL SSL connections
- AWS IAM roles for SQS access
- No sensitive data in logs

## Deployment Architecture

### Development
```
Docker Compose:
- PostgreSQL container
- Redis container
- API container
- 3x Worker containers
- Prometheus container
```

### Production (AWS)
```
AWS Architecture:
- ECS Fargate for API and Workers
- RDS PostgreSQL for database
- ElastiCache Redis for caching
- SQS for job queue
- CloudWatch for logs
- Application Load Balancer
- Auto Scaling Groups
```

## Error Handling

### Job Failures
1. Catch exception in worker
2. Log error details
3. Update job status to FAILED
4. Create execution record
5. Attempt retry if under max_retries
6. Move to DLQ if retries exhausted

### Worker Failures
1. Worker stops sending heartbeats
2. Monitoring marks worker inactive
3. In-progress job can be reassigned
4. New worker can pick up the job

### Infrastructure Failures
- Database connection loss: Retry with backoff
- Redis connection loss: Fall back to direct processing
- Queue unavailable: Retry dequeue operation

## Future Enhancements

1. **Job Dependencies**: Support for job chains and workflows
2. **Scheduled Jobs**: Cron-like scheduling
3. **Job Cancellation**: API to cancel queued/processing jobs
4. **Multi-tenancy**: Isolate jobs by organization
5. **Advanced Routing**: Different workers for different job types
6. **Results Storage**: Store large results in S3
7. **GraphQL API**: Alternative to REST
8. **Admin UI**: Web interface for job management
