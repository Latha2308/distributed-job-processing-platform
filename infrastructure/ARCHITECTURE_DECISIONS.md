# AWS Architecture Decisions

## Key Design Choices

### 1. ECS Fargate vs EC2

**Decision:** Use ECS Fargate

**Rationale:**
- Zero server management overhead
- Pay only for running tasks
- Automatic scaling without managing ASG
- Simpler security model (no host-level patching)

**Trade-off:**
- Higher per-task cost (~40% more than EC2 spot)
- Worth it for < 10,000 jobs/day workload

### 2. Public vs Private Subnets for ECS

**Decision:** ECS tasks in public subnets with public IPs

**Rationale:**
- Saves $32/month (NAT Gateway cost)
- Security maintained via security groups
- VPC endpoints provide secure AWS service access

**Trade-off:**
- $18/month public IPv4 charges (5 tasks × $0.005/hr)
- Net savings: $14/month
- Still secure: RDS/Redis in private subnets, SG restricts ingress

**Alternative:** Private subnets + NAT Gateway
- More "traditional" approach
- Recommended for high-security/compliance environments
- Enable with: `enable_nat_gateway = true`

### 3. RDS vs Aurora

**Decision:** RDS PostgreSQL

**Rationale:**
- Aurora minimum cost: $43/month (1 instance)
- RDS minimum cost: $13/month (db.t4g.micro)
- Workload doesn't need Aurora's advanced features
- Automated backups and Multi-AZ available if needed

**When to upgrade to Aurora:**
- Need > 10,000 jobs/day
- Require sub-second failover
- Want read replicas for analytics

### 4. ElastiCache Single Node vs Cluster

**Decision:** Single node Redis

**Rationale:**
- $11/month vs $22/month for cluster mode
- Job queue workload tolerates brief Redis downtime
- Workers will retry on connection failure
- Recovery service will reschedule stuck jobs

**When to upgrade to cluster:**
- Need 99.99% Redis availability
- Redis becomes bottleneck (> 100,000 ops/sec)
- Require read replicas

### 5. t4g (ARM) vs t3 (x86) Instances

**Decision:** Use t4g (ARM) everywhere possible

**Rationale:**
- 20% cheaper than t3 equivalent
- PostgreSQL 15 and Redis 7 support ARM natively
- No application code changes needed (Python is interpreted)

**Compatibility:**
- RDS: db.t4g.micro (ARM)
- ElastiCache: cache.t4g.micro (ARM)
- ECS: Fargate ARM not yet available, uses x86

### 6. VPC Endpoints vs NAT Gateway

**Decision:** VPC endpoints for AWS services (without NAT Gateway)

**Rationale:**
- 4 VPC endpoints: $29/month
- NAT Gateway: $33/month + data processing
- VPC endpoints: No data processing charges
- ECS tasks can pull ECR images, write logs, read secrets without NAT

**Endpoints created:**
- ECR API (pull images)
- ECR Docker (pull layers)
- CloudWatch Logs (write logs)
- Secrets Manager (read secrets)

**Note:** If NAT Gateway is enabled, VPC endpoints are automatically disabled (redundant)

### 7. HTTP vs HTTPS

**Decision:** HTTP by default, HTTPS optional

**Rationale:**
- Avoids requirement for domain + ACM certificate
- Faster initial setup and testing
- HTTPS easily enabled later with 2 config changes

**To enable HTTPS:**
1. Create ACM certificate
2. Set `enable_https = true` in terraform.tfvars
3. Add certificate ARN
4. Run `terraform apply`

### 8. Task Resource Sizing

**Decision:** 0.25 vCPU, 512MB RAM per task

**Rationale:**
- Minimum Fargate task size: 0.25 vCPU
- Application is I/O bound (database, Redis), not CPU bound
- 512MB RAM sufficient for Python + FastAPI/worker
- Can scale vertically if needed

**Monitoring metrics:**
- CPU > 80%: Increase CPU
- Memory > 80%: Increase RAM
- Neither high: Reduce to save costs

### 9. Worker Count

**Decision:** 2 workers by default

**Rationale:**
- 1 worker: Single point of failure
- 2 workers: Redundancy without over-provisioning
- 3+ workers: Only if job queue consistently backs up

**Formula:** Workers needed ≈ (jobs/hour × avg_duration_seconds) / 3600
- Example: 1000 jobs/hour × 5 sec average = 5000 / 3600 = 1.4 workers minimum
- Add 1 worker for redundancy → 2 workers

### 10. Log Retention

**Decision:** 7 days

**Rationale:**
- Sufficient for debugging recent issues
- Minimal storage costs ($0.03/GB/month)
- 30-day retention: 4x more expensive
- Export critical logs to S3 if longer retention needed

**Cost impact:**
- 7 days at 5GB/day: 35GB × $0.03 = $1.05/month
- 30 days at 5GB/day: 150GB × $0.03 = $4.50/month

### 11. Database Backups

**Decision:** 7-day automated backups, no Multi-AZ

**Rationale:**
- 7 days: Meets most recovery scenarios
- Multi-AZ: Doubles cost ($13 → $26/month)
- Job processing tolerates brief database downtime
- Recovery service will reschedule in-flight jobs

**When to enable Multi-AZ:**
- SLA requires < 1 minute downtime
- Database is > 100GB (slower single-instance recovery)
- Peak traffic cannot tolerate 5-10 minute failover

### 12. Secrets Manager vs Parameter Store

**Decision:** Secrets Manager

**Rationale:**
- Automatic rotation support (future feature)
- Better audit trail (CloudTrail integration)
- Easier ECS integration (native support)
- Cost: $0.40/secret/month (negligible at 3 secrets)

**Parameter Store alternative:**
- Free for standard parameters
- Less featured but viable for static secrets

### 13. Container Registry

**Decision:** ECR (Elastic Container Registry)

**Rationale:**
- Native AWS integration
- No authentication complexity
- Image scanning included
- Cost: $0.10/GB/month storage

**Alternative:** Docker Hub
- Free tier: 1 private repo
- Requires separate authentication
- Not recommended for production

## Security Decisions

### 14. Security Group Strategy

**Decision:** Minimal ingress, broad egress

**Ingress rules:**
- ALB: 80/443 from internet
- ECS: 8000 from ALB only
- RDS: 5432 from ECS only
- Redis: 6379 from ECS only

**Egress rules:**
- All: 0.0.0.0/0 (simplifies troubleshooting)
- Could be tightened to specific CIDRs/security groups

### 15. IAM Roles

**Decision:** Separate execution and task roles

**Execution role:** Pull images, write logs, read secrets
**Task role:** Application runtime permissions (S3, etc.)

**Principle of least privilege:**
- Only grant permissions actually needed
- Use resource-level restrictions where possible
- Avoid wildcards in production

### 16. Database Access

**Decision:** No public access, security group restricted

**Access methods:**
1. ECS tasks (normal operation)
2. ECS run-task for migrations
3. Session Manager/bastion for admin (not included)

**Admin access options:**
- VPN + bastion host
- AWS Systems Manager Session Manager
- Temporary security group rule (not recommended)

## Monitoring & Observability

### 17. CloudWatch Container Insights

**Decision:** Enabled

**Rationale:**
- Automatic ECS task-level metrics
- Minimal cost impact
- Essential for troubleshooting
- Cost: Included in ECS, only pay for metrics storage

### 18. Application Logs

**Decision:** CloudWatch Logs with awslogs driver

**Rationale:**
- Native ECS integration
- Searchable via CloudWatch Logs Insights
- No additional infrastructure needed
- Cost: $0.50/GB ingestion

**Alternative:** Centralized logging (ELK, Datadog)
- Recommended for > 10 services
- Current setup: 4 services, manageable in CloudWatch

### 19. Alarms

**Decision:** Not included in Terraform

**Rationale:**
- Alarm thresholds are environment-specific
- Create manually or via separate Terraform module
- Avoid alert fatigue with generic thresholds

**Recommended alarms:**
- ECS CPU > 80%
- ECS Memory > 80%
- RDS CPU > 80%
- RDS Storage < 20% free
- ALB 5xx errors > 1%
- Job queue depth > 1000

## Cost Optimization Decisions

### 20. Reserved Capacity

**Decision:** Not purchased initially

**Rationale:**
- 1-year commitment required
- Workload pattern unknown initially
- Wait 2-3 months to establish baseline
- Then evaluate RDS and ElastiCache reserved capacity

**Potential savings:**
- RDS: 40% off = $5/month
- ElastiCache: 35% off = $4/month
- Fargate: No reserved capacity available

### 21. Spot Instances

**Decision:** Not used

**Rationale:**
- Fargate Spot: 70% savings but 2-minute interruption notice
- Job processing can't gracefully handle mid-job interruption
- Recovery service handles worker failures but adds latency

**When to use Spot:**
- Batch jobs (not real-time)
- Stateless workers with idempotent operations
- Can checkpoint and resume work

### 22. Auto-Scaling

**Decision:** Not configured initially

**Rationale:**
- Fixed task counts easier to predict costs
- Workload pattern unknown
- Add auto-scaling after establishing baseline

**Future enhancement:**
- Scale workers based on Redis queue depth
- Scale API based on ALB request count
- Use target tracking scaling policies

## Deployment Decisions

### 23. Blue-Green vs Rolling Deployments

**Decision:** Rolling deployments (ECS default)

**Rationale:**
- Simpler configuration
- No need for extra capacity during deployment
- Fast rollback (update desired count)

**Blue-Green alternative:**
- Requires 2x capacity during deployment
- Instant cutover with DNS/ALB
- Recommended for critical production systems

### 24. Database Migration Strategy

**Decision:** Run as one-off ECS task

**Rationale:**
- Runs in same environment as application
- Has access to database (security groups)
- Logs captured in CloudWatch
- No local environment setup needed

**Alternative:** Run locally
- Requires VPN or bastion access
- Useful for development/testing

### 25. CI/CD Strategy

**Decision:** Manual GitHub Actions workflow with OIDC

**Rationale:**
- Explicit deployment approval required
- No long-lived AWS credentials
- OIDC (OpenID Connect) for secure authentication
- Manual trigger prevents accidental deploys

**Automatic deployment:**
- Remove `workflow_dispatch`, use `push` trigger
- Recommended only after thorough testing

## Future Considerations

### When to Upgrade Architecture

**Move to ECS on EC2:**
- Workload > 10,000 jobs/day
- Spot instances become viable (batch processing)
- Need GPU or custom hardware

**Move to EKS (Kubernetes):**
- Multi-cloud strategy required
- Need advanced orchestration features
- Team has Kubernetes expertise
- > 50 microservices

**Add Read Replicas:**
- Read queries > 1000/sec
- Analytics workload separate from transactional
- Multi-region requirements

**Add CDN (CloudFront):**
- API serves static content
- Global user base
- Latency-sensitive application

**Add API Gateway:**
- Need built-in throttling/rate limiting
- Want request/response transformations
- Require API key management
- Multi-environment stage management

## Summary

This architecture prioritizes:
1. **Cost efficiency:** $78-108/month vs $200+ for over-provisioned setup
2. **Simplicity:** Managed services, minimal moving parts
3. **Security:** Private data layer, least privilege IAM
4. **Scalability:** Can grow 10x with configuration changes only

**Not optimized for:**
- Sub-100ms latency requirements
- 99.99% uptime SLA
- > 1M jobs/day
- Multi-region active-active

**Recommended for:**
- Startups and small teams
- 100-10,000 jobs/day
- 99.9% uptime acceptable
- Cost-conscious deployments
