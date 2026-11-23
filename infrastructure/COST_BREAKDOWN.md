# AWS Cost Breakdown and Cleanup Checklist

## Monthly Cost Estimate (Minimal Configuration)

### Compute
| Service | Configuration | Monthly Cost | Notes |
|---------|--------------|--------------|-------|
| ECS Fargate API | 1 task × 0.25 vCPU × 0.5GB | $5.40 | 730 hrs × $0.04048/vCPU-hr + $0.004445/GB-hr |
| ECS Fargate Workers | 2 tasks × 0.25 vCPU × 0.5GB | $10.80 | Same calculation as API |
| ECS Fargate Dispatcher | 1 task × 0.25 vCPU × 0.5GB | $5.40 | Same calculation as API |
| ECS Fargate Recovery | 1 task × 0.25 vCPU × 0.5GB | $5.40 | Same calculation as API |
| **Compute Subtotal** | | **$27.00** | |

### Database & Cache
| Service | Configuration | Monthly Cost | Notes |
|---------|--------------|--------------|-------|
| RDS PostgreSQL | db.t4g.micro, 20GB gp3 | $13.14 | $12.41 instance + $0.73 storage |
| ElastiCache Redis | cache.t4g.micro | $11.14 | On-demand pricing |
| **Data Subtotal** | | **$24.28** | |

### Networking
| Service | Configuration | Monthly Cost | Notes |
|---------|--------------|--------------|-------|
| Application Load Balancer | 1 ALB, minimal traffic | $17.76 | $16.20 base + $1.56 LCU charges |
| NAT Gateway | Disabled | $0.00 | Would add $32.85/month if enabled |
| VPC Endpoints (Interface) | 4 endpoints × $7.30 | $29.20 | ECR API, ECR DKR, Logs, Secrets Manager |
| Data Transfer Out | 50GB | $4.50 | First 10GB free, then $0.09/GB |
| **Networking Subtotal** | | **$51.46** | |

### Storage & Monitoring
| Service | Configuration | Monthly Cost | Notes |
|---------|--------------|--------------|-------|
| CloudWatch Logs | 5GB ingestion, 7-day retention | $2.50 | $0.50/GB ingestion |
| ECR Storage | 5GB images | $0.50 | $0.10/GB/month |
| RDS Snapshots | 20GB (automated) | $0.95 | $0.095/GB/month |
| Secrets Manager | 3 secrets | $1.20 | $0.40/secret/month |
| **Storage Subtotal** | | **$5.15** | |

### Total Monthly Cost: **$107.89**

## Cost Optimization Options

### Option 1: Remove VPC Endpoints (Use NAT Gateway instead)
- Remove: 4 VPC endpoints (-$29.20)
- Add: NAT Gateway (+$32.85)
- Net change: +$3.65/month
- **Not recommended unless ECS tasks need private subnets**

### Option 2: Further Reduce Workers
- Current: 2 workers ($10.80)
- Reduced: 1 worker ($5.40)
- Savings: -$5.40/month
- **Trade-off: Lower job throughput**

### Option 3: Spot Instances (ECS Fargate Spot)
- 70% savings on compute: -$18.90/month
- **Trade-off: Tasks can be interrupted with 2-minute notice**

### Option 4: Use Cheaper Task Sizes
- Reduce from 0.25 vCPU to 0.5GB to minimum viable
- Not recommended: Already at practical minimum

### Option 5: Use Reserved Capacity (1-year commitment)
- RDS: ~40% savings → -$5.26/month
- ElastiCache: ~35% savings → -$3.90/month
- Total savings: -$9.16/month
- **Trade-off: 1-year commitment**

## Recommended Minimal Production Setup

**Cost: ~$78/month**

Changes from default:
- Disable VPC endpoints, use NAT Gateway only if needed
- Keep 2 workers
- Use on-demand pricing (no commitments)
- 7-day log retention

Configuration in `terraform.tfvars`:
```hcl
enable_nat_gateway = false  # ECS tasks in public subnets
worker_count       = 2
log_retention_days = 7
```

## Cleanup Checklist

### Before Destroying Infrastructure

**Data Backup:**
- [ ] Export RDS final snapshot (automatic on destroy)
- [ ] Download CloudWatch logs if needed
- [ ] Backup ECR images to local or Docker Hub
- [ ] Export any application data or job results

**Cost Verification:**
- [ ] Check current month's AWS billing dashboard
- [ ] Review Cost Explorer for unexpected charges
- [ ] Confirm no pending data transfer charges
- [ ] Verify all Fargate tasks are stopped

### Resources to Delete (terraform destroy)

**Compute:**
- [ ] ECS Fargate tasks (5 tasks)
- [ ] ECS services (4 services)
- [ ] ECS cluster

**Networking:**
- [ ] Application Load Balancer
- [ ] Target groups
- [ ] NAT Gateway (if enabled)
- [ ] Elastic IPs (if NAT enabled)
- [ ] VPC endpoints (4 interfaces)
- [ ] Security groups
- [ ] VPC

**Data:**
- [ ] RDS PostgreSQL instance (creates final snapshot)
- [ ] ElastiCache Redis cluster
- [ ] Secrets Manager secrets (30-day recovery window)

**Storage:**
- [ ] CloudWatch log groups
- [ ] ECR repositories (and all images)
- [ ] RDS automated backups

**IAM:**
- [ ] ECS task execution role
- [ ] ECS task role
- [ ] IAM policies

### Manual Cleanup (Not Managed by Terraform)

**After terraform destroy:**
- [ ] Delete RDS final snapshots (if not needed)
- [ ] Empty S3 buckets used for logs (if created)
- [ ] Remove CloudWatch alarms (if manually created)
- [ ] Delete unused Elastic IPs (avoid $0.005/hr charges)

### Cost Accumulation After Destroy

**0-7 days:**
- Secrets Manager: $0.40 × 3 (recovery window)
- RDS snapshots: $0.95 (until deleted)

**After 7 days:**
- Secrets Manager secrets permanently deleted
- Must manually delete RDS snapshots to stop charges

## Hidden Costs to Watch

### Data Transfer
- **Inter-AZ data transfer:** $0.01/GB (between subnets)
- **Internet egress:** $0.09/GB (to clients)
- **CloudWatch Logs ingestion:** $0.50/GB
- Mitigation: Use same AZ for related services, reduce logging

### NAT Gateway (if enabled)
- **Fixed cost:** $0.045/hour = $32.85/month
- **Data processing:** $0.045/GB processed
- For 100GB: Additional $4.50/month
- Mitigation: Use VPC endpoints or public subnets

### Public IPv4 Addresses
- **Cost:** $0.005/hour per address
- ECS tasks with public IPs: 5 tasks × $0.005/hr = $0.025/hr = $18.25/month
- Mitigation: Use NAT Gateway + private subnets (but adds NAT cost)

### RDS Storage Growth
- Initial: 20GB at $0.095/GB = $1.90/month
- Max auto-scaling: 40GB at $0.095/GB = $3.80/month
- Mitigation: Monitor database size, clean old jobs

### CloudWatch Logs
- 5GB/month baseline: $2.50
- Growing to 10GB: $5.00
- Growing to 20GB: $10.00
- Mitigation: Reduce retention to 3 days, filter noisy logs

## Cost Monitoring Setup

### Set Up Budget Alerts

```bash
aws budgets create-budget \
  --account-id $(aws sts get-caller-identity --query Account --output text) \
  --budget '{
    "BudgetName": "job-processing-monthly",
    "BudgetLimit": {
      "Amount": "150",
      "Unit": "USD"
    },
    "TimeUnit": "MONTHLY",
    "BudgetType": "COST"
  }' \
  --notifications-with-subscribers '[{
    "Notification": {
      "NotificationType": "ACTUAL",
      "ComparisonOperator": "GREATER_THAN",
      "Threshold": 80
    },
    "Subscribers": [{
      "SubscriptionType": "EMAIL",
      "Address": "your-email@example.com"
    }]
  }]'
```

### Enable Cost Anomaly Detection

1. Go to AWS Cost Explorer → Cost Anomaly Detection
2. Create monitor for "ECS", "RDS", "ElastiCache"
3. Set alert threshold: $20 or 20% increase
4. Configure SNS notifications

### Weekly Cost Review

```bash
# Current month costs
aws ce get-cost-and-usage \
  --time-period Start=$(date -u -d "$(date +%Y-%m-01)" +%Y-%m-%d),End=$(date -u +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=SERVICE

# Forecast for month
aws ce get-cost-forecast \
  --time-period Start=$(date -u +%Y-%m-%d),End=$(date -u -d "$(date +%Y-%m-01) +1 month -1 day" +%Y-%m-%d) \
  --metric BLENDED_COST \
  --granularity MONTHLY
```

## Break-Even Analysis

**At what job volume does AWS hosting make sense?**

Assuming:
- Local hosting cost: $50/month (VPS)
- AWS cost: $108/month
- Break-even: Never, unless:
  - Need auto-scaling for burst traffic
  - Require managed backups and HA
  - Want zero maintenance overhead
  - Need compliance certifications

**Recommendation:**
- **< 1000 jobs/day**: Local or cheaper VPS
- **1000-10000 jobs/day**: AWS Fargate (current setup)
- **> 10000 jobs/day**: Consider ECS on EC2 or EKS for lower per-task costs
