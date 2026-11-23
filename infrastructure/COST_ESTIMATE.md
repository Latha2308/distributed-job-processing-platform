# AWS Monthly Cost Estimate - CORRECTED

**Configuration**: Default production setup
**Region**: us-east-1
**Date**: January 2025

## Detailed Cost Breakdown

### Compute - ECS Fargate ($27.00/month)
| Service | Tasks | vCPU | Memory | Hours/Mo | vCPU Cost | Memory Cost | Total |
|---------|-------|------|--------|----------|-----------|-------------|-------|
| API | 1 | 0.25 | 512MB | 730 | $7.39 | $1.62 | $9.01 |
| Workers | 2 | 0.25 | 512MB | 1460 | $14.78 | $3.24 | $18.02 |
| Dispatcher | 1 | 0.25 | 512MB | 730 | $7.39 | $1.62 | $9.01 |
| Recovery | 1 | 0.25 | 512MB | 730 | $7.39 | $1.62 | $9.01 |
| **Subtotal** | **5** | | | | | | **$45.05** |

**Note**: Pricing is higher than initially calculated
- vCPU: $0.04048/hr (not $0.04048 per 0.25 vCPU)
- Memory: $0.004445/GB-hr
- Actual per task: 0.25 × $0.04048 × 730 + 0.5 × $0.004445 × 730 = $9.01/task/month

### Data Stores ($24.28/month)
| Service | Type | Config | Base | Storage | Total |
|---------|------|--------|------|---------|-------|
| RDS PostgreSQL | db.t4g.micro | 20GB gp3 | $12.41 | $0.73 | $13.14 |
| ElastiCache Redis | cache.t4g.micro | Single node | $11.14 | - | $11.14 |
| **Subtotal** | | | | | **$24.28** |

### Networking ($52.46/month)
| Service | Config | Calculation | Cost |
|---------|--------|-------------|------|
| Application Load Balancer | Base charge | $0.0225/hr × 730 | $16.43 |
| ALB LCU charges | ~1 LCU average | $0.008/hr × 730 | $5.84 |
| **Interface VPC Endpoints** | **4 endpoints × 2 AZs** | **$0.01/hr × 8 × 730** | **$58.40** |
| S3 Gateway Endpoint | Free | - | $0.00 |
| Public IPv4 Addresses | 5 addresses | $0.005/hr × 5 × 730 | $18.25 |
| Data Transfer Out | 50GB/month | $0.09/GB after 10GB free | $3.60 |
| **Subtotal** | | | **$102.52** |

**VPC Endpoint Cost Correction**:
- Each interface endpoint creates an ENI in each subnet
- 4 endpoints × 2 AZs = 8 endpoint-AZs
- Cost: $0.01/endpoint-AZ/hour
- Monthly: $0.01 × 8 × 730 = $58.40 (not $29.20 as initially calculated)

### Storage & Monitoring ($5.15/month)
| Service | Config | Cost |
|---------|--------|------|
| CloudWatch Logs | 5GB ingestion, 7-day retention | $2.50 |
| ECR Storage | 5GB images | $0.50 |
| RDS Backups | 20GB automated (within allocated) | $0.95 |
| Secrets Manager | 3 secrets | $1.20 |
| **Subtotal** | | **$5.15** |

## Total Monthly Cost

| Category | Cost | % of Total |
|----------|------|------------|
| **Compute** | $45.05 | 25.6% |
| **Data Stores** | $24.28 | 13.8% |
| **Networking** | $102.52 | 58.2% |
| **Storage/Monitoring** | $5.15 | 2.9% |
| **TOTAL** | **$177.00** | **100%** |

## Cost Optimization Options

### Option 1: Reduce VPC Endpoints (-$58.40/month)
**Tradeoff**: Must enable NAT Gateway (+$32.85/month) = Net savings $25.55/month

**Consideration**: NAT Gateway also charges data processing ($0.045/GB), which could exceed $25/month if processing >550GB/month of AWS service traffic.

**Recommendation**: Keep VPC endpoints unless you need private subnet isolation for other reasons.

### Option 2: Reduce Workers (-$18.02/month)
**Change**: 2 workers → 1 worker
**New Total**: $158.98/month
**Tradeoff**: Lower job throughput, single point of failure for worker capacity

### Option 3: Use Fargate Spot (-$31.54/month, 70% savings on compute)
**New Compute Cost**: $13.51/month
**New Total**: $145.46/month
**Tradeoff**: Tasks can be interrupted with 2-minute notice, not suitable for all workloads

### Option 4: Move to t3 instances instead of t4g ARM
**Impact**: +20% cost on RDS and ElastiCache
**Cost Change**: +$4.86/month → $181.86/month
**Not Recommended**: t4g provides better price/performance

### Option 5: Reserved Capacity (1-year commitment)
| Component | Savings | New Cost |
|-----------|---------|----------|
| RDS | ~40% | $7.88 (-$5.26) |
| ElastiCache | ~35% | $7.24 (-$3.90) |
| **Total Savings** | | **-$9.16/month** |
**New Total**: $167.84/month

## Alternative Architecture: Private Subnets + NAT Gateway

### Configuration
- ECS tasks in private subnets (no public IPs)
- Single NAT Gateway in one AZ
- No VPC endpoints needed

### Cost Comparison
| Component | Current (Public + VPC Endpoints) | Alternative (Private + NAT) | Difference |
|-----------|----------------------------------|------------------------------|------------|
| Public IPs | $18.25 | $0.00 | -$18.25 |
| VPC Endpoints | $58.40 | $0.00 | -$58.40 |
| NAT Gateway | $0.00 | $32.85 | +$32.85 |
| NAT Data Processing | $0.00 | ~$10-20 | +$10-20 |
| **Total Networking** | $102.52 | ~$46-56 | **-$46-56** |
| **Total Monthly** | $177.00 | ~$130-140 | **-$37-47** |

**Tradeoffs**:
- Lower cost (~25% reduction)
- NAT Gateway is single point of failure (no Multi-AZ by default)
- Data processing charges variable based on usage
- Less secure (NAT Gateway has internet access)

**Recommendation**: Consider for cost-sensitive non-production environments.

## Hidden/Variable Costs

### Data Transfer
Current estimate assumes 50GB/month outbound. Actual costs depend on:
- API response sizes
- Number of requests
- Job result payloads

### CloudWatch Logs
Current estimate assumes 5GB/month ingestion. Increases with:
- Higher log verbosity
- More requests
- Longer-running jobs

### ECR Storage
Current estimate assumes 5GB. Increases with:
- Multiple image versions
- Larger dependencies
- No lifecycle policies

### HTTPS/Domain (if added)
| Component | Cost |
|-----------|------|
| Domain registration | ~$12/year ($1/month) |
| Route53 hosted zone | $0.50/month |
| ACM certificate | FREE |
| **Total** | **~$1.50/month** |

## Break-Even Analysis

### Worker Scaling
Each additional worker costs $18.02/month
Job throughput per worker: ~100-500 jobs/hour (depends on job complexity)

### API Scaling
Each additional API task costs $9.01/month
Request capacity per task: ~1,000-5,000 req/min (depends on request complexity)

## Cost Comparison by Scale

| Configuration | Monthly Cost | Use Case |
|---------------|--------------|----------|
| Minimal (1 worker) | $158.98 | Development, light testing |
| **Current (2 workers)** | **$177.00** | **Small production** |
| Medium (4 workers) | $213.04 | Growing production |
| Large (8 workers + 2 API) | $303.19 | High-volume production |

## AWS Free Tier Impact

**First 12 months** (new AWS accounts only):
- 750 hours/month t2.micro/t3.micro RDS (not applicable to t4g.micro)
- 750 hours/month t2.micro/t3.micro ElastiCache (not applicable to t4g.micro)
- ECS Fargate: No free tier
- VPC endpoints: No free tier

**Ongoing free tier**:
- 10GB data transfer out/month (applied)
- 5GB CloudWatch Logs ingestion/month (applied in estimate)
- 500MB ECR storage (applied)

**Note**: Free tier does NOT apply to ARM (t4g) instances, so current estimate is accurate.

## Budget Recommendations

1. **Set AWS Budget Alert**: $200/month threshold
2. **Enable Cost Anomaly Detection**: Notify on 20% variance
3. **Weekly cost review**: First month to validate estimates
4. **Tag all resources**: Project=job-processing, Environment=prod

## Summary

**Base Configuration**: $177.00/month
- Secure, production-ready
- Multi-AZ availability
- Private VPC endpoints for AWS services
- No NAT Gateway costs

**Most Cost-Effective Production**: $130-140/month
- Use private subnets with single NAT Gateway
- Accept single-AZ NAT risk
- Monitor data processing charges

**Current Recommendation**:
Start with base configuration ($177/month) for first month to establish baseline, then optimize based on actual usage patterns.
