# Infrastructure as Code

This directory contains Terraform configuration for deploying the Distributed Job Processing Platform to AWS.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Internet                                                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
            ┌────────────────┐
            │ Application    │
            │ Load Balancer  │ (HTTPS optional)
            └────────┬───────┘
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼                         ▼
┌───────────────┐         ┌───────────────┐
│ Public Subnet │         │ Public Subnet │
│    (AZ-1)     │         │    (AZ-2)     │
│               │         │               │
│  ┌─────────┐  │         │  ┌─────────┐  │
│  │ECS Tasks│  │         │  │ECS Tasks│  │
│  │ - API   │  │         │  │ - API   │  │
│  │ - Worker│  │         │  │ - Worker│  │
│  │ - Disp. │  │         │  │ - Disp. │  │
│  │ - Recov.│  │         │  │ - Recov.│  │
│  └────┬────┘  │         │  └────┬────┘  │
└───────┼───────┘         └───────┼───────┘
        │                         │
        └────────────┬────────────┘
                     │
        ┌────────────┴────────────┐
        │                         │
        ▼                         ▼
┌───────────────┐         ┌───────────────┐
│Private Subnet │         │Private Subnet │
│    (AZ-1)     │         │    (AZ-2)     │
│               │         │               │
│  ┌─────────┐  │         │  ┌─────────┐  │
│  │   RDS   │  │         │  │  Redis  │  │
│  │Postgres │  │         │  │ElastiCch│  │
│  └─────────┘  │         │  └─────────┘  │
└───────────────┘         └───────────────┘
```

## Cost

**Estimated monthly cost: $78 - $108**

See [COST_BREAKDOWN.md](COST_BREAKDOWN.md) for detailed breakdown and optimization options.

## Prerequisites

1. **AWS Account** with appropriate permissions
2. **AWS CLI** installed and configured
3. **Terraform** >= 1.5.0
4. **Docker** for building images

## Quick Start

### 1. Configure Variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:
- `secret_key` - Generate with: `openssl rand -base64 48`
- `aws_region` - Your preferred region
- Other customizations

### 2. Deploy Infrastructure

```bash
terraform init
terraform plan
terraform apply
```

### 3. Build and Push Images

```bash
../scripts/build-and-push.sh
```

### 4. Run Migrations

```bash
../scripts/run-migrations.sh
```

### 5. Deploy Services

```bash
../scripts/deploy-services.sh
```

### 6. Test Deployment

```bash
API_URL=$(terraform output -raw alb_url)
curl $API_URL/health
```

## Files

- **terraform/** - Terraform configuration files
  - `main.tf` - Provider and backend configuration
  - `variables.tf` - Input variables
  - `vpc.tf` - VPC and networking
  - `security_groups.tf` - Security groups
  - `rds.tf` - RDS PostgreSQL database
  - `elasticache.tf` - ElastiCache Redis
  - `ecr.tf` - Container registries
  - `iam.tf` - IAM roles and policies
  - `alb.tf` - Application Load Balancer
  - `ecs.tf` - ECS cluster and services
  - `secrets.tf` - Secrets Manager secrets
  - `outputs.tf` - Output values

- **scripts/** - Deployment automation scripts
  - `build-and-push.sh` - Build and push Docker images
  - `run-migrations.sh` - Run database migrations
  - `deploy-services.sh` - Update ECS services

- **Documentation**
  - `DEPLOYMENT.md` - Detailed deployment guide
  - `COST_BREAKDOWN.md` - Cost analysis and cleanup
  - `README.md` - This file

## Network Design

**Cost Optimization: Public Subnets**

ECS tasks run in public subnets with public IPs to avoid NAT Gateway costs ($32/month). This is acceptable for this workload because:

1. Security is maintained through security groups (ingress only from ALB)
2. RDS and Redis remain in private subnets (no internet access)
3. VPC endpoints provide secure access to AWS services (ECR, Logs, Secrets Manager)

**Trade-offs:**
- Each public IP costs $0.005/hr (~$18/month total for 5 tasks)
- Alternative: Use NAT Gateway (~$33/month) with private subnets
- Net savings: ~$15/month using public subnets

## HTTPS Configuration

By default, the ALB serves HTTP only. To enable HTTPS:

1. Register a domain and create an ACM certificate
2. Set in `terraform.tfvars`:
   ```hcl
   enable_https        = true
   domain_name         = "api.example.com"
   acm_certificate_arn = "arn:aws:acm:..."
   ```
3. Run `terraform apply`
4. Create DNS CNAME record pointing to ALB DNS name

## Monitoring

### CloudWatch Logs

```bash
# View API logs
aws logs tail /ecs/job-processing-prod-api --follow

# View worker logs
aws logs tail /ecs/job-processing-prod-worker --follow
```

### ECS Service Status

```bash
CLUSTER=$(terraform output -raw ecs_cluster_name)
aws ecs describe-services --cluster $CLUSTER --services job-processing-prod-api
```

### Cost Monitoring

```bash
# Current month costs
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=SERVICE
```

## Scaling

### Horizontal Scaling

Increase worker count in `terraform.tfvars`:
```hcl
worker_count = 4  # Up from 2
```

Then apply:
```bash
terraform apply
```

### Vertical Scaling

Increase task resources:
```hcl
api_cpu    = 512   # 0.5 vCPU (up from 0.25)
api_memory = 1024  # 1GB (up from 512MB)
```

### Auto-Scaling (Optional)

Add ECS auto-scaling policies based on:
- CPU utilization
- Memory utilization
- Custom CloudWatch metrics (queue depth)

## Disaster Recovery

### Backups

- **RDS**: Automated daily backups (7-day retention)
- **RDS**: Final snapshot on destroy
- **ElastiCache**: Daily snapshots (5-day retention)

### Restore from Backup

```bash
# List RDS snapshots
aws rds describe-db-snapshots --db-instance-identifier job-processing-prod-postgres

# Restore from snapshot
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier job-processing-prod-postgres-restored \
  --db-snapshot-identifier <snapshot-id>
```

## Cleanup

**WARNING: This deletes all data permanently**

```bash
# Remove deletion protection
aws rds modify-db-instance \
  --db-instance-identifier job-processing-prod-postgres \
  --no-deletion-protection

# Destroy infrastructure
terraform destroy
```

See [COST_BREAKDOWN.md](COST_BREAKDOWN.md) for complete cleanup checklist.

## Troubleshooting

### Services won't start

Check CloudWatch logs for errors:
```bash
aws logs tail /ecs/job-processing-prod-api --since 1h
```

### Database connection failures

Verify security group allows traffic from ECS tasks to RDS:
```bash
aws ec2 describe-security-groups --group-ids <rds-sg-id>
```

### High costs

Review Cost Explorer:
```bash
aws ce get-cost-and-usage \
  --time-period Start=$(date -d '7 days ago' +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity DAILY \
  --metrics BlendedCost
```

Check for:
- NAT Gateway data processing charges
- Unexpected data transfer
- Running tasks that should be stopped

## GitHub Actions Deployment

The repository includes a GitHub Actions workflow for automated deployment.

### Setup OIDC

1. Create IAM OIDC provider for GitHub Actions
2. Create IAM role with trust policy for GitHub
3. Add role ARN to repository secrets as `AWS_ROLE_ARN`

See `.github/workflows/deploy-aws.yml` for workflow configuration.

### Manual Deployment

Trigger from GitHub Actions UI:
1. Go to Actions → Deploy to AWS
2. Click "Run workflow"
3. Select environment and options
4. Click "Run workflow"

## Support

For infrastructure questions or issues:
1. Check [DEPLOYMENT.md](DEPLOYMENT.md) for detailed instructions
2. Review [COST_BREAKDOWN.md](COST_BREAKDOWN.md) for cost optimization
3. Check AWS CloudWatch Logs for application errors
4. Review Terraform plan output before applying changes
