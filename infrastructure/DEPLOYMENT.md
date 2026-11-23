# AWS Deployment Guide

## Prerequisites

1. **AWS CLI installed and configured**
   ```bash
   aws --version
   aws configure  # Set access key, secret key, region
   ```

2. **Terraform installed** (>= 1.5.0)
   ```bash
   terraform --version
   ```

3. **Docker installed**
   ```bash
   docker --version
   ```

## Cost Estimate

**Minimal Configuration (default):**
- ECS Fargate (5 tasks × 0.25 vCPU): ~$27/month
- RDS db.t4g.micro: ~$13/month
- ElastiCache cache.t4g.micro: ~$11/month
- ALB: ~$18/month
- CloudWatch Logs (5GB): ~$3/month
- Secrets Manager (3 secrets): ~$1.20/month
- Data Transfer (50GB): ~$4.50/month
- **Total: ~$78/month**

**Main Cost Drivers:**
1. ALB ($18/mo) - Required for public HTTPS access
2. ECS Fargate ($27/mo) - Scales with task count and size
3. RDS ($13/mo) - Can upgrade to larger instance as needed

**Cost Optimization:**
- Using public subnets (no NAT Gateway): Saves $32/month
- Using t4g (ARM) instances: ~20% cheaper than t3
- 2 workers instead of 3: Saves $5/month
- Short log retention (7 days): Minimal storage costs

## Deployment Steps

### 1. Prepare Terraform Variables

```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and set:
- `secret_key`: Generate with `openssl rand -base64 48`
- `aws_region`: Your preferred region (default: us-east-1)
- Other customizations as needed

### 2. Initialize Terraform

```bash
terraform init
```

### 3. Plan Infrastructure

```bash
terraform plan -out=tfplan
```

Review the plan carefully. Expected resources: ~60 resources to be created.

### 4. Apply Infrastructure

```bash
terraform apply tfplan
```

This will create:
- VPC with public/private subnets
- RDS PostgreSQL database
- ElastiCache Redis cluster
- ECS Fargate cluster
- ALB for API access
- ECR repositories
- IAM roles and security groups
- Secrets Manager secrets
- CloudWatch log groups

**Duration:** 10-15 minutes

### 5. Build and Push Docker Images

```bash
# Get ECR URLs from Terraform output
API_REPO=$(terraform output -raw ecr_api_repository_url)
WORKER_REPO=$(terraform output -raw ecr_worker_repository_url)
AWS_REGION=$(terraform output -raw aws_region)

# Authenticate with ECR
aws ecr get-login-password --region $AWS_REGION | \
  docker login --username AWS --password-stdin $API_REPO

# Build and push API image
cd ../..  # Return to project root
docker build -f docker/Dockerfile.api -t $API_REPO:latest .
docker push $API_REPO:latest

# Build and push worker image
docker build -f docker/Dockerfile.worker -t $WORKER_REPO:latest .
docker push $WORKER_REPO:latest
```

### 6. Run Database Migrations

**Option A: Run as ECS task (recommended)**
```bash
cd infrastructure/terraform
CLUSTER=$(terraform output -raw ecs_cluster_name)
TASK_DEF=$(terraform output -raw api_task_definition_family)
SUBNET=$(terraform output -json public_subnet_ids | jq -r '.[0]')
SG=$(terraform output -raw ecs_security_group_id)

aws ecs run-task \
  --cluster $CLUSTER \
  --task-definition $TASK_DEF \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"api","command":["alembic","upgrade","head"]}]}'
```

**Option B: Run locally (requires database access)**
```bash
# Get database credentials from Secrets Manager
aws secretsmanager get-secret-value --secret-id $(terraform output -raw database_url_secret_arn) | \
  jq -r '.SecretString | fromjson | .url'

# Run migrations
DATABASE_URL="<url_from_above>" alembic upgrade head
```

### 7. Force Service Updates

Force ECS services to deploy the new images:

```bash
CLUSTER=$(terraform output -raw ecs_cluster_name)

aws ecs update-service --cluster $CLUSTER --service job-processing-prod-api --force-new-deployment
aws ecs update-service --cluster $CLUSTER --service job-processing-prod-worker --force-new-deployment
aws ecs update-service --cluster $CLUSTER --service job-processing-prod-dispatcher --force-new-deployment
aws ecs update-service --cluster $CLUSTER --service job-processing-prod-recovery --force-new-deployment
```

### 8. Verify Deployment

```bash
# Get API URL
API_URL=$(terraform output -raw alb_url)

# Test health endpoint
curl $API_URL/health

# Expected response: {"status":"healthy","version":"1.0.0"}
```

### 9. Monitor Services

```bash
# Check ECS service status
aws ecs describe-services --cluster $CLUSTER --services job-processing-prod-api

# View API logs
aws logs tail /ecs/job-processing-prod-api --follow

# View worker logs
aws logs tail /ecs/job-processing-prod-worker --follow
```

## Post-Deployment

### Configure DNS (Optional)

If you have a domain:
1. Create ACM certificate in AWS Certificate Manager
2. Add certificate ARN to `terraform.tfvars`
3. Set `enable_https = true`
4. Run `terraform apply`
5. Create CNAME record pointing to ALB DNS name

### Set Up Monitoring

CloudWatch alarms (optional but recommended):
```bash
# CPU utilization alarm
aws cloudwatch put-metric-alarm \
  --alarm-name job-processing-high-cpu \
  --alarm-description "Alert when CPU exceeds 80%" \
  --metric-name CPUUtilization \
  --namespace AWS/ECS \
  --statistic Average \
  --period 300 \
  --threshold 80 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 2

# Database connections alarm
aws cloudwatch put-metric-alarm \
  --alarm-name job-processing-db-connections \
  --alarm-description "Alert when DB connections exceed 80%" \
  --metric-name DatabaseConnections \
  --namespace AWS/RDS \
  --statistic Average \
  --period 300 \
  --threshold 80 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 2
```

## Cleanup / Destruction

**WARNING:** This will delete all resources and data permanently.

```bash
cd infrastructure/terraform

# Remove deletion protection from RDS
aws rds modify-db-instance \
  --db-instance-identifier job-processing-prod-postgres \
  --no-deletion-protection

# Destroy infrastructure
terraform destroy

# Confirm by typing 'yes'
```

**Cost Checklist Before Destroy:**
- [ ] RDS snapshots saved if needed
- [ ] CloudWatch logs exported if needed
- [ ] No unexpected data transfer charges pending
- [ ] All ECS tasks stopped
- [ ] ECR images backed up if needed

## Troubleshooting

### Services won't start
- Check CloudWatch logs for errors
- Verify Secrets Manager secrets are correct
- Ensure security groups allow necessary traffic
- Check ECS task health check failures

### Database connection failures
- Verify security group allows traffic from ECS tasks
- Check database credentials in Secrets Manager
- Ensure RDS is in `available` state

### High costs
- Check NAT Gateway usage (disable if not needed)
- Review CloudWatch Logs ingestion
- Monitor data transfer costs
- Consider reducing worker count or task sizes

### Cannot access API
- Verify ALB security group allows port 80/443
- Check ALB target group health
- Ensure ECS tasks have public IPs (if no NAT Gateway)
- Review ALB access logs in S3 (if enabled)
