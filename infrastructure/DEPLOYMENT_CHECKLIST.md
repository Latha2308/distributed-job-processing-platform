# AWS Deployment Checklist

## Pre-Deployment Requirements

### Tools Installation
- [ ] AWS CLI installed (`aws --version`)
- [ ] AWS CLI configured with credentials (`aws sts get-caller-identity`)
- [ ] Terraform >= 1.5.0 installed (`terraform --version`)
- [ ] Docker installed (`docker --version`)
- [ ] jq installed (for scripts) (`jq --version`)

### AWS Account Setup
- [ ] AWS account created
- [ ] IAM user/role with appropriate permissions:
  - VPC, Subnets, Security Groups
  - ECS, ECR
  - RDS, ElastiCache
  - ALB, Target Groups
  - IAM Roles and Policies
  - Secrets Manager
  - CloudWatch Logs
- [ ] Default region configured (`us-east-1` recommended for lowest cost)

### Configuration
- [ ] Generated secure secret key: `openssl rand -base64 48`
- [ ] Copied `terraform.tfvars.example` to `terraform.tfvars`
- [ ] Set `secret_key` in `terraform.tfvars`
- [ ] Reviewed and customized other variables (region, worker count, etc.)

## Initial Deployment Steps

### 1. Infrastructure Provisioning (10-15 minutes)
- [ ] Navigate to `infrastructure/terraform`
- [ ] Run `terraform init`
- [ ] Run `terraform validate` (should show "Success!")
- [ ] Run `terraform plan -out=tfplan`
- [ ] Review plan output (~60 resources)
- [ ] Run `terraform apply tfplan`
- [ ] Wait for completion (creates VPC, RDS, Redis, ECS, ALB, etc.)
- [ ] Save terraform outputs: `terraform output > ../../terraform-outputs.txt`

### 2. Container Images (5-10 minutes)
- [ ] Get ECR repository URLs: `terraform output ecr_api_repository_url`
- [ ] Run `../scripts/build-and-push.sh`
- [ ] Wait for API image build and push
- [ ] Wait for worker image build and push
- [ ] Verify images in ECR console

### 3. Database Setup (2-5 minutes)
- [ ] Run `../scripts/run-migrations.sh`
- [ ] Wait for migration task to complete
- [ ] Check task exit code (0 = success)
- [ ] Verify migrations in CloudWatch Logs if needed

### 4. Service Deployment (5-10 minutes)
- [ ] Run `../scripts/deploy-services.sh`
- [ ] Wait for all services to update
- [ ] Monitor deployment: `aws ecs describe-services --cluster <cluster-name> --services <service-name>`
- [ ] Wait for services to reach `RUNNING` state

### 5. Verification (2 minutes)
- [ ] Get API URL: `terraform output alb_url`
- [ ] Test health endpoint: `curl <api-url>/health`
- [ ] Expected response: `{"status":"healthy","version":"1.0.0"}`
- [ ] Submit test job via API
- [ ] Verify job completes successfully

## Post-Deployment Configuration

### Monitoring Setup
- [ ] Create CloudWatch alarms for critical metrics
- [ ] Set up SNS topic for alarm notifications
- [ ] Configure email/Slack alerts
- [ ] Enable AWS Budgets for cost tracking

### Security Hardening
- [ ] Review security group rules
- [ ] Enable RDS deletion protection (if not already)
- [ ] Enable ALB deletion protection for production
- [ ] Rotate Secrets Manager secrets regularly
- [ ] Review IAM role permissions

### DNS and HTTPS (Optional)
- [ ] Register domain name
- [ ] Request ACM certificate
- [ ] Update `terraform.tfvars` with certificate ARN
- [ ] Set `enable_https = true`
- [ ] Run `terraform apply`
- [ ] Create CNAME record pointing to ALB
- [ ] Verify HTTPS access

### Backup Verification
- [ ] Confirm RDS automated backups enabled
- [ ] Verify backup retention period (7 days)
- [ ] Test RDS snapshot restore procedure
- [ ] Confirm ElastiCache snapshots enabled

## GitHub Actions Setup (Optional)

### OIDC Configuration
- [ ] Create IAM OIDC provider for GitHub Actions
- [ ] Create IAM role with appropriate trust policy
- [ ] Grant role necessary permissions (ECS, ECR, etc.)
- [ ] Add `AWS_ROLE_ARN` to GitHub repository secrets
- [ ] Add `AWS_REGION` to GitHub repository secrets

### Workflow Testing
- [ ] Go to GitHub Actions → Deploy to AWS
- [ ] Run workflow manually
- [ ] Verify deployment succeeds
- [ ] Check CloudWatch logs for deployment issues

## Operational Verification

### Service Health Checks
- [ ] All ECS services in `RUNNING` state
- [ ] All ECS tasks healthy (passed health checks)
- [ ] RDS instance `available`
- [ ] ElastiCache cluster `available`
- [ ] ALB targets healthy

### Application Testing
- [ ] Submit test jobs via API
- [ ] Verify jobs processed by workers
- [ ] Check dispatcher logs for activity
- [ ] Verify recovery service running
- [ ] Test job retry behavior
- [ ] Verify dead-letter queue handling

### Log Verification
- [ ] API logs flowing to CloudWatch
- [ ] Worker logs flowing to CloudWatch
- [ ] Dispatcher logs flowing to CloudWatch
- [ ] Recovery logs flowing to CloudWatch
- [ ] No error/warning patterns in logs

### Cost Verification
- [ ] Review AWS Cost Explorer for current charges
- [ ] Verify costs align with estimates ($78-108/month)
- [ ] Check for unexpected charges (NAT Gateway, data transfer)
- [ ] Confirm Budget alerts configured

## Maintenance Tasks

### Weekly
- [ ] Review CloudWatch Logs for errors/warnings
- [ ] Check ECS service health and task count
- [ ] Monitor RDS and ElastiCache metrics
- [ ] Review job processing metrics

### Monthly
- [ ] Review AWS bill for unexpected charges
- [ ] Rotate Secrets Manager secrets
- [ ] Clean up old ECR images (automated by lifecycle policy)
- [ ] Review and clean old RDS snapshots
- [ ] Update CloudWatch log retention if needed

### Quarterly
- [ ] Review and update Terraform configuration
- [ ] Update container base images for security patches
- [ ] Evaluate reserved capacity purchase (savings)
- [ ] Review scaling needs (worker count, task sizes)

## Troubleshooting Checklist

### Services Won't Start
- [ ] Check CloudWatch Logs for container errors
- [ ] Verify Secrets Manager secrets are correct
- [ ] Confirm security groups allow necessary traffic
- [ ] Check ECS task IAM role permissions
- [ ] Verify ECR images exist and are tagged correctly

### Database Connection Issues
- [ ] Verify RDS instance is `available`
- [ ] Check security group allows traffic from ECS tasks
- [ ] Confirm database credentials in Secrets Manager
- [ ] Test connection from ECS task (run-task with psql)

### API Not Accessible
- [ ] Verify ALB is active
- [ ] Check ALB security group allows port 80/443
- [ ] Confirm target group health checks passing
- [ ] Verify ECS tasks have public IPs (if no NAT Gateway)
- [ ] Check ALB access logs (if enabled)

### High Costs
- [ ] Check for running but unused resources
- [ ] Verify NAT Gateway not enabled unnecessarily
- [ ] Review data transfer costs (CloudWatch, inter-AZ)
- [ ] Check CloudWatch Logs ingestion volume
- [ ] Confirm task counts match configuration

## Rollback Procedure

### Application Rollback
- [ ] Get previous working image tag
- [ ] Update ECS task definitions with old image tag
- [ ] Force new deployment: `aws ecs update-service --force-new-deployment`
- [ ] Monitor service stability

### Infrastructure Rollback
- [ ] Revert `terraform.tfvars` to previous version
- [ ] Run `terraform plan` to review changes
- [ ] Run `terraform apply` to revert
- [ ] Verify services still running after revert

### Database Rollback
- [ ] Stop all ECS services writing to database
- [ ] Restore from RDS snapshot
- [ ] Update Secrets Manager with new RDS endpoint
- [ ] Restart ECS services
- [ ] Verify application functionality

## Disaster Recovery Test

### Quarterly DR Test
- [ ] Document current state (task counts, versions, etc.)
- [ ] Stop all ECS services
- [ ] Restore RDS from snapshot
- [ ] Restart ECS services
- [ ] Verify application functionality
- [ ] Document recovery time and issues
- [ ] Update DR procedures if needed

## Decommission Checklist

### Before Destroying Infrastructure
- [ ] Backup all data needed
- [ ] Export CloudWatch logs
- [ ] Save final RDS snapshot
- [ ] Download ECR images if needed
- [ ] Document any important configuration

### Destroy Resources
- [ ] Run `../scripts/destroy-infrastructure.sh` (if created)
- [ ] Or manually: `terraform destroy`
- [ ] Confirm all resources deleted in AWS console
- [ ] Manually delete RDS snapshots (if not needed)
- [ ] Remove Secrets Manager secrets (after 30-day recovery window)

### Verify No Ongoing Charges
- [ ] Check Cost Explorer for remaining charges
- [ ] Verify no Elastic IPs allocated
- [ ] Confirm no snapshots incurring storage costs
- [ ] Check for any manually created resources not in Terraform

## Support Resources

- **Deployment Guide**: [DEPLOYMENT.md](DEPLOYMENT.md)
- **Cost Analysis**: [COST_BREAKDOWN.md](COST_BREAKDOWN.md)
- **Architecture Decisions**: [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md)
- **Infrastructure README**: [README.md](README.md)
- **AWS Documentation**: https://docs.aws.amazon.com/
- **Terraform AWS Provider**: https://registry.terraform.io/providers/hashicorp/aws/

## Emergency Contacts

Add your team's contact information:
- **On-call Engineer**:
- **AWS Account Owner**:
- **DevOps Lead**:
- **Emergency Escalation**:

## Sign-off

Deployment completed by: ___________________
Date: ___________________
Environment: ___________________
Terraform Version: ___________________
AWS Region: ___________________
Cluster Name: ___________________
API URL: ___________________
