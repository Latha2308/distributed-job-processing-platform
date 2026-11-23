#!/bin/bash
# Run database migrations as an ECS task

set -e

# Check required tools
command -v aws >/dev/null 2>&1 || { echo "AWS CLI not installed"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq not installed"; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "Terraform not installed"; exit 1; }

# Change to Terraform directory
cd "$(dirname "$0")/../terraform"

# Get infrastructure details from Terraform
echo "Getting infrastructure details..."
CLUSTER=$(terraform output -raw ecs_cluster_name)
API_TASK_DEF=$(aws ecs describe-task-definition --task-definition "$(terraform output -raw ecs_cluster_name | sed 's/-cluster$//')-api" | jq -r '.taskDefinition.family')
SUBNET=$(terraform output -json public_subnet_ids | jq -r '.[0]' 2>/dev/null || aws ec2 describe-subnets --filters "Name=tag:Name,Values=*public*" --query 'Subnets[0].SubnetId' --output text)
SG=$(terraform output -raw ecs_security_group_id 2>/dev/null || aws ec2 describe-security-groups --filters "Name=tag:Name,Values=*ecs-tasks*" --query 'SecurityGroups[0].GroupId' --output text)

echo "Cluster: $CLUSTER"
echo "Task Definition: $API_TASK_DEF"
echo "Subnet: $SUBNET"
echo "Security Group: $SG"
echo ""

# Run migration task
echo "Running database migrations..."
TASK_ARN=$(aws ecs run-task \
  --cluster "$CLUSTER" \
  --task-definition "$API_TASK_DEF" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"api","command":["alembic","upgrade","head"]}]}' \
  --query 'tasks[0].taskArn' \
  --output text)

if [ -z "$TASK_ARN" ]; then
  echo "Failed to start migration task"
  exit 1
fi

echo "Migration task started: $TASK_ARN"
echo "Waiting for task to complete..."

# Wait for task to complete
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$TASK_ARN"

# Check task exit code
EXIT_CODE=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK_ARN" | \
  jq -r '.tasks[0].containers[0].exitCode')

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "✓ Database migrations completed successfully"
  exit 0
else
  echo "✗ Database migrations failed with exit code: $EXIT_CODE"
  echo "Check CloudWatch logs for details"
  exit 1
fi
