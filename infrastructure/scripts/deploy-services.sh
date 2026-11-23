#!/bin/bash
# Force ECS services to deploy new images

set -e

# Check required tools
command -v aws >/dev/null 2>&1 || { echo "AWS CLI not installed"; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "Terraform not installed"; exit 1; }

# Change to Terraform directory
cd "$(dirname "$0")/../terraform"

# Get cluster name
CLUSTER=$(terraform output -raw ecs_cluster_name)
echo "Cluster: $CLUSTER"
echo ""

# Force new deployments for all services
echo "Updating API service..."
aws ecs update-service \
  --cluster "$CLUSTER" \
  --service "${CLUSTER/-cluster/}-api" \
  --force-new-deployment \
  --query 'service.serviceName' \
  --output text

echo "Updating worker service..."
aws ecs update-service \
  --cluster "$CLUSTER" \
  --service "${CLUSTER/-cluster/}-worker" \
  --force-new-deployment \
  --query 'service.serviceName' \
  --output text

echo "Updating dispatcher service..."
aws ecs update-service \
  --cluster "$CLUSTER" \
  --service "${CLUSTER/-cluster/}-dispatcher" \
  --force-new-deployment \
  --query 'service.serviceName' \
  --output text

echo "Updating recovery service..."
aws ecs update-service \
  --cluster "$CLUSTER" \
  --service "${CLUSTER/-cluster/}-recovery" \
  --force-new-deployment \
  --query 'service.serviceName' \
  --output text

echo ""
echo "✓ All services updated"
echo ""
echo "Monitor deployment progress:"
echo "  aws ecs describe-services --cluster $CLUSTER --services ${CLUSTER/-cluster/}-api"
echo ""
echo "View logs:"
echo "  aws logs tail /ecs/${CLUSTER/-cluster/}-api --follow"
