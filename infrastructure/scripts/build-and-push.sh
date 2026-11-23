#!/bin/bash
# Build and push Docker images to ECR

set -e

# Check required tools
command -v aws >/dev/null 2>&1 || { echo "AWS CLI not installed"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "Docker not installed"; exit 1; }
command -v terraform >/dev/null 2>&1 || { echo "Terraform not installed"; exit 1; }

# Change to Terraform directory
cd "$(dirname "$0")/../terraform"

# Get ECR repository URLs from Terraform outputs
echo "Getting ECR repository URLs..."
API_REPO=$(terraform output -raw ecr_api_repository_url)
WORKER_REPO=$(terraform output -raw ecr_worker_repository_url)
AWS_REGION=$(terraform output -raw aws_region 2>/dev/null || echo "us-east-1")

echo "API Repository: $API_REPO"
echo "Worker Repository: $WORKER_REPO"
echo "Region: $AWS_REGION"

# Authenticate with ECR
echo "Authenticating with ECR..."
aws ecr get-login-password --region "$AWS_REGION" | \
  docker login --username AWS --password-stdin "$(echo "$API_REPO" | cut -d/ -f1)"

# Change to project root
cd ../../..

# Build and push API image
echo "Building API image for linux/amd64..."
docker build --platform linux/amd64 -f docker/Dockerfile.api -t "$API_REPO:latest" .

echo "Pushing API image..."
docker push "$API_REPO:latest"

# Build and push worker image
echo "Building worker image for linux/amd64..."
docker build --platform linux/amd64 -f docker/Dockerfile.worker -t "$WORKER_REPO:latest" .

echo "Pushing worker image..."
docker push "$WORKER_REPO:latest"

echo "✓ Images built and pushed successfully"
echo ""
echo "Next steps:"
echo "1. Run database migrations"
echo "2. Force ECS service updates to deploy new images"
