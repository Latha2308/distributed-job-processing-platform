#!/bin/bash

set -e

echo "Building Docker images..."

# Build API image
docker build -t job-processing-api:latest -f docker/Dockerfile.api .

# Build Worker image
docker build -t job-processing-worker:latest -f docker/Dockerfile.worker .

# Tag images for ECR
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=${AWS_REGION:-us-east-1}
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker tag job-processing-api:latest ${ECR_REGISTRY}/job-processing-api:latest
docker tag job-processing-worker:latest ${ECR_REGISTRY}/job-processing-worker:latest

# Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

# Push images
echo "Pushing images to ECR..."
docker push ${ECR_REGISTRY}/job-processing-api:latest
docker push ${ECR_REGISTRY}/job-processing-worker:latest

echo "Deployment complete!"
