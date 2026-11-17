#!/bin/bash

set -e

AWS_REGION=${AWS_REGION:-us-east-1}

echo "Setting up AWS resources for Distributed Job Processing Platform..."

# Create ECR repositories
echo "Creating ECR repositories..."
aws ecr create-repository \
    --repository-name job-processing-api \
    --region ${AWS_REGION} \
    --image-scanning-configuration scanOnPush=true \
    || echo "API repository already exists"

aws ecr create-repository \
    --repository-name job-processing-worker \
    --region ${AWS_REGION} \
    --image-scanning-configuration scanOnPush=true \
    || echo "Worker repository already exists"

# Create SQS queues
echo "Creating SQS queues..."
aws sqs create-queue \
    --queue-name job-queue \
    --region ${AWS_REGION} \
    --attributes VisibilityTimeout=300 \
    || echo "Job queue already exists"

aws sqs create-queue \
    --queue-name job-dlq \
    --region ${AWS_REGION} \
    || echo "DLQ already exists"

# Create ECS cluster
echo "Creating ECS cluster..."
aws ecs create-cluster \
    --cluster-name job-processing-cluster \
    --region ${AWS_REGION} \
    || echo "Cluster already exists"

echo "AWS setup complete!"
echo ""
echo "Next steps:"
echo "1. Update .env file with SQS queue URLs"
echo "2. Create ECS task definitions for API and Worker"
echo "3. Create ECS services"
echo "4. Run ./scripts/deploy.sh to build and push images"
