#!/bin/bash
# Setup S3 backend for Terraform state management
# Run this ONCE before enabling the backend in main.tf

set -e

# Configuration
BUCKET_NAME="${1:-}"
REGION="${2:-us-east-1}"
TABLE_NAME="terraform-state-lock"

if [ -z "$BUCKET_NAME" ]; then
  echo "Usage: ./setup-backend.sh <unique-bucket-name> [region]"
  echo "Example: ./setup-backend.sh mycompany-terraform-state-prod us-east-1"
  exit 1
fi

echo "Setting up Terraform backend infrastructure..."
echo "Bucket: $BUCKET_NAME"
echo "Region: $REGION"
echo "DynamoDB Table: $TABLE_NAME"
echo ""

# Create S3 bucket
echo "Creating S3 bucket..."
aws s3 mb "s3://$BUCKET_NAME" --region "$REGION"

# Enable versioning
echo "Enabling versioning..."
aws s3api put-bucket-versioning \
  --bucket "$BUCKET_NAME" \
  --versioning-configuration Status=Enabled

# Enable encryption
echo "Enabling encryption..."
aws s3api put-bucket-encryption \
  --bucket "$BUCKET_NAME" \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      },
      "BucketKeyEnabled": true
    }]
  }'

# Block public access
echo "Blocking public access..."
aws s3api put-public-access-block \
  --bucket "$BUCKET_NAME" \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Enable bucket lifecycle for old versions
echo "Configuring lifecycle policy..."
aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET_NAME" \
  --lifecycle-configuration '{
    "Rules": [{
      "Id": "DeleteOldVersions",
      "Status": "Enabled",
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 90
      }
    }]
  }'

# Create DynamoDB table for state locking
echo "Creating DynamoDB table for state locking..."
aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" \
  --tags Key=Purpose,Value=TerraformStateLock Key=Project,Value=job-processing

echo ""
echo "✓ Backend infrastructure created successfully"
echo ""
echo "Next steps:"
echo "1. Update main.tf backend block with bucket name: $BUCKET_NAME"
echo "2. Uncomment the backend block in main.tf"
echo "3. Run: terraform init -migrate-state"
echo ""
echo "Backend configuration:"
echo "  bucket         = \"$BUCKET_NAME\""
echo "  key            = \"job-processing/prod/terraform.tfstate\""
echo "  region         = \"$REGION\""
echo "  encrypt        = true"
echo "  dynamodb_table = \"$TABLE_NAME\""
