# Main Terraform Configuration for Distributed Job Processing Platform

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }

  # S3 Backend for State Management
  # PREREQUISITES (create these manually before enabling):
  # 1. S3 bucket: aws s3 mb s3://YOUR-UNIQUE-BUCKET-NAME --region us-east-1
  # 2. Enable versioning: aws s3api put-bucket-versioning --bucket YOUR-UNIQUE-BUCKET-NAME --versioning-configuration Status=Enabled
  # 3. Enable encryption: aws s3api put-bucket-encryption --bucket YOUR-UNIQUE-BUCKET-NAME --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
  # 4. Block public access: aws s3api put-public-access-block --bucket YOUR-UNIQUE-BUCKET-NAME --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
  # 5. DynamoDB table: aws dynamodb create-table --table-name terraform-state-lock --attribute-definitions AttributeName=LockID,AttributeType=S --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST --region us-east-1
  # 6. After creating resources above, uncomment backend block below and run: terraform init -migrate-state
  #
  # backend "s3" {
  #   bucket         = "YOUR-UNIQUE-BUCKET-NAME"
  #   key            = "job-processing/prod/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-lock"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

# Generate random password for RDS
resource "random_password" "db_password" {
  length  = 32
  special = true
}

# Data source for availability zones
data "aws_availability_zones" "available" {
  state = "available"
}

# Locals for resource naming
locals {
  name_prefix = "${var.project_name}-${var.environment}"
  azs         = slice(data.aws_availability_zones.available.names, 0, 2)
}
