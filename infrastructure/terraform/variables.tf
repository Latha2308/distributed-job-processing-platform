# Terraform Variables for Distributed Job Processing Platform

variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "job-processing"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# Networking
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "enable_nat_gateway" {
  description = "Enable NAT Gateway for private subnets (costs ~$32/month)"
  type        = bool
  default     = false # Set to true for production with private subnets
}

variable "enable_vpc_endpoints" {
  description = "Enable VPC endpoints for AWS services (costs ~$29/month)"
  type        = bool
  default     = true # Required if ECS tasks are in public subnets without NAT
}

# ECS Configuration
variable "api_cpu" {
  description = "CPU units for API task (256 = 0.25 vCPU)"
  type        = number
  default     = 256
}

variable "api_memory" {
  description = "Memory for API task in MB"
  type        = number
  default     = 512
}

variable "api_desired_count" {
  description = "Number of API tasks to run"
  type        = number
  default     = 1
}

variable "worker_cpu" {
  description = "CPU units for worker task"
  type        = number
  default     = 256
}

variable "worker_memory" {
  description = "Memory for worker task in MB"
  type        = number
  default     = 512
}

variable "worker_count" {
  description = "Number of worker tasks"
  type        = number
  default     = 2 # Reduced from 3 for cost savings
}

variable "dispatcher_cpu" {
  description = "CPU units for dispatcher task"
  type        = number
  default     = 256
}

variable "dispatcher_memory" {
  description = "Memory for dispatcher task in MB"
  type        = number
  default     = 512
}

variable "recovery_cpu" {
  description = "CPU units for recovery task"
  type        = number
  default     = 256
}

variable "recovery_memory" {
  description = "Memory for recovery task in MB"
  type        = number
  default     = 512
}

# RDS Configuration
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t4g.micro" # ARM-based, cheaper than t3.micro
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 20
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "jobprocessing"
}

variable "db_username" {
  description = "Database master username"
  type        = string
  default     = "dbadmin"
}

variable "db_multi_az" {
  description = "Enable Multi-AZ for RDS (costs 2x)"
  type        = bool
  default     = false # Set to true for production HA
}

variable "db_backup_retention_days" {
  description = "Number of days to retain automated backups"
  type        = number
  default     = 7
}

# ElastiCache Configuration
variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.t4g.micro" # ARM-based, cheaper
}

variable "redis_num_cache_nodes" {
  description = "Number of cache nodes"
  type        = number
  default     = 1
}

# Application Configuration
variable "secret_key" {
  description = "Application secret key (will be stored in Secrets Manager)"
  type        = string
  sensitive   = true
}

variable "domain_name" {
  description = "Domain name for API (optional, leave empty to use ALB DNS). Required for production HTTPS with ACM."
  type        = string
  default     = ""
}

variable "enable_https" {
  description = "Enable HTTPS on ALB (requires ACM certificate)"
  type        = bool
  default     = false # Set to true after obtaining ACM certificate
}

variable "acm_certificate_arn" {
  description = "ACM certificate ARN for HTTPS (required if enable_https is true). Must be in the same region as the ALB."
  type        = string
  default     = ""

  validation {
    condition     = var.enable_https == false || (var.enable_https == true && var.acm_certificate_arn != "")
    error_message = "acm_certificate_arn is required when enable_https is true. Obtain certificate from AWS Certificate Manager first."
  }
}

# CloudWatch Configuration
variable "log_retention_days" {
  description = "CloudWatch logs retention in days"
  type        = number
  default     = 7 # Short retention for cost savings
}

# Tags
variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "job-processing"
    ManagedBy   = "terraform"
    Environment = "prod"
  }
}
