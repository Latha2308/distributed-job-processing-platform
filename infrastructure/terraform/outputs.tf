# Terraform Outputs

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "alb_dns_name" {
  description = "ALB DNS name for API access"
  value       = aws_lb.main.dns_name
}

output "alb_url" {
  description = "Full URL to access the API"
  value       = var.enable_https ? "https://${var.domain_name != "" ? var.domain_name : aws_lb.main.dns_name}" : "http://${aws_lb.main.dns_name}"
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.postgres.endpoint
  sensitive   = true
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = "${aws_elasticache_cluster.redis.cache_nodes[0].address}:${aws_elasticache_cluster.redis.port}"
  sensitive   = true
}

output "ecr_api_repository_url" {
  description = "ECR repository URL for API image"
  value       = aws_ecr_repository.api.repository_url
}

output "ecr_worker_repository_url" {
  description = "ECR repository URL for worker image"
  value       = aws_ecr_repository.worker.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "ecs_security_group_id" {
  description = "ECS tasks security group ID"
  value       = aws_security_group.ecs_tasks.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

output "database_url_secret_arn" {
  description = "ARN of Secrets Manager secret containing database URL"
  value       = aws_secretsmanager_secret.database_url.arn
}

output "redis_url_secret_arn" {
  description = "ARN of Secrets Manager secret containing Redis URL"
  value       = aws_secretsmanager_secret.redis_url.arn
}

output "deployment_commands" {
  description = "Commands to deploy the application"
  value       = <<-EOT
    # 1. Authenticate with ECR
    aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${aws_ecr_repository.api.repository_url}

    # 2. Build and push API image
    docker build -f docker/Dockerfile.api -t ${aws_ecr_repository.api.repository_url}:latest .
    docker push ${aws_ecr_repository.api.repository_url}:latest

    # 3. Build and push worker image
    docker build -f docker/Dockerfile.worker -t ${aws_ecr_repository.worker.repository_url}:latest .
    docker push ${aws_ecr_repository.worker.repository_url}:latest

    # 4. Run database migrations (from a bastion or local with VPN)
    # Option A: Run from ECS task
    aws ecs run-task --cluster ${aws_ecs_cluster.main.name} --task-definition ${aws_ecs_task_definition.api.family} --launch-type FARGATE --network-configuration "awsvpcConfiguration={subnets=[${join(",", aws_subnet.public[*].id)}],securityGroups=[${aws_security_group.ecs_tasks.id}],assignPublicIp=ENABLED}" --overrides '{"containerOverrides":[{"name":"api","command":["alembic","upgrade","head"]}]}'

    # Option B: Run locally with port forwarding
    # DATABASE_URL="postgresql+asyncpg://${var.db_username}:PASSWORD@${aws_db_instance.postgres.endpoint}/${var.db_name}" alembic upgrade head

    # 5. Force new deployment (optional, to pick up new images)
    aws ecs update-service --cluster ${aws_ecs_cluster.main.name} --service ${aws_ecs_service.api.name} --force-new-deployment
    aws ecs update-service --cluster ${aws_ecs_cluster.main.name} --service ${aws_ecs_service.worker.name} --force-new-deployment
    aws ecs update-service --cluster ${aws_ecs_cluster.main.name} --service ${aws_ecs_service.dispatcher.name} --force-new-deployment
    aws ecs update-service --cluster ${aws_ecs_cluster.main.name} --service ${aws_ecs_service.recovery.name} --force-new-deployment

    # 6. Test the deployment
    curl ${var.enable_https ? "https" : "http"}://${var.domain_name != "" ? var.domain_name : aws_lb.main.dns_name}/health
  EOT
}
