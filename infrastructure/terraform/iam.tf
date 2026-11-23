# IAM Roles and Policies

# ECS Task Execution Role (for pulling images, writing logs)
resource "aws_iam_role" "ecs_task_execution" {
  name = "${local.name_prefix}-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = {
    Name = "${local.name_prefix}-ecs-task-execution-role"
  }
}

# Attach AWS managed policy for ECS task execution
resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Additional policy for Secrets Manager access
resource "aws_iam_role_policy" "ecs_task_execution_secrets" {
  name = "${local.name_prefix}-ecs-secrets-policy"
  role = aws_iam_role.ecs_task_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ]
      Resource = [
        aws_secretsmanager_secret.database_url.arn,
        aws_secretsmanager_secret.redis_url.arn,
        aws_secretsmanager_secret.secret_key.arn
      ]
    }]
  })
}

# ECS Task Role (for application runtime permissions)
resource "aws_iam_role" "ecs_task" {
  name = "${local.name_prefix}-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = {
    Name = "${local.name_prefix}-ecs-task-role"
  }
}

# Policy for CloudWatch Logs access from task
resource "aws_iam_role_policy" "ecs_task_cloudwatch" {
  name = "${local.name_prefix}-ecs-cloudwatch-policy"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
      Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/ecs/${local.name_prefix}*:*"
    }]
  })
}

# Optional: Policy for S3 access if needed
# Uncomment if job processing requires S3 access
# resource "aws_iam_role_policy" "ecs_task_s3" {
#   name = "${local.name_prefix}-ecs-s3-policy"
#   role = aws_iam_role.ecs_task.id
#
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect = "Allow"
#       Action = [
#         "s3:GetObject",
#         "s3:PutObject",
#         "s3:ListBucket"
#       ]
#       Resource = [
#         "arn:aws:s3:::your-bucket/*",
#         "arn:aws:s3:::your-bucket"
#       ]
#     }]
#   })
# }
