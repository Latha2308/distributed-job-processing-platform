# ECS Cluster and Services

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "${local.name_prefix}-cluster"
  }
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${local.name_prefix}-api"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${local.name_prefix}-api-logs"
  }
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/ecs/${local.name_prefix}-worker"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${local.name_prefix}-worker-logs"
  }
}

resource "aws_cloudwatch_log_group" "dispatcher" {
  name              = "/ecs/${local.name_prefix}-dispatcher"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${local.name_prefix}-dispatcher-logs"
  }
}

resource "aws_cloudwatch_log_group" "recovery" {
  name              = "/ecs/${local.name_prefix}-recovery"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${local.name_prefix}-recovery-logs"
  }
}

# API Task Definition
resource "aws_ecs_task_definition" "api" {
  family                   = "${local.name_prefix}-api"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.api_cpu
  memory                   = var.api_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name  = "api"
    image = "${aws_ecr_repository.api.repository_url}:latest"

    portMappings = [{
      containerPort = 8000
      protocol      = "tcp"
    }]

    environment = [
      { name = "ENVIRONMENT", value = var.environment },
      { name = "DEBUG", value = "false" }
    ]

    secrets = [
      {
        name      = "DATABASE_URL"
        valueFrom = "${aws_secretsmanager_secret.database_url.arn}:url::"
      },
      {
        name      = "REDIS_URL"
        valueFrom = "${aws_secretsmanager_secret.redis_url.arn}:url::"
      },
      {
        name      = "SECRET_KEY"
        valueFrom = "${aws_secretsmanager_secret.secret_key.arn}:key::"
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.api.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "api"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "curl -f http://localhost:8000/health || exit 1"]
      interval    = 30
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
  }])

  tags = {
    Name = "${local.name_prefix}-api-task"
  }
}

# Worker Task Definition
resource "aws_ecs_task_definition" "worker" {
  family                   = "${local.name_prefix}-worker"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name  = "worker"
    image = "${aws_ecr_repository.worker.repository_url}:latest"

    environment = [
      { name = "ENVIRONMENT", value = var.environment },
      { name = "DEBUG", value = "false" },
      { name = "WORKER_ID", value = "worker-${uuid()}" } # Unique per task
    ]

    secrets = [
      {
        name      = "DATABASE_URL"
        valueFrom = "${aws_secretsmanager_secret.database_url.arn}:url::"
      },
      {
        name      = "REDIS_URL"
        valueFrom = "${aws_secretsmanager_secret.redis_url.arn}:url::"
      },
      {
        name      = "SECRET_KEY"
        valueFrom = "${aws_secretsmanager_secret.secret_key.arn}:key::"
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.worker.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "worker"
      }
    }
  }])

  tags = {
    Name = "${local.name_prefix}-worker-task"
  }
}

# Dispatcher Task Definition
resource "aws_ecs_task_definition" "dispatcher" {
  family                   = "${local.name_prefix}-dispatcher"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.dispatcher_cpu
  memory                   = var.dispatcher_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name    = "dispatcher"
    image   = "${aws_ecr_repository.worker.repository_url}:latest"
    command = ["python", "-m", "app.dispatcher.dispatcher_cli", "--dispatcher-id", "dispatcher-01", "--log-level", "INFO"]

    environment = [
      { name = "ENVIRONMENT", value = var.environment },
      { name = "DEBUG", value = "false" }
    ]

    secrets = [
      {
        name      = "DATABASE_URL"
        valueFrom = "${aws_secretsmanager_secret.database_url.arn}:url::"
      },
      {
        name      = "REDIS_URL"
        valueFrom = "${aws_secretsmanager_secret.redis_url.arn}:url::"
      },
      {
        name      = "SECRET_KEY"
        valueFrom = "${aws_secretsmanager_secret.secret_key.arn}:key::"
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.dispatcher.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "dispatcher"
      }
    }
  }])

  tags = {
    Name = "${local.name_prefix}-dispatcher-task"
  }
}

# Recovery Task Definition
resource "aws_ecs_task_definition" "recovery" {
  family                   = "${local.name_prefix}-recovery"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.recovery_cpu
  memory                   = var.recovery_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name  = "recovery"
    image = "${aws_ecr_repository.worker.repository_url}:latest"
    command = [
      "python", "-m", "app.recovery.recovery_cli",
      "--runner-id", "recovery-01",
      "--batch-size", "10",
      "--poll-interval", "60",
      "--queued-batch-size", "10",
      "--queued-age-threshold-minutes", "10",
      "--queued-throttle-hours", "1",
      "--log-level", "INFO"
    ]

    environment = [
      { name = "ENVIRONMENT", value = var.environment },
      { name = "DEBUG", value = "false" }
    ]

    secrets = [
      {
        name      = "DATABASE_URL"
        valueFrom = "${aws_secretsmanager_secret.database_url.arn}:url::"
      },
      {
        name      = "REDIS_URL"
        valueFrom = "${aws_secretsmanager_secret.redis_url.arn}:url::"
      },
      {
        name      = "SECRET_KEY"
        valueFrom = "${aws_secretsmanager_secret.secret_key.arn}:key::"
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.recovery.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "recovery"
      }
    }
  }])

  tags = {
    Name = "${local.name_prefix}-recovery-task"
  }
}

# API Service
resource "aws_ecs_service" "api" {
  name            = "${local.name_prefix}-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id # Use public subnets to avoid NAT Gateway costs
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true # Required when using public subnets without NAT
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8000
  }

  depends_on = [aws_lb_listener.http]

  tags = {
    Name = "${local.name_prefix}-api-service"
  }
}

# Worker Service
resource "aws_ecs_service" "worker" {
  name            = "${local.name_prefix}-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.worker.arn
  desired_count   = var.worker_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  tags = {
    Name = "${local.name_prefix}-worker-service"
  }
}

# Dispatcher Service
resource "aws_ecs_service" "dispatcher" {
  name            = "${local.name_prefix}-dispatcher"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.dispatcher.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  tags = {
    Name = "${local.name_prefix}-dispatcher-service"
  }
}

# Recovery Service
resource "aws_ecs_service" "recovery" {
  name            = "${local.name_prefix}-recovery"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.recovery.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.public[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  tags = {
    Name = "${local.name_prefix}-recovery-service"
  }
}
