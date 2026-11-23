# AWS Secrets Manager for Application Secrets

# Database URL Secret
resource "aws_secretsmanager_secret" "database_url" {
  name        = "${local.name_prefix}-database-url"
  description = "PostgreSQL database connection URL"

  tags = {
    Name = "${local.name_prefix}-database-url"
  }
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id = aws_secretsmanager_secret.database_url.id
  secret_string = jsonencode({
    url = "postgresql+asyncpg://${var.db_username}:${random_password.db_password.result}@${aws_db_instance.postgres.endpoint}/${var.db_name}"
  })
}

# Redis URL Secret
resource "aws_secretsmanager_secret" "redis_url" {
  name        = "${local.name_prefix}-redis-url"
  description = "Redis connection URL"

  tags = {
    Name = "${local.name_prefix}-redis-url"
  }
}

resource "aws_secretsmanager_secret_version" "redis_url" {
  secret_id = aws_secretsmanager_secret.redis_url.id
  secret_string = jsonencode({
    url = "redis://${aws_elasticache_cluster.redis.cache_nodes[0].address}:${aws_elasticache_cluster.redis.port}/0"
  })
}

# Application Secret Key
resource "aws_secretsmanager_secret" "secret_key" {
  name        = "${local.name_prefix}-secret-key"
  description = "Application secret key for JWT signing"

  tags = {
    Name = "${local.name_prefix}-secret-key"
  }
}

resource "aws_secretsmanager_secret_version" "secret_key" {
  secret_id = aws_secretsmanager_secret.secret_key.id
  secret_string = jsonencode({
    key = var.secret_key
  })
}
