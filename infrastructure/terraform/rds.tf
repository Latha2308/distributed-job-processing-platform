# RDS PostgreSQL Database

# DB Subnet Group
resource "aws_db_subnet_group" "main" {
  name       = "${local.name_prefix}-db-subnet-group"
  subnet_ids = aws_subnet.private[*].id

  tags = {
    Name = "${local.name_prefix}-db-subnet-group"
  }
}

# RDS Instance
resource "aws_db_instance" "postgres" {
  identifier     = "${local.name_prefix}-postgres"
  engine         = "postgres"
  engine_version = "15.5"
  instance_class = var.db_instance_class

  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_allocated_storage * 2 # Auto-scaling up to 2x
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = var.db_name
  username = var.db_username
  password = random_password.db_password.result

  multi_az               = var.db_multi_az
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  backup_retention_period = var.db_backup_retention_days
  backup_window           = "03:00-04:00"
  maintenance_window      = "mon:04:00-mon:05:00"

  deletion_protection       = true # Prevent accidental deletion
  skip_final_snapshot       = false
  final_snapshot_identifier = "${local.name_prefix}-postgres-final-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = {
    Name = "${local.name_prefix}-postgres"
  }
}
