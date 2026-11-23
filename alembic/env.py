"""Alembic environment configuration"""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

# this is the Alembic Config object
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set SQLAlchemy URL from DATABASE_URL env var if present, otherwise from settings
# This allows migration verification scripts to override the database target
database_url = os.environ.get("DATABASE_URL")
if database_url:
    # Convert asyncpg to psycopg2 for Alembic if needed
    if "+asyncpg" in database_url:
        database_url = database_url.replace("+asyncpg", "")
    config.set_main_option("sqlalchemy.url", database_url)
else:
    from app.core.config import settings
    config.set_main_option("sqlalchemy.url", settings.database_url_sync)

# Import Base from engine-independent module
# This avoids triggering async engine creation with asyncpg driver
from app.db.base import Base
from app.db import models  # noqa: F401  # Import models to register them with Base

# add your model's MetaData object here for 'autogenerate' support
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
