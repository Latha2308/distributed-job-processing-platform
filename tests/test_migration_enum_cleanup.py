"""
Tests for PostgreSQL ENUM cleanup in migration downgrade.

These tests verify that application-owned ENUM types are properly
removed during downgrade operations.
"""

import subprocess
import sys
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def test_downgrade_removes_jobstatus_enum():
    """
    Test that downgrading to base removes the jobstatus ENUM type.

    This is a regression test for the issue where PostgreSQL ENUM types
    created implicitly by SQLAlchemy were not being removed during downgrade,
    preventing successful re-upgrade.
    """
    # Get database connection details from settings
    from app.core.config import settings
    import re

    db_url = str(settings.DATABASE_URL).replace("+asyncpg", "")

    # Parse connection details
    match = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", db_url)
    if not match:
        raise ValueError(f"Failed to parse DATABASE_URL: {db_url}")

    user, password, host, port, _ = match.groups()

    # Create a temporary test database
    test_db_name = "test_enum_cleanup_verification"
    base_db_url = f"postgresql://{user}:{password}@{host}:{port}/postgres"
    test_db_url = f"postgresql://{user}:{password}@{host}:{port}/{test_db_name}"

    # Create test database
    conn = psycopg2.connect(base_db_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Drop if exists from previous test run
    cursor.execute(
        """
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = %s
        AND pid <> pg_backend_pid()
        """,
        (test_db_name,),
    )
    cursor.execute(f'DROP DATABASE IF EXISTS "{test_db_name}"')

    # Create fresh test database
    cursor.execute(f'CREATE DATABASE "{test_db_name}"')
    cursor.close()
    conn.close()

    try:
        # Set environment variable for DATABASE_URL
        env = os.environ.copy()
        env["DATABASE_URL"] = test_db_url

        # Run upgrade to head
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Upgrade failed: {result.stderr}"

        # Verify jobstatus ENUM exists
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype = 'e'
            AND n.nspname = 'public'
            AND typname = 'jobstatus'
            """
        )
        enum_count = cursor.fetchone()[0]
        assert enum_count == 1, "jobstatus ENUM should exist after upgrade"
        cursor.close()
        conn.close()

        # Run downgrade to base
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "downgrade", "base"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Downgrade failed: {result.stderr}"

        # Verify jobstatus ENUM was removed
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype = 'e'
            AND n.nspname = 'public'
            AND typname = 'jobstatus'
            """
        )
        enum_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        assert enum_count == 0, "jobstatus ENUM should be removed after downgrade to base"

        # Verify we can re-upgrade successfully
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert (
            result.returncode == 0
        ), f"Re-upgrade failed (ENUM not properly cleaned): {result.stderr}"

    finally:
        # Clean up: drop test database
        conn = psycopg2.connect(base_db_url)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Terminate connections
        cursor.execute(
            """
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = %s
            AND pid <> pg_backend_pid()
            """,
            (test_db_name,),
        )

        # Drop database
        cursor.execute(f'DROP DATABASE IF EXISTS "{test_db_name}"')
        cursor.close()
        conn.close()
