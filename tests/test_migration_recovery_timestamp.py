"""
Tests for last_recovery_attempt_at migration (Step 289A).

These tests verify that:
1. The migration adds the last_recovery_attempt_at column with correct type and constraints
2. The migration creates the idx_jobs_status_recovery_attempt index
3. Existing jobs remain valid with NULL in the new column
4. The migration is reversible (downgrade removes column and index cleanly)
5. The database can be upgraded, downgraded, and re-upgraded successfully
"""

import subprocess
import sys
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime
import uuid


def test_recovery_timestamp_migration_upgrade_downgrade():
    """
    Test that the last_recovery_attempt_at migration is reversible.

    Verifies:
    - Upgrade adds column and index
    - Existing jobs remain valid with NULL
    - Downgrade removes column and index
    - Can re-upgrade successfully
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
    test_db_name = "test_recovery_timestamp_migration"
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

        # Run upgrade to head (includes the new migration)
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Upgrade failed: {result.stderr}"

        # Verify column exists with correct type and constraints
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'jobs'
            AND column_name = 'last_recovery_attempt_at'
            """
        )
        column_info = cursor.fetchone()
        assert column_info is not None, "last_recovery_attempt_at column should exist"
        column_name, data_type, is_nullable = column_info
        assert column_name == "last_recovery_attempt_at"
        assert data_type == "timestamp without time zone", f"Expected timestamp, got {data_type}"
        assert is_nullable == "YES", "Column should be nullable"

        # Verify index exists
        cursor.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'jobs'
            AND indexname = 'idx_jobs_status_recovery_attempt'
            """
        )
        index_info = cursor.fetchone()
        assert index_info is not None, "idx_jobs_status_recovery_attempt index should exist"

        # Insert a test job and verify NULL is valid
        job_id = str(uuid.uuid4())
        cursor.execute(
            """
            INSERT INTO jobs (id, job_type, payload, status, created_at, updated_at)
            VALUES (%s, 'test_job', '{}', 'QUEUED', %s, %s)
            """,
            (job_id, datetime.utcnow(), datetime.utcnow()),
        )

        cursor.execute(
            """
            SELECT last_recovery_attempt_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        recovery_timestamp = cursor.fetchone()[0]
        assert recovery_timestamp is None, "New jobs should have NULL last_recovery_attempt_at"

        # Update the timestamp to verify non-NULL is also valid
        test_timestamp = datetime.utcnow()
        cursor.execute(
            """
            UPDATE jobs
            SET last_recovery_attempt_at = %s
            WHERE id = %s
            """,
            (test_timestamp, job_id),
        )

        cursor.execute(
            """
            SELECT last_recovery_attempt_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        updated_timestamp = cursor.fetchone()[0]
        assert updated_timestamp is not None, "Timestamp should be updatable to non-NULL"

        conn.commit()
        cursor.close()
        conn.close()

        # Run downgrade to previous revision (b0759d909177)
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "downgrade", "b0759d909177"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Downgrade failed: {result.stderr}"

        # Verify column was removed
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'jobs'
            AND column_name = 'last_recovery_attempt_at'
            """
        )
        column_count = cursor.fetchone()[0]
        assert (
            column_count == 0
        ), "last_recovery_attempt_at column should be removed after downgrade"

        # Verify index was removed
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE tablename = 'jobs'
            AND indexname = 'idx_jobs_status_recovery_attempt'
            """
        )
        index_count = cursor.fetchone()[0]
        assert (
            index_count == 0
        ), "idx_jobs_status_recovery_attempt index should be removed after downgrade"

        cursor.close()
        conn.close()

        # Verify we can re-upgrade successfully
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Re-upgrade failed: {result.stderr}"

        # Verify column and index are back after re-upgrade
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'jobs'
            AND column_name = 'last_recovery_attempt_at'
            """
        )
        column_count = cursor.fetchone()[0]
        assert column_count == 1, "Column should exist after re-upgrade"

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_indexes
            WHERE tablename = 'jobs'
            AND indexname = 'idx_jobs_status_recovery_attempt'
            """
        )
        index_count = cursor.fetchone()[0]
        assert index_count == 1, "Index should exist after re-upgrade"

        cursor.close()
        conn.close()

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


def test_recovery_timestamp_existing_jobs_remain_valid():
    """
    Test that existing jobs remain valid after adding last_recovery_attempt_at column.

    This test:
    1. Creates jobs before the migration
    2. Runs the migration
    3. Verifies existing jobs have NULL in the new column
    4. Verifies existing jobs can still be queried and updated
    """
    from app.core.config import settings
    import re

    db_url = str(settings.DATABASE_URL).replace("+asyncpg", "")

    # Parse connection details
    match = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", db_url)
    if not match:
        raise ValueError(f"Failed to parse DATABASE_URL: {db_url}")

    user, password, host, port, _ = match.groups()

    # Create a temporary test database
    test_db_name = "test_recovery_timestamp_existing_jobs"
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

        # Run upgrade to revision BEFORE the new migration (b0759d909177)
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "b0759d909177"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Upgrade to previous revision failed: {result.stderr}"

        # Create test jobs BEFORE the migration
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()

        job_ids = []
        for i in range(5):
            job_id = str(uuid.uuid4())
            job_ids.append(job_id)
            cursor.execute(
                """
                INSERT INTO jobs (id, job_type, payload, status, created_at, updated_at)
                VALUES (%s, %s, '{}', 'QUEUED', %s, %s)
                """,
                (job_id, f"test_job_{i}", datetime.utcnow(), datetime.utcnow()),
            )

        conn.commit()
        cursor.close()
        conn.close()

        # Run the new migration (upgrade to head)
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode == 0, f"Upgrade to head failed: {result.stderr}"

        # Verify existing jobs have NULL in the new column
        conn = psycopg2.connect(test_db_url)
        cursor = conn.cursor()

        for job_id in job_ids:
            cursor.execute(
                """
                SELECT id, job_type, status, last_recovery_attempt_at
                FROM jobs
                WHERE id = %s
                """,
                (job_id,),
            )
            row = cursor.fetchone()
            assert row is not None, f"Job {job_id} should still exist after migration"
            assert (
                row[3] is None
            ), f"Existing job {job_id} should have NULL last_recovery_attempt_at"

        # Verify we can still query and update existing jobs
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM jobs
            WHERE status = 'QUEUED'
            AND last_recovery_attempt_at IS NULL
            """
        )
        count = cursor.fetchone()[0]
        assert count == 5, "Should find all 5 existing jobs with NULL recovery timestamp"

        # Update one job's recovery timestamp
        test_timestamp = datetime.utcnow()
        cursor.execute(
            """
            UPDATE jobs
            SET last_recovery_attempt_at = %s
            WHERE id = %s
            """,
            (test_timestamp, job_ids[0]),
        )

        cursor.execute(
            """
            SELECT last_recovery_attempt_at
            FROM jobs
            WHERE id = %s
            """,
            (job_ids[0],),
        )
        updated_timestamp = cursor.fetchone()[0]
        assert updated_timestamp is not None, "Should be able to update recovery timestamp"

        conn.commit()
        cursor.close()
        conn.close()

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
