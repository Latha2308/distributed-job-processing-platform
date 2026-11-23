"""add_last_recovery_attempt_at_to_jobs

Revision ID: 64de3058b3b7
Revises: b0759d909177
Create Date: 2026-11-20 19:31:22.192430+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '64de3058b3b7'
down_revision = 'b0759d909177'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add last_recovery_attempt_at column for throttling recovery attempts
    # NULL means job has never been subject to recovery
    # Non-NULL means last recovery attempt occurred at this timestamp
    op.add_column('jobs', sa.Column('last_recovery_attempt_at', sa.DateTime(), nullable=True))

    # Add index for efficient recovery query:
    # SELECT id FROM jobs
    # WHERE status = 'QUEUED'
    # AND created_at < NOW() - INTERVAL '10 minutes'
    # AND (last_recovery_attempt_at IS NULL
    #      OR last_recovery_attempt_at < NOW() - INTERVAL '1 hour')
    # ORDER BY created_at ASC
    # LIMIT 100
    #
    # Composite index on (status, last_recovery_attempt_at) to support:
    # 1. Filter by status = 'QUEUED'
    # 2. Filter by last_recovery_attempt_at IS NULL or < threshold
    # Note: created_at already has its own index (idx_created_at)
    op.create_index(
        'idx_jobs_status_recovery_attempt',
        'jobs',
        ['status', 'last_recovery_attempt_at'],
        unique=False
    )


def downgrade() -> None:
    # Remove index first, then column
    op.drop_index('idx_jobs_status_recovery_attempt', table_name='jobs')
    op.drop_column('jobs', 'last_recovery_attempt_at')
