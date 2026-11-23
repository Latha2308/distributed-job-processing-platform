"""add retry attempt to job outbox

Revision ID: 6be3b0b5e3eb
Revises: a9f3d8e2c541
Create Date: 2026-11-19 06:21:12.966111+00:00

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6be3b0b5e3eb"
down_revision = "a9f3d8e2c541"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add retry_attempt column to job_outbox for immutable attempt tracking"""
    # Add nullable integer column for retry attempt tracking
    # NULL for job_created events, set for job_recovered events
    op.add_column("job_outbox", sa.Column("retry_attempt", sa.Integer(), nullable=True))

    # Create index for efficient querying by job_id and retry_attempt
    op.create_index("idx_outbox_job_retry", "job_outbox", ["job_id", "retry_attempt"], unique=False)


def downgrade() -> None:
    """Remove retry_attempt column from job_outbox"""
    op.drop_index("idx_outbox_job_retry", table_name="job_outbox")
    op.drop_column("job_outbox", "retry_attempt")
