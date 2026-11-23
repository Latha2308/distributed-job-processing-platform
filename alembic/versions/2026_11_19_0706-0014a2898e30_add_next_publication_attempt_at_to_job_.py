"""add_next_publication_attempt_at_to_job_outbox

Add next_publication_attempt_at for durable exponential backoff in outbox dispatcher.

This enables:
- Bounded exponential backoff for transient Redis failures
- Durable retry scheduling across dispatcher restarts
- Removal of hard max_attempts limit
- Coordination between multiple dispatcher instances

Revision ID: 0014a2898e30
Revises: 6be3b0b5e3eb
Create Date: 2026-11-19 07:06:52.124912+00:00

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime


# revision identifiers, used by Alembic.
revision = '0014a2898e30'
down_revision = '6be3b0b5e3eb'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add next_publication_attempt_at column
    # NULL means eligible for immediate publication
    # Non-NULL means wait until this timestamp before retrying
    op.add_column(
        'job_outbox',
        sa.Column('next_publication_attempt_at', sa.DateTime(), nullable=True)
    )

    # Add index for efficient polling of eligible events
    # Covers: published=false AND (next_attempt IS NULL OR next_attempt <= now)
    op.create_index(
        'ix_job_outbox_publication_eligibility',
        'job_outbox',
        ['published', 'next_publication_attempt_at'],
        unique=False
    )


def downgrade() -> None:
    # Drop index first
    op.drop_index('ix_job_outbox_publication_eligibility', table_name='job_outbox')

    # Drop column
    op.drop_column('job_outbox', 'next_publication_attempt_at')
