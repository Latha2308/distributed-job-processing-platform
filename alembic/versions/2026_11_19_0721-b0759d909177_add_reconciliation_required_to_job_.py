"""add_reconciliation_required_to_job_outbox

Add reconciliation_required flag to distinguish undeliverable events from published ones.

This enables:
- Accurate published=True meaning (Redis delivery succeeded)
- Explicit reconciliation state for invalid/stale/legacy events
- Operator visibility into events requiring manual review
- Prevention of infinite retry loops for undeliverable events

State Model:
- published=False, reconciliation_required=False: Eligible for publication/retry
- published=True, reconciliation_required=False: Successfully delivered to Redis
- published=False, reconciliation_required=True: Needs manual reconciliation
- published=True, reconciliation_required=True: Invalid (should not occur)

Revision ID: b0759d909177
Revises: 0014a2898e30
Create Date: 2026-11-19 07:21:21.944282+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b0759d909177'
down_revision = '0014a2898e30'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add reconciliation_required column
    # Default False means normal eligible/published events
    # True means needs manual review and won't be auto-retried
    op.add_column(
        'job_outbox',
        sa.Column('reconciliation_required', sa.Boolean(), nullable=False, server_default='false')
    )

    # Add index for monitoring reconciliation events
    op.create_index(
        'ix_job_outbox_reconciliation',
        'job_outbox',
        ['reconciliation_required'],
        unique=False
    )


def downgrade() -> None:
    # Drop index first
    op.drop_index('ix_job_outbox_reconciliation', table_name='job_outbox')

    # Drop column
    op.drop_column('job_outbox', 'reconciliation_required')
