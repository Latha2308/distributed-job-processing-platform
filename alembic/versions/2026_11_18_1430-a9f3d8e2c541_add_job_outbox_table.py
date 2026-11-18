"""add job outbox table

Revision ID: a9f3d8e2c541
Revises: f5ae1047b812
Create Date: 2026-11-18 14:30:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'a9f3d8e2c541'
down_revision: Union[str, None] = 'f5ae1047b812'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create job_outbox table for transactional outbox pattern"""
    op.create_table(
        'job_outbox',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('event_type', sa.String(length=50), nullable=False, server_default='job_created'),
        sa.Column('published', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('published_at', sa.DateTime(), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes
    op.create_index('ix_job_outbox_job_id', 'job_outbox', ['job_id'], unique=False)
    op.create_index('ix_job_outbox_published', 'job_outbox', ['published'], unique=False)
    op.create_index('idx_outbox_published_created', 'job_outbox', ['published', 'created_at'], unique=False)


def downgrade() -> None:
    """Drop job_outbox table"""
    op.drop_index('idx_outbox_published_created', table_name='job_outbox')
    op.drop_index('ix_job_outbox_published', table_name='job_outbox')
    op.drop_index('ix_job_outbox_job_id', table_name='job_outbox')
    op.drop_table('job_outbox')
