"""add validation fields to detected opportunities

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-21
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.add_column(sa.Column("validation_status", sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column("validation_reason", sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column("validated_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.drop_column("validated_at")
        batch_op.drop_column("validation_reason")
        batch_op.drop_column("validation_status")
