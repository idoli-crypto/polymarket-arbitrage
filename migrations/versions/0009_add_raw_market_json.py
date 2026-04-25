"""add raw market json

Revision ID: 0009
Revises: 0008
Create Date: 2026-04-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("markets") as batch_op:
        batch_op.add_column(sa.Column("raw_market_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("markets") as batch_op:
        batch_op.drop_column("raw_market_json")
