"""add order book json to market snapshots

Revision ID: 0010
Revises: 0009
Create Date: 2026-04-24
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("market_snapshots") as batch_op:
        batch_op.add_column(sa.Column("order_book_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("market_snapshots") as batch_op:
        batch_op.drop_column("order_book_json")
