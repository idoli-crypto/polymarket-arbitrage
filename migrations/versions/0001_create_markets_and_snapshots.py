"""create markets and market snapshots

Revision ID: 0001
Revises:
Create Date: 2026-04-20
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "markets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("polymarket_market_id", sa.String(length=255), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_markets_polymarket_market_id", "markets", ["polymarket_market_id"], unique=True)
    op.create_index("ix_markets_slug", "markets", ["slug"], unique=True)

    op.create_table(
        "market_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market_id", sa.Integer(), nullable=False),
        sa.Column("best_bid", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("best_ask", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("bid_depth_usd", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("ask_depth_usd", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["market_id"], ["markets.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("market_id", "captured_at", name="uq_market_snapshots_market_captured_at"),
    )
    op.create_index("ix_market_snapshots_captured_at", "market_snapshots", ["captured_at"], unique=False)
    op.create_index("ix_market_snapshots_market_id", "market_snapshots", ["market_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_market_snapshots_market_id", table_name="market_snapshots")
    op.drop_index("ix_market_snapshots_captured_at", table_name="market_snapshots")
    op.drop_table("market_snapshots")
    op.drop_index("ix_markets_slug", table_name="markets")
    op.drop_index("ix_markets_polymarket_market_id", table_name="markets")
    op.drop_table("markets")
