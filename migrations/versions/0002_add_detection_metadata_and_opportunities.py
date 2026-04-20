"""add detection metadata and detected opportunities

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-20
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("markets", sa.Column("condition_id", sa.String(length=255), nullable=True))
    op.add_column("markets", sa.Column("event_id", sa.String(length=255), nullable=True))
    op.add_column("markets", sa.Column("event_slug", sa.String(length=255), nullable=True))
    op.add_column(
        "markets",
        sa.Column("neg_risk", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index("ix_markets_condition_id", "markets", ["condition_id"], unique=False)
    op.create_index("ix_markets_event_id", "markets", ["event_id"], unique=False)

    op.create_table(
        "detected_opportunities",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("event_group_key", sa.String(length=255), nullable=False),
        sa.Column("involved_market_ids", sa.JSON(), nullable=False),
        sa.Column("opportunity_type", sa.String(length=100), nullable=False),
        sa.Column("outcome_count", sa.Integer(), nullable=False),
        sa.Column("gross_price_sum", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("gross_gap", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("detector_version", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False, server_default="detected"),
        sa.Column("raw_context", sa.JSON(), nullable=True),
    )
    op.create_index(
        "ix_detected_opportunities_detected_at",
        "detected_opportunities",
        ["detected_at"],
        unique=False,
    )
    op.create_index(
        "ix_detected_opportunities_event_group_key",
        "detected_opportunities",
        ["event_group_key"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_detected_opportunities_event_group_key", table_name="detected_opportunities")
    op.drop_index("ix_detected_opportunities_detected_at", table_name="detected_opportunities")
    op.drop_table("detected_opportunities")

    op.drop_index("ix_markets_event_id", table_name="markets")
    op.drop_index("ix_markets_condition_id", table_name="markets")
    op.drop_column("markets", "neg_risk")
    op.drop_column("markets", "event_slug")
    op.drop_column("markets", "event_id")
    op.drop_column("markets", "condition_id")
