"""add execution simulations

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-21
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "execution_simulations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("simulated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("simulation_status", sa.String(length=50), nullable=False),
        sa.Column("intended_size_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("executable_size_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("gross_cost_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("gross_payout_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("estimated_fees_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("estimated_slippage_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("estimated_net_edge_usd", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("fill_completion_ratio", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("simulation_reason", sa.String(length=50), nullable=True),
        sa.Column("raw_context", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["detected_opportunities.id"],
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        "ix_execution_simulations_opportunity_id",
        "execution_simulations",
        ["opportunity_id"],
        unique=False,
    )
    op.create_index(
        "ix_execution_simulations_simulated_at",
        "execution_simulations",
        ["simulated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_execution_simulations_simulated_at", table_name="execution_simulations")
    op.drop_index("ix_execution_simulations_opportunity_id", table_name="execution_simulations")
    op.drop_table("execution_simulations")
