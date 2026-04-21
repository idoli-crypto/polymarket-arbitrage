"""add kpi snapshots

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-21
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "kpi_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("total_opportunities", sa.Integer(), nullable=False),
        sa.Column("valid_opportunities", sa.Integer(), nullable=False),
        sa.Column("executable_opportunities", sa.Integer(), nullable=False),
        sa.Column("partial_opportunities", sa.Integer(), nullable=False),
        sa.Column("rejected_opportunities", sa.Integer(), nullable=False),
        sa.Column("avg_real_edge", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("avg_fill_ratio", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("false_positive_rate", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("total_intended_capital", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("total_executable_capital", sa.Numeric(precision=18, scale=4), nullable=False),
        sa.Column("raw_context", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_kpi_snapshots_created_at",
        "kpi_snapshots",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_kpi_snapshots_created_at", table_name="kpi_snapshots")
    op.drop_table("kpi_snapshots")
