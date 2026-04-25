"""add recommendation scoring runs

Revision ID: 0013
Revises: 0012
Create Date: 2026-04-24
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "recommendation_scoring_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_status", sa.String(length=20), nullable=False),
        sa.Column("opportunities_scored", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("high_conviction_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("review_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("blocked_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scoring_version", sa.String(length=50), nullable=False),
        sa.Column("run_reason", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_recommendation_scoring_runs_started_at",
        "recommendation_scoring_runs",
        ["started_at"],
        unique=False,
    )
    op.create_index(
        "ix_recommendation_scoring_runs_finished_at",
        "recommendation_scoring_runs",
        ["finished_at"],
        unique=False,
    )
    op.create_index(
        "ix_recommendation_scoring_runs_worker_status",
        "recommendation_scoring_runs",
        ["worker_status"],
        unique=False,
    )
    op.create_index(
        "ix_recommendation_scoring_runs_created_at",
        "recommendation_scoring_runs",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_recommendation_scoring_runs_created_at", table_name="recommendation_scoring_runs")
    op.drop_index("ix_recommendation_scoring_runs_worker_status", table_name="recommendation_scoring_runs")
    op.drop_index("ix_recommendation_scoring_runs_finished_at", table_name="recommendation_scoring_runs")
    op.drop_index("ix_recommendation_scoring_runs_started_at", table_name="recommendation_scoring_runs")
    op.drop_table("recommendation_scoring_runs")
