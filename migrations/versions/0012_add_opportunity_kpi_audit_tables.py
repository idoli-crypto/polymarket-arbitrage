"""add opportunity kpi audit tables

Revision ID: 0012
Revises: 0011
Create Date: 2026-04-24
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "kpi_run_summary",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("run_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("kpi_version", sa.String(length=50), nullable=False),
        sa.Column("total_opportunities", sa.Integer(), nullable=False),
        sa.Column("valid_after_rule", sa.Integer(), nullable=False),
        sa.Column("valid_after_semantic", sa.Integer(), nullable=False),
        sa.Column("valid_after_resolution", sa.Integer(), nullable=False),
        sa.Column("valid_after_executable", sa.Integer(), nullable=False),
        sa.Column("valid_after_simulation", sa.Integer(), nullable=False),
        sa.Column("avg_executable_edge", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("avg_fill_ratio", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("avg_capital_lock", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("false_positive_rate", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("family_distribution", sa.JSON(), nullable=False),
        sa.Column("detector_versions_json", sa.JSON(), nullable=False),
        sa.Column("validation_versions_json", sa.JSON(), nullable=False),
        sa.Column("simulation_versions_json", sa.JSON(), nullable=False),
        sa.Column("raw_context", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_kpi_run_summary_created_at",
        "kpi_run_summary",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_kpi_run_summary_run_started_at",
        "kpi_run_summary",
        ["run_started_at"],
        unique=False,
    )
    op.create_index(
        "ix_kpi_run_summary_run_completed_at",
        "kpi_run_summary",
        ["run_completed_at"],
        unique=False,
    )

    op.create_table(
        "opportunity_kpi_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_summary_id", sa.Integer(), sa.ForeignKey("kpi_run_summary.id", ondelete="CASCADE"), nullable=False),
        sa.Column(
            "opportunity_id",
            sa.Integer(),
            sa.ForeignKey("detected_opportunities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("lineage_key", sa.String(length=64), nullable=False),
        sa.Column("kpi_version", sa.String(length=50), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("snapshot_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("family", sa.String(length=100), nullable=False),
        sa.Column("validation_stage_reached", sa.String(length=50), nullable=False),
        sa.Column("final_status", sa.String(length=50), nullable=False),
        sa.Column("rejection_stage", sa.String(length=50), nullable=True),
        sa.Column("rejection_reason", sa.String(length=255), nullable=True),
        sa.Column("detected", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("rule_pass", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("semantic_pass", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("resolution_pass", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("executable_pass", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("simulation_pass", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("s_logic", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("s_sem", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("s_res", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("top_of_book_edge", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("depth_weighted_edge", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("fee_adjusted_edge", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("fill_completion_ratio", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("execution_feasible", sa.Boolean(), nullable=True),
        sa.Column("capital_lock_estimate_hours", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("detector_version", sa.String(length=50), nullable=False),
        sa.Column("validation_version", sa.String(length=50), nullable=True),
        sa.Column("simulation_version", sa.String(length=50), nullable=True),
        sa.Column("first_seen_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("persistence_duration_seconds", sa.Integer(), nullable=False),
        sa.Column("decay_status", sa.String(length=50), nullable=False),
        sa.Column("raw_context", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_run_summary_id",
        "opportunity_kpi_snapshots",
        ["run_summary_id"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_opportunity_id",
        "opportunity_kpi_snapshots",
        ["opportunity_id"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_lineage_key",
        "opportunity_kpi_snapshots",
        ["lineage_key"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_created_at",
        "opportunity_kpi_snapshots",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_snapshot_timestamp",
        "opportunity_kpi_snapshots",
        ["snapshot_timestamp"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_family",
        "opportunity_kpi_snapshots",
        ["family"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_final_status",
        "opportunity_kpi_snapshots",
        ["final_status"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_rejection_stage",
        "opportunity_kpi_snapshots",
        ["rejection_stage"],
        unique=False,
    )
    op.create_index(
        "ix_opportunity_kpi_snapshots_decay_status",
        "opportunity_kpi_snapshots",
        ["decay_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_opportunity_kpi_snapshots_decay_status", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_rejection_stage", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_final_status", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_family", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_snapshot_timestamp", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_created_at", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_lineage_key", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_opportunity_id", table_name="opportunity_kpi_snapshots")
    op.drop_index("ix_opportunity_kpi_snapshots_run_summary_id", table_name="opportunity_kpi_snapshots")
    op.drop_table("opportunity_kpi_snapshots")

    op.drop_index("ix_kpi_run_summary_run_completed_at", table_name="kpi_run_summary")
    op.drop_index("ix_kpi_run_summary_run_started_at", table_name="kpi_run_summary")
    op.drop_index("ix_kpi_run_summary_created_at", table_name="kpi_run_summary")
    op.drop_table("kpi_run_summary")
