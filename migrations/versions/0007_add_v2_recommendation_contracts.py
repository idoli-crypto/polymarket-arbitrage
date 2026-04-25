"""add v2 recommendation contracts

Revision ID: 0007
Revises: 0006
Create Date: 2026-04-22
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


CONFIDENCE_TIER_CHECK = (
    "confidence_tier IS NULL OR confidence_tier IN ('Low', 'Medium', 'High')"
)


def upgrade() -> None:
    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.add_column(
            sa.Column("family", sa.String(length=100), nullable=False, server_default="neg_risk")
        )
        batch_op.add_column(sa.Column("relation_type", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("relation_direction", sa.String(length=100), nullable=True))
        batch_op.add_column(sa.Column("involved_market_ids_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("question_texts_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("normalized_entities_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("normalized_dates_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("normalized_thresholds_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("resolution_sources_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("end_dates_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("clarification_flags_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("dispute_flags_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("s_logic", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("s_sem", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("s_res", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("confidence", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("confidence_tier", sa.String(length=20), nullable=True))
        batch_op.add_column(sa.Column("top_of_book_edge", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("depth_weighted_edge", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("fee_adjusted_edge", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("min_executable_size", sa.Numeric(precision=18, scale=4), nullable=True))
        batch_op.add_column(sa.Column("suggested_notional_bucket", sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column("persistence_seconds_estimate", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column("capital_lock_estimate_hours", sa.Numeric(precision=10, scale=4), nullable=True)
        )
        batch_op.add_column(sa.Column("validation_version", sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column("simulation_version", sa.String(length=50), nullable=True))
        batch_op.add_column(sa.Column("risk_flags_json", sa.JSON(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "recommendation_eligibility",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch_op.add_column(sa.Column("recommendation_block_reason", sa.String(length=255), nullable=True))
        batch_op.create_check_constraint(
            "ck_detected_opportunities_confidence_tier",
            CONFIDENCE_TIER_CHECK,
        )

    op.create_table(
        "validation_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("validation_type", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("score", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("validator_version", sa.String(length=50), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["detected_opportunities.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_validation_results_opportunity_id", "validation_results", ["opportunity_id"], unique=False)
    op.create_index("ix_validation_results_created_at", "validation_results", ["created_at"], unique=False)

    op.create_table(
        "simulation_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("simulation_mode", sa.String(length=100), nullable=False),
        sa.Column("executable_edge", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("fee_cost", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("slippage_cost", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("estimated_fill_quality", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("min_executable_size", sa.Numeric(precision=18, scale=4), nullable=True),
        sa.Column("suggested_notional_bucket", sa.String(length=50), nullable=True),
        sa.Column("persistence_seconds_estimate", sa.Integer(), nullable=True),
        sa.Column("capital_lock_estimate_hours", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("simulation_version", sa.String(length=50), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["detected_opportunities.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_simulation_results_opportunity_id", "simulation_results", ["opportunity_id"], unique=False)
    op.create_index("ix_simulation_results_created_at", "simulation_results", ["created_at"], unique=False)

    op.create_table(
        "recommendation_scores",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("opportunity_id", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("tier", sa.String(length=20), nullable=True),
        sa.Column("reason_summary", sa.Text(), nullable=True),
        sa.Column("warning_summary", sa.Text(), nullable=True),
        sa.Column(
            "manual_review_required",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("scoring_version", sa.String(length=50), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["opportunity_id"], ["detected_opportunities.id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_recommendation_scores_opportunity_id",
        "recommendation_scores",
        ["opportunity_id"],
        unique=False,
    )
    op.create_index("ix_recommendation_scores_created_at", "recommendation_scores", ["created_at"], unique=False)

    op.execute(
        sa.text(
            """
            UPDATE detected_opportunities
            SET family = COALESCE(family, 'neg_risk'),
                involved_market_ids_json = COALESCE(involved_market_ids_json, involved_market_ids),
                recommendation_eligibility = COALESCE(recommendation_eligibility, false)
            """
        )
    )


def downgrade() -> None:
    op.drop_index("ix_recommendation_scores_created_at", table_name="recommendation_scores")
    op.drop_index("ix_recommendation_scores_opportunity_id", table_name="recommendation_scores")
    op.drop_table("recommendation_scores")

    op.drop_index("ix_simulation_results_created_at", table_name="simulation_results")
    op.drop_index("ix_simulation_results_opportunity_id", table_name="simulation_results")
    op.drop_table("simulation_results")

    op.drop_index("ix_validation_results_created_at", table_name="validation_results")
    op.drop_index("ix_validation_results_opportunity_id", table_name="validation_results")
    op.drop_table("validation_results")

    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.drop_constraint("ck_detected_opportunities_confidence_tier", type_="check")
        batch_op.drop_column("recommendation_block_reason")
        batch_op.drop_column("recommendation_eligibility")
        batch_op.drop_column("risk_flags_json")
        batch_op.drop_column("simulation_version")
        batch_op.drop_column("validation_version")
        batch_op.drop_column("capital_lock_estimate_hours")
        batch_op.drop_column("persistence_seconds_estimate")
        batch_op.drop_column("suggested_notional_bucket")
        batch_op.drop_column("min_executable_size")
        batch_op.drop_column("fee_adjusted_edge")
        batch_op.drop_column("depth_weighted_edge")
        batch_op.drop_column("top_of_book_edge")
        batch_op.drop_column("confidence_tier")
        batch_op.drop_column("confidence")
        batch_op.drop_column("s_res")
        batch_op.drop_column("s_sem")
        batch_op.drop_column("s_logic")
        batch_op.drop_column("dispute_flags_json")
        batch_op.drop_column("clarification_flags_json")
        batch_op.drop_column("end_dates_json")
        batch_op.drop_column("resolution_sources_json")
        batch_op.drop_column("normalized_thresholds_json")
        batch_op.drop_column("normalized_dates_json")
        batch_op.drop_column("normalized_entities_json")
        batch_op.drop_column("question_texts_json")
        batch_op.drop_column("involved_market_ids_json")
        batch_op.drop_column("relation_direction")
        batch_op.drop_column("relation_type")
        batch_op.drop_column("family")
