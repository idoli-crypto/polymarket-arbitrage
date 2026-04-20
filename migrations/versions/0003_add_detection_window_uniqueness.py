"""add detection window uniqueness for opportunities

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-20
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "detected_opportunities",
        sa.Column("detection_window_start", sa.DateTime(timezone=True), nullable=True),
    )

    op.execute(
        """
        UPDATE detected_opportunities
        SET detection_window_start = detected_at
        WHERE detection_window_start IS NULL
        """
    )

    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.alter_column("detection_window_start", nullable=False)
        batch_op.create_unique_constraint(
            "uq_detected_opportunities_event_type_version_window",
            ["event_group_key", "opportunity_type", "detector_version", "detection_window_start"],
        )


def downgrade() -> None:
    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.drop_constraint(
            "uq_detected_opportunities_event_type_version_window",
            type_="unique",
        )
        batch_op.drop_column("detection_window_start")
