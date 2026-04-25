"""normalize detection family values

Revision ID: 0008
Revises: 0007
Create Date: 2026-04-22
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


LEGACY_NEG_RISK_FAMILY = "neg_risk"
CANONICAL_NEG_RISK_FAMILY = "neg_risk_conversion"


def upgrade() -> None:
    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.alter_column(
            "family",
            existing_type=sa.String(length=100),
            server_default=CANONICAL_NEG_RISK_FAMILY,
        )

    op.execute(
        sa.text(
            """
            UPDATE detected_opportunities
            SET family = :canonical_family
            WHERE family = :legacy_family OR family IS NULL
            """
        ).bindparams(
            canonical_family=CANONICAL_NEG_RISK_FAMILY,
            legacy_family=LEGACY_NEG_RISK_FAMILY,
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE detected_opportunities
            SET family = :legacy_family
            WHERE family = :canonical_family
            """
        ).bindparams(
            canonical_family=CANONICAL_NEG_RISK_FAMILY,
            legacy_family=LEGACY_NEG_RISK_FAMILY,
        )
    )

    with op.batch_alter_table("detected_opportunities") as batch_op:
        batch_op.alter_column(
            "family",
            existing_type=sa.String(length=100),
            server_default=LEGACY_NEG_RISK_FAMILY,
        )
