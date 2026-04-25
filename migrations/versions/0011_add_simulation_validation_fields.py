"""add simulation validation fields

Revision ID: 0011
Revises: 0010
Create Date: 2026-04-24
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("simulation_results") as batch_op:
        batch_op.add_column(sa.Column("fill_completion_ratio", sa.Numeric(precision=10, scale=4), nullable=True))
        batch_op.add_column(sa.Column("execution_feasible", sa.Boolean(), nullable=True))
        batch_op.add_column(sa.Column("execution_risk_flag", sa.String(length=50), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("simulation_results") as batch_op:
        batch_op.drop_column("execution_risk_flag")
        batch_op.drop_column("execution_feasible")
        batch_op.drop_column("fill_completion_ratio")
