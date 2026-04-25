from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.db.models import KpiRunSummary, KpiSnapshot, OpportunityKpiSnapshot


def get_latest_opportunity_kpi_snapshot(
    session: Session,
    opportunity_id: int,
) -> OpportunityKpiSnapshot | None:
    return session.scalar(
        select(OpportunityKpiSnapshot)
        .where(OpportunityKpiSnapshot.opportunity_id == opportunity_id)
        .order_by(
            OpportunityKpiSnapshot.snapshot_timestamp.desc(),
            OpportunityKpiSnapshot.id.desc(),
        )
        .limit(1)
    )


def get_kpi_run_summary(
    session: Session,
    run_id: int,
) -> KpiRunSummary | None:
    return session.scalar(select(KpiRunSummary).where(KpiRunSummary.id == run_id))


def get_latest_kpi_run_summary(session: Session) -> KpiRunSummary | None:
    return session.scalar(
        select(KpiRunSummary)
        .order_by(KpiRunSummary.run_completed_at.desc(), KpiRunSummary.id.desc())
        .limit(1)
    )


def get_latest_legacy_kpi_snapshot(session: Session) -> KpiSnapshot | None:
    return session.scalar(
        select(KpiSnapshot)
        .order_by(KpiSnapshot.created_at.desc(), KpiSnapshot.id.desc())
        .limit(1)
    )
