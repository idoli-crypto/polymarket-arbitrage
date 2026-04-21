from __future__ import annotations

import logging

from sqlalchemy import func, select

from apps.api.db.models import KpiSnapshot
from apps.api.db.session import SessionLocal
from apps.worker.metrics.kpi import calculate_and_persist_kpi_snapshot


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with SessionLocal() as session:
        snapshot = calculate_and_persist_kpi_snapshot(session)
        total_snapshots = session.scalar(select(func.count()).select_from(KpiSnapshot))

    logging.info(
        "KPI snapshot completed. snapshot_id=%s total_snapshots=%s total_opportunities=%s valid_opportunities=%s executable=%s partial=%s rejected=%s avg_real_edge=%s avg_fill_ratio=%s false_positive_rate=%s total_intended_capital=%s total_executable_capital=%s created_at=%s",
        snapshot.id,
        total_snapshots,
        snapshot.total_opportunities,
        snapshot.valid_opportunities,
        snapshot.executable_opportunities,
        snapshot.partial_opportunities,
        snapshot.rejected_opportunities,
        snapshot.avg_real_edge,
        snapshot.avg_fill_ratio,
        snapshot.false_positive_rate,
        snapshot.total_intended_capital,
        snapshot.total_executable_capital,
        snapshot.created_at.isoformat(),
    )


if __name__ == "__main__":
    main()
