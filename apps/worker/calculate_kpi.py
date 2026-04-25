from __future__ import annotations

import logging

from sqlalchemy import func, select

from apps.api.db.models import KpiRunSummary, KpiSnapshot
from apps.api.db.session import SessionLocal
from apps.worker.metrics.kpi import calculate_and_persist_kpi_snapshot


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with SessionLocal() as session:
        run_summary = calculate_and_persist_kpi_snapshot(session)
        total_run_summaries = session.scalar(select(func.count()).select_from(KpiRunSummary))
        total_snapshots = session.scalar(select(func.count()).select_from(KpiSnapshot))

    if run_summary is None:
        logging.info("KPI snapshot completed. no validated opportunities were available for KPI persistence.")
        return

    logging.info(
        "KPI snapshot completed. run_summary_id=%s total_run_summaries=%s total_legacy_snapshots=%s total_opportunities=%s valid_after_rule=%s valid_after_semantic=%s valid_after_resolution=%s valid_after_executable=%s valid_after_simulation=%s avg_executable_edge=%s avg_fill_ratio=%s avg_capital_lock=%s false_positive_rate=%s created_at=%s",
        run_summary.id,
        total_run_summaries,
        total_snapshots,
        run_summary.total_opportunities,
        run_summary.valid_after_rule,
        run_summary.valid_after_semantic,
        run_summary.valid_after_resolution,
        run_summary.valid_after_executable,
        run_summary.valid_after_simulation,
        run_summary.avg_executable_edge,
        run_summary.avg_fill_ratio,
        run_summary.avg_capital_lock,
        run_summary.false_positive_rate,
        run_summary.created_at.isoformat(),
    )


if __name__ == "__main__":
    main()
