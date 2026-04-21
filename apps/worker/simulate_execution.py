from __future__ import annotations

import logging

from sqlalchemy import func, select

from apps.api.db.models import ExecutionSimulation
from apps.api.db.session import SessionLocal
from apps.worker.execution_simulation import simulate_pending_validated_opportunities


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with SessionLocal() as session:
        persisted = simulate_pending_validated_opportunities(session)
        total_count = session.scalar(select(func.count()).select_from(ExecutionSimulation))
        executable_count = session.scalar(
            select(func.count())
            .select_from(ExecutionSimulation)
            .where(ExecutionSimulation.simulation_status == "executable")
        )
        partial_count = session.scalar(
            select(func.count())
            .select_from(ExecutionSimulation)
            .where(ExecutionSimulation.simulation_status == "partially_executable")
        )
        rejected_count = session.scalar(
            select(func.count())
            .select_from(ExecutionSimulation)
            .where(ExecutionSimulation.simulation_status == "rejected")
        )

    logging.info(
        "Execution simulation completed. simulated_opportunities=%s total_simulations=%s executable=%s partially_executable=%s rejected=%s",
        len(persisted),
        total_count,
        executable_count,
        partial_count,
        rejected_count,
    )
    for result in persisted:
        logging.info(
            "simulation id=%s opportunity_id=%s status=%s reason=%s executable_size_usd=%s estimated_net_edge_usd=%s fill_completion_ratio=%s simulated_at=%s",
            result.id,
            result.opportunity_id,
            result.simulation_status,
            result.simulation_reason,
            result.executable_size_usd,
            result.estimated_net_edge_usd,
            result.fill_completion_ratio,
            result.simulated_at.isoformat(),
        )


if __name__ == "__main__":
    main()
