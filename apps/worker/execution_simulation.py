from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, Market, MarketSnapshot
from apps.api.repositories.opportunities import attach_simulation_result
from apps.worker.simulators.execution import (
    DEFAULT_INTENDED_SIZE_USD,
    ExecutionSimulationResult,
    MONEY_PRECISION,
    SIMULATION_VERSION,
    SimulationOpportunityInput,
    SimulationSnapshotInput,
    simulate_validated_opportunity,
)


@dataclass(slots=True)
class PersistedExecutionSimulation:
    id: int
    opportunity_id: int
    simulated_at: datetime
    simulation_status: str
    executable_size_usd: Decimal
    estimated_net_edge_usd: Decimal
    fill_completion_ratio: Decimal
    simulation_reason: str | None


def simulate_pending_validated_opportunities(
    session: Session,
    *,
    intended_size_usd: Decimal = DEFAULT_INTENDED_SIZE_USD,
) -> list[PersistedExecutionSimulation]:
    simulation_exists = (
        select(ExecutionSimulation.id)
        .where(ExecutionSimulation.opportunity_id == DetectedOpportunity.id)
        .exists()
    )
    opportunities = session.scalars(
        select(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status == "valid")
        .where(~simulation_exists)
        .order_by(DetectedOpportunity.validated_at.asc(), DetectedOpportunity.id.asc())
    ).all()

    all_market_ids = sorted(
        {
            market_id
            for opportunity in opportunities
            for market_id in opportunity.involved_market_ids
        }
    )
    latest_snapshots = _load_latest_snapshots(session, all_market_ids)

    persisted: list[PersistedExecutionSimulation] = []
    for opportunity in opportunities:
        result = simulate_validated_opportunity(
            SimulationOpportunityInput(
                opportunity_id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                involved_market_ids=list(opportunity.involved_market_ids),
                opportunity_type=opportunity.opportunity_type,
                gross_price_sum=opportunity.gross_price_sum,
                detection_window_start=opportunity.detection_window_start,
                raw_context=opportunity.raw_context,
            ),
            latest_snapshots={
                market_id: latest_snapshots.get(market_id)
                for market_id in opportunity.involved_market_ids
            },
            intended_size_usd=intended_size_usd,
        )
        simulation = ExecutionSimulation(
            opportunity_id=opportunity.id,
            simulated_at=datetime.now(timezone.utc),
            simulation_status=result.simulation_status,
            intended_size_usd=result.intended_size_usd,
            executable_size_usd=result.executable_size_usd,
            gross_cost_usd=result.gross_cost_usd,
            gross_payout_usd=result.gross_payout_usd,
            estimated_fees_usd=result.estimated_fees_usd,
            estimated_slippage_usd=result.estimated_slippage_usd,
            estimated_net_edge_usd=result.estimated_net_edge_usd,
            fill_completion_ratio=result.fill_completion_ratio,
            simulation_reason=result.simulation_reason,
            raw_context=_merge_simulation_context(opportunity.raw_context, result),
        )
        session.add(simulation)
        session.flush()
        executable_edge = _derive_executable_edge(result)
        opportunity.simulation_version = SIMULATION_VERSION
        attach_simulation_result(
            session,
            opportunity.id,
            simulation_mode="top_of_book_execution",
            executable_edge=executable_edge,
            fee_cost=result.estimated_fees_usd,
            slippage_cost=result.estimated_slippage_usd,
            estimated_fill_quality=result.fill_completion_ratio,
            fill_completion_ratio=result.fill_completion_ratio,
            execution_feasible=result.simulation_status == "executable",
            min_executable_size=result.executable_size_usd,
            persistence_seconds_estimate=None,
            capital_lock_estimate_hours=None,
            execution_risk_flag=result.simulation_reason,
            suggested_notional_bucket=None,
            simulation_version=SIMULATION_VERSION,
            details_json=result.raw_context,
            created_at=simulation.simulated_at,
        )
        persisted.append(
            PersistedExecutionSimulation(
                id=simulation.id,
                opportunity_id=simulation.opportunity_id,
                simulated_at=simulation.simulated_at,
                simulation_status=simulation.simulation_status,
                executable_size_usd=simulation.executable_size_usd,
                estimated_net_edge_usd=simulation.estimated_net_edge_usd,
                fill_completion_ratio=simulation.fill_completion_ratio,
                simulation_reason=simulation.simulation_reason,
            )
        )

    session.commit()
    return persisted


def _load_latest_snapshots(
    session: Session,
    market_ids: list[int],
) -> dict[int, SimulationSnapshotInput | None]:
    latest_snapshot_by_market: dict[int, SimulationSnapshotInput | None] = {
        market_id: None for market_id in market_ids
    }
    if not market_ids:
        return latest_snapshot_by_market

    ranked_snapshots = (
        select(
            MarketSnapshot.id.label("snapshot_id"),
            MarketSnapshot.market_id.label("market_id"),
            MarketSnapshot.captured_at.label("captured_at"),
            MarketSnapshot.best_bid.label("best_bid"),
            MarketSnapshot.best_ask.label("best_ask"),
            MarketSnapshot.bid_depth_usd.label("bid_depth_usd"),
            MarketSnapshot.ask_depth_usd.label("ask_depth_usd"),
            MarketSnapshot.order_book_json.label("order_book_json"),
            Market.raw_market_json.label("raw_market_json"),
            func.row_number()
            .over(
                partition_by=MarketSnapshot.market_id,
                order_by=(MarketSnapshot.captured_at.desc(), MarketSnapshot.id.desc()),
            )
            .label("snapshot_rank"),
        )
        .join(Market, Market.id == MarketSnapshot.market_id)
        .where(MarketSnapshot.market_id.in_(market_ids))
        .subquery()
    )

    rows = session.execute(
        select(ranked_snapshots).where(ranked_snapshots.c.snapshot_rank == 1)
    ).mappings()
    for row in rows:
        latest_snapshot_by_market[row["market_id"]] = SimulationSnapshotInput(
            snapshot_id=row["snapshot_id"],
            market_id=row["market_id"],
            captured_at=row["captured_at"],
            best_bid=row["best_bid"],
            best_ask=row["best_ask"],
            bid_depth_usd=row["bid_depth_usd"],
            ask_depth_usd=row["ask_depth_usd"],
            order_book_json=row["order_book_json"],
            raw_market_json=row["raw_market_json"],
        )

    return latest_snapshot_by_market


def _merge_simulation_context(
    raw_context: dict | None,
    result: ExecutionSimulationResult,
) -> dict:
    merged = dict(raw_context or {})
    merged["execution_simulation_status"] = result.simulation_status
    merged["execution_simulation_reason"] = result.simulation_reason
    merged["execution_simulation"] = result.raw_context
    return merged


def _derive_executable_edge(result: ExecutionSimulationResult) -> Decimal | None:
    if result.executable_size_usd <= Decimal("0"):
        return None

    return (result.estimated_net_edge_usd / result.executable_size_usd).quantize(MONEY_PRECISION)
