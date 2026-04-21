from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, KpiSnapshot


KPI_VERSION = "kpi_v1"
MONEY_PRECISION = Decimal("0.0001")
RATIO_PRECISION = Decimal("0.0001")
ZERO = Decimal("0.0000")
VALIDATION_STATUS_VALID = "valid"
VALIDATION_STATUS_REJECTED = "rejected"
SIMULATION_STATUS_EXECUTABLE = "executable"
SIMULATION_STATUS_PARTIAL = "partially_executable"
SIMULATION_STATUS_REJECTED = "rejected"


@dataclass(slots=True)
class OpportunityKpi:
    simulation_id: int
    opportunity_id: int
    event_group_key: str
    simulation_status: str
    simulated_at: datetime
    intended_size_usd: Decimal
    executable_size_usd: Decimal
    real_edge: Decimal
    fill_ratio: Decimal
    execution_quality: Decimal
    slippage_cost: Decimal
    capital_efficiency: Decimal


@dataclass(slots=True)
class PersistedKpiSnapshot:
    id: int
    created_at: datetime
    total_opportunities: int
    valid_opportunities: int
    executable_opportunities: int
    partial_opportunities: int
    rejected_opportunities: int
    avg_real_edge: Decimal
    avg_fill_ratio: Decimal
    false_positive_rate: Decimal
    total_intended_capital: Decimal
    total_executable_capital: Decimal
    raw_context: dict


def derive_opportunity_kpi(
    *,
    simulation: ExecutionSimulation,
    opportunity: DetectedOpportunity,
) -> OpportunityKpi:
    fill_ratio = _quantize_ratio(
        simulation.fill_completion_ratio
        if simulation.fill_completion_ratio is not None
        else _safe_divide(simulation.executable_size_usd, simulation.intended_size_usd)
    )
    capital_efficiency = _quantize_ratio(
        _safe_divide(simulation.estimated_net_edge_usd, simulation.intended_size_usd)
    )
    return OpportunityKpi(
        simulation_id=simulation.id,
        opportunity_id=opportunity.id,
        event_group_key=opportunity.event_group_key,
        simulation_status=simulation.simulation_status,
        simulated_at=simulation.simulated_at,
        intended_size_usd=_quantize_money(simulation.intended_size_usd),
        executable_size_usd=_quantize_money(simulation.executable_size_usd),
        real_edge=_quantize_money(simulation.estimated_net_edge_usd),
        fill_ratio=fill_ratio,
        execution_quality=fill_ratio,
        slippage_cost=ZERO,
        capital_efficiency=capital_efficiency,
    )


def list_validated_simulation_kpis(session: Session) -> list[OpportunityKpi]:
    rows = session.execute(_simulation_with_opportunity_query()).all()
    return [
        derive_opportunity_kpi(simulation=simulation, opportunity=opportunity)
        for simulation, opportunity in rows
        if opportunity.validation_status == VALIDATION_STATUS_VALID
    ]


def calculate_and_persist_kpi_snapshot(session: Session) -> PersistedKpiSnapshot:
    rows = session.execute(_simulation_with_opportunity_query()).all()

    total_simulated_opportunities = len(rows)
    valid_rows = [
        (simulation, opportunity)
        for simulation, opportunity in rows
        if opportunity.validation_status == VALIDATION_STATUS_VALID
    ]
    opportunity_kpis = [
        derive_opportunity_kpi(simulation=simulation, opportunity=opportunity)
        for simulation, opportunity in valid_rows
    ]

    total_intended_capital = _quantize_money(
        sum((kpi.intended_size_usd for kpi in opportunity_kpis), start=ZERO)
    )
    total_executable_capital = _quantize_money(
        sum((kpi.executable_size_usd for kpi in opportunity_kpis), start=ZERO)
    )
    total_real_edge = _quantize_money(sum((kpi.real_edge for kpi in opportunity_kpis), start=ZERO))
    avg_real_edge = _quantize_ratio(_safe_divide(total_real_edge, total_intended_capital))
    avg_fill_ratio = _quantize_ratio(
        _safe_divide(sum((kpi.fill_ratio for kpi in opportunity_kpis), start=ZERO), len(opportunity_kpis))
    )

    validation_total_count = session.scalar(
        select(func.count())
        .select_from(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status.is_not(None))
    )
    validation_rejected_count = session.scalar(
        select(func.count())
        .select_from(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status == VALIDATION_STATUS_REJECTED)
    )
    validation_valid_count = session.scalar(
        select(func.count())
        .select_from(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status == VALIDATION_STATUS_VALID)
    )
    false_positive_rate = _quantize_ratio(
        _safe_divide(validation_rejected_count or 0, validation_total_count or 0)
    )

    executable_count = sum(
        1 for kpi in opportunity_kpis if kpi.simulation_status == SIMULATION_STATUS_EXECUTABLE
    )
    partial_count = sum(
        1 for kpi in opportunity_kpis if kpi.simulation_status == SIMULATION_STATUS_PARTIAL
    )
    rejected_count = sum(
        1 for kpi in opportunity_kpis if kpi.simulation_status == SIMULATION_STATUS_REJECTED
    )

    snapshot = KpiSnapshot(
        total_opportunities=total_simulated_opportunities,
        valid_opportunities=len(opportunity_kpis),
        executable_opportunities=executable_count,
        partial_opportunities=partial_count,
        rejected_opportunities=rejected_count,
        avg_real_edge=avg_real_edge,
        avg_fill_ratio=avg_fill_ratio,
        false_positive_rate=false_positive_rate,
        total_intended_capital=total_intended_capital,
        total_executable_capital=total_executable_capital,
        raw_context={
            "kpi_version": KPI_VERSION,
            "calculation_basis": {
                "edge_and_fill_metrics": "execution_simulations joined to detected_opportunities where validation_status=valid",
                "false_positive_rate": "detected_opportunities where validation_status is not null",
                "avg_real_edge_formula": "sum(estimated_net_edge_usd) / sum(intended_size_usd)",
                "avg_fill_ratio_formula": "average(fill_completion_ratio)",
            },
            "validation_summary": {
                "total_validated_opportunities": validation_total_count or 0,
                "valid_validated_opportunities": validation_valid_count or 0,
                "rejected_validated_opportunities": validation_rejected_count or 0,
            },
            "simulation_summary": {
                "total_simulated_opportunities": total_simulated_opportunities,
                "valid_simulated_opportunities": len(opportunity_kpis),
                "executable_opportunities": executable_count,
                "partial_opportunities": partial_count,
                "rejected_opportunities": rejected_count,
            },
            "opportunity_ids": [kpi.opportunity_id for kpi in opportunity_kpis],
        },
    )
    session.add(snapshot)
    session.flush()
    session.commit()

    return PersistedKpiSnapshot(
        id=snapshot.id,
        created_at=snapshot.created_at,
        total_opportunities=snapshot.total_opportunities,
        valid_opportunities=snapshot.valid_opportunities,
        executable_opportunities=snapshot.executable_opportunities,
        partial_opportunities=snapshot.partial_opportunities,
        rejected_opportunities=snapshot.rejected_opportunities,
        avg_real_edge=snapshot.avg_real_edge,
        avg_fill_ratio=snapshot.avg_fill_ratio,
        false_positive_rate=snapshot.false_positive_rate,
        total_intended_capital=snapshot.total_intended_capital,
        total_executable_capital=snapshot.total_executable_capital,
        raw_context=snapshot.raw_context,
    )


def _simulation_with_opportunity_query() -> Select[tuple[ExecutionSimulation, DetectedOpportunity]]:
    return (
        select(ExecutionSimulation, DetectedOpportunity)
        .join(DetectedOpportunity, DetectedOpportunity.id == ExecutionSimulation.opportunity_id)
        .order_by(ExecutionSimulation.simulated_at.asc(), ExecutionSimulation.id.asc())
    )


def _safe_divide(numerator: Decimal | int, denominator: Decimal | int) -> Decimal:
    numerator_decimal = Decimal(numerator)
    denominator_decimal = Decimal(denominator)
    if denominator_decimal <= ZERO:
        return ZERO
    return numerator_decimal / denominator_decimal


def _quantize_money(value: Decimal | int) -> Decimal:
    return Decimal(value).quantize(MONEY_PRECISION)


def _quantize_ratio(value: Decimal | int) -> Decimal:
    return Decimal(value).quantize(RATIO_PRECISION)
