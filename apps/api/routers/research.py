from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, KpiSnapshot, MarketSnapshot
from apps.api.db.session import get_db_session


VALIDATION_STATUS_VALID = "valid"

router = APIRouter(tags=["research"])


class OpportunityResponse(BaseModel):
    opportunity_id: int
    event_id: str
    validation_status: str
    simulation_status: str | None
    real_edge: Decimal | None
    fill_ratio: Decimal | None
    intended_size: Decimal | None
    executable_size: Decimal | None


class SimulationResponse(BaseModel):
    opportunity_id: int
    simulation_status: str
    net_edge: Decimal
    fill_ratio: Decimal
    reason: str | None


class KpiLatestResponse(BaseModel):
    avg_real_edge: Decimal
    avg_fill_ratio: Decimal
    false_positive_rate: Decimal
    total_intended_capital: Decimal
    total_executable_capital: Decimal
    total_opportunities: int
    valid_opportunities: int


class SystemStatusResponse(BaseModel):
    last_snapshot_time: datetime | None
    last_detection_time: datetime | None
    last_simulation_time: datetime | None
    last_kpi_time: datetime | None


DbSession = Annotated[Session, Depends(get_db_session)]


@router.get("/opportunities", response_model=list[OpportunityResponse])
def list_opportunities(session: DbSession) -> list[OpportunityResponse]:
    ranked_simulations = (
        select(
            ExecutionSimulation.opportunity_id.label("opportunity_id"),
            ExecutionSimulation.simulation_status.label("simulation_status"),
            ExecutionSimulation.estimated_net_edge_usd.label("real_edge"),
            ExecutionSimulation.fill_completion_ratio.label("fill_ratio"),
            ExecutionSimulation.intended_size_usd.label("intended_size"),
            ExecutionSimulation.executable_size_usd.label("executable_size"),
            func.row_number()
            .over(
                partition_by=ExecutionSimulation.opportunity_id,
                order_by=(ExecutionSimulation.simulated_at.desc(), ExecutionSimulation.id.desc()),
            )
            .label("simulation_rank"),
        )
        .subquery()
    )
    latest_simulations = (
        select(ranked_simulations)
        .where(ranked_simulations.c.simulation_rank == 1)
        .subquery()
    )

    rows = session.execute(
        select(
            DetectedOpportunity.id.label("opportunity_id"),
            # V1 exposes the detector's persisted event grouping key as the event identifier.
            DetectedOpportunity.event_group_key.label("event_id"),
            DetectedOpportunity.validation_status.label("validation_status"),
            latest_simulations.c.simulation_status,
            latest_simulations.c.real_edge,
            latest_simulations.c.fill_ratio,
            latest_simulations.c.intended_size,
            latest_simulations.c.executable_size,
        )
        .outerjoin(latest_simulations, latest_simulations.c.opportunity_id == DetectedOpportunity.id)
        .where(DetectedOpportunity.validation_status == VALIDATION_STATUS_VALID)
        .order_by(DetectedOpportunity.detected_at.desc(), DetectedOpportunity.id.desc())
    ).all()

    return [OpportunityResponse.model_validate(row._mapping) for row in rows]


@router.get("/simulations", response_model=list[SimulationResponse])
def list_simulations(session: DbSession) -> list[SimulationResponse]:
    rows = session.execute(
        select(
            ExecutionSimulation.opportunity_id.label("opportunity_id"),
            ExecutionSimulation.simulation_status.label("simulation_status"),
            ExecutionSimulation.estimated_net_edge_usd.label("net_edge"),
            ExecutionSimulation.fill_completion_ratio.label("fill_ratio"),
            ExecutionSimulation.simulation_reason.label("reason"),
        ).order_by(ExecutionSimulation.simulated_at.desc(), ExecutionSimulation.id.desc())
    ).all()

    return [SimulationResponse.model_validate(row._mapping) for row in rows]


@router.get("/kpi/latest", response_model=KpiLatestResponse)
def get_latest_kpi(session: DbSession) -> KpiLatestResponse:
    snapshot = session.scalar(
        select(KpiSnapshot).order_by(KpiSnapshot.created_at.desc(), KpiSnapshot.id.desc()).limit(1)
    )
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No KPI snapshots found")

    return KpiLatestResponse(
        avg_real_edge=snapshot.avg_real_edge,
        avg_fill_ratio=snapshot.avg_fill_ratio,
        false_positive_rate=snapshot.false_positive_rate,
        total_intended_capital=snapshot.total_intended_capital,
        total_executable_capital=snapshot.total_executable_capital,
        total_opportunities=snapshot.total_opportunities,
        valid_opportunities=snapshot.valid_opportunities,
    )


@router.get("/system/status", response_model=SystemStatusResponse)
def get_system_status(session: DbSession) -> SystemStatusResponse:
    return SystemStatusResponse(
        last_snapshot_time=session.scalar(select(func.max(MarketSnapshot.captured_at))),
        last_detection_time=session.scalar(select(func.max(DetectedOpportunity.detected_at))),
        last_simulation_time=session.scalar(select(func.max(ExecutionSimulation.simulated_at))),
        last_kpi_time=session.scalar(select(func.max(KpiSnapshot.created_at))),
    )
