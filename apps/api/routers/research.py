from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import (
    DetectedOpportunity,
    ExecutionSimulation,
    KpiRunSummary,
    KpiSnapshot,
    MarketSnapshot,
    OpportunityKpiSnapshot,
)
from apps.api.db.session import get_db_session
from apps.api.repositories.kpi import (
    get_kpi_run_summary,
    get_latest_kpi_run_summary,
    get_latest_legacy_kpi_snapshot,
    get_latest_opportunity_kpi_snapshot,
)
from apps.api.repositories.opportunities import (
    get_opportunity_with_context,
    list_opportunities as repo_list_opportunities,
    list_ranked_recommendations,
)
from apps.api.repositories.recommendation_scoring import get_recommendation_freshness_status
from apps.api.services.opportunity_classification import DetectionFamily


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
    family: str
    relation_type: str | None
    confidence_tier: str | None
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    min_executable_size: Decimal | None
    suggested_notional_bucket: str | None
    persistence_seconds_estimate: int | None
    capital_lock_estimate_hours: Decimal | None
    recommendation_eligibility: bool
    risk_flags_json: dict[str, Any] | list[Any] | None


class SimulationResponse(BaseModel):
    opportunity_id: int
    simulation_status: str
    net_edge: Decimal
    fill_ratio: Decimal
    reason: str | None


class ValidationResultResponse(BaseModel):
    id: int
    validation_type: str
    status: str
    score: Decimal | None
    summary: str | None
    details_json: dict[str, Any] | list[Any] | None
    validator_version: str
    created_at: datetime


class SimulationResultResponse(BaseModel):
    id: int
    simulation_mode: str
    executable_edge: Decimal | None
    fee_cost: Decimal | None
    slippage_cost: Decimal | None
    estimated_fill_quality: Decimal | None
    fill_completion_ratio: Decimal | None
    execution_feasible: bool | None
    min_executable_size: Decimal | None
    suggested_notional_bucket: str | None
    persistence_seconds_estimate: int | None
    capital_lock_estimate_hours: Decimal | None
    execution_risk_flag: str | None
    simulation_version: str
    details_json: dict[str, Any] | list[Any] | None
    created_at: datetime


class RecommendationScoreResponse(BaseModel):
    id: int
    score: Decimal | None
    tier: str | None
    reason_summary: str | None
    warning_summary: str | None
    manual_review_required: bool
    scoring_version: str
    created_at: datetime


class RecommendationQueueItemResponse(BaseModel):
    opportunity_id: int
    event_id: str
    detected_at: datetime
    ranking_position: int
    family: str
    confidence_tier: str | None
    validation_status: str | None
    recommendation_eligibility: bool
    recommendation_block_reason: str | None
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    min_executable_size: Decimal | None
    suggested_notional_bucket: str | None
    persistence_seconds_estimate: int | None
    capital_lock_estimate_hours: Decimal | None
    capital_lock_estimate: Decimal | None
    simulation_status: str | None
    real_edge: Decimal | None
    fill_completion_ratio: Decimal | None
    score: Decimal | None
    tier: str | None
    reason_summary: str | None
    warning_summary: str | None
    manual_review_required: bool
    scoring_version: str
    created_at: datetime
    freshness_status: str
    stale_reasons: list[str]
    executable_edge: dict[str, Any]


class RecommendationStatusResponse(BaseModel):
    latest_scoring_run_timestamp: datetime | None
    scoring_worker_status: str
    opportunities_scored_last_run: int
    high_conviction_last_run: int
    review_last_run: int
    blocked_last_run: int
    scoring_version: str | None
    run_reason: str | None
    latest_validation_time: datetime | None
    latest_kpi_time: datetime | None
    freshness_status: str
    stale_reasons: list[str]


class OpportunityDetailResponse(BaseModel):
    opportunity_id: int
    detected_at: datetime
    detection_window_start: datetime
    event_id: str
    involved_market_ids: list[int]
    opportunity_type: str
    outcome_count: int
    gross_price_sum: Decimal
    gross_gap: Decimal
    detector_version: str
    validation_version: str | None
    simulation_version: str | None
    status: str
    validation_status: str | None
    validation_reason: str | None
    validated_at: datetime | None
    raw_context: dict[str, Any] | None
    family: str
    relation_type: str | None
    relation_direction: str | None
    involved_market_ids_json: list[int] | None
    question_texts_json: list[str] | None
    normalized_entities_json: dict[str, Any] | list[Any] | None
    normalized_dates_json: dict[str, Any] | list[Any] | None
    normalized_thresholds_json: dict[str, Any] | list[Any] | None
    resolution_sources_json: dict[str, Any] | list[Any] | None
    end_dates_json: dict[str, Any] | list[Any] | None
    clarification_flags_json: dict[str, Any] | list[Any] | None
    dispute_flags_json: dict[str, Any] | list[Any] | None
    s_logic: Decimal | None
    s_sem: Decimal | None
    s_res: Decimal | None
    confidence: Decimal | None
    confidence_tier: str | None
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    min_executable_size: Decimal | None
    suggested_notional_bucket: str | None
    persistence_seconds_estimate: int | None
    capital_lock_estimate_hours: Decimal | None
    risk_flags_json: dict[str, Any] | list[Any] | None
    recommendation_eligibility: bool
    recommendation_block_reason: str | None
    involved_markets: list[dict[str, Any]]
    validation_results: list[ValidationResultResponse]
    simulation_results: list[SimulationResultResponse]
    scores: list[RecommendationScoreResponse]
    simulation_status: str | None
    real_edge: Decimal | None
    fill_ratio: Decimal | None
    intended_size: Decimal | None
    executable_size: Decimal | None


class RecommendationValidationLayerResponse(BaseModel):
    status: str | None
    summary: str | None
    score: Decimal | None
    validator_version: str | None
    details_json: dict[str, Any] | list[Any] | None
    raw_context: dict[str, Any] | None


class RecommendationEvidenceResponse(BaseModel):
    rule_validation: RecommendationValidationLayerResponse | None
    semantic_validation: RecommendationValidationLayerResponse | None
    resolution_validation: RecommendationValidationLayerResponse | None
    all_validation_results: list[ValidationResultResponse]


class RecommendationSummaryResponse(BaseModel):
    opportunity_id: int
    event_id: str
    detected_at: datetime
    ranking_position: int | None
    family: str
    confidence_tier: str | None
    validation_status: str | None
    recommendation_eligibility: bool
    recommendation_block_reason: str | None
    tier: str | None
    score: Decimal | None
    reason_summary: str | None
    warning_summary: str | None
    manual_review_required: bool
    scoring_version: str | None
    score_created_at: datetime | None
    freshness_status: str
    stale_reasons: list[str]


class RecommendationAuditResponse(BaseModel):
    raw_context: dict[str, Any] | None
    involved_markets: list[dict[str, Any]]
    scores: list[RecommendationScoreResponse]


class RecommendationDetailResponse(BaseModel):
    summary: RecommendationSummaryResponse
    validation_evidence: RecommendationEvidenceResponse
    executable_edge: dict[str, Any]
    latest_execution_simulation: dict[str, Any] | None
    simulation_results: list[SimulationResultResponse]
    kpi_snapshot: OpportunityKpiSnapshotResponse | None
    audit: RecommendationAuditResponse


class KpiLatestResponse(BaseModel):
    avg_real_edge: Decimal
    avg_fill_ratio: Decimal
    false_positive_rate: Decimal
    total_intended_capital: Decimal
    total_executable_capital: Decimal
    total_opportunities: int
    valid_opportunities: int


class OpportunityKpiSnapshotResponse(BaseModel):
    id: int
    run_summary_id: int
    opportunity_id: int
    lineage_key: str
    kpi_version: str
    snapshot_timestamp: datetime
    created_at: datetime
    family: str
    validation_stage_reached: str
    final_status: str
    rejection_stage: str | None
    rejection_reason: str | None
    detected: bool
    rule_pass: bool
    semantic_pass: bool
    resolution_pass: bool
    executable_pass: bool
    simulation_pass: bool
    s_logic: Decimal | None
    s_sem: Decimal | None
    s_res: Decimal | None
    top_of_book_edge: Decimal | None
    depth_weighted_edge: Decimal | None
    fee_adjusted_edge: Decimal | None
    fill_completion_ratio: Decimal | None
    execution_feasible: bool | None
    capital_lock_estimate_hours: Decimal | None
    detector_version: str
    validation_version: str | None
    simulation_version: str | None
    first_seen_timestamp: datetime
    last_seen_timestamp: datetime
    persistence_duration_seconds: int
    decay_status: str
    raw_context: dict[str, Any]


class KpiRunSummaryResponse(BaseModel):
    id: int
    created_at: datetime
    run_started_at: datetime
    run_completed_at: datetime
    kpi_version: str
    total_opportunities: int
    valid_after_rule: int
    valid_after_semantic: int
    valid_after_resolution: int
    valid_after_executable: int
    valid_after_simulation: int
    avg_executable_edge: Decimal
    avg_fill_ratio: Decimal
    avg_capital_lock: Decimal
    false_positive_rate: Decimal
    family_distribution: dict[str, int]
    detector_versions_json: list[str]
    validation_versions_json: list[str]
    simulation_versions_json: list[str]
    raw_context: dict[str, Any]


class SystemStatusResponse(BaseModel):
    last_snapshot_time: datetime | None
    last_detection_time: datetime | None
    last_simulation_time: datetime | None
    last_kpi_time: datetime | None


DbSession = Annotated[Session, Depends(get_db_session)]


@router.get("/opportunities", response_model=list[OpportunityResponse])
def list_opportunities(
    session: DbSession,
    family: DetectionFamily | None = Query(default=None),
    confidence_tier: str | None = Query(default=None),
) -> list[OpportunityResponse]:
    opportunities = [
        opportunity
        for opportunity in repo_list_opportunities(session, family=family, confidence_tier=confidence_tier)
        if opportunity.validation_status == VALIDATION_STATUS_VALID
    ]
    latest_simulations = _load_latest_execution_simulations(session, [opportunity.id for opportunity in opportunities])

    return [
        OpportunityResponse(
            opportunity_id=opportunity.id,
            event_id=opportunity.event_group_key,
            validation_status=opportunity.validation_status or "",
            simulation_status=_simulation_attr(latest_simulations, opportunity.id, "simulation_status"),
            real_edge=_simulation_attr(latest_simulations, opportunity.id, "estimated_net_edge_usd"),
            fill_ratio=_simulation_attr(latest_simulations, opportunity.id, "fill_completion_ratio"),
            intended_size=_simulation_attr(latest_simulations, opportunity.id, "intended_size_usd"),
            executable_size=_simulation_attr(latest_simulations, opportunity.id, "executable_size_usd"),
            family=opportunity.family,
            relation_type=opportunity.relation_type,
            confidence_tier=opportunity.confidence_tier,
            top_of_book_edge=opportunity.top_of_book_edge,
            depth_weighted_edge=opportunity.depth_weighted_edge,
            fee_adjusted_edge=opportunity.fee_adjusted_edge,
            min_executable_size=opportunity.min_executable_size,
            suggested_notional_bucket=opportunity.suggested_notional_bucket,
            persistence_seconds_estimate=opportunity.persistence_seconds_estimate,
            capital_lock_estimate_hours=opportunity.capital_lock_estimate_hours,
            recommendation_eligibility=opportunity.recommendation_eligibility,
            risk_flags_json=opportunity.risk_flags_json,
        )
        for opportunity in opportunities
    ]


@router.get("/recommendations", response_model=list[RecommendationQueueItemResponse])
def get_recommendations(
    response: Response,
    session: DbSession,
    tier: Literal["high_conviction", "review", "blocked"] | None = Query(default=None),
    family: DetectionFamily | None = Query(default=None),
    min_score: Decimal | None = Query(default=None),
    sort: Literal["score", "edge", "recency"] = Query(default="score"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[RecommendationQueueItemResponse]:
    freshness = get_recommendation_freshness_status(session)
    queue_page = list_ranked_recommendations(
        session,
        tier=tier,
        family=family,
        min_score=min_score,
        sort_by=sort,
        limit=limit,
        offset=offset,
    )
    response.headers["X-Recommendation-Freshness"] = freshness.freshness_status
    response.headers["X-Recommendation-Stale-Reasons"] = ",".join(freshness.stale_reasons)
    response.headers["X-Total-Count"] = str(queue_page.total_count)
    response.headers["X-Limit"] = str(limit)
    response.headers["X-Offset"] = str(offset)

    return [
        RecommendationQueueItemResponse.model_validate(_serialize_recommendation_queue_row(row, freshness))
        for row in queue_page.rows
    ]


@router.get("/recommendations/status", response_model=RecommendationStatusResponse)
def get_recommendation_status(session: DbSession) -> RecommendationStatusResponse:
    return RecommendationStatusResponse.model_validate(
        get_recommendation_freshness_status(session),
        from_attributes=True,
    )


@router.get("/recommendations/{opportunity_id}", response_model=RecommendationDetailResponse)
def get_recommendation_detail(opportunity_id: int, session: DbSession) -> RecommendationDetailResponse:
    opportunity = get_opportunity_with_context(session, opportunity_id)
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    freshness = get_recommendation_freshness_status(session)
    involved_markets = _build_involved_markets(opportunity)
    latest_execution_simulation = _load_latest_execution_simulations(session, [opportunity.id]).get(opportunity.id)
    latest_score = _get_latest_score(opportunity)
    kpi_snapshot = get_latest_opportunity_kpi_snapshot(session, opportunity.id)
    validation_results = sorted(opportunity.validation_results, key=lambda row: (row.created_at, row.id))
    simulation_results = sorted(opportunity.simulation_results, key=lambda row: (row.created_at, row.id))
    scores = sorted(opportunity.recommendation_scores, key=lambda row: (row.created_at, row.id))
    ranking_position = _get_recommendation_rank_position(session, opportunity.id)

    return RecommendationDetailResponse(
        summary=RecommendationSummaryResponse(
            opportunity_id=opportunity.id,
            event_id=opportunity.event_group_key,
            detected_at=opportunity.detected_at,
            ranking_position=ranking_position,
            family=opportunity.family,
            confidence_tier=opportunity.confidence_tier,
            validation_status=opportunity.validation_status,
            recommendation_eligibility=opportunity.recommendation_eligibility,
            recommendation_block_reason=opportunity.recommendation_block_reason,
            tier=getattr(latest_score, "tier", None),
            score=getattr(latest_score, "score", None),
            reason_summary=getattr(latest_score, "reason_summary", None),
            warning_summary=getattr(latest_score, "warning_summary", None),
            manual_review_required=getattr(latest_score, "manual_review_required", False),
            scoring_version=getattr(latest_score, "scoring_version", None),
            score_created_at=getattr(latest_score, "created_at", None),
            freshness_status=freshness.freshness_status,
            stale_reasons=freshness.stale_reasons,
        ),
        validation_evidence=RecommendationEvidenceResponse(
            rule_validation=_build_validation_layer("rule", validation_results, opportunity.raw_context),
            semantic_validation=_build_validation_layer("semantic", validation_results, opportunity.raw_context),
            resolution_validation=_build_validation_layer("resolution", validation_results, opportunity.raw_context),
            all_validation_results=[
                ValidationResultResponse.model_validate(result, from_attributes=True) for result in validation_results
            ],
        ),
        executable_edge=_build_executable_edge_summary(opportunity, latest_execution_simulation),
        latest_execution_simulation=(
            {
                "simulation_status": latest_execution_simulation.simulation_status,
                "simulated_at": latest_execution_simulation.simulated_at,
                "intended_size": latest_execution_simulation.intended_size_usd,
                "executable_size": latest_execution_simulation.executable_size_usd,
                "gross_cost": latest_execution_simulation.gross_cost_usd,
                "gross_payout": latest_execution_simulation.gross_payout_usd,
                "estimated_fees": latest_execution_simulation.estimated_fees_usd,
                "estimated_slippage": latest_execution_simulation.estimated_slippage_usd,
                "real_edge": latest_execution_simulation.estimated_net_edge_usd,
                "fill_completion_ratio": latest_execution_simulation.fill_completion_ratio,
                "simulation_reason": latest_execution_simulation.simulation_reason,
                "raw_context": latest_execution_simulation.raw_context,
            }
            if latest_execution_simulation is not None
            else None
        ),
        simulation_results=[
            SimulationResultResponse.model_validate(result, from_attributes=True) for result in simulation_results
        ],
        kpi_snapshot=(
            OpportunityKpiSnapshotResponse.model_validate(kpi_snapshot, from_attributes=True)
            if kpi_snapshot is not None
            else None
        ),
        audit=RecommendationAuditResponse(
            raw_context=opportunity.raw_context,
            involved_markets=involved_markets,
            scores=[RecommendationScoreResponse.model_validate(score, from_attributes=True) for score in scores],
        ),
    )


@router.get("/opportunities/{opportunity_id}", response_model=OpportunityDetailResponse)
def get_opportunity(opportunity_id: int, session: DbSession) -> OpportunityDetailResponse:
    opportunity = get_opportunity_with_context(session, opportunity_id)
    if opportunity is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")

    latest_simulation = _load_latest_execution_simulations(session, [opportunity.id]).get(opportunity.id)
    involved_markets = _build_involved_markets(opportunity)

    return OpportunityDetailResponse(
        opportunity_id=opportunity.id,
        detected_at=opportunity.detected_at,
        detection_window_start=opportunity.detection_window_start,
        event_id=opportunity.event_group_key,
        involved_market_ids=list(opportunity.involved_market_ids),
        opportunity_type=opportunity.opportunity_type,
        outcome_count=opportunity.outcome_count,
        gross_price_sum=opportunity.gross_price_sum,
        gross_gap=opportunity.gross_gap,
        detector_version=opportunity.detector_version,
        validation_version=opportunity.validation_version,
        simulation_version=opportunity.simulation_version,
        status=opportunity.status,
        validation_status=opportunity.validation_status,
        validation_reason=opportunity.validation_reason,
        validated_at=opportunity.validated_at,
        raw_context=opportunity.raw_context,
        family=opportunity.family,
        relation_type=opportunity.relation_type,
        relation_direction=opportunity.relation_direction,
        involved_market_ids_json=opportunity.involved_market_ids_json,
        question_texts_json=opportunity.question_texts_json,
        normalized_entities_json=opportunity.normalized_entities_json,
        normalized_dates_json=opportunity.normalized_dates_json,
        normalized_thresholds_json=opportunity.normalized_thresholds_json,
        resolution_sources_json=opportunity.resolution_sources_json,
        end_dates_json=opportunity.end_dates_json,
        clarification_flags_json=opportunity.clarification_flags_json,
        dispute_flags_json=opportunity.dispute_flags_json,
        s_logic=opportunity.s_logic,
        s_sem=opportunity.s_sem,
        s_res=opportunity.s_res,
        confidence=opportunity.confidence,
        confidence_tier=opportunity.confidence_tier,
        top_of_book_edge=opportunity.top_of_book_edge,
        depth_weighted_edge=opportunity.depth_weighted_edge,
        fee_adjusted_edge=opportunity.fee_adjusted_edge,
        min_executable_size=opportunity.min_executable_size,
        suggested_notional_bucket=opportunity.suggested_notional_bucket,
        persistence_seconds_estimate=opportunity.persistence_seconds_estimate,
        capital_lock_estimate_hours=opportunity.capital_lock_estimate_hours,
        risk_flags_json=opportunity.risk_flags_json,
        recommendation_eligibility=opportunity.recommendation_eligibility,
        recommendation_block_reason=opportunity.recommendation_block_reason,
        involved_markets=involved_markets,
        validation_results=[
            ValidationResultResponse.model_validate(result, from_attributes=True)
            for result in sorted(opportunity.validation_results, key=lambda row: (row.created_at, row.id))
        ],
        simulation_results=[
            SimulationResultResponse.model_validate(result, from_attributes=True)
            for result in sorted(opportunity.simulation_results, key=lambda row: (row.created_at, row.id))
        ],
        scores=[
            RecommendationScoreResponse.model_validate(result, from_attributes=True)
            for result in sorted(opportunity.recommendation_scores, key=lambda row: (row.created_at, row.id))
        ],
        simulation_status=getattr(latest_simulation, "simulation_status", None),
        real_edge=getattr(latest_simulation, "estimated_net_edge_usd", None),
        fill_ratio=getattr(latest_simulation, "fill_completion_ratio", None),
        intended_size=getattr(latest_simulation, "intended_size_usd", None),
        executable_size=getattr(latest_simulation, "executable_size_usd", None),
    )


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
    run_summary = get_latest_kpi_run_summary(session)
    if run_summary is not None:
        legacy_projection = run_summary.raw_context.get("legacy_projection", {})
        return KpiLatestResponse(
            avg_real_edge=run_summary.avg_executable_edge,
            avg_fill_ratio=run_summary.avg_fill_ratio,
            false_positive_rate=run_summary.false_positive_rate,
            total_intended_capital=Decimal(str(legacy_projection.get("total_intended_capital", "0.0000"))),
            total_executable_capital=Decimal(
                str(legacy_projection.get("total_executable_capital", "0.0000"))
            ),
            total_opportunities=run_summary.total_opportunities,
            valid_opportunities=run_summary.valid_after_simulation,
        )

    snapshot = get_latest_legacy_kpi_snapshot(session)
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


@router.get("/kpi/opportunities/{opportunity_id}", response_model=OpportunityKpiSnapshotResponse)
def get_opportunity_kpi(opportunity_id: int, session: DbSession) -> OpportunityKpiSnapshotResponse:
    snapshot = get_latest_opportunity_kpi_snapshot(session, opportunity_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No KPI snapshot found for opportunity")
    return OpportunityKpiSnapshotResponse.model_validate(snapshot, from_attributes=True)


@router.get("/kpi/runs/latest", response_model=KpiRunSummaryResponse)
def get_latest_kpi_run(session: DbSession) -> KpiRunSummaryResponse:
    run_summary = get_latest_kpi_run_summary(session)
    if run_summary is None:
        raise HTTPException(status_code=404, detail="No KPI run summaries found")
    return KpiRunSummaryResponse.model_validate(run_summary, from_attributes=True)


@router.get("/kpi/runs/{run_id}", response_model=KpiRunSummaryResponse)
def get_kpi_run(run_id: int, session: DbSession) -> KpiRunSummaryResponse:
    run_summary = get_kpi_run_summary(session, run_id)
    if run_summary is None:
        raise HTTPException(status_code=404, detail="KPI run summary not found")
    return KpiRunSummaryResponse.model_validate(run_summary, from_attributes=True)


@router.get("/system/status", response_model=SystemStatusResponse)
def get_system_status(session: DbSession) -> SystemStatusResponse:
    last_legacy_kpi_time = session.scalar(select(func.max(KpiSnapshot.created_at)))
    last_run_kpi_time = session.scalar(select(func.max(KpiRunSummary.created_at)))
    return SystemStatusResponse(
        last_snapshot_time=session.scalar(select(func.max(MarketSnapshot.captured_at))),
        last_detection_time=session.scalar(select(func.max(DetectedOpportunity.detected_at))),
        last_simulation_time=session.scalar(select(func.max(ExecutionSimulation.simulated_at))),
        last_kpi_time=max(
            [timestamp for timestamp in [last_legacy_kpi_time, last_run_kpi_time] if timestamp is not None],
            default=None,
        ),
    )


def _serialize_recommendation_queue_row(
    row: dict[str, Any],
    freshness: Any,
) -> dict[str, Any]:
    serialized = dict(row)
    serialized["capital_lock_estimate"] = row.get("capital_lock_estimate_hours")
    serialized["freshness_status"] = freshness.freshness_status
    serialized["stale_reasons"] = freshness.stale_reasons
    serialized["executable_edge"] = {
        "simulation_status": row.get("simulation_status"),
        "top_of_book_edge": row.get("top_of_book_edge"),
        "depth_weighted_edge": row.get("depth_weighted_edge"),
        "fee_adjusted_edge": row.get("fee_adjusted_edge"),
        "real_edge": row.get("real_edge"),
        "min_executable_size": row.get("min_executable_size"),
        "suggested_notional_bucket": row.get("suggested_notional_bucket"),
        "persistence_seconds_estimate": row.get("persistence_seconds_estimate"),
        "fill_completion_ratio": row.get("fill_completion_ratio"),
        "capital_lock_estimate_hours": row.get("capital_lock_estimate_hours"),
    }
    return serialized


def _get_latest_score(opportunity: DetectedOpportunity) -> Any | None:
    if not opportunity.recommendation_scores:
        return None
    return max(opportunity.recommendation_scores, key=lambda row: (row.created_at, row.id))


def _get_recommendation_rank_position(session: Session, opportunity_id: int) -> int | None:
    queue_page = list_ranked_recommendations(session, limit=None, offset=0)
    for row in queue_page.rows:
        if row["opportunity_id"] == opportunity_id:
            return int(row["ranking_position"])
    return None


def _build_validation_layer(
    validation_type_prefix: str,
    validation_results: list[Any],
    raw_context: dict[str, Any] | None,
) -> RecommendationValidationLayerResponse | None:
    matching_results = [
        result
        for result in validation_results
        if result.validation_type.startswith(validation_type_prefix)
    ]
    latest_result = matching_results[-1] if matching_results else None
    context = _extract_validation_context(validation_type_prefix, raw_context)
    if latest_result is None and not context:
        return None
    return RecommendationValidationLayerResponse(
        status=(latest_result.status if latest_result is not None else context.get("status")),
        summary=(latest_result.summary if latest_result is not None else context.get("summary")),
        score=(latest_result.score if latest_result is not None else None),
        validator_version=(
            latest_result.validator_version if latest_result is not None else context.get("validator_version")
        ),
        details_json=(latest_result.details_json if latest_result is not None else context.get("details_json")),
        raw_context=context or None,
    )


def _extract_validation_context(
    validation_type_prefix: str,
    raw_context: dict[str, Any] | None,
) -> dict[str, Any]:
    if raw_context is None:
        return {}
    context: dict[str, Any] = {}
    for key, value in raw_context.items():
        if key.startswith(f"{validation_type_prefix}_validation_"):
            context[key.removeprefix(f"{validation_type_prefix}_validation_")] = value
    return context


def _build_executable_edge_summary(
    opportunity: DetectedOpportunity,
    latest_execution_simulation: ExecutionSimulation | None,
) -> dict[str, Any]:
    return {
        "top_of_book_edge": opportunity.top_of_book_edge,
        "depth_weighted_edge": opportunity.depth_weighted_edge,
        "fee_adjusted_edge": opportunity.fee_adjusted_edge,
        "min_executable_size": opportunity.min_executable_size,
        "suggested_notional_bucket": opportunity.suggested_notional_bucket,
        "persistence_seconds_estimate": opportunity.persistence_seconds_estimate,
        "capital_lock_estimate_hours": opportunity.capital_lock_estimate_hours,
        "simulation_status": getattr(latest_execution_simulation, "simulation_status", None),
        "real_edge": getattr(latest_execution_simulation, "estimated_net_edge_usd", None),
        "fill_completion_ratio": getattr(latest_execution_simulation, "fill_completion_ratio", None),
        "intended_size": getattr(latest_execution_simulation, "intended_size_usd", None),
        "executable_size": getattr(latest_execution_simulation, "executable_size_usd", None),
        "simulation_reason": getattr(latest_execution_simulation, "simulation_reason", None),
    }


def _load_latest_execution_simulations(
    session: Session,
    opportunity_ids: list[int],
) -> dict[int, ExecutionSimulation]:
    if not opportunity_ids:
        return {}

    ranked_simulations = (
        select(
            ExecutionSimulation.id.label("simulation_id"),
            ExecutionSimulation.opportunity_id.label("opportunity_id"),
            func.row_number()
            .over(
                partition_by=ExecutionSimulation.opportunity_id,
                order_by=(ExecutionSimulation.simulated_at.desc(), ExecutionSimulation.id.desc()),
            )
            .label("simulation_rank"),
        )
        .where(ExecutionSimulation.opportunity_id.in_(opportunity_ids))
        .subquery()
    )
    rows = session.execute(
        select(ExecutionSimulation)
        .join(ranked_simulations, ranked_simulations.c.simulation_id == ExecutionSimulation.id)
        .where(ranked_simulations.c.simulation_rank == 1)
    ).scalars()
    return {row.opportunity_id: row for row in rows}


def _simulation_attr(
    latest_simulations: dict[int, ExecutionSimulation],
    opportunity_id: int,
    attribute: str,
) -> Any:
    simulation = latest_simulations.get(opportunity_id)
    return getattr(simulation, attribute, None) if simulation is not None else None


def _build_involved_markets(opportunity: DetectedOpportunity) -> list[dict[str, Any]]:
    questions = list(opportunity.question_texts_json or [])
    raw_markets = []
    if opportunity.raw_context is not None:
        raw_markets = opportunity.raw_context.get("markets", [])

    involved_markets: list[dict[str, Any]] = []
    for index, market_id in enumerate(opportunity.involved_market_ids_json or opportunity.involved_market_ids):
        raw_market = raw_markets[index] if index < len(raw_markets) else {}
        involved_markets.append(
            {
                "market_id": market_id,
                "question_text": questions[index] if index < len(questions) else raw_market.get("question"),
                "polymarket_market_id": raw_market.get("polymarket_market_id"),
                "slug": raw_market.get("slug"),
                "condition_id": raw_market.get("condition_id"),
                "snapshot_id": raw_market.get("snapshot_id"),
            }
        )

    return involved_markets
