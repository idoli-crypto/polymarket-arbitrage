from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session, selectinload

from apps.api.db.models import (
    DetectedOpportunity,
    ExecutionSimulation,
    RecommendationScore,
    SimulationResult,
    ValidationResult,
)
from apps.api.services.opportunity_classification import DetectionFamily, OpportunityClassification


def create_opportunity_extended(
    session: Session,
    *,
    detection_window_start: datetime,
    event_group_key: str,
    involved_market_ids: list[int],
    opportunity_type: str,
    outcome_count: int,
    gross_price_sum: Decimal,
    gross_gap: Decimal,
    detector_version: str,
    classification: OpportunityClassification,
    status: str = "detected",
    detected_at: datetime | None = None,
    involved_market_ids_json: list[int] | None = None,
    question_texts_json: list[str] | None = None,
    normalized_entities_json: dict | list | None = None,
    normalized_dates_json: dict | list | None = None,
    normalized_thresholds_json: dict | list | None = None,
    resolution_sources_json: dict | list | None = None,
    end_dates_json: dict | list | None = None,
    clarification_flags_json: dict | list | None = None,
    dispute_flags_json: dict | list | None = None,
    s_logic: Decimal | None = None,
    s_sem: Decimal | None = None,
    s_res: Decimal | None = None,
    confidence: Decimal | None = None,
    confidence_tier: str | None = None,
    top_of_book_edge: Decimal | None = None,
    depth_weighted_edge: Decimal | None = None,
    fee_adjusted_edge: Decimal | None = None,
    min_executable_size: Decimal | None = None,
    suggested_notional_bucket: str | None = None,
    persistence_seconds_estimate: int | None = None,
    capital_lock_estimate_hours: Decimal | None = None,
    validation_version: str | None = None,
    simulation_version: str | None = None,
    validation_status: str | None = None,
    validation_reason: str | None = None,
    validated_at: datetime | None = None,
    risk_flags_json: dict | list | None = None,
    recommendation_eligibility: bool = False,
    recommendation_block_reason: str | None = None,
    raw_context: dict | None = None,
) -> DetectedOpportunity:
    opportunity_kwargs = {
        "detection_window_start": detection_window_start,
        "event_group_key": event_group_key,
        "involved_market_ids": involved_market_ids,
        "opportunity_type": opportunity_type,
        "outcome_count": outcome_count,
        "gross_price_sum": gross_price_sum,
        "gross_gap": gross_gap,
        "family": classification.family.value,
        "relation_type": (
            classification.relation_type.value if classification.relation_type is not None else None
        ),
        "relation_direction": classification.relation_direction,
        "involved_market_ids_json": (
            involved_market_ids_json if involved_market_ids_json is not None else involved_market_ids
        ),
        "question_texts_json": question_texts_json,
        "normalized_entities_json": normalized_entities_json,
        "normalized_dates_json": normalized_dates_json,
        "normalized_thresholds_json": normalized_thresholds_json,
        "resolution_sources_json": resolution_sources_json,
        "end_dates_json": end_dates_json,
        "clarification_flags_json": clarification_flags_json,
        "dispute_flags_json": dispute_flags_json,
        "s_logic": s_logic,
        "s_sem": s_sem,
        "s_res": s_res,
        "confidence": confidence,
        "confidence_tier": confidence_tier,
        "top_of_book_edge": top_of_book_edge,
        "depth_weighted_edge": depth_weighted_edge,
        "fee_adjusted_edge": fee_adjusted_edge,
        "min_executable_size": min_executable_size,
        "suggested_notional_bucket": suggested_notional_bucket,
        "persistence_seconds_estimate": persistence_seconds_estimate,
        "capital_lock_estimate_hours": capital_lock_estimate_hours,
        "detector_version": detector_version,
        "validation_version": validation_version,
        "simulation_version": simulation_version,
        "status": status,
        "validation_status": validation_status,
        "validation_reason": validation_reason,
        "validated_at": validated_at,
        "risk_flags_json": risk_flags_json,
        "recommendation_eligibility": recommendation_eligibility,
        "recommendation_block_reason": recommendation_block_reason,
        "raw_context": raw_context,
    }
    if detected_at is not None:
        opportunity_kwargs["detected_at"] = detected_at
    opportunity = DetectedOpportunity(**opportunity_kwargs)
    session.add(opportunity)
    session.flush()
    return opportunity


def get_opportunity_with_context(session: Session, opportunity_id: int) -> DetectedOpportunity | None:
    return session.scalar(
        select(DetectedOpportunity)
        .where(DetectedOpportunity.id == opportunity_id)
        .options(
            selectinload(DetectedOpportunity.validation_results),
            selectinload(DetectedOpportunity.simulation_results),
            selectinload(DetectedOpportunity.recommendation_scores),
            selectinload(DetectedOpportunity.simulations),
        )
    )


def list_opportunities(
    session: Session,
    *,
    family: DetectionFamily | None = None,
    confidence_tier: str | None = None,
) -> list[DetectedOpportunity]:
    query = select(DetectedOpportunity)
    if family is not None:
        query = query.where(DetectedOpportunity.family == family)
    if confidence_tier is not None:
        query = query.where(DetectedOpportunity.confidence_tier == confidence_tier)
    query = query.order_by(DetectedOpportunity.detected_at.desc(), DetectedOpportunity.id.desc())
    return list(session.scalars(query).all())


def attach_validation_result(
    session: Session,
    opportunity_id: int,
    *,
    validation_type: str,
    status: str,
    validator_version: str,
    score: Decimal | None = None,
    summary: str | None = None,
    details_json: dict | list | None = None,
    created_at: datetime | None = None,
) -> ValidationResult:
    result_kwargs = dict(
        opportunity_id=opportunity_id,
        validation_type=validation_type,
        status=status,
        score=score,
        summary=summary,
        details_json=details_json,
        validator_version=validator_version,
    )
    if created_at is not None:
        result_kwargs["created_at"] = created_at
    result = ValidationResult(**result_kwargs)
    session.add(result)
    session.flush()
    return result


def attach_simulation_result(
    session: Session,
    opportunity_id: int,
    *,
    simulation_mode: str,
    simulation_version: str,
    executable_edge: Decimal | None = None,
    fee_cost: Decimal | None = None,
    slippage_cost: Decimal | None = None,
    estimated_fill_quality: Decimal | None = None,
    fill_completion_ratio: Decimal | None = None,
    execution_feasible: bool | None = None,
    min_executable_size: Decimal | None = None,
    suggested_notional_bucket: str | None = None,
    persistence_seconds_estimate: int | None = None,
    capital_lock_estimate_hours: Decimal | None = None,
    execution_risk_flag: str | None = None,
    details_json: dict | list | None = None,
    created_at: datetime | None = None,
) -> SimulationResult:
    result_kwargs = dict(
        opportunity_id=opportunity_id,
        simulation_mode=simulation_mode,
        executable_edge=executable_edge,
        fee_cost=fee_cost,
        slippage_cost=slippage_cost,
        estimated_fill_quality=estimated_fill_quality,
        fill_completion_ratio=fill_completion_ratio,
        execution_feasible=execution_feasible,
        min_executable_size=min_executable_size,
        suggested_notional_bucket=suggested_notional_bucket,
        persistence_seconds_estimate=persistence_seconds_estimate,
        capital_lock_estimate_hours=capital_lock_estimate_hours,
        execution_risk_flag=execution_risk_flag,
        simulation_version=simulation_version,
        details_json=details_json,
    )
    if created_at is not None:
        result_kwargs["created_at"] = created_at
    result = SimulationResult(**result_kwargs)
    session.add(result)
    session.flush()
    return result


def attach_recommendation_score(
    session: Session,
    opportunity_id: int,
    *,
    scoring_version: str,
    score: Decimal | None = None,
    tier: str | None = None,
    reason_summary: str | None = None,
    warning_summary: str | None = None,
    manual_review_required: bool = False,
    created_at: datetime | None = None,
) -> RecommendationScore:
    score_kwargs = dict(
        opportunity_id=opportunity_id,
        score=score,
        tier=tier,
        reason_summary=reason_summary,
        warning_summary=warning_summary,
        manual_review_required=manual_review_required,
        scoring_version=scoring_version,
    )
    if created_at is not None:
        score_kwargs["created_at"] = created_at
    result = RecommendationScore(**score_kwargs)
    session.add(result)
    session.flush()
    return result


@dataclass(frozen=True, slots=True)
class RecommendationQueuePage:
    total_count: int
    rows: list[dict[str, object]]


def list_ranked_recommendations(
    session: Session,
    *,
    tier: str | None = None,
    family: DetectionFamily | None = None,
    min_score: Decimal | None = None,
    sort_by: str = "score",
    limit: int | None = None,
    offset: int = 0,
) -> RecommendationQueuePage:
    latest_scores = (
        select(
            RecommendationScore.id.label("score_id"),
            RecommendationScore.opportunity_id.label("opportunity_id"),
            RecommendationScore.score.label("score"),
            RecommendationScore.tier.label("tier"),
            RecommendationScore.reason_summary.label("reason_summary"),
            RecommendationScore.warning_summary.label("warning_summary"),
            RecommendationScore.manual_review_required.label("manual_review_required"),
            RecommendationScore.scoring_version.label("scoring_version"),
            RecommendationScore.created_at.label("created_at"),
            func.row_number()
            .over(
                partition_by=RecommendationScore.opportunity_id,
                order_by=(RecommendationScore.created_at.desc(), RecommendationScore.id.desc()),
            )
            .label("score_rank"),
        ).subquery()
    )

    latest_execution_simulations = (
        select(
            ExecutionSimulation.id.label("simulation_id"),
            ExecutionSimulation.opportunity_id.label("opportunity_id"),
            ExecutionSimulation.simulation_status.label("simulation_status"),
            ExecutionSimulation.estimated_net_edge_usd.label("real_edge"),
            ExecutionSimulation.fill_completion_ratio.label("fill_completion_ratio"),
            ExecutionSimulation.simulated_at.label("simulated_at"),
            func.row_number()
            .over(
                partition_by=ExecutionSimulation.opportunity_id,
                order_by=(ExecutionSimulation.simulated_at.desc(), ExecutionSimulation.id.desc()),
            )
            .label("simulation_rank"),
        ).subquery()
    )

    query = (
        select(
            DetectedOpportunity.id.label("opportunity_id"),
            DetectedOpportunity.event_group_key.label("event_id"),
            DetectedOpportunity.detected_at.label("detected_at"),
            DetectedOpportunity.family.label("family"),
            DetectedOpportunity.confidence_tier.label("confidence_tier"),
            DetectedOpportunity.validation_status.label("validation_status"),
            DetectedOpportunity.recommendation_eligibility.label("recommendation_eligibility"),
            DetectedOpportunity.recommendation_block_reason.label("recommendation_block_reason"),
            DetectedOpportunity.top_of_book_edge.label("top_of_book_edge"),
            DetectedOpportunity.depth_weighted_edge.label("depth_weighted_edge"),
            DetectedOpportunity.fee_adjusted_edge.label("fee_adjusted_edge"),
            DetectedOpportunity.min_executable_size.label("min_executable_size"),
            DetectedOpportunity.suggested_notional_bucket.label("suggested_notional_bucket"),
            DetectedOpportunity.persistence_seconds_estimate.label("persistence_seconds_estimate"),
            DetectedOpportunity.capital_lock_estimate_hours.label("capital_lock_estimate_hours"),
            latest_scores.c.score,
            latest_scores.c.tier,
            latest_scores.c.reason_summary,
            latest_scores.c.warning_summary,
            latest_scores.c.manual_review_required,
            latest_scores.c.scoring_version,
            latest_scores.c.created_at,
            latest_execution_simulations.c.simulation_status,
            latest_execution_simulations.c.real_edge,
            latest_execution_simulations.c.fill_completion_ratio,
            latest_execution_simulations.c.simulated_at,
        )
        .join(latest_scores, latest_scores.c.opportunity_id == DetectedOpportunity.id)
        .outerjoin(
            latest_execution_simulations,
            (latest_execution_simulations.c.opportunity_id == DetectedOpportunity.id)
            & (latest_execution_simulations.c.simulation_rank == 1),
        )
        .where(latest_scores.c.score_rank == 1)
    )

    if tier is not None:
        query = query.where(latest_scores.c.tier == tier)
    if family is not None:
        query = query.where(DetectedOpportunity.family == family.value)
    if min_score is not None:
        query = query.where(latest_scores.c.score >= min_score)

    score_sort = func.coalesce(latest_scores.c.score, Decimal("-1.0000"))
    edge_sort = func.coalesce(DetectedOpportunity.fee_adjusted_edge, Decimal("-1.0000"))
    recency_sort = DetectedOpportunity.detected_at.desc()
    if sort_by == "edge":
        query = query.order_by(
            edge_sort.desc(),
            score_sort.desc(),
            recency_sort,
            DetectedOpportunity.id.asc(),
        )
    elif sort_by == "recency":
        query = query.order_by(
            recency_sort,
            score_sort.desc(),
            edge_sort.desc(),
            DetectedOpportunity.id.asc(),
        )
    else:
        query = query.order_by(
            score_sort.desc(),
            edge_sort.desc(),
            recency_sort,
            DetectedOpportunity.id.asc(),
        )

    ordered_rows = [dict(row) for row in session.execute(query).mappings()]
    total_count = len(ordered_rows)
    page_rows = ordered_rows[offset : offset + limit if limit is not None else None]

    enriched_rows: list[dict[str, object]] = []
    for ranking_position, row in enumerate(page_rows, start=offset + 1):
        enriched_row = dict(row)
        enriched_row["ranking_position"] = ranking_position
        enriched_rows.append(enriched_row)

    return RecommendationQueuePage(total_count=total_count, rows=enriched_rows)
