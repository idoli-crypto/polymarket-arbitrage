from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, Market, MarketSnapshot
from apps.api.repositories.opportunities import attach_simulation_result, attach_validation_result
from apps.worker.validators.rule_based_relation import (
    STATUS_INCONCLUSIVE,
    STATUS_INVALID,
    STATUS_VALID,
    RuleBasedRelationValidationInput,
    validate_rule_based_relation,
)
from apps.worker.validators.semantic import (
    STATUS_INCONCLUSIVE as SEMANTIC_STATUS_INCONCLUSIVE,
    STATUS_INVALID as SEMANTIC_STATUS_INVALID,
    STATUS_VALID as SEMANTIC_STATUS_VALID,
    SemanticValidationInput,
    validate_semantic_opportunity,
)
from apps.worker.validators.resolution import (
    STATUS_INCONCLUSIVE as RESOLUTION_STATUS_INCONCLUSIVE,
    STATUS_INVALID as RESOLUTION_STATUS_INVALID,
    STATUS_RISKY as RESOLUTION_STATUS_RISKY,
    STATUS_VALID as RESOLUTION_STATUS_VALID,
    VALIDATOR_VERSION as RESOLUTION_VALIDATOR_VERSION,
    ResolutionValidationInput,
    validate_resolution,
)
from apps.worker.validators.resolution_extraction import (
    ResolutionExtractionInput,
    build_resolution_column_payloads,
    extract_resolution_metadata,
)
from apps.worker.validators.executable_edge import (
    STATUS_INCONCLUSIVE as EXECUTION_STATUS_INCONCLUSIVE,
    STATUS_INVALID as EXECUTION_STATUS_INVALID,
    STATUS_VALID as EXECUTION_STATUS_VALID,
    ExecutableEdgeValidationInput,
    VALIDATOR_VERSION as EXECUTION_VALIDATOR_VERSION,
    parse_executable_market_snapshot,
    validate_executable_edge,
)
from apps.worker.validators.simulation import (
    STATUS_INCONCLUSIVE as SIMULATION_STATUS_INCONCLUSIVE,
    STATUS_INVALID as SIMULATION_STATUS_INVALID,
    STATUS_VALID as SIMULATION_STATUS_VALID,
    SimulationValidationInput,
    validate_simulation_execution,
)
from apps.worker.metrics.kpi import OpportunityKpiSnapshotInput, persist_kpi_run


@dataclass(slots=True)
class PersistedValidationResult:
    id: int
    event_group_key: str
    validation_status: str
    validation_reason: str | None
    validated_at: datetime


def validate_pending_opportunities(session: Session) -> list[PersistedValidationResult]:
    run_started_at = datetime.now(timezone.utc)
    opportunities = session.scalars(
        select(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status.is_(None))
        .order_by(DetectedOpportunity.detected_at.asc(), DetectedOpportunity.id.asc())
    ).all()

    persisted_results: list[PersistedValidationResult] = []
    kpi_inputs: list[OpportunityKpiSnapshotInput] = []
    for opportunity in opportunities:
        rule_result = validate_rule_based_relation(
            RuleBasedRelationValidationInput(
                opportunity_id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                family=opportunity.family,
                relation_type=opportunity.relation_type,
                relation_direction=opportunity.relation_direction,
                involved_market_ids=list(opportunity.involved_market_ids),
                normalized_dates=opportunity.normalized_dates_json,
                raw_context=opportunity.raw_context,
            )
        )
        semantic_result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                family=opportunity.family,
                relation_type=opportunity.relation_type,
                involved_market_ids=list(opportunity.involved_market_ids),
                question_texts=opportunity.question_texts_json,
                raw_context=opportunity.raw_context,
            )
        )
        extracted_resolution_markets = _extract_resolution_markets(session, opportunity.involved_market_ids)
        resolution_result = validate_resolution(
            ResolutionValidationInput(
                opportunity_id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                family=opportunity.family,
                relation_type=opportunity.relation_type,
                extracted_markets=extracted_resolution_markets,
            )
        )
        validated_at = datetime.now(timezone.utc)
        execution_result = None
        simulation_result = None
        if _should_run_execution_validation(
            rule_status=rule_result.status,
            semantic_status=semantic_result.status,
            resolution_status=resolution_result.status,
        ):
            executable_snapshots = _load_latest_executable_market_snapshots(
                session,
                opportunity.involved_market_ids,
            )
            execution_result = validate_executable_edge(
                ExecutableEdgeValidationInput(
                    opportunity_id=opportunity.id,
                    event_group_key=opportunity.event_group_key,
                    involved_market_ids=list(opportunity.involved_market_ids),
                    family=opportunity.family,
                    opportunity_type=opportunity.opportunity_type,
                ),
                market_snapshots=executable_snapshots,
                reference_time=validated_at,
            )
            if execution_result.status == EXECUTION_STATUS_VALID:
                simulation_result = validate_simulation_execution(
                    SimulationValidationInput(
                        opportunity_id=opportunity.id,
                        event_group_key=opportunity.event_group_key,
                        involved_market_ids=list(opportunity.involved_market_ids),
                        family=opportunity.family,
                        opportunity_type=opportunity.opportunity_type,
                    ),
                    execution_result=execution_result,
                    market_snapshots=executable_snapshots,
                )
        (
            opportunity.resolution_sources_json,
            opportunity.end_dates_json,
            opportunity.clarification_flags_json,
            opportunity.dispute_flags_json,
        ) = build_resolution_column_payloads(extracted_resolution_markets)
        opportunity.validation_status = _derive_opportunity_validation_status(
            rule_status=rule_result.status,
            semantic_status=semantic_result.status,
            resolution_status=resolution_result.status,
            execution_status=execution_result.status if execution_result is not None else None,
            simulation_status=simulation_result.status if simulation_result is not None else None,
        )
        opportunity.validation_reason = _derive_validation_reason(
            rule_result=rule_result,
            semantic_result=semantic_result,
            resolution_result=resolution_result,
            execution_result=execution_result,
            simulation_result=simulation_result,
        )
        opportunity.validated_at = validated_at
        opportunity.validation_version = (
            EXECUTION_VALIDATOR_VERSION if execution_result is not None else RESOLUTION_VALIDATOR_VERSION
        )
        opportunity.simulation_version = simulation_result.simulation_version if simulation_result is not None else None
        opportunity.normalized_entities_json = semantic_result.normalized_entities
        opportunity.normalized_dates_json = semantic_result.normalized_dates
        opportunity.normalized_thresholds_json = semantic_result.normalized_thresholds
        opportunity.s_sem = semantic_result.score
        opportunity.s_res = resolution_result.score
        if execution_result is not None:
            opportunity.top_of_book_edge = execution_result.top_of_book_edge
            opportunity.depth_weighted_edge = execution_result.depth_weighted_edge
            opportunity.fee_adjusted_edge = execution_result.fee_adjusted_edge
            opportunity.min_executable_size = execution_result.min_executable_size
            opportunity.suggested_notional_bucket = execution_result.suggested_notional_bucket
        if simulation_result is not None:
            opportunity.capital_lock_estimate_hours = simulation_result.capital_lock_estimate_hours
        opportunity.raw_context = _merge_validation_context(
            opportunity.raw_context,
            rule_result=rule_result,
            semantic_result=semantic_result,
            resolution_result=resolution_result,
            extracted_resolution_markets=extracted_resolution_markets,
            execution_result=execution_result,
            simulation_result=simulation_result,
        )
        opportunity.risk_flags_json = _build_risk_flags(
            rule_result,
            semantic_result,
            resolution_result,
            execution_result=execution_result,
            simulation_result=simulation_result,
        )
        attach_validation_result(
            session,
            opportunity.id,
            validation_type=rule_result.validation_type,
            status=rule_result.status,
            score=rule_result.score,
            summary=rule_result.summary,
            details_json=rule_result.details,
            validator_version=rule_result.validator_version,
            created_at=validated_at,
        )
        attach_validation_result(
            session,
            opportunity.id,
            validation_type=semantic_result.validation_type,
            status=semantic_result.status,
            score=semantic_result.score,
            summary=semantic_result.summary,
            details_json=semantic_result.details,
            validator_version=semantic_result.validator_version,
            created_at=validated_at,
        )
        attach_validation_result(
            session,
            opportunity.id,
            validation_type=resolution_result.validation_type,
            status=resolution_result.status,
            score=resolution_result.score,
            summary=resolution_result.summary,
            details_json=resolution_result.details,
            validator_version=resolution_result.validator_version,
            created_at=validated_at,
        )
        if execution_result is not None:
            attach_validation_result(
                session,
                opportunity.id,
                validation_type=execution_result.validation_type,
                status=execution_result.status,
                score=execution_result.score,
                summary=execution_result.summary,
                details_json=execution_result.details,
                validator_version=execution_result.validator_version,
                created_at=validated_at,
            )
        if simulation_result is not None:
            attach_simulation_result(
                session,
                opportunity.id,
                simulation_mode="simulation_validation",
                executable_edge=simulation_result.executable_edge,
                fee_cost=simulation_result.fee_cost_usd,
                slippage_cost=simulation_result.slippage_cost_usd,
                estimated_fill_quality=simulation_result.fill_completion_ratio,
                fill_completion_ratio=simulation_result.fill_completion_ratio,
                execution_feasible=simulation_result.execution_feasible,
                min_executable_size=simulation_result.executable_size_usd,
                suggested_notional_bucket=execution_result.suggested_notional_bucket,
                persistence_seconds_estimate=simulation_result.execution_time_sensitivity_seconds,
                capital_lock_estimate_hours=simulation_result.capital_lock_estimate_hours,
                execution_risk_flag=simulation_result.execution_risk_flag,
                simulation_version=simulation_result.simulation_version,
                details_json=simulation_result.details,
                created_at=validated_at,
            )
            attach_validation_result(
                session,
                opportunity.id,
                validation_type=simulation_result.validation_type,
                status=simulation_result.status,
                score=simulation_result.score,
                summary=simulation_result.summary,
                details_json=simulation_result.details,
                validator_version=simulation_result.simulation_version,
                created_at=validated_at,
            )
        persisted_results.append(
            PersistedValidationResult(
                id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                validation_status=opportunity.validation_status,
                validation_reason=opportunity.validation_reason,
                validated_at=validated_at,
            )
        )
        kpi_inputs.append(
            OpportunityKpiSnapshotInput(
                opportunity_id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                involved_market_ids=list(opportunity.involved_market_ids),
                opportunity_type=opportunity.opportunity_type,
                family=opportunity.family,
                relation_type=opportunity.relation_type,
                relation_direction=opportunity.relation_direction,
                detection_window_start=opportunity.detection_window_start,
                snapshot_timestamp=validated_at,
                final_status=opportunity.validation_status,
                rejection_reason=opportunity.validation_reason,
                s_logic=opportunity.s_logic,
                s_sem=opportunity.s_sem,
                s_res=opportunity.s_res,
                top_of_book_edge=opportunity.top_of_book_edge,
                depth_weighted_edge=opportunity.depth_weighted_edge,
                fee_adjusted_edge=opportunity.fee_adjusted_edge,
                fill_completion_ratio=(
                    simulation_result.fill_completion_ratio if simulation_result is not None else None
                ),
                execution_feasible=(
                    simulation_result.execution_feasible if simulation_result is not None else None
                ),
                capital_lock_estimate_hours=opportunity.capital_lock_estimate_hours,
                detector_version=opportunity.detector_version,
                validation_version=opportunity.validation_version,
                simulation_version=opportunity.simulation_version,
                rule_status=rule_result.status,
                semantic_status=semantic_result.status,
                resolution_status=resolution_result.status,
                execution_status=execution_result.status if execution_result is not None else None,
                simulation_status=simulation_result.status if simulation_result is not None else None,
                intended_size_usd=(
                    simulation_result.intended_size_usd if simulation_result is not None else None
                ),
                executable_size_usd=(
                    simulation_result.executable_size_usd if simulation_result is not None else None
                ),
                fee_cost_usd=simulation_result.fee_cost_usd if simulation_result is not None else None,
                slippage_cost_usd=(
                    simulation_result.slippage_cost_usd if simulation_result is not None else None
                ),
            )
        )

    persist_kpi_run(
        session,
        kpi_inputs,
        run_started_at=run_started_at,
        run_completed_at=datetime.now(timezone.utc),
    )

    session.commit()
    return persisted_results


def _merge_validation_context(
    raw_context: dict[str, Any] | None,
    *,
    rule_result,
    semantic_result,
    resolution_result,
    extracted_resolution_markets: list[dict[str, Any]],
    execution_result,
    simulation_result,
) -> dict[str, Any]:
    merged = dict(raw_context or {})
    merged["rule_validation_status"] = rule_result.status
    merged["rule_validation_summary"] = rule_result.summary
    merged["rule_validation_details"] = rule_result.details
    merged["semantic_validation_status"] = semantic_result.status
    merged["semantic_validation_summary"] = semantic_result.summary
    merged["semantic_validation_details"] = semantic_result.details
    merged["semantic_normalized_markets"] = semantic_result.semantic_context["normalized_markets"]
    merged["resolution_validation_status"] = resolution_result.status
    merged["resolution_validation_summary"] = resolution_result.summary
    merged["resolution_validation_details"] = resolution_result.details
    merged["resolution_extracted_markets"] = extracted_resolution_markets
    if execution_result is not None:
        merged["execution_validation_status"] = execution_result.status
        merged["execution_validation_summary"] = execution_result.summary
        merged["execution_validation_details"] = execution_result.details
    if simulation_result is not None:
        merged["simulation_validation_status"] = simulation_result.status
        merged["simulation_validation_summary"] = simulation_result.summary
        merged["simulation_validation_details"] = simulation_result.details
    return merged


def _build_risk_flags(
    rule_result,
    semantic_result,
    resolution_result,
    *,
    execution_result,
    simulation_result,
) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = []
    for stage, result in (
        ("rule_based_relation", rule_result),
        ("semantic_validation", semantic_result),
        ("resolution_validation", resolution_result),
    ):
        if result.status == STATUS_VALID:
            continue
        flags.append(
            {
                "type": "validation",
                "stage": stage,
                "flag": result.details["reason_code"],
                "status": result.status,
            }
        )
    if execution_result is not None and execution_result.status != EXECUTION_STATUS_VALID:
        flags.append(
            {
                "type": "validation",
                "stage": "executable_edge_validation",
                "flag": execution_result.reason_code,
                "status": execution_result.status,
            }
        )
    if simulation_result is not None and simulation_result.status != SIMULATION_STATUS_VALID:
        flags.append(
            {
                "type": "validation",
                "stage": "simulation_validation",
                "flag": simulation_result.details["reason_code"],
                "status": simulation_result.status,
            }
        )
    return flags


def _derive_opportunity_validation_status(
    *,
    rule_status: str,
    semantic_status: str,
    resolution_status: str,
    execution_status: str | None,
    simulation_status: str | None,
) -> str:
    if (
        rule_status == STATUS_VALID
        and semantic_status == SEMANTIC_STATUS_VALID
        and resolution_status == RESOLUTION_STATUS_VALID
        and execution_status in {None, EXECUTION_STATUS_VALID}
        and simulation_status in {None, SIMULATION_STATUS_VALID}
    ):
        return STATUS_VALID
    if rule_status not in {STATUS_VALID, STATUS_INVALID, STATUS_INCONCLUSIVE}:
        raise ValueError(f"Unexpected rule validation status: {rule_status!r}")
    if semantic_status not in {SEMANTIC_STATUS_VALID, SEMANTIC_STATUS_INVALID, SEMANTIC_STATUS_INCONCLUSIVE}:
        raise ValueError(f"Unexpected semantic validation status: {semantic_status!r}")
    if resolution_status not in {
        RESOLUTION_STATUS_VALID,
        RESOLUTION_STATUS_INVALID,
        RESOLUTION_STATUS_RISKY,
        RESOLUTION_STATUS_INCONCLUSIVE,
    }:
        raise ValueError(f"Unexpected resolution validation status: {resolution_status!r}")
    if execution_status is not None and execution_status not in {
        EXECUTION_STATUS_VALID,
        EXECUTION_STATUS_INVALID,
        EXECUTION_STATUS_INCONCLUSIVE,
    }:
        raise ValueError(f"Unexpected execution validation status: {execution_status!r}")
    if simulation_status is not None and simulation_status not in {
        SIMULATION_STATUS_VALID,
        SIMULATION_STATUS_INVALID,
        SIMULATION_STATUS_INCONCLUSIVE,
    }:
        raise ValueError(f"Unexpected simulation validation status: {simulation_status!r}")
    if (
        rule_status != STATUS_VALID
        or semantic_status != SEMANTIC_STATUS_VALID
        or resolution_status != RESOLUTION_STATUS_VALID
        or execution_status not in {None, EXECUTION_STATUS_VALID}
        or simulation_status not in {None, SIMULATION_STATUS_VALID}
    ):
        return "rejected"
    raise ValueError("Unexpected validation rollup state")


def _derive_validation_reason(
    *,
    rule_result,
    semantic_result,
    resolution_result,
    execution_result,
    simulation_result,
) -> str | None:
    if rule_result.status != STATUS_VALID:
        return rule_result.details["reason_code"]
    if semantic_result.status != SEMANTIC_STATUS_VALID:
        return semantic_result.details["reason_code"]
    if resolution_result.status != RESOLUTION_STATUS_VALID:
        return resolution_result.details["reason_code"]
    if execution_result is not None and execution_result.status != EXECUTION_STATUS_VALID:
        return execution_result.reason_code
    if simulation_result is not None and simulation_result.status != SIMULATION_STATUS_VALID:
        return simulation_result.details["reason_code"]
    return None


def _extract_resolution_markets(session: Session, involved_market_ids: list[int]) -> list[dict[str, Any]]:
    markets = session.scalars(select(Market).where(Market.id.in_(involved_market_ids))).all()
    markets_by_id = {market.id: market for market in markets}
    return [
        extract_resolution_metadata(
            ResolutionExtractionInput(
                market_id=market_id,
                raw_market_json=getattr(markets_by_id.get(market_id), "raw_market_json", None),
            )
        )
        for market_id in involved_market_ids
    ]


def _load_latest_executable_market_snapshots(
    session: Session,
    involved_market_ids: list[int],
) -> dict[int, Any]:
    latest_snapshot_by_market: dict[int, Any] = {market_id: None for market_id in involved_market_ids}
    if not involved_market_ids:
        return latest_snapshot_by_market

    ranked_snapshots = (
        select(
            MarketSnapshot.id.label("snapshot_id"),
            MarketSnapshot.market_id.label("market_id"),
            MarketSnapshot.captured_at.label("captured_at"),
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
        .where(MarketSnapshot.market_id.in_(involved_market_ids))
        .subquery()
    )
    rows = session.execute(
        select(ranked_snapshots).where(ranked_snapshots.c.snapshot_rank == 1)
    ).mappings()
    for row in rows:
        latest_snapshot_by_market[row["market_id"]] = parse_executable_market_snapshot(
            market_id=row["market_id"],
            snapshot_id=row["snapshot_id"],
            captured_at=row["captured_at"],
            order_book_json=row["order_book_json"],
            raw_market_json=row["raw_market_json"],
        )
    return latest_snapshot_by_market


def _should_run_execution_validation(
    *,
    rule_status: str,
    semantic_status: str,
    resolution_status: str,
) -> bool:
    return (
        rule_status == STATUS_VALID
        and semantic_status == SEMANTIC_STATUS_VALID
        and resolution_status == RESOLUTION_STATUS_VALID
    )
