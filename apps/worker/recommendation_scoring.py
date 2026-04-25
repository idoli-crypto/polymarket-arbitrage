from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from apps.api.db.models import (
    DetectedOpportunity,
    OpportunityKpiSnapshot,
    RecommendationScore,
    SimulationResult,
    ValidationResult,
)
from apps.api.repositories.opportunities import attach_recommendation_score


SCORING_VERSION = "recommendation_scoring_v1"

TIER_HIGH_CONVICTION = "high_conviction"
TIER_REVIEW = "review"
TIER_BLOCKED = "blocked"

VALIDATION_TYPE_RULE = "rule_based_relation"
VALIDATION_TYPE_SEMANTIC = "semantic_validation"
VALIDATION_TYPE_RESOLUTION = "resolution_validation"
VALIDATION_TYPE_EXECUTABLE = "executable_edge_validation"
VALIDATION_TYPE_SIMULATION = "simulation_validation"

SIMULATION_MODE_VALIDATION = "simulation_validation"

STATUS_VALID = "valid"

DECAY_STATUS_ALIVE = "alive"

ZERO = Decimal("0.0000")
ONE = Decimal("1.0000")
SCORE_SCALE = Decimal("100.0000")
SCORE_PRECISION = Decimal("0.0001")

PILLAR_WEIGHTS = {
    "executable_edge_quality": Decimal("0.3000"),
    "logic_strength": Decimal("0.2500"),
    "semantic_confidence": Decimal("0.1500"),
    "resolution_cleanliness": Decimal("0.1500"),
    "persistence_and_simulation_quality": Decimal("0.1500"),
}

WARNING_PENALTIES = {
    "low_executable_size": Decimal("7.0000"),
    "high_capital_lock": Decimal("6.0000"),
    "weak_persistence": Decimal("8.0000"),
    "stale_data": Decimal("20.0000"),
    "unsupported_payout_contract": Decimal("25.0000"),
    "semantic_ambiguity": Decimal("20.0000"),
    "resolution_risk": Decimal("22.0000"),
    "simulation_infeasible": Decimal("25.0000"),
    "missing_evidence": Decimal("30.0000"),
    "logic_failure": Decimal("30.0000"),
    "blocking_risk_flag": Decimal("25.0000"),
}

HIGH_CONVICTION_MIN_SCORE = Decimal("80.0000")
LOW_EXECUTABLE_SIZE_WARNING = Decimal("50.0000")
WEAK_PERSISTENCE_WARNING_SECONDS = 120
HIGH_CAPITAL_LOCK_WARNING_HOURS = Decimal("0.0500")


@dataclass(frozen=True, slots=True)
class PersistedRecommendation:
    id: int
    opportunity_id: int
    score: Decimal
    tier: str
    reason_summary: str
    warning_summary: str | None
    manual_review_required: bool
    scoring_version: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class _Warning:
    code: str
    text: str
    penalty: Decimal
    blocking: bool
    severity: int


@dataclass(frozen=True, slots=True)
class _Decision:
    score: Decimal
    tier: str
    reason_summary: str
    warning_summary: str | None
    manual_review_required: bool
    recommendation_eligibility: bool
    block_reason: str | None


def score_pending_recommendations(session: Session) -> list[PersistedRecommendation]:
    existing_score = (
        select(RecommendationScore.id)
        .where(RecommendationScore.opportunity_id == DetectedOpportunity.id)
        .where(RecommendationScore.scoring_version == SCORING_VERSION)
        .exists()
    )
    opportunities = session.scalars(
        select(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status.is_not(None))
        .where(~existing_score)
        .options(
            selectinload(DetectedOpportunity.validation_results),
            selectinload(DetectedOpportunity.simulation_results),
            selectinload(DetectedOpportunity.kpi_snapshots),
        )
        .order_by(DetectedOpportunity.validated_at.asc(), DetectedOpportunity.id.asc())
    ).all()

    persisted: list[PersistedRecommendation] = []
    scored_at = datetime.now(timezone.utc)
    for opportunity in opportunities:
        decision = _score_opportunity(opportunity)
        opportunity.recommendation_eligibility = decision.recommendation_eligibility
        opportunity.recommendation_block_reason = decision.block_reason
        score_row = attach_recommendation_score(
            session,
            opportunity.id,
            scoring_version=SCORING_VERSION,
            score=decision.score,
            tier=decision.tier,
            reason_summary=decision.reason_summary,
            warning_summary=decision.warning_summary,
            manual_review_required=decision.manual_review_required,
            created_at=scored_at,
        )
        persisted.append(
            PersistedRecommendation(
                id=score_row.id,
                opportunity_id=opportunity.id,
                score=decision.score,
                tier=decision.tier,
                reason_summary=decision.reason_summary,
                warning_summary=decision.warning_summary,
                manual_review_required=decision.manual_review_required,
                scoring_version=score_row.scoring_version,
                created_at=score_row.created_at,
            )
        )

    session.commit()
    return persisted


def _score_opportunity(opportunity: DetectedOpportunity) -> _Decision:
    validation_by_type = _latest_validation_results(opportunity.validation_results)
    simulation_result = _latest_simulation_result(opportunity.simulation_results)
    kpi_snapshot = _latest_kpi_snapshot(opportunity.kpi_snapshots)

    warnings = _build_warnings(
        opportunity,
        validation_by_type=validation_by_type,
        simulation_result=simulation_result,
        kpi_snapshot=kpi_snapshot,
    )
    pillar_scores = _pillar_scores(
        opportunity,
        validation_by_type=validation_by_type,
        simulation_result=simulation_result,
        kpi_snapshot=kpi_snapshot,
    )
    base_score = _quantize_score(
        sum(
            pillar_scores[name] * weight * SCORE_SCALE
            for name, weight in PILLAR_WEIGHTS.items()
        )
    )
    penalty_total = sum((warning.penalty for warning in warnings), ZERO)
    final_score = _quantize_score(max(ZERO, base_score - penalty_total))

    has_blocking_warning = any(warning.blocking for warning in warnings)
    if has_blocking_warning:
        tier = TIER_BLOCKED
    elif warnings or final_score < HIGH_CONVICTION_MIN_SCORE:
        tier = TIER_REVIEW
    else:
        tier = TIER_HIGH_CONVICTION

    reason_summary = _build_reason_summary(
        opportunity,
        validation_by_type=validation_by_type,
        simulation_result=simulation_result,
        kpi_snapshot=kpi_snapshot,
    )
    ordered_warnings = sorted(
        warnings,
        key=lambda warning: (-int(warning.blocking), -warning.severity, warning.text),
    )
    warning_summary = "; ".join(warning.text for warning in ordered_warnings[:3]) or None
    block_reason = ordered_warnings[0].code if ordered_warnings else None

    return _Decision(
        score=final_score,
        tier=tier,
        reason_summary=reason_summary,
        warning_summary=warning_summary,
        manual_review_required=tier == TIER_REVIEW,
        recommendation_eligibility=tier == TIER_HIGH_CONVICTION,
        block_reason=(
            None
            if tier == TIER_HIGH_CONVICTION
            else (block_reason or "manual_review_required")
        ),
    )


def _build_warnings(
    opportunity: DetectedOpportunity,
    *,
    validation_by_type: dict[str, ValidationResult],
    simulation_result: SimulationResult | None,
    kpi_snapshot: OpportunityKpiSnapshot | None,
) -> list[_Warning]:
    warnings: list[_Warning] = []

    if not opportunity.family:
        warnings.append(_warning("missing_evidence", "missing family evidence", blocking=True, severity=5))
    if not opportunity.detector_version:
        warnings.append(_warning("missing_evidence", "missing detector version", blocking=True, severity=5))
    if not opportunity.validation_version:
        warnings.append(_warning("missing_evidence", "missing validation version", blocking=True, severity=5))
    if not opportunity.simulation_version:
        warnings.append(_warning("missing_evidence", "missing simulation version", blocking=True, severity=5))

    for validation_type, code, text in (
        (VALIDATION_TYPE_RULE, "logic_failure", "logic evidence failed"),
        (VALIDATION_TYPE_SEMANTIC, "semantic_ambiguity", "semantic ambiguity"),
        (VALIDATION_TYPE_RESOLUTION, "resolution_risk", "resolution risk"),
        (VALIDATION_TYPE_EXECUTABLE, "simulation_infeasible", "executable evidence failed"),
        (VALIDATION_TYPE_SIMULATION, "simulation_infeasible", "simulation infeasible"),
    ):
        result = validation_by_type.get(validation_type)
        if result is None:
            warnings.append(
                _warning(
                    "missing_evidence",
                    f"missing {validation_type} evidence",
                    blocking=True,
                    severity=5,
                )
            )
            continue
        if result.status == STATUS_VALID:
            continue
        warning = _warning(code, text, blocking=True, severity=5)
        reason_code = _reason_code(result)
        if reason_code == "stale_order_book_snapshot":
            warning = _warning("stale_data", "stale data", blocking=True, severity=5)
        elif reason_code == "unsupported_payout_contract":
            warning = _warning("unsupported_payout_contract", "unsupported payout contract", blocking=True, severity=5)
        elif validation_type == VALIDATION_TYPE_EXECUTABLE:
            warning = _warning("simulation_infeasible", "executable evidence failed", blocking=True, severity=5)
        warnings.append(warning)

    if simulation_result is None:
        warnings.append(_warning("missing_evidence", "missing simulation metrics", blocking=True, severity=5))
    if kpi_snapshot is None:
        warnings.append(_warning("missing_evidence", "missing persistence evidence", blocking=True, severity=5))
    elif kpi_snapshot.decay_status != DECAY_STATUS_ALIVE:
        warnings.append(_warning("stale_data", "stale data", blocking=True, severity=5))

    for flag in _iter_risk_flags(opportunity.risk_flags_json):
        status = str(flag.get("status") or "")
        if status and status != STATUS_VALID:
            warnings.append(_warning("blocking_risk_flag", "blocking risk flag", blocking=True, severity=5))
            break

    executable_size = opportunity.min_executable_size or getattr(simulation_result, "min_executable_size", None)
    if executable_size is not None and executable_size < LOW_EXECUTABLE_SIZE_WARNING and executable_size > ZERO:
        warnings.append(_warning("low_executable_size", "low executable size", blocking=False, severity=2))

    if kpi_snapshot is not None and 0 <= kpi_snapshot.persistence_duration_seconds < WEAK_PERSISTENCE_WARNING_SECONDS:
        warnings.append(_warning("weak_persistence", "weak persistence", blocking=False, severity=3))

    capital_lock = getattr(simulation_result, "capital_lock_estimate_hours", None)
    if capital_lock is not None and capital_lock > HIGH_CAPITAL_LOCK_WARNING_HOURS:
        warnings.append(_warning("high_capital_lock", "high capital lock", blocking=False, severity=2))

    return _dedupe_warnings(warnings)


def _pillar_scores(
    opportunity: DetectedOpportunity,
    *,
    validation_by_type: dict[str, ValidationResult],
    simulation_result: SimulationResult | None,
    kpi_snapshot: OpportunityKpiSnapshot | None,
) -> dict[str, Decimal]:
    rule_score = _score_from_validation(validation_by_type.get(VALIDATION_TYPE_RULE))
    semantic_score = _score_from_validation(validation_by_type.get(VALIDATION_TYPE_SEMANTIC))
    resolution_score = _score_from_validation(validation_by_type.get(VALIDATION_TYPE_RESOLUTION))

    executable_edge_quality = _average(
        [
            _edge_strength(opportunity.fee_adjusted_edge),
            _size_strength(opportunity.min_executable_size),
        ]
    )
    logic_strength = _average(
        [
            rule_score,
            ONE if opportunity.family else ZERO,
            _bounded_ratio(opportunity.s_logic) if opportunity.s_logic is not None else rule_score,
        ]
    )
    persistence_and_simulation_quality = _average(
        [
            _persistence_strength(kpi_snapshot.persistence_duration_seconds if kpi_snapshot is not None else None),
            _fill_strength(getattr(simulation_result, "fill_completion_ratio", None)),
            _capital_lock_strength(getattr(simulation_result, "capital_lock_estimate_hours", None)),
        ]
    )

    return {
        "executable_edge_quality": executable_edge_quality,
        "logic_strength": logic_strength,
        "semantic_confidence": semantic_score,
        "resolution_cleanliness": resolution_score,
        "persistence_and_simulation_quality": persistence_and_simulation_quality,
    }


def _build_reason_summary(
    opportunity: DetectedOpportunity,
    *,
    validation_by_type: dict[str, ValidationResult],
    simulation_result: SimulationResult | None,
    kpi_snapshot: OpportunityKpiSnapshot | None,
) -> str:
    reasons: list[str] = []
    critical_results = [
        validation_by_type.get(VALIDATION_TYPE_RULE),
        validation_by_type.get(VALIDATION_TYPE_SEMANTIC),
        validation_by_type.get(VALIDATION_TYPE_RESOLUTION),
        validation_by_type.get(VALIDATION_TYPE_EXECUTABLE),
        validation_by_type.get(VALIDATION_TYPE_SIMULATION),
    ]
    if all(result is not None and result.status == STATUS_VALID for result in critical_results):
        reasons.append("all critical validations passed")
    else:
        passed_count = sum(1 for result in critical_results if result is not None and result.status == STATUS_VALID)
        if passed_count >= 3:
            reasons.append("partial validation stack passed")
        else:
            reasons.append("persisted evidence retained for audit")

    if opportunity.fee_adjusted_edge is not None and opportunity.min_executable_size is not None:
        reasons.append(
            f"fee-adjusted edge {format(opportunity.fee_adjusted_edge, 'f')} on {format(opportunity.min_executable_size, 'f')} USD"
        )

    if kpi_snapshot is not None:
        persistence_text = f"persistence {kpi_snapshot.persistence_duration_seconds}s"
        fill_ratio = getattr(simulation_result, "fill_completion_ratio", None)
        if fill_ratio is not None:
            persistence_text = f"{persistence_text} with fill {format(fill_ratio, 'f')}"
        reasons.append(persistence_text)

    return "; ".join(reasons[:3])


def _latest_validation_results(results: list[ValidationResult]) -> dict[str, ValidationResult]:
    latest: dict[str, ValidationResult] = {}
    for result in sorted(results, key=lambda row: (row.created_at, row.id)):
        latest[result.validation_type] = result
    return latest


def _latest_simulation_result(results: list[SimulationResult]) -> SimulationResult | None:
    ordered = sorted(results, key=lambda row: (row.created_at, row.id))
    validation_mode_results = [result for result in ordered if result.simulation_mode == SIMULATION_MODE_VALIDATION]
    if validation_mode_results:
        return validation_mode_results[-1]
    return ordered[-1] if ordered else None


def _latest_kpi_snapshot(results: list[OpportunityKpiSnapshot]) -> OpportunityKpiSnapshot | None:
    if not results:
        return None
    return max(results, key=lambda row: (row.snapshot_timestamp, row.id))


def _score_from_validation(result: ValidationResult | None) -> Decimal:
    if result is None or result.score is None:
        return ZERO
    return _bounded_ratio(result.score)


def _edge_strength(value: Decimal | None) -> Decimal:
    if value is None or value <= ZERO:
        return ZERO
    if value >= Decimal("0.2000"):
        return ONE
    if value >= Decimal("0.1000"):
        return Decimal("0.8500")
    if value >= Decimal("0.0500"):
        return Decimal("0.6500")
    return Decimal("0.4000")


def _size_strength(value: Decimal | None) -> Decimal:
    if value is None or value <= ZERO:
        return ZERO
    if value >= Decimal("100.0000"):
        return ONE
    if value >= Decimal("50.0000"):
        return Decimal("0.8000")
    if value >= Decimal("25.0000"):
        return Decimal("0.6000")
    if value >= Decimal("10.0000"):
        return Decimal("0.4000")
    return Decimal("0.2000")


def _persistence_strength(seconds: int | None) -> Decimal:
    if seconds is None or seconds < 0:
        return ZERO
    if seconds >= 600:
        return ONE
    if seconds >= 300:
        return Decimal("0.8500")
    if seconds >= 120:
        return Decimal("0.7000")
    if seconds >= 60:
        return Decimal("0.5000")
    if seconds > 0:
        return Decimal("0.3000")
    return ZERO


def _fill_strength(fill_ratio: Decimal | None) -> Decimal:
    if fill_ratio is None or fill_ratio <= ZERO:
        return ZERO
    if fill_ratio >= Decimal("1.0000"):
        return ONE
    if fill_ratio >= Decimal("0.9000"):
        return Decimal("0.8500")
    if fill_ratio >= Decimal("0.7500"):
        return Decimal("0.6500")
    if fill_ratio >= Decimal("0.5000"):
        return Decimal("0.4000")
    return Decimal("0.2000")


def _capital_lock_strength(hours: Decimal | None) -> Decimal:
    if hours is None or hours < ZERO:
        return ZERO
    if hours <= Decimal("0.0200"):
        return ONE
    if hours <= Decimal("0.0500"):
        return Decimal("0.8500")
    if hours <= Decimal("0.2500"):
        return Decimal("0.7000")
    if hours <= Decimal("1.0000"):
        return Decimal("0.4500")
    return Decimal("0.2000")


def _bounded_ratio(value: Decimal) -> Decimal:
    return min(ONE, max(ZERO, value.quantize(SCORE_PRECISION)))


def _average(values: list[Decimal]) -> Decimal:
    if not values:
        return ZERO
    return _bounded_ratio(sum(values, ZERO) / Decimal(len(values)))


def _reason_code(result: ValidationResult) -> str | None:
    details = result.details_json
    if not isinstance(details, dict):
        return None
    reason_code = details.get("reason_code")
    return str(reason_code) if reason_code is not None else None


def _iter_risk_flags(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [flag for flag in value if isinstance(flag, dict)]


def _warning(code: str, text: str, *, blocking: bool, severity: int) -> _Warning:
    return _Warning(
        code=code,
        text=text,
        penalty=WARNING_PENALTIES[code],
        blocking=blocking,
        severity=severity,
    )


def _dedupe_warnings(warnings: list[_Warning]) -> list[_Warning]:
    deduped: dict[tuple[str, str], _Warning] = {}
    for warning in warnings:
        key = (warning.code, warning.text)
        existing = deduped.get(key)
        if existing is None or (warning.blocking and not existing.blocking):
            deduped[key] = warning
    return list(deduped.values())


def _quantize_score(value: Decimal) -> Decimal:
    return value.quantize(SCORE_PRECISION)
