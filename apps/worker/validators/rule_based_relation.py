from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from apps.api.services.opportunity_classification import DetectionFamily


VALIDATOR_VERSION = "rule_based_relation_v1"
VALIDATION_TYPE = "rule_based_relation"

STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_INCONCLUSIVE = "inconclusive"

RELATION_IMPLICATION = "implication"
RELATION_SUBSET = "subset"
RELATION_EQUIVALENCE = "equivalence"
RELATION_TEMPORAL_NESTING = "temporal_nesting"
RELATION_NO_FORMAL = "no_formal_relation"

APPROVED_RELATION_TYPES = {
    RELATION_IMPLICATION,
    RELATION_SUBSET,
    RELATION_EQUIVALENCE,
    RELATION_TEMPORAL_NESTING,
    RELATION_NO_FORMAL,
}

REASON_NO_FORMAL_RELATION = "no_formal_relation"
REASON_MISSING_RELATION_TYPE = "missing_relation_type"
REASON_UNSUPPORTED_RELATION_TYPE = "unsupported_relation_type"
REASON_MISSING_FORMAL_CLAIM = "missing_formal_claim"
REASON_RELATION_TYPE_MISMATCH = "relation_type_mismatch"
REASON_INVALID_MARKET_SCOPE = "invalid_market_scope"
REASON_MISSING_TEMPORAL_BOUNDS = "missing_temporal_bounds"
REASON_TEMPORAL_NOT_NESTED = "temporal_not_nested"

SCORE_VALID = Decimal("1.0000")
SCORE_INVALID = Decimal("0.0000")


@dataclass(frozen=True, slots=True)
class RuleBasedRelationValidationInput:
    opportunity_id: int
    event_group_key: str
    family: str
    relation_type: str | None
    relation_direction: str | None
    involved_market_ids: list[int]
    normalized_dates: dict[str, Any] | list[Any] | None
    raw_context: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class RuleBasedRelationValidationResult:
    validation_type: str
    status: str
    score: Decimal | None
    summary: str
    details: dict[str, Any]
    validator_version: str


def validate_rule_based_relation(
    candidate: RuleBasedRelationValidationInput,
) -> RuleBasedRelationValidationResult:
    if candidate.family == DetectionFamily.NEG_RISK_CONVERSION.value and candidate.relation_type is None:
        return _result(
            status=STATUS_VALID,
            score=SCORE_VALID,
            summary="validated no_formal_relation for neg_risk_conversion",
            candidate=candidate,
            canonical_relation_type=RELATION_NO_FORMAL,
            reason_code=REASON_NO_FORMAL_RELATION,
            evidence={
                "family_default": DetectionFamily.NEG_RISK_CONVERSION.value,
            },
        )

    if candidate.relation_type is None:
        return _result(
            status=STATUS_INCONCLUSIVE,
            score=None,
            summary="inconclusive: missing relation_type",
            candidate=candidate,
            canonical_relation_type=None,
            reason_code=REASON_MISSING_RELATION_TYPE,
        )

    if candidate.relation_type not in APPROVED_RELATION_TYPES:
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            summary=f"invalid: unsupported relation_type {candidate.relation_type}",
            candidate=candidate,
            canonical_relation_type=candidate.relation_type,
            reason_code=REASON_UNSUPPORTED_RELATION_TYPE,
        )

    if candidate.relation_type == RELATION_NO_FORMAL:
        return _result(
            status=STATUS_VALID,
            score=SCORE_VALID,
            summary="validated explicit no_formal_relation",
            candidate=candidate,
            canonical_relation_type=RELATION_NO_FORMAL,
            reason_code=REASON_NO_FORMAL_RELATION,
        )

    formal_claim = _extract_formal_relation_claim(candidate.raw_context)
    if formal_claim is None:
        return _result(
            status=STATUS_INCONCLUSIVE,
            score=None,
            summary="inconclusive: missing formal relation claim",
            candidate=candidate,
            canonical_relation_type=candidate.relation_type,
            reason_code=REASON_MISSING_FORMAL_CLAIM,
        )

    claim_kind = formal_claim.get("kind")
    if claim_kind != candidate.relation_type:
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            summary="invalid: formal relation claim does not match relation_type",
            candidate=candidate,
            canonical_relation_type=candidate.relation_type,
            reason_code=REASON_RELATION_TYPE_MISMATCH,
            evidence={"formal_claim": formal_claim},
        )

    scope_validation = _validate_market_scope(candidate.involved_market_ids, formal_claim)
    if scope_validation is not None:
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            summary="invalid: formal relation market scope is inconsistent",
            candidate=candidate,
            canonical_relation_type=candidate.relation_type,
            reason_code=REASON_INVALID_MARKET_SCOPE,
            evidence={"formal_claim": formal_claim, "scope_validation": scope_validation},
        )

    if candidate.relation_type == RELATION_TEMPORAL_NESTING:
        return _validate_temporal_nesting(candidate, formal_claim)

    return _result(
        status=STATUS_VALID,
        score=SCORE_VALID,
        summary=f"validated explicit {candidate.relation_type} relation metadata",
        candidate=candidate,
        canonical_relation_type=candidate.relation_type,
        reason_code="formal_relation_verified",
        evidence={"formal_claim": formal_claim},
    )


def _validate_temporal_nesting(
    candidate: RuleBasedRelationValidationInput,
    formal_claim: dict[str, Any],
) -> RuleBasedRelationValidationResult:
    source_market_ids = _coerce_market_ids(formal_claim.get("source_market_ids"))
    target_market_ids = _coerce_market_ids(formal_claim.get("target_market_ids"))
    if len(source_market_ids) != 1 or len(target_market_ids) != 1:
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            summary="invalid: temporal_nesting requires exactly one source and one target market",
            candidate=candidate,
            canonical_relation_type=RELATION_TEMPORAL_NESTING,
            reason_code=REASON_INVALID_MARKET_SCOPE,
            evidence={"formal_claim": formal_claim},
        )

    source_interval = _extract_market_interval(candidate.normalized_dates, source_market_ids[0])
    target_interval = _extract_market_interval(candidate.normalized_dates, target_market_ids[0])
    if source_interval is None or target_interval is None:
        return _result(
            status=STATUS_INCONCLUSIVE,
            score=None,
            summary="inconclusive: temporal_nesting requires normalized start and end bounds",
            candidate=candidate,
            canonical_relation_type=RELATION_TEMPORAL_NESTING,
            reason_code=REASON_MISSING_TEMPORAL_BOUNDS,
            evidence={
                "formal_claim": formal_claim,
                "source_interval": source_interval,
                "target_interval": target_interval,
            },
        )

    source_start, source_end = source_interval
    target_start, target_end = target_interval
    if source_start < target_start or source_end > target_end:
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            summary="invalid: temporal interval is not nested within the claimed container",
            candidate=candidate,
            canonical_relation_type=RELATION_TEMPORAL_NESTING,
            reason_code=REASON_TEMPORAL_NOT_NESTED,
            evidence={
                "formal_claim": formal_claim,
                "source_interval": _serialize_interval(source_interval),
                "target_interval": _serialize_interval(target_interval),
            },
        )

    return _result(
        status=STATUS_VALID,
        score=SCORE_VALID,
        summary="validated temporal_nesting relation",
        candidate=candidate,
        canonical_relation_type=RELATION_TEMPORAL_NESTING,
        reason_code="temporal_nesting_verified",
        evidence={
            "formal_claim": formal_claim,
            "source_interval": _serialize_interval(source_interval),
            "target_interval": _serialize_interval(target_interval),
        },
    )


def _validate_market_scope(
    involved_market_ids: list[int],
    formal_claim: dict[str, Any],
) -> dict[str, Any] | None:
    opportunity_market_ids = set(involved_market_ids)
    source_market_ids = _coerce_market_ids(formal_claim.get("source_market_ids"))
    target_market_ids = _coerce_market_ids(formal_claim.get("target_market_ids"))
    if not source_market_ids or not target_market_ids:
        return {
            "source_market_ids": source_market_ids,
            "target_market_ids": target_market_ids,
            "reason": "missing_source_or_target",
        }

    source_set = set(source_market_ids)
    target_set = set(target_market_ids)
    if not source_set.issubset(opportunity_market_ids) or not target_set.issubset(opportunity_market_ids):
        return {
            "source_market_ids": source_market_ids,
            "target_market_ids": target_market_ids,
            "reason": "markets_outside_opportunity_scope",
        }

    if source_set & target_set:
        return {
            "source_market_ids": source_market_ids,
            "target_market_ids": target_market_ids,
            "reason": "source_target_overlap",
        }

    return None


def _extract_formal_relation_claim(raw_context: dict[str, Any] | None) -> dict[str, Any] | None:
    if raw_context is None:
        return None
    claim = raw_context.get("formal_relation")
    if isinstance(claim, dict):
        return claim
    return None


def _extract_market_interval(
    normalized_dates: dict[str, Any] | list[Any] | None,
    market_id: int,
) -> tuple[datetime, datetime] | None:
    if not isinstance(normalized_dates, dict):
        return None

    market_dates = normalized_dates.get(str(market_id))
    if market_dates is None:
        market_dates = normalized_dates.get(market_id)
    if not isinstance(market_dates, dict):
        return None

    start_at = _parse_datetime(market_dates.get("start_at") or market_dates.get("start"))
    end_at = _parse_datetime(market_dates.get("end_at") or market_dates.get("end"))
    if start_at is None or end_at is None or start_at > end_at:
        return None

    return start_at, end_at


def _coerce_market_ids(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    market_ids: list[int] = []
    for item in value:
        if isinstance(item, int):
            market_ids.append(item)
            continue
        if isinstance(item, str) and item.isdigit():
            market_ids.append(int(item))
    return market_ids


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _serialize_interval(interval: tuple[datetime, datetime]) -> dict[str, str]:
    return {
        "start_at": interval[0].isoformat(),
        "end_at": interval[1].isoformat(),
    }


def _result(
    *,
    status: str,
    score: Decimal | None,
    summary: str,
    candidate: RuleBasedRelationValidationInput,
    canonical_relation_type: str | None,
    reason_code: str,
    evidence: dict[str, Any] | None = None,
) -> RuleBasedRelationValidationResult:
    details = {
        "opportunity_id": candidate.opportunity_id,
        "event_group_key": candidate.event_group_key,
        "family": candidate.family,
        "relation_type": canonical_relation_type,
        "relation_direction": candidate.relation_direction,
        "reason_code": reason_code,
        "involved_market_ids": candidate.involved_market_ids,
        "evidence": evidence or {},
    }
    return RuleBasedRelationValidationResult(
        validation_type=VALIDATION_TYPE,
        status=status,
        score=score,
        summary=summary,
        details=details,
        validator_version=VALIDATOR_VERSION,
    )
