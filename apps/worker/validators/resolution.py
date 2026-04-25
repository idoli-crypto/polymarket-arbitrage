from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


VALIDATION_TYPE = "resolution_validation"
VALIDATOR_VERSION = "resolution_validation_v1"

STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_RISKY = "risky"
STATUS_INCONCLUSIVE = "inconclusive"

SCORE_VALID = Decimal("1.0000")
SCORE_INVALID = Decimal("0.0000")
SCORE_RISKY = Decimal("0.5000")


@dataclass(frozen=True, slots=True)
class ResolutionValidationInput:
    opportunity_id: int
    event_group_key: str
    family: str
    relation_type: str | None
    extracted_markets: list[dict[str, Any]]


@dataclass(frozen=True, slots=True)
class ResolutionValidationResult:
    validation_type: str
    status: str
    score: Decimal | None
    summary: str
    details: dict[str, Any]
    validator_version: str


def validate_resolution(candidate: ResolutionValidationInput) -> ResolutionValidationResult:
    if len(candidate.extracted_markets) < 2:
        details = {
            "reason_code": "insufficient_markets",
            "checks": [],
            "extracted_markets": candidate.extracted_markets,
            "comparison_evidence": {},
            "missing_fields": {
                str(market["market_id"]): list(market.get("missing_fields", []))
                for market in candidate.extracted_markets
            },
            "event_group_key": candidate.event_group_key,
            "family": candidate.family,
            "relation_type": candidate.relation_type,
        }
        return ResolutionValidationResult(
            validation_type=VALIDATION_TYPE,
            status=STATUS_INCONCLUSIVE,
            score=None,
            summary="inconclusive: resolution validation requires at least two markets",
            details=details,
            validator_version=VALIDATOR_VERSION,
        )

    checks = [
        _compare_resolution_sources(candidate.extracted_markets),
        _compare_end_dates(candidate.extracted_markets),
        _compare_resolution_conditions(candidate.extracted_markets),
        _compare_clarifications(candidate.extracted_markets),
        _compare_disputes(candidate.extracted_markets),
        _compare_edge_cases(candidate.extracted_markets),
    ]

    reason_code, status, score, summary = _finalize_result(checks)
    missing_fields = {
        str(market["market_id"]): list(market.get("missing_fields", []))
        for market in candidate.extracted_markets
    }
    comparison_evidence = {
        check["name"]: check.get("evidence")
        for check in checks
    }
    details = {
        "reason_code": reason_code,
        "checks": checks,
        "extracted_markets": candidate.extracted_markets,
        "comparison_evidence": comparison_evidence,
        "missing_fields": missing_fields,
        "event_group_key": candidate.event_group_key,
        "family": candidate.family,
        "relation_type": candidate.relation_type,
    }
    return ResolutionValidationResult(
        validation_type=VALIDATION_TYPE,
        status=status,
        score=score,
        summary=summary,
        details=details,
        validator_version=VALIDATOR_VERSION,
    )


def _compare_resolution_sources(extracted_markets: list[dict[str, Any]]) -> dict[str, Any]:
    values = _collect_scalar_values(extracted_markets, "resolution_source")
    if values is None:
        return _inconclusive_check("resolution_source_consistency", "missing_resolution_source", extracted_markets)
    if len(set(values.values())) != 1:
        return _invalid_check("resolution_source_consistency", "resolution_source_mismatch", values)
    return _valid_check("resolution_source_consistency", "resolution_source_aligned", values)


def _compare_end_dates(extracted_markets: list[dict[str, Any]]) -> dict[str, Any]:
    values = _collect_scalar_values(extracted_markets, "end_date")
    if values is None:
        return _inconclusive_check("end_date_compatibility", "missing_end_date", extracted_markets)
    if len(set(values.values())) != 1:
        return _invalid_check("end_date_compatibility", "end_date_mismatch", values)
    return _valid_check("end_date_compatibility", "end_date_aligned", values)


def _compare_resolution_conditions(extracted_markets: list[dict[str, Any]]) -> dict[str, Any]:
    serialized = _collect_serialized_lists(extracted_markets, "resolution_conditions")
    if serialized is None:
        return _inconclusive_check("condition_alignment", "missing_resolution_conditions", extracted_markets)
    if len(set(serialized.values())) != 1:
        return _risky_check("condition_alignment", "resolution_conditions_mismatch", serialized)
    return _valid_check("condition_alignment", "resolution_conditions_aligned", serialized)


def _compare_clarifications(extracted_markets: list[dict[str, Any]]) -> dict[str, Any]:
    serialized = _collect_serialized_lists(extracted_markets, "clarification_flags")
    if serialized is None:
        return _inconclusive_check("clarification_mismatch", "missing_clarification_flags", extracted_markets)
    if len(set(serialized.values())) != 1:
        return _risky_check("clarification_mismatch", "clarification_flags_mismatch", serialized)
    return _valid_check("clarification_mismatch", "clarification_flags_aligned", serialized)


def _compare_disputes(extracted_markets: list[dict[str, Any]]) -> dict[str, Any]:
    dispute_flags = _collect_lists(extracted_markets, "dispute_flags")
    if dispute_flags is None:
        return _inconclusive_check("dispute_presence", "missing_dispute_flags", extracted_markets)
    if any(_has_dispute_risk(flags) for flags in dispute_flags.values()):
        return _risky_check("dispute_presence", "dispute_flag_present", dispute_flags)
    serialized = {
        market_id: json.dumps(flags, sort_keys=True)
        for market_id, flags in dispute_flags.items()
    }
    if len(set(serialized.values())) != 1:
        return _risky_check("dispute_presence", "dispute_flags_mismatch", dispute_flags)
    return _valid_check("dispute_presence", "no_dispute_flags", dispute_flags)


def _compare_edge_cases(extracted_markets: list[dict[str, Any]]) -> dict[str, Any]:
    serialized = _collect_serialized_lists(extracted_markets, "edge_case_rules")
    if serialized is None:
        return _inconclusive_check("edge_case_handling", "missing_edge_case_rules", extracted_markets)
    if len(set(serialized.values())) != 1:
        return _risky_check("edge_case_handling", "edge_case_rules_mismatch", serialized)
    return _valid_check("edge_case_handling", "edge_case_rules_aligned", serialized)


def _collect_scalar_values(
    extracted_markets: list[dict[str, Any]],
    key: str,
) -> dict[str, Any] | None:
    values: dict[str, Any] = {}
    for market in extracted_markets:
        payload = market.get(key)
        if not isinstance(payload, dict) or "value" not in payload:
            return None
        values[str(market["market_id"])] = payload["value"]
    return values


def _collect_serialized_lists(
    extracted_markets: list[dict[str, Any]],
    key: str,
) -> dict[str, str] | None:
    lists = _collect_lists(extracted_markets, key)
    if lists is None:
        return None
    return {
        market_id: json.dumps(payload, sort_keys=True)
        for market_id, payload in lists.items()
    }


def _collect_lists(
    extracted_markets: list[dict[str, Any]],
    key: str,
) -> dict[str, list[dict[str, Any]]] | None:
    collected: dict[str, list[dict[str, Any]]] = {}
    for market in extracted_markets:
        payload = market.get(key)
        if not isinstance(payload, list) or not payload:
            return None
        collected[str(market["market_id"])] = payload
    return collected


def _finalize_result(checks: list[dict[str, Any]]) -> tuple[str, str, Decimal | None, str]:
    for status, summary_prefix, score in (
        (STATUS_INVALID, "invalid", SCORE_INVALID),
        (STATUS_RISKY, "risky", SCORE_RISKY),
        (STATUS_INCONCLUSIVE, "inconclusive", None),
    ):
        for check in checks:
            if check["status"] == status:
                return check["reason_code"], status, score, f"{summary_prefix}: {check['reason_code']}"
    return "resolution_alignment_verified", STATUS_VALID, SCORE_VALID, "valid: resolution alignment verified"


def _valid_check(name: str, reason_code: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "status": STATUS_VALID,
        "reason_code": reason_code,
        "evidence": evidence,
    }


def _invalid_check(name: str, reason_code: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "status": STATUS_INVALID,
        "reason_code": reason_code,
        "evidence": evidence,
    }


def _risky_check(name: str, reason_code: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "status": STATUS_RISKY,
        "reason_code": reason_code,
        "evidence": evidence,
    }


def _inconclusive_check(
    name: str,
    reason_code: str,
    extracted_markets: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "name": name,
        "status": STATUS_INCONCLUSIVE,
        "reason_code": reason_code,
        "evidence": {
            str(market["market_id"]): list(market.get("missing_fields", []))
            for market in extracted_markets
        },
    }


def _has_dispute_risk(flags: list[dict[str, Any]]) -> bool:
    for flag in flags:
        value = flag.get("value")
        if isinstance(value, bool):
            if value:
                return True
            continue
        if isinstance(value, (int, float)):
            if value != 0:
                return True
            continue
        if value is None:
            continue
        normalized = str(value).strip().lower()
        if normalized in {"", "false", "0", "no", "none", "resolved", "undisputed"}:
            continue
        return True
    return False
