from __future__ import annotations

from dataclasses import dataclass
from typing import Any


RESOLUTION_SOURCE_PATH = "resolutionSource"
END_DATE_PATHS = ("endDate", "endDateIso")
RESOLUTION_CONDITION_PATHS = ("description",)
CLARIFICATION_FLAG_PATHS = (
    "clarification",
    "clarifications",
    "clarificationFlag",
    "clarificationFlags",
)
DISPUTE_FLAG_PATHS = (
    "dispute",
    "disputes",
    "disputeFlag",
    "disputeFlags",
    "isDisputed",
    "umaResolutionStatus",
)
EDGE_CASE_RULE_PATHS = ("resolutionRules", "edgeCaseRules", "edgeCases")

RAW_PATHS_CHECKED = (
    RESOLUTION_SOURCE_PATH,
    *END_DATE_PATHS,
    *RESOLUTION_CONDITION_PATHS,
    *CLARIFICATION_FLAG_PATHS,
    *DISPUTE_FLAG_PATHS,
    *EDGE_CASE_RULE_PATHS,
)


@dataclass(frozen=True, slots=True)
class ResolutionExtractionInput:
    market_id: int
    raw_market_json: dict[str, Any] | list[Any] | None


def extract_resolution_metadata(candidate: ResolutionExtractionInput) -> dict[str, Any]:
    raw_payload = candidate.raw_market_json if isinstance(candidate.raw_market_json, dict) else {}

    resolution_source = _extract_scalar_field(raw_payload, RESOLUTION_SOURCE_PATH)
    end_date = _extract_first_scalar_field(raw_payload, END_DATE_PATHS)
    resolution_conditions = _extract_list_fields(raw_payload, RESOLUTION_CONDITION_PATHS)
    clarification_flags = _extract_flag_fields(raw_payload, CLARIFICATION_FLAG_PATHS)
    dispute_flags = _extract_flag_fields(raw_payload, DISPUTE_FLAG_PATHS)
    edge_case_rules = _extract_list_fields(raw_payload, EDGE_CASE_RULE_PATHS)

    missing_fields: list[str] = []
    if resolution_source is None:
        missing_fields.append(RESOLUTION_SOURCE_PATH)
    if end_date is None:
        missing_fields.append("endDate")
    if not resolution_conditions:
        missing_fields.append("resolution_conditions")
    if not clarification_flags:
        missing_fields.append("clarification_flags")
    if not dispute_flags:
        missing_fields.append("dispute_flags")
    if not edge_case_rules:
        missing_fields.append("edge_case_rules")

    return {
        "market_id": candidate.market_id,
        "resolution_source": resolution_source,
        "end_date": end_date,
        "resolution_conditions": resolution_conditions,
        "clarification_flags": clarification_flags,
        "dispute_flags": dispute_flags,
        "edge_case_rules": edge_case_rules,
        "missing_fields": missing_fields,
        "raw_paths_checked": list(RAW_PATHS_CHECKED),
    }


def build_resolution_column_payloads(
    extracted_markets: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    resolution_sources: dict[str, Any] = {}
    end_dates: dict[str, Any] = {}
    clarification_flags: dict[str, Any] = {}
    dispute_flags: dict[str, Any] = {}

    for market in extracted_markets:
        market_key = str(market["market_id"])
        resolution_sources[market_key] = market["resolution_source"]
        end_dates[market_key] = market["end_date"]
        clarification_flags[market_key] = market["clarification_flags"]
        dispute_flags[market_key] = market["dispute_flags"]

    return resolution_sources, end_dates, clarification_flags, dispute_flags


def _extract_scalar_field(raw_payload: dict[str, Any], path: str) -> dict[str, Any] | None:
    if path not in raw_payload:
        return None

    value = raw_payload[path]
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return {"value": value, "evidence_path": path}


def _extract_first_scalar_field(raw_payload: dict[str, Any], paths: tuple[str, ...]) -> dict[str, Any] | None:
    for path in paths:
        extracted = _extract_scalar_field(raw_payload, path)
        if extracted is not None:
            return extracted
    return None


def _extract_list_fields(raw_payload: dict[str, Any], paths: tuple[str, ...]) -> list[dict[str, Any]]:
    extracted: list[dict[str, Any]] = []
    for path in paths:
        if path not in raw_payload:
            continue
        value = raw_payload[path]
        if value is None:
            continue
        extracted.append(
            {
                "field": path,
                "value": value,
                "evidence_path": path,
            }
        )
    return extracted


def _extract_flag_fields(raw_payload: dict[str, Any], paths: tuple[str, ...]) -> list[dict[str, Any]]:
    extracted: list[dict[str, Any]] = []
    for path in paths:
        if path not in raw_payload:
            continue
        value = raw_payload[path]
        if value is None:
            continue
        extracted.append(
            {
                "flag": path,
                "value": value,
                "evidence_path": path,
            }
        )
    return extracted
