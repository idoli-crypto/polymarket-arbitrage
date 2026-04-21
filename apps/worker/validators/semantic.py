from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


VALIDATION_STATUS_VALID = "valid"
VALIDATION_STATUS_REJECTED = "rejected"
VALIDATION_REASON_EVENT_MISMATCH = "event_mismatch"
VALIDATION_REASON_INCOMPLETE_OUTCOMES = "incomplete_outcomes"
VALIDATION_REASON_NOT_NEG_RISK = "not_neg_risk"
VALIDATION_REASON_DUPLICATE_MARKETS = "duplicate_markets"
VALIDATION_REASON_MISSING_SNAPSHOT = "missing_snapshot"
RULES_VALIDATION_STATUS_PENDING = "pending"
SNAPSHOT_FRESHNESS_STATUS_PENDING = "pending"


@dataclass(frozen=True, slots=True)
class ValidationMarketInput:
    market_id: int
    question: str
    condition_id: str | None
    event_id: str | None
    event_slug: str | None
    neg_risk: bool


@dataclass(frozen=True, slots=True)
class ValidationSnapshotInput:
    snapshot_id: int
    market_id: int
    captured_at: datetime


@dataclass(frozen=True, slots=True)
class SemanticValidationInput:
    opportunity_id: int
    event_group_key: str
    involved_market_ids: list[int]


@dataclass(frozen=True, slots=True)
class SemanticValidationResult:
    validation_status: str
    validation_reason: str | None
    rules_validation_status: str
    snapshot_freshness_status: str
    evidence: dict[str, Any]


def validate_semantic_opportunity(
    opportunity: SemanticValidationInput,
    *,
    markets: list[ValidationMarketInput],
    latest_snapshots: dict[int, ValidationSnapshotInput | None],
) -> SemanticValidationResult:
    market_by_id = {market.market_id: market for market in markets}
    ordered_markets = [
        market_by_id[market_id]
        for market_id in opportunity.involved_market_ids
        if market_id in market_by_id
    ]

    if len(ordered_markets) != len(opportunity.involved_market_ids):
        return _rejected(
            VALIDATION_REASON_EVENT_MISMATCH,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
            missing_market_ids=[
                market_id
                for market_id in opportunity.involved_market_ids
                if market_id not in market_by_id
            ],
        )

    shared_event_ids = {market.event_id for market in ordered_markets}
    shared_event_slugs = {market.event_slug for market in ordered_markets}
    if (
        None in shared_event_ids
        or None in shared_event_slugs
        or len(shared_event_ids) != 1
        or len(shared_event_slugs) != 1
        or next(iter(shared_event_ids)) != opportunity.event_group_key
    ):
        return _rejected(
            VALIDATION_REASON_EVENT_MISMATCH,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
        )

    missing_snapshot_market_ids = [
        market.market_id
        for market in ordered_markets
        if latest_snapshots.get(market.market_id) is None
    ]
    if missing_snapshot_market_ids:
        return _rejected(
            VALIDATION_REASON_MISSING_SNAPSHOT,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
            missing_snapshot_market_ids=missing_snapshot_market_ids,
        )

    if len(opportunity.involved_market_ids) < 2:
        return _rejected(
            VALIDATION_REASON_INCOMPLETE_OUTCOMES,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
        )

    if any(market.neg_risk is not True for market in ordered_markets):
        return _rejected(
            VALIDATION_REASON_NOT_NEG_RISK,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
        )

    if _has_duplicate_market_ids(opportunity.involved_market_ids):
        return _rejected(
            VALIDATION_REASON_DUPLICATE_MARKETS,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
        )

    if _has_duplicate_outcomes(ordered_markets):
        return _rejected(
            VALIDATION_REASON_DUPLICATE_MARKETS,
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
        )

    return SemanticValidationResult(
        validation_status=VALIDATION_STATUS_VALID,
        validation_reason=None,
        rules_validation_status=RULES_VALIDATION_STATUS_PENDING,
        snapshot_freshness_status=SNAPSHOT_FRESHNESS_STATUS_PENDING,
        evidence=_build_evidence(
            opportunity=opportunity,
            ordered_markets=ordered_markets,
            latest_snapshots=latest_snapshots,
        ),
    )


def _has_duplicate_market_ids(market_ids: list[int]) -> bool:
    return len(set(market_ids)) != len(market_ids)


def _has_duplicate_outcomes(markets: list[ValidationMarketInput]) -> bool:
    outcome_keys: list[str] = []
    for market in markets:
        if market.condition_id:
            outcome_keys.append(f"condition:{market.condition_id}")
            continue
        outcome_keys.append(f"question:{_normalize_text(market.question)}")
    return len(set(outcome_keys)) != len(outcome_keys)


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())


def _rejected(
    reason: str,
    *,
    opportunity: SemanticValidationInput,
    ordered_markets: list[ValidationMarketInput],
    latest_snapshots: dict[int, ValidationSnapshotInput | None],
    missing_market_ids: list[int] | None = None,
    missing_snapshot_market_ids: list[int] | None = None,
) -> SemanticValidationResult:
    evidence = _build_evidence(
        opportunity=opportunity,
        ordered_markets=ordered_markets,
        latest_snapshots=latest_snapshots,
    )
    if missing_market_ids:
        evidence["missing_market_ids"] = missing_market_ids
    if missing_snapshot_market_ids:
        evidence["missing_snapshot_market_ids"] = missing_snapshot_market_ids
    return SemanticValidationResult(
        validation_status=VALIDATION_STATUS_REJECTED,
        validation_reason=reason,
        rules_validation_status=RULES_VALIDATION_STATUS_PENDING,
        snapshot_freshness_status=SNAPSHOT_FRESHNESS_STATUS_PENDING,
        evidence=evidence,
    )


def _build_evidence(
    *,
    opportunity: SemanticValidationInput,
    ordered_markets: list[ValidationMarketInput],
    latest_snapshots: dict[int, ValidationSnapshotInput | None],
) -> dict[str, Any]:
    event_ids = sorted({market.event_id for market in ordered_markets if market.event_id is not None})
    event_slugs = sorted(
        {market.event_slug for market in ordered_markets if market.event_slug is not None}
    )
    return {
        "opportunity_id": opportunity.opportunity_id,
        "event_group_key": opportunity.event_group_key,
        "involved_market_ids": opportunity.involved_market_ids,
        "market_ids_loaded": [market.market_id for market in ordered_markets],
        "event_ids_seen": event_ids,
        "event_slugs_seen": event_slugs,
        "selected_snapshot_ids": {
            str(market_id): (
                snapshot.snapshot_id if snapshot is not None else None
            )
            for market_id, snapshot in sorted(latest_snapshots.items())
        },
    }
