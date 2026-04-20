from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable


DETECTOR_VERSION = "neg_risk_v1"
OPPORTUNITY_TYPE = "neg_risk_long_yes_bundle"
PRICE_PRECISION = Decimal("0.0001")
UNIT_PAYOUT = Decimal("1.0000")


@dataclass(slots=True)
class DetectionMarketInput:
    market_id: int
    polymarket_market_id: str
    question: str
    slug: str | None
    condition_id: str | None
    event_id: str | None
    event_slug: str | None
    neg_risk: bool
    snapshot_id: int
    snapshot_captured_at: datetime
    best_bid: Decimal | None
    best_ask: Decimal | None


@dataclass(slots=True)
class NegRiskCandidate:
    detection_window_start: datetime
    event_group_key: str
    involved_market_ids: list[int]
    opportunity_type: str
    outcome_count: int
    gross_price_sum: Decimal
    gross_gap: Decimal
    detector_version: str
    raw_context: dict[str, Any]


def detect_neg_risk_candidates(
    rows: Iterable[DetectionMarketInput],
    *,
    detector_version: str = DETECTOR_VERSION,
) -> list[NegRiskCandidate]:
    grouped_rows: dict[str, list[DetectionMarketInput]] = defaultdict(list)
    for row in rows:
        if row.neg_risk is True and row.event_id:
            grouped_rows[row.event_id].append(row)

    candidates: list[NegRiskCandidate] = []
    for event_group_key, group_rows in grouped_rows.items():
        candidate = _detect_long_bundle_candidate(
            event_group_key,
            group_rows,
            detector_version=detector_version,
        )
        if candidate is not None:
            candidates.append(candidate)

    return sorted(
        candidates,
        key=lambda candidate: (candidate.gross_gap, candidate.event_group_key),
        reverse=True,
    )


def _detect_long_bundle_candidate(
    event_group_key: str,
    rows: list[DetectionMarketInput],
    *,
    detector_version: str,
) -> NegRiskCandidate | None:
    ordered_rows = sorted(rows, key=lambda row: row.market_id)
    if len(ordered_rows) < 2:
        return None

    if any(not row.neg_risk for row in ordered_rows):
        return None

    if any(row.best_ask is None for row in ordered_rows):
        return None

    if any(row.condition_id is None for row in ordered_rows):
        return None

    normalized_questions = {_normalize_text(row.question) for row in ordered_rows}
    if len(normalized_questions) != len(ordered_rows):
        return None

    distinct_conditions = {row.condition_id for row in ordered_rows}
    if len(distinct_conditions) != len(ordered_rows):
        return None

    gross_price_sum = _quantize(sum(row.best_ask for row in ordered_rows if row.best_ask is not None))
    gross_gap = _quantize(UNIT_PAYOUT - gross_price_sum)
    if gross_gap <= Decimal("0"):
        return None

    return NegRiskCandidate(
        detection_window_start=_detection_window_start(ordered_rows),
        event_group_key=event_group_key,
        involved_market_ids=[row.market_id for row in ordered_rows],
        opportunity_type=OPPORTUNITY_TYPE,
        outcome_count=len(ordered_rows),
        gross_price_sum=gross_price_sum,
        gross_gap=gross_gap,
        detector_version=detector_version,
        raw_context={
            "pricing_basis": "latest_yes_best_ask_sum",
            "semantic_validation_status": "pending",
            "execution_evaluation_status": "pending",
            "event_slug": ordered_rows[0].event_slug,
            "markets": [
                {
                    "market_id": row.market_id,
                    "polymarket_market_id": row.polymarket_market_id,
                    "question": row.question,
                    "slug": row.slug,
                    "condition_id": row.condition_id,
                    "snapshot_id": row.snapshot_id,
                    "snapshot_captured_at": row.snapshot_captured_at.isoformat(),
                    "best_bid": _decimal_to_string(row.best_bid),
                    "best_ask": _decimal_to_string(row.best_ask),
                }
                for row in ordered_rows
            ],
        },
    )


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(_quantize(value), "f")


def _detection_window_start(rows: list[DetectionMarketInput]) -> datetime:
    latest_snapshot = max(row.snapshot_captured_at for row in rows)
    return latest_snapshot.replace(second=0, microsecond=0)


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(PRICE_PRECISION)
