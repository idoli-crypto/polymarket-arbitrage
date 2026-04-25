from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from apps.worker.validators.semantic_comparison import (
    REASON_MISSING_QUESTION,
    SCORE_INCONCLUSIVE,
    STATUS_INCONCLUSIVE,
    compare_semantic_markets,
)
from apps.worker.validators.semantic_normalization import SemanticMarketInput, normalize_semantic_markets


VALIDATION_TYPE = "semantic_validation"
VALIDATOR_VERSION = "semantic_validation_v1"

STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_INCONCLUSIVE = "inconclusive"


@dataclass(frozen=True, slots=True)
class SemanticValidationInput:
    opportunity_id: int
    event_group_key: str
    family: str
    relation_type: str | None
    involved_market_ids: list[int]
    question_texts: list[str] | None
    raw_context: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class SemanticValidationResult:
    validation_type: str
    status: str
    score: Decimal
    summary: str
    details: dict[str, Any]
    validator_version: str
    normalized_entities: dict[str, list[dict[str, Any]]]
    normalized_dates: dict[str, list[dict[str, Any]]]
    normalized_thresholds: dict[str, list[dict[str, Any]]]
    semantic_context: dict[str, Any]


def validate_semantic_opportunity(candidate: SemanticValidationInput) -> SemanticValidationResult:
    markets = _load_market_inputs(candidate)
    if len(markets) != len(candidate.involved_market_ids):
        return _build_result(
            candidate=candidate,
            comparison={
                "status": STATUS_INCONCLUSIVE,
                "score": SCORE_INCONCLUSIVE,
                "summary": "inconclusive: one or more markets are missing question text",
                "reason_code": REASON_MISSING_QUESTION,
                "checks": [],
            },
            normalized_markets={},
        )
    if not markets:
        return _build_result(
            candidate=candidate,
            comparison={
                "status": STATUS_INCONCLUSIVE,
                "score": SCORE_INCONCLUSIVE,
                "summary": "inconclusive: no market question text available for semantic validation",
                "reason_code": REASON_MISSING_QUESTION,
                "checks": [],
            },
            normalized_markets={},
        )

    normalized_markets = normalize_semantic_markets(markets)
    comparison = compare_semantic_markets(normalized_markets)
    return _build_result(
        candidate=candidate,
        comparison=comparison,
        normalized_markets=normalized_markets,
    )


def _load_market_inputs(candidate: SemanticValidationInput) -> list[SemanticMarketInput]:
    questions_by_market_id: dict[int, str] = {}
    raw_markets = candidate.raw_context.get("markets", []) if isinstance(candidate.raw_context, dict) else []
    if isinstance(raw_markets, list):
        for market in raw_markets:
            if not isinstance(market, dict):
                continue
            market_id = market.get("market_id")
            question = market.get("question")
            if isinstance(market_id, int) and isinstance(question, str) and question.strip():
                questions_by_market_id[market_id] = question

    ordered_questions = list(candidate.question_texts or [])
    markets: list[SemanticMarketInput] = []
    for index, market_id in enumerate(candidate.involved_market_ids):
        question = questions_by_market_id.get(market_id)
        if question is None and index < len(ordered_questions):
            fallback = ordered_questions[index]
            question = fallback if isinstance(fallback, str) and fallback.strip() else None
        if question is None:
            continue
        markets.append(SemanticMarketInput(market_id=market_id, question=question))
    return markets


def _build_result(
    *,
    candidate: SemanticValidationInput,
    comparison: dict[str, Any],
    normalized_markets: dict[str, dict[str, Any]],
) -> SemanticValidationResult:
    details = {
        "reason_code": comparison["reason_code"],
        "family": candidate.family,
        "relation_type": candidate.relation_type,
        "event_group_key": candidate.event_group_key,
        "normalized_markets": normalized_markets,
        "checks": comparison["checks"],
    }
    semantic_context = {
        "opportunity_id": candidate.opportunity_id,
        "event_group_key": candidate.event_group_key,
        "validation_type": VALIDATION_TYPE,
        "status": comparison["status"],
        "summary": comparison["summary"],
        "reason_code": comparison["reason_code"],
        "normalized_markets": normalized_markets,
        "checks": comparison["checks"],
    }
    return SemanticValidationResult(
        validation_type=VALIDATION_TYPE,
        status=comparison["status"],
        score=comparison["score"],
        summary=comparison["summary"],
        details=details,
        validator_version=VALIDATOR_VERSION,
        normalized_entities={
            market_id: payload["entities"]
            for market_id, payload in normalized_markets.items()
        },
        normalized_dates={
            market_id: payload["dates"]
            for market_id, payload in normalized_markets.items()
        },
        normalized_thresholds={
            market_id: payload["thresholds"]
            for market_id, payload in normalized_markets.items()
        },
        semantic_context=semantic_context,
    )
