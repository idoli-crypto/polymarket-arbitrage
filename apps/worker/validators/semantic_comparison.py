from __future__ import annotations

from decimal import Decimal
from typing import Any


STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_INCONCLUSIVE = "inconclusive"

SCORE_VALID = Decimal("1.0000")
SCORE_INVALID = Decimal("0.0000")
SCORE_INCONCLUSIVE = Decimal("0.5000")

REASON_INSUFFICIENT_MARKETS = "insufficient_markets"
REASON_MISSING_QUESTION = "missing_question"
REASON_TEMPLATE_MISMATCH = "template_mismatch"
REASON_POLARITY_MISMATCH = "polarity_mismatch"
REASON_CONTEXT_ENTITY_MISMATCH = "context_entity_mismatch"
REASON_DATE_MISMATCH = "date_mismatch"
REASON_THRESHOLD_MISMATCH = "threshold_mismatch"
REASON_UNIT_MISMATCH = "unit_mismatch"
REASON_PARTIAL_STRUCTURED_DATA = "partial_structured_data"


def compare_semantic_markets(normalized_markets: dict[str, dict[str, Any]]) -> dict[str, Any]:
    ordered = [normalized_markets[key] for key in sorted(normalized_markets, key=lambda value: int(value))]
    if len(ordered) < 2:
        return _result(
            status=STATUS_INCONCLUSIVE,
            score=SCORE_INCONCLUSIVE,
            reason_code=REASON_INSUFFICIENT_MARKETS,
            summary="inconclusive: semantic validation requires at least two markets",
            checks=[_check("market_count", STATUS_INCONCLUSIVE, {"market_count": len(ordered)})],
        )

    if any(not market.get("question") for market in ordered):
        return _result(
            status=STATUS_INCONCLUSIVE,
            score=SCORE_INCONCLUSIVE,
            reason_code=REASON_MISSING_QUESTION,
            summary="inconclusive: one or more markets are missing question text",
            checks=[_check("question_text", STATUS_INCONCLUSIVE, {"market_ids": [market["market_id"] for market in ordered]})],
        )

    checks = [
        _compare_polarity(ordered),
        _compare_context_entities(ordered),
        _compare_dates_and_timeframes(ordered),
        _compare_thresholds(ordered),
        _compare_units(ordered),
        _compare_templates(ordered),
    ]

    non_template_mismatches = [
        check
        for check in checks
        if check["status"] == STATUS_INVALID and check.get("reason_code") != REASON_TEMPLATE_MISMATCH
    ]
    if non_template_mismatches:
        first = non_template_mismatches[0]
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            reason_code=first["reason_code"],
            summary=f"invalid: {first['reason_code']}",
            checks=checks,
        )

    inconclusive_checks = [check for check in checks if check["status"] == STATUS_INCONCLUSIVE]
    if inconclusive_checks:
        first = inconclusive_checks[0]
        return _result(
            status=STATUS_INCONCLUSIVE,
            score=SCORE_INCONCLUSIVE,
            reason_code=first["reason_code"],
            summary=f"inconclusive: {first['reason_code']}",
            checks=checks,
        )

    template_mismatches = [
        check
        for check in checks
        if check["status"] == STATUS_INVALID and check.get("reason_code") == REASON_TEMPLATE_MISMATCH
    ]
    if template_mismatches:
        first = template_mismatches[0]
        return _result(
            status=STATUS_INVALID,
            score=SCORE_INVALID,
            reason_code=first["reason_code"],
            summary=f"invalid: {first['reason_code']}",
            checks=checks,
        )

    return _result(
        status=STATUS_VALID,
        score=SCORE_VALID,
        reason_code="semantic_alignment_verified",
        summary="valid: semantic alignment verified",
        checks=checks,
    )


def _compare_templates(markets: list[dict[str, Any]]) -> dict[str, Any]:
    templates = {market["semantic_template"] for market in markets}
    if len(templates) != 1:
        return _check(
            "semantic_template",
            STATUS_INVALID,
            {
                "templates": {
                    str(market["market_id"]): market["semantic_template"]
                    for market in markets
                }
            },
            reason_code=REASON_TEMPLATE_MISMATCH,
        )
    return _check(
        "semantic_template",
        STATUS_VALID,
        {"template": next(iter(templates))},
    )


def _compare_polarity(markets: list[dict[str, Any]]) -> dict[str, Any]:
    signatures = {
        (
            market["polarity"]["direction"],
            market["polarity"]["negated"],
        )
        for market in markets
    }
    if len(signatures) != 1:
        return _check(
            "polarity",
            STATUS_INVALID,
            {
                "signatures": {
                    str(market["market_id"]): market["polarity"]
                    for market in markets
                }
            },
            reason_code=REASON_POLARITY_MISMATCH,
        )
    return _check(
        "polarity",
        STATUS_VALID,
        {"signature": markets[0]["polarity"]},
    )


def _compare_context_entities(markets: list[dict[str, Any]]) -> dict[str, Any]:
    contextual = [_context_entity_signature(market) for market in markets]
    if any(signature is None for signature in contextual):
        return _check(
            "entities",
            STATUS_INCONCLUSIVE,
            {"signatures": contextual},
            reason_code=REASON_PARTIAL_STRUCTURED_DATA,
        )
    if len(set(contextual)) != 1:
        return _check(
            "entities",
            STATUS_INVALID,
            {"signatures": contextual},
            reason_code=REASON_CONTEXT_ENTITY_MISMATCH,
        )
    return _check(
        "entities",
        STATUS_VALID,
        {"context_entities": contextual[0]},
    )


def _compare_dates_and_timeframes(markets: list[dict[str, Any]]) -> dict[str, Any]:
    date_signatures = [_date_signature(market) for market in markets]
    timeframe_signatures = [_timeframe_signature(market) for market in markets]

    if _has_partial_presence(date_signatures) or _has_partial_presence(timeframe_signatures):
        return _check(
            "dates",
            STATUS_INCONCLUSIVE,
            {
                "date_signatures": date_signatures,
                "timeframe_signatures": timeframe_signatures,
            },
            reason_code=REASON_PARTIAL_STRUCTURED_DATA,
        )

    if len({signature for signature in date_signatures if signature is not None}) > 1:
        return _check(
            "dates",
            STATUS_INVALID,
            {"date_signatures": date_signatures},
            reason_code=REASON_DATE_MISMATCH,
        )

    if len({signature for signature in timeframe_signatures if signature is not None}) > 1:
        return _check(
            "timeframes",
            STATUS_INVALID,
            {"timeframe_signatures": timeframe_signatures},
            reason_code=REASON_DATE_MISMATCH,
        )

    return _check(
        "dates",
        STATUS_VALID,
        {
            "date_signature": next((signature for signature in date_signatures if signature is not None), None),
            "timeframe_signature": next(
                (signature for signature in timeframe_signatures if signature is not None),
                None,
            ),
        },
    )


def _compare_thresholds(markets: list[dict[str, Any]]) -> dict[str, Any]:
    signatures = [_threshold_signature(market) for market in markets]
    if _has_partial_presence(signatures):
        return _check(
            "thresholds",
            STATUS_INCONCLUSIVE,
            {"signatures": signatures},
            reason_code=REASON_PARTIAL_STRUCTURED_DATA,
        )
    if len({signature for signature in signatures if signature is not None}) > 1:
        return _check(
            "thresholds",
            STATUS_INVALID,
            {"signatures": signatures},
            reason_code=REASON_THRESHOLD_MISMATCH,
        )
    return _check(
        "thresholds",
        STATUS_VALID,
        {"signature": next((signature for signature in signatures if signature is not None), None)},
    )


def _compare_units(markets: list[dict[str, Any]]) -> dict[str, Any]:
    signatures = [tuple(market["units"]) for market in markets]
    if _has_partial_presence(signatures):
        return _check(
            "units",
            STATUS_INCONCLUSIVE,
            {"signatures": signatures},
            reason_code=REASON_PARTIAL_STRUCTURED_DATA,
        )
    if len({signature for signature in signatures if signature}) > 1:
        return _check(
            "units",
            STATUS_INVALID,
            {"signatures": signatures},
            reason_code=REASON_UNIT_MISMATCH,
        )
    return _check(
        "units",
        STATUS_VALID,
        {"signature": signatures[0] if signatures else ()},
    )


def _context_entity_signature(market: dict[str, Any]) -> tuple[tuple[str, str], ...] | None:
    entities = market["entities"]
    if not entities:
        return ()
    context_entities = entities[1:]
    if not context_entities:
        return ()
    return tuple((entity["kind"], entity["canonical"]) for entity in context_entities)


def _date_signature(market: dict[str, Any]) -> tuple[tuple[str, str], ...] | None:
    dates = market["dates"]
    if not dates:
        return None
    return tuple((date["granularity"], date["canonical"]) for date in dates)


def _timeframe_signature(market: dict[str, Any]) -> tuple[tuple[str, str], ...] | None:
    timeframes = market["timeframes"]
    if not timeframes:
        return None
    return tuple((timeframe["relation"], timeframe["canonical"]) for timeframe in timeframes)


def _threshold_signature(
    market: dict[str, Any],
) -> tuple[tuple[str, str | None, str | None, str | None, str | None], ...] | None:
    thresholds = market["thresholds"]
    if not thresholds:
        return None
    signature: list[tuple[str, str | None, str | None, str | None, str | None]] = []
    for threshold in thresholds:
        signature.append(
            (
                threshold["comparator"],
                threshold.get("value"),
                threshold.get("low"),
                threshold.get("high"),
                threshold.get("unit"),
            )
        )
    return tuple(signature)


def _has_partial_presence(values: list[object | None]) -> bool:
    present = [value for value in values if value not in (None, (), [])]
    missing = [value for value in values if value in (None, (), [])]
    return bool(present) and bool(missing)


def _check(
    name: str,
    status: str,
    evidence: dict[str, Any],
    *,
    reason_code: str | None = None,
) -> dict[str, Any]:
    payload = {
        "name": name,
        "status": status,
        "evidence": evidence,
    }
    if reason_code is not None:
        payload["reason_code"] = reason_code
    return payload


def _result(
    *,
    status: str,
    score: Decimal,
    reason_code: str,
    summary: str,
    checks: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": status,
        "score": score,
        "summary": summary,
        "reason_code": reason_code,
        "checks": checks,
    }
