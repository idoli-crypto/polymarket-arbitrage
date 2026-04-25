from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import re


MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
MONTH_PATTERN = "|".join(MONTHS.keys())
CAPITALIZED_TOKEN_EXCLUSIONS = {
    "A",
    "An",
    "And",
    "Are",
    "At",
    "Before",
    "By",
    "Did",
    "During",
    "For",
    "From",
    "If",
    "In",
    "Is",
    "On",
    "Or",
    "The",
    "To",
    "Was",
    "Were",
    "Will",
}
LOCATION_PREPOSITIONS = {"in", "at", "from", "within"}
ORGANIZATION_SUFFIXES = {"inc", "corp", "corporation", "llc", "ltd", "plc", "bank", "party", "committee"}
NEGATION_PATTERN = re.compile(r"\b(?:not|no|never|fail to|won't|cannot|can't)\b", re.IGNORECASE)
POSITIVE_DIRECTION_PATTERN = re.compile(
    r"\b(?:win|wins|won|increase|increases|rise|rises|grow|grows|approve|approved|pass|passes|elect|elected|above|over|higher)\b",
    re.IGNORECASE,
)
NEGATIVE_DIRECTION_PATTERN = re.compile(
    r"\b(?:lose|loses|lost|decrease|decreases|fall|falls|drop|drops|reject|rejected|below|under|lower)\b",
    re.IGNORECASE,
)
THRESHOLD_PATTERNS = (
    (
        "range",
        re.compile(
            r"\bbetween\s+(?P<low>\d+(?:\.\d+)?)\s*(?P<unit1>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?\s+and\s+(?P<high>\d+(?:\.\d+)?)\s*(?P<unit2>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?",
            re.IGNORECASE,
        ),
    ),
    (
        "gte",
        re.compile(
            r"\b(?:at least|no less than)\s+\$?(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?",
            re.IGNORECASE,
        ),
    ),
    (
        "lte",
        re.compile(
            r"\b(?:at most|no more than)\s+\$?(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?",
            re.IGNORECASE,
        ),
    ),
    (
        "gt",
        re.compile(
            r"\b(?:more than|over|above)\s+\$?(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?",
            re.IGNORECASE,
        ),
    ),
    (
        "lt",
        re.compile(
            r"\b(?:less than|under|below)\s+\$?(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?",
            re.IGNORECASE,
        ),
    ),
    (
        "eq",
        re.compile(
            r"\bexactly\s+\$?(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|percent|percentage|usd|dollars?|points?|votes?|seats?)?",
            re.IGNORECASE,
        ),
    ),
)
TIMEFRAME_PATTERN = re.compile(
    rf"\b(?P<relation>by|before|after|on|during)\s+(?P<value>\d{{4}}-\d{{2}}-\d{{2}}|(?:{MONTH_PATTERN})\s+\d{{1,2}},\s+\d{{4}}|(?:{MONTH_PATTERN})\s+\d{{4}}|\d{{4}})\b",
    re.IGNORECASE,
)
YEAR_END_PATTERN = re.compile(r"\bby end of (?P<year>\d{4})\b", re.IGNORECASE)
ISO_DATE_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
MONTH_DAY_YEAR_PATTERN = re.compile(
    rf"\b(?P<month>{MONTH_PATTERN})\s+(?P<day>\d{{1,2}}),\s+(?P<year>\d{{4}})\b",
    re.IGNORECASE,
)
MONTH_YEAR_PATTERN = re.compile(rf"\b(?P<month>{MONTH_PATTERN})\s+(?P<year>\d{{4}})\b", re.IGNORECASE)
YEAR_PATTERN = re.compile(r"\b(20\d{2})\b")
WORD_PATTERN = re.compile(r"\b[A-Za-z][A-Za-z'.-]*\b")


@dataclass(frozen=True, slots=True)
class SemanticMarketInput:
    market_id: int
    question: str


def normalize_semantic_markets(markets: list[SemanticMarketInput]) -> dict[str, dict[str, Any]]:
    return {str(market.market_id): normalize_market_semantics(market) for market in markets}


def normalize_market_semantics(market: SemanticMarketInput) -> dict[str, Any]:
    question = " ".join(market.question.split())
    dates = _extract_dates(question)
    thresholds = _extract_thresholds(question)
    entities = _extract_entities(question)
    polarity = _extract_polarity(question)
    timeframes = _extract_timeframes(question, dates)
    units = _extract_units(question, thresholds)
    semantic_template = _build_semantic_template(question, entities, dates, thresholds)

    return {
        "market_id": market.market_id,
        "question": question,
        "semantic_template": semantic_template,
        "entities": entities,
        "dates": dates,
        "thresholds": thresholds,
        "units": units,
        "polarity": polarity,
        "timeframes": timeframes,
    }


def _extract_entities(question: str) -> list[dict[str, Any]]:
    tokens = list(WORD_PATTERN.finditer(question))
    entities: list[dict[str, Any]] = []
    slot_index = 0
    index = 0
    while index < len(tokens):
        token = tokens[index]
        token_text = token.group(0)
        if not token_text[0].isupper() or token_text in CAPITALIZED_TOKEN_EXCLUSIONS or token_text.lower() in MONTHS:
            index += 1
            continue

        span_start = token.start()
        span_end = token.end()
        parts = [token_text]
        lookahead = index + 1
        while lookahead < len(tokens):
            next_token = tokens[lookahead]
            next_text = next_token.group(0)
            if not next_text[0].isupper() or next_text in CAPITALIZED_TOKEN_EXCLUSIONS or next_text.lower() in MONTHS:
                break
            parts.append(next_text)
            span_end = next_token.end()
            lookahead += 1

        entity_text = " ".join(parts)
        prefix = question[:span_start].rstrip().split(" ")[-1].lower() if question[:span_start].strip() else ""
        entity_kind = _classify_entity(entity_text, prefix)
        entities.append(
            {
                "slot": slot_index,
                "role": "subject_candidate" if slot_index == 0 else "context_candidate",
                "kind": entity_kind,
                "text": entity_text,
                "canonical": _normalize_text(entity_text),
                "span": [span_start, span_end],
            }
        )
        slot_index += 1
        index = lookahead
    return entities


def _classify_entity(entity_text: str, prefix: str) -> str:
    lowered = _normalize_text(entity_text)
    tail = lowered.split(" ")[-1]
    if prefix in LOCATION_PREPOSITIONS:
        return "location"
    if tail in ORGANIZATION_SUFFIXES:
        return "organization"
    if len(entity_text.split(" ")) <= 2:
        return "person"
    return "organization"


def _extract_dates(question: str) -> list[dict[str, Any]]:
    dates: list[dict[str, Any]] = []
    used_spans: list[tuple[int, int]] = []
    for match in YEAR_END_PATTERN.finditer(question):
        year = match.group("year")
        dates.append(
            {
                "text": match.group(0),
                "canonical": f"{year}-12-31",
                "granularity": "day",
                "kind": "year_end_deadline",
                "span": [match.start(), match.end()],
            }
        )
        used_spans.append((match.start(), match.end()))

    for match in ISO_DATE_PATTERN.finditer(question):
        if _span_is_used(match.span(), used_spans):
            continue
        dates.append(
            {
                "text": match.group(0),
                "canonical": match.group(0),
                "granularity": "day",
                "kind": "absolute_date",
                "span": [match.start(), match.end()],
            }
        )
        used_spans.append((match.start(), match.end()))

    for match in MONTH_DAY_YEAR_PATTERN.finditer(question):
        if _span_is_used(match.span(), used_spans):
            continue
        month = MONTHS[match.group("month").lower()]
        day = int(match.group("day"))
        year = int(match.group("year"))
        dates.append(
            {
                "text": match.group(0),
                "canonical": f"{year:04d}-{month:02d}-{day:02d}",
                "granularity": "day",
                "kind": "absolute_date",
                "span": [match.start(), match.end()],
            }
        )
        used_spans.append((match.start(), match.end()))

    for match in MONTH_YEAR_PATTERN.finditer(question):
        if _span_is_used(match.span(), used_spans):
            continue
        month = MONTHS[match.group("month").lower()]
        year = int(match.group("year"))
        dates.append(
            {
                "text": match.group(0),
                "canonical": f"{year:04d}-{month:02d}",
                "granularity": "month",
                "kind": "absolute_month",
                "span": [match.start(), match.end()],
            }
        )
        used_spans.append((match.start(), match.end()))

    for match in YEAR_PATTERN.finditer(question):
        if _span_is_used(match.span(), used_spans):
            continue
        dates.append(
            {
                "text": match.group(1),
                "canonical": match.group(1),
                "granularity": "year",
                "kind": "absolute_year",
                "span": [match.start(), match.end()],
            }
        )
        used_spans.append((match.start(), match.end()))

    return sorted(dates, key=lambda item: item["span"][0])


def _extract_thresholds(question: str) -> list[dict[str, Any]]:
    thresholds: list[dict[str, Any]] = []
    for comparator, pattern in THRESHOLD_PATTERNS:
        for match in pattern.finditer(question):
            if comparator == "range":
                unit = _normalize_unit(match.group("unit1") or match.group("unit2"))
                thresholds.append(
                    {
                        "comparator": comparator,
                        "low": _normalize_numeric(match.group("low")),
                        "high": _normalize_numeric(match.group("high")),
                        "unit": unit,
                        "text": match.group(0),
                        "span": [match.start(), match.end()],
                    }
                )
                continue
            thresholds.append(
                {
                    "comparator": comparator,
                    "value": _normalize_numeric(match.group("value")),
                    "unit": _normalize_unit(match.group("unit")),
                    "text": match.group(0),
                    "span": [match.start(), match.end()],
                }
            )
    return sorted(thresholds, key=lambda item: item["span"][0])


def _extract_units(question: str, thresholds: list[dict[str, Any]]) -> list[str]:
    units = {threshold["unit"] for threshold in thresholds if threshold.get("unit")}
    lowered = question.lower()
    if "%" in question or "percent" in lowered or "percentage" in lowered:
        units.add("percentage")
    if "$" in question or "usd" in lowered or "dollar" in lowered:
        units.add("currency_usd")
    if "seat" in lowered:
        units.add("count_seats")
    if "vote" in lowered:
        units.add("count_votes")
    if "point" in lowered:
        units.add("count_points")
    return sorted(units)


def _extract_polarity(question: str) -> dict[str, Any]:
    negated = bool(NEGATION_PATTERN.search(question))
    direction_tokens: list[str] = []
    direction = "neutral"
    positive_match = POSITIVE_DIRECTION_PATTERN.search(question)
    negative_match = NEGATIVE_DIRECTION_PATTERN.search(question)
    if positive_match and not negative_match:
        direction = "positive"
        direction_tokens.append(positive_match.group(0).lower())
    elif negative_match and not positive_match:
        direction = "negative"
        direction_tokens.append(negative_match.group(0).lower())
    elif positive_match and negative_match:
        direction = "mixed"
        direction_tokens.extend([positive_match.group(0).lower(), negative_match.group(0).lower()])

    return {
        "negated": negated,
        "direction": direction,
        "tokens": direction_tokens,
    }


def _extract_timeframes(question: str, dates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timeframes: list[dict[str, Any]] = []
    canonical_by_text = {date["text"].lower(): date["canonical"] for date in dates}
    for match in TIMEFRAME_PATTERN.finditer(question):
        timeframes.append(
            {
                "relation": match.group("relation").lower(),
                "text": match.group(0),
                "value_text": match.group("value"),
                "canonical": canonical_by_text.get(match.group("value").lower(), match.group("value")),
            }
        )
    for match in YEAR_END_PATTERN.finditer(question):
        timeframes.append(
            {
                "relation": "by",
                "text": match.group(0),
                "value_text": match.group("year"),
                "canonical": f"{match.group('year')}-12-31",
            }
        )
    return timeframes


def _build_semantic_template(
    question: str,
    entities: list[dict[str, Any]],
    dates: list[dict[str, Any]],
    thresholds: list[dict[str, Any]],
) -> str:
    replacements: list[tuple[int, int, str]] = []
    for entity in entities:
        replacements.append((entity["span"][0], entity["span"][1], f"<entity_{entity['slot']}>"))
    for index, date in enumerate(dates):
        replacements.append((date["span"][0], date["span"][1], f"<date_{index}>"))
    for index, threshold in enumerate(thresholds):
        replacements.append((threshold["span"][0], threshold["span"][1], f"<threshold_{index}>"))

    parts: list[str] = []
    cursor = 0
    for start, end, placeholder in sorted(replacements, key=lambda item: item[0]):
        if start < cursor:
            continue
        parts.append(question[cursor:start].lower())
        parts.append(placeholder)
        cursor = end
    parts.append(question[cursor:].lower())
    return " ".join("".join(parts).split())


def _normalize_numeric(value: str) -> str:
    if "." in value:
        return value.rstrip("0").rstrip(".")
    return value


def _normalize_unit(value: str | None) -> str | None:
    if value is None:
        return None
    lowered = value.lower()
    if lowered in {"%", "percent", "percentage"}:
        return "percentage"
    if lowered in {"usd", "dollar", "dollars"}:
        return "currency_usd"
    if lowered in {"seat", "seats"}:
        return "count_seats"
    if lowered in {"vote", "votes"}:
        return "count_votes"
    if lowered in {"point", "points"}:
        return "count_points"
    return lowered


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())


def _span_is_used(span: tuple[int, int], used_spans: list[tuple[int, int]]) -> bool:
    start, end = span
    return any(start < used_end and end > used_start for used_start, used_end in used_spans)
