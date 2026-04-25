from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.db.models import Market, MarketSnapshot
from apps.worker.integrations.polymarket import MarketRecord, OrderBookLevel, OrderBookSnapshot, PollResult


DECIMAL_PLACES = Decimal("0.0001")


@dataclass(slots=True)
class PersistedSnapshot:
    market_id: int
    polymarket_market_id: str
    snapshot_id: int
    captured_at: datetime
    best_bid: Decimal | None
    best_ask: Decimal | None
    bid_depth_usd: Decimal | None
    ask_depth_usd: Decimal | None


def persist_poll_result(session: Session, poll_result: PollResult) -> list[PersistedSnapshot]:
    persisted: list[PersistedSnapshot] = []

    for market_record in poll_result.sampled_markets:
        order_books = poll_result.sampled_order_books.get(market_record.market_id, [])
        if not order_books:
            continue

        market = _get_or_create_market(session, market_record)
        snapshot = _build_market_snapshot(market.id, market_record, order_books)
        snapshot.captured_at = _next_available_captured_at(session, market.id, snapshot.captured_at)
        session.add(snapshot)
        session.flush()

        persisted.append(
            PersistedSnapshot(
                market_id=market.id,
                polymarket_market_id=market.polymarket_market_id,
                snapshot_id=snapshot.id,
                captured_at=snapshot.captured_at,
                best_bid=snapshot.best_bid,
                best_ask=snapshot.best_ask,
                bid_depth_usd=snapshot.bid_depth_usd,
                ask_depth_usd=snapshot.ask_depth_usd,
            )
        )

    session.commit()
    return persisted


def _get_or_create_market(session: Session, market_record: MarketRecord) -> Market:
    market = session.scalar(
        select(Market).where(Market.polymarket_market_id == market_record.market_id)
    )
    if market is None:
        market = Market(
            polymarket_market_id=market_record.market_id,
            question=market_record.question,
            slug=market_record.slug,
            condition_id=market_record.condition_id,
            event_id=market_record.event_id,
            event_slug=market_record.event_slug,
            raw_market_json=market_record.raw,
            neg_risk=market_record.neg_risk,
        )
        session.add(market)
        session.flush()
        return market

    market.question = market_record.question
    market.slug = market_record.slug
    market.condition_id = market_record.condition_id
    market.event_id = market_record.event_id
    market.event_slug = market_record.event_slug
    market.raw_market_json = market_record.raw
    market.neg_risk = market_record.neg_risk
    market.status = "active"
    return market


def _build_market_snapshot(
    market_id: int,
    market_record: MarketRecord,
    order_books: list[OrderBookSnapshot],
) -> MarketSnapshot:
    order_book_json = _build_order_book_json(market_record, order_books)
    pricing_books = _pricing_books(order_book_json)
    best_bid = _best_price(pricing_books, side="bids", fn=max)
    best_ask = _best_price(pricing_books, side="asks", fn=min)
    bid_depth_usd = _sum_depth_usd(pricing_books, side="bids")
    ask_depth_usd = _sum_depth_usd(pricing_books, side="asks")
    captured_at = _captured_at(order_books)

    return MarketSnapshot(
        market_id=market_id,
        captured_at=captured_at,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_depth_usd=bid_depth_usd,
        ask_depth_usd=ask_depth_usd,
        order_book_json=order_book_json,
    )


def _best_price(
    order_books: list[dict[str, object]],
    *,
    side: str,
    fn: Callable[[list[Decimal]], Decimal],
) -> Decimal | None:
    prices: list[Decimal] = []
    for order_book in order_books:
        levels = order_book.get(side, [])
        if not isinstance(levels, list):
            continue
        if not levels:
            continue
        first_level = levels[0]
        if not isinstance(first_level, dict):
            continue
        parsed = _to_decimal(str(first_level.get("price") or ""))
        if parsed is not None:
            prices.append(parsed)

    return _quantize(fn(prices)) if prices else None


def _sum_depth_usd(order_books: list[dict[str, object]], *, side: str) -> Decimal | None:
    total = Decimal("0")
    found_level = False

    for order_book in order_books:
        levels = order_book.get(side, [])
        if not isinstance(levels, list):
            continue
        for level in levels:
            if not isinstance(level, dict):
                continue
            price = _to_decimal(str(level.get("price") or ""))
            size = _to_decimal(str(level.get("size") or ""))
            if price is None or size is None:
                continue
            total += price * size
            found_level = True

    return _quantize(total) if found_level else None


def _build_order_book_json(
    market_record: MarketRecord,
    order_books: list[OrderBookSnapshot],
) -> dict[str, object]:
    tokens: list[dict[str, object]] = []
    pricing_outcome: str | None = None

    for token, snapshot in zip(market_record.tokens, order_books, strict=False):
        outcome = token.outcome or ""
        normalized_book = {
            "outcome": outcome,
            "token_id": token.token_id,
            "market_id": snapshot.market_id,
            "timestamp": snapshot.timestamp,
            "bids": _normalize_levels(snapshot.bids),
            "asks": _normalize_levels(snapshot.asks),
        }
        tokens.append(normalized_book)
        if pricing_outcome is None and outcome.strip().lower() == "yes":
            pricing_outcome = outcome

    return {
        "pricing_outcome": pricing_outcome,
        "tokens": tokens,
    }


def _pricing_books(order_book_json: dict[str, object]) -> list[dict[str, object]]:
    tokens = order_book_json.get("tokens", [])
    pricing_outcome = order_book_json.get("pricing_outcome")
    if not isinstance(tokens, list):
        return []

    if isinstance(pricing_outcome, str):
        selected = [
            token
            for token in tokens
            if isinstance(token, dict) and str(token.get("outcome") or "").strip().lower() == pricing_outcome.lower()
        ]
        if selected:
            return selected

    return [token for token in tokens if isinstance(token, dict)]


def _normalize_levels(levels: list[OrderBookLevel]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for level in levels:
        price = _to_decimal(level.price)
        size = _to_decimal(level.size)
        if price is None or size is None:
            continue
        normalized.append(
            {
                "price": format(_quantize(price), "f"),
                "size": format(_quantize(size), "f"),
            }
        )
    return normalized


def _captured_at(order_books: list[OrderBookSnapshot]) -> datetime:
    timestamps = [timestamp for order_book in order_books if (timestamp := _parse_timestamp(order_book.timestamp))]
    return max(timestamps) if timestamps else datetime.now(UTC)


def _next_available_captured_at(session: Session, market_id: int, captured_at: datetime) -> datetime:
    adjusted = captured_at
    while session.scalar(
        select(MarketSnapshot.id).where(
            MarketSnapshot.market_id == market_id,
            MarketSnapshot.captured_at == adjusted,
        )
    ):
        adjusted = adjusted.replace(microsecond=adjusted.microsecond + 1)
    return adjusted


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None

    try:
        raw_value = Decimal(value)
    except InvalidOperation:
        return None

    timestamp = float(raw_value)
    if timestamp > 10_000_000_000:
        timestamp /= 1000

    return datetime.fromtimestamp(timestamp, tz=UTC)


def _to_decimal(value: str) -> Decimal | None:
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(DECIMAL_PLACES)
