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
        snapshot = _build_market_snapshot(market.id, order_books)
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
        )
        session.add(market)
        session.flush()
        return market

    market.question = market_record.question
    market.slug = market_record.slug
    market.status = "active"
    return market


def _build_market_snapshot(market_id: int, order_books: list[OrderBookSnapshot]) -> MarketSnapshot:
    best_bid = _best_price(order_books, side="bids", fn=max)
    best_ask = _best_price(order_books, side="asks", fn=min)
    bid_depth_usd = _sum_depth_usd(order_books, side="bids")
    ask_depth_usd = _sum_depth_usd(order_books, side="asks")
    captured_at = _captured_at(order_books)

    return MarketSnapshot(
        market_id=market_id,
        captured_at=captured_at,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_depth_usd=bid_depth_usd,
        ask_depth_usd=ask_depth_usd,
    )


def _best_price(
    order_books: list[OrderBookSnapshot],
    *,
    side: str,
    fn: Callable[[list[Decimal]], Decimal],
) -> Decimal | None:
    prices: list[Decimal] = []
    for order_book in order_books:
        levels = getattr(order_book, side)
        if not levels:
            continue
        parsed = _to_decimal(levels[0].price)
        if parsed is not None:
            prices.append(parsed)

    return _quantize(fn(prices)) if prices else None


def _sum_depth_usd(order_books: list[OrderBookSnapshot], *, side: str) -> Decimal | None:
    total = Decimal("0")
    found_level = False

    for order_book in order_books:
        levels: list[OrderBookLevel] = getattr(order_book, side)
        for level in levels:
            price = _to_decimal(level.price)
            size = _to_decimal(level.size)
            if price is None or size is None:
                continue
            total += price * size
            found_level = True

    return _quantize(total) if found_level else None


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
