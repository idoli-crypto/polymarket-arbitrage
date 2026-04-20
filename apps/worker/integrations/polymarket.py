from __future__ import annotations

import json
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import certifi

from apps.worker.constants import (
    POLYMARKET_CLOB_BASE_URL,
    POLYMARKET_GAMMA_BASE_URL,
)


LOGGER = logging.getLogger(__name__)


class PolymarketClientError(RuntimeError):
    """Raised when a Polymarket API request fails."""


@dataclass(slots=True)
class MarketToken:
    outcome: str
    token_id: str


@dataclass(slots=True)
class MarketRecord:
    market_id: str
    question: str
    slug: str | None
    condition_id: str | None
    event_id: str | None
    event_slug: str | None
    neg_risk: bool
    tokens: list[MarketToken]
    raw: dict[str, Any]


@dataclass(slots=True)
class OrderBookLevel:
    price: str
    size: str


@dataclass(slots=True)
class OrderBookSnapshot:
    market_id: str
    token_id: str
    timestamp: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    raw: dict[str, Any]


@dataclass(slots=True)
class PollResult:
    sampled_markets: list[MarketRecord]
    sampled_order_books: dict[str, list[OrderBookSnapshot]]
    raw_markets: list[dict[str, Any]]


class PolymarketClient:
    def __init__(
        self,
        *,
        gamma_base_url: str = POLYMARKET_GAMMA_BASE_URL,
        clob_base_url: str = POLYMARKET_CLOB_BASE_URL,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self.clob_base_url = clob_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    def fetch_markets(
        self,
        *,
        limit: int = 10,
        active: bool = True,
        closed: bool = False,
    ) -> list[MarketRecord]:
        params = {
            "limit": str(limit),
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        response = self._get_json(f"{self.gamma_base_url}/markets", params=params)

        if not isinstance(response, list):
            raise PolymarketClientError("Unexpected markets response shape")

        markets: list[MarketRecord] = []
        for item in response:
            if not isinstance(item, dict):
                continue

            market_id = str(item.get("id") or "")
            question = str(item.get("question") or "")
            if not market_id or not question:
                continue

            markets.append(
                MarketRecord(
                    market_id=market_id,
                    question=question,
                    slug=self._optional_str(item.get("slug")),
                    condition_id=self._optional_str(item.get("conditionId")),
                    event_id=self._extract_event_id(item),
                    event_slug=self._extract_event_slug(item),
                    neg_risk=self._extract_neg_risk(item),
                    tokens=self._extract_tokens(item),
                    raw=item,
                )
            )

        return markets

    def fetch_order_book(self, token_id: str) -> OrderBookSnapshot:
        response = self._get_json(
            f"{self.clob_base_url}/book",
            params={"token_id": token_id},
        )

        if not isinstance(response, dict):
            raise PolymarketClientError("Unexpected order book response shape")

        return OrderBookSnapshot(
            market_id=str(response.get("market") or ""),
            token_id=str(response.get("asset_id") or token_id),
            timestamp=str(response.get("timestamp") or ""),
            bids=self._extract_levels(response.get("bids")),
            asks=self._extract_levels(response.get("asks")),
            raw=response,
        )

    def fetch_order_books_for_market(self, market: MarketRecord) -> list[OrderBookSnapshot]:
        snapshots: list[OrderBookSnapshot] = []
        for token in market.tokens:
            snapshots.append(self.fetch_order_book(token.token_id))
        return snapshots

    def poll_markets(
        self,
        *,
        market_limit: int = 5,
        market_sample_size: int = 2,
    ) -> PollResult:
        markets = self.fetch_markets(limit=market_limit)
        sampled_markets = [market for market in markets if market.tokens][:market_sample_size]
        sampled_order_books = {
            market.market_id: self.fetch_order_books_for_market(market)
            for market in sampled_markets
        }
        return PollResult(
            sampled_markets=sampled_markets,
            sampled_order_books=sampled_order_books,
            raw_markets=[market.raw for market in markets],
        )

    def run_polling_loop(
        self,
        *,
        interval_seconds: float = 5.0,
        market_limit: int = 5,
        market_sample_size: int = 2,
        iterations: int = 0,
    ) -> list[PollResult]:
        history: list[PollResult] = []
        completed_iterations = 0

        while iterations == 0 or completed_iterations < iterations:
            poll_result = self.poll_markets(
                market_limit=market_limit,
                market_sample_size=market_sample_size,
            )
            history.append(poll_result)
            self._log_poll_result(poll_result)
            completed_iterations += 1

            if iterations > 0 and completed_iterations >= iterations:
                break

            time.sleep(interval_seconds)

        return history

    def _get_json(self, url: str, *, params: dict[str, str] | None = None) -> Any:
        request_url = url if not params else f"{url}?{urlencode(params)}"
        request = Request(
            request_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "polymarket-arbitrage-worker/0.1",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds, context=self.ssl_context) as response:
                return json.load(response)
        except Exception as exc:  # pragma: no cover - network failure path
            raise PolymarketClientError(f"Request failed for {request_url}: {exc}") from exc

    def _extract_tokens(self, item: dict[str, Any]) -> list[MarketToken]:
        token_ids = item.get("clobTokenIds")
        outcomes = item.get("outcomes")

        parsed_token_ids = self._coerce_json_list(token_ids)
        parsed_outcomes = self._coerce_json_list(outcomes)

        tokens: list[MarketToken] = []
        for index, token_id in enumerate(parsed_token_ids):
            if not token_id:
                continue
            outcome = parsed_outcomes[index] if index < len(parsed_outcomes) else f"outcome_{index}"
            tokens.append(MarketToken(outcome=str(outcome), token_id=str(token_id)))
        return tokens

    def _extract_levels(self, value: Any) -> list[OrderBookLevel]:
        if not isinstance(value, list):
            return []
        levels: list[OrderBookLevel] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            levels.append(
                OrderBookLevel(
                    price=str(item.get("price") or ""),
                    size=str(item.get("size") or ""),
                )
            )
        return levels

    def _coerce_json_list(self, value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return []
            return parsed if isinstance(parsed, list) else []
        return []

    def _optional_str(self, value: Any) -> str | None:
        if value is None:
            return None
        string_value = str(value)
        return string_value or None

    def _extract_event_id(self, item: dict[str, Any]) -> str | None:
        first_event = self._first_event(item)
        if first_event is None:
            return None
        return self._optional_str(first_event.get("id"))

    def _extract_event_slug(self, item: dict[str, Any]) -> str | None:
        first_event = self._first_event(item)
        if first_event is None:
            return None
        return self._optional_str(first_event.get("slug"))

    def _extract_neg_risk(self, item: dict[str, Any]) -> bool:
        first_event = self._first_event(item)
        if first_event is not None and isinstance(first_event.get("negRisk"), bool):
            return first_event["negRisk"]
        return bool(item.get("negRisk"))

    def _first_event(self, item: dict[str, Any]) -> dict[str, Any] | None:
        events = item.get("events")
        if not isinstance(events, list):
            return None
        for event in events:
            if isinstance(event, dict):
                return event
        return None

    def _log_poll_result(self, poll_result: PollResult) -> None:
        LOGGER.info(
            "Polymarket poll completed: markets=%s sampled_markets=%s",
            len(poll_result.raw_markets),
            len(poll_result.sampled_markets),
        )
        for market in poll_result.sampled_markets:
            snapshots = poll_result.sampled_order_books.get(market.market_id, [])
            token_summaries = [
                {
                    "outcome": token.outcome,
                    "token_id": token.token_id,
                    "bids": len(snapshot.bids),
                    "asks": len(snapshot.asks),
                    "timestamp": snapshot.timestamp,
                }
                for token, snapshot in zip(market.tokens, snapshots, strict=False)
            ]
            LOGGER.info(
                "Market=%s question=%s books=%s",
                market.market_id,
                market.question,
                token_summaries,
            )
