from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import Market, MarketSnapshot
from apps.worker.integrations.polymarket import MarketRecord, MarketToken, OrderBookLevel, OrderBookSnapshot, PollResult
from apps.worker.persistence import persist_poll_result


class WorkerPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "worker-persistence.db"
        self.engine = create_engine(f"sqlite:///{database_path}")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temp_dir.cleanup()

    def test_persist_poll_result_stores_raw_market_json_and_normalized_yes_order_book(self) -> None:
        raw_market = {
            "id": "pm-1",
            "question": "Will Alice win?",
            "resolutionSource": "official-results",
            "endDate": "2026-12-31T00:00:00Z",
            "description": "Use the official certified result.",
            "takerBaseFee": 40,
        }
        poll_result = PollResult(
            sampled_markets=[
                MarketRecord(
                    market_id="pm-1",
                    question="Will Alice win?",
                    slug="alice-win",
                    condition_id="condition-1",
                    event_id="event-1",
                    event_slug="event-1",
                    neg_risk=True,
                    tokens=[
                        MarketToken(outcome="Yes", token_id="token-yes"),
                        MarketToken(outcome="No", token_id="token-no"),
                    ],
                    raw=raw_market,
                )
            ],
            sampled_order_books={
                "pm-1": [
                    OrderBookSnapshot(
                        market_id="pm-1",
                        token_id="token-yes",
                        timestamp=str(int(datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc).timestamp())),
                        bids=[OrderBookLevel(price="0.1000", size="100.0")],
                        asks=[OrderBookLevel(price="0.2000", size="100.0")],
                        raw={},
                    ),
                    OrderBookSnapshot(
                        market_id="pm-1",
                        token_id="token-no",
                        timestamp=str(int(datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc).timestamp())),
                        bids=[OrderBookLevel(price="0.7000", size="30.0")],
                        asks=[OrderBookLevel(price="0.8000", size="50.0")],
                        raw={},
                    )
                ]
            },
            raw_markets=[raw_market],
        )

        with self.SessionLocal() as session:
            persisted = persist_poll_result(session, poll_result)
            self.assertEqual(len(persisted), 1)

            market = session.scalar(select(Market).where(Market.polymarket_market_id == "pm-1"))
            assert market is not None
            snapshot = session.scalar(select(MarketSnapshot).where(MarketSnapshot.market_id == market.id))

        assert snapshot is not None
        self.assertEqual(market.question, "Will Alice win?")
        self.assertEqual(market.raw_market_json["resolutionSource"], "official-results")
        self.assertEqual(market.raw_market_json["endDate"], "2026-12-31T00:00:00Z")
        self.assertEqual(snapshot.best_ask, Decimal("0.2000"))
        self.assertEqual(snapshot.ask_depth_usd, Decimal("20.0000"))
        self.assertEqual(snapshot.order_book_json["pricing_outcome"], "Yes")
        self.assertEqual(len(snapshot.order_book_json["tokens"]), 2)
        self.assertEqual(snapshot.order_book_json["tokens"][0]["asks"][0]["size"], "100.0000")


if __name__ == "__main__":
    unittest.main()
