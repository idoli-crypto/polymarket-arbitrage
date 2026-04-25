from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import DetectedOpportunity, Market, MarketSnapshot
from apps.api.services.opportunity_classification import (
    CLASSIFICATION_VERSION,
    DetectionFamily,
)
from apps.worker.neg_risk_detection import scan_and_persist_neg_risk_candidates


class NegRiskDetectionPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "neg-risk-detection.db"
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

    def test_scan_and_persist_neg_risk_candidates_persists_canonical_family(self) -> None:
        with self.SessionLocal() as session:
            self._add_market(
                session,
                market_id="pm-1",
                question="Will Alice win?",
                condition_id="condition-1",
                best_ask=Decimal("0.3000"),
            )
            self._add_market(
                session,
                market_id="pm-2",
                question="Will Bob win?",
                condition_id="condition-2",
                best_ask=Decimal("0.2500"),
            )
            session.commit()

            persisted = scan_and_persist_neg_risk_candidates(session)

            self.assertEqual(len(persisted), 1)
            row = session.scalar(select(DetectedOpportunity).where(DetectedOpportunity.id == persisted[0].id))

        assert row is not None
        self.assertEqual(row.family, DetectionFamily.NEG_RISK_CONVERSION.value)
        self.assertIsNone(row.relation_type)
        self.assertIsNone(row.relation_direction)
        self.assertEqual(row.detector_version, "neg_risk_v1")
        self.assertEqual(row.raw_context["classification"]["version"], CLASSIFICATION_VERSION)
        self.assertEqual(
            row.raw_context["classification"]["family"],
            DetectionFamily.NEG_RISK_CONVERSION.value,
        )
        self.assertEqual(
            row.raw_context["classification"]["inputs"]["opportunity_type"],
            "neg_risk_long_yes_bundle",
        )

    def _add_market(
        self,
        session,
        *,
        market_id: str,
        question: str,
        condition_id: str,
        best_ask: Decimal,
    ) -> None:
        market = Market(
            polymarket_market_id=market_id,
            question=question,
            slug=market_id,
            condition_id=condition_id,
            event_id="event-1",
            event_slug="event-1",
            neg_risk=True,
            status="active",
        )
        session.add(market)
        session.flush()
        session.add(
            MarketSnapshot(
                market_id=market.id,
                best_bid=Decimal("0.1000"),
                best_ask=best_ask,
                bid_depth_usd=Decimal("10.0000"),
                ask_depth_usd=Decimal("10.0000"),
                captured_at=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
            )
        )


if __name__ == "__main__":
    unittest.main()
