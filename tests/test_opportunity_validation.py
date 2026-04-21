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
from apps.worker.opportunity_validation import validate_pending_opportunities


class OpportunityValidationIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "validation-test.db"
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

    def test_validate_pending_opportunities_updates_db_fields_and_context(self) -> None:
        with self.SessionLocal() as session:
            market_a = Market(
                polymarket_market_id="pm-1",
                question="Will Alice win?",
                slug="alice-win",
                condition_id="condition-1",
                event_id="event-1",
                event_slug="event-1",
                neg_risk=True,
            )
            market_b = Market(
                polymarket_market_id="pm-2",
                question="Will Bob win?",
                slug="bob-win",
                condition_id="condition-2",
                event_id="event-1",
                event_slug="event-1",
                neg_risk=True,
            )
            market_c = Market(
                polymarket_market_id="pm-3",
                question="Will Carol win?",
                slug="carol-win",
                condition_id="condition-3",
                event_id="event-2",
                event_slug="event-2",
                neg_risk=True,
            )
            market_d = Market(
                polymarket_market_id="pm-4",
                question="Will Dave win?",
                slug="dave-win",
                condition_id="condition-4",
                event_id="event-2",
                event_slug="event-2",
                neg_risk=True,
            )
            session.add_all([market_a, market_b, market_c, market_d])
            session.flush()

            session.add_all(
                [
                    MarketSnapshot(
                        market_id=market_a.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.3000"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("20.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_b.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.2500"),
                        bid_depth_usd=Decimal("11.0000"),
                        ask_depth_usd=Decimal("21.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 1, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_d.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.2200"),
                        bid_depth_usd=Decimal("12.0000"),
                        ask_depth_usd=Decimal("22.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 2, tzinfo=timezone.utc),
                    ),
                ]
            )

            valid_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 1, tzinfo=timezone.utc),
                event_group_key="event-1",
                involved_market_ids=[market_a.id, market_b.id],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.5500"),
                gross_gap=Decimal("0.4500"),
                detector_version="neg_risk_v1",
                status="detected",
                raw_context={"semantic_validation_status": "pending"},
            )
            missing_snapshot_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 2, tzinfo=timezone.utc),
                event_group_key="event-2",
                involved_market_ids=[market_c.id, market_d.id],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.5000"),
                gross_gap=Decimal("0.5000"),
                detector_version="neg_risk_v1",
                status="detected",
                raw_context={"semantic_validation_status": "pending"},
            )
            session.add_all([valid_opportunity, missing_snapshot_opportunity])
            session.commit()

            before_rows = session.scalars(
                select(DetectedOpportunity).order_by(DetectedOpportunity.id.asc())
            ).all()
            self.assertEqual(
                [
                    (row.id, row.validation_status, row.validation_reason, row.validated_at)
                    for row in before_rows
                ],
                [
                    (valid_opportunity.id, None, None, None),
                    (missing_snapshot_opportunity.id, None, None, None),
                ],
            )

            results = validate_pending_opportunities(session)
            self.assertEqual(len(results), 2)

            after_rows = session.scalars(
                select(DetectedOpportunity).order_by(DetectedOpportunity.id.asc())
            ).all()

            self.assertEqual(after_rows[0].status, "detected")
            self.assertEqual(after_rows[0].validation_status, "valid")
            self.assertIsNone(after_rows[0].validation_reason)
            self.assertIsNotNone(after_rows[0].validated_at)
            self.assertEqual(after_rows[0].raw_context["semantic_validation_status"], "valid")
            self.assertEqual(after_rows[0].raw_context["rules_validation_status"], "pending")
            self.assertEqual(after_rows[0].raw_context["snapshot_freshness_status"], "pending")
            self.assertEqual(
                after_rows[0].raw_context["validation_evidence"]["selected_snapshot_ids"],
                {str(market_a.id): 1, str(market_b.id): 2},
            )

            self.assertEqual(after_rows[1].status, "detected")
            self.assertEqual(after_rows[1].validation_status, "rejected")
            self.assertEqual(after_rows[1].validation_reason, "missing_snapshot")
            self.assertIsNotNone(after_rows[1].validated_at)
            self.assertEqual(
                after_rows[1].raw_context["semantic_validation_reason"],
                "missing_snapshot",
            )
            self.assertEqual(
                after_rows[1].raw_context["validation_evidence"]["missing_snapshot_market_ids"],
                [market_c.id],
            )


if __name__ == "__main__":
    unittest.main()
