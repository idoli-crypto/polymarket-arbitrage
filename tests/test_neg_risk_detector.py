from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from apps.worker.detectors.neg_risk import (
    DETECTOR_VERSION,
    OPPORTUNITY_TYPE,
    DetectionMarketInput,
    detect_neg_risk_candidates,
)


def _market(
    *,
    market_id: int,
    condition_id: str,
    question: str,
    best_ask: str,
    event_id: str = "event-1",
    neg_risk: bool = True,
) -> DetectionMarketInput:
    return DetectionMarketInput(
        market_id=market_id,
        polymarket_market_id=f"pm-{market_id}",
        question=question,
        slug=f"market-{market_id}",
        condition_id=condition_id,
        event_id=event_id,
        event_slug="event-1",
        neg_risk=neg_risk,
        snapshot_id=market_id * 10,
        snapshot_captured_at=datetime(2026, 4, 20, tzinfo=timezone.utc),
        best_bid=Decimal("0.1000"),
        best_ask=Decimal(best_ask),
    )


class NegRiskDetectorTests(unittest.TestCase):
    def test_detects_long_bundle_when_sum_of_latest_best_asks_is_below_one(self) -> None:
        candidates = detect_neg_risk_candidates(
            [
                _market(
                    market_id=1,
                    condition_id="condition-1",
                    question="Will Alice win?",
                    best_ask="0.3000",
                ),
                _market(
                    market_id=2,
                    condition_id="condition-2",
                    question="Will Bob win?",
                    best_ask="0.2500",
                ),
                _market(
                    market_id=3,
                    condition_id="condition-3",
                    question="Will Carol win?",
                    best_ask="0.2000",
                ),
            ]
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.event_group_key, "event-1")
        self.assertEqual(candidate.opportunity_type, OPPORTUNITY_TYPE)
        self.assertEqual(candidate.outcome_count, 3)
        self.assertEqual(candidate.gross_price_sum, Decimal("0.7500"))
        self.assertEqual(candidate.gross_gap, Decimal("0.2500"))
        self.assertEqual(candidate.detector_version, DETECTOR_VERSION)

    def test_skips_groups_without_structurally_complete_neg_risk_metadata(self) -> None:
        candidates = detect_neg_risk_candidates(
            [
                _market(
                    market_id=1,
                    condition_id="condition-1",
                    question="Will Alice win?",
                    best_ask="0.3000",
                ),
                _market(
                    market_id=2,
                    condition_id="condition-2",
                    question="Will Bob win?",
                    best_ask="0.2500",
                    neg_risk=False,
                ),
            ]
        )
        self.assertEqual(candidates, [])

    def test_filters_out_non_neg_risk_markets_before_grouping(self) -> None:
        candidates = detect_neg_risk_candidates(
            [
                _market(
                    market_id=1,
                    condition_id="condition-1",
                    question="Will Alice win?",
                    best_ask="0.3000",
                    event_id="event-neg",
                    neg_risk=True,
                ),
                _market(
                    market_id=2,
                    condition_id="condition-2",
                    question="Will Bob win?",
                    best_ask="0.2500",
                    event_id="event-neg",
                    neg_risk=True,
                ),
                _market(
                    market_id=3,
                    condition_id="condition-3",
                    question="Will Carol win?",
                    best_ask="0.0500",
                    event_id="event-non-neg",
                    neg_risk=False,
                ),
            ]
        )

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.event_group_key, "event-neg")
        self.assertEqual(candidate.gross_price_sum, Decimal("0.5500"))
        self.assertEqual(candidate.gross_gap, Decimal("0.4500"))

    def test_uses_latest_snapshot_minute_as_detection_window(self) -> None:
        candidates = detect_neg_risk_candidates(
            [
                DetectionMarketInput(
                    market_id=1,
                    polymarket_market_id="pm-1",
                    question="Will Alice win?",
                    slug="market-1",
                    condition_id="condition-1",
                    event_id="event-1",
                    event_slug="event-1",
                    neg_risk=True,
                    snapshot_id=10,
                    snapshot_captured_at=datetime(2026, 4, 20, 12, 34, 10, tzinfo=timezone.utc),
                    best_bid=Decimal("0.1000"),
                    best_ask=Decimal("0.3000"),
                ),
                DetectionMarketInput(
                    market_id=2,
                    polymarket_market_id="pm-2",
                    question="Will Bob win?",
                    slug="market-2",
                    condition_id="condition-2",
                    event_id="event-1",
                    event_slug="event-1",
                    neg_risk=True,
                    snapshot_id=20,
                    snapshot_captured_at=datetime(2026, 4, 20, 12, 34, 59, tzinfo=timezone.utc),
                    best_bid=Decimal("0.1000"),
                    best_ask=Decimal("0.2500"),
                ),
            ]
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(
            candidates[0].detection_window_start,
            datetime(2026, 4, 20, 12, 34, 0, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
