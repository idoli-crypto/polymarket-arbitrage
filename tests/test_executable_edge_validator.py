from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from apps.worker.validators.executable_edge import (
    STATUS_INCONCLUSIVE,
    STATUS_INVALID,
    STATUS_VALID,
    ExecutableEdgeValidationInput,
    evaluate_execution_at_size,
    parse_executable_market_snapshot,
    validate_executable_edge,
)


REFERENCE_TIME = datetime(2026, 4, 24, 12, 0, 30, tzinfo=timezone.utc)


def _snapshot(
    *,
    market_id: int,
    asks: list[tuple[str, str]],
    taker_fee_bps: int = 40,
    min_order_size: str = "1.0000",
    captured_at: datetime | None = None,
) -> object:
    return parse_executable_market_snapshot(
        market_id=market_id,
        snapshot_id=market_id * 10,
        captured_at=captured_at or datetime(2026, 4, 24, 12, 0, 10 + market_id, tzinfo=timezone.utc),
        order_book_json={
            "pricing_outcome": "Yes",
            "tokens": [
                {
                    "outcome": "Yes",
                    "token_id": f"token-{market_id}",
                    "asks": [{"price": price, "size": size} for price, size in asks],
                    "bids": [],
                }
            ],
        },
        raw_market_json={
            "takerBaseFee": taker_fee_bps,
            "orderMinSize": min_order_size,
        },
    )


class ExecutableEdgeValidatorTests(unittest.TestCase):
    def test_computes_simple_top_of_book_execution(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")]),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.top_of_book_edge, Decimal("0.4500"))
        self.assertEqual(result.depth_weighted_edge, Decimal("0.4500"))
        self.assertEqual(result.min_executable_size, Decimal("1.0000"))
        self.assertEqual(result.suggested_notional_bucket, "100-250")

    def test_consumes_multiple_levels_for_depth_weighted_edge(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(
                    market_id=1,
                    asks=[("0.3000", "50.0000"), ("0.4000", "50.0000")],
                ),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.top_of_book_edge, Decimal("0.4500"))
        self.assertEqual(result.depth_weighted_edge, Decimal("0.4000"))
        self.assertEqual(result.execution_size, Decimal("100.0000"))
        self.assertEqual(result.slippage_cost_usd, Decimal("5.0000"))

    def test_slippage_increases_with_size(self) -> None:
        snapshots = [
            _snapshot(market_id=1, asks=[("0.3000", "50.0000"), ("0.4500", "50.0000")]),
            _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
        ]

        small = evaluate_execution_at_size(size=Decimal("25.0000"), market_snapshots=snapshots)
        large = evaluate_execution_at_size(size=Decimal("75.0000"), market_snapshots=snapshots)

        assert small is not None
        assert large is not None
        self.assertEqual(small.slippage_cost_usd, Decimal("0.0000"))
        self.assertEqual(large.slippage_cost_usd, Decimal("3.7500"))
        self.assertGreater(large.slippage_cost_usd, small.slippage_cost_usd)

    def test_applies_taker_fees_to_fee_adjusted_edge(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(market_id=1, asks=[("0.3000", "10.0000")], taker_fee_bps=40),
                2: _snapshot(market_id=2, asks=[("0.2500", "10.0000")], taker_fee_bps=40),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.depth_weighted_edge, Decimal("0.4500"))
        self.assertEqual(result.fee_cost_usd, Decimal("0.0159"))
        self.assertEqual(result.fee_adjusted_edge, Decimal("0.4484"))

    def test_rejects_when_edge_disappears_after_depth(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(
                    market_id=1,
                    asks=[("0.3000", "10.0000"), ("0.8000", "90.0000")],
                ),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.min_executable_size, Decimal("1.0000"))
        self.assertEqual(result.execution_size, Decimal("10.0000"))
        self.assertEqual(result.depth_weighted_edge, Decimal("0.4500"))
        self.assertEqual(result.suggested_notional_bucket, "10-25")

    def test_rejects_when_fee_adjusted_edge_is_never_positive(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(market_id=1, asks=[("0.6000", "100.0000")]),
                2: _snapshot(market_id=2, asks=[("0.4500", "100.0000")]),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.reason_code, "non_positive_fee_adjusted_edge")
        self.assertIsNone(result.min_executable_size)
        self.assertIsNone(result.suggested_notional_bucket)

    def test_returns_inconclusive_for_unsupported_payout_contract(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="timeframe_nesting",
                opportunity_type="timeframe_pair",
            ),
            market_snapshots={
                1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")]),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_INCONCLUSIVE)
        self.assertEqual(result.reason_code, "unsupported_payout_contract")
        self.assertEqual(result.details["supported_contract_type"], "neg_risk_long_yes_bundle")
        self.assertEqual(result.details["unsupported_payout_contract"], "timeframe_pair")

    def test_returns_inconclusive_for_stale_order_book_snapshot(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(
                    market_id=1,
                    asks=[("0.3000", "100.0000")],
                    captured_at=datetime(2026, 4, 24, 11, 59, 0, tzinfo=timezone.utc),
                ),
                2: _snapshot(
                    market_id=2,
                    asks=[("0.2500", "100.0000")],
                    captured_at=datetime(2026, 4, 24, 12, 0, 10, tzinfo=timezone.utc),
                ),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_INCONCLUSIVE)
        self.assertEqual(result.reason_code, "stale_order_book_snapshot")
        self.assertEqual(result.details["stale_market_ids"], [1])
        self.assertEqual(result.details["markets"][0]["snapshot_age_seconds"], "90.0000")

    def test_rejects_when_fee_metadata_is_missing(self) -> None:
        result = validate_executable_edge(
            ExecutableEdgeValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            market_snapshots={
                1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")], taker_fee_bps=40),
                2: parse_executable_market_snapshot(
                    market_id=2,
                    snapshot_id=20,
                    captured_at=datetime(2026, 4, 24, 12, 0, 15, tzinfo=timezone.utc),
                    order_book_json={
                        "pricing_outcome": "Yes",
                        "tokens": [{"outcome": "Yes", "token_id": "token-2", "asks": [{"price": "0.2500", "size": "100.0000"}], "bids": []}],
                    },
                    raw_market_json={"orderMinSize": "1.0000"},
                ),
            },
            reference_time=REFERENCE_TIME,
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.reason_code, "missing_taker_fee_rate")
        self.assertEqual(result.details["fee_source"], "persisted_market_metadata_only")


if __name__ == "__main__":
    unittest.main()
