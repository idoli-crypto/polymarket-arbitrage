from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from apps.worker.simulators.execution import (
    SIMULATION_REASON_INSUFFICIENT_DEPTH,
    SIMULATION_REASON_INSUFFICIENT_EDGE,
    SIMULATION_REASON_MISSING_ORDER_BOOK,
    SIMULATION_REASON_MISSING_SNAPSHOT,
    SIMULATION_STATUS_EXECUTABLE,
    SIMULATION_STATUS_PARTIALLY_EXECUTABLE,
    SIMULATION_STATUS_REJECTED,
    SimulationOpportunityInput,
    SimulationSnapshotInput,
    simulate_validated_opportunity,
)


def _opportunity() -> SimulationOpportunityInput:
    return SimulationOpportunityInput(
        opportunity_id=1,
        event_group_key="event-1",
        involved_market_ids=[1, 2],
        opportunity_type="neg_risk_long_yes_bundle",
        gross_price_sum=Decimal("0.5500"),
        detection_window_start=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
        raw_context={"execution_evaluation_status": "pending"},
    )


def _snapshot(
    *,
    market_id: int,
    asks: list[tuple[str, str]],
    taker_fee_bps: int = 40,
    min_order_size: str = "1.0000",
    snapshot_id: int | None = None,
) -> SimulationSnapshotInput:
    best_ask = Decimal(asks[0][0]) if asks else None
    ask_depth_usd = sum(Decimal(price) * Decimal(size) for price, size in asks) if asks else None
    return SimulationSnapshotInput(
        snapshot_id=snapshot_id or market_id * 10,
        market_id=market_id,
        captured_at=datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc),
        best_bid=Decimal("0.1000"),
        best_ask=best_ask,
        bid_depth_usd=Decimal("10.0000"),
        ask_depth_usd=ask_depth_usd,
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


class ExecutionSimulatorTests(unittest.TestCase):
    def test_marks_opportunity_executable_when_requested_size_fits_positive_depth(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")]),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_EXECUTABLE)
        self.assertEqual(result.executable_size_usd, Decimal("100.0000"))
        self.assertEqual(result.gross_cost_usd, Decimal("55.0000"))
        self.assertEqual(result.estimated_fees_usd, Decimal("0.1590"))
        self.assertEqual(result.estimated_net_edge_usd, Decimal("44.8410"))
        self.assertEqual(result.fill_completion_ratio, Decimal("1.0000"))
        self.assertEqual(result.raw_context["selected_snapshot_ids"], {"1": 10, "2": 20})

    def test_marks_opportunity_partially_executable_when_positive_edge_ends_before_requested_size(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(
                    market_id=1,
                    asks=[("0.3000", "10.0000"), ("0.9000", "90.0000")],
                ),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_PARTIALLY_EXECUTABLE)
        self.assertEqual(result.executable_size_usd, Decimal("10.0000"))
        self.assertEqual(result.gross_cost_usd, Decimal("5.5000"))
        self.assertEqual(result.estimated_net_edge_usd, Decimal("4.4841"))
        self.assertEqual(result.fill_completion_ratio, Decimal("0.1000"))

    def test_rejects_when_any_snapshot_is_missing(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")]),
                2: None,
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_REJECTED)
        self.assertEqual(result.simulation_reason, SIMULATION_REASON_MISSING_SNAPSHOT)
        self.assertEqual(result.raw_context["missing_snapshot_market_ids"], [2])

    def test_rejects_when_persisted_order_book_has_no_depth(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, asks=[]),
                2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_REJECTED)
        self.assertEqual(result.simulation_reason, SIMULATION_REASON_MISSING_ORDER_BOOK)
        self.assertEqual(result.executable_size_usd, Decimal("0.0000"))

    def test_rejects_when_latest_snapshot_prices_remove_bundle_edge(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, asks=[("0.6000", "100.0000")]),
                2: _snapshot(market_id=2, asks=[("0.4500", "100.0000")]),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_REJECTED)
        self.assertEqual(result.simulation_reason, SIMULATION_REASON_INSUFFICIENT_EDGE)
        self.assertEqual(result.executable_size_usd, Decimal("0.0000"))


if __name__ == "__main__":
    unittest.main()
