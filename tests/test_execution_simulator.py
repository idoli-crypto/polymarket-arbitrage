from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from apps.worker.simulators.execution import (
    SIMULATION_REASON_INSUFFICIENT_DEPTH,
    SIMULATION_REASON_INSUFFICIENT_EDGE,
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
    best_ask: str | None,
    ask_depth_usd: str | None,
    snapshot_id: int | None = None,
) -> SimulationSnapshotInput:
    return SimulationSnapshotInput(
        snapshot_id=snapshot_id or market_id * 10,
        market_id=market_id,
        captured_at=datetime(2026, 4, 21, 12, market_id, tzinfo=timezone.utc),
        best_bid=Decimal("0.1000"),
        best_ask=Decimal(best_ask) if best_ask is not None else None,
        bid_depth_usd=Decimal("10.0000"),
        ask_depth_usd=Decimal(ask_depth_usd) if ask_depth_usd is not None else None,
    )


class ExecutionSimulatorTests(unittest.TestCase):
    def test_marks_opportunity_executable_when_requested_size_fits_available_depth(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, best_ask="0.3000", ask_depth_usd="60.0000"),
                2: _snapshot(market_id=2, best_ask="0.2500", ask_depth_usd="75.0000"),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_EXECUTABLE)
        self.assertEqual(result.executable_size_usd, Decimal("100.0000"))
        self.assertEqual(result.gross_cost_usd, Decimal("55.0000"))
        self.assertEqual(result.gross_payout_usd, Decimal("100.0000"))
        self.assertEqual(result.estimated_net_edge_usd, Decimal("45.0000"))
        self.assertEqual(result.fill_completion_ratio, Decimal("1.0000"))
        self.assertEqual(result.raw_context["selected_snapshot_ids"], {"1": 10, "2": 20})

    def test_marks_opportunity_partially_executable_when_depth_is_shallow(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, best_ask="0.3000", ask_depth_usd="15.0000"),
                2: _snapshot(market_id=2, best_ask="0.2500", ask_depth_usd="75.0000"),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_PARTIALLY_EXECUTABLE)
        self.assertEqual(result.executable_size_usd, Decimal("50.0000"))
        self.assertEqual(result.gross_cost_usd, Decimal("27.5000"))
        self.assertEqual(result.gross_payout_usd, Decimal("50.0000"))
        self.assertEqual(result.estimated_net_edge_usd, Decimal("22.5000"))
        self.assertEqual(result.fill_completion_ratio, Decimal("0.5000"))

    def test_rejects_when_any_snapshot_is_missing(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, best_ask="0.3000", ask_depth_usd="60.0000"),
                2: None,
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_REJECTED)
        self.assertEqual(result.simulation_reason, SIMULATION_REASON_MISSING_SNAPSHOT)
        self.assertEqual(result.raw_context["missing_snapshot_market_ids"], [2])

    def test_rejects_when_latest_top_of_book_has_no_executable_depth(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, best_ask="0.3000", ask_depth_usd="0.0000"),
                2: _snapshot(market_id=2, best_ask="0.2500", ask_depth_usd="75.0000"),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_REJECTED)
        self.assertEqual(result.simulation_reason, SIMULATION_REASON_INSUFFICIENT_DEPTH)
        self.assertEqual(result.executable_size_usd, Decimal("0.0000"))

    def test_rejects_when_latest_snapshot_prices_remove_bundle_edge(self) -> None:
        result = simulate_validated_opportunity(
            _opportunity(),
            latest_snapshots={
                1: _snapshot(market_id=1, best_ask="0.6000", ask_depth_usd="120.0000"),
                2: _snapshot(market_id=2, best_ask="0.4500", ask_depth_usd="90.0000"),
            },
        )

        self.assertEqual(result.simulation_status, SIMULATION_STATUS_REJECTED)
        self.assertEqual(result.simulation_reason, SIMULATION_REASON_INSUFFICIENT_EDGE)
        self.assertEqual(result.executable_size_usd, Decimal("100.0000"))
        self.assertEqual(result.estimated_net_edge_usd, Decimal("-5.0000"))


if __name__ == "__main__":
    unittest.main()
