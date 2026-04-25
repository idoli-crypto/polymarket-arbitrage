from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import unittest

from apps.worker.validators.executable_edge import (
    ExecutableEdgeValidationInput,
    parse_executable_market_snapshot,
    validate_executable_edge,
)
from apps.worker.validators.simulation import (
    STATUS_INVALID,
    STATUS_VALID,
    SimulationValidationInput,
    validate_simulation_execution,
)


REFERENCE_TIME = datetime(2026, 4, 24, 12, 0, 30, tzinfo=timezone.utc)


def _snapshot(
    *,
    market_id: int,
    asks: list[tuple[str, str]],
    taker_fee_bps: int = 40,
    min_order_size: str = "1.0000",
) -> object:
    return parse_executable_market_snapshot(
        market_id=market_id,
        snapshot_id=market_id * 10,
        captured_at=datetime(2026, 4, 24, 12, 0, 10 + market_id, tzinfo=timezone.utc),
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


def _execution_result(snapshots: dict[int, object]):
    return validate_executable_edge(
        ExecutableEdgeValidationInput(
            opportunity_id=1,
            event_group_key="event-1",
            involved_market_ids=list(snapshots.keys()),
            family="neg_risk_conversion",
            opportunity_type="neg_risk_long_yes_bundle",
        ),
        market_snapshots=snapshots,
        reference_time=REFERENCE_TIME,
    )


class SimulationValidatorTests(unittest.TestCase):
    def test_validates_full_sequential_execution(self) -> None:
        snapshots = {
            1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")]),
            2: _snapshot(market_id=2, asks=[("0.2500", "100.0000")]),
        }

        result = validate_simulation_execution(
            SimulationValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            execution_result=_execution_result(snapshots),
            market_snapshots=snapshots,
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertTrue(result.execution_feasible)
        self.assertEqual(result.fill_completion_ratio, Decimal("1.0000"))
        self.assertEqual(result.executable_edge, Decimal("0.4484"))
        self.assertEqual(result.capital_required_usd, Decimal("55.1590"))
        self.assertEqual(result.capital_lock_estimate_hours, Decimal("0.0069"))
        self.assertEqual(result.execution_time_sensitivity_seconds, 25)

    def test_detects_partial_fill_when_later_leg_cannot_complete_requested_size(self) -> None:
        snapshots = {
            1: _snapshot(market_id=1, asks=[("0.3000", "100.0000")]),
            2: _snapshot(market_id=2, asks=[("0.2500", "40.0000")]),
        }

        result = validate_simulation_execution(
            SimulationValidationInput(
                opportunity_id=2,
                event_group_key="event-2",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            execution_result=_execution_result(snapshots),
            market_snapshots=snapshots,
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertFalse(result.execution_feasible)
        self.assertEqual(result.fill_completion_ratio, Decimal("0.4000"))
        self.assertEqual(result.execution_risk_flag, "partial_fill_risk")
        self.assertEqual(result.details["reason_code"], "partial_fill_detected")
        self.assertEqual(result.details["legs"][0]["exposed_unmatched_size"], "60.0000")
        self.assertEqual(result.details["legs"][0]["exposed_unmatched_cost_usd"], "18.0000")

    def test_detects_multi_leg_execution_failure_and_carries_reduced_size_forward(self) -> None:
        snapshots = {
            1: _snapshot(market_id=1, asks=[("0.2000", "100.0000")]),
            2: _snapshot(market_id=2, asks=[("0.3000", "40.0000")]),
            3: _snapshot(market_id=3, asks=[("0.1000", "40.0000")]),
        }

        result = validate_simulation_execution(
            SimulationValidationInput(
                opportunity_id=3,
                event_group_key="event-3",
                involved_market_ids=[1, 2, 3],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            execution_result=_execution_result(snapshots),
            market_snapshots=snapshots,
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.fill_completion_ratio, Decimal("0.4000"))
        self.assertEqual(result.details["legs"][2]["requested_size"], "40.0000")
        self.assertEqual(result.details["legs"][2]["filled_size"], "40.0000")
        self.assertEqual(result.capital_required_usd, Decimal("36.1120"))

    def test_rejects_when_execution_validation_prerequisite_failed(self) -> None:
        snapshots = {
            1: _snapshot(market_id=1, asks=[("0.6000", "100.0000")]),
            2: _snapshot(market_id=2, asks=[("0.4500", "100.0000")]),
        }
        execution_result = _execution_result(snapshots)

        result = validate_simulation_execution(
            SimulationValidationInput(
                opportunity_id=4,
                event_group_key="event-4",
                involved_market_ids=[1, 2],
                family="neg_risk_conversion",
                opportunity_type="neg_risk_long_yes_bundle",
            ),
            execution_result=execution_result,
            market_snapshots=snapshots,
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "execution_validation_prerequisite_failed")
        self.assertEqual(result.fill_completion_ratio, Decimal("0.0000"))


if __name__ == "__main__":
    unittest.main()
