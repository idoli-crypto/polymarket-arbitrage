from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, Market, MarketSnapshot, SimulationResult
from apps.worker.execution_simulation import simulate_pending_validated_opportunities
from apps.worker.simulators.execution import SIMULATION_VERSION


def _book(asks: list[tuple[str, str]]) -> dict:
    return {
        "pricing_outcome": "Yes",
        "tokens": [
            {
                "outcome": "Yes",
                "token_id": "token",
                "asks": [{"price": price, "size": size} for price, size in asks],
                "bids": [],
            }
        ],
    }


class ExecutionSimulationIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "simulation-test.db"
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

    def test_simulate_pending_validated_opportunities_persists_only_valid_unsimulated_rows(self) -> None:
        with self.SessionLocal() as session:
            market_a = self._market("pm-1", "Will Alice win?", "event-1")
            market_b = self._market("pm-2", "Will Bob win?", "event-1")
            market_c = self._market("pm-3", "Will Carol win?", "event-2")
            market_d = self._market("pm-4", "Will Dave win?", "event-2")
            market_e = self._market("pm-5", "Will Erin win?", "event-3")
            market_f = self._market("pm-6", "Will Frank win?", "event-3")
            session.add_all([market_a, market_b, market_c, market_d, market_e, market_f])
            session.flush()

            session.add_all(
                [
                    self._snapshot(
                        market_id=market_a.id,
                        best_ask="0.3000",
                        ask_depth_usd="30.0000",
                        order_book_json=_book([("0.3000", "100.0000")]),
                        captured_at=datetime(2026, 4, 21, 10, 5, 0, tzinfo=timezone.utc),
                    ),
                    self._snapshot(
                        market_id=market_b.id,
                        best_ask="0.2000",
                        ask_depth_usd="20.0000",
                        order_book_json=_book([("0.2000", "100.0000")]),
                        captured_at=datetime(2026, 4, 21, 10, 5, 0, tzinfo=timezone.utc),
                    ),
                    self._snapshot(
                        market_id=market_c.id,
                        best_ask="0.3000",
                        ask_depth_usd="5.0000",
                        order_book_json=_book([("0.3000", "10.0000"), ("0.9000", "90.0000")]),
                        captured_at=datetime(2026, 4, 21, 10, 7, 0, tzinfo=timezone.utc),
                    ),
                    self._snapshot(
                        market_id=market_d.id,
                        best_ask="0.2500",
                        ask_depth_usd="25.0000",
                        order_book_json=_book([("0.2500", "100.0000")]),
                        captured_at=datetime(2026, 4, 21, 10, 7, 0, tzinfo=timezone.utc),
                    ),
                    self._snapshot(
                        market_id=market_e.id,
                        best_ask="0.3100",
                        ask_depth_usd="31.0000",
                        order_book_json=_book([("0.3100", "100.0000")]),
                        captured_at=datetime(2026, 4, 21, 10, 8, 0, tzinfo=timezone.utc),
                    ),
                    self._snapshot(
                        market_id=market_f.id,
                        best_ask="0.3200",
                        ask_depth_usd="32.0000",
                        order_book_json=_book([("0.3200", "100.0000")]),
                        captured_at=datetime(2026, 4, 21, 10, 8, 0, tzinfo=timezone.utc),
                    ),
                ]
            )

            latest_snapshot_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 6, tzinfo=timezone.utc),
                event_group_key="event-1",
                involved_market_ids=[market_a.id, market_b.id],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.5000"),
                gross_gap=Decimal("0.5000"),
                detector_version="neg_risk_v1",
                status="detected",
                validation_status="valid",
                validated_at=datetime(2026, 4, 21, 10, 6, tzinfo=timezone.utc),
                raw_context={"semantic_validation_status": "valid"},
            )
            insufficient_depth_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 7, tzinfo=timezone.utc),
                event_group_key="event-2",
                involved_market_ids=[market_c.id, market_d.id],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.5500"),
                gross_gap=Decimal("0.4500"),
                detector_version="neg_risk_v1",
                status="detected",
                validation_status="valid",
                validated_at=datetime(2026, 4, 21, 10, 7, tzinfo=timezone.utc),
                raw_context={"semantic_validation_status": "valid"},
            )
            already_simulated_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 8, tzinfo=timezone.utc),
                event_group_key="event-3",
                involved_market_ids=[market_e.id, market_f.id],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6300"),
                gross_gap=Decimal("0.3700"),
                detector_version="neg_risk_v1",
                status="detected",
                validation_status="valid",
                validated_at=datetime(2026, 4, 21, 10, 8, tzinfo=timezone.utc),
                raw_context={"semantic_validation_status": "valid"},
            )
            rejected_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 9, tzinfo=timezone.utc),
                event_group_key="event-4",
                involved_market_ids=[market_a.id, market_c.id],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6000"),
                gross_gap=Decimal("0.4000"),
                detector_version="neg_risk_v1",
                status="detected",
                validation_status="rejected",
                validated_at=datetime(2026, 4, 21, 10, 9, tzinfo=timezone.utc),
                raw_context={"semantic_validation_status": "rejected"},
            )
            session.add_all(
                [
                    latest_snapshot_opportunity,
                    insufficient_depth_opportunity,
                    already_simulated_opportunity,
                    rejected_opportunity,
                ]
            )
            session.flush()
            session.add(
                ExecutionSimulation(
                    opportunity_id=already_simulated_opportunity.id,
                    simulated_at=datetime(2026, 4, 21, 10, 9, tzinfo=timezone.utc),
                    simulation_status="executable",
                    intended_size_usd=Decimal("100.0000"),
                    executable_size_usd=Decimal("100.0000"),
                    gross_cost_usd=Decimal("63.0000"),
                    gross_payout_usd=Decimal("100.0000"),
                    estimated_fees_usd=Decimal("0.1683"),
                    estimated_slippage_usd=Decimal("0.0000"),
                    estimated_net_edge_usd=Decimal("36.8317"),
                    fill_completion_ratio=Decimal("1.0000"),
                    simulation_reason="executable",
                    raw_context={"simulation_version": "execution_sim_v2"},
                )
            )
            session.commit()

            results = simulate_pending_validated_opportunities(session)
            self.assertEqual(len(results), 2)

            simulations = session.scalars(
                select(ExecutionSimulation).order_by(ExecutionSimulation.opportunity_id.asc())
            ).all()
            self.assertEqual(len(simulations), 3)

            latest_snapshot_result = next(
                row for row in simulations if row.opportunity_id == latest_snapshot_opportunity.id
            )
            self.assertEqual(latest_snapshot_result.simulation_status, "executable")
            self.assertEqual(latest_snapshot_result.executable_size_usd, Decimal("100.0000"))
            self.assertEqual(latest_snapshot_result.gross_cost_usd, Decimal("50.0000"))
            self.assertEqual(latest_snapshot_result.estimated_fees_usd, Decimal("0.1480"))
            self.assertEqual(latest_snapshot_result.estimated_net_edge_usd, Decimal("49.8520"))
            self.assertEqual(
                latest_snapshot_result.raw_context["execution_simulation"]["selected_snapshot_ids"],
                {str(market_a.id): 1, str(market_b.id): 2},
            )

            insufficient_depth_result = next(
                row for row in simulations if row.opportunity_id == insufficient_depth_opportunity.id
            )
            self.assertEqual(insufficient_depth_result.simulation_status, "partially_executable")
            self.assertEqual(insufficient_depth_result.simulation_reason, "partially_executable")
            self.assertEqual(insufficient_depth_result.executable_size_usd, Decimal("10.0000"))

            refreshed_opportunities = session.scalars(
                select(DetectedOpportunity).order_by(DetectedOpportunity.id.asc())
            ).all()
            self.assertEqual(refreshed_opportunities[0].simulation_version, SIMULATION_VERSION)
            self.assertIsNone(refreshed_opportunities[0].fee_adjusted_edge)
            self.assertIsNone(refreshed_opportunities[0].min_executable_size)

            simulation_results = session.scalars(
                select(SimulationResult).order_by(SimulationResult.opportunity_id.asc())
            ).all()
            self.assertEqual(len(simulation_results), 2)
            self.assertEqual(simulation_results[0].simulation_version, SIMULATION_VERSION)
            self.assertEqual(simulation_results[0].executable_edge, Decimal("0.4985"))
            self.assertEqual(simulation_results[1].executable_edge, Decimal("0.4484"))

            rerun_results = simulate_pending_validated_opportunities(session)
            self.assertEqual(rerun_results, [])

    def _market(self, polymarket_market_id: str, question: str, event_id: str) -> Market:
        return Market(
            polymarket_market_id=polymarket_market_id,
            question=question,
            slug=polymarket_market_id,
            condition_id=f"condition-{polymarket_market_id}",
            event_id=event_id,
            event_slug=event_id,
            neg_risk=True,
            raw_market_json={"takerBaseFee": 40, "orderMinSize": 1},
        )

    def _snapshot(
        self,
        *,
        market_id: int,
        best_ask: str,
        ask_depth_usd: str,
        order_book_json: dict,
        captured_at: datetime,
    ) -> MarketSnapshot:
        return MarketSnapshot(
            market_id=market_id,
            best_bid=Decimal("0.1000"),
            best_ask=Decimal(best_ask),
            bid_depth_usd=Decimal("10.0000"),
            ask_depth_usd=Decimal(ask_depth_usd),
            order_book_json=order_book_json,
            captured_at=captured_at,
        )


if __name__ == "__main__":
    unittest.main()
