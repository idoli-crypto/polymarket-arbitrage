from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, Market, MarketSnapshot
from apps.worker.execution_simulation import simulate_pending_validated_opportunities


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
            market_e = Market(
                polymarket_market_id="pm-5",
                question="Will Erin win?",
                slug="erin-win",
                condition_id="condition-5",
                event_id="event-3",
                event_slug="event-3",
                neg_risk=True,
            )
            market_f = Market(
                polymarket_market_id="pm-6",
                question="Will Frank win?",
                slug="frank-win",
                condition_id="condition-6",
                event_id="event-3",
                event_slug="event-3",
                neg_risk=True,
            )
            session.add_all([market_a, market_b, market_c, market_d, market_e, market_f])
            session.flush()

            session.add_all(
                [
                    MarketSnapshot(
                        market_id=market_a.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.3000"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("15.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_a.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.4000"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("30.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_b.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.2000"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("40.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 6, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_c.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.3000"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("0.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 7, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_d.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.2500"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("50.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 7, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_e.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.3100"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("31.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 8, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market_f.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.3200"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("32.0000"),
                        captured_at=datetime(2026, 4, 21, 10, 8, tzinfo=timezone.utc),
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
                    estimated_fees_usd=Decimal("0.0000"),
                    estimated_slippage_usd=Decimal("0.0000"),
                    estimated_net_edge_usd=Decimal("37.0000"),
                    fill_completion_ratio=Decimal("1.0000"),
                    simulation_reason="executable",
                    raw_context={"simulation_version": "execution_sim_v1"},
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
            self.assertEqual(latest_snapshot_result.simulation_status, "partially_executable")
            self.assertEqual(latest_snapshot_result.executable_size_usd, Decimal("75.0000"))
            self.assertEqual(latest_snapshot_result.gross_cost_usd, Decimal("45.0000"))
            self.assertEqual(latest_snapshot_result.estimated_net_edge_usd, Decimal("30.0000"))
            self.assertEqual(
                latest_snapshot_result.raw_context["execution_simulation"]["selected_snapshot_ids"],
                {str(market_a.id): 2, str(market_b.id): 3},
            )

            insufficient_depth_result = next(
                row for row in simulations if row.opportunity_id == insufficient_depth_opportunity.id
            )
            self.assertEqual(insufficient_depth_result.simulation_status, "rejected")
            self.assertEqual(insufficient_depth_result.simulation_reason, "insufficient_depth")
            self.assertEqual(insufficient_depth_result.executable_size_usd, Decimal("0.0000"))

            rerun_results = simulate_pending_validated_opportunities(session)
            self.assertEqual(rerun_results, [])


if __name__ == "__main__":
    unittest.main()
