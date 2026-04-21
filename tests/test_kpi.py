from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, KpiSnapshot
from apps.worker.metrics.kpi import calculate_and_persist_kpi_snapshot, derive_opportunity_kpi


class OpportunityKpiUnitTests(unittest.TestCase):
    def test_derive_opportunity_kpi_uses_stored_fill_ratio_and_weighted_capital_fields(self) -> None:
        opportunity = DetectedOpportunity(
            id=10,
            detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
            event_group_key="event-1",
            involved_market_ids=[1, 2],
            opportunity_type="neg_risk_long_yes_bundle",
            outcome_count=2,
            gross_price_sum=Decimal("0.7000"),
            gross_gap=Decimal("0.3000"),
            detector_version="neg_risk_v1",
            validation_status="valid",
        )
        simulation = ExecutionSimulation(
            id=20,
            opportunity_id=10,
            simulated_at=datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc),
            simulation_status="partially_executable",
            intended_size_usd=Decimal("100.0000"),
            executable_size_usd=Decimal("25.0000"),
            gross_cost_usd=Decimal("17.5000"),
            gross_payout_usd=Decimal("25.0000"),
            estimated_fees_usd=Decimal("0.0000"),
            estimated_slippage_usd=Decimal("0.0000"),
            estimated_net_edge_usd=Decimal("7.5000"),
            fill_completion_ratio=Decimal("0.2500"),
            simulation_reason="partially_executable",
            raw_context={"simulation_version": "execution_sim_v1"},
        )

        result = derive_opportunity_kpi(simulation=simulation, opportunity=opportunity)

        self.assertEqual(result.opportunity_id, 10)
        self.assertEqual(result.real_edge, Decimal("7.5000"))
        self.assertEqual(result.fill_ratio, Decimal("0.2500"))
        self.assertEqual(result.execution_quality, Decimal("0.2500"))
        self.assertEqual(result.slippage_cost, Decimal("0.0000"))
        self.assertEqual(result.capital_efficiency, Decimal("0.0750"))

    def test_derive_opportunity_kpi_is_zero_safe_for_rejected_rows(self) -> None:
        opportunity = DetectedOpportunity(
            id=11,
            detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
            event_group_key="event-2",
            involved_market_ids=[3, 4],
            opportunity_type="neg_risk_long_yes_bundle",
            outcome_count=2,
            gross_price_sum=Decimal("1.1000"),
            gross_gap=Decimal("-0.1000"),
            detector_version="neg_risk_v1",
            validation_status="valid",
        )
        simulation = ExecutionSimulation(
            id=21,
            opportunity_id=11,
            simulated_at=datetime(2026, 4, 21, 10, 6, tzinfo=timezone.utc),
            simulation_status="rejected",
            intended_size_usd=Decimal("100.0000"),
            executable_size_usd=Decimal("0.0000"),
            gross_cost_usd=Decimal("0.0000"),
            gross_payout_usd=Decimal("0.0000"),
            estimated_fees_usd=Decimal("0.0000"),
            estimated_slippage_usd=Decimal("0.0000"),
            estimated_net_edge_usd=Decimal("0.0000"),
            fill_completion_ratio=Decimal("0.0000"),
            simulation_reason="insufficient_depth",
            raw_context={"simulation_version": "execution_sim_v1"},
        )

        result = derive_opportunity_kpi(simulation=simulation, opportunity=opportunity)

        self.assertEqual(result.real_edge, Decimal("0.0000"))
        self.assertEqual(result.fill_ratio, Decimal("0.0000"))
        self.assertEqual(result.capital_efficiency, Decimal("0.0000"))


class KpiSnapshotIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "kpi-test.db"
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

    def test_calculate_and_persist_kpi_snapshot_uses_validated_simulations_and_validation_false_positives(
        self,
    ) -> None:
        with self.SessionLocal() as session:
            valid_executable = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6000"),
                gross_gap=Decimal("0.4000"),
                detector_version="neg_risk_v1",
                validation_status="valid",
            )
            valid_partial = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 1, tzinfo=timezone.utc),
                event_group_key="event-2",
                involved_market_ids=[3, 4],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6500"),
                gross_gap=Decimal("0.3500"),
                detector_version="neg_risk_v1",
                validation_status="valid",
            )
            valid_rejected = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 2, tzinfo=timezone.utc),
                event_group_key="event-3",
                involved_market_ids=[5, 6],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.9800"),
                gross_gap=Decimal("0.0200"),
                detector_version="neg_risk_v1",
                validation_status="valid",
            )
            validation_rejected = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 3, tzinfo=timezone.utc),
                event_group_key="event-4",
                involved_market_ids=[7, 8],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("1.0200"),
                gross_gap=Decimal("-0.0200"),
                detector_version="neg_risk_v1",
                validation_status="rejected",
            )
            simulated_but_not_valid = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 4, tzinfo=timezone.utc),
                event_group_key="event-5",
                involved_market_ids=[9, 10],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.9000"),
                gross_gap=Decimal("0.1000"),
                detector_version="neg_risk_v1",
                validation_status="rejected",
            )
            session.add_all(
                [
                    valid_executable,
                    valid_partial,
                    valid_rejected,
                    validation_rejected,
                    simulated_but_not_valid,
                ]
            )
            session.flush()
            session.add_all(
                [
                    ExecutionSimulation(
                        opportunity_id=valid_executable.id,
                        simulated_at=datetime(2026, 4, 21, 11, 0, tzinfo=timezone.utc),
                        simulation_status="executable",
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("100.0000"),
                        gross_cost_usd=Decimal("70.0000"),
                        gross_payout_usd=Decimal("100.0000"),
                        estimated_fees_usd=Decimal("0.0000"),
                        estimated_slippage_usd=Decimal("0.0000"),
                        estimated_net_edge_usd=Decimal("30.0000"),
                        fill_completion_ratio=Decimal("1.0000"),
                        simulation_reason="executable",
                        raw_context={"simulation_version": "execution_sim_v1"},
                    ),
                    ExecutionSimulation(
                        opportunity_id=valid_partial.id,
                        simulated_at=datetime(2026, 4, 21, 11, 1, tzinfo=timezone.utc),
                        simulation_status="partially_executable",
                        intended_size_usd=Decimal("200.0000"),
                        executable_size_usd=Decimal("50.0000"),
                        gross_cost_usd=Decimal("35.0000"),
                        gross_payout_usd=Decimal("50.0000"),
                        estimated_fees_usd=Decimal("0.0000"),
                        estimated_slippage_usd=Decimal("0.0000"),
                        estimated_net_edge_usd=Decimal("15.0000"),
                        fill_completion_ratio=Decimal("0.2500"),
                        simulation_reason="partially_executable",
                        raw_context={"simulation_version": "execution_sim_v1"},
                    ),
                    ExecutionSimulation(
                        opportunity_id=valid_rejected.id,
                        simulated_at=datetime(2026, 4, 21, 11, 2, tzinfo=timezone.utc),
                        simulation_status="rejected",
                        intended_size_usd=Decimal("50.0000"),
                        executable_size_usd=Decimal("0.0000"),
                        gross_cost_usd=Decimal("0.0000"),
                        gross_payout_usd=Decimal("0.0000"),
                        estimated_fees_usd=Decimal("0.0000"),
                        estimated_slippage_usd=Decimal("0.0000"),
                        estimated_net_edge_usd=Decimal("0.0000"),
                        fill_completion_ratio=Decimal("0.0000"),
                        simulation_reason="insufficient_edge",
                        raw_context={"simulation_version": "execution_sim_v1"},
                    ),
                    ExecutionSimulation(
                        opportunity_id=simulated_but_not_valid.id,
                        simulated_at=datetime(2026, 4, 21, 11, 3, tzinfo=timezone.utc),
                        simulation_status="executable",
                        intended_size_usd=Decimal("500.0000"),
                        executable_size_usd=Decimal("500.0000"),
                        gross_cost_usd=Decimal("450.0000"),
                        gross_payout_usd=Decimal("500.0000"),
                        estimated_fees_usd=Decimal("0.0000"),
                        estimated_slippage_usd=Decimal("0.0000"),
                        estimated_net_edge_usd=Decimal("50.0000"),
                        fill_completion_ratio=Decimal("1.0000"),
                        simulation_reason="executable",
                        raw_context={"simulation_version": "execution_sim_v1"},
                    ),
                ]
            )
            session.commit()

            snapshot = calculate_and_persist_kpi_snapshot(session)

            self.assertEqual(snapshot.total_opportunities, 4)
            self.assertEqual(snapshot.valid_opportunities, 3)
            self.assertEqual(snapshot.executable_opportunities, 1)
            self.assertEqual(snapshot.partial_opportunities, 1)
            self.assertEqual(snapshot.rejected_opportunities, 1)
            self.assertEqual(snapshot.avg_real_edge, Decimal("0.1286"))
            self.assertEqual(snapshot.avg_fill_ratio, Decimal("0.4167"))
            self.assertEqual(snapshot.false_positive_rate, Decimal("0.4000"))
            self.assertEqual(snapshot.total_intended_capital, Decimal("350.0000"))
            self.assertEqual(snapshot.total_executable_capital, Decimal("150.0000"))
            self.assertEqual(
                snapshot.raw_context["validation_summary"]["rejected_validated_opportunities"],
                2,
            )

            stored = session.scalars(select(KpiSnapshot)).all()
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].raw_context["simulation_summary"]["valid_simulated_opportunities"], 3)

    def test_calculate_and_persist_kpi_snapshot_is_zero_safe_for_empty_state(self) -> None:
        with self.SessionLocal() as session:
            snapshot = calculate_and_persist_kpi_snapshot(session)

            self.assertEqual(snapshot.total_opportunities, 0)
            self.assertEqual(snapshot.valid_opportunities, 0)
            self.assertEqual(snapshot.avg_real_edge, Decimal("0.0000"))
            self.assertEqual(snapshot.avg_fill_ratio, Decimal("0.0000"))
            self.assertEqual(snapshot.false_positive_rate, Decimal("0.0000"))
            self.assertEqual(snapshot.total_intended_capital, Decimal("0.0000"))
            self.assertEqual(snapshot.total_executable_capital, Decimal("0.0000"))


if __name__ == "__main__":
    unittest.main()
