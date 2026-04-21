from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import DetectedOpportunity, ExecutionSimulation, KpiSnapshot, Market, MarketSnapshot
from apps.api.db.session import get_db_session
from apps.api.main import create_app


class ResearchApiIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "research-api-test.db"
        self.engine = create_engine(f"sqlite:///{database_path}")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
        self.app = create_app()
        self.app.dependency_overrides[get_db_session] = self._override_db_session
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.app.dependency_overrides.clear()
        self.engine.dispose()
        self.temp_dir.cleanup()

    def _override_db_session(self):
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def test_opportunities_endpoint_returns_only_valid_rows_and_latest_simulation(self) -> None:
        with self.SessionLocal() as session:
            valid_with_simulation = self._create_opportunity(
                session,
                event_group_key="event-1",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 5, tzinfo=timezone.utc),
            )
            valid_without_simulation = self._create_opportunity(
                session,
                event_group_key="event-2",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 6, tzinfo=timezone.utc),
            )
            self._create_opportunity(
                session,
                event_group_key="event-3",
                validation_status="rejected",
                detected_at=datetime(2026, 4, 21, 12, 7, tzinfo=timezone.utc),
            )
            self._create_opportunity(
                session,
                event_group_key="event-4",
                validation_status=None,
                detected_at=datetime(2026, 4, 21, 12, 8, tzinfo=timezone.utc),
            )
            session.flush()
            session.add_all(
                [
                    ExecutionSimulation(
                        opportunity_id=valid_with_simulation.id,
                        simulated_at=datetime(2026, 4, 21, 12, 10, tzinfo=timezone.utc),
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
                    ),
                    ExecutionSimulation(
                        opportunity_id=valid_with_simulation.id,
                        simulated_at=datetime(2026, 4, 21, 12, 11, tzinfo=timezone.utc),
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
                    ),
                ]
            )
            session.commit()

        response = self.client.get("/opportunities")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual([row["opportunity_id"] for row in payload], [valid_without_simulation.id, valid_with_simulation.id])
        self.assertEqual(payload[0]["event_id"], "event-2")
        self.assertIsNone(payload[0]["simulation_status"])
        self.assertIsNone(payload[0]["real_edge"])
        self.assertEqual(payload[1]["event_id"], "event-1")
        self.assertEqual(payload[1]["simulation_status"], "executable")
        self.assertEqual(payload[1]["real_edge"], "37.0000")
        self.assertEqual(payload[1]["fill_ratio"], "1.0000")
        self.assertEqual(payload[1]["intended_size"], "100.0000")
        self.assertEqual(payload[1]["executable_size"], "100.0000")

    def test_simulations_endpoint_returns_newest_first(self) -> None:
        with self.SessionLocal() as session:
            first_opportunity = self._create_opportunity(session, event_group_key="event-1", validation_status="valid")
            second_opportunity = self._create_opportunity(session, event_group_key="event-2", validation_status="valid")
            session.flush()
            session.add_all(
                [
                    ExecutionSimulation(
                        opportunity_id=first_opportunity.id,
                        simulated_at=datetime(2026, 4, 21, 12, 15, tzinfo=timezone.utc),
                        simulation_status="partially_executable",
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("75.0000"),
                        gross_cost_usd=Decimal("45.0000"),
                        gross_payout_usd=Decimal("75.0000"),
                        estimated_fees_usd=Decimal("0.0000"),
                        estimated_slippage_usd=Decimal("0.0000"),
                        estimated_net_edge_usd=Decimal("30.0000"),
                        fill_completion_ratio=Decimal("0.7500"),
                        simulation_reason="partial_fill",
                    ),
                    ExecutionSimulation(
                        opportunity_id=second_opportunity.id,
                        simulated_at=datetime(2026, 4, 21, 12, 16, tzinfo=timezone.utc),
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
                    ),
                ]
            )
            session.commit()

        response = self.client.get("/simulations")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual([row["opportunity_id"] for row in payload], [second_opportunity.id, first_opportunity.id])
        self.assertEqual(payload[0]["simulation_status"], "rejected")
        self.assertEqual(payload[0]["net_edge"], "0.0000")
        self.assertEqual(payload[0]["reason"], "insufficient_depth")
        self.assertEqual(payload[1]["fill_ratio"], "0.7500")

    def test_kpi_latest_endpoint_returns_latest_snapshot_and_context_counts(self) -> None:
        with self.SessionLocal() as session:
            session.add_all(
                [
                    KpiSnapshot(
                        created_at=datetime(2026, 4, 21, 12, 20, tzinfo=timezone.utc),
                        total_opportunities=2,
                        valid_opportunities=2,
                        executable_opportunities=1,
                        partial_opportunities=1,
                        rejected_opportunities=0,
                        avg_real_edge=Decimal("0.1200"),
                        avg_fill_ratio=Decimal("0.8000"),
                        false_positive_rate=Decimal("0.1000"),
                        total_intended_capital=Decimal("200.0000"),
                        total_executable_capital=Decimal("175.0000"),
                        raw_context={"snapshot": "older"},
                    ),
                    KpiSnapshot(
                        created_at=datetime(2026, 4, 21, 12, 21, tzinfo=timezone.utc),
                        total_opportunities=4,
                        valid_opportunities=3,
                        executable_opportunities=1,
                        partial_opportunities=1,
                        rejected_opportunities=1,
                        avg_real_edge=Decimal("0.1286"),
                        avg_fill_ratio=Decimal("0.4167"),
                        false_positive_rate=Decimal("0.4000"),
                        total_intended_capital=Decimal("350.0000"),
                        total_executable_capital=Decimal("150.0000"),
                        raw_context={"snapshot": "latest"},
                    ),
                ]
            )
            session.commit()

        response = self.client.get("/kpi/latest")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "avg_real_edge": "0.1286",
                "avg_fill_ratio": "0.4167",
                "false_positive_rate": "0.4000",
                "total_intended_capital": "350.0000",
                "total_executable_capital": "150.0000",
                "total_opportunities": 4,
                "valid_opportunities": 3,
            },
        )

    def test_kpi_latest_endpoint_returns_not_found_when_empty(self) -> None:
        response = self.client.get("/kpi/latest")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "No KPI snapshots found")

    def test_system_status_endpoint_returns_latest_stage_timestamps(self) -> None:
        with self.SessionLocal() as session:
            market = Market(
                polymarket_market_id="pm-1",
                question="Will Alice win?",
                event_id="event-1",
                event_slug="event-1",
                neg_risk=True,
            )
            session.add(market)
            session.flush()
            session.add_all(
                [
                    MarketSnapshot(
                        market_id=market.id,
                        best_bid=Decimal("0.1000"),
                        best_ask=Decimal("0.2000"),
                        bid_depth_usd=Decimal("10.0000"),
                        ask_depth_usd=Decimal("20.0000"),
                        captured_at=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
                    ),
                    MarketSnapshot(
                        market_id=market.id,
                        best_bid=Decimal("0.1100"),
                        best_ask=Decimal("0.2100"),
                        bid_depth_usd=Decimal("11.0000"),
                        ask_depth_usd=Decimal("21.0000"),
                        captured_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                    ),
                ]
            )
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-1",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
            )
            session.flush()
            session.add(
                ExecutionSimulation(
                    opportunity_id=opportunity.id,
                    simulated_at=datetime(2026, 4, 21, 12, 33, tzinfo=timezone.utc),
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
                )
            )
            session.add(
                KpiSnapshot(
                    created_at=datetime(2026, 4, 21, 12, 34, tzinfo=timezone.utc),
                    total_opportunities=1,
                    valid_opportunities=1,
                    executable_opportunities=1,
                    partial_opportunities=0,
                    rejected_opportunities=0,
                    avg_real_edge=Decimal("0.3700"),
                    avg_fill_ratio=Decimal("1.0000"),
                    false_positive_rate=Decimal("0.0000"),
                    total_intended_capital=Decimal("100.0000"),
                    total_executable_capital=Decimal("100.0000"),
                    raw_context={"snapshot": "latest"},
                )
            )
            session.commit()

        response = self.client.get("/system/status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "last_snapshot_time": "2026-04-21T12:31:00",
                "last_detection_time": "2026-04-21T12:32:00",
                "last_simulation_time": "2026-04-21T12:33:00",
                "last_kpi_time": "2026-04-21T12:34:00",
            },
        )

    def _create_opportunity(
        self,
        session,
        *,
        event_group_key: str,
        validation_status: str | None,
        detected_at: datetime | None = None,
    ) -> DetectedOpportunity:
        opportunity = DetectedOpportunity(
            detected_at=detected_at or datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
            detection_window_start=datetime(2026, 4, 21, 11, 59, tzinfo=timezone.utc),
            event_group_key=event_group_key,
            involved_market_ids=[1, 2],
            opportunity_type="neg_risk_long_yes_bundle",
            outcome_count=2,
            gross_price_sum=Decimal("0.6300"),
            gross_gap=Decimal("0.3700"),
            detector_version="neg_risk_v1",
            status="detected",
            validation_status=validation_status,
        )
        session.add(opportunity)
        return opportunity
