from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import DetectedOpportunity, KpiRunSummary, KpiSnapshot, OpportunityKpiSnapshot
from apps.api.services.opportunity_classification import DetectionFamily
from apps.worker.metrics.kpi import (
    OpportunityKpiSnapshotInput,
    persist_kpi_run,
)


class KpiPersistenceIntegrationTests(unittest.TestCase):
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

    def test_persist_kpi_run_creates_append_only_snapshots_and_run_summary(self) -> None:
        with self.SessionLocal() as session:
            first = self._create_opportunity(session, opportunity_id=1, event_group_key="event-1")
            second = self._create_opportunity(
                session,
                opportunity_id=2,
                event_group_key="event-2",
                family=DetectionFamily.CROSS_MARKET_LOGIC,
            )
            third = self._create_opportunity(session, opportunity_id=3, event_group_key="event-3")
            session.commit()

            run_summary = persist_kpi_run(
                session,
                [
                    self._build_input(
                        opportunity=first,
                        detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc),
                        final_status="valid",
                        rejection_reason=None,
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="valid",
                        execution_status="valid",
                        simulation_status="valid",
                        fee_adjusted_edge=Decimal("0.3000"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0100"),
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("100.0000"),
                    ),
                    self._build_input(
                        opportunity=second,
                        detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 6, tzinfo=timezone.utc),
                        final_status="rejected",
                        rejection_reason="dispute_flag_present",
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="risky",
                        execution_status=None,
                        simulation_status=None,
                        fee_adjusted_edge=None,
                        fill_completion_ratio=None,
                        execution_feasible=None,
                        capital_lock_estimate_hours=None,
                        intended_size_usd=None,
                        executable_size_usd=None,
                    ),
                    self._build_input(
                        opportunity=third,
                        detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 7, tzinfo=timezone.utc),
                        final_status="rejected",
                        rejection_reason="partial_fill_detected",
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="valid",
                        execution_status="valid",
                        simulation_status="invalid",
                        fee_adjusted_edge=Decimal("0.1200"),
                        fill_completion_ratio=Decimal("0.4000"),
                        execution_feasible=False,
                        capital_lock_estimate_hours=Decimal("0.0200"),
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("40.0000"),
                    ),
                ],
                run_started_at=datetime(2026, 4, 21, 10, 4, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 10, 8, tzinfo=timezone.utc),
            )
            session.commit()

            assert run_summary is not None
            self.assertEqual(run_summary.total_opportunities, 3)
            self.assertEqual(run_summary.valid_after_rule, 3)
            self.assertEqual(run_summary.valid_after_semantic, 3)
            self.assertEqual(run_summary.valid_after_resolution, 2)
            self.assertEqual(run_summary.valid_after_executable, 2)
            self.assertEqual(run_summary.valid_after_simulation, 1)
            self.assertEqual(run_summary.avg_executable_edge, Decimal("0.2100"))
            self.assertEqual(run_summary.avg_fill_ratio, Decimal("0.7000"))
            self.assertEqual(run_summary.avg_capital_lock, Decimal("0.0150"))
            self.assertEqual(run_summary.false_positive_rate, Decimal("0.6667"))
            self.assertEqual(
                run_summary.family_distribution,
                {
                    DetectionFamily.CROSS_MARKET_LOGIC.value: 1,
                    DetectionFamily.NEG_RISK_CONVERSION.value: 2,
                },
            )

            stored_run = session.scalar(select(KpiRunSummary))
            assert stored_run is not None
            self.assertEqual(stored_run.raw_context["absence_decay_count"], 0)
            self.assertEqual(stored_run.raw_context["rejection_distribution"]["resolution"], 1)
            self.assertEqual(stored_run.raw_context["rejection_distribution"]["simulation"], 1)

            stored_snapshots = session.scalars(
                select(OpportunityKpiSnapshot).order_by(OpportunityKpiSnapshot.opportunity_id.asc())
            ).all()
            self.assertEqual(len(stored_snapshots), 3)

            self.assertEqual(stored_snapshots[0].validation_stage_reached, "simulation_pass")
            self.assertEqual(stored_snapshots[0].final_status, "valid")
            self.assertIsNone(stored_snapshots[0].rejection_stage)
            self.assertEqual(stored_snapshots[0].decay_status, "alive")

            self.assertEqual(stored_snapshots[1].validation_stage_reached, "semantic_pass")
            self.assertEqual(stored_snapshots[1].rejection_stage, "resolution")
            self.assertEqual(stored_snapshots[1].rejection_reason, "dispute_flag_present")
            self.assertFalse(stored_snapshots[1].resolution_pass)
            self.assertEqual(stored_snapshots[1].decay_status, "decayed")

            self.assertEqual(stored_snapshots[2].validation_stage_reached, "executable_pass")
            self.assertEqual(stored_snapshots[2].rejection_stage, "simulation")
            self.assertEqual(stored_snapshots[2].fill_completion_ratio, Decimal("0.4000"))
            self.assertFalse(stored_snapshots[2].execution_feasible)

            legacy_snapshot = session.scalar(select(KpiSnapshot))
            assert legacy_snapshot is not None
            self.assertEqual(legacy_snapshot.total_opportunities, 3)
            self.assertEqual(legacy_snapshot.valid_opportunities, 1)
            self.assertEqual(legacy_snapshot.executable_opportunities, 1)
            self.assertEqual(legacy_snapshot.partial_opportunities, 1)
            self.assertEqual(legacy_snapshot.rejected_opportunities, 2)
            self.assertEqual(legacy_snapshot.avg_real_edge, Decimal("0.2100"))
            self.assertEqual(legacy_snapshot.avg_fill_ratio, Decimal("0.7000"))
            self.assertEqual(legacy_snapshot.total_intended_capital, Decimal("200.0000"))
            self.assertEqual(legacy_snapshot.total_executable_capital, Decimal("140.0000"))

    def test_persist_kpi_run_tracks_persistence_decay_and_reappearance(self) -> None:
        with self.SessionLocal() as session:
            first = self._create_opportunity(session, opportunity_id=10, event_group_key="event-1")
            second = self._create_opportunity(session, opportunity_id=11, event_group_key="event-1")
            third = self._create_opportunity(session, opportunity_id=12, event_group_key="event-2")
            fourth = self._create_opportunity(session, opportunity_id=13, event_group_key="event-1")
            session.commit()

            persist_kpi_run(
                session,
                [
                    self._build_input(
                        opportunity=first,
                        detection_window_start=datetime(2026, 4, 21, 10, 0, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc),
                        final_status="valid",
                        rejection_reason=None,
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="valid",
                        execution_status="valid",
                        simulation_status="valid",
                        fee_adjusted_edge=Decimal("0.2500"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0100"),
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("100.0000"),
                    )
                ],
                run_started_at=datetime(2026, 4, 21, 10, 4, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc),
            )
            session.commit()

            persist_kpi_run(
                session,
                [
                    self._build_input(
                        opportunity=second,
                        detection_window_start=datetime(2026, 4, 21, 10, 10, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 15, tzinfo=timezone.utc),
                        final_status="valid",
                        rejection_reason=None,
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="valid",
                        execution_status="valid",
                        simulation_status="valid",
                        fee_adjusted_edge=Decimal("0.2200"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0110"),
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("100.0000"),
                    )
                ],
                run_started_at=datetime(2026, 4, 21, 10, 14, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 10, 15, tzinfo=timezone.utc),
            )
            session.commit()

            persist_kpi_run(
                session,
                [
                    self._build_input(
                        opportunity=third,
                        detection_window_start=datetime(2026, 4, 21, 10, 20, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 21, tzinfo=timezone.utc),
                        final_status="valid",
                        rejection_reason=None,
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="valid",
                        execution_status="valid",
                        simulation_status="valid",
                        fee_adjusted_edge=Decimal("0.1000"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0100"),
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("100.0000"),
                    )
                ],
                run_started_at=datetime(2026, 4, 21, 10, 20, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 10, 21, tzinfo=timezone.utc),
            )
            session.commit()

            persist_kpi_run(
                session,
                [
                    self._build_input(
                        opportunity=fourth,
                        detection_window_start=datetime(2026, 4, 21, 10, 30, tzinfo=timezone.utc),
                        snapshot_timestamp=datetime(2026, 4, 21, 10, 31, tzinfo=timezone.utc),
                        final_status="valid",
                        rejection_reason=None,
                        rule_status="valid",
                        semantic_status="valid",
                        resolution_status="valid",
                        execution_status="valid",
                        simulation_status="valid",
                        fee_adjusted_edge=Decimal("0.1800"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0090"),
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("100.0000"),
                    )
                ],
                run_started_at=datetime(2026, 4, 21, 10, 30, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 10, 31, tzinfo=timezone.utc),
            )
            session.commit()

            event_one_snapshots = session.scalars(
                select(OpportunityKpiSnapshot)
                .where(OpportunityKpiSnapshot.lineage_key == self._lineage_key_for(session, 10))
                .order_by(OpportunityKpiSnapshot.snapshot_timestamp.asc(), OpportunityKpiSnapshot.id.asc())
            ).all()

            self.assertEqual(len(event_one_snapshots), 4)
            self.assertEqual(self._as_utc(event_one_snapshots[0].first_seen_timestamp), datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc))
            self.assertEqual(self._as_utc(event_one_snapshots[1].first_seen_timestamp), datetime(2026, 4, 21, 10, 5, tzinfo=timezone.utc))
            self.assertEqual(self._as_utc(event_one_snapshots[1].last_seen_timestamp), datetime(2026, 4, 21, 10, 15, tzinfo=timezone.utc))
            self.assertEqual(event_one_snapshots[1].persistence_duration_seconds, 600)
            self.assertEqual(event_one_snapshots[2].rejection_stage, "persistence")
            self.assertEqual(event_one_snapshots[2].rejection_reason, "no_longer_present")
            self.assertEqual(self._as_utc(event_one_snapshots[2].snapshot_timestamp), datetime(2026, 4, 21, 10, 20, tzinfo=timezone.utc))
            self.assertEqual(self._as_utc(event_one_snapshots[2].last_seen_timestamp), datetime(2026, 4, 21, 10, 15, tzinfo=timezone.utc))
            self.assertEqual(event_one_snapshots[2].persistence_duration_seconds, 600)
            self.assertEqual(event_one_snapshots[2].decay_status, "decayed")
            self.assertEqual(event_one_snapshots[3].opportunity_id, 13)
            self.assertEqual(self._as_utc(event_one_snapshots[3].first_seen_timestamp), datetime(2026, 4, 21, 10, 31, tzinfo=timezone.utc))
            self.assertEqual(event_one_snapshots[3].persistence_duration_seconds, 0)
            self.assertEqual(event_one_snapshots[3].decay_status, "alive")

    def _create_opportunity(
        self,
        session,
        *,
        opportunity_id: int,
        event_group_key: str,
        family: DetectionFamily = DetectionFamily.NEG_RISK_CONVERSION,
    ) -> DetectedOpportunity:
        opportunity = DetectedOpportunity(
            id=opportunity_id,
            detection_window_start=datetime(2026, 4, 21, 9, opportunity_id, tzinfo=timezone.utc),
            event_group_key=event_group_key,
            involved_market_ids=[1, 2],
            involved_market_ids_json=[1, 2],
            opportunity_type="neg_risk_long_yes_bundle",
            outcome_count=2,
            gross_price_sum=Decimal("0.6300"),
            gross_gap=Decimal("0.3700"),
            family=family.value,
            relation_type=None,
            relation_direction=None,
            detector_version="neg_risk_v1",
            status="detected",
        )
        session.add(opportunity)
        return opportunity

    def _build_input(
        self,
        *,
        opportunity: DetectedOpportunity,
        detection_window_start: datetime,
        snapshot_timestamp: datetime,
        final_status: str,
        rejection_reason: str | None,
        rule_status: str,
        semantic_status: str,
        resolution_status: str,
        execution_status: str | None,
        simulation_status: str | None,
        fee_adjusted_edge: Decimal | None,
        fill_completion_ratio: Decimal | None,
        execution_feasible: bool | None,
        capital_lock_estimate_hours: Decimal | None,
        intended_size_usd: Decimal | None,
        executable_size_usd: Decimal | None,
    ) -> OpportunityKpiSnapshotInput:
        return OpportunityKpiSnapshotInput(
            opportunity_id=opportunity.id,
            event_group_key=opportunity.event_group_key,
            involved_market_ids=list(opportunity.involved_market_ids),
            opportunity_type=opportunity.opportunity_type,
            family=opportunity.family,
            relation_type=opportunity.relation_type,
            relation_direction=opportunity.relation_direction,
            detection_window_start=detection_window_start,
            snapshot_timestamp=snapshot_timestamp,
            final_status=final_status,
            rejection_reason=rejection_reason,
            s_logic=Decimal("1.0000"),
            s_sem=Decimal("1.0000") if semantic_status == "valid" else None,
            s_res=Decimal("1.0000") if resolution_status == "valid" else (Decimal("0.5000") if resolution_status == "risky" else None),
            top_of_book_edge=Decimal("0.3500"),
            depth_weighted_edge=Decimal("0.3300"),
            fee_adjusted_edge=fee_adjusted_edge,
            fill_completion_ratio=fill_completion_ratio,
            execution_feasible=execution_feasible,
            capital_lock_estimate_hours=capital_lock_estimate_hours,
            detector_version=opportunity.detector_version,
            validation_version="validation_v1",
            simulation_version="simulation_v1" if simulation_status is not None else None,
            rule_status=rule_status,
            semantic_status=semantic_status,
            resolution_status=resolution_status,
            execution_status=execution_status,
            simulation_status=simulation_status,
            intended_size_usd=intended_size_usd,
            executable_size_usd=executable_size_usd,
        )

    def _lineage_key_for(self, session, opportunity_id: int) -> str:
        snapshot = session.scalar(
            select(OpportunityKpiSnapshot)
            .where(OpportunityKpiSnapshot.opportunity_id == opportunity_id)
            .order_by(OpportunityKpiSnapshot.id.asc())
            .limit(1)
        )
        assert snapshot is not None
        return snapshot.lineage_key

    def _as_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


if __name__ == "__main__":
    unittest.main()
