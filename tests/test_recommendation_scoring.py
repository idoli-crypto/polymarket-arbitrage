from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.db.models import (
    KpiRunSummary,
    OpportunityKpiSnapshot,
    RecommendationScore,
    RecommendationScoringRun,
)
from apps.api.repositories.opportunities import (
    attach_simulation_result,
    attach_validation_result,
    create_opportunity_extended,
    list_ranked_recommendations,
)
from apps.api.repositories.recommendation_scoring import (
    WORKER_STATUS_EMPTY,
    WORKER_STATUS_SUCCESS,
    create_scoring_run,
    finalize_scoring_run,
    get_recommendation_freshness_status,
)
from apps.api.services.opportunity_classification import DetectionFamily, OpportunityClassification
from apps.worker.recommendation_scoring import (
    SCORING_VERSION,
    TIER_BLOCKED,
    TIER_HIGH_CONVICTION,
    TIER_REVIEW,
    score_pending_recommendations,
)


class RecommendationScoringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "recommendation-scoring.db"
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

    def test_scores_high_conviction_when_all_evidence_is_strong(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_base_opportunity(
                session,
                event_group_key="event-high",
                fee_adjusted_edge=Decimal("0.1800"),
                min_executable_size=Decimal("150.0000"),
            )
            self._attach_validations(session, opportunity.id)
            self._attach_simulation(
                session,
                opportunity.id,
                fill_completion_ratio=Decimal("1.0000"),
                capital_lock_estimate_hours=Decimal("0.0100"),
                min_executable_size=Decimal("150.0000"),
            )
            self._attach_kpi_snapshot(session, opportunity.id, persistence_duration_seconds=600)
            session.commit()

            persisted = score_pending_recommendations(session)
            score_row = session.scalar(select(RecommendationScore).where(RecommendationScore.opportunity_id == opportunity.id))
            refreshed = session.get(type(opportunity), opportunity.id)

        self.assertEqual(len(persisted), 1)
        assert score_row is not None
        assert refreshed is not None
        self.assertEqual(score_row.tier, TIER_HIGH_CONVICTION)
        self.assertGreaterEqual(score_row.score, Decimal("80.0000"))
        self.assertFalse(score_row.manual_review_required)
        self.assertIsNone(score_row.warning_summary)
        self.assertIn("all critical validations passed", score_row.reason_summary)
        self.assertTrue(refreshed.recommendation_eligibility)
        self.assertIsNone(refreshed.recommendation_block_reason)

    def test_assigns_review_when_non_blocking_warnings_exist(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_base_opportunity(
                session,
                event_group_key="event-review",
                fee_adjusted_edge=Decimal("0.0900"),
                min_executable_size=Decimal("25.0000"),
            )
            self._attach_validations(session, opportunity.id)
            self._attach_simulation(
                session,
                opportunity.id,
                fill_completion_ratio=Decimal("1.0000"),
                capital_lock_estimate_hours=Decimal("0.1000"),
                min_executable_size=Decimal("25.0000"),
            )
            self._attach_kpi_snapshot(session, opportunity.id, persistence_duration_seconds=90)
            session.commit()

            score_pending_recommendations(session)
            score_row = session.scalar(select(RecommendationScore).where(RecommendationScore.opportunity_id == opportunity.id))
            refreshed = session.get(type(opportunity), opportunity.id)

        assert score_row is not None
        assert refreshed is not None
        self.assertEqual(score_row.tier, TIER_REVIEW)
        self.assertTrue(score_row.manual_review_required)
        self.assertIn("low executable size", score_row.warning_summary)
        self.assertIn("weak persistence", score_row.warning_summary)
        self.assertIn("high capital lock", score_row.warning_summary)
        self.assertFalse(refreshed.recommendation_eligibility)
        self.assertEqual(refreshed.recommendation_block_reason, "weak_persistence")

    def test_assigns_blocked_when_hard_gate_fails(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_base_opportunity(
                session,
                event_group_key="event-blocked",
                fee_adjusted_edge=Decimal("0.1500"),
                min_executable_size=Decimal("100.0000"),
            )
            self._attach_validations(session, opportunity.id, resolution_status="risky", resolution_score=Decimal("0.5000"))
            self._attach_simulation(session, opportunity.id)
            self._attach_kpi_snapshot(session, opportunity.id, persistence_duration_seconds=300)
            session.commit()

            score_pending_recommendations(session)
            score_row = session.scalar(select(RecommendationScore).where(RecommendationScore.opportunity_id == opportunity.id))
            refreshed = session.get(type(opportunity), opportunity.id)

        assert score_row is not None
        assert refreshed is not None
        self.assertEqual(score_row.tier, TIER_BLOCKED)
        self.assertIn("resolution risk", score_row.warning_summary)
        self.assertFalse(score_row.manual_review_required)
        self.assertEqual(refreshed.recommendation_block_reason, "resolution_risk")

    def test_blocks_when_required_evidence_is_missing(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_base_opportunity(
                session,
                event_group_key="event-missing",
                fee_adjusted_edge=Decimal("0.2000"),
                min_executable_size=Decimal("100.0000"),
            )
            self._attach_validations(session, opportunity.id)
            session.commit()

            score_pending_recommendations(session)
            score_row = session.scalar(select(RecommendationScore).where(RecommendationScore.opportunity_id == opportunity.id))

        assert score_row is not None
        self.assertEqual(score_row.tier, TIER_BLOCKED)
        self.assertIn("missing simulation metrics", score_row.warning_summary)
        self.assertIn("missing persistence evidence", score_row.warning_summary)

    def test_persists_score_once_per_scoring_version(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_base_opportunity(session, event_group_key="event-once")
            self._attach_validations(session, opportunity.id)
            self._attach_simulation(session, opportunity.id)
            self._attach_kpi_snapshot(session, opportunity.id, persistence_duration_seconds=300)
            session.commit()

            first = score_pending_recommendations(session)
            second = score_pending_recommendations(session)
            stored = session.scalars(select(RecommendationScore).where(RecommendationScore.opportunity_id == opportunity.id)).all()

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].scoring_version, SCORING_VERSION)

    def test_scoring_status_metadata_creation(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_base_opportunity(session, event_group_key="event-status")
            self._attach_validations(session, opportunity.id)
            self._attach_simulation(session, opportunity.id)
            self._attach_kpi_snapshot(session, opportunity.id, persistence_duration_seconds=300)
            started_at = datetime(2026, 4, 24, 11, 0, tzinfo=timezone.utc)
            run = create_scoring_run(session, started_at=started_at, scoring_version=SCORING_VERSION)
            session.commit()

            persisted = score_pending_recommendations(session)
            finalize_scoring_run(
                session,
                run.id,
                finished_at=datetime(2026, 4, 24, 11, 1, tzinfo=timezone.utc),
                worker_status=WORKER_STATUS_SUCCESS,
                opportunities_scored=len(persisted),
                high_conviction_count=sum(1 for row in persisted if row.tier == TIER_HIGH_CONVICTION),
                review_count=sum(1 for row in persisted if row.tier == TIER_REVIEW),
                blocked_count=sum(1 for row in persisted if row.tier == TIER_BLOCKED),
                run_reason=None,
            )
            session.commit()

            stored = session.scalar(select(RecommendationScoringRun).where(RecommendationScoringRun.id == run.id))

        assert stored is not None
        self.assertEqual(stored.worker_status, WORKER_STATUS_SUCCESS)
        self.assertEqual(stored.opportunities_scored, 1)
        self.assertEqual(stored.high_conviction_count, 1)
        self.assertEqual(stored.review_count, 0)
        self.assertEqual(stored.blocked_count, 0)
        self.assertEqual(stored.scoring_version, SCORING_VERSION)

    def test_empty_scoring_run_behavior(self) -> None:
        with self.SessionLocal() as session:
            run = create_scoring_run(
                session,
                started_at=datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc),
                scoring_version=SCORING_VERSION,
            )
            session.commit()

            persisted = score_pending_recommendations(session)
            finalize_scoring_run(
                session,
                run.id,
                finished_at=datetime(2026, 4, 24, 12, 1, tzinfo=timezone.utc),
                worker_status=WORKER_STATUS_EMPTY,
                opportunities_scored=len(persisted),
                high_conviction_count=0,
                review_count=0,
                blocked_count=0,
                run_reason="no_unscored_validated_opportunities",
            )
            session.commit()

            freshness = get_recommendation_freshness_status(session)

        self.assertEqual(freshness.scoring_worker_status, WORKER_STATUS_EMPTY)
        self.assertEqual(freshness.opportunities_scored_last_run, 0)
        self.assertEqual(freshness.run_reason, "no_unscored_validated_opportunities")
        self.assertEqual(freshness.freshness_status, "fresh")

    def test_missing_and_stale_scoring_state_behavior(self) -> None:
        with self.SessionLocal() as session:
            missing = get_recommendation_freshness_status(session)

            opportunity = self._create_base_opportunity(session, event_group_key="event-stale")
            self._attach_validations(session, opportunity.id)
            self._attach_simulation(session, opportunity.id)
            self._attach_kpi_snapshot(session, opportunity.id, persistence_duration_seconds=300)
            run = create_scoring_run(
                session,
                started_at=datetime(2026, 4, 24, 9, 0, tzinfo=timezone.utc),
                scoring_version=SCORING_VERSION,
            )
            session.commit()
            score_pending_recommendations(session)
            finalize_scoring_run(
                session,
                run.id,
                finished_at=datetime(2026, 4, 24, 9, 1, tzinfo=timezone.utc),
                worker_status=WORKER_STATUS_SUCCESS,
                opportunities_scored=1,
                high_conviction_count=1,
                review_count=0,
                blocked_count=0,
                run_reason=None,
            )
            opportunity.validated_at = datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc)
            session.commit()

            stale = get_recommendation_freshness_status(session)

        self.assertEqual(missing.freshness_status, "missing")
        self.assertIn("recommendation_scoring_not_run", missing.stale_reasons)
        self.assertEqual(stale.freshness_status, "stale")
        self.assertIn("validation_newer_than_scoring", stale.stale_reasons)

    def test_ranking_is_deterministic(self) -> None:
        with self.SessionLocal() as session:
            high = self._create_base_opportunity(
                session,
                event_group_key="event-high-rank",
                fee_adjusted_edge=Decimal("0.2000"),
                min_executable_size=Decimal("150.0000"),
            )
            self._attach_validations(session, high.id)
            self._attach_simulation(session, high.id, capital_lock_estimate_hours=Decimal("0.0100"))
            self._attach_kpi_snapshot(session, high.id, persistence_duration_seconds=600)

            review = self._create_base_opportunity(
                session,
                event_group_key="event-review-rank",
                fee_adjusted_edge=Decimal("0.0900"),
                min_executable_size=Decimal("25.0000"),
            )
            self._attach_validations(session, review.id)
            self._attach_simulation(session, review.id, capital_lock_estimate_hours=Decimal("0.1000"), min_executable_size=Decimal("25.0000"))
            self._attach_kpi_snapshot(session, review.id, persistence_duration_seconds=90)

            blocked = self._create_base_opportunity(
                session,
                event_group_key="event-blocked-rank",
                fee_adjusted_edge=Decimal("0.1200"),
                min_executable_size=Decimal("100.0000"),
            )
            self._attach_validations(session, blocked.id, semantic_status="inconclusive", semantic_score=Decimal("0.5000"))
            self._attach_kpi_snapshot(session, blocked.id, persistence_duration_seconds=300)
            session.commit()

            score_pending_recommendations(session)
            ranked = list_ranked_recommendations(session).rows

        self.assertEqual(
            [row["event_id"] for row in ranked],
            ["event-high-rank", "event-review-rank", "event-blocked-rank"],
        )
        self.assertEqual([row["tier"] for row in ranked], [TIER_HIGH_CONVICTION, TIER_REVIEW, TIER_BLOCKED])

    def _create_base_opportunity(
        self,
        session,
        *,
        event_group_key: str,
        fee_adjusted_edge: Decimal = Decimal("0.1500"),
        min_executable_size: Decimal = Decimal("100.0000"),
    ):
        return create_opportunity_extended(
            session,
            detected_at=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
            detection_window_start=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
            event_group_key=event_group_key,
            involved_market_ids=[1, 2],
            opportunity_type="neg_risk_long_yes_bundle",
            outcome_count=2,
            gross_price_sum=Decimal("0.6500"),
            gross_gap=Decimal("0.3500"),
            detector_version="neg_risk_v1",
            classification=OpportunityClassification(family=DetectionFamily.NEG_RISK_CONVERSION),
            s_logic=Decimal("1.0000"),
            fee_adjusted_edge=fee_adjusted_edge,
            min_executable_size=min_executable_size,
            validation_status="valid",
            validation_version="executable_edge_validation_v1",
            simulation_version="simulation_validation_v1",
        )

    def _attach_validations(
        self,
        session,
        opportunity_id: int,
        *,
        rule_status: str = "valid",
        rule_score: Decimal | None = Decimal("1.0000"),
        semantic_status: str = "valid",
        semantic_score: Decimal | None = Decimal("1.0000"),
        resolution_status: str = "valid",
        resolution_score: Decimal | None = Decimal("1.0000"),
        executable_status: str = "valid",
        executable_score: Decimal | None = Decimal("1.0000"),
        simulation_status: str = "valid",
        simulation_score: Decimal | None = Decimal("1.0000"),
    ) -> None:
        attach_validation_result(
            session,
            opportunity_id,
            validation_type="rule_based_relation",
            status=rule_status,
            score=rule_score,
            summary=f"{rule_status}: rule",
            details_json={"reason_code": "formal_relation_verified"},
            validator_version="rule_based_relation_v1",
            created_at=datetime(2026, 4, 24, 10, 1, tzinfo=timezone.utc),
        )
        attach_validation_result(
            session,
            opportunity_id,
            validation_type="semantic_validation",
            status=semantic_status,
            score=semantic_score,
            summary=f"{semantic_status}: semantic",
            details_json={"reason_code": "semantic_alignment_verified"},
            validator_version="semantic_validation_v1",
            created_at=datetime(2026, 4, 24, 10, 2, tzinfo=timezone.utc),
        )
        attach_validation_result(
            session,
            opportunity_id,
            validation_type="resolution_validation",
            status=resolution_status,
            score=resolution_score,
            summary=f"{resolution_status}: resolution",
            details_json={"reason_code": "resolution_alignment_verified" if resolution_status == "valid" else "dispute_flag_present"},
            validator_version="resolution_validation_v1",
            created_at=datetime(2026, 4, 24, 10, 3, tzinfo=timezone.utc),
        )
        attach_validation_result(
            session,
            opportunity_id,
            validation_type="executable_edge_validation",
            status=executable_status,
            score=executable_score,
            summary=f"{executable_status}: executable",
            details_json={"reason_code": "positive_fee_adjusted_edge"},
            validator_version="executable_edge_validation_v1",
            created_at=datetime(2026, 4, 24, 10, 4, tzinfo=timezone.utc),
        )
        attach_validation_result(
            session,
            opportunity_id,
            validation_type="simulation_validation",
            status=simulation_status,
            score=simulation_score,
            summary=f"{simulation_status}: simulation",
            details_json={"reason_code": "full_sequential_execution_verified"},
            validator_version="simulation_validation_v1",
            created_at=datetime(2026, 4, 24, 10, 5, tzinfo=timezone.utc),
        )

    def _attach_simulation(
        self,
        session,
        opportunity_id: int,
        *,
        fill_completion_ratio: Decimal = Decimal("1.0000"),
        capital_lock_estimate_hours: Decimal = Decimal("0.0100"),
        min_executable_size: Decimal = Decimal("100.0000"),
    ) -> None:
        attach_simulation_result(
            session,
            opportunity_id,
            simulation_mode="simulation_validation",
            simulation_version="simulation_validation_v1",
            executable_edge=Decimal("0.1800"),
            estimated_fill_quality=fill_completion_ratio,
            fill_completion_ratio=fill_completion_ratio,
            execution_feasible=fill_completion_ratio == Decimal("1.0000"),
            min_executable_size=min_executable_size,
            persistence_seconds_estimate=30,
            capital_lock_estimate_hours=capital_lock_estimate_hours,
            execution_risk_flag="none",
            details_json={"reason_code": "full_sequential_execution_verified"},
            created_at=datetime(2026, 4, 24, 10, 6, tzinfo=timezone.utc),
        )

    def _attach_kpi_snapshot(
        self,
        session,
        opportunity_id: int,
        *,
        persistence_duration_seconds: int,
        decay_status: str = "alive",
    ) -> None:
        run_summary = KpiRunSummary(
            created_at=datetime(2026, 4, 24, 10, 7, tzinfo=timezone.utc),
            run_started_at=datetime(2026, 4, 24, 10, 6, tzinfo=timezone.utc),
            run_completed_at=datetime(2026, 4, 24, 10, 7, tzinfo=timezone.utc),
            kpi_version="kpi_v2",
            total_opportunities=1,
            valid_after_rule=1,
            valid_after_semantic=1,
            valid_after_resolution=1,
            valid_after_executable=1,
            valid_after_simulation=1,
            avg_executable_edge=Decimal("0.1800"),
            avg_fill_ratio=Decimal("1.0000"),
            avg_capital_lock=Decimal("0.0100"),
            false_positive_rate=Decimal("0.0000"),
            family_distribution={DetectionFamily.NEG_RISK_CONVERSION.value: 1},
            detector_versions_json=["neg_risk_v1"],
            validation_versions_json=["executable_edge_validation_v1"],
            simulation_versions_json=["simulation_validation_v1"],
            raw_context={"source": "test"},
        )
        session.add(run_summary)
        session.flush()
        session.add(
            OpportunityKpiSnapshot(
                run_summary_id=run_summary.id,
                opportunity_id=opportunity_id,
                lineage_key=f"lineage-{opportunity_id}",
                kpi_version="kpi_v2",
                snapshot_timestamp=datetime(2026, 4, 24, 10, 7, tzinfo=timezone.utc),
                family=DetectionFamily.NEG_RISK_CONVERSION.value,
                validation_stage_reached="simulation_pass",
                final_status="valid",
                rejection_stage=None,
                rejection_reason=None,
                detected=True,
                rule_pass=True,
                semantic_pass=True,
                resolution_pass=True,
                executable_pass=True,
                simulation_pass=True,
                s_logic=Decimal("1.0000"),
                s_sem=Decimal("1.0000"),
                s_res=Decimal("1.0000"),
                top_of_book_edge=Decimal("0.2000"),
                depth_weighted_edge=Decimal("0.1900"),
                fee_adjusted_edge=Decimal("0.1800"),
                fill_completion_ratio=Decimal("1.0000"),
                execution_feasible=True,
                capital_lock_estimate_hours=Decimal("0.0100"),
                detector_version="neg_risk_v1",
                validation_version="executable_edge_validation_v1",
                simulation_version="simulation_validation_v1",
                first_seen_timestamp=datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc),
                last_seen_timestamp=datetime(2026, 4, 24, 10, 7, tzinfo=timezone.utc),
                persistence_duration_seconds=persistence_duration_seconds,
                decay_status=decay_status,
                raw_context={"source": "test"},
            )
        )


if __name__ == "__main__":
    unittest.main()
