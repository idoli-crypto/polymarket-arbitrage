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
from apps.api.db.models import (
    DetectedOpportunity,
    ExecutionSimulation,
    KpiRunSummary,
    KpiSnapshot,
    Market,
    MarketSnapshot,
    OpportunityKpiSnapshot,
    RecommendationScore,
    RecommendationScoringRun,
    SimulationResult,
    ValidationResult,
)
from apps.api.db.session import get_db_session
from apps.api.main import create_app
from apps.api.services.opportunity_classification import DetectionFamily


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
        self.assertEqual(payload[0]["family"], DetectionFamily.NEG_RISK_CONVERSION.value)
        self.assertFalse(payload[0]["recommendation_eligibility"])
        self.assertEqual(payload[1]["event_id"], "event-1")
        self.assertEqual(payload[1]["simulation_status"], "executable")
        self.assertEqual(payload[1]["real_edge"], "37.0000")
        self.assertEqual(payload[1]["fill_ratio"], "1.0000")
        self.assertEqual(payload[1]["intended_size"], "100.0000")
        self.assertEqual(payload[1]["executable_size"], "100.0000")
        self.assertIn("confidence_tier", payload[1])
        self.assertIn("top_of_book_edge", payload[1])
        self.assertIn("min_executable_size", payload[1])
        self.assertIn("suggested_notional_bucket", payload[1])
        self.assertIn("risk_flags_json", payload[1])

    def test_opportunity_detail_endpoint_returns_extended_context_and_preserves_summary_fields(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-9",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 40, tzinfo=timezone.utc),
            )
            session.flush()
            opportunity.validation_version = "semantic_validation_v1"
            opportunity.simulation_version = "simulation_validation_v1"
            opportunity.question_texts_json = ["Will Alice win?", "Will Bob win?"]
            opportunity.normalized_entities_json = {
                "1": [{"slot": 0, "canonical": "alice", "kind": "person"}],
                "2": [{"slot": 0, "canonical": "bob", "kind": "person"}],
            }
            opportunity.normalized_dates_json = {
                "1": [{"canonical": "2026-12-31", "granularity": "day"}],
                "2": [{"canonical": "2026-12-31", "granularity": "day"}],
            }
            opportunity.normalized_thresholds_json = {"1": [], "2": []}
            opportunity.s_sem = Decimal("1.0000")
            opportunity.top_of_book_edge = Decimal("0.3700")
            opportunity.fee_adjusted_edge = Decimal("0.3000")
            opportunity.min_executable_size = Decimal("75.0000")
            opportunity.risk_flags_json = [{"type": "validation", "flag": "none"}]
            opportunity.recommendation_eligibility = False
            opportunity.recommendation_block_reason = "scoring_not_run"
            opportunity.raw_context = {
                "semantic_validation_status": "valid",
                "semantic_validation_summary": "valid: semantic alignment verified",
                "semantic_validation_details": {
                    "reason_code": "semantic_alignment_verified",
                    "checks": [{"name": "semantic_template", "status": "valid"}],
                },
                "semantic_normalized_markets": {
                    "1": {"semantic_template": "will <entity_0> win?"},
                    "2": {"semantic_template": "will <entity_0> win?"},
                },
            }
            session.add(
                ValidationResult(
                    opportunity_id=opportunity.id,
                    validation_type="semantic_validation",
                    status="valid",
                    score=Decimal("1.0000"),
                    summary="valid: semantic alignment verified",
                    details_json={
                        "reason_code": "semantic_alignment_verified",
                        "normalized_markets": {
                            "1": {"semantic_template": "will <entity_0> win?"},
                            "2": {"semantic_template": "will <entity_0> win?"},
                        },
                        "checks": [{"name": "semantic_template", "status": "valid"}],
                    },
                    validator_version="semantic_validation_v1",
                    created_at=datetime(2026, 4, 21, 12, 41, tzinfo=timezone.utc),
                )
            )
            session.add(
                SimulationResult(
                    opportunity_id=opportunity.id,
                    simulation_mode="simulation_validation",
                    executable_edge=Decimal("0.3000"),
                    fee_cost=Decimal("0.0636"),
                    slippage_cost=Decimal("0.0000"),
                    estimated_fill_quality=Decimal("0.7500"),
                    fill_completion_ratio=Decimal("0.7500"),
                    execution_feasible=False,
                    min_executable_size=Decimal("75.0000"),
                    suggested_notional_bucket=None,
                    persistence_seconds_estimate=25,
                    capital_lock_estimate_hours=Decimal("0.0069"),
                    execution_risk_flag="partial_fill_risk",
                    simulation_version="simulation_validation_v1",
                    details_json={"path": "latest", "reason_code": "partial_fill_detected"},
                    created_at=datetime(2026, 4, 21, 12, 42, tzinfo=timezone.utc),
                )
            )
            session.add(
                RecommendationScore(
                    opportunity_id=opportunity.id,
                    score=None,
                    tier=None,
                    reason_summary=None,
                    warning_summary=None,
                    manual_review_required=False,
                    scoring_version="placeholder_v1",
                    created_at=datetime(2026, 4, 21, 12, 43, tzinfo=timezone.utc),
                )
            )
            session.add(
                ExecutionSimulation(
                    opportunity_id=opportunity.id,
                    simulated_at=datetime(2026, 4, 21, 12, 44, tzinfo=timezone.utc),
                    simulation_status="partially_executable",
                    intended_size_usd=Decimal("100.0000"),
                    executable_size_usd=Decimal("75.0000"),
                    gross_cost_usd=Decimal("45.0000"),
                    gross_payout_usd=Decimal("75.0000"),
                    estimated_fees_usd=Decimal("0.0000"),
                    estimated_slippage_usd=Decimal("0.0000"),
                    estimated_net_edge_usd=Decimal("30.0000"),
                    fill_completion_ratio=Decimal("0.7500"),
                    simulation_reason="partially_executable",
                )
            )
            session.commit()

        response = self.client.get(f"/opportunities/{opportunity.id}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["opportunity_id"], opportunity.id)
        self.assertEqual(payload["family"], DetectionFamily.NEG_RISK_CONVERSION.value)
        self.assertEqual(payload["validation_version"], "semantic_validation_v1")
        self.assertEqual(payload["simulation_version"], "simulation_validation_v1")
        self.assertEqual(payload["question_texts_json"], ["Will Alice win?", "Will Bob win?"])
        self.assertEqual(payload["normalized_entities_json"]["1"][0]["canonical"], "alice")
        self.assertEqual(payload["normalized_dates_json"]["1"][0]["canonical"], "2026-12-31")
        self.assertEqual(payload["s_sem"], "1.0000")
        self.assertEqual(payload["top_of_book_edge"], "0.3700")
        self.assertEqual(payload["fee_adjusted_edge"], "0.3000")
        self.assertEqual(payload["validation_results"][0]["validation_type"], "semantic_validation")
        self.assertEqual(payload["validation_results"][0]["details_json"]["reason_code"], "semantic_alignment_verified")
        self.assertEqual(payload["simulation_results"][0]["simulation_mode"], "simulation_validation")
        self.assertEqual(payload["simulation_results"][0]["fill_completion_ratio"], "0.7500")
        self.assertFalse(payload["simulation_results"][0]["execution_feasible"])
        self.assertEqual(payload["simulation_results"][0]["capital_lock_estimate_hours"], "0.0069")
        self.assertEqual(payload["simulation_results"][0]["execution_risk_flag"], "partial_fill_risk")
        self.assertEqual(payload["scores"][0]["scoring_version"], "placeholder_v1")
        self.assertEqual(payload["simulation_status"], "partially_executable")
        self.assertEqual(payload["real_edge"], "30.0000")

    def test_recommendations_endpoint_returns_ranked_queue(self) -> None:
        with self.SessionLocal() as session:
            high = self._create_opportunity(
                session,
                event_group_key="event-rec-high",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
            )
            review = self._create_opportunity(
                session,
                event_group_key="event-rec-review",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
            )
            blocked = self._create_opportunity(
                session,
                event_group_key="event-rec-blocked",
                validation_status="rejected",
                detected_at=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
            )
            session.flush()
            high.fee_adjusted_edge = Decimal("0.1800")
            high.min_executable_size = Decimal("150.0000")
            high.persistence_seconds_estimate = 300
            review.fee_adjusted_edge = Decimal("0.0900")
            review.min_executable_size = Decimal("25.0000")
            review.persistence_seconds_estimate = 90
            blocked.fee_adjusted_edge = Decimal("0.1200")
            blocked.min_executable_size = Decimal("100.0000")
            blocked.persistence_seconds_estimate = 180
            high.recommendation_eligibility = True
            review.recommendation_eligibility = False
            review.recommendation_block_reason = "weak_persistence"
            blocked.recommendation_eligibility = False
            blocked.recommendation_block_reason = "resolution_risk"
            session.add_all(
                [
                    RecommendationScore(
                        opportunity_id=high.id,
                        score=Decimal("91.0000"),
                        tier="high_conviction",
                        reason_summary="all critical validations passed",
                        warning_summary=None,
                        manual_review_required=False,
                        scoring_version="recommendation_scoring_v1",
                        created_at=datetime(2026, 4, 21, 12, 33, tzinfo=timezone.utc),
                    ),
                    RecommendationScore(
                        opportunity_id=review.id,
                        score=Decimal("68.0000"),
                        tier="review",
                        reason_summary="all critical validations passed",
                        warning_summary="weak persistence",
                        manual_review_required=True,
                        scoring_version="recommendation_scoring_v1",
                        created_at=datetime(2026, 4, 21, 12, 34, tzinfo=timezone.utc),
                    ),
                    RecommendationScore(
                        opportunity_id=blocked.id,
                        score=Decimal("18.0000"),
                        tier="blocked",
                        reason_summary="persisted evidence retained for audit",
                        warning_summary="resolution risk",
                        manual_review_required=False,
                        scoring_version="recommendation_scoring_v1",
                        created_at=datetime(2026, 4, 21, 12, 35, tzinfo=timezone.utc),
                    ),
                ]
            )
            session.commit()

        response = self.client.get("/recommendations")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            [row["event_id"] for row in payload],
            ["event-rec-high", "event-rec-review", "event-rec-blocked"],
        )
        self.assertEqual(
            [row["tier"] for row in payload],
            ["high_conviction", "review", "blocked"],
        )
        self.assertEqual([row["ranking_position"] for row in payload], [1, 2, 3])
        self.assertEqual(payload[0]["score"], "91.0000")
        self.assertFalse(payload[0]["manual_review_required"])
        self.assertTrue(payload[0]["recommendation_eligibility"])
        self.assertEqual(payload[0]["freshness_status"], "missing")
        self.assertIn("recommendation_scoring_not_run", payload[0]["stale_reasons"])
        self.assertEqual(payload[0]["capital_lock_estimate"], payload[0]["capital_lock_estimate_hours"])
        self.assertIn("fee_adjusted_edge", payload[0]["executable_edge"])
        self.assertEqual(payload[1]["warning_summary"], "weak persistence")
        self.assertTrue(payload[1]["manual_review_required"])
        self.assertEqual(payload[2]["recommendation_block_reason"], "resolution_risk")

    def test_recommendations_endpoint_supports_filters_sorting_and_pagination(self) -> None:
        with self.SessionLocal() as session:
            for index, (event_id, family, tier, score, edge, detected_at) in enumerate(
                [
                    (
                        "event-page-1",
                        DetectionFamily.NEG_RISK_CONVERSION.value,
                        "high_conviction",
                        Decimal("70.0000"),
                        Decimal("0.1100"),
                        datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
                    ),
                    (
                        "event-page-2",
                        DetectionFamily.CROSS_MARKET_LOGIC.value,
                        "review",
                        Decimal("88.0000"),
                        Decimal("0.2100"),
                        datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                    ),
                    (
                        "event-page-3",
                        DetectionFamily.NEG_RISK_CONVERSION.value,
                        "high_conviction",
                        Decimal("92.0000"),
                        Decimal("0.1800"),
                        datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
                    ),
                ]
            ):
                opportunity = self._create_opportunity(
                    session,
                    event_group_key=event_id,
                    validation_status="valid",
                    detected_at=detected_at,
                )
                session.flush()
                opportunity.family = family
                opportunity.fee_adjusted_edge = edge
                opportunity.confidence_tier = "high" if index != 1 else "medium"
                opportunity.recommendation_eligibility = tier == "high_conviction"
                session.add(
                    RecommendationScore(
                        opportunity_id=opportunity.id,
                        score=score,
                        tier=tier,
                        reason_summary=f"summary-{event_id}",
                        warning_summary=None if tier == "high_conviction" else f"warning-{event_id}",
                        manual_review_required=tier == "review",
                        scoring_version="recommendation_scoring_v1",
                        created_at=detected_at,
                    )
                )
            session.commit()

        response = self.client.get(
            "/recommendations",
            params={
                "tier": "high_conviction",
                "family": DetectionFamily.NEG_RISK_CONVERSION.value,
                "min_score": "80",
                "sort": "recency",
                "limit": 1,
                "offset": 0,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Total-Count"], "1")
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["event_id"], "event-page-3")
        self.assertEqual(payload[0]["ranking_position"], 1)

        page_two_response = self.client.get(
            "/recommendations",
            params={"sort": "score", "limit": 1, "offset": 1},
        )

        self.assertEqual(page_two_response.status_code, 200)
        self.assertEqual(page_two_response.headers["X-Total-Count"], "3")
        page_two_payload = page_two_response.json()
        self.assertEqual(len(page_two_payload), 1)
        self.assertEqual(page_two_payload[0]["event_id"], "event-page-2")
        self.assertEqual(page_two_payload[0]["ranking_position"], 2)

    def test_recommendation_detail_endpoint_returns_full_evidence_and_kpi_snapshot(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-rec-detail",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 50, tzinfo=timezone.utc),
            )
            session.flush()
            opportunity.validation_version = "validation_v2"
            opportunity.simulation_version = "simulation_v2"
            opportunity.confidence_tier = "high"
            opportunity.fee_adjusted_edge = Decimal("0.2500")
            opportunity.depth_weighted_edge = Decimal("0.2600")
            opportunity.min_executable_size = Decimal("80.0000")
            opportunity.suggested_notional_bucket = "standard"
            opportunity.persistence_seconds_estimate = 180
            opportunity.capital_lock_estimate_hours = Decimal("0.0200")
            opportunity.raw_context = {
                "rule_validation_status": "valid",
                "rule_validation_summary": "rule check passed",
                "rule_validation_details": {"checks": [{"name": "relation", "status": "valid"}]},
                "semantic_validation_status": "valid",
                "semantic_validation_summary": "semantic check passed",
                "semantic_validation_details": {"checks": [{"name": "template", "status": "valid"}]},
                "resolution_validation_status": "risky",
                "resolution_validation_summary": "resolution review required",
                "resolution_validation_details": {"checks": [{"name": "source_overlap", "status": "risky"}]},
            }
            session.add_all(
                [
                    ValidationResult(
                        opportunity_id=opportunity.id,
                        validation_type="rule_validation",
                        status="valid",
                        score=Decimal("1.0000"),
                        summary="rule check passed",
                        details_json={"checks": [{"name": "relation", "status": "valid"}]},
                        validator_version="rule_v1",
                        created_at=datetime(2026, 4, 21, 12, 51, tzinfo=timezone.utc),
                    ),
                    ValidationResult(
                        opportunity_id=opportunity.id,
                        validation_type="semantic_validation",
                        status="valid",
                        score=Decimal("1.0000"),
                        summary="semantic check passed",
                        details_json={"checks": [{"name": "template", "status": "valid"}]},
                        validator_version="semantic_v1",
                        created_at=datetime(2026, 4, 21, 12, 52, tzinfo=timezone.utc),
                    ),
                    ValidationResult(
                        opportunity_id=opportunity.id,
                        validation_type="resolution_validation",
                        status="risky",
                        score=Decimal("0.4000"),
                        summary="resolution review required",
                        details_json={"checks": [{"name": "source_overlap", "status": "risky"}]},
                        validator_version="resolution_v1",
                        created_at=datetime(2026, 4, 21, 12, 53, tzinfo=timezone.utc),
                    ),
                    SimulationResult(
                        opportunity_id=opportunity.id,
                        simulation_mode="simulation_validation",
                        executable_edge=Decimal("0.2500"),
                        fee_cost=Decimal("0.0100"),
                        slippage_cost=Decimal("0.0200"),
                        estimated_fill_quality=Decimal("0.9000"),
                        fill_completion_ratio=Decimal("0.8000"),
                        execution_feasible=True,
                        min_executable_size=Decimal("80.0000"),
                        suggested_notional_bucket="standard",
                        persistence_seconds_estimate=180,
                        capital_lock_estimate_hours=Decimal("0.0200"),
                        execution_risk_flag=None,
                        simulation_version="simulation_v2",
                        details_json={"reason_code": "stable_depth"},
                        created_at=datetime(2026, 4, 21, 12, 54, tzinfo=timezone.utc),
                    ),
                    RecommendationScore(
                        opportunity_id=opportunity.id,
                        score=Decimal("93.0000"),
                        tier="high_conviction",
                        reason_summary="strong edge and evidence",
                        warning_summary=None,
                        manual_review_required=False,
                        scoring_version="recommendation_scoring_v1",
                        created_at=datetime(2026, 4, 21, 12, 55, tzinfo=timezone.utc),
                    ),
                    ExecutionSimulation(
                        opportunity_id=opportunity.id,
                        simulated_at=datetime(2026, 4, 21, 12, 56, tzinfo=timezone.utc),
                        simulation_status="executable",
                        intended_size_usd=Decimal("100.0000"),
                        executable_size_usd=Decimal("80.0000"),
                        gross_cost_usd=Decimal("62.0000"),
                        gross_payout_usd=Decimal("100.0000"),
                        estimated_fees_usd=Decimal("1.0000"),
                        estimated_slippage_usd=Decimal("2.0000"),
                        estimated_net_edge_usd=Decimal("35.0000"),
                        fill_completion_ratio=Decimal("0.8000"),
                        simulation_reason="executable",
                        raw_context={"path": "latest"},
                    ),
                ]
            )
            run_summary = KpiRunSummary(
                created_at=datetime(2026, 4, 21, 12, 57, tzinfo=timezone.utc),
                run_started_at=datetime(2026, 4, 21, 12, 56, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 12, 57, tzinfo=timezone.utc),
                kpi_version="kpi_v2",
                total_opportunities=1,
                valid_after_rule=1,
                valid_after_semantic=1,
                valid_after_resolution=0,
                valid_after_executable=0,
                valid_after_simulation=0,
                avg_executable_edge=Decimal("0.2500"),
                avg_fill_ratio=Decimal("0.8000"),
                avg_capital_lock=Decimal("0.0200"),
                false_positive_rate=Decimal("0.0000"),
                family_distribution={DetectionFamily.NEG_RISK_CONVERSION.value: 1},
                detector_versions_json=["neg_risk_v1"],
                validation_versions_json=["validation_v2"],
                simulation_versions_json=["simulation_v2"],
                raw_context={"source": "test"},
            )
            session.add(run_summary)
            session.flush()
            session.add(
                OpportunityKpiSnapshot(
                    run_summary_id=run_summary.id,
                    opportunity_id=opportunity.id,
                    lineage_key="lineage-rec-detail",
                    kpi_version="kpi_v2",
                    snapshot_timestamp=datetime(2026, 4, 21, 12, 57, tzinfo=timezone.utc),
                    created_at=datetime(2026, 4, 21, 12, 57, tzinfo=timezone.utc),
                    family=DetectionFamily.NEG_RISK_CONVERSION.value,
                    validation_stage_reached="resolution_review",
                    final_status="rejected",
                    rejection_stage="resolution",
                    rejection_reason="resolution review required",
                    detected=True,
                    rule_pass=True,
                    semantic_pass=True,
                    resolution_pass=False,
                    executable_pass=False,
                    simulation_pass=False,
                    s_logic=Decimal("1.0000"),
                    s_sem=Decimal("1.0000"),
                    s_res=Decimal("0.4000"),
                    top_of_book_edge=Decimal("0.2700"),
                    depth_weighted_edge=Decimal("0.2600"),
                    fee_adjusted_edge=Decimal("0.2500"),
                    fill_completion_ratio=Decimal("0.8000"),
                    execution_feasible=True,
                    capital_lock_estimate_hours=Decimal("0.0200"),
                    detector_version="neg_risk_v1",
                    validation_version="validation_v2",
                    simulation_version="simulation_v2",
                    first_seen_timestamp=datetime(2026, 4, 21, 12, 50, tzinfo=timezone.utc),
                    last_seen_timestamp=datetime(2026, 4, 21, 12, 57, tzinfo=timezone.utc),
                    persistence_duration_seconds=420,
                    decay_status="alive",
                    raw_context={"audit": "snapshot"},
                )
            )
            session.add(
                RecommendationScoringRun(
                    started_at=datetime(2026, 4, 21, 12, 55, tzinfo=timezone.utc),
                    finished_at=datetime(2026, 4, 21, 12, 58, tzinfo=timezone.utc),
                    worker_status="success",
                    opportunities_scored=1,
                    high_conviction_count=1,
                    review_count=0,
                    blocked_count=0,
                    scoring_version="recommendation_scoring_v1",
                    run_reason=None,
                )
            )
            session.commit()

        response = self.client.get(f"/recommendations/{opportunity.id}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["summary"]["opportunity_id"], opportunity.id)
        self.assertEqual(payload["summary"]["tier"], "high_conviction")
        self.assertEqual(payload["summary"]["freshness_status"], "fresh")
        self.assertEqual(payload["validation_evidence"]["rule_validation"]["status"], "valid")
        self.assertEqual(payload["validation_evidence"]["semantic_validation"]["status"], "valid")
        self.assertEqual(payload["validation_evidence"]["resolution_validation"]["status"], "risky")
        self.assertEqual(payload["executable_edge"]["fee_adjusted_edge"], "0.2500")
        self.assertEqual(payload["latest_execution_simulation"]["simulation_status"], "executable")
        self.assertEqual(payload["simulation_results"][0]["simulation_mode"], "simulation_validation")
        self.assertEqual(payload["kpi_snapshot"]["lineage_key"], "lineage-rec-detail")
        self.assertEqual(payload["audit"]["raw_context"]["resolution_validation_status"], "risky")

    def test_recommendation_status_endpoint_exposes_scoring_freshness(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-status",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
            )
            opportunity.validated_at = datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc)
            session.flush()
            run_summary = KpiRunSummary(
                created_at=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
                run_started_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
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
                validation_versions_json=["validation_v1"],
                simulation_versions_json=["simulation_v1"],
                raw_context={"source": "test"},
            )
            session.add(run_summary)
            session.flush()
            session.add(
                OpportunityKpiSnapshot(
                    run_summary_id=run_summary.id,
                    opportunity_id=opportunity.id,
                    lineage_key="lineage-status",
                    kpi_version="kpi_v2",
                    created_at=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
                    snapshot_timestamp=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
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
                    validation_version="validation_v1",
                    simulation_version="simulation_v1",
                    first_seen_timestamp=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
                    last_seen_timestamp=datetime(2026, 4, 21, 12, 32, tzinfo=timezone.utc),
                    persistence_duration_seconds=120,
                    decay_status="alive",
                    raw_context={"source": "test"},
                )
            )
            session.add(
                RecommendationScoringRun(
                    started_at=datetime(2026, 4, 21, 12, 33, tzinfo=timezone.utc),
                    finished_at=datetime(2026, 4, 21, 12, 34, tzinfo=timezone.utc),
                    worker_status="success",
                    opportunities_scored=3,
                    high_conviction_count=1,
                    review_count=1,
                    blocked_count=1,
                    scoring_version="recommendation_scoring_v1",
                    run_reason=None,
                )
            )
            session.commit()

        response = self.client.get("/recommendations/status")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["scoring_worker_status"], "success")
        self.assertEqual(payload["opportunities_scored_last_run"], 3)
        self.assertEqual(payload["high_conviction_last_run"], 1)
        self.assertEqual(payload["review_last_run"], 1)
        self.assertEqual(payload["blocked_last_run"], 1)
        self.assertEqual(payload["freshness_status"], "fresh")
        self.assertEqual(payload["stale_reasons"], [])
        self.assertEqual(payload["scoring_version"], "recommendation_scoring_v1")

    def test_recommendation_status_endpoint_reports_missing_and_stale_state(self) -> None:
        missing_response = self.client.get("/recommendations/status")
        self.assertEqual(missing_response.status_code, 200)
        self.assertEqual(missing_response.json()["freshness_status"], "missing")
        self.assertIn("recommendation_scoring_not_run", missing_response.json()["stale_reasons"])

        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-stale-status",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 40, tzinfo=timezone.utc),
            )
            opportunity.validated_at = datetime(2026, 4, 21, 12, 45, tzinfo=timezone.utc)
            session.add(
                RecommendationScoringRun(
                    started_at=datetime(2026, 4, 21, 12, 41, tzinfo=timezone.utc),
                    finished_at=datetime(2026, 4, 21, 12, 42, tzinfo=timezone.utc),
                    worker_status="empty",
                    opportunities_scored=0,
                    high_conviction_count=0,
                    review_count=0,
                    blocked_count=0,
                    scoring_version="recommendation_scoring_v1",
                    run_reason="no_unscored_validated_opportunities",
                )
            )
            session.commit()

        stale_response = self.client.get("/recommendations/status")

        self.assertEqual(stale_response.status_code, 200)
        stale_payload = stale_response.json()
        self.assertEqual(stale_payload["freshness_status"], "stale")
        self.assertIn("validation_newer_than_scoring", stale_payload["stale_reasons"])
        self.assertEqual(stale_payload["scoring_worker_status"], "empty")
        self.assertEqual(stale_payload["run_reason"], "no_unscored_validated_opportunities")

    def test_opportunity_detail_endpoint_exposes_rule_validation_evidence(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-10",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 45, tzinfo=timezone.utc),
            )
            session.flush()
            opportunity.validation_version = "rule_based_relation_v1"
            opportunity.raw_context = {
                "rule_validation_status": "valid",
                "rule_validation_summary": "validated temporal_nesting relation",
            }
            session.add(
                ValidationResult(
                    opportunity_id=opportunity.id,
                    validation_type="rule_based_relation",
                    status="valid",
                    score=Decimal("1.0000"),
                    summary="validated temporal_nesting relation",
                    details_json={
                        "relation_type": "temporal_nesting",
                        "reason_code": "temporal_nesting_verified",
                        "evidence": {
                            "formal_claim": {
                                "kind": "temporal_nesting",
                                "source_market_ids": [1],
                                "target_market_ids": [2],
                            }
                        },
                    },
                    validator_version="rule_based_relation_v1",
                    created_at=datetime(2026, 4, 21, 12, 46, tzinfo=timezone.utc),
                )
            )
            session.commit()

        response = self.client.get(f"/opportunities/{opportunity.id}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["validation_version"], "rule_based_relation_v1")
        self.assertEqual(payload["validation_results"][0]["validation_type"], "rule_based_relation")
        self.assertEqual(payload["validation_results"][0]["status"], "valid")
        self.assertEqual(
            payload["validation_results"][0]["details_json"]["reason_code"],
            "temporal_nesting_verified",
        )
        self.assertEqual(
            payload["validation_results"][0]["details_json"]["evidence"]["formal_claim"]["kind"],
            "temporal_nesting",
        )

    def test_opportunity_detail_endpoint_exposes_semantic_validation_evidence(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-11",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 47, tzinfo=timezone.utc),
            )
            session.flush()
            opportunity.validation_version = "semantic_validation_v1"
            opportunity.raw_context = {
                "semantic_validation_status": "valid",
                "semantic_validation_summary": "valid: semantic alignment verified",
                "semantic_validation_details": {
                    "reason_code": "semantic_alignment_verified",
                    "checks": [{"name": "semantic_template", "status": "valid"}],
                },
                "semantic_normalized_markets": {
                    "1": {
                        "semantic_template": "will <entity_0> win by <date_0>?",
                        "dates": [{"canonical": "2026-12-31", "granularity": "day"}],
                    },
                    "2": {
                        "semantic_template": "will <entity_0> win by <date_0>?",
                        "dates": [{"canonical": "2026-12-31", "granularity": "day"}],
                    },
                },
            }
            session.add(
                ValidationResult(
                    opportunity_id=opportunity.id,
                    validation_type="semantic_validation",
                    status="valid",
                    score=Decimal("1.0000"),
                    summary="valid: semantic alignment verified",
                    details_json={
                        "reason_code": "semantic_alignment_verified",
                        "normalized_markets": {
                            "1": {"semantic_template": "will <entity_0> win by <date_0>?"},
                            "2": {"semantic_template": "will <entity_0> win by <date_0>?"},
                        },
                        "checks": [{"name": "dates", "status": "valid"}],
                    },
                    validator_version="semantic_validation_v1",
                    created_at=datetime(2026, 4, 21, 12, 48, tzinfo=timezone.utc),
                )
            )
            session.commit()

        response = self.client.get(f"/opportunities/{opportunity.id}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["validation_version"], "semantic_validation_v1")
        self.assertEqual(payload["raw_context"]["semantic_validation_status"], "valid")
        self.assertEqual(payload["raw_context"]["semantic_validation_details"]["reason_code"], "semantic_alignment_verified")
        self.assertEqual(payload["validation_results"][0]["validation_type"], "semantic_validation")
        self.assertEqual(payload["validation_results"][0]["details_json"]["checks"][0]["name"], "dates")

    def test_opportunity_detail_endpoint_exposes_resolution_validation_evidence(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-12",
                validation_status="rejected",
                detected_at=datetime(2026, 4, 21, 12, 49, tzinfo=timezone.utc),
            )
            session.flush()
            opportunity.validation_version = "resolution_validation_v1"
            opportunity.validation_reason = "dispute_flag_present"
            opportunity.resolution_sources_json = {
                "1": {"value": "official-results", "evidence_path": "resolutionSource"},
                "2": {"value": "official-results", "evidence_path": "resolutionSource"},
            }
            opportunity.end_dates_json = {
                "1": {"value": "2026-12-31T00:00:00Z", "evidence_path": "endDate"},
                "2": {"value": "2026-12-31T00:00:00Z", "evidence_path": "endDate"},
            }
            opportunity.clarification_flags_json = {
                "1": [{"flag": "clarificationFlags", "value": {"late_fill": "ignore"}, "evidence_path": "clarificationFlags"}],
                "2": [{"flag": "clarificationFlags", "value": {"late_fill": "ignore"}, "evidence_path": "clarificationFlags"}],
            }
            opportunity.dispute_flags_json = {
                "1": [{"flag": "isDisputed", "value": False, "evidence_path": "isDisputed"}],
                "2": [{"flag": "umaResolutionStatus", "value": "disputed", "evidence_path": "umaResolutionStatus"}],
            }
            opportunity.s_res = Decimal("0.5000")
            opportunity.raw_context = {
                "resolution_validation_status": "risky",
                "resolution_validation_summary": "risky: dispute_flag_present",
                "resolution_validation_details": {
                    "reason_code": "dispute_flag_present",
                    "checks": [{"name": "dispute_presence", "status": "risky"}],
                },
                "resolution_extracted_markets": [
                    {"market_id": 1, "resolution_source": {"value": "official-results", "evidence_path": "resolutionSource"}},
                    {"market_id": 2, "resolution_source": {"value": "official-results", "evidence_path": "resolutionSource"}},
                ],
            }
            session.add(
                ValidationResult(
                    opportunity_id=opportunity.id,
                    validation_type="resolution_validation",
                    status="risky",
                    score=Decimal("0.5000"),
                    summary="risky: dispute_flag_present",
                    details_json={
                        "reason_code": "dispute_flag_present",
                        "comparison_evidence": {
                            "dispute_presence": {
                                "1": [{"flag": "isDisputed", "value": False, "evidence_path": "isDisputed"}],
                                "2": [{"flag": "umaResolutionStatus", "value": "disputed", "evidence_path": "umaResolutionStatus"}],
                            }
                        },
                    },
                    validator_version="resolution_validation_v1",
                    created_at=datetime(2026, 4, 21, 12, 50, tzinfo=timezone.utc),
                )
            )
            session.commit()

        response = self.client.get(f"/opportunities/{opportunity.id}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["validation_version"], "resolution_validation_v1")
        self.assertEqual(payload["validation_reason"], "dispute_flag_present")
        self.assertEqual(payload["s_res"], "0.5000")
        self.assertEqual(payload["resolution_sources_json"]["1"]["value"], "official-results")
        self.assertEqual(payload["dispute_flags_json"]["2"][0]["value"], "disputed")
        self.assertEqual(payload["raw_context"]["resolution_validation_status"], "risky")
        self.assertEqual(payload["validation_results"][0]["validation_type"], "resolution_validation")
        self.assertEqual(payload["validation_results"][0]["details_json"]["comparison_evidence"]["dispute_presence"]["2"][0]["value"], "disputed")

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

    def test_kpi_latest_endpoint_projects_latest_run_summary(self) -> None:
        with self.SessionLocal() as session:
            session.add(
                KpiRunSummary(
                    created_at=datetime(2026, 4, 21, 12, 22, tzinfo=timezone.utc),
                    run_started_at=datetime(2026, 4, 21, 12, 20, tzinfo=timezone.utc),
                    run_completed_at=datetime(2026, 4, 21, 12, 21, tzinfo=timezone.utc),
                    kpi_version="kpi_v2",
                    total_opportunities=4,
                    valid_after_rule=4,
                    valid_after_semantic=4,
                    valid_after_resolution=3,
                    valid_after_executable=2,
                    valid_after_simulation=2,
                    avg_executable_edge=Decimal("0.1286"),
                    avg_fill_ratio=Decimal("0.4167"),
                    avg_capital_lock=Decimal("0.0100"),
                    false_positive_rate=Decimal("0.5000"),
                    family_distribution={"neg_risk_conversion": 4},
                    detector_versions_json=["neg_risk_v1"],
                    validation_versions_json=["validation_v1"],
                    simulation_versions_json=["simulation_v1"],
                    raw_context={
                        "legacy_projection": {
                            "total_intended_capital": "350.0000",
                            "total_executable_capital": "150.0000",
                        }
                    },
                )
            )
            session.commit()

        response = self.client.get("/kpi/latest")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "avg_real_edge": "0.1286",
                "avg_fill_ratio": "0.4167",
                "false_positive_rate": "0.5000",
                "total_intended_capital": "350.0000",
                "total_executable_capital": "150.0000",
                "total_opportunities": 4,
                "valid_opportunities": 2,
            },
        )

    def test_opportunity_kpi_endpoint_returns_latest_snapshot(self) -> None:
        with self.SessionLocal() as session:
            opportunity = self._create_opportunity(
                session,
                event_group_key="event-kpi",
                validation_status="valid",
                detected_at=datetime(2026, 4, 21, 12, 35, tzinfo=timezone.utc),
            )
            session.flush()
            run_summary = KpiRunSummary(
                created_at=datetime(2026, 4, 21, 12, 40, tzinfo=timezone.utc),
                run_started_at=datetime(2026, 4, 21, 12, 39, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 12, 40, tzinfo=timezone.utc),
                kpi_version="kpi_v2",
                total_opportunities=1,
                valid_after_rule=1,
                valid_after_semantic=1,
                valid_after_resolution=1,
                valid_after_executable=1,
                valid_after_simulation=1,
                avg_executable_edge=Decimal("0.2000"),
                avg_fill_ratio=Decimal("1.0000"),
                avg_capital_lock=Decimal("0.0100"),
                false_positive_rate=Decimal("0.0000"),
                family_distribution={"neg_risk_conversion": 1},
                detector_versions_json=["neg_risk_v1"],
                validation_versions_json=["validation_v1"],
                simulation_versions_json=["simulation_v1"],
                raw_context={"snapshot": "latest"},
            )
            session.add(run_summary)
            session.flush()
            session.add_all(
                [
                    OpportunityKpiSnapshot(
                        run_summary_id=run_summary.id,
                        opportunity_id=opportunity.id,
                        lineage_key="lineage-1",
                        kpi_version="kpi_v2",
                        snapshot_timestamp=datetime(2026, 4, 21, 12, 38, tzinfo=timezone.utc),
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
                        top_of_book_edge=Decimal("0.2500"),
                        depth_weighted_edge=Decimal("0.2200"),
                        fee_adjusted_edge=Decimal("0.2000"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0100"),
                        detector_version="neg_risk_v1",
                        validation_version="validation_v1",
                        simulation_version="simulation_v1",
                        first_seen_timestamp=datetime(2026, 4, 21, 12, 38, tzinfo=timezone.utc),
                        last_seen_timestamp=datetime(2026, 4, 21, 12, 38, tzinfo=timezone.utc),
                        persistence_duration_seconds=0,
                        decay_status="alive",
                        raw_context={"snapshot": "older"},
                    ),
                    OpportunityKpiSnapshot(
                        run_summary_id=run_summary.id,
                        opportunity_id=opportunity.id,
                        lineage_key="lineage-1",
                        kpi_version="kpi_v2",
                        snapshot_timestamp=datetime(2026, 4, 21, 12, 41, tzinfo=timezone.utc),
                        family=DetectionFamily.NEG_RISK_CONVERSION.value,
                        validation_stage_reached="simulation_pass",
                        final_status="rejected",
                        rejection_stage="persistence",
                        rejection_reason="no_longer_present",
                        detected=True,
                        rule_pass=True,
                        semantic_pass=True,
                        resolution_pass=True,
                        executable_pass=True,
                        simulation_pass=True,
                        s_logic=Decimal("1.0000"),
                        s_sem=Decimal("1.0000"),
                        s_res=Decimal("1.0000"),
                        top_of_book_edge=Decimal("0.2500"),
                        depth_weighted_edge=Decimal("0.2200"),
                        fee_adjusted_edge=Decimal("0.2000"),
                        fill_completion_ratio=Decimal("1.0000"),
                        execution_feasible=True,
                        capital_lock_estimate_hours=Decimal("0.0100"),
                        detector_version="neg_risk_v1",
                        validation_version="validation_v1",
                        simulation_version="simulation_v1",
                        first_seen_timestamp=datetime(2026, 4, 21, 12, 38, tzinfo=timezone.utc),
                        last_seen_timestamp=datetime(2026, 4, 21, 12, 38, tzinfo=timezone.utc),
                        persistence_duration_seconds=0,
                        decay_status="decayed",
                        raw_context={"snapshot": "latest"},
                    ),
                ]
            )
            session.commit()

        response = self.client.get(f"/kpi/opportunities/{opportunity.id}")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["opportunity_id"], opportunity.id)
        self.assertEqual(payload["final_status"], "rejected")
        self.assertEqual(payload["rejection_stage"], "persistence")
        self.assertEqual(payload["rejection_reason"], "no_longer_present")
        self.assertEqual(payload["decay_status"], "decayed")
        self.assertEqual(payload["raw_context"]["snapshot"], "latest")

    def test_kpi_run_endpoints_return_latest_and_specific_summary(self) -> None:
        with self.SessionLocal() as session:
            older = KpiRunSummary(
                created_at=datetime(2026, 4, 21, 12, 50, tzinfo=timezone.utc),
                run_started_at=datetime(2026, 4, 21, 12, 49, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 12, 50, tzinfo=timezone.utc),
                kpi_version="kpi_v2",
                total_opportunities=1,
                valid_after_rule=1,
                valid_after_semantic=1,
                valid_after_resolution=1,
                valid_after_executable=1,
                valid_after_simulation=1,
                avg_executable_edge=Decimal("0.2000"),
                avg_fill_ratio=Decimal("1.0000"),
                avg_capital_lock=Decimal("0.0100"),
                false_positive_rate=Decimal("0.0000"),
                family_distribution={"neg_risk_conversion": 1},
                detector_versions_json=["neg_risk_v1"],
                validation_versions_json=["validation_v1"],
                simulation_versions_json=["simulation_v1"],
                raw_context={"snapshot": "older"},
            )
            latest = KpiRunSummary(
                created_at=datetime(2026, 4, 21, 12, 55, tzinfo=timezone.utc),
                run_started_at=datetime(2026, 4, 21, 12, 54, tzinfo=timezone.utc),
                run_completed_at=datetime(2026, 4, 21, 12, 55, tzinfo=timezone.utc),
                kpi_version="kpi_v2",
                total_opportunities=2,
                valid_after_rule=2,
                valid_after_semantic=2,
                valid_after_resolution=2,
                valid_after_executable=1,
                valid_after_simulation=1,
                avg_executable_edge=Decimal("0.1800"),
                avg_fill_ratio=Decimal("0.7000"),
                avg_capital_lock=Decimal("0.0150"),
                false_positive_rate=Decimal("0.5000"),
                family_distribution={"neg_risk_conversion": 2},
                detector_versions_json=["neg_risk_v1"],
                validation_versions_json=["validation_v1"],
                simulation_versions_json=["simulation_v1"],
                raw_context={"snapshot": "latest"},
            )
            session.add_all([older, latest])
            session.commit()

        latest_response = self.client.get("/kpi/runs/latest")
        specific_response = self.client.get(f"/kpi/runs/{latest.id}")

        self.assertEqual(latest_response.status_code, 200)
        self.assertEqual(latest_response.json()["id"], latest.id)
        self.assertEqual(latest_response.json()["raw_context"]["snapshot"], "latest")
        self.assertEqual(specific_response.status_code, 200)
        self.assertEqual(specific_response.json()["id"], latest.id)
        self.assertEqual(specific_response.json()["avg_fill_ratio"], "0.7000")

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
            involved_market_ids_json=[1, 2],
            opportunity_type="neg_risk_long_yes_bundle",
            outcome_count=2,
            gross_price_sum=Decimal("0.6300"),
            gross_gap=Decimal("0.3700"),
            family=DetectionFamily.NEG_RISK_CONVERSION.value,
            question_texts_json=["Question 1", "Question 2"],
            top_of_book_edge=Decimal("0.3700"),
            detector_version="neg_risk_v1",
            status="detected",
            validation_status=validation_status,
            recommendation_eligibility=False,
        )
        session.add(opportunity)
        return opportunity
