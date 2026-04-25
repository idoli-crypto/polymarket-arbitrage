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
    DetectedOpportunity,
    KpiRunSummary,
    KpiSnapshot,
    Market,
    MarketSnapshot,
    OpportunityKpiSnapshot,
    SimulationResult,
    ValidationResult,
)
from apps.api.services.opportunity_classification import DetectionFamily
from apps.worker.opportunity_validation import validate_pending_opportunities
from apps.worker.validators.rule_based_relation import VALIDATOR_VERSION as RULE_VALIDATOR_VERSION
from apps.worker.validators.resolution import VALIDATOR_VERSION as RESOLUTION_VALIDATOR_VERSION
from apps.worker.validators.semantic import VALIDATOR_VERSION as SEMANTIC_VALIDATOR_VERSION


class OpportunityValidationIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "validation-test.db"
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

    def test_validate_pending_opportunities_persists_validation_and_execution_results(self) -> None:
        with self.SessionLocal() as session:
            fresh_snapshot_time = datetime.now(timezone.utc).replace(microsecond=0)
            session.add_all(
                [
                    self._build_market(
                        local_id=1,
                        polymarket_market_id="pm-1",
                        question="Will Alice win by December 31, 2026?",
                        raw_market_json=self._resolution_payload(taker_base_fee=40, order_min_size=1),
                    ),
                    self._build_market(
                        local_id=2,
                        polymarket_market_id="pm-2",
                        question="Will Bob win by December 31, 2026?",
                        raw_market_json=self._resolution_payload(taker_base_fee=40, order_min_size=1),
                    ),
                    self._build_market(
                        local_id=10,
                        polymarket_market_id="pm-10",
                        question="Will Alice win in California by December 31, 2026?",
                        raw_market_json=self._resolution_payload(
                            end_date="2026-12-31T00:00:00Z",
                            description="Market resolves to official statewide certification.",
                            taker_base_fee=40,
                            order_min_size=1,
                        ),
                    ),
                    self._build_market(
                        local_id=20,
                        polymarket_market_id="pm-20",
                        question="Will Bob win in California by December 31, 2026?",
                        raw_market_json=self._resolution_payload(
                            end_date="2026-12-31T00:00:00Z",
                            description="Market resolves to official statewide certification.",
                            taker_base_fee=40,
                            order_min_size=1,
                        ),
                    ),
                    self._build_market(
                        local_id=50,
                        polymarket_market_id="pm-50",
                        question="Will Alice win by December 31, 2026?",
                        raw_market_json=self._resolution_payload(
                            dispute_value=False,
                            taker_base_fee=40,
                            order_min_size=1,
                        ),
                    ),
                    self._build_market(
                        local_id=60,
                        polymarket_market_id="pm-60",
                        question="Will Bob win by December 31, 2026?",
                        raw_market_json=self._resolution_payload(
                            dispute_value="disputed",
                            taker_base_fee=40,
                            order_min_size=1,
                        ),
                    ),
                ]
            )
            session.add_all(
                [
                    self._build_snapshot(
                        market_id=1,
                        best_ask="0.3000",
                        ask_depth_usd="30.0000",
                        order_book_json=self._order_book([("0.3000", "100.0000")]),
                        captured_at=fresh_snapshot_time,
                    ),
                    self._build_snapshot(
                        market_id=2,
                        best_ask="0.2500",
                        ask_depth_usd="25.0000",
                        order_book_json=self._order_book([("0.2500", "100.0000")]),
                        captured_at=fresh_snapshot_time,
                    ),
                    self._build_snapshot(
                        market_id=10,
                        best_ask="0.4000",
                        ask_depth_usd="40.0000",
                        order_book_json=self._order_book([("0.4000", "100.0000")]),
                        captured_at=fresh_snapshot_time,
                    ),
                    self._build_snapshot(
                        market_id=20,
                        best_ask="0.3900",
                        ask_depth_usd="39.0000",
                        order_book_json=self._order_book([("0.3900", "100.0000")]),
                        captured_at=fresh_snapshot_time,
                    ),
                ]
            )
            neg_risk_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 1, tzinfo=timezone.utc),
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                involved_market_ids_json=[1, 2],
                question_texts_json=["Will Alice win by December 31, 2026?", "Will Bob win by December 31, 2026?"],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.5500"),
                gross_gap=Decimal("0.4500"),
                family=DetectionFamily.NEG_RISK_CONVERSION.value,
                detector_version="neg_risk_v1",
                status="detected",
                raw_context={
                    "semantic_validation_status": "pending",
                    "markets": [
                        {"market_id": 1, "question": "Will Alice win by December 31, 2026?"},
                        {"market_id": 2, "question": "Will Bob win by December 31, 2026?"},
                    ],
                },
            )
            temporal_nesting_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 2, tzinfo=timezone.utc),
                event_group_key="event-2",
                involved_market_ids=[10, 20],
                involved_market_ids_json=[10, 20],
                question_texts_json=[
                    "Will Alice win in California by December 31, 2026?",
                    "Will Bob win in California by December 31, 2026?",
                ],
                opportunity_type="timeframe_pair",
                outcome_count=2,
                gross_price_sum=Decimal("0.8000"),
                gross_gap=Decimal("0.2000"),
                family=DetectionFamily.TIMEFRAME_NESTING.value,
                relation_type="temporal_nesting",
                relation_direction="market_10_within_market_20",
                normalized_dates_json={
                    "10": {
                        "start_at": "2026-04-10T00:00:00+00:00",
                        "end_at": "2026-04-20T00:00:00+00:00",
                    },
                    "20": {
                        "start_at": "2026-04-01T00:00:00+00:00",
                        "end_at": "2026-04-30T00:00:00+00:00",
                    },
                },
                detector_version="timeframe_v1",
                status="detected",
                raw_context={
                    "formal_relation": {
                        "kind": "temporal_nesting",
                        "source_market_ids": [10],
                        "target_market_ids": [20],
                    },
                    "semantic_validation_status": "pending",
                    "markets": [
                        {"market_id": 10, "question": "Will Alice win in California by December 31, 2026?"},
                        {"market_id": 20, "question": "Will Bob win in California by December 31, 2026?"},
                    ],
                },
            )
            inconclusive_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 3, tzinfo=timezone.utc),
                event_group_key="event-3",
                involved_market_ids=[30, 40],
                involved_market_ids_json=[30, 40],
                question_texts_json=["Will Alice win by December 31, 2026?"],
                opportunity_type="cross_market_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.7000"),
                gross_gap=Decimal("0.3000"),
                family=DetectionFamily.CROSS_MARKET_LOGIC.value,
                relation_type="subset",
                relation_direction="market_30_subset_market_40",
                detector_version="cross_market_v1",
                status="detected",
                raw_context={"semantic_validation_status": "pending"},
            )
            risky_resolution_opportunity = DetectedOpportunity(
                detection_window_start=datetime(2026, 4, 21, 10, 4, tzinfo=timezone.utc),
                event_group_key="event-4",
                involved_market_ids=[50, 60],
                involved_market_ids_json=[50, 60],
                question_texts_json=["Will Alice win by December 31, 2026?", "Will Bob win by December 31, 2026?"],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6100"),
                gross_gap=Decimal("0.3900"),
                family=DetectionFamily.NEG_RISK_CONVERSION.value,
                detector_version="neg_risk_v1",
                status="detected",
                raw_context={
                    "semantic_validation_status": "pending",
                    "markets": [
                        {"market_id": 50, "question": "Will Alice win by December 31, 2026?"},
                        {"market_id": 60, "question": "Will Bob win by December 31, 2026?"},
                    ],
                },
            )
            session.add_all(
                [
                    neg_risk_opportunity,
                    temporal_nesting_opportunity,
                    inconclusive_opportunity,
                    risky_resolution_opportunity,
                ]
            )
            session.commit()

            results = validate_pending_opportunities(session)
            self.assertEqual(len(results), 4)

            after_rows = session.scalars(
                select(DetectedOpportunity).order_by(DetectedOpportunity.id.asc())
            ).all()

            self.assertEqual(after_rows[0].validation_status, "valid")
            self.assertIsNone(after_rows[0].validation_reason)
            self.assertEqual(after_rows[0].validation_version, "executable_edge_validation_v1")
            self.assertEqual(after_rows[0].simulation_version, "simulation_validation_v1")
            self.assertEqual(after_rows[0].raw_context["semantic_validation_status"], "valid")
            self.assertEqual(after_rows[0].raw_context["rule_validation_status"], "valid")
            self.assertEqual(after_rows[0].raw_context["resolution_validation_status"], "valid")
            self.assertEqual(after_rows[0].raw_context["execution_validation_status"], "valid")
            self.assertEqual(after_rows[0].raw_context["simulation_validation_status"], "valid")
            self.assertEqual(
                after_rows[0].raw_context["execution_validation_details"]["supported_contract_type"],
                "neg_risk_long_yes_bundle",
            )
            self.assertEqual(
                after_rows[0].raw_context["simulation_validation_details"]["reason_code"],
                "full_sequential_execution_verified",
            )
            self.assertEqual(
                after_rows[0].raw_context["execution_validation_details"]["fee_source"],
                "persisted_market_metadata_only",
            )
            self.assertEqual(after_rows[0].raw_context["semantic_validation_details"]["reason_code"], "semantic_alignment_verified")
            self.assertEqual(
                after_rows[0].raw_context["rule_validation_details"]["relation_type"],
                "no_formal_relation",
            )
            self.assertEqual(after_rows[0].normalized_dates_json["1"][0]["canonical"], "2026-12-31")
            self.assertEqual(after_rows[0].s_sem, Decimal("1.0000"))
            self.assertEqual(after_rows[0].s_res, Decimal("1.0000"))
            self.assertEqual(after_rows[0].top_of_book_edge, Decimal("0.4500"))
            self.assertEqual(after_rows[0].depth_weighted_edge, Decimal("0.4500"))
            self.assertEqual(after_rows[0].fee_adjusted_edge, Decimal("0.4484"))
            self.assertEqual(after_rows[0].min_executable_size, Decimal("1.0000"))
            self.assertEqual(after_rows[0].suggested_notional_bucket, "100-250")
            self.assertEqual(after_rows[0].capital_lock_estimate_hours, Decimal("0.0069"))
            self.assertEqual(after_rows[0].resolution_sources_json["1"]["value"], "official-results")
            self.assertEqual(after_rows[0].end_dates_json["1"]["value"], "2026-12-31T00:00:00Z")
            self.assertEqual(after_rows[0].clarification_flags_json["1"][0]["flag"], "clarificationFlags")
            self.assertEqual(after_rows[0].dispute_flags_json["1"][0]["flag"], "isDisputed")

            self.assertEqual(after_rows[1].validation_status, "rejected")
            self.assertEqual(after_rows[1].validation_reason, "unsupported_payout_contract")
            self.assertIsNone(after_rows[1].top_of_book_edge)
            self.assertEqual(after_rows[1].raw_context["rule_validation_status"], "valid")
            self.assertEqual(after_rows[1].raw_context["semantic_validation_status"], "valid")
            self.assertEqual(after_rows[1].raw_context["resolution_validation_status"], "valid")
            self.assertEqual(after_rows[1].raw_context["execution_validation_status"], "inconclusive")
            self.assertEqual(
                after_rows[1].raw_context["rule_validation_details"]["reason_code"],
                "temporal_nesting_verified",
            )
            self.assertEqual(
                after_rows[1].raw_context["rule_validation_details"]["evidence"]["formal_claim"]["kind"],
                "temporal_nesting",
            )
            self.assertEqual(
                after_rows[1].raw_context["execution_validation_details"]["reason_code"],
                "unsupported_payout_contract",
            )
            self.assertEqual(
                after_rows[1].raw_context["execution_validation_details"]["unsupported_payout_contract"],
                "timeframe_pair",
            )

            self.assertEqual(after_rows[2].validation_status, "rejected")
            self.assertEqual(after_rows[2].validation_reason, "missing_formal_claim")
            self.assertEqual(after_rows[2].risk_flags_json[0]["status"], "inconclusive")
            self.assertEqual(after_rows[2].risk_flags_json[0]["stage"], "rule_based_relation")
            self.assertEqual(after_rows[2].raw_context["rule_validation_status"], "inconclusive")
            self.assertEqual(after_rows[2].raw_context["semantic_validation_status"], "inconclusive")
            self.assertEqual(after_rows[2].raw_context["resolution_validation_status"], "inconclusive")
            self.assertEqual(after_rows[2].raw_context["semantic_validation_details"]["reason_code"], "missing_question")

            self.assertEqual(after_rows[3].validation_status, "rejected")
            self.assertEqual(after_rows[3].validation_reason, "dispute_flag_present")
            self.assertEqual(after_rows[3].raw_context["rule_validation_status"], "valid")
            self.assertEqual(after_rows[3].raw_context["semantic_validation_status"], "valid")
            self.assertEqual(after_rows[3].raw_context["resolution_validation_status"], "risky")
            self.assertEqual(after_rows[3].s_res, Decimal("0.5000"))
            self.assertEqual(after_rows[3].risk_flags_json[0]["stage"], "resolution_validation")
            self.assertEqual(after_rows[3].risk_flags_json[0]["status"], "risky")

            validation_results = session.scalars(
                select(ValidationResult).order_by(
                    ValidationResult.opportunity_id.asc(),
                    ValidationResult.id.asc(),
                )
            ).all()
            self.assertEqual(len(validation_results), 15)
            self.assertEqual(validation_results[0].validation_type, "rule_based_relation")
            self.assertEqual(validation_results[0].status, "valid")
            self.assertEqual(validation_results[0].validator_version, RULE_VALIDATOR_VERSION)
            self.assertEqual(validation_results[1].validation_type, "semantic_validation")
            self.assertEqual(validation_results[1].status, "valid")
            self.assertEqual(validation_results[1].validator_version, SEMANTIC_VALIDATOR_VERSION)
            self.assertEqual(validation_results[1].details_json["reason_code"], "semantic_alignment_verified")
            self.assertEqual(validation_results[2].validation_type, "resolution_validation")
            self.assertEqual(validation_results[2].status, "valid")
            self.assertEqual(validation_results[2].validator_version, RESOLUTION_VALIDATOR_VERSION)
            self.assertEqual(validation_results[2].details_json["reason_code"], "resolution_alignment_verified")
            self.assertEqual(validation_results[3].validation_type, "executable_edge_validation")
            self.assertEqual(validation_results[3].status, "valid")
            self.assertEqual(validation_results[3].validator_version, "executable_edge_validation_v1")
            self.assertEqual(validation_results[3].details_json["reason_code"], "positive_fee_adjusted_edge_verified")
            self.assertEqual(validation_results[4].validation_type, "simulation_validation")
            self.assertEqual(validation_results[4].status, "valid")
            self.assertEqual(validation_results[4].validator_version, "simulation_validation_v1")
            self.assertEqual(validation_results[4].details_json["reason_code"], "full_sequential_execution_verified")
            self.assertEqual(validation_results[5].details_json["reason_code"], "temporal_nesting_verified")
            self.assertEqual(validation_results[6].validation_type, "semantic_validation")
            self.assertEqual(validation_results[6].status, "valid")
            self.assertEqual(validation_results[7].validation_type, "resolution_validation")
            self.assertEqual(validation_results[7].status, "valid")
            self.assertEqual(validation_results[8].validation_type, "executable_edge_validation")
            self.assertEqual(validation_results[8].status, "inconclusive")
            self.assertEqual(validation_results[8].details_json["reason_code"], "unsupported_payout_contract")
            self.assertEqual(validation_results[9].status, "inconclusive")
            self.assertEqual(validation_results[9].details_json["reason_code"], "missing_formal_claim")
            self.assertEqual(validation_results[10].validation_type, "semantic_validation")
            self.assertEqual(validation_results[10].status, "inconclusive")
            self.assertEqual(validation_results[10].details_json["reason_code"], "missing_question")
            self.assertEqual(validation_results[11].validation_type, "resolution_validation")
            self.assertEqual(validation_results[11].status, "inconclusive")
            self.assertEqual(validation_results[11].details_json["reason_code"], "missing_resolution_source")
            self.assertEqual(validation_results[14].validation_type, "resolution_validation")
            self.assertEqual(validation_results[14].status, "risky")
            self.assertEqual(validation_results[14].details_json["reason_code"], "dispute_flag_present")

            simulation_results = session.scalars(
                select(SimulationResult).order_by(SimulationResult.opportunity_id.asc(), SimulationResult.id.asc())
            ).all()
            self.assertEqual(len(simulation_results), 1)
            self.assertEqual(simulation_results[0].simulation_mode, "simulation_validation")
            self.assertEqual(simulation_results[0].executable_edge, Decimal("0.4484"))
            self.assertEqual(simulation_results[0].slippage_cost, Decimal("0.0000"))
            self.assertEqual(simulation_results[0].fill_completion_ratio, Decimal("1.0000"))
            self.assertTrue(simulation_results[0].execution_feasible)
            self.assertEqual(simulation_results[0].execution_risk_flag, "none")

            kpi_run_summary = session.scalar(select(KpiRunSummary))
            assert kpi_run_summary is not None
            self.assertEqual(kpi_run_summary.total_opportunities, 4)
            self.assertEqual(kpi_run_summary.valid_after_rule, 3)
            self.assertEqual(kpi_run_summary.valid_after_semantic, 3)
            self.assertEqual(kpi_run_summary.valid_after_resolution, 2)
            self.assertEqual(kpi_run_summary.valid_after_executable, 1)
            self.assertEqual(kpi_run_summary.valid_after_simulation, 1)
            self.assertEqual(kpi_run_summary.false_positive_rate, Decimal("0.7500"))

            kpi_snapshots = session.scalars(
                select(OpportunityKpiSnapshot).order_by(OpportunityKpiSnapshot.opportunity_id.asc())
            ).all()
            self.assertEqual(len(kpi_snapshots), 4)
            self.assertEqual(kpi_snapshots[0].validation_stage_reached, "simulation_pass")
            self.assertEqual(kpi_snapshots[0].final_status, "valid")
            self.assertEqual(kpi_snapshots[0].decay_status, "alive")
            self.assertEqual(kpi_snapshots[1].rejection_stage, "executable")
            self.assertEqual(kpi_snapshots[1].validation_stage_reached, "resolution_pass")
            self.assertEqual(kpi_snapshots[2].rejection_stage, "rule")
            self.assertEqual(kpi_snapshots[2].validation_stage_reached, "detected")
            self.assertEqual(kpi_snapshots[3].rejection_stage, "resolution")

            legacy_snapshot = session.scalar(select(KpiSnapshot))
            assert legacy_snapshot is not None
            self.assertEqual(legacy_snapshot.total_opportunities, 4)
            self.assertEqual(legacy_snapshot.valid_opportunities, 1)

    def test_validate_pending_opportunities_rejects_partial_fill_simulation(self) -> None:
        with self.SessionLocal() as session:
            captured_at = datetime.now(timezone.utc).replace(microsecond=0)
            session.add_all(
                [
                    self._build_market(
                        local_id=101,
                        polymarket_market_id="pm-101",
                        question="Will Alice win by December 31, 2026?",
                        raw_market_json=self._resolution_payload(taker_base_fee=40, order_min_size=1),
                    ),
                    self._build_market(
                        local_id=102,
                        polymarket_market_id="pm-102",
                        question="Will Bob win by December 31, 2026?",
                        raw_market_json=self._resolution_payload(taker_base_fee=40, order_min_size=1),
                    ),
                ]
            )
            session.add_all(
                [
                    self._build_snapshot(
                        market_id=101,
                        best_ask="0.3000",
                        ask_depth_usd="30.0000",
                        order_book_json=self._order_book([("0.3000", "100.0000")]),
                        captured_at=captured_at,
                    ),
                    self._build_snapshot(
                        market_id=102,
                        best_ask="0.2500",
                        ask_depth_usd="10.0000",
                        order_book_json=self._order_book([("0.2500", "40.0000")]),
                        captured_at=captured_at,
                    ),
                ]
            )
            session.add(
                DetectedOpportunity(
                    detection_window_start=datetime(2026, 4, 21, 11, 0, tzinfo=timezone.utc),
                    event_group_key="event-partial",
                    involved_market_ids=[101, 102],
                    involved_market_ids_json=[101, 102],
                    question_texts_json=[
                        "Will Alice win by December 31, 2026?",
                        "Will Bob win by December 31, 2026?",
                    ],
                    opportunity_type="neg_risk_long_yes_bundle",
                    outcome_count=2,
                    gross_price_sum=Decimal("0.5500"),
                    gross_gap=Decimal("0.4500"),
                    family=DetectionFamily.NEG_RISK_CONVERSION.value,
                    detector_version="neg_risk_v1",
                    status="detected",
                    raw_context={
                        "markets": [
                            {"market_id": 101, "question": "Will Alice win by December 31, 2026?"},
                            {"market_id": 102, "question": "Will Bob win by December 31, 2026?"},
                        ],
                    },
                )
            )
            session.commit()

            results = validate_pending_opportunities(session)
            self.assertEqual(len(results), 1)

            refreshed = session.scalar(select(DetectedOpportunity).where(DetectedOpportunity.event_group_key == "event-partial"))
            assert refreshed is not None
            self.assertEqual(refreshed.validation_status, "rejected")
            self.assertEqual(refreshed.validation_reason, "partial_fill_detected")
            self.assertEqual(refreshed.simulation_version, "simulation_validation_v1")
            self.assertEqual(refreshed.raw_context["simulation_validation_status"], "invalid")
            self.assertEqual(refreshed.raw_context["simulation_validation_details"]["fill_completion_ratio"], "0.4000")
            self.assertEqual(refreshed.risk_flags_json[-1]["stage"], "simulation_validation")

            validation_results = session.scalars(
                select(ValidationResult).where(ValidationResult.opportunity_id == refreshed.id).order_by(ValidationResult.id.asc())
            ).all()
            self.assertEqual([row.validation_type for row in validation_results], [
                "rule_based_relation",
                "semantic_validation",
                "resolution_validation",
                "executable_edge_validation",
                "simulation_validation",
            ])
            self.assertEqual(validation_results[-1].status, "invalid")
            self.assertEqual(validation_results[-1].details_json["reason_code"], "partial_fill_detected")

            simulation_results = session.scalars(
                select(SimulationResult).where(SimulationResult.opportunity_id == refreshed.id)
            ).all()
            self.assertEqual(len(simulation_results), 1)
            self.assertEqual(simulation_results[0].simulation_mode, "simulation_validation")
            self.assertEqual(simulation_results[0].fill_completion_ratio, Decimal("0.4000"))
            self.assertFalse(simulation_results[0].execution_feasible)
            self.assertEqual(simulation_results[0].execution_risk_flag, "partial_fill_risk")

            kpi_snapshot = session.scalar(
                select(OpportunityKpiSnapshot).where(OpportunityKpiSnapshot.opportunity_id == refreshed.id)
            )
            assert kpi_snapshot is not None
            self.assertEqual(kpi_snapshot.rejection_stage, "simulation")
            self.assertEqual(kpi_snapshot.validation_stage_reached, "executable_pass")
            self.assertEqual(kpi_snapshot.fill_completion_ratio, Decimal("0.4000"))
            self.assertFalse(kpi_snapshot.execution_feasible)
            self.assertEqual(kpi_snapshot.decay_status, "decayed")

    def _build_market(
        self,
        *,
        local_id: int,
        polymarket_market_id: str,
        question: str,
        raw_market_json: dict,
    ) -> Market:
        return Market(
            id=local_id,
            polymarket_market_id=polymarket_market_id,
            question=question,
            slug=polymarket_market_id,
            condition_id=f"condition-{local_id}",
            event_id=f"event-{local_id}",
            event_slug=f"event-{local_id}",
            raw_market_json=raw_market_json,
            neg_risk=True,
            status="active",
        )

    def _resolution_payload(
        self,
        *,
        end_date: str = "2026-12-31T00:00:00Z",
        description: str = "Resolves according to the official certified result.",
        clarification_value: dict | None = None,
        dispute_value: bool | str = False,
        edge_case_value: str = "If delayed, the official certification still governs resolution.",
        taker_base_fee: int | None = None,
        order_min_size: int | None = None,
    ) -> dict:
        return {
            "resolutionSource": "official-results",
            "endDate": end_date,
            "description": description,
            "clarificationFlags": clarification_value or {"delayed_certification": "use official certification"},
            "isDisputed": dispute_value,
            "edgeCaseRules": edge_case_value,
            "takerBaseFee": taker_base_fee,
            "orderMinSize": order_min_size,
        }

    def _build_snapshot(
        self,
        *,
        market_id: int,
        best_ask: str,
        ask_depth_usd: str,
        order_book_json: dict[str, object],
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

    def _order_book(self, asks: list[tuple[str, str]]) -> dict[str, object]:
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


if __name__ == "__main__":
    unittest.main()
