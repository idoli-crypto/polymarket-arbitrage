from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.db.base import Base
from apps.api.repositories.opportunities import (
    attach_simulation_result,
    attach_validation_result,
    create_opportunity_extended,
    get_opportunity_with_context,
    list_opportunities,
)
from apps.api.services.opportunity_classification import (
    DetectionFamily,
    OpportunityClassification,
)


class OpportunityRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        database_path = Path(self.temp_dir.name) / "opportunity-repository.db"
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

    def test_create_and_read_extended_opportunity_with_context(self) -> None:
        with self.SessionLocal() as session:
            opportunity = create_opportunity_extended(
                session,
                detected_at=datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc),
                detection_window_start=datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc),
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6300"),
                gross_gap=Decimal("0.3700"),
                detector_version="neg_risk_v1",
                classification=OpportunityClassification(family=DetectionFamily.NEG_RISK_CONVERSION),
                question_texts_json=["Will Alice win?", "Will Bob win?"],
                confidence_tier="High",
                top_of_book_edge=Decimal("0.3700"),
                recommendation_eligibility=False,
                raw_context={"source": "test"},
            )
            attach_validation_result(
                session,
                opportunity.id,
                validation_type="semantic",
                status="valid",
                validator_version="semantic_validator_v1",
                summary="valid",
                details_json={"evidence": "ok"},
            )
            attach_simulation_result(
                session,
                opportunity.id,
                simulation_mode="top_of_book_execution",
                simulation_version="execution_sim_v1",
                executable_edge=Decimal("0.3000"),
                estimated_fill_quality=Decimal("0.7500"),
                min_executable_size=Decimal("75.0000"),
                details_json={"path": "latest"},
            )
            session.commit()

            loaded = get_opportunity_with_context(session, opportunity.id)

        assert loaded is not None
        self.assertEqual(loaded.family, DetectionFamily.NEG_RISK_CONVERSION.value)
        self.assertEqual(loaded.question_texts_json, ["Will Alice win?", "Will Bob win?"])
        self.assertEqual(loaded.confidence_tier, "High")
        self.assertEqual(len(loaded.validation_results), 1)
        self.assertEqual(loaded.validation_results[0].validator_version, "semantic_validator_v1")
        self.assertEqual(len(loaded.simulation_results), 1)
        self.assertEqual(loaded.simulation_results[0].simulation_version, "execution_sim_v1")

    def test_list_opportunities_filters_by_family_and_confidence_tier(self) -> None:
        with self.SessionLocal() as session:
            create_opportunity_extended(
                session,
                detection_window_start=datetime(2026, 4, 22, 10, 0, tzinfo=timezone.utc),
                event_group_key="event-1",
                involved_market_ids=[1, 2],
                opportunity_type="neg_risk_long_yes_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.6300"),
                gross_gap=Decimal("0.3700"),
                detector_version="neg_risk_v1",
                classification=OpportunityClassification(family=DetectionFamily.NEG_RISK_CONVERSION),
                confidence_tier="High",
                recommendation_eligibility=False,
            )
            create_opportunity_extended(
                session,
                detection_window_start=datetime(2026, 4, 22, 10, 1, tzinfo=timezone.utc),
                event_group_key="event-2",
                involved_market_ids=[3, 4],
                opportunity_type="cross_market_bundle",
                outcome_count=2,
                gross_price_sum=Decimal("0.7000"),
                gross_gap=Decimal("0.3000"),
                detector_version="cross_market_v1",
                classification=OpportunityClassification(family=DetectionFamily.CROSS_MARKET_LOGIC),
                confidence_tier="Medium",
                recommendation_eligibility=False,
            )
            session.commit()

            neg_risk_rows = list_opportunities(
                session,
                family=DetectionFamily.NEG_RISK_CONVERSION,
            )
            high_confidence_rows = list_opportunities(session, confidence_tier="High")

        self.assertEqual([row.event_group_key for row in neg_risk_rows], ["event-1"])
        self.assertEqual([row.event_group_key for row in high_confidence_rows], ["event-1"])


if __name__ == "__main__":
    unittest.main()
