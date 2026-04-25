from __future__ import annotations

import unittest

from apps.worker.validators.semantic import (
    STATUS_INCONCLUSIVE,
    STATUS_INVALID,
    STATUS_VALID,
    SemanticValidationInput,
    validate_semantic_opportunity,
)
from apps.worker.validators.semantic_normalization import SemanticMarketInput, normalize_market_semantics


class SemanticNormalizationTests(unittest.TestCase):
    def test_normalize_market_semantics_extracts_structured_features(self) -> None:
        normalized = normalize_market_semantics(
            SemanticMarketInput(
                market_id=1,
                question="Will Alice win in California by December 31, 2026 with over 55% of the vote?",
            )
        )

        self.assertEqual(normalized["semantic_template"], "will <entity_0> win in <entity_1> by <date_0> with <threshold_0> of the vote?")
        self.assertEqual(normalized["entities"][0]["canonical"], "alice")
        self.assertEqual(normalized["entities"][0]["kind"], "person")
        self.assertEqual(normalized["entities"][1]["canonical"], "california")
        self.assertEqual(normalized["entities"][1]["kind"], "location")
        self.assertEqual(normalized["dates"][0]["canonical"], "2026-12-31")
        self.assertEqual(normalized["thresholds"][0]["comparator"], "gt")
        self.assertEqual(normalized["thresholds"][0]["value"], "55")
        self.assertEqual(normalized["thresholds"][0]["unit"], "percentage")
        self.assertTrue(normalized["timeframes"])
        self.assertEqual(normalized["polarity"]["direction"], "positive")
        self.assertEqual(normalized["units"], ["count_votes", "percentage"])


class SemanticComparisonTests(unittest.TestCase):
    def test_accepts_aligned_entity_variants_with_same_template(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                family="neg_risk_conversion",
                relation_type=None,
                involved_market_ids=[1, 2],
                question_texts=["Will Alice win by December 31, 2026?", "Will Bob win by December 31, 2026?"],
                raw_context={
                    "markets": [
                        {"market_id": 1, "question": "Will Alice win by December 31, 2026?"},
                        {"market_id": 2, "question": "Will Bob win by December 31, 2026?"},
                    ]
                },
            )
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.details["reason_code"], "semantic_alignment_verified")
        self.assertEqual(result.normalized_dates["1"][0]["canonical"], "2026-12-31")

    def test_rejects_context_entity_mismatch(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=2,
                event_group_key="event-2",
                family="cross_market_logic",
                relation_type="equivalence",
                involved_market_ids=[10, 20],
                question_texts=[
                    "Will Alice win in California?",
                    "Will Bob win in Texas?",
                ],
                raw_context=None,
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "context_entity_mismatch")

    def test_rejects_date_mismatch(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=3,
                event_group_key="event-3",
                family="cross_market_logic",
                relation_type="equivalence",
                involved_market_ids=[1, 2],
                question_texts=[
                    "Will Alice win by December 31, 2026?",
                    "Will Bob win by December 31, 2027?",
                ],
                raw_context=None,
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "date_mismatch")

    def test_rejects_threshold_mismatch(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=4,
                event_group_key="event-4",
                family="cross_market_logic",
                relation_type="equivalence",
                involved_market_ids=[1, 2],
                question_texts=[
                    "Will Alice win with over 55% of the vote?",
                    "Will Bob win with over 60% of the vote?",
                ],
                raw_context=None,
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "threshold_mismatch")

    def test_rejects_polarity_mismatch(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=5,
                event_group_key="event-5",
                family="cross_market_logic",
                relation_type="equivalence",
                involved_market_ids=[1, 2],
                question_texts=[
                    "Will Alice win the election?",
                    "Will Bob lose the election?",
                ],
                raw_context=None,
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "polarity_mismatch")

    def test_returns_inconclusive_when_structured_fields_are_partial(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=6,
                event_group_key="event-6",
                family="cross_market_logic",
                relation_type="equivalence",
                involved_market_ids=[1, 2],
                question_texts=[
                    "Will Alice win by December 31, 2026?",
                    "Will Bob win?",
                ],
                raw_context=None,
            )
        )

        self.assertEqual(result.status, STATUS_INCONCLUSIVE)
        self.assertEqual(result.details["reason_code"], "partial_structured_data")

    def test_returns_inconclusive_when_market_text_is_missing(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=7,
                event_group_key="event-7",
                family="cross_market_logic",
                relation_type="equivalence",
                involved_market_ids=[1, 2],
                question_texts=["Will Alice win?"],
                raw_context=None,
            )
        )

        self.assertEqual(result.status, STATUS_INCONCLUSIVE)
        self.assertEqual(result.details["reason_code"], "missing_question")


if __name__ == "__main__":
    unittest.main()
