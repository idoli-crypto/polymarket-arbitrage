from __future__ import annotations

from decimal import Decimal
import unittest

from apps.api.services.opportunity_classification import (
    CANONICAL_DETECTION_FAMILIES,
    CLASSIFICATION_VERSION,
    DetectionFamily,
    OpportunityClassificationInput,
    classify_opportunity,
    merge_classification_context,
)


class OpportunityClassificationTests(unittest.TestCase):
    def test_canonical_family_definitions_match_approved_values(self) -> None:
        self.assertEqual(
            CANONICAL_DETECTION_FAMILIES,
            (
                "timeframe_nesting",
                "cross_market_logic",
                "semantic_near_duplicates",
                "resolution_divergence",
                "neg_risk_conversion",
                "intra_market_parity_baseline",
            ),
        )

    def test_classifies_current_neg_risk_opportunities_as_neg_risk_conversion(self) -> None:
        classification = classify_opportunity(
            OpportunityClassificationInput(
                opportunity_type="neg_risk_long_yes_bundle",
                detector_version="neg_risk_v1",
                event_group_key="event-1",
                involved_market_ids=(1, 2, 3),
                raw_context={"pricing_basis": "latest_yes_best_ask_sum"},
            )
        )

        self.assertEqual(classification.family, DetectionFamily.NEG_RISK_CONVERSION)
        self.assertIsNone(classification.relation_type)
        self.assertIsNone(classification.relation_direction)
        self.assertEqual(classification.rationale, "neg_risk_bundle_detector_match")

    def test_merge_classification_context_adds_auditable_classification_block(self) -> None:
        raw_context = {
            "pricing_basis": "latest_yes_best_ask_sum",
            "markets": [{"market_id": 1, "best_ask": format(Decimal("0.3000"), "f")}],
        }
        classification = classify_opportunity(
            OpportunityClassificationInput(
                opportunity_type="neg_risk_long_yes_bundle",
                detector_version="neg_risk_v1",
                event_group_key="event-1",
                involved_market_ids=(1, 2),
                raw_context=raw_context,
            )
        )

        merged = merge_classification_context(
            raw_context,
            classification,
            detector_version="neg_risk_v1",
            opportunity_type="neg_risk_long_yes_bundle",
        )

        self.assertEqual(merged["pricing_basis"], "latest_yes_best_ask_sum")
        self.assertEqual(merged["classification"]["version"], CLASSIFICATION_VERSION)
        self.assertEqual(merged["classification"]["family"], DetectionFamily.NEG_RISK_CONVERSION.value)
        self.assertEqual(merged["classification"]["relation_type"], None)
        self.assertEqual(merged["classification"]["relation_direction"], None)
        self.assertEqual(merged["classification"]["inputs"]["detector_version"], "neg_risk_v1")
        self.assertNotIn("classification", raw_context)


if __name__ == "__main__":
    unittest.main()
