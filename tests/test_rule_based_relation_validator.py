from __future__ import annotations

import unittest

from apps.api.services.opportunity_classification import DetectionFamily
from apps.worker.validators.rule_based_relation import (
    STATUS_INCONCLUSIVE,
    STATUS_INVALID,
    STATUS_VALID,
    RuleBasedRelationValidationInput,
    validate_rule_based_relation,
)


class RuleBasedRelationValidatorTests(unittest.TestCase):
    def test_neg_risk_defaults_to_valid_no_formal_relation(self) -> None:
        result = validate_rule_based_relation(
            RuleBasedRelationValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                family=DetectionFamily.NEG_RISK_CONVERSION.value,
                relation_type=None,
                relation_direction=None,
                involved_market_ids=[1, 2],
                normalized_dates=None,
                raw_context={"semantic_validation_status": "pending"},
            )
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.details["relation_type"], "no_formal_relation")
        self.assertEqual(result.details["reason_code"], "no_formal_relation")

    def test_validates_temporal_nesting_on_controlled_inputs(self) -> None:
        result = validate_rule_based_relation(
            RuleBasedRelationValidationInput(
                opportunity_id=2,
                event_group_key="event-2",
                family=DetectionFamily.TIMEFRAME_NESTING.value,
                relation_type="temporal_nesting",
                relation_direction="market_11_within_market_22",
                involved_market_ids=[11, 22],
                normalized_dates={
                    "11": {
                        "start_at": "2026-04-10T00:00:00+00:00",
                        "end_at": "2026-04-20T00:00:00+00:00",
                    },
                    "22": {
                        "start_at": "2026-04-01T00:00:00+00:00",
                        "end_at": "2026-04-30T00:00:00+00:00",
                    },
                },
                raw_context={
                    "formal_relation": {
                        "kind": "temporal_nesting",
                        "source_market_ids": [11],
                        "target_market_ids": [22],
                    }
                },
            )
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.details["reason_code"], "temporal_nesting_verified")

    def test_validates_explicit_subset_relation_metadata(self) -> None:
        result = validate_rule_based_relation(
            RuleBasedRelationValidationInput(
                opportunity_id=3,
                event_group_key="event-3",
                family=DetectionFamily.CROSS_MARKET_LOGIC.value,
                relation_type="subset",
                relation_direction="market_31_subset_market_32",
                involved_market_ids=[31, 32],
                normalized_dates=None,
                raw_context={
                    "formal_relation": {
                        "kind": "subset",
                        "source_market_ids": [31],
                        "target_market_ids": [32],
                    }
                },
            )
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.details["reason_code"], "formal_relation_verified")

    def test_returns_inconclusive_when_structured_inputs_are_missing(self) -> None:
        result = validate_rule_based_relation(
            RuleBasedRelationValidationInput(
                opportunity_id=4,
                event_group_key="event-4",
                family=DetectionFamily.CROSS_MARKET_LOGIC.value,
                relation_type="implication",
                relation_direction="market_41_implies_market_42",
                involved_market_ids=[41, 42],
                normalized_dates=None,
                raw_context={},
            )
        )

        self.assertEqual(result.status, STATUS_INCONCLUSIVE)
        self.assertEqual(result.details["reason_code"], "missing_formal_claim")

    def test_returns_invalid_when_temporal_bounds_do_not_nest(self) -> None:
        result = validate_rule_based_relation(
            RuleBasedRelationValidationInput(
                opportunity_id=5,
                event_group_key="event-5",
                family=DetectionFamily.TIMEFRAME_NESTING.value,
                relation_type="temporal_nesting",
                relation_direction="market_51_within_market_52",
                involved_market_ids=[51, 52],
                normalized_dates={
                    "51": {
                        "start_at": "2026-05-01T00:00:00+00:00",
                        "end_at": "2026-05-31T00:00:00+00:00",
                    },
                    "52": {
                        "start_at": "2026-05-10T00:00:00+00:00",
                        "end_at": "2026-05-20T00:00:00+00:00",
                    },
                },
                raw_context={
                    "formal_relation": {
                        "kind": "temporal_nesting",
                        "source_market_ids": [51],
                        "target_market_ids": [52],
                    }
                },
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "temporal_not_nested")


if __name__ == "__main__":
    unittest.main()
