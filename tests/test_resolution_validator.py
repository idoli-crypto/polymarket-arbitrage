from __future__ import annotations

from decimal import Decimal
import unittest

from apps.worker.validators.resolution import (
    STATUS_INCONCLUSIVE,
    STATUS_INVALID,
    STATUS_RISKY,
    STATUS_VALID,
    ResolutionValidationInput,
    validate_resolution,
)
from apps.worker.validators.resolution_extraction import (
    ResolutionExtractionInput,
    extract_resolution_metadata,
)


class ResolutionExtractionTests(unittest.TestCase):
    def test_extracts_explicit_resolution_fields(self) -> None:
        extracted = extract_resolution_metadata(
            ResolutionExtractionInput(
                market_id=1,
                raw_market_json={
                    "resolutionSource": "official-results",
                    "endDate": "2026-12-31T00:00:00Z",
                    "description": "Use the official certified result.",
                    "clarificationFlags": {"late_fill": "ignore"},
                    "isDisputed": False,
                    "edgeCaseRules": "If delayed, official certification controls.",
                },
            )
        )

        self.assertEqual(extracted["resolution_source"]["value"], "official-results")
        self.assertEqual(extracted["end_date"]["value"], "2026-12-31T00:00:00Z")
        self.assertEqual(extracted["resolution_conditions"][0]["field"], "description")
        self.assertEqual(extracted["clarification_flags"][0]["flag"], "clarificationFlags")
        self.assertEqual(extracted["dispute_flags"][0]["flag"], "isDisputed")
        self.assertEqual(extracted["edge_case_rules"][0]["field"], "edgeCaseRules")
        self.assertEqual(extracted["raw_paths_checked"][0], "resolutionSource")

    def test_records_missing_fields_without_inference(self) -> None:
        extracted = extract_resolution_metadata(
            ResolutionExtractionInput(
                market_id=2,
                raw_market_json={"resolutionSource": "official-results"},
            )
        )

        self.assertEqual(extracted["resolution_source"]["value"], "official-results")
        self.assertIn("endDate", extracted["missing_fields"])
        self.assertIn("resolution_conditions", extracted["missing_fields"])
        self.assertIn("clarification_flags", extracted["missing_fields"])
        self.assertIn("dispute_flags", extracted["missing_fields"])
        self.assertIn("edge_case_rules", extracted["missing_fields"])


class ResolutionValidationTests(unittest.TestCase):
    def test_validates_matching_resolution_cases(self) -> None:
        result = validate_resolution(
            ResolutionValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                family="neg_risk_conversion",
                relation_type=None,
                extracted_markets=[
                    self._extracted_market(1),
                    self._extracted_market(2),
                ],
            )
        )

        self.assertEqual(result.status, STATUS_VALID)
        self.assertEqual(result.score, Decimal("1.0000"))
        self.assertEqual(result.details["reason_code"], "resolution_alignment_verified")

    def test_rejects_mismatched_source(self) -> None:
        result = validate_resolution(
            ResolutionValidationInput(
                opportunity_id=2,
                event_group_key="event-2",
                family="cross_market_logic",
                relation_type="equivalence",
                extracted_markets=[
                    self._extracted_market(1, resolution_source="official-results"),
                    self._extracted_market(2, resolution_source="oracle-feed"),
                ],
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.score, Decimal("0.0000"))
        self.assertEqual(result.details["reason_code"], "resolution_source_mismatch")

    def test_rejects_mismatched_end_date(self) -> None:
        result = validate_resolution(
            ResolutionValidationInput(
                opportunity_id=3,
                event_group_key="event-3",
                family="cross_market_logic",
                relation_type="equivalence",
                extracted_markets=[
                    self._extracted_market(1, end_date="2026-12-31T00:00:00Z"),
                    self._extracted_market(2, end_date="2027-01-01T00:00:00Z"),
                ],
            )
        )

        self.assertEqual(result.status, STATUS_INVALID)
        self.assertEqual(result.details["reason_code"], "end_date_mismatch")

    def test_marks_dispute_presence_as_risky(self) -> None:
        result = validate_resolution(
            ResolutionValidationInput(
                opportunity_id=4,
                event_group_key="event-4",
                family="resolution_divergence",
                relation_type="equivalence",
                extracted_markets=[
                    self._extracted_market(1, dispute_flags=[{"flag": "isDisputed", "value": False, "evidence_path": "isDisputed"}]),
                    self._extracted_market(2, dispute_flags=[{"flag": "umaResolutionStatus", "value": "disputed", "evidence_path": "umaResolutionStatus"}]),
                ],
            )
        )

        self.assertEqual(result.status, STATUS_RISKY)
        self.assertEqual(result.score, Decimal("0.5000"))
        self.assertEqual(result.details["reason_code"], "dispute_flag_present")

    def test_returns_inconclusive_when_required_data_is_missing(self) -> None:
        result = validate_resolution(
            ResolutionValidationInput(
                opportunity_id=5,
                event_group_key="event-5",
                family="cross_market_logic",
                relation_type="equivalence",
                extracted_markets=[
                    self._extracted_market(1),
                    self._extracted_market(2, end_date=None),
                ],
            )
        )

        self.assertEqual(result.status, STATUS_INCONCLUSIVE)
        self.assertIsNone(result.score)
        self.assertEqual(result.details["reason_code"], "missing_end_date")

    def _extracted_market(
        self,
        market_id: int,
        *,
        resolution_source: str = "official-results",
        end_date: str | None = "2026-12-31T00:00:00Z",
        description: str = "Use the official certified result.",
        clarification_flags: list[dict] | None = None,
        dispute_flags: list[dict] | None = None,
        edge_case_rules: list[dict] | None = None,
    ) -> dict:
        missing_fields: list[str] = []
        end_date_payload = None
        if end_date is not None:
            end_date_payload = {"value": end_date, "evidence_path": "endDate"}
        else:
            missing_fields.append("endDate")

        return {
            "market_id": market_id,
            "resolution_source": {"value": resolution_source, "evidence_path": "resolutionSource"},
            "end_date": end_date_payload,
            "resolution_conditions": [
                {"field": "description", "value": description, "evidence_path": "description"}
            ],
            "clarification_flags": clarification_flags
            or [{"flag": "clarificationFlags", "value": {"late_fill": "ignore"}, "evidence_path": "clarificationFlags"}],
            "dispute_flags": dispute_flags
            or [{"flag": "isDisputed", "value": False, "evidence_path": "isDisputed"}],
            "edge_case_rules": edge_case_rules
            or [{"field": "edgeCaseRules", "value": "If delayed, official certification controls.", "evidence_path": "edgeCaseRules"}],
            "missing_fields": missing_fields,
            "raw_paths_checked": [
                "resolutionSource",
                "endDate",
                "description",
                "clarificationFlags",
                "isDisputed",
                "edgeCaseRules",
            ],
        }


if __name__ == "__main__":
    unittest.main()
