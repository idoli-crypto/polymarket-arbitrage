from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


CLASSIFICATION_VERSION = "detection_family_v1"


class DetectionFamily(StrEnum):
    TIMEFRAME_NESTING = "timeframe_nesting"
    CROSS_MARKET_LOGIC = "cross_market_logic"
    SEMANTIC_NEAR_DUPLICATES = "semantic_near_duplicates"
    RESOLUTION_DIVERGENCE = "resolution_divergence"
    NEG_RISK_CONVERSION = "neg_risk_conversion"
    INTRA_MARKET_PARITY_BASELINE = "intra_market_parity_baseline"


class RelationType(StrEnum):
    IMPLICATION = "implication"
    SUBSET = "subset"
    EQUIVALENCE = "equivalence"
    TEMPORAL_NESTING = "temporal_nesting"
    RULE_DIVERGENCE = "rule_divergence"
    SEMANTIC_SIMILARITY = "semantic_similarity"


CANONICAL_DETECTION_FAMILIES: tuple[str, ...] = tuple(family.value for family in DetectionFamily)
CANONICAL_RELATION_TYPES: tuple[str, ...] = tuple(relation_type.value for relation_type in RelationType)

NEG_RISK_OPPORTUNITY_TYPE = "neg_risk_long_yes_bundle"


@dataclass(frozen=True, slots=True)
class OpportunityClassificationInput:
    opportunity_type: str
    detector_version: str
    event_group_key: str
    involved_market_ids: tuple[int, ...]
    raw_context: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class OpportunityClassification:
    family: DetectionFamily
    relation_type: RelationType | None = None
    relation_direction: str | None = None
    rationale: str = ""

    def to_raw_context(self, *, detector_version: str, opportunity_type: str) -> dict[str, Any]:
        return {
            "version": CLASSIFICATION_VERSION,
            "family": self.family.value,
            "relation_type": self.relation_type.value if self.relation_type is not None else None,
            "relation_direction": self.relation_direction,
            "rationale": self.rationale,
            "inputs": {
                "detector_version": detector_version,
                "opportunity_type": opportunity_type,
            },
        }


def classify_opportunity(input_data: OpportunityClassificationInput) -> OpportunityClassification:
    if _is_neg_risk_conversion(input_data):
        return OpportunityClassification(
            family=DetectionFamily.NEG_RISK_CONVERSION,
            rationale="neg_risk_bundle_detector_match",
        )

    raise ValueError(
        "No canonical detection family mapping is defined for "
        f"opportunity_type={input_data.opportunity_type!r} detector_version={input_data.detector_version!r}"
    )


def merge_classification_context(
    raw_context: dict[str, Any] | None,
    classification: OpportunityClassification,
    *,
    detector_version: str,
    opportunity_type: str,
) -> dict[str, Any]:
    merged = deepcopy(raw_context) if raw_context is not None else {}
    merged["classification"] = classification.to_raw_context(
        detector_version=detector_version,
        opportunity_type=opportunity_type,
    )
    return merged


def _is_neg_risk_conversion(input_data: OpportunityClassificationInput) -> bool:
    if input_data.opportunity_type != NEG_RISK_OPPORTUNITY_TYPE:
        return False

    pricing_basis = None
    if input_data.raw_context is not None:
        pricing_basis = input_data.raw_context.get("pricing_basis")

    return (
        input_data.detector_version.startswith("neg_risk_")
        or pricing_basis == "latest_yes_best_ask_sum"
    )
