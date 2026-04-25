"""Service layer for the API application."""

from apps.api.services.opportunity_classification import (
    CANONICAL_DETECTION_FAMILIES,
    CANONICAL_RELATION_TYPES,
    CLASSIFICATION_VERSION,
    DetectionFamily,
    OpportunityClassification,
    OpportunityClassificationInput,
    RelationType,
    classify_opportunity,
    merge_classification_context,
)

__all__ = [
    "CANONICAL_DETECTION_FAMILIES",
    "CANONICAL_RELATION_TYPES",
    "CLASSIFICATION_VERSION",
    "DetectionFamily",
    "OpportunityClassification",
    "OpportunityClassificationInput",
    "RelationType",
    "classify_opportunity",
    "merge_classification_context",
]
