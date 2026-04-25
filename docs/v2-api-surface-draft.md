# V2 API Surface Draft

## Status

Draft only. This document defines the intended read-only V2 recommendation API surface for future PRs. It does not describe implemented behavior in PR 1.

PR 1 does not supersede existing V1 research endpoints.

## Purpose

The V2 API exists to expose recommendation-oriented, audit-oriented, and decision-support views over persisted research outputs. It is not a trading API.

## API Principles

- read-only
- recommendation-only
- version-aware
- audit-oriented
- no order placement
- no wallet actions
- no execution commands

## Conceptual Surface

### Recommendation List

Primary use: present ranked recommendation candidates for dashboard review.

Conceptual response areas:

- recommendation identifier
- detection family
- recommendation status
- ranking or score summary
- validation summary
- executable-edge summary
- freshness or recency context
- detector and scoring version references

### Recommendation Detail

Primary use: inspect one recommendation in a human review flow.

Conceptual response areas:

- recommendation headline and summary
- family classification
- validation stack results
- pricing and executable-edge context
- simulation context
- linked research artifacts
- detector and scoring version references

### Audit / Detail Evidence View

Primary use: review the evidence and lineage behind a recommendation.

Conceptual response areas:

- source artifact references
- validation evidence by layer
- rationale trail
- timestamps
- version traceability
- persistence and audit references

### Detector / Scoring Version Metadata

Primary use: explain which logic versions produced the current recommendation set.

Conceptual response areas:

- active detector versions
- active scoring versions
- version labels or identifiers
- effective timestamps

### System / Health Context

Primary use: support confidence and freshness review, not operational automation.

Conceptual response areas:

- snapshot freshness
- detection freshness
- validation freshness
- simulation freshness
- recommendation publication freshness

## V1 To V2 Data Contract Outline

This section defines the management-level upstream contract that V1 must expose so V2 can assemble, rank, and present recommendations without inventing its own research truth.

This is a control outline, not a final schema.

### Required Opportunity Identity And Detection Fields

V1 must expose enough identity and detection context for V2 to anchor a recommendation:

- opportunity identifier
- detected timestamp
- event or grouping key
- involved market identifiers
- opportunity type
- outcome count
- gross price sum
- gross gap
- detector version
- raw detection context reference

### Required Validation Fields

V1 must expose enough validation context for V2 to determine whether an opportunity is defensible:

- validation status
- validation reason
- validated timestamp
- rule-based relation evidence reference
- semantic validation evidence reference
- resolution validation evidence reference

PR 1 does not require these evidence references to exist as final API fields yet. It establishes that V2 ranking cannot depend on hidden or implied validation outcomes.

### Required Execution And Simulation Fields

V1 must expose enough executable-edge context for V2 to distinguish theoretical ideas from operationally credible recommendations:

- latest simulation status
- intended size
- executable size
- gross cost
- gross payout
- estimated fees
- estimated slippage
- estimated net edge
- fill completion ratio
- simulation reason
- simulated timestamp

### Required Freshness And System Context Fields

V1 must expose enough recency context for V2 review quality:

- latest snapshot time
- latest detection time
- latest validation time when available
- latest simulation time
- latest KPI time when available

### Required Version And Audit Fields

V1 must expose enough lineage for V2 auditability:

- detector version
- scoring version once V2 scoring exists
- source artifact references
- raw context or lineage references needed to reproduce the recommendation rationale

### V2 Contract Rule

V2 may derive recommendation score, tier, warnings, review flags, and read models from these fields. V2 must not replace missing V1 truth with inferred placeholders that appear authoritative.

## Draft Endpoint Categories

The eventual API is expected to expose categories similar to:

- `GET /v2/recommendations`
- `GET /v2/recommendations/{recommendation_id}`
- `GET /v2/recommendations/{recommendation_id}/audit`
- `GET /v2/versions`
- `GET /v2/system/status`

These paths are planning placeholders only. PR 1 does not commit final route names, query semantics, pagination, sorting rules, or response schemas.

## Non-Goals For This Draft

This draft intentionally does not define:

- final JSON schema
- filter semantics
- pagination contract
- sort contract
- authentication model
- write operations
- database schema changes

Those decisions belong to later PRs.
