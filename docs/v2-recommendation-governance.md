# V2 Recommendation Governance

## Purpose

This document defines the governance rules for all V2 recommendation outputs. It is the control standard for what the system may surface, how it must justify that output, and which evidence must be preserved for audit.

## Governing Principles

The following principles are mandatory:

1. GitHub is the source of truth
2. recommendation only
3. no auto trade
4. logic first, price second
5. title similarity alone is never enough
6. midpoint is never enough
7. all recommendation outputs must be auditable
8. all detector and scoring versions must be stored

## Recommendation Standard

A V2 recommendation is valid only when it is a reviewable output for a human decision-maker. It is not an instruction to trade and it is never an automated action trigger.

The recommendation record must explain:

- what detection family produced the candidate
- which validation layers were satisfied
- what executable edge evidence exists
- which detector and scoring versions generated the output
- what source inputs and rationale support the recommendation

## Minimum Evidence Requirements

Every recommendation must carry at least the following evidence categories:

| Evidence category | Required content |
| --- | --- |
| Family classification | The approved detection family assigned to the candidate |
| Validation evidence by layer | Outcome or evidence reference for each layer in the validation stack |
| Executable-edge evidence | Evidence that the opportunity is defensible beyond theoretical pricing alone |
| Version references | Stored detector version and scoring version identifiers |
| Auditability | Inputs, timestamps, rationale, and traceable lineage to persisted research artifacts |

## Must-Not-Recommend Conditions

The system must not surface a recommendation when any of the following is true:

- the candidate is supported only by title similarity
- the candidate is supported only by midpoint pricing
- the detection family is missing or ambiguous
- one or more validation layers have no evidence record
- executable-edge evidence is absent
- detector or scoring version references are missing
- the output cannot be traced back to persisted research artifacts

## Validation Stack Requirement

All recommendations must be evaluated against the full validation stack:

1. rule based relation validation
2. semantic validation
3. resolution validation
4. executable edge validation
5. simulation validation
6. persistence validation

No single layer is sufficient by itself to justify recommendation output.

## Versioning Requirement

All future recommendation-producing logic must be versioned. At minimum, the repository must preserve:

- detector version
- scoring version
- version-to-output traceability

This requirement exists for reproducibility, review, rollback analysis, and audit defense.

## Ranking And Review Model Outline

This outline defines the management-level shape of the recommendation ranking model. It is not the final scoring formula.

### Ranking Objectives

The ranking model must prioritize recommendations using the governance rule `logic first, price second`.

That means the ranking flow must first determine whether the recommendation is logically defensible and auditable, and only then use pricing or executable-edge strength to order comparable candidates.

### Required Ranking Inputs

The future ranking layer must consider, at minimum:

- detection-family classification
- validation-stack completeness and strength
- executable-edge evidence
- simulation support
- freshness of upstream research inputs
- audit completeness
- detector version and scoring version traceability

### Required Ranking Outputs

Every ranked recommendation must eventually expose:

- `score`: the composite ranking output
- `tier`: the management-level priority bucket derived from the score and evidence quality
- `warnings`: explicit reasons the operator should treat the recommendation cautiously
- `review_flag`: an explicit marker that manual review is required before operational reliance

### Warning Conditions

Warnings are expected when recommendation quality is degraded but not automatically disqualifying, such as:

- stale upstream inputs
- partial validation support
- weak executable-edge evidence
- simulation weakness
- family ambiguity that still requires human judgment

### Review-Flag Conditions

A review flag is required when the recommendation can still be surfaced for analysis but must not be treated as straightforward:

- resolution complexity is high
- semantic similarity is materially uncertain
- executable-edge evidence is borderline
- audit lineage is incomplete but still sufficient for limited review
- multiple warnings are present together

### Non-Goals For PR 1

PR 1 does not define:

- a final scoring formula
- final numeric score bands
- final tier labels
- final warning taxonomy
- final review-flag thresholds

Those decisions belong to later implementation PRs, but this outline is now the management baseline they must follow.

## Repository Control

GitHub is the official control surface for:

- approved architecture
- governance rules
- PR sequencing
- acceptance criteria
- future changes to recommendation standards

If code or runtime behavior diverges from the approved repository documentation, the documentation remains the authoritative reference until a new approved PR updates it.
