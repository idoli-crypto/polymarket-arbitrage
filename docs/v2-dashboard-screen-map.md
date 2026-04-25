# V2 Dashboard Screen Map

## Status

Planning artifact only. This document maps the intended V2 decision-support screens. It does not define implemented routes, UI components, or frontend behavior in PR 1.

## Purpose

The V2 dashboard is a recommendation review surface. It helps a human operator inspect ranked opportunities, understand supporting evidence, and decide whether an opportunity is credible enough for external action outside the system.

The dashboard does not place trades.

## Screen Map

| Screen | Purpose | Primary user decision supported |
| --- | --- | --- |
| Recommendation overview / queue | Present the ranked set of recommendation candidates with family, score, validation summary, and freshness context | Which recommendations deserve immediate review first |
| Recommendation detail | Show the full context for a single recommendation, including logic, edge summary, and supporting artifacts | Whether the recommendation is strong enough to keep, compare, or reject |
| Audit trail / evidence panel | Expose validation evidence, timestamps, rationale, lineage, and version history | Whether the recommendation is auditable and defensible |
| Family and validation status view | Show family classification and validation stack status across recommendations | Whether issues cluster by family or validation layer and where review attention is needed |
| System status / freshness context | Show the recency and availability of research inputs and downstream recommendation context | Whether the displayed recommendation set is current enough to trust for review |

## Required Information Themes

Each screen should eventually draw from the same control concepts:

- recommendation
- detection family
- validation stack
- executable-edge evidence
- audit trail
- versioned detector/scoring logic

## Design Boundary

This document intentionally does not define:

- final route names
- page layout
- component tree
- styling
- interaction details
- implementation sequence within the frontend codebase

Those decisions belong to later implementation PRs.
