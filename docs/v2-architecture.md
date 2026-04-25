# V2 Architecture

## Purpose

V2 formalizes the project as a recommendation dashboard for Polymarket arbitrage research. Its job is to find, validate, score, rank, and present candidate opportunities for human review.

V2 is recommendation-only. It does not place trades, manage orders, manage positions, or automate execution.

## Product Definition

The system presents arbitrage recommendations with enough evidence for a human operator to review the logic, executable edge, and audit trail before taking any action outside the system.

The product does not execute trades.

## Architecture Boundary

### V1 Responsibilities

V1 remains the research engine and source of truth for:

- ingestion
- snapshots
- detection
- validation
- simulation
- research KPI

### V2 Responsibilities

V2 adds the recommendation layer for:

- recommendation assembly from V1 research outputs
- ranking and prioritization
- read-only API exposure for recommendation views
- dashboard decision support
- audit-oriented presentation of evidence and version history

### Hard Boundaries For PR 1

This PR does not:

- add product logic
- modify detectors
- change database schema
- implement API behavior
- build dashboard UI
- add wallet, order, or execution flows

## Operational Source Of Truth Map

This section defines ownership between V1 and V2 at an operational level.

### What V1 Stores

V1 remains the system of record for persisted research artifacts, including:

- market metadata
- market snapshots
- detected opportunities
- validation outcomes
- execution simulation outcomes
- research KPI snapshots
- timestamps, raw context, and detector-version references already attached to those research artifacts

### What V2 Derives

V2 derives recommendation-layer outputs from V1 research truth, including:

- recommendation eligibility
- detection-family classification
- recommendation score
- recommendation tier
- warnings
- review flags
- audit-oriented read models for API and dashboard presentation

### What V2 Only Reads

V2 reads, but does not redefine, the following upstream truth from V1:

- market and opportunity identifiers
- stored pricing and depth context
- validation status and rationale
- simulation status and executable-edge context
- KPI and freshness timestamps
- persisted version references and raw audit context already attached to V1 outputs

### What V2 Never Creates Itself

V2 must never originate or overwrite the following classes of truth:

- raw market data
- order book snapshots
- detector output
- validation output
- simulation output
- research KPI truth
- trade, order, wallet, or position records

If V2 needs one of these inputs, it must read the persisted V1 artifact rather than create a parallel source of truth.

## Official V2 Terms

The following terms are the official vocabulary for V2 documents and future implementation PRs:

| Term | Meaning |
| --- | --- |
| `recommendation` | A candidate arbitrage opportunity prepared for human review. |
| `detection family` | The logical relationship category used to classify a candidate opportunity. |
| `validation stack` | The ordered set of checks used to confirm a recommendation is logically and operationally defensible. |
| `audit trail` | The evidence record that explains why a recommendation exists, what inputs supported it, and which versions produced it. |
| `versioned detector/scoring logic` | Persisted detector and scoring identifiers required for reproducibility and review. |
| `recommendation-only` | A governance boundary stating that the system informs decisions but does not trade. |

## Core Detection Families

V2 focuses on six approved detection families:

1. timeframe nesting
2. cross market logic
3. semantic near duplicates
4. resolution divergence
5. Neg Risk conversion
6. intra market parity as baseline control

These families define the recommendation search space. They do not authorize detector changes in PR 1.

## Validation Stack

All V2 recommendations must be evaluated through the approved validation stack:

1. rule based relation validation
2. semantic validation
3. resolution validation
4. executable edge validation
5. simulation validation
6. persistence validation

The stack is ordered from logical relationship checks to operational evidence and audit persistence.

## High-Level V2 Flow

V2 consumes persisted V1 outputs and turns them into reviewable recommendations:

1. ingest V1 research outputs
2. classify by detection family
3. evaluate through the validation stack
4. score and rank qualified recommendations
5. expose read-only recommendation views
6. present decision-support screens with audit evidence

## Governance Alignment

V2 is governed by the following principles:

- GitHub is the source of truth
- recommendation only
- no auto trade
- logic first, price second
- title similarity alone is never enough
- midpoint is never enough
- all recommendation outputs must be auditable
- all detector and scoring versions must be stored

## Acceptance Boundary For PR 1

PR 1 is complete when the repository contains a consistent set of V2 control documents that:

- define the recommendation-only product boundary
- preserve V1 as the research source of truth
- name the approved detection families
- name the approved validation stack
- define governance, roadmap, API draft, dashboard map, and PMO tracking

No code behavior changes are part of this PR.
