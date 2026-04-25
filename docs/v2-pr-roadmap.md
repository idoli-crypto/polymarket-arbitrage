# V2 PR Roadmap

## Purpose

This roadmap defines the approved PR sequence for V2. It keeps GitHub as the source of truth for scope, sequencing, and status.

All roadmap items remain inside the recommendation-only boundary.

## PR Sequence

| PR | Title | Status | Scope intent |
| --- | --- | --- | --- |
| PR 1 | V2 architecture freeze and project control docs | `in progress` | Establish the approved V2 architecture, governance, PMO tracker, roadmap, API draft, and dashboard screen map in repository docs only |
| PR 2 | recommendation entity model and versioning contract | `not started` | Define the recommendation record, audit expectations, and versioning contract without introducing auto-trade behavior |
| PR 3 | detection family classification framework | `not started` | Add the classification framework that maps research outputs into approved V2 detection families |
| PR 4 | rule-based relation validation integration | `not started` | Integrate the first validation layer for explicit relation checks across recommendation candidates |
| PR 5 | semantic validation integration | `not started` | Integrate semantic validation into the recommendation qualification flow |
| PR 6 | resolution validation integration | `not started` | Integrate resolution-rule validation so recommendation logic is grounded in actual settlement criteria |
| PR 7 | executable-edge validation integration | `not started` | Integrate executable-edge checks so recommendation output is not based on theoretical pricing alone |
| PR 8 | simulation validation integration | `not started` | Integrate simulation-backed validation into recommendation qualification and confidence review |
| PR 9 | persistence validation and audit lineage | `not started` | Ensure recommendation outputs have persistence controls and traceable audit lineage |
| PR 10 | recommendation scoring and ranking layer | `not started` | Build the scoring and prioritization layer for recommendation ordering |
| PR 11 | V2 read-only recommendation API | `not started` | Expose read-only recommendation views for downstream dashboard use |
| PR 12 | dashboard information architecture and navigation | `not started` | Establish the dashboard structure and navigation model for recommendation review |
| PR 13 | dashboard recommendation decision-support views | `not started` | Implement the dashboard views used to inspect ranked recommendations and evidence |
| PR 14 | acceptance hardening, audit review, and release gate | `not started` | Run final acceptance, audit review, and release gating for the V2 recommendation layer |

## Control Rules

- No roadmap item authorizes trade execution.
- No roadmap item changes the recommendation-only product definition.
- Detector rewrites are not implied by this roadmap.
- Schema, API, and dashboard details remain controlled by their own future PR scopes.

## Status Rule

PR 1 is the active work item. PR 2 through PR 14 remain blocked until PR 1 documentation is approved.
