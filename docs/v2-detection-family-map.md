# V2 Detection Family Map

## Purpose

This document is the control map for the approved V2 detection families. It defines the logical categories that may produce recommendation candidates and the validation emphasis each category requires.

This is a taxonomy document, not detector implementation guidance.

## Family Map

| Detection family | Relationship captured | Why it can generate a candidate | Most critical validation layers | Difference from intra market parity baseline control |
| --- | --- | --- | --- | --- |
| timeframe nesting | Markets that describe nested or overlapping time windows around the same underlying event or condition | Misaligned pricing can appear when broader and narrower windows imply inconsistent probability structure | rule based relation validation, resolution validation, executable edge validation | Parity tests a single market's internal pricing balance; timeframe nesting compares linked windows across related contracts |
| cross market logic | Markets whose outcomes are connected by explicit logical dependency across separate markets | A candidate appears when related markets imply incompatible combined pricing or payout logic | rule based relation validation, semantic validation, resolution validation | Parity stays inside one market; cross market logic depends on multi-market consistency |
| semantic near duplicates | Markets that are not exact duplicates in storage but appear economically close enough to require review | Similar markets can diverge in price even when they may resolve in nearly the same way | semantic validation, resolution validation, executable edge validation | Parity does not depend on language similarity; semantic near duplicates must prove more than title overlap |
| resolution divergence | Markets that look similar at the title level but have materially different resolution criteria or boundary rules | Mispriced candidates can appear when the market surface looks aligned while the resolution logic is not | resolution validation, rule based relation validation, persistence validation | Parity assumes one market structure; resolution divergence exists to prevent false equivalence across markets |
| Neg Risk conversion | Bundles or related contracts that convert market structure into a payout profile similar to Neg Risk | A candidate appears when bundle cost and payout logic create a defensible edge after validation | executable edge validation, simulation validation, persistence validation | Parity is the baseline internal control; Neg Risk conversion is a structured payout transformation across contracts |
| intra market parity as baseline control | Internal consistency checks within a single market or tightly coupled market set used as the control benchmark | It establishes whether a market family behaves normally before higher-complexity recommendation logic is trusted | executable edge validation, simulation validation, persistence validation | This is the baseline control itself and serves as the reference point for higher-complexity families |

## Control Notes

- `intra market parity as baseline control` is required as the benchmark family for calibration and comparison.
- `semantic near duplicates` must never rely on title similarity alone.
- `resolution divergence` is both a candidate family and a false-positive defense layer for other families.
- `Neg Risk conversion` remains in scope as an approved family, but PR 1 does not modify the existing Neg Risk detector.

## Validation Interpretation

Each family must eventually produce recommendation evidence across all six validation layers:

1. rule based relation validation
2. semantic validation
3. resolution validation
4. executable edge validation
5. simulation validation
6. persistence validation

The "most critical" column above shows where each family is most likely to fail or require deeper review. It does not reduce the full validation requirement.

## Governance Reminder

Family classification is necessary but not sufficient. A recommendation may only be surfaced when the family relationship, validation evidence, executable edge, and audit trail are all present.
