from __future__ import annotations

from datetime import datetime, timezone
import unittest

from apps.worker.validators.semantic import (
    RULES_VALIDATION_STATUS_PENDING,
    SNAPSHOT_FRESHNESS_STATUS_PENDING,
    VALIDATION_REASON_DUPLICATE_MARKETS,
    VALIDATION_REASON_EVENT_MISMATCH,
    VALIDATION_REASON_INCOMPLETE_OUTCOMES,
    VALIDATION_REASON_MISSING_SNAPSHOT,
    VALIDATION_REASON_NOT_NEG_RISK,
    VALIDATION_STATUS_REJECTED,
    VALIDATION_STATUS_VALID,
    SemanticValidationInput,
    ValidationMarketInput,
    ValidationSnapshotInput,
    validate_semantic_opportunity,
)


def _market(
    *,
    market_id: int,
    question: str,
    condition_id: str | None = None,
    event_id: str = "event-1",
    event_slug: str = "event-1",
    neg_risk: bool = True,
) -> ValidationMarketInput:
    return ValidationMarketInput(
        market_id=market_id,
        question=question,
        condition_id=condition_id,
        event_id=event_id,
        event_slug=event_slug,
        neg_risk=neg_risk,
    )


def _snapshot(market_id: int, snapshot_id: int | None = None) -> ValidationSnapshotInput:
    return ValidationSnapshotInput(
        snapshot_id=snapshot_id or market_id * 10,
        market_id=market_id,
        captured_at=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
    )


class SemanticValidatorTests(unittest.TestCase):
    def test_accepts_valid_group(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=1,
                event_group_key="event-1",
                involved_market_ids=[1, 2],
            ),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="condition-1"),
                _market(market_id=2, question="Will Bob win?", condition_id="condition-2"),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_status, VALIDATION_STATUS_VALID)
        self.assertIsNone(result.validation_reason)
        self.assertEqual(result.rules_validation_status, RULES_VALIDATION_STATUS_PENDING)
        self.assertEqual(result.snapshot_freshness_status, SNAPSHOT_FRESHNESS_STATUS_PENDING)
        self.assertEqual(result.evidence["event_ids_seen"], ["event-1"])
        self.assertEqual(result.evidence["selected_snapshot_ids"], {"1": 10, "2": 20})

    def test_rejects_when_event_ids_do_not_match(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="condition-1"),
                _market(
                    market_id=2,
                    question="Will Bob win?",
                    condition_id="condition-2",
                    event_id="event-2",
                ),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_status, VALIDATION_STATUS_REJECTED)
        self.assertEqual(result.validation_reason, VALIDATION_REASON_EVENT_MISMATCH)

    def test_rejects_when_event_slugs_do_not_match(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="condition-1"),
                _market(
                    market_id=2,
                    question="Will Bob win?",
                    condition_id="condition-2",
                    event_slug="event-2",
                ),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_EVENT_MISMATCH)

    def test_rejects_when_event_group_key_does_not_match_shared_event_id(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "detector-group", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="condition-1"),
                _market(market_id=2, question="Will Bob win?", condition_id="condition-2"),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_EVENT_MISMATCH)

    def test_rejects_when_any_market_has_no_snapshot(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="condition-1"),
                _market(market_id=2, question="Will Bob win?", condition_id="condition-2"),
            ],
            latest_snapshots={1: _snapshot(1), 2: None},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_MISSING_SNAPSHOT)
        self.assertEqual(result.evidence["missing_snapshot_market_ids"], [2])
        self.assertEqual(result.snapshot_freshness_status, SNAPSHOT_FRESHNESS_STATUS_PENDING)

    def test_rejects_when_group_has_fewer_than_two_markets(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1]),
            markets=[_market(market_id=1, question="Will Alice win?", condition_id="condition-1")],
            latest_snapshots={1: _snapshot(1)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_INCOMPLETE_OUTCOMES)

    def test_rejects_when_any_market_is_not_neg_risk(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="condition-1"),
                _market(
                    market_id=2,
                    question="Will Bob win?",
                    condition_id="condition-2",
                    neg_risk=False,
                ),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_NOT_NEG_RISK)

    def test_rejects_when_market_id_is_duplicated(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 1]),
            markets=[_market(market_id=1, question="Will Alice win?", condition_id="condition-1")],
            latest_snapshots={1: _snapshot(1)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_DUPLICATE_MARKETS)

    def test_rejects_duplicate_condition_ids(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id="shared-condition"),
                _market(market_id=2, question="Will Bob win?", condition_id="shared-condition"),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_DUPLICATE_MARKETS)

    def test_rejects_duplicate_questions_when_condition_ids_are_missing(self) -> None:
        result = validate_semantic_opportunity(
            SemanticValidationInput(1, "event-1", [1, 2]),
            markets=[
                _market(market_id=1, question="Will Alice win?", condition_id=None),
                _market(market_id=2, question="  will   alice win?  ", condition_id=None),
            ],
            latest_snapshots={1: _snapshot(1), 2: _snapshot(2)},
        )

        self.assertEqual(result.validation_reason, VALIDATION_REASON_DUPLICATE_MARKETS)


if __name__ == "__main__":
    unittest.main()
