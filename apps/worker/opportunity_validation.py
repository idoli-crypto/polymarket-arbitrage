from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, Market, MarketSnapshot
from apps.worker.validators.semantic import (
    SemanticValidationInput,
    ValidationMarketInput,
    ValidationSnapshotInput,
    validate_semantic_opportunity,
)


@dataclass(slots=True)
class PersistedValidationResult:
    id: int
    event_group_key: str
    validation_status: str
    validation_reason: str | None
    validated_at: datetime


def validate_pending_opportunities(session: Session) -> list[PersistedValidationResult]:
    opportunities = session.scalars(
        select(DetectedOpportunity)
        .where(DetectedOpportunity.validation_status.is_(None))
        .order_by(DetectedOpportunity.detected_at.asc(), DetectedOpportunity.id.asc())
    ).all()

    persisted_results: list[PersistedValidationResult] = []
    for opportunity in opportunities:
        market_ids = list(opportunity.involved_market_ids)
        markets = _load_markets(session, market_ids)
        latest_snapshots = _load_latest_snapshots(session, market_ids)
        result = validate_semantic_opportunity(
            SemanticValidationInput(
                opportunity_id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                involved_market_ids=market_ids,
            ),
            markets=markets,
            latest_snapshots=latest_snapshots,
        )
        validated_at = datetime.now(timezone.utc)
        opportunity.validation_status = result.validation_status
        opportunity.validation_reason = result.validation_reason
        opportunity.validated_at = validated_at
        opportunity.raw_context = _merge_validation_context(
            opportunity.raw_context,
            result=result,
        )
        persisted_results.append(
            PersistedValidationResult(
                id=opportunity.id,
                event_group_key=opportunity.event_group_key,
                validation_status=result.validation_status,
                validation_reason=result.validation_reason,
                validated_at=validated_at,
            )
        )

    session.commit()
    return persisted_results


def _load_markets(session: Session, market_ids: list[int]) -> list[ValidationMarketInput]:
    if not market_ids:
        return []

    rows = session.scalars(select(Market).where(Market.id.in_(market_ids))).all()
    return [
        ValidationMarketInput(
            market_id=row.id,
            question=row.question,
            condition_id=row.condition_id,
            event_id=row.event_id,
            event_slug=row.event_slug,
            neg_risk=row.neg_risk,
        )
        for row in rows
    ]


def _load_latest_snapshots(
    session: Session,
    market_ids: list[int],
) -> dict[int, ValidationSnapshotInput | None]:
    latest_snapshot_by_market: dict[int, ValidationSnapshotInput | None] = {
        market_id: None for market_id in market_ids
    }
    if not market_ids:
        return latest_snapshot_by_market

    ranked_snapshots = (
        select(
            MarketSnapshot.id.label("snapshot_id"),
            MarketSnapshot.market_id.label("market_id"),
            MarketSnapshot.captured_at.label("captured_at"),
            func.row_number()
            .over(
                partition_by=MarketSnapshot.market_id,
                order_by=(MarketSnapshot.captured_at.desc(), MarketSnapshot.id.desc()),
            )
            .label("snapshot_rank"),
        )
        .where(MarketSnapshot.market_id.in_(market_ids))
        .subquery()
    )

    rows = session.execute(
        select(ranked_snapshots).where(ranked_snapshots.c.snapshot_rank == 1)
    ).mappings()
    for row in rows:
        latest_snapshot_by_market[row["market_id"]] = ValidationSnapshotInput(
            snapshot_id=row["snapshot_id"],
            market_id=row["market_id"],
            captured_at=row["captured_at"],
        )

    return latest_snapshot_by_market


def _merge_validation_context(
    raw_context: dict[str, Any] | None,
    *,
    result,
) -> dict[str, Any]:
    merged = dict(raw_context or {})
    merged["semantic_validation_status"] = result.validation_status
    merged["semantic_validation_reason"] = result.validation_reason
    merged["rules_validation_status"] = result.rules_validation_status
    merged["snapshot_freshness_status"] = result.snapshot_freshness_status
    merged["validation_evidence"] = result.evidence
    return merged
