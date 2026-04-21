from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, Market, MarketSnapshot
from apps.worker.detectors.neg_risk import (
    DETECTOR_VERSION,
    DetectionMarketInput,
    detect_neg_risk_candidates,
)


@dataclass(slots=True)
class PersistedOpportunity:
    id: int
    detected_at: datetime
    detection_window_start: datetime
    event_group_key: str
    involved_market_ids: list[int]
    opportunity_type: str
    outcome_count: int
    gross_price_sum: Decimal
    gross_gap: Decimal
    detector_version: str
    status: str


def scan_and_persist_neg_risk_candidates(
    session: Session,
    *,
    detector_version: str = DETECTOR_VERSION,
) -> list[PersistedOpportunity]:
    market_inputs = load_latest_market_inputs(session)
    candidates = detect_neg_risk_candidates(market_inputs, detector_version=detector_version)

    persisted: list[PersistedOpportunity] = []
    for candidate in candidates:
        existing = session.scalar(
            select(DetectedOpportunity).where(
                DetectedOpportunity.event_group_key == candidate.event_group_key,
                DetectedOpportunity.opportunity_type == candidate.opportunity_type,
                DetectedOpportunity.detector_version == candidate.detector_version,
                DetectedOpportunity.detection_window_start == candidate.detection_window_start,
            )
        )
        if existing is not None:
            persisted.append(
                PersistedOpportunity(
                    id=existing.id,
                    detected_at=existing.detected_at,
                    detection_window_start=existing.detection_window_start,
                    event_group_key=existing.event_group_key,
                    involved_market_ids=existing.involved_market_ids,
                    opportunity_type=existing.opportunity_type,
                    outcome_count=existing.outcome_count,
                    gross_price_sum=existing.gross_price_sum,
                    gross_gap=existing.gross_gap,
                    detector_version=existing.detector_version,
                    status=existing.status,
                )
            )
            continue

        row = DetectedOpportunity(
            detection_window_start=candidate.detection_window_start,
            event_group_key=candidate.event_group_key,
            involved_market_ids=candidate.involved_market_ids,
            opportunity_type=candidate.opportunity_type,
            outcome_count=candidate.outcome_count,
            gross_price_sum=candidate.gross_price_sum,
            gross_gap=candidate.gross_gap,
            detector_version=candidate.detector_version,
            status="detected",
            raw_context=candidate.raw_context,
        )
        session.add(row)
        session.flush()
        persisted.append(
            PersistedOpportunity(
                id=row.id,
                detected_at=row.detected_at,
                detection_window_start=row.detection_window_start,
                event_group_key=row.event_group_key,
                involved_market_ids=row.involved_market_ids,
                opportunity_type=row.opportunity_type,
                outcome_count=row.outcome_count,
                gross_price_sum=row.gross_price_sum,
                gross_gap=row.gross_gap,
                detector_version=row.detector_version,
                status=row.status,
            )
        )

    session.commit()
    return persisted


def load_latest_market_inputs(session: Session) -> list[DetectionMarketInput]:
    ranked_snapshots = (
        select(
            Market.id.label("market_id"),
            Market.polymarket_market_id.label("polymarket_market_id"),
            Market.question.label("question"),
            Market.slug.label("slug"),
            Market.condition_id.label("condition_id"),
            Market.event_id.label("event_id"),
            Market.event_slug.label("event_slug"),
            Market.neg_risk.label("neg_risk"),
            MarketSnapshot.id.label("snapshot_id"),
            MarketSnapshot.captured_at.label("snapshot_captured_at"),
            MarketSnapshot.best_bid.label("best_bid"),
            MarketSnapshot.best_ask.label("best_ask"),
            func.row_number()
            .over(
                partition_by=MarketSnapshot.market_id,
                order_by=(MarketSnapshot.captured_at.desc(), MarketSnapshot.id.desc()),
            )
            .label("snapshot_rank"),
        )
        .join(MarketSnapshot, MarketSnapshot.market_id == Market.id)
        .where(Market.status == "active")
        .subquery()
    )

    rows = session.execute(
        select(ranked_snapshots).where(ranked_snapshots.c.snapshot_rank == 1)
    ).mappings()

    return [
        DetectionMarketInput(
            market_id=row["market_id"],
            polymarket_market_id=row["polymarket_market_id"],
            question=row["question"],
            slug=row["slug"],
            condition_id=row["condition_id"],
            event_id=row["event_id"],
            event_slug=row["event_slug"],
            neg_risk=row["neg_risk"],
            snapshot_id=row["snapshot_id"],
            snapshot_captured_at=row["snapshot_captured_at"],
            best_bid=row["best_bid"],
            best_ask=row["best_ask"],
        )
        for row in rows
    ]
