from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.db.models import DetectedOpportunity, OpportunityKpiSnapshot, RecommendationScoringRun


WORKER_STATUS_RUNNING = "running"
WORKER_STATUS_SUCCESS = "success"
WORKER_STATUS_EMPTY = "empty"
WORKER_STATUS_FAILED = "failed"

FRESHNESS_FRESH = "fresh"
FRESHNESS_STALE = "stale"
FRESHNESS_MISSING = "missing"


@dataclass(frozen=True, slots=True)
class RecommendationFreshnessStatus:
    latest_scoring_run_timestamp: datetime | None
    scoring_worker_status: str
    opportunities_scored_last_run: int
    high_conviction_last_run: int
    review_last_run: int
    blocked_last_run: int
    scoring_version: str | None
    run_reason: str | None
    latest_validation_time: datetime | None
    latest_kpi_time: datetime | None
    freshness_status: str
    stale_reasons: list[str]


def create_scoring_run(
    session: Session,
    *,
    started_at: datetime,
    scoring_version: str,
) -> RecommendationScoringRun:
    run = RecommendationScoringRun(
        started_at=started_at,
        worker_status=WORKER_STATUS_RUNNING,
        opportunities_scored=0,
        high_conviction_count=0,
        review_count=0,
        blocked_count=0,
        scoring_version=scoring_version,
    )
    session.add(run)
    session.flush()
    return run


def finalize_scoring_run(
    session: Session,
    run_id: int,
    *,
    finished_at: datetime,
    worker_status: str,
    opportunities_scored: int,
    high_conviction_count: int,
    review_count: int,
    blocked_count: int,
    run_reason: str | None,
) -> RecommendationScoringRun | None:
    run = session.get(RecommendationScoringRun, run_id)
    if run is None:
        return None
    run.finished_at = finished_at
    run.worker_status = worker_status
    run.opportunities_scored = opportunities_scored
    run.high_conviction_count = high_conviction_count
    run.review_count = review_count
    run.blocked_count = blocked_count
    run.run_reason = run_reason
    session.flush()
    return run


def get_latest_scoring_run(session: Session) -> RecommendationScoringRun | None:
    return session.scalar(
        select(RecommendationScoringRun)
        .order_by(
            RecommendationScoringRun.finished_at.desc().nullslast(),
            RecommendationScoringRun.started_at.desc(),
            RecommendationScoringRun.id.desc(),
        )
        .limit(1)
    )


def get_recommendation_freshness_status(session: Session) -> RecommendationFreshnessStatus:
    latest_run = get_latest_scoring_run(session)
    latest_validation_time = _normalize_datetime(session.scalar(select(func.max(DetectedOpportunity.validated_at))))
    latest_kpi_time = _normalize_datetime(session.scalar(select(func.max(OpportunityKpiSnapshot.created_at))))

    if latest_run is None:
        return RecommendationFreshnessStatus(
            latest_scoring_run_timestamp=None,
            scoring_worker_status="missing",
            opportunities_scored_last_run=0,
            high_conviction_last_run=0,
            review_last_run=0,
            blocked_last_run=0,
            scoring_version=None,
            run_reason="no_scoring_run_recorded",
            latest_validation_time=latest_validation_time,
            latest_kpi_time=latest_kpi_time,
            freshness_status=FRESHNESS_MISSING,
            stale_reasons=["recommendation_scoring_not_run"],
        )

    stale_reasons: list[str] = []
    freshness_status = FRESHNESS_FRESH
    latest_timestamp = _normalize_datetime(latest_run.finished_at or latest_run.started_at)
    if latest_run.worker_status == WORKER_STATUS_FAILED:
        freshness_status = FRESHNESS_STALE
        stale_reasons.append("last_scoring_run_failed")
    if latest_run.worker_status == WORKER_STATUS_RUNNING:
        freshness_status = FRESHNESS_STALE
        stale_reasons.append("last_scoring_run_incomplete")
    if latest_validation_time is not None and latest_timestamp < latest_validation_time:
        freshness_status = FRESHNESS_STALE
        stale_reasons.append("validation_newer_than_scoring")
    if latest_kpi_time is not None and latest_timestamp < latest_kpi_time:
        freshness_status = FRESHNESS_STALE
        stale_reasons.append("kpi_newer_than_scoring")

    return RecommendationFreshnessStatus(
        latest_scoring_run_timestamp=latest_timestamp,
        scoring_worker_status=latest_run.worker_status,
        opportunities_scored_last_run=latest_run.opportunities_scored,
        high_conviction_last_run=latest_run.high_conviction_count,
        review_last_run=latest_run.review_count,
        blocked_last_run=latest_run.blocked_count,
        scoring_version=latest_run.scoring_version,
        run_reason=latest_run.run_reason,
        latest_validation_time=latest_validation_time,
        latest_kpi_time=latest_kpi_time,
        freshness_status=freshness_status,
        stale_reasons=stale_reasons,
    )


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
