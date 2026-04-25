from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import func, select

from apps.api.db.models import RecommendationScore
from apps.api.db.session import SessionLocal
from apps.api.repositories.recommendation_scoring import (
    WORKER_STATUS_EMPTY,
    WORKER_STATUS_FAILED,
    WORKER_STATUS_SUCCESS,
    create_scoring_run,
    finalize_scoring_run,
)
from apps.worker.recommendation_scoring import score_pending_recommendations
from apps.worker.recommendation_scoring import (
    SCORING_VERSION,
    TIER_BLOCKED,
    TIER_HIGH_CONVICTION,
    TIER_REVIEW,
)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    started_at = datetime.now(timezone.utc)
    with SessionLocal() as session:
        run = create_scoring_run(
            session,
            started_at=started_at,
            scoring_version=SCORING_VERSION,
        )
        run_id = run.id
        session.commit()

    try:
        with SessionLocal() as session:
            persisted = score_pending_recommendations(session)
            total_scores = session.scalar(select(func.count()).select_from(RecommendationScore))

        with SessionLocal() as session:
            finalize_scoring_run(
                session,
                run_id,
                finished_at=datetime.now(timezone.utc),
                worker_status=(WORKER_STATUS_SUCCESS if persisted else WORKER_STATUS_EMPTY),
                opportunities_scored=len(persisted),
                high_conviction_count=sum(1 for row in persisted if row.tier == TIER_HIGH_CONVICTION),
                review_count=sum(1 for row in persisted if row.tier == TIER_REVIEW),
                blocked_count=sum(1 for row in persisted if row.tier == TIER_BLOCKED),
                run_reason=(None if persisted else "no_unscored_validated_opportunities"),
            )
            session.commit()
    except Exception as exc:
        with SessionLocal() as session:
            finalize_scoring_run(
                session,
                run_id,
                finished_at=datetime.now(timezone.utc),
                worker_status=WORKER_STATUS_FAILED,
                opportunities_scored=0,
                high_conviction_count=0,
                review_count=0,
                blocked_count=0,
                run_reason=str(exc)[:255],
            )
            session.commit()
        raise

    logging.info(
        "Recommendation scoring completed. scored_opportunities=%s total_scores=%s reads=validated_opportunities+validation_results+simulation_results+opportunity_kpi_snapshots writes=recommendation_scores+recommendation_scoring_runs freshness_source=/recommendations/status",
        len(persisted),
        total_scores,
    )
    for result in persisted:
        logging.info(
            "recommendation opportunity_id=%s tier=%s score=%s manual_review_required=%s created_at=%s",
            result.opportunity_id,
            result.tier,
            result.score,
            result.manual_review_required,
            result.created_at.isoformat(),
        )


if __name__ == "__main__":
    main()
