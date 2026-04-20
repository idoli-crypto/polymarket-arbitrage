from __future__ import annotations

import logging

from sqlalchemy import func, select

from apps.api.db.models import DetectedOpportunity
from apps.api.db.session import SessionLocal
from apps.worker.neg_risk_detection import scan_and_persist_neg_risk_candidates


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with SessionLocal() as session:
        persisted = scan_and_persist_neg_risk_candidates(session)
        total_count = session.scalar(select(func.count()).select_from(DetectedOpportunity))

    logging.info(
        "Neg Risk detection completed. persisted_candidates=%s total_detected_opportunities=%s",
        len(persisted),
        total_count,
    )
    for candidate in persisted:
        logging.info(
            "opportunity id=%s event_group_key=%s type=%s outcome_count=%s gross_price_sum=%s gross_gap=%s status=%s",
            candidate.id,
            candidate.event_group_key,
            candidate.opportunity_type,
            candidate.outcome_count,
            candidate.gross_price_sum,
            candidate.gross_gap,
            candidate.status,
        )


if __name__ == "__main__":
    main()
