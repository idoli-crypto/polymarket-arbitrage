from __future__ import annotations

import logging

from sqlalchemy import func, select

from apps.api.db.models import DetectedOpportunity
from apps.api.db.session import SessionLocal
from apps.worker.opportunity_validation import validate_pending_opportunities


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    with SessionLocal() as session:
        persisted = validate_pending_opportunities(session)
        total_count = session.scalar(select(func.count()).select_from(DetectedOpportunity))
        valid_count = session.scalar(
            select(func.count())
            .select_from(DetectedOpportunity)
            .where(DetectedOpportunity.validation_status == "valid")
        )
        rejected_count = session.scalar(
            select(func.count())
            .select_from(DetectedOpportunity)
            .where(DetectedOpportunity.validation_status == "rejected")
        )

    logging.info(
        "Opportunity validation completed. validated_opportunities=%s total_detected_opportunities=%s valid=%s rejected=%s",
        len(persisted),
        total_count,
        valid_count,
        rejected_count,
    )
    for result in persisted:
        logging.info(
            "opportunity id=%s event_group_key=%s validation_status=%s validation_reason=%s validated_at=%s",
            result.id,
            result.event_group_key,
            result.validation_status,
            result.validation_reason,
            result.validated_at.isoformat(),
        )


if __name__ == "__main__":
    main()
