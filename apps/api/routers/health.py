from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.db.session import get_db_session
from apps.api.services.health import (
    HealthResponse,
    ReadinessResponse,
    get_health_status,
    get_readiness_status,
)

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return get_health_status()


@router.get("/health/ready", response_model=ReadinessResponse)
def readiness_check(session: Session = Depends(get_db_session)) -> ReadinessResponse:
    try:
        return get_readiness_status(session)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="database_unavailable") from exc
