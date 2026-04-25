from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.config.settings import get_settings


class HealthResponse(BaseModel):
    status: str


class ReadinessResponse(BaseModel):
    status: str
    database: str
    app_env: str


def get_health_status() -> HealthResponse:
    return HealthResponse(status="ok")


def get_readiness_status(session: Session) -> ReadinessResponse:
    session.execute(text("SELECT 1"))
    settings = get_settings()
    return ReadinessResponse(status="ok", database="ok", app_env=settings.app_env)
