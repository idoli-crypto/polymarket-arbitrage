from fastapi import APIRouter

from apps.api.services.health import HealthResponse, get_health_status

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return get_health_status()
