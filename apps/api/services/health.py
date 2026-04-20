from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str


def get_health_status() -> HealthResponse:
    return HealthResponse(status="ok")
