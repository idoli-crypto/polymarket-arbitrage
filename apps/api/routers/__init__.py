from fastapi import APIRouter

from apps.api.routers.health import router as health_router
from apps.api.routers.research import router as research_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(research_router)
