from fastapi import FastAPI

from apps.api.config.settings import get_settings
from apps.api.routers import api_router


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)
    app.include_router(api_router, prefix=settings.api_prefix)
    return app


app = create_app()
