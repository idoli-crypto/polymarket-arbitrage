from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Polymarket Arbitrage API"
    app_env: str = "development"
    api_prefix: str = ""

    model_config = SettingsConfigDict(
        env_file="apps/api/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
