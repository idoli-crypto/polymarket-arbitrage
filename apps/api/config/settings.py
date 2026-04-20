from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Polymarket Arbitrage API"
    app_env: str = "development"
    api_prefix: str = ""
    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/polymarket_arbitrage"
    database_echo: bool = False

    model_config = SettingsConfigDict(
        env_file="apps/api/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def sqlalchemy_database_url(self) -> str:
        if self.database_url.startswith("postgres://"):
            return self.database_url.replace("postgres://", "postgresql+psycopg://", 1)
        if self.database_url.startswith("postgresql://"):
            return self.database_url.replace("postgresql://", "postgresql+psycopg://", 1)
        return self.database_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
