"""Application configuration loaded from environment variables and `.env`.

Use the singleton ``settings`` exported at the bottom of this module — never
instantiate ``Settings()`` directly elsewhere.
"""

from __future__ import annotations

import logging

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_VALID_APP_ENVS = {"development", "production"}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    database_url: str
    redis_url: str
    rabbitmq_url: str

    tiny_client_id: str
    tiny_client_secret: str
    tiny_access_token: str
    tiny_refresh_token: str
    # Optional: when set, webhook handlers reject payloads whose `cnpj`
    # field does not match. Leave empty to accept any cnpj (still logged).
    tiny_expected_cnpj: str = ""

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "development"

    log_level: str = "INFO"

    sync_products_cron: str = "0 2 * * *"
    sync_orders_cron: str = "0 * * * *"
    sync_stock_cron: str = "0 3 * * *"
    sync_buckets_cron: str = "0 4 * * *"
    token_rotation_cron: str = "0 */2 * * *"

    @field_validator("database_url", mode="before")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("DATABASE_URL must be a string")
        if v.startswith("postgresql+asyncpg://"):
            return v
        if v.startswith("postgresql://"):
            logging.getLogger(__name__).warning(
                "DATABASE_URL is missing the asyncpg driver prefix; "
                "rewriting 'postgresql://' -> 'postgresql+asyncpg://'."
            )
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        raise ValueError(
            "DATABASE_URL must start with 'postgresql+asyncpg://' "
            "(or 'postgresql://' which will be rewritten)."
        )

    @field_validator("app_env", mode="before")
    @classmethod
    def validate_app_env(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("APP_ENV must be a string")
        normalized = v.lower()
        if normalized not in _VALID_APP_ENVS:
            raise ValueError(f"APP_ENV must be one of {sorted(_VALID_APP_ENVS)}, got {v!r}")
        return normalized

    @field_validator("log_level", mode="before")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("LOG_LEVEL must be a string")
        normalized = v.upper()
        if normalized not in _VALID_LOG_LEVELS:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}, got {v!r}")
        return normalized

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        return self.app_env == "development"


settings = Settings()  # type: ignore[call-arg]
