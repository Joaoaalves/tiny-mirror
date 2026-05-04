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

    # Optional Seq sink. Empty url disables shipping; the stdout handler
    # remains active either way.
    seq_url: str = ""
    seq_api_key: str = ""

    sync_products_cron: str = "0 2 * * *"
    # Orders run every 30 min. _filter_new_order_ids skips ids already in
    # the local table, so the only Tiny calls are pagination + GET per
    # genuinely new order — well under the 60 req/min budget.
    sync_orders_cron: str = "*/30 * * * *"
    # Daily reconciliation: re-fetch every order whose dataAtualizacao was
    # yesterday, regardless of whether it is already in the DB. Catches
    # status changes (cancellations, deliveries) the incremental cron
    # cannot see, since _filter_new_order_ids skips known ids.
    sync_orders_reconciliation_cron: str = "0 3 * * *"
    # Stock full sync runs daily (03:00 UTC). Stock is refreshed only here
    # — neither order webhooks nor the hourly order cron fan out per-product
    # stock refreshes anymore. Tiny order webhooks are too unreliable to
    # depend on for stock freshness, and the per-product fan-out flooded
    # the queue under the 60 req/min budget.
    sync_stock_cron: str = "0 3 * * *"
    sync_buckets_cron: str = "0 4 * * *"
    token_rotation_cron: str = "0 */2 * * *"

    # Dedup window for manual sync triggers. Repeated POSTs to /sync/* within
    # this many seconds get a 409 instead of fanning out duplicates.
    # Mercado Livre OAuth + stock (optional — leave empty to disable ML sync)
    ml_user_id: str = ""
    ml_client_id: str = ""
    ml_client_secret: str = ""
    ml_refresh_token: str = ""
    ml_access_token: str = ""
    sync_ml_stock_cron: str = "30 3 * * *"

    sync_trigger_lock_seconds: int = 300

    # sync_log watchdog. Runs every N minutes via the scheduler; any sync_log
    # still in 'running' for longer than running_max_minutes is force-closed
    # to 'failed'. Tiny rate limit is ~60 req/min, so a full product fan-out
    # of 661 items can legitimately take 11+ minutes; pick a generous bound
    # so genuinely-running jobs are not killed.
    sync_log_watchdog_cron: str = "*/5 * * * *"
    sync_log_running_max_minutes: int = 90

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
