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
        extra="ignore",
    )

    database_url: str
    redis_url: str
    rabbitmq_url: str

    tiny_client_id: str
    tiny_client_secret: str
    tiny_access_token: str
    tiny_refresh_token: str
    # Tiny v2 static API token (used for write ops and stock history sync)
    tiny_v2_token: str = ""
    # Optional: when set, webhook handlers reject payloads whose `cnpj`
    # field does not match. Leave empty to accept any cnpj (still logged).
    tiny_expected_cnpj: str = ""

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "development"

    # API auth (X-API-Key header).
    # Required in production. In development the empty default is accepted so
    # local dev works without ceremony; production startup fails fast if unset.
    api_key: str = ""
    # Comma-separated IPs that bypass the X-API-Key check. The default covers
    # loopback (uvicorn behind nginx on the same host) plus the VPS public IP
    # (scripts on the VPS that curl via the public hostname). Add commas for
    # extras; whitespace is trimmed. Set to empty string to disable bypass.
    api_key_ip_allowlist: str = "127.0.0.1,::1,212.85.1.135"

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
    # Mercado Livre OAuth (optional — leave empty to disable ML overlay).
    # When set, the per-product stock sync also pulls the Full ML
    # available_quantity for that SKU and overwrites the (unreliable)
    # Tiny "Full Mercado Livre" deposit row in stock_deposits.
    ml_user_id: str = ""
    ml_client_id: str = ""
    ml_client_secret: str = ""
    ml_refresh_token: str = ""
    ml_access_token: str = ""

    sync_trigger_lock_seconds: int = 300

    # sync_log watchdog. Runs every N minutes via the scheduler; any sync_log
    # still in 'running' for longer than running_max_minutes is force-closed
    # to 'failed'. Tiny rate limit is ~60 req/min, so a full product fan-out
    # of 661 items can legitimately take 11+ minutes; pick a generous bound
    # so genuinely-running jobs are not killed.
    # Invoice sync. Runs daily at 05:00 UTC to pick up any NFs not yet mirrored.
    # The incremental path (2-day lookback) also fires automatically after each
    # order sync cycle to keep NF coverage fresh.
    sync_invoices_cron: str = "0 5 * * *"
    # Stock history (deposit snapshots): daily at 01:00 UTC
    sync_stock_history_cron: str = "0 1 * * *"
    # Purchase orders: weekly on Sunday at 06:00 UTC
    sync_purchase_orders_cron: str = "0 6 * * 0"
    # ML listings (full snapshot of seller's active listings): daily at 00:30 UTC
    sync_ml_listings_cron: str = "30 0 * * *"
    # ML-only Full stock refresh. Runs every 15 min to keep the 'Full
    # Mercado Livre' deposit row in stock_deposits fresh against ML's
    # Inventory API. Bypasses Tiny entirely so it stays well under the
    # Tiny rate limit; only ~100 FL-exposed products x 1 ML call each.
    # Daily Tiny stock_full_sync still runs at 03:00 UTC for the other
    # deposits (galpão, A Caminho, etc.).
    sync_ml_fl_stock_cron: str = "*/15 * * * *"
    # Twice-daily Tiny correction (06:00 and 18:00 UTC): detect mismatch
    # between Tiny FL deposit saldo and our DB (= ML truth), apply tipo=B
    # balance for base SKUs that have *settled* in Tiny accounting (no
    # pending orders/NFs — see fl_stock_correction_service._load_candidates).
    # Captures full forensic snapshot in fl_stock_corrections_log so each
    # detected drift can be later investigated.
    #
    # The point of this job is NOT to be a permanent drift patcher running
    # every hour — that would cause double-baixa when our correction and
    # Tiny's own NF-driven decrement land out of order. The point is to
    # surface (via the forensic log) the *cases* where Tiny's accounting
    # is wrong (phantom products, cancelled NFs that never reversed stock,
    # kit decomposition bugs, etc.) so the underlying Tiny problem can be
    # fixed at the source. Twice/day is enough to keep base SKUs aligned
    # with ML reality without racing the Tiny invoicing pipeline.
    sync_fl_correction_cron: str = "0 6,18 * * *"
    # Daily phantom detection: identifies SKUs where the Tiny catalog has
    # excluded duplicates AND ML orders kept arriving (= the listing's
    # SELLER_SKU points to nothing and Tiny auto-creates a phantom per
    # order). Writes to phantom_products_log for operator review.
    # Runs after products sync (02:00) and ml_listings sync (00:30).
    sync_phantom_detection_cron: str = "30 6 * * *"

    sync_log_watchdog_cron: str = "*/5 * * * *"
    sync_log_running_max_minutes: int = 90
    # Fulfillment reception scan: poll ML INBOUND_RECEPTION every 6h to mark
    # pending transfers as received once stock arrives at Full ML CD.
    sync_fulfillment_reception_cron: str = "0 */6 * * *"

    # DIFAL (Diferencial de Alíquota) tax — fallback when the GAS payload
    # does not include `difalPct` (legacy single-MLB endpoint). Currently
    # 11.5%; the GAS bulk endpoint sends the live value as part of the
    # response so this value is only used in degraded/legacy paths.
    margin_difal_pct: float = 0.115

    # Daily job that (1) refreshes GAS cost snapshots for every active MLB
    # and (2) recomputes ml_promo_caps targeting 10% margin / 30% max
    # discount. Runs after products (02:00), stock (03:00), manual_status
    # (04:30) so caps are based on the freshest cost data of the day.
    sync_ml_promo_recompute_cron: str = "0 5 * * *"

    # Daily job that refreshes ml_catalog_status by calling
    # /items/{MLB}/price_to_win for every active MLB. Reads stay in DB so
    # the dry-run analysis runs in seconds. Scheduled BEFORE the cap
    # recompute so the analyze endpoint always uses fresh catalog data.
    sync_ml_catalog_status_cron: str = "30 4 * * *"

    # Controle 4.0 GAS Web App. Single deployment URL with action routing:
    #   ?action=manual_status         → SKU color tags from GERAL B+C
    #   ?action=costs_all             → bulk dump of every MLB cost row
    #   ?action=cost&mlbid=MLB...     → single-MLB cost (legacy)
    # Empty URL disables every dependent job.
    gas_base_url: str = ""
    gas_token: str = ""
    # HTTP timeout for the GAS call (cold start can be ~5s; bulk dump may be ~20s).
    gas_http_timeout_seconds: float = 60.0

    # Daily at 04:30 UTC — manual_status sync runs after products (02:00)
    # and stock_full_sync (03:00).
    sync_manual_status_cron: str = "30 4 * * *"

    # Webhook-driven FL transfer detection. When a Tiny stock webhook arrives
    # and the raw 'Full Mercado Livre' deposit value grew vs the previous
    # snapshot, we infer the operator did a manual Tiny transfer and insert
    # a pending fulfillment_transfers row (source='tiny_webhook'). The
    # idempotency window prevents duplicate rows if Tiny retries on 5xx or
    # the stock cron races the webhook.
    fl_webhook_delta_idempotency_minutes: int = 30

    # Corroboration threshold for the FL webhook delta detector. When the
    # Tiny FL deposit grows by N units we accept it as a real transfer only
    # if the galpão deposit dropped by at least N * this fraction in the
    # same window. Sale cancellations and Tiny↔ML reconciliations leave
    # galpão untouched, so a low |galpao_delta| flags them as
    # false-positives and we skip the insert.
    #
    # 0.8 = "galpão must have dropped by 80% of the FL gain". Calibrated to
    # tolerate small races (operator types the transfer in two passes) and
    # to catch obvious cancellations (galpao_delta ≈ 0).
    fl_webhook_galpao_corroboration_ratio: float = 0.8

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

    @property
    def api_key_ip_allowlist_set(self) -> frozenset[str]:
        if not self.api_key_ip_allowlist:
            return frozenset()
        return frozenset(
            entry.strip() for entry in self.api_key_ip_allowlist.split(",") if entry.strip()
        )

    def require_production_secrets(self) -> None:
        """Abort startup in production if security-sensitive envs are unset.

        Called from the FastAPI lifespan. Fails loud rather than silently
        running with an unauthenticated API or a no-op CNPJ check.
        """
        if not self.is_production:
            return
        missing = []
        if not self.api_key or len(self.api_key) < 32:
            missing.append(
                'API_KEY (>=32 chars; generate with `python -c "import secrets; print(secrets.token_urlsafe(32))"`)'
            )
        if not self.tiny_expected_cnpj:
            missing.append("TINY_EXPECTED_CNPJ")
        if missing:
            raise RuntimeError(
                "Refusing to start in production with missing security env vars: "
                + ", ".join(missing)
            )


settings = Settings()  # type: ignore[call-arg]
