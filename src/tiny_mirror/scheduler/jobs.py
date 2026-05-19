"""APScheduler jobs.

The jobs themselves never execute sync logic — they create a sync_log
row and publish a message on the corresponding queue. The actual work
runs on the consumers, so a slow job here cannot back-pressure the
scheduler.

Initial-sync logic lives next to the scheduler because it is part of
the same lifecycle: on the first boot against an empty database, we
fan out the historical 90-day catch-up the same way the cron jobs
would.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.repositories.invoice_repository import (
    PostgreSQLInvoiceRepository,
)
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.queue.publisher import QueuePublisher

if TYPE_CHECKING:
    from fastapi import FastAPI

    from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
    from tiny_mirror.services.token_service import TokenService


logger = structlog.get_logger(__name__)

# Defaults — overridable for tests.
TOKEN_ROTATION_MAX_RETRIES = 3
TOKEN_ROTATION_RETRY_DELAY_SECONDS = 300

JOB_DEFAULTS = {
    "coalesce": True,
    "max_instances": 1,
    "misfire_grace_time": 900,  # 15 min
}


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------
def setup_scheduler(app: FastAPI) -> AsyncIOScheduler:
    """Create the scheduler, register the 5 cron jobs and start it.

    Cron expressions come from ``settings.*_cron`` — invalid expressions
    raise ``ValueError`` which we re-raise after a CRITICAL log so the
    service refuses to boot with broken schedules.
    """
    publisher: QueuePublisher = app.state.queue_publisher
    token_service: TokenService = app.state.token_service

    scheduler = AsyncIOScheduler(
        timezone="UTC",
        job_defaults=JOB_DEFAULTS,
    )

    try:
        triggers = {
            "token_rotation": CronTrigger.from_crontab(
                settings.token_rotation_cron, timezone="UTC"
            ),
            "products_sync": CronTrigger.from_crontab(settings.sync_products_cron, timezone="UTC"),
            "orders_sync": CronTrigger.from_crontab(settings.sync_orders_cron, timezone="UTC"),
            "orders_reconciliation": CronTrigger.from_crontab(
                settings.sync_orders_reconciliation_cron, timezone="UTC"
            ),
            "stock_full_sync": CronTrigger.from_crontab(settings.sync_stock_cron, timezone="UTC"),
            "sale_buckets_refresh": CronTrigger.from_crontab(
                settings.sync_buckets_cron, timezone="UTC"
            ),
            "invoices_sync": CronTrigger.from_crontab(settings.sync_invoices_cron, timezone="UTC"),
            "stock_history_sync": CronTrigger.from_crontab(
                settings.sync_stock_history_cron, timezone="UTC"
            ),
            "purchase_orders_sync": CronTrigger.from_crontab(
                settings.sync_purchase_orders_cron, timezone="UTC"
            ),
            "ml_listings_sync": CronTrigger.from_crontab(
                settings.sync_ml_listings_cron, timezone="UTC"
            ),
            "sync_log_watchdog": CronTrigger.from_crontab(
                settings.sync_log_watchdog_cron, timezone="UTC"
            ),
            "fulfillment_reception_scan": CronTrigger.from_crontab(
                settings.sync_fulfillment_reception_cron, timezone="UTC"
            ),
            "manual_status_sync": CronTrigger.from_crontab(
                settings.sync_manual_status_cron, timezone="UTC"
            ),
        }
    except ValueError as exc:
        logger.critical(
            "Invalid cron expression in settings; service cannot start",
            error=str(exc),
        )
        raise

    async def _token_rotation() -> None:
        await token_rotation_job(token_service)

    async def _products_sync() -> None:
        await products_sync_job(publisher)

    async def _orders_sync() -> None:
        await orders_sync_job(publisher)

    async def _orders_reconciliation() -> None:
        await orders_reconciliation_job(publisher)

    async def _stock_full_sync() -> None:
        await stock_full_sync_job(publisher)

    async def _sale_buckets_refresh() -> None:
        await sale_buckets_refresh_job(publisher)

    async def _invoices_sync() -> None:
        await invoices_sync_job(publisher)

    async def _stock_history_sync() -> None:
        await stock_history_sync_job(publisher)

    async def _purchase_orders_sync() -> None:
        await purchase_orders_sync_job(publisher)

    async def _ml_listings_sync() -> None:
        await ml_listings_sync_job(publisher)

    async def _sync_log_watchdog() -> None:
        await sync_log_watchdog_job()

    ml_client: MercadoLivreAPIClient | None = getattr(app.state, "ml_client", None)

    async def _fulfillment_reception_scan() -> None:
        if ml_client is not None:
            await fulfillment_reception_scan_job(ml_client)
        else:
            logger.debug("Fulfillment reception scan skipped: ML client not configured")

    async def _manual_status_sync() -> None:
        await manual_status_sync_job()

    scheduler.add_job(
        _token_rotation,
        trigger=triggers["token_rotation"],
        id="token_rotation",
        replace_existing=True,
    )
    scheduler.add_job(
        _products_sync,
        trigger=triggers["products_sync"],
        id="products_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _orders_sync,
        trigger=triggers["orders_sync"],
        id="orders_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _orders_reconciliation,
        trigger=triggers["orders_reconciliation"],
        id="orders_reconciliation",
        replace_existing=True,
    )
    scheduler.add_job(
        _stock_full_sync,
        trigger=triggers["stock_full_sync"],
        id="stock_full_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _sale_buckets_refresh,
        trigger=triggers["sale_buckets_refresh"],
        id="sale_buckets_refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        _invoices_sync,
        trigger=triggers["invoices_sync"],
        id="invoices_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _stock_history_sync,
        trigger=triggers["stock_history_sync"],
        id="stock_history_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _purchase_orders_sync,
        trigger=triggers["purchase_orders_sync"],
        id="purchase_orders_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _ml_listings_sync,
        trigger=triggers["ml_listings_sync"],
        id="ml_listings_sync",
        replace_existing=True,
    )
    scheduler.add_job(
        _sync_log_watchdog,
        trigger=triggers["sync_log_watchdog"],
        id="sync_log_watchdog",
        replace_existing=True,
    )
    scheduler.add_job(
        _fulfillment_reception_scan,
        trigger=triggers["fulfillment_reception_scan"],
        id="fulfillment_reception_scan",
        replace_existing=True,
    )
    scheduler.add_job(
        _manual_status_sync,
        trigger=triggers["manual_status_sync"],
        id="manual_status_sync",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started", jobs_count=len(scheduler.get_jobs()))
    return scheduler


def shutdown_scheduler(scheduler: AsyncIOScheduler | None) -> None:
    if scheduler is None:
        return
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


# ---------------------------------------------------------------------------
# Initial sync trigger (called from lifespan after setup_scheduler)
# ---------------------------------------------------------------------------
async def check_and_trigger_initial_sync(app: FastAPI) -> None:
    publisher: QueuePublisher = app.state.queue_publisher

    async with AsyncSessionLocal() as session:
        product_count = await PostgreSQLProductRepository(session).count()
        invoice_count = await PostgreSQLInvoiceRepository(session).count()

    if product_count > 0:
        logger.info(
            "Database has existing data, skipping orders/products/stock initial sync",
            product_count=product_count,
        )
    else:
        logger.info(
            "Empty database detected, triggering initial historical sync. This may take a while."
        )

        today = datetime.now(UTC).date()
        history_start = today - timedelta(days=90)

        async with AsyncSessionLocal() as session:
            sync_logs = SyncLogRepository(session)
            products_log = await sync_logs.create_sync_log(
                "products", metadata={"triggered_by": "initial_sync"}
            )
            orders_log = await sync_logs.create_sync_log(
                "orders",
                metadata={
                    "triggered_by": "initial_sync",
                    "days": 90,
                    "date_from": history_start.isoformat(),
                    "date_to": today.isoformat(),
                },
            )
            stock_log = await sync_logs.create_sync_log(
                "stock", metadata={"triggered_by": "initial_sync"}
            )

        await publisher.publish_sync_message(
            "products.full",
            {
                "triggered_by": "initial_sync",
                "sync_log_id": products_log,
                "published_at": datetime.now(UTC).isoformat(),
            },
        )
        await publisher.publish_sync_message(
            "orders.full",
            {
                "is_historical": True,
                "date_from": history_start.isoformat(),
                "date_to": today.isoformat(),
                "lookback_hours": None,
                "sync_log_id": orders_log,
                "published_at": datetime.now(UTC).isoformat(),
            },
        )
        await publisher.publish_sync_message(
            "stock.full",
            {
                "sync_log_id": stock_log,
                "published_at": datetime.now(UTC).isoformat(),
            },
        )

        logger.info(
            "Initial sync triggered. Products, orders (90 days) and stock are "
            "being synchronized in the background.",
            products_sync_log_id=products_log,
            orders_sync_log_id=orders_log,
            stock_sync_log_id=stock_log,
        )

    # Invoice cold start is independent of the orders/products check — it runs
    # whenever the invoices table is empty, even on a redeploy against an
    # existing DB that already has orders but was updated to add invoice sync.
    if invoice_count == 0:
        logger.info(
            "Invoices table empty, triggering cold start. "
            "This will sync all historical NFs and may take several minutes."
        )
        async with AsyncSessionLocal() as session:
            invoices_log = await SyncLogRepository(session).create_sync_log(
                "invoices",
                metadata={"triggered_by": "cold_start"},
            )
        await publisher.publish_sync_message(
            "invoices.full",
            {
                "is_cold_start": True,
                "sync_log_id": invoices_log,
                "published_at": datetime.now(UTC).isoformat(),
            },
        )
        logger.info("Invoice cold start triggered", invoices_sync_log_id=invoices_log)
    else:
        logger.info(
            "Invoices table has existing data, skipping cold start",
            invoice_count=invoice_count,
        )


# ---------------------------------------------------------------------------
# Individual jobs
# ---------------------------------------------------------------------------
async def token_rotation_job(
    token_service: TokenService,
    *,
    max_retries: int = TOKEN_ROTATION_MAX_RETRIES,
    retry_delay_seconds: int = TOKEN_ROTATION_RETRY_DELAY_SECONDS,
) -> None:
    """Rotate the OAuth token proactively, retrying internally on failure.

    APScheduler swallows job exceptions and continues scheduling, so the
    job logs CRITICAL on exhausted retries instead of raising — the
    operator must see the alert in the structured log stream.
    """
    logger.info("Token rotation job started")

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            new_token = await token_service.refresh_tokens()
            logger.info(
                "Token rotation completed successfully",
                attempt=attempt,
                new_expires_at=new_token.expires_at.isoformat(),
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt < max_retries:
                logger.warning(
                    "Token rotation retry scheduled",
                    attempt=attempt,
                    error=str(exc),
                )
                await asyncio.sleep(retry_delay_seconds)
            else:
                logger.critical(
                    "Token rotation failed after all retries. " "Manual intervention required.",
                    total_attempts=attempt,
                    error=str(exc),
                )

    # Defensive: only reachable if max_retries == 0.
    if last_error is not None and max_retries == 0:
        logger.critical(
            "Token rotation skipped: max_retries=0",
            error=str(last_error),
        )


async def products_sync_job(publisher: QueuePublisher) -> None:
    logger.info("Products sync job started")
    await _trigger_sync(
        publisher,
        sync_type="products",
        queue_type="products.full",
        message_extra={"triggered_by": "scheduler"},
        log_metadata={"triggered_by": "scheduler"},
    )


async def orders_sync_job(publisher: QueuePublisher) -> None:
    logger.info("Orders sync job started")
    await _trigger_sync(
        publisher,
        sync_type="orders",
        queue_type="orders.full",
        message_extra={
            "is_historical": False,
            "lookback_hours": 2,
            "date_from": None,
            "date_to": None,
        },
        log_metadata={"triggered_by": "scheduler", "lookback_hours": 2},
    )


async def orders_reconciliation_job(publisher: QueuePublisher) -> None:
    """Daily safety net: re-fetch every order updated yesterday and upsert
    them, so cancellations and other status drift the incremental cron
    cannot see still land in the mirror within ~24h.
    """
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    logger.info("Orders reconciliation job started", target_date=yesterday.isoformat())
    await _trigger_sync(
        publisher,
        sync_type="orders",
        queue_type="orders.full",
        message_extra={
            "mode": "reconcile",
            "target_date": yesterday.isoformat(),
        },
        log_metadata={
            "triggered_by": "scheduler",
            "mode": "reconcile",
            "target_date": yesterday.isoformat(),
        },
    )


async def stock_full_sync_job(publisher: QueuePublisher) -> None:
    logger.info("Stock full sync job started")
    await _trigger_sync(
        publisher,
        sync_type="stock",
        queue_type="stock.full",
        message_extra={},
        log_metadata={"triggered_by": "scheduler"},
    )


async def sync_log_watchdog_job() -> None:
    """Force-close sync_logs that overstay the running window.

    A row may stay in ``running`` indefinitely when items escape into a
    DLQ (no counter update) or when the consumer crashed mid-run. Tiny's
    60 req/min rate limit makes legitimate runs slow, so the threshold
    is intentionally generous (``settings.sync_log_running_max_minutes``,
    default 90). The dashboard then reflects reality without killing
    healthy long-running passes.
    """
    async with AsyncSessionLocal() as session:
        closed = await SyncLogRepository(session).mark_stalled_as_failed(
            max_minutes=settings.sync_log_running_max_minutes
        )
    if closed:
        logger.warning(
            "Watchdog auto-closed stalled sync_logs",
            count=closed,
            max_minutes=settings.sync_log_running_max_minutes,
        )


async def invoices_sync_job(publisher: QueuePublisher) -> None:
    logger.info("Invoices sync job started")
    await _trigger_sync(
        publisher,
        sync_type="invoices",
        queue_type="invoices.full",
        message_extra={},
        log_metadata={"triggered_by": "scheduler"},
    )


async def stock_history_sync_job(publisher: QueuePublisher) -> None:
    logger.info("Stock history sync job started")
    await _trigger_sync(
        publisher,
        sync_type="stock_history",
        queue_type="stock_history.full",
        message_extra={"triggered_by": "scheduler"},
        log_metadata={"triggered_by": "scheduler"},
    )


async def purchase_orders_sync_job(publisher: QueuePublisher) -> None:
    logger.info("Purchase orders sync job started")
    await _trigger_sync(
        publisher,
        sync_type="purchase_orders",
        queue_type="purchase_orders.full",
        message_extra={"triggered_by": "scheduler"},
        log_metadata={"triggered_by": "scheduler"},
    )


async def ml_listings_sync_job(publisher: QueuePublisher) -> None:
    logger.info("ML listings sync job started")
    await _trigger_sync(
        publisher,
        sync_type="ml_listings",
        queue_type="ml_listings.full",
        message_extra={"triggered_by": "scheduler"},
        log_metadata={"triggered_by": "scheduler"},
    )


async def sale_buckets_refresh_job(publisher: QueuePublisher) -> None:
    logger.info("Sale buckets refresh job started")
    today: date = datetime.now(UTC).date()
    history_start = today - timedelta(days=90)
    try:
        await publisher.publish_sync_message(
            "buckets.refresh",
            {
                "date_from": history_start.isoformat(),
                "date_to": today.isoformat(),
                "triggered_by": "scheduler",
                "published_at": datetime.now(UTC).isoformat(),
            },
        )
        logger.info(
            "Sale buckets refresh job triggered",
            date_from=history_start.isoformat(),
            date_to=today.isoformat(),
        )
    except Exception as exc:
        logger.error(
            "Sale buckets refresh job failed to trigger",
            error=str(exc),
        )


# ---------------------------------------------------------------------------
async def _trigger_sync(
    publisher: QueuePublisher,
    *,
    sync_type: str,
    queue_type: str,
    message_extra: dict[str, Any],
    log_metadata: dict[str, Any],
) -> None:
    sync_log_id: int | None = None
    try:
        async with AsyncSessionLocal() as session:
            sync_log_id = await SyncLogRepository(session).create_sync_log(
                sync_type, metadata=log_metadata
            )
        message = {
            "sync_log_id": sync_log_id,
            "published_at": datetime.now(UTC).isoformat(),
            **message_extra,
        }
        await publisher.publish_sync_message(queue_type, message)
        logger.info(
            f"{sync_type.capitalize()} sync job triggered",
            sync_log_id=sync_log_id,
        )
    except Exception as exc:
        logger.error(
            f"{sync_type.capitalize()} sync job failed to trigger",
            error=str(exc),
            sync_log_id=sync_log_id,
        )
        if sync_log_id is not None:
            try:
                async with AsyncSessionLocal() as session:
                    await SyncLogRepository(session).update_sync_log_failed(
                        sync_log_id,
                        error_message=f"Job trigger failed: {exc}",
                        items_processed=0,
                        items_failed=0,
                    )
            except Exception:  # pragma: no cover — secondary failure logged
                logger.exception("Failed to mark sync_log as failed", sync_log_id=sync_log_id)


async def manual_status_sync_job() -> None:
    """Pull operator's manual SKU classification from the GAS Web App and
    upsert into ``products.manual_status``. No-ops if not configured.
    """
    if not settings.gas_manual_status_url or not settings.gas_manual_status_token:
        logger.debug("Manual status sync skipped: GAS URL/token not configured")
        return
    import httpx

    from tiny_mirror.services.manual_status_sync_service import (
        ManualStatusSyncError,
        ManualStatusSyncService,
    )

    logger.info("Manual status sync job started")
    async with httpx.AsyncClient() as http:
        service = ManualStatusSyncService(
            http=http,
            gas_url=settings.gas_manual_status_url,
            gas_token=settings.gas_manual_status_token,
            timeout_seconds=settings.manual_status_http_timeout_seconds,
        )
        try:
            async with AsyncSessionLocal() as session:
                stats = await service.run(session)
            logger.info("Manual status sync job completed", **stats)
        except ManualStatusSyncError as exc:
            logger.error("Manual status sync job failed", error=str(exc))


async def fulfillment_reception_scan_job(ml_client: MercadoLivreAPIClient) -> None:
    """Poll ML INBOUND_RECEPTION and mark pending fulfillment transfers as received."""
    from tiny_mirror.services.fulfillment_reception_service import FulfillmentReceptionService

    logger.info("Fulfillment reception scan job started")
    try:
        service = FulfillmentReceptionService(ml_client=ml_client)
        result = await service.scan_and_reconcile()
        logger.info(
            "Fulfillment reception scan job completed",
            skus_scanned=result.skus_scanned,
            transfers_received=result.transfers_received,
            errors=len(result.errors),
        )
    except Exception as exc:
        logger.error("Fulfillment reception scan job failed", error=str(exc))


# ---------------------------------------------------------------------------
# Convenience for the order_sync_service which still imports from here.
# ---------------------------------------------------------------------------

# Compatibility re-export: order_sync_service used to import setup_scheduler
# from this module's older stub. Keep nothing extra public — the lifespan
# is the single entry point.
__all__ = [
    "TOKEN_ROTATION_MAX_RETRIES",
    "TOKEN_ROTATION_RETRY_DELAY_SECONDS",
    "check_and_trigger_initial_sync",
    "fulfillment_reception_scan_job",
    "manual_status_sync_job",
    "ml_listings_sync_job",
    "orders_reconciliation_job",
    "orders_sync_job",
    "products_sync_job",
    "purchase_orders_sync_job",
    "sale_buckets_refresh_job",
    "setup_scheduler",
    "shutdown_scheduler",
    "stock_full_sync_job",
    "stock_history_sync_job",
    "sync_log_watchdog_job",
    "token_rotation_job",
]
