"""Mercado Livre fulfillment stock synchronization.

Fetches stock directly from the ML API for every active SKU in the
products table. Unlike the Tiny stock sync, this runs inline — there is
no RabbitMQ fan-out. The ML API is not rate-constrained the same way as
Tiny, so the full sync completes in under 5 minutes for a typical catalog.

For each SKU:
  1. Call ``/users/{user_id}/items/search?seller_sku={sku}`` → list of MLB IDs.
  2. For each MLB ID, call ``/items/{mlb_id}`` → filter ``logistic_type='fulfillment'``.
  3. Persist all fulfillment listings to ``mercadolivre_stock`` (atomic replace per SKU).

Edge cases handled:
  - 0 MLB IDs → no rows for that SKU (coverage query treats this as ml_full_stock=0).
  - Multiple MLBs per SKU → all persisted; sum in the coverage query.
  - Non-fulfillment logistic_type → persisted anyway (coverage query filters in SQL).
  - ``status='paused'`` with ``available_quantity=0`` → persisted as 0 (normal for OOS).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
from tiny_mirror.infrastructure.repositories.mercadolivre_stock_repository import (
    MercadoLivreStockRepository,
)
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository

logger = structlog.get_logger(__name__)


class MercadoLivreStockService:
    def __init__(self, ml_client: MercadoLivreAPIClient) -> None:
        self._ml = ml_client

    # ------------------------------------------------------------------
    # Entry point — called directly from the scheduler job
    # ------------------------------------------------------------------
    async def run_full_sync(self, sync_log_id: int) -> None:
        logger.info("Starting ML stock full sync", sync_log_id=sync_log_id)

        async with AsyncSessionLocal() as session:
            skus = await PostgreSQLProductRepository(session).list_active_skus()

        logger.info("SKUs to sync ML stock for", count=len(skus))

        items_ok = 0
        items_fail = 0

        for sku in skus:
            try:
                await self._sync_sku(sku)
                items_ok += 1
            except Exception as exc:
                items_fail += 1
                logger.error(
                    "ML stock sync failed for SKU",
                    sku=sku,
                    error=str(exc),
                    sync_log_id=sync_log_id,
                )

        async with AsyncSessionLocal() as session:
            sync_repo = SyncLogRepository(session)
            if items_fail == 0:
                await sync_repo.update_sync_log_complete(
                    sync_log_id,
                    items_processed=items_ok,
                    items_failed=0,
                )
            else:
                await sync_repo.update_sync_log_failed(
                    sync_log_id,
                    error_message=f"{items_fail} SKUs failed during ML stock sync",
                    items_processed=items_ok,
                    items_failed=items_fail,
                )

        logger.info(
            "ML stock full sync finished",
            sync_log_id=sync_log_id,
            items_ok=items_ok,
            items_fail=items_fail,
        )

    # ------------------------------------------------------------------
    # Per-SKU sync (also callable directly for targeted refreshes)
    # ------------------------------------------------------------------
    async def _sync_sku(self, sku: str) -> None:
        logger.debug("Fetching ML listings for SKU", sku=sku)

        mlb_ids = await self._ml.list_items_by_sku(sku)

        if not mlb_ids:
            logger.debug("No ML listings for SKU", sku=sku)
            async with AsyncSessionLocal() as session:
                await MercadoLivreStockRepository(session).replace_for_sku(sku, [])
            return

        listings: list[dict[str, Any]] = []
        now = datetime.now(UTC)

        for mlb_id in mlb_ids:
            try:
                item = await self._ml.get_item(mlb_id)
            except TinyAPIException as exc:
                logger.warning(
                    "Failed to fetch ML item, skipping",
                    mlb_id=mlb_id,
                    sku=sku,
                    error=str(exc),
                )
                continue

            shipping = item.get("shipping") or {}
            logistic_type = shipping.get("logistic_type") or "unknown"
            listings.append(
                {
                    "mlb_id": mlb_id,
                    "available_quantity": int(item.get("available_quantity") or 0),
                    "logistic_type": logistic_type,
                    "status": item.get("status") or "unknown",
                    "last_synced_at": now,
                }
            )

        async with AsyncSessionLocal() as session:
            await MercadoLivreStockRepository(session).replace_for_sku(sku, listings)

        fulfillment_qty = sum(
            int(ln["available_quantity"]) for ln in listings if ln["logistic_type"] == "fulfillment"
        )
        logger.info(
            "ML stock synced for SKU",
            sku=sku,
            mlb_count=len(listings),
            fulfillment_qty=fulfillment_qty,
        )
