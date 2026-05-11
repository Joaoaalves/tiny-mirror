"""Stock synchronization orchestrator.

The daily entry point is :meth:`run_full_sync`, which fans out one
``stock.item`` message per active product. Per-product processing
happens in :meth:`process_stock_item`. Webhook-driven updates use the
same :meth:`process_stock_item` with ``sync_log_id=None`` (the sync_log
counters are no-ops in that case).

When an :class:`MercadoLivreAPIClient` is wired in, every per-product
call also pulls the SKU's Full ML stock straight from the ML API and
overwrites the (unreliable) Tiny "Full Mercado Livre" deposit row in
``stock_deposits``. Both sources land atomically in the same upsert,
so the coverage query just reads ``stock_deposits`` without special
casing.

Each method opens its own ``AsyncSession`` so the service is safe to
share between long-lived consumer contexts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.stock_repository import (
    PostgreSQLStockRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.mappers.stock_mapper import StockMapper
from tiny_mirror.queue.publisher import QueuePublisher

logger = structlog.get_logger(__name__)

# Deposit name we use to mark the ML-API-sourced Full ML row so it can be
# distinguished from the (unreliable) Tiny "Full Mercado Livre" deposit.
# Matching is by name — if Tiny already returns a row with this name we
# overwrite its values; otherwise we append a synthetic row.
ML_FULL_DEPOSIT_NAME = "Full Mercado Livre"
# Sentinel deposit_tiny_id used when we have to append a synthetic row
# because Tiny did not return a "Full Mercado Livre" deposit at all.
# Real Tiny deposit IDs are positive — 0 is safe as a sentinel.
ML_FULL_DEPOSIT_SENTINEL_ID = 0


class StockSyncService:
    def __init__(
        self,
        tiny_client: TinyAPIClient,
        queue_publisher: QueuePublisher,
        ml_client: MercadoLivreAPIClient | None = None,
    ) -> None:
        self._tiny = tiny_client
        self._publisher = queue_publisher
        self._ml = ml_client

    # ------------------------------------------------------------------
    # Daily entry point — fan-out for every active product
    # ------------------------------------------------------------------
    async def run_full_sync(self, sync_log_id: int) -> None:
        logger.info("Starting full stock sync", sync_log_id=sync_log_id)

        async with AsyncSessionLocal() as session:
            product_ids = await PostgreSQLProductRepository(session).list_active()

        logger.info("Products to sync stock for", count=len(product_ids))

        for product_id in product_ids:
            await self._publisher.publish_sync_message(
                "stock.item",
                {
                    "product_tiny_id": int(product_id),
                    "sync_log_id": sync_log_id,
                    "published_at": datetime.now(UTC).isoformat(),
                },
            )

        await self._record_total_enqueued(sync_log_id, len(product_ids))

        logger.info(
            "Full stock sync enqueued",
            sync_log_id=sync_log_id,
            total_queued=len(product_ids),
        )

    # ------------------------------------------------------------------
    # Incremental — called by other services with a list of product ids
    # ------------------------------------------------------------------
    async def run_incremental_sync_for_products(
        self, product_tiny_ids: list[int], sync_log_id: int | None
    ) -> None:
        if not product_tiny_ids:
            logger.debug("No products to sync stock for")
            return

        logger.info(
            "Starting incremental stock sync",
            products_count=len(product_tiny_ids),
            sync_log_id=sync_log_id,
        )

        for product_id in product_tiny_ids:
            await self._publisher.publish_sync_message(
                "stock.item",
                {
                    "product_tiny_id": int(product_id),
                    "sync_log_id": sync_log_id,
                    "published_at": datetime.now(UTC).isoformat(),
                },
            )

        logger.debug(
            "Incremental stock sync enqueued",
            count=len(product_tiny_ids),
        )

    # ------------------------------------------------------------------
    # Per-product — used both by the queue consumer and the webhook
    # consumer. ``sync_log_id`` is None for webhook-driven calls.
    # ------------------------------------------------------------------
    async def process_stock_item(self, product_tiny_id: int, sync_log_id: int | None) -> None:
        logger.debug("Processing stock item", product_tiny_id=product_tiny_id)

        try:
            raw = await self._tiny.get_stock(product_tiny_id)
        except TinyNotFoundException:
            logger.warning(
                "Stock not found for product, possibly no stock configured",
                product_tiny_id=product_tiny_id,
            )
            return

        stock_data = StockMapper.from_tiny_api(raw)
        deposits = StockMapper.extract_deposits(raw)
        sku = stock_data.get("sku") or ""

        # Same isolation pattern as product_sync_service: capture errors
        # so the sync-log update always runs in a fresh session and never
        # inherits an aborted-transaction state from the stock upsert.
        processing_error: Exception | None = None
        async with AsyncSessionLocal() as session:
            # Stock rows have a FK -> products.tiny_id (CASCADE on delete).
            # If the product has not been mirrored yet, the FK insert would
            # fail; degrade to a warning + skip instead of raising. The
            # daily product sync will pick the product up and the next
            # stock pass will succeed.
            product_repo = PostgreSQLProductRepository(session)
            product_exists = await product_repo.get_by_tiny_id(product_tiny_id)
            if product_exists is None:
                logger.warning(
                    "Skipping stock for product not yet synced",
                    product_tiny_id=product_tiny_id,
                )
                return

            # Optionally pull Full ML stock straight from ML API and overlay it
            # onto the deposits list, replacing the unreliable Tiny row. None
            # means "we don't know" (skip mutation); 0 means "ML has no stock".
            # Kit contributions: kits that contain this SKU as a component also
            # hold fulfillment inventory for it — e.g. a kit with 8 units and
            # component_qty=1 adds 8 more units of available stock.
            if self._ml is not None and sku:
                parent_kits = await product_repo.get_parent_kits_for_sku(sku)
                ml_qty = await self._fetch_ml_full_qty(sku, parent_kits=parent_kits)
                if ml_qty is not None:
                    _overlay_ml_full_deposit(deposits, ml_qty)

            stock_repo = PostgreSQLStockRepository(session)
            try:
                await stock_repo.upsert(stock_data)
                await stock_repo.upsert_deposits(product_tiny_id, deposits)
            except TinyAPIException as exc:
                logger.error(
                    "Tiny API error while syncing stock",
                    product_tiny_id=product_tiny_id,
                    error=str(exc),
                    status_code=exc.status_code,
                )
                processing_error = exc
            except Exception as exc:
                logger.error(
                    "Database error while syncing stock",
                    product_tiny_id=product_tiny_id,
                    error=str(exc),
                )
                processing_error = exc

        if sync_log_id is not None:
            async with AsyncSessionLocal() as log_session:
                sync_logs = SyncLogRepository(log_session)
                if processing_error is not None:
                    await sync_logs.increment_failed(sync_log_id)
                else:
                    await sync_logs.increment_processed(sync_log_id)
                await sync_logs.try_finalize(sync_log_id)

        if processing_error is not None:
            raise processing_error

        logger.info(
            "Stock synced",
            product_tiny_id=product_tiny_id,
            sku=stock_data.get("sku"),
            balance=stock_data["balance"],
            available=stock_data["available"],
            deposits_count=len(deposits),
        )

    # ------------------------------------------------------------------
    # ML helper — sums fulfillment available_quantity across all MLB IDs
    # for a given SKU, including kits that contain the SKU as a component.
    #
    # parent_kits: list of (kit_sku, component_quantity) from product_kit_components.
    #   A kit with available_quantity=8 and component_quantity=1 contributes 8
    #   effective units of this component (8 kits x 1 unit each).
    #
    # Returns:
    #   - None if no ML listings found at all (we don't know),
    #   - int >= 0 otherwise (sum across logistic_type=='fulfillment' only).
    # On API error it logs and returns None (preserves previous Tiny row).
    #
    # Deduplication: listings sharing the same inventory_id represent the same
    # physical stock pool; we keep the max effective quantity per inventory_id
    # and sum across distinct pools. Direct and kit inventories are typically
    # separate physical pools at the ML warehouse, so they add up correctly.
    # ------------------------------------------------------------------
    async def _fetch_ml_full_qty(
        self,
        sku: str,
        parent_kits: list[tuple[str, int]] | None = None,
    ) -> int | None:
        if self._ml is None:
            return None

        # Build list of (mlb_id, component_multiplier) to evaluate.
        # multiplier=1 for direct listings; multiplier=component_qty for kits.
        mlb_candidates: list[tuple[str, int]] = []

        try:
            direct_mlb_ids = await self._ml.list_items_by_sku(sku)
        except Exception as exc:
            logger.warning("ML search failed for SKU, skipping ML overlay", sku=sku, error=str(exc))
            return None

        for mlb_id in direct_mlb_ids:
            mlb_candidates.append((mlb_id, 1))

        for kit_sku, component_qty in parent_kits or []:
            try:
                kit_mlb_ids = await self._ml.list_items_by_sku(kit_sku)
            except Exception as exc:
                logger.warning(
                    "ML search failed for parent kit SKU, skipping kit contribution",
                    sku=sku,
                    kit_sku=kit_sku,
                    error=str(exc),
                )
                continue
            for mlb_id in kit_mlb_ids:
                mlb_candidates.append((mlb_id, component_qty))

        if not mlb_candidates:
            return None

        # Group by inventory_id — same inventory_id = same physical pool.
        # Keep the max effective quantity (available x multiplier) per pool.
        inventory_qty: dict[str, int] = {}
        any_fulfillment = False
        for mlb_id, multiplier in mlb_candidates:
            try:
                item = await self._ml.get_item(mlb_id)
            except Exception as exc:
                logger.warning(
                    "ML item fetch failed, skipping that listing",
                    sku=sku,
                    mlb_id=mlb_id,
                    error=str(exc),
                )
                continue
            shipping = item.get("shipping") or {}
            if shipping.get("logistic_type") != "fulfillment":
                continue
            any_fulfillment = True
            inv_key = item.get("inventory_id") or mlb_id
            effective_qty = int(item.get("available_quantity") or 0) * multiplier
            if inv_key not in inventory_qty or effective_qty > inventory_qty[inv_key]:
                inventory_qty[inv_key] = effective_qty

        # If no listing (direct or kit) is fulfillment, treat as None so the
        # caller leaves the Tiny "Full Mercado Livre" row alone.
        return sum(inventory_qty.values()) if any_fulfillment else None

    # ------------------------------------------------------------------
    async def _record_total_enqueued(self, sync_log_id: int, total_enqueued: int) -> None:
        from sqlalchemy import select, update

        from tiny_mirror.infrastructure.orm.models import SyncLogORM

        async with AsyncSessionLocal() as session:
            current = await session.execute(
                select(SyncLogORM.sync_metadata).where(SyncLogORM.id == sync_log_id)
            )
            metadata = current.scalar_one_or_none() or {}
            metadata = {**metadata, "total_enqueued": total_enqueued}
            await session.execute(
                update(SyncLogORM)
                .where(SyncLogORM.id == sync_log_id)
                .values(sync_metadata=metadata)
            )
            await session.commit()
            # Close immediately if nothing was enqueued (no consumer will run).
            await SyncLogRepository(session).try_finalize(sync_log_id)


# ---------------------------------------------------------------------------
def _overlay_ml_full_deposit(deposits: list[dict[str, Any]], ml_qty: int) -> None:
    """Mutate `deposits` so the Full ML row reflects the authoritative ML
    quantity and counts in coverage (``ignore=False``).

    If Tiny returned a row named "Full Mercado Livre", overwrite its
    balance/available with `ml_qty` and flip ``ignore`` off. Otherwise
    append a synthetic row with a sentinel ``deposit_tiny_id`` (the
    table's unique constraint is per (product, deposit_tiny_id), so a
    fixed sentinel is safe per product).
    """
    for d in deposits:
        if d.get("deposit_name") == ML_FULL_DEPOSIT_NAME:
            d["balance"] = float(ml_qty)
            d["available"] = float(ml_qty)
            d["reserved"] = 0.0
            d["ignore"] = False
            return

    deposits.append(
        {
            "deposit_tiny_id": ML_FULL_DEPOSIT_SENTINEL_ID,
            "deposit_name": ML_FULL_DEPOSIT_NAME,
            "ignore": False,
            "balance": float(ml_qty),
            "reserved": 0.0,
            "available": float(ml_qty),
            "company": "Mercado Livre",
        }
    )
