"""Stock synchronization orchestrator.

The daily entry point is :meth:`run_full_sync`, which fans out one
``stock.item`` message per active product. Per-product processing
happens in :meth:`process_stock_item`. Webhook-driven updates use the
same :meth:`process_stock_item` with ``sync_log_id=None`` (the sync_log
counters are no-ops in that case).

Each method opens its own ``AsyncSession`` so the service is safe to
share between long-lived consumer contexts.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
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


class StockSyncService:
    def __init__(
        self,
        tiny_client: TinyAPIClient,
        queue_publisher: QueuePublisher,
    ) -> None:
        self._tiny = tiny_client
        self._publisher = queue_publisher

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
    async def process_stock_item(
        self, product_tiny_id: int, sync_log_id: int | None
    ) -> None:
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

        async with AsyncSessionLocal() as session:
            # Stock rows have a FK -> products.tiny_id (CASCADE on delete).
            # If the product has not been mirrored yet, the FK insert would
            # fail; degrade to a warning + skip instead of raising. The
            # daily product sync will pick the product up and the next
            # stock pass will succeed.
            product_exists = await PostgreSQLProductRepository(
                session
            ).get_by_tiny_id(product_tiny_id)
            if product_exists is None:
                logger.warning(
                    "Skipping stock for product not yet synced",
                    product_tiny_id=product_tiny_id,
                )
                return

            stock_repo = PostgreSQLStockRepository(session)
            sync_logs = SyncLogRepository(session)

            try:
                await stock_repo.upsert(stock_data)
                await stock_repo.upsert_deposits(product_tiny_id, deposits)
                if sync_log_id is not None:
                    await sync_logs.increment_processed(sync_log_id)
            except TinyAPIException as exc:
                logger.error(
                    "Tiny API error while syncing stock",
                    product_tiny_id=product_tiny_id,
                    error=str(exc),
                    status_code=exc.status_code,
                )
                if sync_log_id is not None:
                    await sync_logs.increment_failed(sync_log_id)
                raise
            except Exception as exc:
                logger.error(
                    "Database error while syncing stock",
                    product_tiny_id=product_tiny_id,
                    error=str(exc),
                )
                if sync_log_id is not None:
                    await sync_logs.increment_failed(sync_log_id)
                raise

        logger.info(
            "Stock synced",
            product_tiny_id=product_tiny_id,
            sku=stock_data.get("sku"),
            balance=stock_data["balance"],
            available=stock_data["available"],
            deposits_count=len(deposits),
        )

    # ------------------------------------------------------------------
    async def _record_total_enqueued(
        self, sync_log_id: int, total_enqueued: int
    ) -> None:
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
