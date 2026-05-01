"""Product synchronization orchestrator.

``run_full_sync`` only paginates Tiny's product list and fans out one
``products.item`` message per product — it never fetches details.
``process_product_item`` runs once per fan-out message and does the heavy
work: detail fetch, mapping, upsert and (for kits) component refresh.

Each method opens its own ``AsyncSession`` so the service is safe to use
from long-lived consumer contexts without sharing transactions across
messages.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import select, update

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import SyncLogORM
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.mappers.product_mapper import ProductMapper
from tiny_mirror.queue.publisher import QueuePublisher

logger = structlog.get_logger(__name__)

PAGE_SIZE = 100


class ProductSyncService:
    def __init__(
        self,
        tiny_client: TinyAPIClient,
        queue_publisher: QueuePublisher,
    ) -> None:
        self._tiny = tiny_client
        self._publisher = queue_publisher

    async def run_full_sync(self, sync_log_id: int) -> None:
        logger.info("Starting full product sync", sync_log_id=sync_log_id)

        total_published = 0
        offset = 0
        total: int | None = None

        while True:
            response = await self._tiny.list_products(situation="A", limit=PAGE_SIZE, offset=offset)
            items = response.get("itens", []) or []
            pagination = response.get("paginacao", {}) or {}
            total = int(pagination.get("total", 0))

            logger.debug(
                "Listed product page",
                offset=offset,
                count=len(items),
                total=total,
            )

            if not items:
                break

            for item in items:
                product_tiny_id = int(item["id"])
                await self._publisher.publish_sync_message(
                    "products.item",
                    {
                        "product_tiny_id": product_tiny_id,
                        "sync_log_id": sync_log_id,
                        "published_at": datetime.now(UTC).isoformat(),
                    },
                )
                logger.debug(
                    "Published product item",
                    product_tiny_id=product_tiny_id,
                    sync_log_id=sync_log_id,
                )
                total_published += 1

            offset += PAGE_SIZE
            if (total is not None and offset >= total) or len(items) < PAGE_SIZE:
                break

        # Record fan-out size in metadata.total_enqueued so the per-item
        # handler can own items_processed without double-counting.
        await self._record_total_enqueued(sync_log_id, total_published)

        logger.info(
            "Full product sync enqueued",
            sync_log_id=sync_log_id,
            total_published=total_published,
        )

    async def process_product_item(self, product_tiny_id: int, sync_log_id: int) -> None:
        logger.debug("Processing product item", product_tiny_id=product_tiny_id)

        try:
            raw = await self._tiny.get_product(product_tiny_id)
        except TinyNotFoundException:
            logger.warning(
                "Product not found in Tiny API, skipping",
                product_tiny_id=product_tiny_id,
            )
            return

        product_data = ProductMapper.from_tiny_api(raw)

        async with AsyncSessionLocal() as session:
            products = PostgreSQLProductRepository(session)
            sync_logs = SyncLogRepository(session)

            try:
                action = await products.upsert(product_data)

                if product_data["type"] == "K":
                    components = ProductMapper.extract_kit_components(raw)
                    if components:
                        await products.upsert_kit_components(product_tiny_id, components)
                    logger.debug(
                        "Kit processed",
                        product_tiny_id=product_tiny_id,
                        sku=product_data["sku"],
                        components_count=len(components),
                    )

                await sync_logs.increment_processed(sync_log_id)
            except TinyAPIException as exc:
                logger.error(
                    "Tiny API error while syncing product",
                    product_tiny_id=product_tiny_id,
                    error=str(exc),
                    status_code=exc.status_code,
                )
                await sync_logs.increment_failed(sync_log_id)
                raise
            except Exception as exc:
                logger.error(
                    "Database error while syncing product",
                    product_tiny_id=product_tiny_id,
                    error=str(exc),
                )
                await sync_logs.increment_failed(sync_log_id)
                raise

        logger.info(
            "Product synced",
            tiny_id=product_tiny_id,
            sku=product_data["sku"],
            type=product_data["type"],
            action=action,
        )

    async def _record_total_enqueued(self, sync_log_id: int, total_enqueued: int) -> None:
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
