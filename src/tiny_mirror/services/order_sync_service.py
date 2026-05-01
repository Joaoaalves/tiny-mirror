"""Order synchronization orchestrator.

The hourly entry point is :meth:`run_incremental_sync` (lookback = 2h);
the first-deploy entry point is :meth:`run_historical_sync(days=90)`,
which slices the period into 7-day windows and fans out one
``orders.full`` message per window. Per-window pagination happens in
:meth:`run_date_range_sync`. Detail fetch + persistence happens in
:meth:`process_order_item`.

Each method opens its own ``AsyncSession`` so the service is safe to
share between long-lived consumer contexts.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import structlog
from sqlalchemy import select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import OrderORM
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.mappers.order_mapper import OrderMapper
from tiny_mirror.queue.publisher import QueuePublisher

logger = structlog.get_logger(__name__)

PAGE_SIZE = 100
INCREMENTAL_LOOKBACK_HOURS = 2
HISTORICAL_WINDOW_DAYS = 7


class OrderSyncService:
    def __init__(
        self,
        tiny_client: TinyAPIClient,
        queue_publisher: QueuePublisher,
    ) -> None:
        self._tiny = tiny_client
        self._publisher = queue_publisher

    # ------------------------------------------------------------------
    # Incremental — hourly scheduler entry point
    # ------------------------------------------------------------------
    async def run_incremental_sync(self, sync_log_id: int) -> None:
        lookback_dt = datetime.now(UTC) - timedelta(hours=INCREMENTAL_LOOKBACK_HOURS)
        logger.info(
            "Starting incremental order sync",
            sync_log_id=sync_log_id,
            lookback_from=lookback_dt.isoformat(),
        )

        total_published = await self._fan_out_orders(
            sync_log_id=sync_log_id,
            updated_after=lookback_dt,
        )

        # Fan out an incremental stock refresh for every product touched.
        async with AsyncSessionLocal() as session:
            product_ids = await PostgreSQLOrderRepository(session).get_recent_product_tiny_ids(
                hours=INCREMENTAL_LOOKBACK_HOURS
            )

        for product_id in product_ids:
            await self._publisher.publish_sync_message(
                "stock.item",
                {
                    "product_tiny_id": int(product_id),
                    "sync_log_id": sync_log_id,
                    "published_at": datetime.now(UTC).isoformat(),
                },
            )

        # Trigger a sale-bucket refresh covering the same window.
        date_from = (datetime.now(UTC) - timedelta(hours=INCREMENTAL_LOOKBACK_HOURS)).date()
        date_to = datetime.now(UTC).date()
        await self._publisher.publish_sync_message(
            "buckets.refresh",
            {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "triggered_by": "order_sync",
                "published_at": datetime.now(UTC).isoformat(),
            },
        )

        await self._record_total_enqueued(sync_log_id, total_published)

        logger.info(
            "Incremental order sync completed",
            sync_log_id=sync_log_id,
            total_published=total_published,
            stock_products_queued=len(product_ids),
        )

    # ------------------------------------------------------------------
    # Historical — first-deploy / empty-DB entry point
    # ------------------------------------------------------------------
    async def run_historical_sync(self, days: int, sync_log_id: int) -> None:
        end_date = datetime.now(UTC).date()
        start_date = end_date - timedelta(days=days)

        windows: list[tuple[date, date]] = []
        cursor = start_date
        while cursor < end_date:
            window_end = min(cursor + timedelta(days=HISTORICAL_WINDOW_DAYS), end_date)
            windows.append((cursor, window_end))
            cursor = window_end

        for window_start, window_end in windows:
            await self._publisher.publish_sync_message(
                "orders.full",
                {
                    "is_historical": True,
                    "date_from": window_start.isoformat(),
                    "date_to": window_end.isoformat(),
                    "sync_log_id": sync_log_id,
                    "lookback_hours": None,
                    "published_at": datetime.now(UTC).isoformat(),
                },
            )

        logger.info(
            "Historical order sync triggered",
            days=days,
            windows_count=len(windows),
            sync_log_id=sync_log_id,
        )

    # ------------------------------------------------------------------
    # Per-window — called by OrderFullSyncConsumer when is_historical=True
    # ------------------------------------------------------------------
    async def run_date_range_sync(self, date_from: date, date_to: date, sync_log_id: int) -> None:
        logger.info(
            "Starting date range order sync",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            sync_log_id=sync_log_id,
        )

        total_published = await self._fan_out_orders(
            sync_log_id=sync_log_id,
            date_initial=date_from,
            date_final=date_to,
        )

        logger.info(
            "Date range order sync enqueued",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            sync_log_id=sync_log_id,
            total_published=total_published,
        )

    # ------------------------------------------------------------------
    # Per-order — called by OrderItemConsumer for each fan-out message
    # ------------------------------------------------------------------
    async def process_order_item(self, order_tiny_id: int, sync_log_id: int | None) -> None:
        """Sync a single order. ``sync_log_id`` is None for webhook-driven
        calls — counter updates are skipped in that case.
        """
        logger.debug("Processing order item", order_tiny_id=order_tiny_id)

        try:
            raw = await self._tiny.get_order(order_tiny_id)
        except TinyNotFoundException:
            logger.warning(
                "Order not found in Tiny API, skipping",
                order_tiny_id=order_tiny_id,
            )
            return

        order_data = OrderMapper.from_tiny_api(raw)
        items = OrderMapper.extract_items(raw)

        async with AsyncSessionLocal() as session:
            orders = PostgreSQLOrderRepository(session)
            sync_logs = SyncLogRepository(session)
            try:
                action = await orders.upsert(order_data)
                await orders.upsert_items(order_tiny_id, items)
                if sync_log_id is not None:
                    await sync_logs.increment_processed(sync_log_id)
                    await sync_logs.try_finalize(sync_log_id)
            except TinyAPIException as exc:
                logger.error(
                    "Tiny API error while syncing order",
                    order_tiny_id=order_tiny_id,
                    error=str(exc),
                    status_code=exc.status_code,
                )
                if sync_log_id is not None:
                    await sync_logs.increment_failed(sync_log_id)
                    await sync_logs.try_finalize(sync_log_id)
                raise
            except Exception as exc:
                logger.error(
                    "Database error while syncing order",
                    order_tiny_id=order_tiny_id,
                    error=str(exc),
                )
                if sync_log_id is not None:
                    await sync_logs.increment_failed(sync_log_id)
                    await sync_logs.try_finalize(sync_log_id)
                raise

        logger.info(
            "Order synced",
            tiny_id=order_tiny_id,
            order_number=order_data["order_number"],
            situation=order_data["situation"],
            action=action,
            items_count=len(items),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _fan_out_orders(
        self,
        *,
        sync_log_id: int,
        updated_after: datetime | None = None,
        date_initial: date | None = None,
        date_final: date | None = None,
    ) -> int:
        """Paginate either updated_after (incremental) or date range (historical)
        and publish one ``orders.item`` message per order. Returns the count.
        """
        total_published = 0
        offset = 0
        while True:
            response = await self._tiny.list_orders(
                updated_after=updated_after,
                date_initial=date_initial,
                date_final=date_final,
                limit=PAGE_SIZE,
                offset=offset,
                order_by="asc",
            )
            items = response.get("itens", []) or []
            pagination = response.get("paginacao", {}) or {}
            total = int(pagination.get("total", 0))

            if not items:
                break

            page_ids = [int(item["id"]) for item in items]
            new_ids = await self._filter_new_order_ids(page_ids)
            skipped = len(page_ids) - len(new_ids)

            for order_tiny_id in new_ids:
                await self._publisher.publish_sync_message(
                    "orders.item",
                    {
                        "order_tiny_id": order_tiny_id,
                        "sync_log_id": sync_log_id,
                        "published_at": datetime.now(UTC).isoformat(),
                    },
                )
                logger.debug(
                    "Published order item",
                    order_tiny_id=order_tiny_id,
                    sync_log_id=sync_log_id,
                )
                total_published += 1

            logger.debug(
                "Listed order page",
                offset=offset,
                count=len(items),
                total=total,
                published=len(new_ids),
                skipped_existing=skipped,
            )

            offset += PAGE_SIZE
            if (total and offset >= total) or len(items) < PAGE_SIZE:
                break

        return total_published

    async def _filter_new_order_ids(self, candidate_ids: list[int]) -> list[int]:
        """Drop ids that are already in the local orders table.

        The cron lookback window overlaps with prior runs, so the same
        order surfaces hour after hour until it falls out of the window.
        Re-fetching its detail every hour wastes the 60 req/min Tiny
        budget without changing the row — status changes already arrive
        through the order webhook, which calls process_order_item
        directly without going through this fan-out.
        """
        if not candidate_ids:
            return []
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(OrderORM.tiny_id).where(OrderORM.tiny_id.in_(candidate_ids))
            )
            existing = {int(tid) for (tid,) in result.all()}
        return [oid for oid in candidate_ids if oid not in existing]

    async def _record_total_enqueued(self, sync_log_id: int, total_enqueued: int) -> None:
        from sqlalchemy import update

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
            # Edge case: when no items were enqueued (e.g. every Tiny page
            # row was already in the DB), the per-item finalizer never runs.
            # Try to close the row right after the fan-out.
            await SyncLogRepository(session).try_finalize(sync_log_id)
