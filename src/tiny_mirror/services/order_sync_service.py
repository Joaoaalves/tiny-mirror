"""Order synchronization orchestrator.

The hourly entry point is :meth:`run_incremental_sync` (lookback = 2h);
the first-deploy entry point is :meth:`run_historical_sync(days=90)`,
which slices the period into 7-day windows and fans out one
``orders.full`` message per window. Per-window pagination happens in
:meth:`run_date_range_sync`. Detail fetch + persistence happens in
:meth:`process_order_item`.

Each method opens its own ``AsyncSession`` so the service is safe to
share between long-lived consumer contexts.

The incremental and reconciliation paths list by ``dataAtualizacao``,
which Tiny bumps on any change to an order — including the deletion of an
old NF to free storage. Two guards keep such bulk edits from flooding the
item queue: orders created before :meth:`_cutoff_date` are skipped, and an
id already waiting on ``tiny.sync.orders.item`` is not enqueued again.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import select

from tiny_mirror.config import settings
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

if TYPE_CHECKING:
    import redis.asyncio as redis

    from tiny_mirror.services.invoice_sync_service import InvoiceSyncService

logger = structlog.get_logger(__name__)

PAGE_SIZE = 100
INCREMENTAL_LOOKBACK_HOURS = 2
HISTORICAL_WINDOW_DAYS = 7
PENDING_KEY_PREFIX = "tiny-mirror:orders-item-pending:"


class OrderSyncService:
    def __init__(
        self,
        tiny_client: TinyAPIClient,
        queue_publisher: QueuePublisher,
        invoice_sync: InvoiceSyncService | None = None,
        redis_client: redis.Redis | None = None,
    ) -> None:
        self._tiny = tiny_client
        self._publisher = queue_publisher
        self._invoice_sync = invoice_sync
        # None disables the pending-id dedupe (e.g. API-side instances that
        # never fan out).
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Incremental — hourly scheduler entry point
    # ------------------------------------------------------------------
    async def run_reconciliation_sync(self, target_date: date, sync_log_id: int) -> None:
        """Re-fetch every order updated since ``target_date`` and upsert them.

        Tiny v3 exposes ``dataAtualizacao=YYYY-MM-DD`` which returns every
        order whose last update is on or after that day (verified
        2026-09-30: totals shrink as the date advances), regardless of when
        the order was created. An order created on day N and cancelled on
        N+2 has its ``dataAtualizacao`` advanced to N+2 and shows up here.
        Unlike the incremental path, this one re-fetches every listed id,
        not only new or situation-changed ones — a daily full re-upsert
        also picks up drift in fields other than the situation.
        """
        logger.info(
            "Starting order reconciliation sync",
            sync_log_id=sync_log_id,
            target_date=target_date.isoformat(),
        )
        total_published = await self._fan_out_orders(
            sync_log_id=sync_log_id,
            updated_after=target_date,
            skip_existing=False,
            created_since=self._cutoff_date(),
        )
        await self._record_total_enqueued(sync_log_id, total_published)
        logger.info(
            "Order reconciliation sync enqueued",
            sync_log_id=sync_log_id,
            target_date=target_date.isoformat(),
            total_published=total_published,
        )

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
            created_since=self._cutoff_date(),
        )

        # Trigger a sale-bucket refresh covering the same window. Stock is
        # not refreshed here — the daily stock cron is the single owner.
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

        # Trigger an invoice sync for the same window so that NFs for newly
        # synced orders are pulled without waiting for the daily invoice cron.
        # sync_log_id is omitted — this lightweight sync runs untracked.
        await self._publisher.publish_sync_message(
            "invoices.full",
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
        # From here on the id is no longer waiting in the queue: a later
        # fan-out may enqueue it again (e.g. the reconciliation run).
        await self._release_pending(order_tiny_id)

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
                action = await orders.upsert(
                    order_data, invoice_link_frozen_before=self._cutoff_date()
                )
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
        updated_after: datetime | date | None = None,
        date_initial: date | None = None,
        date_final: date | None = None,
        skip_existing: bool = True,
        created_since: date | None = None,
    ) -> int:
        """Paginate Tiny orders and publish one ``orders.item`` per id.

        ``skip_existing=True`` (default) keeps only ids that are not
        mirrored yet or whose listed ``situacao`` differs from the mirror
        (e.g. cancelled after the last fetch) — used by the incremental
        cron, so a status change lands within one run without re-fetching
        every order. ``skip_existing=False`` publishes every id; used by the
        reconciliation path.

        ``created_since`` drops listing rows whose ``dataCriacao`` is older
        (rows without a parseable date are kept). Ids already pending on
        the item queue are never published twice.
        """
        total_published = 0
        skipped_old = 0
        skipped_pending = 0
        drifted = 0
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

            recent = [item for item in items if not _created_before(item, created_since)]
            skipped_old += len(items) - len(recent)
            page_ids = [int(item["id"]) for item in recent]
            if skip_existing:
                target_ids, page_drifted = await self._filter_new_or_changed(recent)
                drifted += page_drifted
            else:
                target_ids = page_ids
            skipped = len(page_ids) - len(target_ids)

            for order_tiny_id in target_ids:
                if not await self._claim_pending(order_tiny_id):
                    skipped_pending += 1
                    continue
                try:
                    await self._publisher.publish_sync_message(
                        "orders.item",
                        {
                            "order_tiny_id": order_tiny_id,
                            "sync_log_id": sync_log_id,
                            "published_at": datetime.now(UTC).isoformat(),
                        },
                    )
                except Exception:
                    await self._release_pending(order_tiny_id)
                    raise
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
                published=len(target_ids),
                skipped_existing=skipped,
            )

            offset += PAGE_SIZE
            if (total and offset >= total) or len(items) < PAGE_SIZE:
                break

        if skipped_old or skipped_pending or drifted:
            logger.info(
                "Order fan-out skipped ids",
                sync_log_id=sync_log_id,
                skipped_old=skipped_old,
                skipped_pending=skipped_pending,
                situation_drift=drifted,
                created_since=created_since.isoformat() if created_since else None,
                total_published=total_published,
            )
        return total_published

    @staticmethod
    def _cutoff_date() -> date:
        """Oldest creation date the incremental/reconcile paths still sync.

        Also the boundary below which an order's NF link is frozen: Tiny
        zeroes ``idNotaFiscal`` when an old NF is deleted to free storage,
        and the mirror keeps the last known link instead.
        """
        return datetime.now(UTC).date() - timedelta(days=settings.orders_sync_max_age_days)

    async def _claim_pending(self, order_tiny_id: int) -> bool:
        """Mark the id as waiting on the item queue. False = already waiting.

        Fails open: if Redis is unavailable the id is published anyway —
        a duplicate fetch is cheaper than a missed order.
        """
        if self._redis is None:
            return True
        try:
            claimed = await self._redis.set(
                f"{PENDING_KEY_PREFIX}{order_tiny_id}",
                "1",
                nx=True,
                ex=settings.orders_item_pending_ttl_seconds,
            )
        except Exception as exc:
            logger.warning(
                "Order pending claim failed, publishing anyway",
                order_tiny_id=order_tiny_id,
                error=str(exc),
            )
            return True
        return bool(claimed)

    async def _release_pending(self, order_tiny_id: int) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.delete(f"{PENDING_KEY_PREFIX}{order_tiny_id}")
        except Exception as exc:
            # The TTL clears the key eventually; never fail the sync over it.
            logger.warning(
                "Order pending release failed",
                order_tiny_id=order_tiny_id,
                error=str(exc),
            )

    async def _filter_new_or_changed(self, items: list[dict[str, Any]]) -> tuple[list[int], int]:
        """Ids worth fetching: not mirrored yet, or mirrored with a different
        situation than the listing reports. Returns ``(ids, drifted_count)``.

        The cron lookback overlaps with prior runs, so the same order is
        listed run after run; re-fetching unchanged ones would waste the
        60 req/min Tiny budget. The listing already carries ``situacao``,
        so a cancellation (or any status move) is detected for free — the
        Tiny order webhook is not configured, so this is the fast path.
        """
        listed: dict[int, int | None] = {
            int(item["id"]): _to_situation(item.get("situacao")) for item in items
        }
        if not listed:
            return [], 0
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(OrderORM.tiny_id, OrderORM.situation).where(
                    OrderORM.tiny_id.in_(list(listed))
                )
            )
            mirrored = {int(tid): int(sit) for tid, sit in result.all()}
        new = [oid for oid in listed if oid not in mirrored]
        changed = [
            oid
            for oid, situation in listed.items()
            if oid in mirrored and situation is not None and situation != mirrored[oid]
        ]
        if changed:
            logger.info(
                "Order situation drift detected",
                count=len(changed),
                sample=[(oid, mirrored[oid], listed[oid]) for oid in changed[:10]],
            )
        keep = set(new) | set(changed)
        return [oid for oid in listed if oid in keep], len(changed)

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


def _created_before(item: dict[str, Any], cutoff: date | None) -> bool:
    """True when the listing row's ``dataCriacao`` is older than ``cutoff``.

    Missing or unparseable dates return False so the order is still synced.
    """
    if cutoff is None:
        return False
    raw = item.get("dataCriacao")
    if not isinstance(raw, str) or len(raw) < 10:
        return False
    try:
        return date.fromisoformat(raw[:10]) < cutoff
    except ValueError:
        return False


def _to_situation(value: Any) -> int | None:
    """Listing ``situacao`` as the int stored in ``orders.situation``."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
