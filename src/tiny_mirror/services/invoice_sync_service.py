"""Invoice (NF) synchronization service.

Cold start:  :meth:`run_cold_start` fans out 30-day date windows from
``COLD_START_FROM`` to today, publishing one ``invoices.full`` message per
window. All windows share the same ``sync_log_id``; ``total_enqueued`` is set
to the global invoice count (from a preflight API call) so ``try_finalize``
closes the log automatically when every page has been upserted.

Incremental: :meth:`run_incremental_sync` fetches the last
``INCREMENTAL_LOOKBACK_DAYS`` of NFs inline (no fan-out needed; the window
is small) and closes the log with ``update_sync_log_complete``.

Date-range: :meth:`run_date_range_sync` is the shared worker called by
both paths. It paginates the Tiny ``/notas`` endpoint, maps each page with
:class:`InvoiceMapper`, batch-upserts via the repository, and atomically
increments ``items_processed`` on the sync_log.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.repositories.invoice_item_repository import (
    PostgreSQLInvoiceItemRepository,
)
from tiny_mirror.infrastructure.repositories.invoice_repository import (
    PostgreSQLInvoiceRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository
from tiny_mirror.mappers.invoice_mapper import InvoiceMapper
from tiny_mirror.queue.publisher import QueuePublisher

logger = structlog.get_logger(__name__)

PAGE_SIZE = 100
INCREMENTAL_LOOKBACK_DAYS = 2
COLD_START_WINDOW_DAYS = 30
COLD_START_FROM = date(2024, 5, 8)  # Tiny plan only allows queries from this date


class InvoiceSyncService:
    def __init__(
        self,
        tiny_client: TinyAPIClient,
        queue_publisher: QueuePublisher,
    ) -> None:
        self._tiny = tiny_client
        self._publisher = queue_publisher

    # ------------------------------------------------------------------
    # Cold start — initial full historical sync
    # ------------------------------------------------------------------
    async def run_cold_start(self, sync_log_id: int) -> None:
        """Fan out 30-day windows covering all historical NFs.

        ``total_enqueued`` is set to the number of windows (not the number of
        invoices) so ``try_finalize`` closes the log once every window has been
        processed — regardless of how many new NFs were created while the cold
        start was running across ~15 minutes.
        """
        end_date = datetime.now(UTC).date()
        windows: list[tuple[date, date]] = []
        cursor = COLD_START_FROM
        while cursor < end_date:
            window_end = min(cursor + timedelta(days=COLD_START_WINDOW_DAYS), end_date)
            windows.append((cursor, window_end))
            cursor = window_end

        # total_enqueued = number of windows; each window increments by 1 when done.
        await _set_total_enqueued(sync_log_id, len(windows))

        for date_from, date_to in windows:
            await self._publisher.publish_sync_message(
                "invoices.full",
                {
                    "is_cold_start_window": True,
                    "date_from": date_from.isoformat(),
                    "date_to": date_to.isoformat(),
                    "sync_log_id": sync_log_id,
                    "published_at": datetime.now(UTC).isoformat(),
                },
            )

        logger.info(
            "Invoice cold start triggered",
            sync_log_id=sync_log_id,
            windows=len(windows),
        )

    async def finalize_cold_start_window(self, sync_log_id: int) -> None:
        """Increment the window counter and attempt to close the cold-start log.

        Called once per window after ``run_date_range_sync`` completes. When
        the last window finishes, ``try_finalize`` transitions the sync_log
        from 'running' to 'completed'.
        """
        async with AsyncSessionLocal() as session:
            sync_logs = SyncLogRepository(session)
            await sync_logs.increment_processed(sync_log_id)
            await sync_logs.try_finalize(sync_log_id)

    # ------------------------------------------------------------------
    # Incremental — triggered by scheduler or order sync
    # ------------------------------------------------------------------
    async def run_incremental_sync(self, sync_log_id: int | None) -> None:
        """Fetch NFs for the past INCREMENTAL_LOOKBACK_DAYS and close the log."""
        date_to = datetime.now(UTC).date()
        date_from = date_to - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)

        logger.info(
            "Starting incremental invoice sync",
            sync_log_id=sync_log_id,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )

        total = await self.run_date_range_sync(date_from, date_to, sync_log_id)

        if sync_log_id is not None:
            async with AsyncSessionLocal() as session:
                await SyncLogRepository(session).update_sync_log_complete(
                    sync_log_id, items_processed=total, items_failed=0
                )

        logger.info(
            "Incremental invoice sync completed",
            sync_log_id=sync_log_id,
            total=total,
        )

    # ------------------------------------------------------------------
    # Per-window worker — called by InvoiceFullSyncConsumer
    # ------------------------------------------------------------------
    async def run_date_range_sync(
        self,
        date_from: date,
        date_to: date,
        sync_log_id: int | None,
        *,
        fetch_items: bool = True,
    ) -> int:
        """Paginate Tiny /notas for ``[date_from, date_to]``, map and upsert.

        When ``fetch_items`` is True (default), every invoice in the window
        also gets a ``GET /notas/{id}`` detail call so its ``itens`` are
        persisted into ``invoice_items``. Cold-start passes False to keep
        the historical full-catalog backfill cheap — items are populated
        later by a focused backfill script for the relevant date window.

        Returns the total number of NFs upserted. When ``sync_log_id`` is
        not None, atomically increments ``items_processed`` after each page
        and calls ``try_finalize`` to let the cold-start log close itself
        once all windows are done.
        """
        total_processed = 0
        offset = 0

        logger.info(
            "Starting invoice date range sync",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            sync_log_id=sync_log_id,
        )

        while True:
            try:
                response = await self._tiny.list_invoices(
                    date_initial=date_from,
                    date_final=date_to,
                    limit=PAGE_SIZE,
                    offset=offset,
                )
            except TinyAPIException as exc:
                logger.error(
                    "Tiny API error fetching invoices",
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    offset=offset,
                    error=str(exc),
                )
                raise

            items = response.get("itens") or []
            pagination = response.get("paginacao") or {}
            total = int(pagination.get("total", 0))

            if not items:
                break

            invoices = [InvoiceMapper.from_tiny_api(item) for item in items]

            async with AsyncSessionLocal() as session:
                repo = PostgreSQLInvoiceRepository(session)
                sync_logs = SyncLogRepository(session)

                count = await repo.upsert_batch(invoices)
                total_processed += count

                if sync_log_id is not None:
                    await sync_logs.add_to_processed(sync_log_id, count)
                    await sync_logs.try_finalize(sync_log_id)

            if fetch_items:
                await self._sync_items_for_invoices(invoices)

            logger.debug(
                "Invoice page upserted",
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                offset=offset,
                page_count=len(items),
                api_total=total,
                cumulative=total_processed,
                fetched_items=fetch_items,
            )

            offset += PAGE_SIZE
            if (total and offset >= total) or len(items) < PAGE_SIZE:
                break

        logger.info(
            "Invoice date range sync done",
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
            total_processed=total_processed,
        )
        return total_processed

    # ------------------------------------------------------------------
    # Items detail backfill helper
    # ------------------------------------------------------------------
    async def sync_items_for_invoice(self, invoice_tiny_id: int) -> int:
        """Fetch ``GET /notas/{id}`` and persist its ``itens`` array.

        Returns the number of lines persisted. Tolerates per-NF failures —
        callers can retry the same invoice without side effects because the
        repo replaces the line set atomically.
        """
        detail = await self._tiny.get_invoice(invoice_tiny_id)
        items = InvoiceMapper.items_from_tiny_detail(invoice_tiny_id, detail)
        async with AsyncSessionLocal() as session:
            repo = PostgreSQLInvoiceItemRepository(session)
            return await repo.replace_for_invoice(invoice_tiny_id, items)

    async def _sync_items_for_invoices(self, invoices: list[dict[str, Any]]) -> None:
        """Best-effort items fetch for a batch of invoices already upserted.

        One Tiny call per NF, run sequentially because the client already
        rate-limits at a global level. Failures are logged and swallowed —
        we keep the header upsert even when the detail call fails so the
        next incremental run can retry just the items.
        """
        for inv in invoices:
            tiny_id = int(inv["tiny_id"])
            try:
                await self.sync_items_for_invoice(tiny_id)
            except Exception as exc:
                logger.warning(
                    "Invoice items detail fetch failed, continuing",
                    invoice_tiny_id=tiny_id,
                    error=str(exc),
                )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _set_total_enqueued(sync_log_id: int, total: int) -> None:
    from sqlalchemy import select, update

    from tiny_mirror.infrastructure.orm.models import SyncLogORM

    async with AsyncSessionLocal() as session:
        current = await session.execute(
            select(SyncLogORM.sync_metadata).where(SyncLogORM.id == sync_log_id)
        )
        metadata = current.scalar_one_or_none() or {}
        metadata = {**metadata, "total_enqueued": total}
        await session.execute(
            update(SyncLogORM).where(SyncLogORM.id == sync_log_id).values(sync_metadata=metadata)
        )
        await session.commit()
