"""Invoice (NF) synchronization service.

Cold start:  :meth:`run_cold_start` fans out 30-day date windows from
``COLD_START_FROM`` to today, publishing one ``invoices.full`` message per
window. All windows share the same ``sync_log_id``; ``total_enqueued`` is set
to the global invoice count (from a preflight API call) so ``try_finalize``
closes the log automatically when every page has been upserted.

Incremental: :meth:`run_incremental_sync` fetches the last
``INCREMENTAL_LOOKBACK_DAYS`` of NFs inline (no fan-out needed; the window
is small), then re-reads the supplier NFs of the last
``SUPPLIER_ENTRY_LOOKBACK_DAYS`` and closes the log with
``update_sync_log_complete``.

Tiny's ``/notas`` date filter is the ISSUE date. A supplier NF is registered
in Tiny (XML import, ``tipo=E`` / ``situacao=8``) only when the goods arrive,
days or weeks after the supplier issued it, so it is already outside the
2-day window when it appears. Without the second pass 252 of 306 supplier
NFs issued between 2026-05-15 and 2026-11-18 never reached the mirror.

Date-range: :meth:`run_date_range_sync` is the shared worker called by
both paths. It paginates the Tiny ``/notas`` endpoint, maps each page with
:class:`InvoiceMapper`, batch-upserts via the repository, and atomically
increments ``items_processed`` on the sync_log.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import exists, select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import InvoiceItemORM, InvoiceORM
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
SUPPLIER_ENTRY_LOOKBACK_DAYS = 90
SUPPLIER_ENTRY_TYPE = "E"
SUPPLIER_ENTRY_STATUS = "8"
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
        """Fetch recent NFs plus late-registered supplier NFs and close the log."""
        date_to = datetime.now(UTC).date()
        date_from = date_to - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)

        logger.info(
            "Starting incremental invoice sync",
            sync_log_id=sync_log_id,
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat(),
        )

        total = await self.run_date_range_sync(date_from, date_to, sync_log_id)
        total += await self.sync_supplier_entries(
            date_to - timedelta(days=SUPPLIER_ENTRY_LOOKBACK_DAYS), date_to, sync_log_id
        )

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

    async def sync_supplier_entries(
        self, date_from: date, date_to: date, sync_log_id: int | None
    ) -> int:
        """Upsert supplier NFs (tipo E, situacao 8) issued in ``[date_from, date_to]``.

        Re-reads the whole window every run because the issue date says
        nothing about when the NF was registered. Items are fetched only for
        NFs that have none yet, so a daily 90-day pass costs a few list pages.
        """
        return await self.run_date_range_sync(
            date_from,
            date_to,
            sync_log_id,
            invoice_type=SUPPLIER_ENTRY_TYPE,
            status=SUPPLIER_ENTRY_STATUS,
            items_only_when_missing=True,
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
        invoice_type: str | None = None,
        status: str | None = None,
        items_only_when_missing: bool = False,
    ) -> int:
        """Paginate Tiny /notas for ``[date_from, date_to]``, map and upsert.

        When ``fetch_items`` is True (default), the ``itens`` of every
        invoice in the window are persisted into ``invoice_items``. The list
        payload already carries them, field for field the same as
        ``GET /notas/{id}``, so the detail call is only a fallback for a row
        that arrives without lines. Cold-start passes False to keep the
        historical full-catalog backfill cheap — items are populated later by
        a focused backfill script for the relevant date window.

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
                    invoice_type=invoice_type,
                    status=status,
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
                await self._persist_items(invoices, items, items_only_when_missing)

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

    async def backfill_missing_items(
        self,
        *,
        origin_type: str | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int | None = None,
    ) -> dict[str, int]:
        """Fetch items for mirrored invoices that have none.

        The 2026-05-07 cold start stored headers only (items skipped to keep
        it cheap); every NF issued before mid-May 2026 lacks its lines —
        including 619 return NFs the DASH needs per SKU. Newest first, since
        old NFs are being deleted in Tiny to free storage (oldest first); a
        NF already gone (404) is counted and skipped. Sequential: the Tiny
        client rate-limits globally.
        """
        query = select(InvoiceORM.tiny_id).where(
            ~exists().where(InvoiceItemORM.invoice_tiny_id == InvoiceORM.tiny_id)
        )
        if origin_type is not None:
            query = query.where(InvoiceORM.origin_type == origin_type)
        if date_from is not None:
            query = query.where(InvoiceORM.issue_date >= date_from)
        if date_to is not None:
            query = query.where(InvoiceORM.issue_date <= date_to)
        query = query.order_by(InvoiceORM.issue_date.desc(), InvoiceORM.tiny_id.desc())
        if limit is not None:
            query = query.limit(limit)
        async with AsyncSessionLocal() as session:
            ids = [int(tid) for (tid,) in (await session.execute(query)).all()]

        stats = {"candidates": len(ids), "filled": 0, "lines": 0, "gone_in_tiny": 0, "failed": 0}
        logger.info("invoice_items_backfill.started", origin_type=origin_type, **stats)
        for tiny_id in ids:
            try:
                stats["lines"] += await self.sync_items_for_invoice(tiny_id)
                stats["filled"] += 1
            except TinyNotFoundException:
                stats["gone_in_tiny"] += 1
            except Exception as exc:
                stats["failed"] += 1
                logger.warning(
                    "invoice_items_backfill.failed", invoice_tiny_id=tiny_id, error=str(exc)
                )
        logger.info("invoice_items_backfill.done", origin_type=origin_type, **stats)
        return stats

    async def _persist_items(
        self,
        invoices: list[dict[str, Any]],
        raw_items: list[dict[str, Any]],
        only_missing: bool,
    ) -> None:
        """Store the lines of one ``/notas`` page.

        Re-reading every NF of the day through ``GET /notas/{id}`` on each
        30-min order sync was ~90% of the mirror's Tiny calls (2026-11-24);
        the list rows carry the same ``itens`` (checked up to 49 lines).
        """
        if only_missing:
            missing = {int(inv["tiny_id"]) for inv in await _without_items(invoices)}
            raw_items = [raw for raw in raw_items if int(raw["id"]) in missing]
        listed = [raw for raw in raw_items if _listed_lines(raw)]
        if listed:
            async with AsyncSessionLocal() as session:
                repo = PostgreSQLInvoiceItemRepository(session)
                for raw in listed:
                    tiny_id = int(raw["id"])
                    try:
                        await repo.replace_for_invoice(
                            tiny_id, InvoiceMapper.items_from_tiny_detail(tiny_id, raw)
                        )
                    except Exception as exc:
                        await session.rollback()
                        logger.warning(
                            "Invoice items from listing failed, continuing",
                            invoice_tiny_id=tiny_id,
                            error=str(exc),
                        )
        await self._sync_items_for_invoices(
            [{"tiny_id": int(raw["id"])} for raw in raw_items if not _listed_lines(raw)]
        )

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
def _listed_lines(raw: dict[str, Any]) -> bool:
    lines = raw.get("itens")
    return isinstance(lines, list) and bool(lines)


async def _without_items(invoices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ids = [int(inv["tiny_id"]) for inv in invoices]
    if not ids:
        return []
    async with AsyncSessionLocal() as session:
        rows = await session.execute(
            select(InvoiceItemORM.invoice_tiny_id)
            .where(InvoiceItemORM.invoice_tiny_id.in_(ids))
            .distinct()
        )
        with_items = {int(tid) for (tid,) in rows.all()}
    return [inv for inv in invoices if int(inv["tiny_id"]) not in with_items]


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
