"""Syncs daily deposit-level stock snapshots from Tiny v2 lista.atualizacoes.estoque.

Called once per day. Fetches all products with stock changes in the past
day and upserts one row per (product, deposit) into stock_history. The
snapshots represent the current balance at fetch time, not individual
transactions.

The 30-day lookback limit on the Tiny v2 API means we can't backfill
history beyond that. The cold start uses yesterday (1 day back) so we
capture everything that moved in the last 24h.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import text

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.infrastructure.external.tiny_v2_client import TinyV2Client
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository

logger = structlog.get_logger(__name__)


class StockHistorySyncService:
    def __init__(self, tiny_v2: TinyV2Client) -> None:
        self._v2 = tiny_v2

    async def run_sync(self, sync_log_id: int, lookback_days: int = 1) -> None:
        """Fetch stock updates since ``lookback_days`` ago and upsert into stock_history."""
        lookback_days = max(1, min(lookback_days, 30))
        since = datetime.now(UTC).date() - timedelta(days=lookback_days)
        logger.info(
            "Starting stock history sync",
            since=since.isoformat(),
            lookback_days=lookback_days,
            sync_log_id=sync_log_id,
        )

        try:
            products = await self._v2.list_stock_updates(since)
        except TinyAPIException as exc:
            logger.error("Tiny v2 stock history fetch failed", error=str(exc))
            async with AsyncSessionLocal() as session:
                await SyncLogRepository(session).update_sync_log_failed(
                    sync_log_id, error_message=str(exc), items_processed=0, items_failed=1
                )
            return

        snapshot_date = datetime.now(UTC).date()
        processed = 0
        failed = 0

        async with AsyncSessionLocal() as session:
            for product in products:
                try:
                    await self._upsert_product_snapshot(session, product, snapshot_date)
                    processed += 1
                except Exception as exc:
                    logger.warning(
                        "Failed to upsert stock_history row",
                        product_id=product.get("id"),
                        error=str(exc),
                    )
                    failed += 1
            await session.commit()

        async with AsyncSessionLocal() as session:
            repo = SyncLogRepository(session)
            if failed > 0 and processed == 0:
                await repo.update_sync_log_failed(
                    sync_log_id,
                    error_message=f"All {failed} products failed",
                    items_processed=0,
                    items_failed=failed,
                )
            else:
                await repo.update_sync_log_complete(
                    sync_log_id, items_processed=processed, items_failed=failed
                )

        logger.info(
            "Stock history sync complete",
            snapshot_date=snapshot_date.isoformat(),
            processed=processed,
            failed=failed,
        )

    async def _upsert_product_snapshot(
        self, session: Any, product: dict[str, Any], snapshot_date: date
    ) -> None:
        product_id = int(product["id"])
        sku = product.get("codigo") or ""
        deposits = product.get("depositos") or []

        for dep_item in deposits:
            dep = dep_item.get("deposito") if isinstance(dep_item, dict) else None
            if dep is None:
                continue
            deposit_name = dep.get("nome") or ""
            balance = int(float(dep.get("saldo") or 0))

            await session.execute(
                text("""
                    INSERT INTO stock_history
                        (product_tiny_id, product_sku, snapshot_date, deposit_name, balance, synced_at)
                    VALUES
                        (:pid, :sku, :snap_date, :dep_name, :balance, NOW())
                    ON CONFLICT (product_tiny_id, snapshot_date, deposit_name)
                    DO UPDATE SET
                        product_sku  = EXCLUDED.product_sku,
                        balance      = EXCLUDED.balance,
                        synced_at    = EXCLUDED.synced_at
                """),
                {
                    "pid": product_id,
                    "sku": sku,
                    "snap_date": snapshot_date,
                    "dep_name": deposit_name,
                    "balance": balance,
                },
            )
