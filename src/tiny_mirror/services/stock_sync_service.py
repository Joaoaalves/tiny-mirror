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

FL stock computation rules (mirrors ml_fl_stock_dryrun.py):

  Simple / variant (type S/V):
      own FL inventory (Inventory API) +
      sum(parent_kit FL inventory x component_qty_in_kit)

  Quantity kit (type K, SKU ~ ^\\d+U-):
      base_sku FL inventory ÷ X  (integer division)

  Combo (type K, SKU not matching ^\\d+U-):
      own FL inventory only

Inventory stock is read from GET /inventories/{id}/stock/fulfillment
(the authoritative FL warehouse count), not from item.available_quantity.
ML listings are looked up via the ml_listings DB table (populated daily
by MLListingSyncService) rather than per-SKU ML API searches.

Each method opens its own ``AsyncSession`` so the service is safe to
share between long-lived consumer contexts.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException, TinyNotFoundException
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import MLListingORM, MLListingVariationORM
from tiny_mirror.infrastructure.repositories.fulfillment_transfer_repository import (
    FulfillmentTransferRepository,
)
from tiny_mirror.infrastructure.repositories.ml_listing_repository import (
    MLListingRepository,
)
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.stock_repository import (
    PostgreSQLStockRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.infrastructure.repositories.tiny_fl_stock_snapshot_repository import (
    TinyFLStockSnapshotRepository,
)
from tiny_mirror.mappers.stock_mapper import StockMapper
from tiny_mirror.queue.publisher import QueuePublisher

_QUANTITY_KIT_RE = re.compile(r"^(\d+)U-(.+)$")

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
    # High-frequency ML-only refresh — bypasses Tiny entirely
    # ------------------------------------------------------------------
    async def run_ml_fl_only_sync(self, sync_log_id: int) -> None:
        """Refresh the 'Full Mercado Livre' stock_deposits row for every
        product whose Full ML stock can be non-zero (own FL listing or
        component of a kit with an FL listing).

        Skips Tiny completely: only hits ML's Inventory API and writes the
        single FL deposit row via :meth:`upsert_ml_full_deposit`. Galpão,
        A Caminho and Avaria are left untouched — those are the daily
        Tiny stock_full_sync's job.

        Designed to run every ~15 min: ~100 products x 1 ML call ~ 30s,
        well under any reasonable cron interval. No fan-out per item —
        the in-process loop is simpler and keeps ordering predictable.
        """
        if self._ml is None:
            logger.warning(
                "ML FL stock sync skipped: ML client not configured",
                sync_log_id=sync_log_id,
            )
            async with AsyncSessionLocal() as session:
                await SyncLogRepository(session).update_sync_log_complete(
                    sync_log_id, items_processed=0, items_failed=0
                )
            return

        async with AsyncSessionLocal() as session:
            product_repo = PostgreSQLProductRepository(session)
            fl_products = await product_repo.list_fl_exposed_active()

        logger.info(
            "Starting ML FL-only stock sync",
            sync_log_id=sync_log_id,
            products_count=len(fl_products),
        )

        processed = 0
        failed = 0
        for tiny_id, sku, ptype in fl_products:
            try:
                async with AsyncSessionLocal() as session:
                    product_repo = PostgreSQLProductRepository(session)
                    parent_kits = await product_repo.get_parent_kits_for_sku(sku)

                ml_qty = await self._fetch_ml_full_qty(sku, ptype=ptype, parent_kits=parent_kits)
                # _fetch_ml_full_qty returns None on transient failure; treat
                # as "skip this product" (don't zero out a stale row over a
                # blip). Next tick recovers.
                if ml_qty is None:
                    logger.debug(
                        "ML FL fetch returned None, skipping update",
                        sku=sku,
                        tiny_id=tiny_id,
                    )
                    continue

                async with AsyncSessionLocal() as session:
                    stock_repo = PostgreSQLStockRepository(session)
                    await stock_repo.upsert_ml_full_deposit(
                        tiny_id,
                        int(ml_qty),
                        deposit_name=ML_FULL_DEPOSIT_NAME,
                        sentinel_deposit_id=ML_FULL_DEPOSIT_SENTINEL_ID,
                    )
                processed += 1
            except Exception as exc:
                failed += 1
                logger.warning(
                    "ML FL stock refresh failed for product, continuing",
                    tiny_id=tiny_id,
                    sku=sku,
                    error=str(exc),
                )

        # Single-pass cron: no fan-out, so we can flip 'running' → 'completed'
        # synchronously. try_finalize is gated on metadata.total_enqueued and
        # would silently leave the row in 'running' until the stale watchdog
        # marked it 'failed' 90 min later.
        async with AsyncSessionLocal() as session:
            await SyncLogRepository(session).update_sync_log_complete(
                sync_log_id, items_processed=processed, items_failed=failed
            )

        logger.info(
            "ML FL-only stock sync completed",
            sync_log_id=sync_log_id,
            processed=processed,
            failed=failed,
            total=len(fl_products),
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

        # Raw Tiny FL + galpão values BEFORE any ML overlay. Both drive
        # the webhook corroboration path so we never compare the
        # ML-overlaid stock_deposits row against the next raw Tiny reading.
        new_tiny_fl_qty = _sum_tiny_fl_available(deposits)
        new_stock_galpao_qty = _sum_tiny_galpao_available(deposits)

        # Same isolation pattern as product_sync_service: capture errors
        # so the sync-log update always runs in a fresh session and never
        # inherits an aborted-transaction state from the stock upsert.
        processing_error: Exception | None = None
        product_data: dict[str, Any] | None = None
        async with AsyncSessionLocal() as session:
            # Stock rows have a FK -> products.tiny_id (CASCADE on delete).
            # If the product has not been mirrored yet, the FK insert would
            # fail; degrade to a warning + skip instead of raising. The
            # daily product sync will pick the product up and the next
            # stock pass will succeed.
            product_repo = PostgreSQLProductRepository(session)
            product_data = await product_repo.get_by_tiny_id(product_tiny_id)
            if product_data is None:
                logger.warning(
                    "Skipping stock for product not yet synced",
                    product_tiny_id=product_tiny_id,
                )
                return

            # The Tiny "Full Mercado Livre" deposit is operationally
            # meaningless — kept by the operator only so the Tiny saída
            # flow has a target deposit. The mv_coverage stock_full_ml
            # column MUST come from Mercado Livre: ML.available_quantity
            # when the SKU has an active FL listing, zero otherwise.
            # Transient ML failures also force zero — see
            # _fetch_ml_full_qty's docstring for the rationale.
            ml_qty = 0
            if self._ml is not None and sku:
                ptype = (product_data or {}).get("type") or "S"
                parent_kits = await product_repo.get_parent_kits_for_sku(sku)
                fetched = await self._fetch_ml_full_qty(sku, ptype=ptype, parent_kits=parent_kits)
                ml_qty = fetched or 0
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

        # Delta-driven pending transfer detection. Done in its own session
        # so a failure here can never poison the upsert above. Only runs
        # when the stock upsert succeeded (processing_error is None) — we
        # don't want to invent transfers off a half-applied state.
        if processing_error is None and sku:
            try:
                await self._maybe_record_webhook_transfer(
                    product_tiny_id=product_tiny_id,
                    sku=sku,
                    new_tiny_fl_qty=new_tiny_fl_qty,
                    new_stock_galpao_qty=new_stock_galpao_qty,
                    product_data=product_data,
                )
            except Exception as exc:
                # Never raise — the snapshot/transfer side is best-effort.
                # If it fails repeatedly we'll see it in Seq; the regular
                # reception scan keeps the system self-correcting.
                logger.warning(
                    "Webhook delta detection failed, ignoring",
                    product_tiny_id=product_tiny_id,
                    sku=sku,
                    error=str(exc),
                )

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
    # Webhook delta path — compares the raw Tiny FL value vs the previous
    # snapshot. On a positive delta we also require the galpão deposit to
    # have dropped by approximately the same amount (configurable ratio)
    # before inserting a pending transfer; otherwise the +FL is likely a
    # sale cancellation or Tiny↔ML reconciliation, not a real
    # galpão→Full move.
    # ------------------------------------------------------------------
    async def _maybe_record_webhook_transfer(
        self,
        product_tiny_id: int,
        sku: str,
        new_tiny_fl_qty: int,
        product_data: dict[str, Any] | None,
        new_stock_galpao_qty: int,
    ) -> None:
        async with AsyncSessionLocal() as session:
            snapshots = TinyFLStockSnapshotRepository(session)
            previous = await snapshots.get(product_tiny_id)
            await snapshots.upsert(
                product_tiny_id,
                tiny_fl_qty=new_tiny_fl_qty,
                stock_galpao_qty=new_stock_galpao_qty,
            )

            # First observation: seed both snapshots, can't infer a
            # transfer yet (no prior reference).
            if previous is None:
                logger.debug(
                    "FL snapshot seeded (first observation)",
                    product_tiny_id=product_tiny_id,
                    sku=sku,
                    tiny_fl_qty=new_tiny_fl_qty,
                    stock_galpao_qty=new_stock_galpao_qty,
                )
                return

            fl_delta = new_tiny_fl_qty - previous.tiny_fl_qty
            if fl_delta <= 0:
                return

            # Corroboration rule (2026-05-25): a real galpão→Full transfer
            # drops galpão by ~fl_delta. Sale cancellations and
            # Tiny↔ML reconciliations leave galpão untouched (galpao_delta
            # ≈ 0). Skip when galpão didn't decrease by enough.
            galpao_delta = new_stock_galpao_qty - previous.stock_galpao_qty
            required_drop = fl_delta * settings.fl_webhook_galpao_corroboration_ratio
            galpao_drop = -galpao_delta  # positive when galpão decreased
            if galpao_drop + 0.01 < required_drop:
                logger.info(
                    "FL positive delta detected but galpão did not drop "
                    "enough — likely sale cancellation or reconciliation, "
                    "not a real transfer; skipping",
                    product_tiny_id=product_tiny_id,
                    sku=sku,
                    fl_delta=fl_delta,
                    galpao_delta=galpao_delta,
                    required_drop=required_drop,
                    new_tiny_fl_qty=new_tiny_fl_qty,
                    new_stock_galpao_qty=new_stock_galpao_qty,
                )
                return

            transfers = FulfillmentTransferRepository(session)
            now = datetime.now(UTC)
            window_start = now - timedelta(minutes=settings.fl_webhook_delta_idempotency_minutes)
            if await transfers.has_recent_pending(sku, since=window_start):
                logger.info(
                    "FL positive delta corroborated but a recent pending "
                    "transfer already exists — skipping duplicate",
                    product_tiny_id=product_tiny_id,
                    sku=sku,
                    fl_delta=fl_delta,
                    new_tiny_fl_qty=new_tiny_fl_qty,
                )
                return

            # Logistic-type guard: only create a pending transfer when the
            # SKU has at least one ml_listings row with
            # logistic_type='fulfillment'. Otherwise we'd be queuing a
            # transfer that ML will never confirm (xd_drop_off products are
            # shipped directly by the seller — no INBOUND_RECEPTION event)
            # and that distorts coverage math until manually cancelled.
            # SKUs absent from ml_listings entirely (typically kit
            # components) keep the previous behaviour — we don't know
            # which kit's MLB drives them, so we still record and let the
            # operator review.
            fl_rows, any_rows = await MLListingRepository(session).sku_logistic_status(sku)
            if any_rows > 0 and fl_rows == 0:
                logger.info(
                    "FL positive delta detected but SKU has no fulfillment "
                    "listing on ML — skipping transfer (would never reconcile)",
                    product_tiny_id=product_tiny_id,
                    sku=sku,
                    fl_delta=fl_delta,
                    new_tiny_fl_qty=new_tiny_fl_qty,
                )
                return

            cost = _extract_cost_price(product_data)
            await transfers.create(
                product_tiny_id=product_tiny_id,
                product_sku=sku,
                quantity=fl_delta,
                cost_per_unit=cost,
                transferred_at=now,
                notes=(
                    "Detected via Tiny stock webhook: Full ML deposit grew "
                    f"from {previous.tiny_fl_qty} to {new_tiny_fl_qty} "
                    f"(galpão {previous.stock_galpao_qty} → {new_stock_galpao_qty})."
                ),
                source="tiny_webhook",
            )
            await session.commit()

            logger.info(
                "FL pending transfer recorded from webhook delta",
                product_tiny_id=product_tiny_id,
                sku=sku,
                previous_tiny_fl=previous.tiny_fl_qty,
                new_tiny_fl=new_tiny_fl_qty,
                fl_delta=fl_delta,
                galpao_delta=galpao_delta,
            )

    # ------------------------------------------------------------------
    # ML helper — computes the authoritative Full-ML stock for a SKU.
    # Uses ml_listings DB (populated daily by MLListingSyncService) to
    # look up inventory_ids, then calls GET /inventories/{id}/stock/fulfillment
    # for the true warehouse count. parent_kits comes from
    # product_kit_components and lists kits whose FL stock contributes
    # units of this SKU.
    #
    # Returns ``int >= 0`` when ML responded successfully (the sum of
    # ``available_quantity`` across all relevant inventories, or zero
    # when the SKU has no FL listing). Returns ``None`` only on a
    # transient ML API failure — the caller treats that as zero too, so
    # the Tiny FL deposit never leaks into mv_coverage. The trade-off:
    # during an ML outage a daily sync zeroes everyone out; the next
    # sync recovers. This is preferred over trusting Tiny's view, which
    # is operationally meaningless.
    # ------------------------------------------------------------------
    async def _fetch_ml_full_qty(
        self,
        sku: str,
        ptype: str = "S",
        parent_kits: list[tuple[str, int]] | None = None,
    ) -> int | None:
        if self._ml is None:
            return None

        try:
            # Quantity kit (e.g. "3U-BASE-SKU"): divide base SKU inventory by X.
            m = _QUANTITY_KIT_RE.match(sku) if ptype == "K" else None
            if ptype == "K" and m:
                x = int(m.group(1))
                base_sku = m.group(2)
                if x <= 0:
                    return 0
                base_fl = await self._fl_for_sku(base_sku)
                return (base_fl // x) if base_fl is not None else None

            # Simple (S) / variant (V) / combo (K without qty-kit pattern):
            # own FL inventory + sum of parent-kit contributions.
            own_fl = await self._fl_for_sku(sku)
            has_any = own_fl is not None
            total = own_fl or 0

            for kit_sku, component_qty in parent_kits or []:
                kit_fl = await self._fl_for_sku(kit_sku)
                if kit_fl is not None:
                    has_any = True
                    total += kit_fl * component_qty

            return total if has_any else None

        except Exception as exc:
            logger.warning("ML FL fetch failed, skipping ML overlay", sku=sku, error=str(exc))
            return None

    async def _fl_for_sku(self, sku: str) -> int | None:
        """Return total FL fulfillment inventory for a SKU.

        Looks up ml_listings (populated by MLListingSyncService) for all
        fulfillment listings matching the SKU, then calls the Inventory API for
        each distinct inventory_id.  Returns None when no fulfillment listing
        exists (caller should treat as "unknown"), or an int >= 0 otherwise.
        """
        async with AsyncSessionLocal() as session:
            listing_rows = (
                await session.execute(
                    select(
                        MLListingORM.mlb_id,
                        MLListingORM.inventory_id,
                        MLListingORM.has_variations,
                    ).where(
                        MLListingORM.sku == sku,
                        MLListingORM.logistic_type == "fulfillment",
                    )
                )
            ).all()

            if not listing_rows:
                return None

            inv_ids: set[str] = set()
            for mlb_id, inventory_id, has_variations in listing_rows:
                if has_variations:
                    var_inv_ids = (
                        (
                            await session.execute(
                                select(MLListingVariationORM.inventory_id).where(
                                    MLListingVariationORM.mlb_id == mlb_id,
                                    MLListingVariationORM.inventory_id.isnot(None),
                                )
                            )
                        )
                        .scalars()
                        .all()
                    )
                    inv_ids.update(x for x in var_inv_ids if x is not None)
                elif inventory_id:
                    inv_ids.add(inventory_id)

        if not inv_ids:
            return 0

        assert self._ml is not None
        total = 0
        for inv_id in inv_ids:
            stock_data = await self._ml.get_inventory_stock(inv_id)
            total += int(stock_data.get("available_quantity") or 0)
        return total

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
def _sum_tiny_fl_available(deposits: list[dict[str, Any]]) -> int:
    """Sum of ``available`` across Tiny's 'Full Mercado Livre' deposits.

    Matches by name (case-insensitive) so we are insulated from Tiny
    deposit-id renames. Floors at 0 because Tiny occasionally returns
    negative balances on the FL row (desync between Tiny and ML), which
    are not meaningful as a "qty in transit" baseline.
    """
    total = 0.0
    for d in deposits:
        name = (d.get("deposit_name") or "").lower()
        if "full mercado livre" in name:
            total += float(d.get("available") or 0)
    return max(0, int(total))


def _sum_tiny_galpao_available(deposits: list[dict[str, Any]]) -> int:
    """Sum of ``available`` across Tiny's 'Galpão' deposits.

    Used by the webhook delta detector to corroborate that a positive FL
    delta is matched by a galpão drop (= real transfer) rather than a
    sale cancellation (= galpão untouched). Same case-insensitive name
    match as ``_sum_tiny_fl_available``; floors at 0 for symmetry.
    """
    total = 0.0
    for d in deposits:
        name = (d.get("deposit_name") or "").lower()
        if "galpão" in name or "galpao" in name:
            total += float(d.get("available") or 0)
    return max(0, int(total))


def _extract_cost_price(product_data: dict[str, Any] | None) -> Decimal:
    """Best-effort cost lookup for the webhook transfer row.

    Reads ``prices.cost_price`` from the products table (ProductORM.prices
    JSONB). Returns Decimal('0') when missing — fulfillment_transfers
    requires NOT NULL and the cost is only informational on the
    webhook-inferred row (the operator already booked the real cost on
    the Tiny saída/entrada movements).
    """
    if not product_data:
        return Decimal("0")
    prices = product_data.get("prices") or {}
    raw = prices.get("cost_price") or prices.get("price") or 0
    try:
        return Decimal(str(raw))
    except Exception:
        return Decimal("0")


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
