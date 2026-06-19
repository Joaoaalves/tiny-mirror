"""Detects ML fulfillment reception events and marks pending transfers received.

Runs on a schedule (every 6h by default). For each SKU with pending transfers:

1. Looks up ALL fulfillment ``inventory_id`` values for the SKU — from
   ``ml_listings`` and (when the listing has variations) from
   ``ml_listing_variations``. A SKU split across multiple MLBs / variations
   maps to multiple inventories; counting only one would under-credit
   receptions.
2. For each inventory, lists fulfillment operations from ``oldest_pending``
   to now (chunked at ≤59 days, ML's 60d hard limit).
3. Keeps only events whose type is ``INBOUND_RECEPTION`` (batch barcode
   receptions) or ``TRANSFER_DELIVERY`` (unit-by-unit seller-managed
   inbounds) and whose ``detail.available_quantity`` is positive — both
   represent units physically arriving at the FL CD.
4. FIFO-matches transfers (oldest first) against events. **Chronology
   guard**: an event is eligible to fulfill a transfer only when its
   ``date_created`` ≥ the transfer's ``transferred_at``; otherwise newer
   transfers would steal credit from older, post-event arrivals.

Examples:
- 1 transfer of 10 + 1 INBOUND_RECEPTION of 10 → marked received.
- 2 transfers of 5 each + 17 TRANSFER_DELIVERY events of 1 each → both
  received (TRANSFER_DELIVERY flow now counts).
- 1 transfer of 10 (2026-05-15) + 5 events of 2 each, all dated
  2026-05-10 → stays pending (events older than transfer can't fulfill).

This module owns the ``Decision`` shape (no separate domain layer). The
scan persists ``received_at`` to the timestamp of the event that finally
covered the transfer's quantity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import structlog

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.repositories.fulfillment_transfer_repository import (
    FulfillmentTransferRepository,
)

if TYPE_CHECKING:
    from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
    from tiny_mirror.infrastructure.orm.models import FulfillmentTransferORM

logger = structlog.get_logger(__name__)


# Event types ML emits that represent units PHYSICALLY ARRIVING from the
# seller (i.e. units that should decrement our pending fulfillment_transfers).
#
# Doc story (per ML's official Fulfillment Operations spec):
#  - ``INBOUND_RECEPTION``: "Novo estoque: o processo inbound disponibiliza
#    unidades para venda" → seller inbound. Should count.
#  - ``TRANSFER_DELIVERY``: "Ação INTERNA do estoque do Mercado Livre" →
#    warehouse-to-warehouse moves. Should NOT count.
#
# 2026-06-05 took the doc literally and restricted to INBOUND_RECEPTION
# only. 2026-06-07 audit (RON-COLL-PRE + 49 other inventories, n=2019
# TRANSFER_DELIVERY events) proved the doc is wrong in practice:
#  - 0 INBOUND_RECEPTION events for RON-COLL-PRE in 60d despite ~25 known
#    seller T entries arriving.
#  - 2019/2019 TRANSFER_DELIVERY events carry
#    ``external_references[].type == "inbound_id"`` linking them to a
#    seller inbound order. They ARE the unit-by-unit reception.
#  - True ML-internal moves use TRANSFER_RESERVATION (negative qty, empty
#    external_references) + a paired TRANSFER_DELIVERY (which we never
#    observe — those would also be empty external_references if they did).
#
# Discriminator: include both types as candidates, then filter at the
# event level via ``_is_seller_inbound_event``. Internal-move
# TRANSFER_DELIVERY (no inbound_id) is filtered out; seller-inbound
# TRANSFER_DELIVERY (with inbound_id) credits the FIFO normally.
INBOUND_EVENT_TYPES = frozenset({"INBOUND_RECEPTION", "TRANSFER_DELIVERY"})


def _is_seller_inbound_event(op: dict[str, Any]) -> bool:
    """True when ``op`` represents the seller's shipment arriving at ML.

    INBOUND_RECEPTION is always a seller reception per the docs (rare in
    practice; some categories/SKUs never emit it). TRANSFER_DELIVERY must
    additionally carry an ``inbound_id`` link in its external_references
    — that's the empirical marker that distinguishes ML's documented
    seller inbound flow (which they implemented as TRANSFER_DELIVERY in
    practice) from ML's internal warehouse-to-warehouse moves.

    Negative-quantity events (TRANSFER_RESERVATION et al.) are filtered
    out downstream by the ``qty > 0`` check in ``_fetch_chunk_inbound``;
    this discriminator is purely about event TYPE intent.
    """
    op_type = op.get("type")
    if op_type == "INBOUND_RECEPTION":
        return True
    if op_type == "TRANSFER_DELIVERY":
        refs = op.get("external_references") or []
        return any(isinstance(r, dict) and r.get("type") == "inbound_id" for r in refs)
    return False


@dataclass
class ReconciliationResult:
    skus_scanned: int = 0
    transfers_received: int = 0
    transfers_partially_received: int = 0
    transfers_cancelled: int = 0
    skus_with_no_inventory: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class FulfillmentReceptionService:
    def __init__(self, ml_client: MercadoLivreAPIClient) -> None:
        self._ml = ml_client

    async def scan_and_reconcile(self) -> ReconciliationResult:
        """Scan all pending transfers and mark them received when ML confirms arrival.

        First runs ``_cancel_non_fulfillment_pending`` to clean up transfers
        whose SKU is no longer on the FL channel — those will never get an
        INBOUND_RECEPTION event, so leaving them pending corrupts coverage
        math forever. Then groups by SKU, fetches inbound events from ML,
        and credits each transfer in FIFO+chronology order (supporting
        partial reception so a transfer of 11 that arrives 6+5 in two
        events is correctly credited as the events land).
        """
        from sqlalchemy import select

        from tiny_mirror.infrastructure.orm.models import (
            MLListingORM,
            MLListingVariationORM,
        )

        result = ReconciliationResult()

        # 1) Auto-cancel transfers whose SKU is no longer fulfillment on ML.
        #    Webhook may have created these legitimately (the product *was*
        #    FL at lance time) or by accident — either way ML will never
        #    emit an inbound event, so they must come out of the pending pool.
        cancelled = await self._cancel_non_fulfillment_pending()
        result.transfers_cancelled = cancelled

        async with AsyncSessionLocal() as session:
            repo = FulfillmentTransferRepository(session)
            pending_rows, _ = await repo.list_all(status="pending", limit=500)

        if not pending_rows:
            logger.info("No pending fulfillment transfers to reconcile")
            return result

        # Group transfers by SKU.
        by_sku: dict[str, list[FulfillmentTransferORM]] = {}
        for row in pending_rows:
            by_sku.setdefault(row.product_sku, []).append(row)

        # Build a SKU -> list of (inventory_id, multiplier) map.
        #
        # multiplier=1 → SKU is sold as itself on FL (direct listing).
        # multiplier=N → SKU has no direct FL listing but is a component
        #                of a kit whose FL listing exposes this base SKU
        #                through its parent inventory. One unit recorded
        #                by ML on the parent's inventory means N component
        #                units physically arrived. Used for Bug-4 SKUs like
        #                RTA-GAV4-P that were swapped to xd_drop_off but
        #                are still shipped to FL as part of 6U/12U/3U kits.
        sku_list = list(by_sku.keys())
        inventory_map: dict[str, list[tuple[str, int]]] = {}
        async with AsyncSessionLocal() as session:
            main_rows = (
                await session.execute(
                    select(
                        MLListingORM.sku,
                        MLListingORM.mlb_id,
                        MLListingORM.inventory_id,
                        MLListingORM.has_variations,
                    ).where(
                        MLListingORM.sku.in_(sku_list),
                        MLListingORM.logistic_type == "fulfillment",
                    )
                )
            ).all()

            variation_mlb_ids: list[str] = []
            for sku, mlb_id, inventory_id, has_variations in main_rows:
                if has_variations:
                    variation_mlb_ids.append(mlb_id)
                elif inventory_id:
                    inventory_map.setdefault(sku, []).append((inventory_id, 1))

            if variation_mlb_ids:
                var_rows = (
                    await session.execute(
                        select(
                            MLListingORM.sku,
                            MLListingVariationORM.inventory_id,
                        )
                        .join(MLListingORM, MLListingORM.mlb_id == MLListingVariationORM.mlb_id)
                        .where(
                            MLListingVariationORM.mlb_id.in_(variation_mlb_ids),
                            MLListingVariationORM.inventory_id.isnot(None),
                        )
                    )
                ).all()
                for sku, inventory_id in var_rows:
                    if inventory_id:
                        inventory_map.setdefault(sku, []).append((inventory_id, 1))

            # Kit-parent inventories (with multiplier = comp_per_kit). Run for
            # EVERY SKU, not only those without a direct FL inventory: a
            # kit-component like RTA-GAV6-P is sold standalone (own inventory)
            # AND inside kits, so a kit shipment inflates the component's Tiny
            # FL deposit and creates a component transfer whose units physically
            # land on the PARENT kit's inventory. Checking only the component's
            # own inventory left those transfers pending forever (759 sent / 33
            # received on RTA-GAV6-P). We union both inventory sources; per-SKU
            # FIFO matching caps crediting at the transfer qty, so the distinct
            # own- vs parent-inventory events never double-credit.
            skus_with_parents = sku_list
            if skus_with_parents:
                from sqlalchemy import text

                parent_rows = (
                    await session.execute(
                        text(
                            """
                            SELECT
                                ft.product_sku AS base_sku,
                                ml.mlb_id,
                                ml.inventory_id,
                                ml.has_variations,
                                kc.quantity AS comp_per_kit
                            FROM (SELECT DISTINCT product_sku, product_tiny_id
                                  FROM fulfillment_transfers
                                  WHERE product_sku = ANY(:sku_list)) ft
                            JOIN product_kit_components kc
                              ON kc.component_product_tiny_id = ft.product_tiny_id
                            JOIN products p_kit
                              ON p_kit.tiny_id = kc.kit_product_tiny_id
                            JOIN ml_listings ml
                              ON ml.sku = p_kit.sku
                            WHERE ml.logistic_type = 'fulfillment'
                              AND ml.status IN ('active', 'paused')
                            """
                        ),
                        {"sku_list": skus_with_parents},
                    )
                ).all()

                parent_variation_mlbs: list[tuple[str, str, int]] = []
                for base_sku, mlb_id, inv_id, has_var, comp_per_kit in parent_rows:
                    mult = max(1, int(comp_per_kit or 1))
                    if has_var:
                        parent_variation_mlbs.append((base_sku, mlb_id, mult))
                    elif inv_id:
                        inventory_map.setdefault(base_sku, []).append((inv_id, mult))

                if parent_variation_mlbs:
                    mlb_ids = list({m for _, m, _ in parent_variation_mlbs})
                    parent_var_rows = (
                        await session.execute(
                            select(
                                MLListingVariationORM.mlb_id,
                                MLListingVariationORM.inventory_id,
                            ).where(
                                MLListingVariationORM.mlb_id.in_(mlb_ids),
                                MLListingVariationORM.inventory_id.isnot(None),
                            )
                        )
                    ).all()
                    by_mlb_inv: dict[str, list[str]] = {}
                    for mlb_id, inv_id in parent_var_rows:
                        if inv_id:
                            by_mlb_inv.setdefault(mlb_id, []).append(inv_id)
                    for base_sku, mlb_id, mult in parent_variation_mlbs:
                        for inv_id in by_mlb_inv.get(mlb_id, []):
                            inventory_map.setdefault(base_sku, []).append((inv_id, mult))

        # Deduplicate (inventory_id, multiplier) per SKU.
        for sku in list(inventory_map.keys()):
            inventory_map[sku] = sorted(set(inventory_map[sku]))

        for sku, transfers in by_sku.items():
            result.skus_scanned += 1
            inventory_sources = inventory_map.get(sku) or []
            if not inventory_sources:
                logger.warning(
                    "No fulfillment inventory_id found for SKU, skipping",
                    sku=sku,
                )
                result.skus_with_no_inventory.append(sku)
                continue

            transfers_sorted = sorted(transfers, key=lambda t: t.transferred_at)
            oldest_date = transfers_sorted[0].transferred_at

            try:
                events = await self._fetch_inbound_events(
                    inventory_sources=inventory_sources,
                    oldest_transfer_date=oldest_date,
                )
            except Exception as exc:
                logger.error(
                    "Failed to fetch fulfillment operations for SKU",
                    sku=sku,
                    inventory_sources=inventory_sources,
                    error=str(exc),
                )
                result.errors.append(f"{sku}: {exc}")
                continue

            if not events:
                logger.debug(
                    "No INBOUND_RECEPTION/TRANSFER_DELIVERY events found for SKU",
                    sku=sku,
                    inventory_sources=inventory_sources,
                )
                continue

            decisions = _fifo_match_with_chronology(transfers_sorted, events)
            actionable = [d for d in decisions if d.delta_units > 0]

            if not actionable:
                logger.debug(
                    "Events present but none eligible (date filter) or "
                    "no new units to credit existing pendings",
                    sku=sku,
                    inventory_sources=inventory_sources,
                    events_count=len(events),
                )
                continue

            full_decisions = [d for d in actionable if d.is_full]
            partial_decisions = [d for d in actionable if not d.is_full]

            logger.info(
                "Reception events found for SKU",
                sku=sku,
                inventory_sources=inventory_sources,
                event_count=len(events),
                full_count=len(full_decisions),
                partial_count=len(partial_decisions),
            )

            async with AsyncSessionLocal() as session:
                repo = FulfillmentTransferRepository(session)
                for d in actionable:
                    assert d.last_event_at is not None  # mypy narrow
                    await repo.apply_partial_reception(
                        d.transfer_id,
                        delta_quantity=d.delta_units,
                        last_event_at=d.last_event_at,
                    )
                await session.commit()

            result.transfers_received += len(full_decisions)
            result.transfers_partially_received += len(partial_decisions)
            logger.info(
                "Applied receptions",
                sku=sku,
                fully_received_ids=[d.transfer_id for d in full_decisions],
                partial_ids=[d.transfer_id for d in partial_decisions],
            )

        logger.info(
            "Fulfillment reception reconciliation complete",
            skus_scanned=result.skus_scanned,
            transfers_received=result.transfers_received,
            skus_missing_inventory=len(result.skus_with_no_inventory),
            errors=len(result.errors),
        )
        return result

    async def _cancel_non_fulfillment_pending(self) -> int:
        """Cancel pending transfers whose SKU is no longer on the FL channel.

        A SKU is treated as non-fulfillment when none of the following holds:
          (a) It has at least one ``ml_listings`` row with
              ``logistic_type='fulfillment'`` AND ``status IN ('active','paused')``
              — paused counts because sub_status=out_of_stock means the
              listing is still FL, just temporarily without stock.
          (b) It is a kit component of at least one product whose own
              ``ml_listings`` is fulfillment+active/paused — even though
              the base SKU has no direct FL listing, the units physically
              shipped reach FL via the parent kit's MLB inventory.

        SKUs with **zero** ml_listings rows AND no FL-listed parent kit are
        ALSO cancelled (policy change 2026-06-18): with no FL listing and no
        FL parent kit a unit has no Full channel to arrive on, so the transfer
        is phantom and would inflate the reposição coverage forever. The webhook
        only creates transfers when the SKU has an FL listing at lance time
        (``fl_rows > 0`` guard), so a no-FL-channel pending row is always stale
        legacy/test garbage — never a fresh real transfer.
        """
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            non_fl_rows = await session.execute(
                text(
                    """
                    SELECT ft.id, ft.product_sku
                    FROM fulfillment_transfers ft
                    WHERE ft.status = 'pending'
                      AND NOT EXISTS (
                          SELECT 1 FROM ml_listings ml
                          WHERE ml.sku = ft.product_sku
                            AND ml.logistic_type = 'fulfillment'
                            AND ml.status IN ('active', 'paused')
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM product_kit_components kc
                          JOIN products p_kit
                            ON p_kit.tiny_id = kc.kit_product_tiny_id
                          JOIN ml_listings ml2
                            ON ml2.sku = p_kit.sku
                          WHERE kc.component_product_tiny_id = ft.product_tiny_id
                            AND ml2.logistic_type = 'fulfillment'
                            AND ml2.status IN ('active', 'paused')
                      )
                    """
                )
            )
            rows = non_fl_rows.all()

            if not rows:
                return 0

            repo = FulfillmentTransferRepository(session)
            cancelled = 0
            for transfer_id, _sku in rows:
                await repo.mark_cancelled(
                    int(transfer_id),
                    reason="SKU no longer fulfillment in ml_listings",
                )
                cancelled += 1
            await session.commit()
            logger.info(
                "Cancelled pending transfers whose SKU is not fulfillment",
                count=cancelled,
            )
            return cancelled

    async def drain_stale_phantoms(self) -> int:
        """Cancel long-stale pending transfers ML can't account for.

        Invoked by the reception job AFTER ``scan_and_reconcile`` commits, so
        every genuinely-arrived transfer has already left the pending pool and
        only never-materialised rows remain to evaluate. Kept separate from
        ``scan_and_reconcile`` so the crediting path stays pure.

        A genuine galpão→Full send shows up as ML ``transfer`` (em
        transferência) and then ``available`` within days. ML's API never
        exposes the seller's "Entrada pendente" plan, so we can't verify that
        bucket — but a pending row older than ``fl_transfer_stale_days`` whose
        units ML does NOT currently show as ``transfer`` on the SKU's own OR
        parent-kit inventories never materialised (derived kit-stock noise or
        an abandoned inbound). We keep, per SKU:

            keep = ML em-transferência (own + parent inventories, scaled)
                 + net of rows still inside the grace window

        and cancel whole stale rows oldest-first until net ≤ keep — never
        below (residual stays ≥ what ML confirms, the safe direction). On any
        ML fetch error the SKU is skipped (no cancel). Self-heals each run.
        """
        from tiny_mirror.config import settings

        grace_days = settings.fl_transfer_stale_days
        now = datetime.now(UTC)
        cutoff = now - timedelta(days=grace_days)

        async with AsyncSessionLocal() as session:
            repo = FulfillmentTransferRepository(session)
            pending_rows, _ = await repo.list_all(status="pending", limit=500)

        by_sku: dict[str, list[FulfillmentTransferORM]] = {}
        for row in pending_rows:
            by_sku.setdefault(row.product_sku, []).append(row)

        stale_skus = sorted(
            {
                row.product_sku
                for row in pending_rows
                if row.transferred_at.astimezone(UTC) < cutoff
                and (row.quantity - (row.quantity_received or 0)) > 0
            }
        )
        if not stale_skus:
            return 0

        inv_map = await self._inventory_map_for(stale_skus)

        cancelled = 0
        async with AsyncSessionLocal() as session:
            repo = FulfillmentTransferRepository(session)
            for sku in stale_skus:
                rows = sorted(by_sku.get(sku, []), key=lambda t: t.transferred_at)
                net_total = sum(int(r.quantity) - int(r.quantity_received or 0) for r in rows)
                recent_net = sum(
                    int(r.quantity) - int(r.quantity_received or 0)
                    for r in rows
                    if r.transferred_at.astimezone(UTC) >= cutoff
                )

                sources = inv_map.get(sku) or []
                try:
                    ml_transfer = 0
                    for inventory_id, multiplier in sources:
                        stock = await self._ml.get_inventory_stock(inventory_id)
                        ml_transfer += _extract_transfer_qty(stock) * multiplier
                except Exception as exc:
                    logger.warning(
                        "Stale-phantom drain: ML inventory fetch failed, skipping SKU",
                        sku=sku,
                        error=str(exc),
                    )
                    continue

                keep = ml_transfer + recent_net
                if net_total <= keep:
                    continue

                for r in rows:
                    if net_total <= keep:
                        break
                    if r.transferred_at.astimezone(UTC) >= cutoff:
                        continue  # never touch rows inside the grace window
                    net = int(r.quantity) - int(r.quantity_received or 0)
                    if net <= 0:
                        continue
                    await repo.mark_cancelled(
                        int(r.id),
                        reason=(
                            f"stale phantom drain: >{grace_days}d, ML em-transferência="
                            f"{ml_transfer}, not materialised"
                        ),
                    )
                    net_total -= net
                    cancelled += 1
                    logger.info(
                        "Stale phantom transfer cancelled",
                        sku=sku,
                        transfer_id=int(r.id),
                        net=net,
                        transferred_at=r.transferred_at.isoformat(),
                        ml_transfer=ml_transfer,
                    )
            await session.commit()

        if cancelled:
            logger.info("Stale-phantom drain complete", cancelled=cancelled)
        return cancelled

    async def _inventory_map_for(self, skus: list[str]) -> dict[str, list[tuple[str, int]]]:
        """SKU → deduped [(inventory_id, multiplier)] over own FL listings/
        variations + parent-kit listings/variations (multiplier=comp_per_kit).

        Same coverage as ``scan_and_reconcile``'s inline map, expressed as one
        UNION query — used by ``_drain_stale_phantoms`` so the em-transferência
        ceiling is never understated (which would over-cancel).
        """
        from sqlalchemy import text

        sql = text(
            """
            SELECT ml.sku AS base_sku, ml.inventory_id AS inv, 1 AS mult
            FROM ml_listings ml
            WHERE ml.sku = ANY(:skus) AND ml.logistic_type = 'fulfillment'
              AND ml.has_variations = false AND ml.inventory_id IS NOT NULL
            UNION ALL
            SELECT ml.sku, v.inventory_id, 1
            FROM ml_listings ml
            JOIN ml_listing_variations v ON v.mlb_id = ml.mlb_id
            WHERE ml.sku = ANY(:skus) AND ml.logistic_type = 'fulfillment'
              AND ml.has_variations = true AND v.inventory_id IS NOT NULL
            UNION ALL
            SELECT ft.product_sku, ml.inventory_id, kc.quantity
            FROM (SELECT DISTINCT product_sku, product_tiny_id
                  FROM fulfillment_transfers WHERE product_sku = ANY(:skus)) ft
            JOIN product_kit_components kc
              ON kc.component_product_tiny_id = ft.product_tiny_id
            JOIN products p_kit ON p_kit.tiny_id = kc.kit_product_tiny_id
            JOIN ml_listings ml ON ml.sku = p_kit.sku
            WHERE ml.logistic_type = 'fulfillment' AND ml.status IN ('active','paused')
              AND ml.has_variations = false AND ml.inventory_id IS NOT NULL
            UNION ALL
            SELECT ft.product_sku, v.inventory_id, kc.quantity
            FROM (SELECT DISTINCT product_sku, product_tiny_id
                  FROM fulfillment_transfers WHERE product_sku = ANY(:skus)) ft
            JOIN product_kit_components kc
              ON kc.component_product_tiny_id = ft.product_tiny_id
            JOIN products p_kit ON p_kit.tiny_id = kc.kit_product_tiny_id
            JOIN ml_listings ml ON ml.sku = p_kit.sku
            JOIN ml_listing_variations v ON v.mlb_id = ml.mlb_id
            WHERE ml.logistic_type = 'fulfillment' AND ml.status IN ('active','paused')
              AND ml.has_variations = true AND v.inventory_id IS NOT NULL
            """
        )
        out: dict[str, set[tuple[str, int]]] = {}
        async with AsyncSessionLocal() as session:
            rows = (await session.execute(sql, {"skus": skus})).all()
        for base_sku, inv, mult in rows:
            if inv:
                out.setdefault(base_sku, set()).add((inv, max(1, int(mult or 1))))
        return {sku: sorted(pairs) for sku, pairs in out.items()}

    async def _fetch_inbound_events(
        self,
        inventory_sources: list[tuple[str, int]],
        oldest_transfer_date: datetime,
    ) -> list[dict[str, Any]]:
        """Return all inbound-positive events (INBOUND_RECEPTION +
        TRANSFER_DELIVERY) across every inventory source, from
        oldest_transfer_date to now. Chunks at <= 59 days because ML caps
        the date range at 60d.

        Each ``(inventory_id, multiplier)`` source applies its multiplier
        to ``detail.available_quantity`` so the FIFO matcher credits the
        right number of base-SKU units when a kit-parent inventory is
        used as the source.
        """
        now_utc = datetime.now(UTC)
        oldest_utc = oldest_transfer_date.astimezone(UTC)

        collected: list[dict[str, Any]] = []
        for inventory_id, multiplier in inventory_sources:
            chunk_start = oldest_utc
            while chunk_start < now_utc:
                chunk_end = min(chunk_start + timedelta(days=59), now_utc)
                chunk_events = await self._fetch_chunk_inbound(
                    inventory_id=inventory_id,
                    date_from=_format_ml_datetime(chunk_start),
                    date_to=_format_ml_datetime(chunk_end),
                )
                if multiplier != 1:
                    for e in chunk_events:
                        scaled = dict(e)
                        detail = dict(scaled.get("detail") or {})
                        raw_qty = _extract_received_qty(e)
                        detail["available_quantity"] = raw_qty * multiplier
                        scaled["detail"] = detail
                        collected.append(scaled)
                else:
                    collected.extend(chunk_events)
                chunk_start = chunk_end + timedelta(milliseconds=1)
        return collected

    async def _fetch_chunk_inbound(
        self,
        inventory_id: str,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        """Page through a single <=59d window and return inbound-positive events."""
        out: list[dict[str, Any]] = []
        offset = 0
        limit = 50
        while True:
            data = await self._ml.list_fulfillment_operations(
                inventory_id=inventory_id,
                date_from=date_from,
                date_to=date_to,
                operation_type=None,  # no type filter → all event types
                limit=limit,
                offset=offset,
            )
            results: list[dict[str, Any]] = data.get("results") or []
            for op in results:
                # Two-step filter: (1) type must be one we consider for
                # inbound credit, (2) the event must actually represent a
                # seller inbound (not a ML-internal warehouse move).
                # TRANSFER_DELIVERY without an inbound_id is dropped here.
                if op.get("type") not in INBOUND_EVENT_TYPES:
                    continue
                if not _is_seller_inbound_event(op):
                    continue
                if _extract_received_qty(op) > 0:
                    out.append(op)

            paging = data.get("paging") or {}
            fetched_so_far = offset + len(results)
            api_total = int(paging.get("total") or 0)
            if fetched_so_far >= api_total or not results:
                break
            offset += limit
        return out


# ---------------------------------------------------------------------------
# Decision shape used by scan_and_reconcile to drive the persistence step.
# Decoupled from FulfillmentTransferORM so the FIFO matcher stays a pure
# function and is straightforward to unit-test.
# ---------------------------------------------------------------------------
@dataclass
class _MatchDecision:
    transfer_id: int
    # NEW units to credit on this run (delta vs already-stored
    # quantity_received). 0 means no new events touched this transfer.
    delta_units: int
    # True when the total credited (already-stored + delta) covers
    # the ordered quantity. Drives the status='received' transition.
    is_full: bool
    # Latest event date that contributed to delta_units. Used for both
    # received_at and last_event_at on the row.
    last_event_at: datetime | None


def _fifo_match_with_chronology(
    transfers: list[FulfillmentTransferORM],
    events: list[dict[str, Any]],
) -> list[_MatchDecision]:
    """Match transfers (oldest first) against events (oldest first) with
    chronology guard and partial-credit support.

    Rules:
    - Each transfer has an *already-credited* amount (``quantity_received``)
      and an outstanding amount (``quantity - quantity_received``). Events
      fill the outstanding amount FIFO.
    - An event is eligible to credit a transfer only when its
      ``date_created`` is at or after the transfer's ``transferred_at``
      (otherwise newer transfers would steal credit from older arrivals).
    - Events are consumed greedily; leftover units in an event flow to the
      next-oldest transfer.

    Returns one decision per input transfer:
    - ``delta_units = 0`` → nothing new this run (no events left, none
      eligible, or all events older than this transfer).
    - ``delta_units > 0, is_full = False`` → partial reception; caller
      bumps ``quantity_received``.
    - ``delta_units > 0, is_full = True`` → final units arrived; caller
      bumps ``quantity_received`` AND sets status='received'.
    """
    transfers_sorted = sorted(transfers, key=lambda t: t.transferred_at)
    events_sorted = sorted(
        events,
        key=lambda e: _extract_received_at(e) or datetime.min.replace(tzinfo=UTC),
    )
    remaining_per_event = [_extract_received_qty(e) for e in events_sorted]

    decisions: list[_MatchDecision] = []
    for t in transfers_sorted:
        already = int(t.quantity_received or 0)
        outstanding = max(0, int(t.quantity) - already)
        delta = 0
        last_event_date: datetime | None = None
        transfer_at = t.transferred_at.astimezone(UTC)
        # Idempotency floor: events at or before the last credited timestamp
        # were already counted in a prior run — skip them. Without this guard
        # every subsequent execution re-credits the same events, inflating
        # quantity_received until it caps at quantity. (2026-06-05 audit
        # showed this drove ~108 phantom units on the 6 pending transfers
        # alone, plus undetermined inflation on the 156 historical
        # 'received' rows.)
        last_seen = t.last_event_at.astimezone(UTC) if t.last_event_at else None
        for i, e in enumerate(events_sorted):
            if outstanding <= 0:
                break
            if remaining_per_event[i] <= 0:
                continue
            evt_date = _extract_received_at(e)
            if evt_date is None:
                continue
            if evt_date < transfer_at:
                continue  # chronology guard: older events can't be ours
            if last_seen is not None and evt_date <= last_seen:
                continue  # idempotency: already credited in a prior run
            take = min(outstanding, remaining_per_event[i])
            remaining_per_event[i] -= take
            outstanding -= take
            delta += take
            if take > 0:
                last_event_date = evt_date
        is_full = (already + delta) >= int(t.quantity)
        decisions.append(
            _MatchDecision(
                transfer_id=int(t.id),
                delta_units=delta,
                is_full=is_full,
                last_event_at=last_event_date,
            )
        )
    return decisions


# ---------------------------------------------------------------------------
# Helpers — event-shape extraction
# ---------------------------------------------------------------------------
def _format_ml_datetime(dt: datetime) -> str:
    """Format a UTC datetime for the ML operations search API.

    ML accepts ISO-8601 with millisecond precision and trailing Z, e.g.
    ``2026-05-01T00:00:00.000Z``.
    """
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _extract_received_qty(op: dict[str, Any]) -> int:
    """Extract units received in a single fulfillment operation event.

    Verified against production ML API (2026-05): canonical field is
    ``detail.available_quantity`` — the units processed in this event.
    Positive for inbound types (INBOUND_RECEPTION, TRANSFER_DELIVERY);
    negative for sales/reservations. Returns 0 when the field is absent.

    Fallbacks (``quantities.received``, ``quantity``) preserve forward
    compatibility if ML adds new shapes.
    """
    detail = op.get("detail") or {}
    if isinstance(detail, dict) and "available_quantity" in detail:
        try:
            return int(detail["available_quantity"])
        except (TypeError, ValueError):
            return 0

    quantities = op.get("quantities") or {}
    if isinstance(quantities, dict) and "received" in quantities:
        try:
            return int(quantities["received"])
        except (TypeError, ValueError):
            return 0

    for key in ("quantity", "units", "total_units"):
        val = op.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                pass

    return 0


def _extract_transfer_qty(stock: dict[str, Any]) -> int:
    """Units in the ``transfer`` (em transferência) bucket of a
    ``GET /inventories/{id}/stock/fulfillment`` response.

    Shape (verified 2026-06-19):
      ``{"available_quantity": A, "not_available_quantity": N,
         "not_available_detail": [{"status": "transfer", "quantity": Q}, ...]}``
    Only ``status == 'transfer'`` is em-transferência; ``lost``/``damaged``/
    etc. are not in-transit. ML never reports "Entrada pendente" here.
    """
    detail = stock.get("not_available_detail") or []
    if not isinstance(detail, list):
        return 0
    total = 0
    for d in detail:
        if isinstance(d, dict) and d.get("status") == "transfer":
            try:
                total += int(d.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
    return total


def _extract_received_at(op: dict[str, Any]) -> datetime | None:
    """Parse ``date_created`` (ISO-8601, e.g. ``2026-05-14T12:15:00Z``) into a
    timezone-aware UTC datetime. Returns ``None`` when the field is missing
    or unparseable.
    """
    raw = op.get("date_created")
    if not raw or not isinstance(raw, str):
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).astimezone(UTC)
    except (ValueError, TypeError):
        return None
