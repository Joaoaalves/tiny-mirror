"""Detects ML INBOUND_RECEPTION events and marks pending fulfillment transfers as received.

Runs on a schedule (every 6h by default). For each SKU with pending transfers:
1. Looks up the inventory_id in ml_listings.
2. Calls GET /stock/fulfillment/operations/search?type=INBOUND_RECEPTION for that inventory.
3. Totals received quantity from all events after the oldest pending transfer date.
4. Marks transfers as received FIFO until the received total is exhausted.

This means:
- 1 transfer of 10 + 1 INBOUND_RECEPTION of 10 → transfer marked received.
- 2 transfers of 5 each + 1 INBOUND_RECEPTION of 10 → both marked received.
- 1 transfer of 10 + INBOUND_RECEPTION of 8 (partial) → stays pending until ≥ 10 arrive.
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


@dataclass
class ReconciliationResult:
    skus_scanned: int = 0
    transfers_received: int = 0
    skus_with_no_inventory: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class FulfillmentReceptionService:
    def __init__(self, ml_client: MercadoLivreAPIClient) -> None:
        self._ml = ml_client

    async def scan_and_reconcile(self) -> ReconciliationResult:
        """Scan all pending transfers and mark as received when ML confirms arrival."""
        from sqlalchemy import select

        from tiny_mirror.infrastructure.orm.models import MLListingORM

        result = ReconciliationResult()

        async with AsyncSessionLocal() as session:
            repo = FulfillmentTransferRepository(session)
            pending_rows, _ = await repo.list_all(status="pending", limit=500)

        if not pending_rows:
            logger.info("No pending fulfillment transfers to reconcile")
            return result

        # Group transfers by SKU
        by_sku: dict[str, list[FulfillmentTransferORM]] = {}
        for row in pending_rows:
            by_sku.setdefault(row.product_sku, []).append(row)

        # Look up inventory_ids for all SKUs in one query
        async with AsyncSessionLocal() as session:
            sku_list = list(by_sku.keys())
            q_result = await session.execute(
                select(MLListingORM.sku, MLListingORM.inventory_id)
                .where(
                    MLListingORM.sku.in_(sku_list),
                    MLListingORM.logistic_type == "fulfillment",
                    MLListingORM.inventory_id.isnot(None),
                )
                .distinct(MLListingORM.sku)
            )
            inventory_map: dict[str, str] = {
                row.sku: row.inventory_id for row in q_result if row.sku and row.inventory_id
            }

        for sku, transfers in by_sku.items():
            result.skus_scanned += 1
            inventory_id = inventory_map.get(sku)
            if not inventory_id:
                logger.warning(
                    "No fulfillment inventory_id found for SKU, skipping",
                    sku=sku,
                )
                result.skus_with_no_inventory.append(sku)
                continue

            # Sort transfers FIFO (oldest first)
            transfers_sorted = sorted(transfers, key=lambda t: t.transferred_at)
            oldest_date = transfers_sorted[0].transferred_at

            try:
                received_qty = await self._fetch_total_received(
                    inventory_id=inventory_id,
                    oldest_transfer_date=oldest_date,
                )
            except Exception as exc:
                logger.error(
                    "Failed to fetch INBOUND_RECEPTION for SKU",
                    sku=sku,
                    inventory_id=inventory_id,
                    error=str(exc),
                )
                result.errors.append(f"{sku}: {exc}")
                continue

            if received_qty <= 0:
                logger.debug(
                    "No INBOUND_RECEPTION events found for SKU",
                    sku=sku,
                    inventory_id=inventory_id,
                )
                continue

            logger.info(
                "INBOUND_RECEPTION total for SKU",
                sku=sku,
                inventory_id=inventory_id,
                received_qty=received_qty,
                pending_transfers=len(transfers_sorted),
            )

            # FIFO matching: consume received_qty across transfers oldest-first
            remaining = received_qty
            ids_to_mark: list[int] = []
            for transfer in transfers_sorted:
                if remaining >= transfer.quantity:
                    ids_to_mark.append(transfer.id)
                    remaining -= transfer.quantity
                else:
                    break  # Not enough received to cover this transfer yet

            if not ids_to_mark:
                logger.debug(
                    "Received quantity insufficient to cover oldest pending transfer",
                    sku=sku,
                    received_qty=received_qty,
                    oldest_transfer_qty=transfers_sorted[0].quantity,
                )
                continue

            # Persist received status
            now = datetime.now(UTC)
            async with AsyncSessionLocal() as session:
                repo = FulfillmentTransferRepository(session)
                for transfer_id in ids_to_mark:
                    await repo.mark_received(transfer_id, now)
                await session.commit()

            result.transfers_received += len(ids_to_mark)
            logger.info(
                "Marked fulfillment transfers as received",
                sku=sku,
                transfer_ids=ids_to_mark,
                count=len(ids_to_mark),
            )

        logger.info(
            "Fulfillment reception reconciliation complete",
            skus_scanned=result.skus_scanned,
            transfers_received=result.transfers_received,
            skus_missing_inventory=len(result.skus_with_no_inventory),
            errors=len(result.errors),
        )
        return result

    async def _fetch_total_received(self, inventory_id: str, oldest_transfer_date: datetime) -> int:
        """Sum all INBOUND_RECEPTION received quantities for an inventory.

        Walks the time window from ``oldest_transfer_date`` to ``now`` in
        chunks of <= 60 days (ML's max date range per request). For each
        chunk, sums ``detail.available_quantity`` across all returned
        events.
        """
        total = 0
        now_utc = datetime.now(UTC)
        oldest_utc = oldest_transfer_date.astimezone(UTC)

        chunk_start = oldest_utc
        while chunk_start < now_utc:
            chunk_end = min(chunk_start + timedelta(days=59), now_utc)
            total += await self._sum_chunk(
                inventory_id=inventory_id,
                date_from=_format_ml_datetime(chunk_start),
                date_to=_format_ml_datetime(chunk_end),
            )
            chunk_start = chunk_end + timedelta(milliseconds=1)

        return total

    async def _sum_chunk(self, inventory_id: str, date_from: str, date_to: str) -> int:
        """Page through a single <=60d window and sum detail.available_quantity."""
        chunk_total = 0
        offset = 0
        limit = 50

        while True:
            data = await self._ml.list_fulfillment_inbound_operations(
                inventory_id=inventory_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
            )
            results: list[dict[str, Any]] = data.get("results") or []
            for op in results:
                qty = _extract_received_qty(op)
                if qty > 0:
                    chunk_total += qty

            paging = data.get("paging") or {}
            fetched_so_far = offset + len(results)
            api_total = int(paging.get("total") or 0)
            if fetched_so_far >= api_total or not results:
                break
            offset += limit

        return chunk_total


# ---------------------------------------------------------------------------
# Helpers — extraction of ML INBOUND_RECEPTION fields
# ---------------------------------------------------------------------------
def _format_ml_datetime(dt: datetime) -> str:
    """Format a UTC datetime for the ML operations search API.

    ML accepts ISO-8601 with millisecond precision and trailing Z, e.g.
    ``2026-05-01T00:00:00.000Z``.
    """
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _extract_received_qty(op: dict[str, Any]) -> int:
    """Extract units received in a single INBOUND_RECEPTION operation event.

    Verified against production ML API (2026-05): the canonical field is
    ``detail.available_quantity`` — the units processed and made available
    in this specific event. A single physical inbound may be split across
    multiple events, so summing this field across events in a date range
    gives the total units actually received.

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
