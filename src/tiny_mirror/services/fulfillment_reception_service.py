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
# Per ML's official Fulfillment Operations docs (verified 2026-06-05):
#  - ``INBOUND_RECEPTION``: "Novo estoque: o processo inbound disponibiliza
#    unidades para venda no fluxo de acesso." This is the seller-side inbound
#    flow we want to track.
#  - ``TRANSFER_DELIVERY``: "Esta é a ação INTERNA do estoque do Mercado Livre,
#    NÃO do vendedor. Unidades entram em transferência (warehouse-to-warehouse
#    internal moves)." Including it credits internal ML moves as if they were
#    our shipment arriving — that bled ~700 phantom units into our log in 30d.
#
# An earlier comment claimed TRANSFER_DELIVERY was a "newer seller-managed
# transfer flow" (per a POL-PSTABA observation in 2026-05). The 2026-06-05
# audit against ML's official docs disproved that — those POL-PSTABA events
# were ML's internal warehouse moves and shouldn't have credited the seller
# transfer either.
INBOUND_EVENT_TYPES = frozenset({"INBOUND_RECEPTION"})


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

        # Build a SKU -> [inventory_id] map covering BOTH main listings and
        # their variations. A SKU split across multiple MLBs and/or
        # variations maps to multiple inventories.
        sku_list = list(by_sku.keys())
        inventory_map: dict[str, list[str]] = {}
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
                    inventory_map.setdefault(sku, []).append(inventory_id)

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
                        inventory_map.setdefault(sku, []).append(inventory_id)

        # Deduplicate inventory_ids per SKU (two listings can share an inventory).
        for sku in list(inventory_map.keys()):
            inventory_map[sku] = sorted(set(inventory_map[sku]))

        for sku, transfers in by_sku.items():
            result.skus_scanned += 1
            inventory_ids = inventory_map.get(sku) or []
            if not inventory_ids:
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
                    inventory_ids=inventory_ids,
                    oldest_transfer_date=oldest_date,
                )
            except Exception as exc:
                logger.error(
                    "Failed to fetch fulfillment operations for SKU",
                    sku=sku,
                    inventory_ids=inventory_ids,
                    error=str(exc),
                )
                result.errors.append(f"{sku}: {exc}")
                continue

            if not events:
                logger.debug(
                    "No INBOUND_RECEPTION/TRANSFER_DELIVERY events found for SKU",
                    sku=sku,
                    inventory_ids=inventory_ids,
                )
                continue

            decisions = _fifo_match_with_chronology(transfers_sorted, events)
            actionable = [d for d in decisions if d.delta_units > 0]

            if not actionable:
                logger.debug(
                    "Events present but none eligible (date filter) or "
                    "no new units to credit existing pendings",
                    sku=sku,
                    inventory_ids=inventory_ids,
                    events_count=len(events),
                )
                continue

            full_decisions = [d for d in actionable if d.is_full]
            partial_decisions = [d for d in actionable if not d.is_full]

            logger.info(
                "Reception events found for SKU",
                sku=sku,
                inventory_ids=inventory_ids,
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

        A SKU is treated as non-fulfillment when none of its ``ml_listings``
        rows has ``logistic_type='fulfillment'`` (anymore). In that case ML
        will never emit an INBOUND_RECEPTION / TRANSFER_DELIVERY for the
        SKU, so leaving the transfer in 'pending' permanently distorts
        coverage math. Cancellation is a metadata-only operation — Tiny
        stock is untouched.

        SKUs with **zero** ml_listings rows (e.g. kit components) are
        deliberately *not* cancelled here: we don't know whether they're
        legitimately fulfilled via a parent kit's MLB; safer to leave them
        pending for operator review.
        """
        from sqlalchemy import text

        async with AsyncSessionLocal() as session:
            non_fl_rows = await session.execute(
                text(
                    """
                    SELECT ft.id, ft.product_sku
                    FROM fulfillment_transfers ft
                    WHERE ft.status = 'pending'
                      AND EXISTS (
                          SELECT 1 FROM ml_listings ml WHERE ml.sku = ft.product_sku
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM ml_listings ml
                          WHERE ml.sku = ft.product_sku
                            AND ml.logistic_type = 'fulfillment'
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

    async def _fetch_inbound_events(
        self,
        inventory_ids: list[str],
        oldest_transfer_date: datetime,
    ) -> list[dict[str, Any]]:
        """Return all inbound-positive events (INBOUND_RECEPTION +
        TRANSFER_DELIVERY) across every inventory, from oldest_transfer_date
        to now. Chunks at <= 59 days because ML caps the date range at 60d.
        """
        now_utc = datetime.now(UTC)
        oldest_utc = oldest_transfer_date.astimezone(UTC)

        collected: list[dict[str, Any]] = []
        for inventory_id in inventory_ids:
            chunk_start = oldest_utc
            while chunk_start < now_utc:
                chunk_end = min(chunk_start + timedelta(days=59), now_utc)
                collected.extend(
                    await self._fetch_chunk_inbound(
                        inventory_id=inventory_id,
                        date_from=_format_ml_datetime(chunk_start),
                        date_to=_format_ml_datetime(chunk_end),
                    )
                )
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
                if op.get("type") not in INBOUND_EVENT_TYPES:
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
