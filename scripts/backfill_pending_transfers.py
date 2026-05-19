"""One-off backfill for fulfillment_transfers from historical Tiny saídas.

Use when operators did Galpão → Full ML transfers in the Tiny UI before the
webhook delta path was deployed (so we don't have the rows automatically).
The script:

1. Reads the hardcoded BACKFILL list (or `--input <csv>`).
2. For each unique SKU, gathers ALL fulfillment ``inventory_id`` values from
   ``ml_listings`` AND ``ml_listing_variations`` — the existing reception
   service picks only one inventory per SKU, which misses multi-MLB cases.
3. Queries ML ``INBOUND_RECEPTION`` operations per inventory_id from the
   earliest transfer date to now, summing units received across ALL
   inventories belonging to the SKU.
4. FIFO-matches each transfer (oldest first) against the cumulative received
   total: each transfer is marked ``received`` if the running sum covers
   its quantity, else ``pending``.
5. Prints a table per SKU showing the decision. With ``--apply`` it inserts
   the rows; without, it's a dry-run.

Run on the VPS so the ML credentials in /opt/tiny-mirror/.env are loaded:

    cd /opt/tiny-mirror/current && \\
      .venv/bin/python scripts/backfill_pending_transfers.py            # dry-run
    cd /opt/tiny-mirror/current && \\
      .venv/bin/python scripts/backfill_pending_transfers.py --apply    # commits
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import select

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
from tiny_mirror.infrastructure.orm.models import (
    MLListingORM,
    MLListingVariationORM,
    ProductORM,
)
from tiny_mirror.infrastructure.repositories.fulfillment_transfer_repository import (
    FulfillmentTransferRepository,
)
from tiny_mirror.redis_client import close_redis, get_redis, initialize_redis
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService

# ---------------------------------------------------------------------------
# INPUT — list of historical transfers to backfill.
# Each tuple: (sku, quantity, transferred_at_utc_iso, cost_per_unit)
# Datetimes are UTC (BRT + 3h).
# ---------------------------------------------------------------------------
BACKFILL: list[tuple[str, int, str, str]] = [
    # DEL-ORGN-OBJ-CLL-PRET — cost 31,26
    ("DEL-ORGN-OBJ-CLL-PRET", 6, "2026-05-18T16:12:00+00:00", "31.26"),
    ("DEL-ORGN-OBJ-CLL-PRET", 4, "2026-05-15T17:26:00+00:00", "31.26"),
    ("DEL-ORGN-OBJ-CLL-PRET", 13, "2026-05-13T12:05:00+00:00", "31.26"),
    ("DEL-ORGN-OBJ-CLL-PRET", 4, "2026-05-11T19:02:00+00:00", "31.26"),
    # POL-PSTABA-OFCSFT-CRST — cost 6,00
    ("POL-PSTABA-OFCSFT-CRST", 35, "2026-05-18T16:09:00+00:00", "6.00"),
    ("POL-PSTABA-OFCSFT-CRST", 80, "2026-05-13T12:13:00+00:00", "6.00"),
    ("POL-PSTABA-OFCSFT-CRST", 20, "2026-05-13T12:03:00+00:00", "6.00"),
    ("POL-PSTABA-OFCSFT-CRST", 30, "2026-05-07T13:21:00+00:00", "6.00"),
    ("POL-PSTABA-OFCSFT-CRST", 30, "2026-05-07T13:20:00+00:00", "6.00"),
    # MAST-ALMF-CARMB-PRET — cost 9,16
    ("MAST-ALMF-CARMB-PRET", 5, "2026-05-18T16:26:00+00:00", "9.16"),
    ("MAST-ALMF-CARMB-PRET", 20, "2026-05-15T17:28:00+00:00", "9.16"),
    ("MAST-ALMF-CARMB-PRET", 5, "2026-05-13T12:12:00+00:00", "9.16"),
    ("MAST-ALMF-CARMB-PRET", 25, "2026-05-07T14:56:00+00:00", "9.16"),
    # MAST-PAP-DIPLO-180-A450 — cost 15,47
    ("MAST-PAP-DIPLO-180-A450", 4, "2026-05-18T16:26:00+00:00", "15.47"),
    ("MAST-PAP-DIPLO-180-A450", 25, "2026-05-15T17:27:00+00:00", "15.47"),
    ("MAST-PAP-DIPLO-180-A450", 10, "2026-05-07T15:03:00+00:00", "15.47"),
    # MAST-PAP-GLOS-230-A450 — cost 21,79
    ("MAST-PAP-GLOS-230-A450", 10, "2026-05-18T16:27:00+00:00", "21.79"),
    ("MAST-PAP-GLOS-230-A450", 25, "2026-05-15T17:28:00+00:00", "21.79"),
    ("MAST-PAP-GLOS-230-A450", 8, "2026-05-13T12:02:00+00:00", "21.79"),
    # NIT-CSV-ALM-4DV-PRM-N — cost 12,32
    ("NIT-CSV-ALM-4DV-PRM-N", 10, "2026-05-11T12:34:00+00:00", "12.32"),
    ("NIT-CSV-ALM-4DV-PRM-N", 10, "2026-05-05T12:33:00+00:00", "12.32"),
    # OUR-BOLI-S30-4W-2700K — cost 14,42
    ("OUR-BOLI-S30-4W-2700K", 6, "2026-05-11T19:03:00+00:00", "14.42"),
    ("OUR-BOLI-S30-4W-2700K", 20, "2026-05-05T12:34:00+00:00", "14.42"),
    # SLF-CNJ-PORCOPO-PR — cost 47,05
    ("SLF-CNJ-PORCOPO-PR", 10, "2026-05-18T16:25:00+00:00", "47.05"),
    ("SLF-CNJ-PORCOPO-PR", 30, "2026-05-15T17:32:00+00:00", "47.05"),
    ("SLF-CNJ-PORCOPO-PR", 5, "2026-05-13T12:09:00+00:00", "47.05"),
    ("SLF-CNJ-PORCOPO-PR", 30, "2026-05-07T13:21:00+00:00", "47.05"),
    # SLF-SUP-PHIGREQ-CR — cost 72,32
    ("SLF-SUP-PHIGREQ-CR", 10, "2026-05-13T11:46:00+00:00", "72.32"),
    ("SLF-SUP-PHIGREQ-CR", 12, "2026-05-11T12:30:00+00:00", "72.32"),
    ("SLF-SUP-PHIGREQ-CR", 5, "2026-05-07T13:11:00+00:00", "72.32"),
    # SOZ-APOI-BLKPIANO — cost 66,00
    ("SOZ-APOI-BLKPIANO", 90, "2026-05-18T16:28:00+00:00", "66.00"),
    # ------------------------------------------------------------------
    # Batch 2 — added 2026-05-19. SLF-SUP-PHIGREQ-CR's 3 dates already in
    # batch 1 are deduped automatically by the row-level idempotency check
    # (only the new 29/04 row gets inserted).
    # ------------------------------------------------------------------
    # DEL-PSTSFUM-31D-A4
    ("DEL-PSTSFUM-31D-A4", 20, "2026-05-18T16:10:00+00:00", "0"),
    ("DEL-PSTSFUM-31D-A4", 24, "2026-05-15T17:29:00+00:00", "0"),
    ("DEL-PSTSFUM-31D-A4", 9, "2026-04-27T12:20:00+00:00", "0"),
    # EMB-ETQ-TERM-60X40
    ("EMB-ETQ-TERM-60X40", 5, "2026-05-11T12:35:00+00:00", "0"),
    ("EMB-ETQ-TERM-60X40", 10, "2026-04-30T19:05:00+00:00", "0"),
    # EVA-AIMP240-70CM-PRT
    ("EVA-AIMP240-70CM-PRT", 25, "2026-05-18T16:15:00+00:00", "0"),
    ("EVA-AIMP240-70CM-PRT", 28, "2026-05-11T18:56:00+00:00", "0"),
    # MAST-PLAST-125-A4100
    ("MAST-PLAST-125-A4100", 3, "2026-05-15T17:32:00+00:00", "0"),
    # MXCR-PRANCH-A4-PRET
    ("MXCR-PRANCH-A4-PRET", 10, "2026-04-30T19:03:00+00:00", "0"),
    # NIT-CST-ORG-RTT-G-CZ
    ("NIT-CST-ORG-RTT-G-CZ", 6, "2026-05-13T12:00:00+00:00", "0"),
    ("NIT-CST-ORG-RTT-G-CZ", 5, "2026-05-11T12:33:00+00:00", "0"),
    ("NIT-CST-ORG-RTT-G-CZ", 3, "2026-04-30T19:00:00+00:00", "0"),
    # POL-PSTRTCLIP-NLP-CRST
    ("POL-PSTRTCLIP-NLP-CRST", 5, "2026-05-11T19:01:00+00:00", "0"),
    ("POL-PSTRTCLIP-NLP-CRST", 2, "2026-04-30T18:57:00+00:00", "0"),
    # PRE-SBT-EBLU-LAVN-5L
    ("PRE-SBT-EBLU-LAVN-5L", 56, "2026-05-05T12:28:00+00:00", "0"),
    ("PRE-SBT-EBLU-LAVN-5L", 32, "2026-04-30T18:52:00+00:00", "0"),
    # SLF-PTALHER-PR
    ("SLF-PTALHER-PR", 14, "2026-05-11T12:36:00+00:00", "0"),
    ("SLF-PTALHER-PR", 7, "2026-05-07T13:13:00+00:00", "0"),
    # SLF-SUP-PHIGREQ-CR — only the 29/04 row is new (other 3 dedup'd)
    ("SLF-SUP-PHIGREQ-CR", 10, "2026-05-13T11:46:00+00:00", "72.32"),
    ("SLF-SUP-PHIGREQ-CR", 12, "2026-05-11T12:30:00+00:00", "72.32"),
    ("SLF-SUP-PHIGREQ-CR", 5, "2026-05-07T13:11:00+00:00", "72.32"),
    ("SLF-SUP-PHIGREQ-CR", 35, "2026-04-29T18:47:00+00:00", "72.32"),
    # SOZ-CAV-PIN75CM
    ("SOZ-CAV-PIN75CM", 20, "2026-05-13T11:47:00+00:00", "0"),
    ("SOZ-CAV-PIN75CM", 40, "2026-05-11T18:55:00+00:00", "0"),
    ("SOZ-CAV-PIN75CM", 10, "2026-05-05T12:29:00+00:00", "0"),
]

BACKFILL_TAG = "[BACKFILL 2026-05-19]"


@dataclass
class Transfer:
    sku: str
    tiny_id: int
    qty: int
    transferred_at: datetime
    cost: Decimal


@dataclass
class Decision:
    transfer: Transfer
    status: str  # "received" or "pending"
    received_at: datetime | None
    inventory_ids_used: list[str]
    total_received_in_window: int


def _format_ml_datetime(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _extract_received_qty(op: dict[str, Any]) -> int:
    detail = op.get("detail") or {}
    if isinstance(detail, dict) and "available_quantity" in detail:
        try:
            return int(detail["available_quantity"] or 0)
        except (ValueError, TypeError):
            return 0
    return 0


def _extract_received_at(op: dict[str, Any]) -> datetime | None:
    raw = op.get("date_created")
    if not raw:
        return None
    try:
        # Normalize "2026-04-23T02:04:25Z" → datetime
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw).astimezone(UTC)
    except (ValueError, TypeError):
        return None


async def _gather_inventory_ids(sku: str) -> list[str]:
    """Return ALL fulfillment inventory_ids for a SKU.

    Unlike the existing reception service which picks one inventory per SKU,
    this enumerates every fulfillment listing AND its variations' inventories.
    """
    async with AsyncSessionLocal() as session:
        listings = (
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

        inv_ids: set[str] = set()
        for mlb_id, inventory_id, has_variations in listings:
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
                inv_ids.update(x for x in var_inv_ids if x)
            elif inventory_id:
                inv_ids.add(inventory_id)
    return sorted(inv_ids)


# Event types ML emits that represent units physically arriving at the FL CD.
# Both have detail.available_quantity > 0 when the event ADDS units to
# available. INBOUND_RECEPTION fires for batch barcode receptions; ML also
# uses TRANSFER_DELIVERY for the unit-by-unit seller-managed transfer flow
# (verified against POL-PSTABA-OFCSFT-CRST 2026-05-19 — 17 such events).
INBOUND_EVENT_TYPES = {"INBOUND_RECEPTION", "TRANSFER_DELIVERY"}


async def _list_reception_events(
    ml: MercadoLivreAPIClient,
    inventory_id: str,
    oldest: datetime,
    http_client: httpx.AsyncClient,
    token_service: MercadoLivreTokenService,
) -> list[dict[str, Any]]:
    """Page through all fulfillment operations for ``inventory_id`` from
    ``oldest`` to now (chunked at <= 59 days). Returns events of the inbound
    types only — i.e., those whose ``detail.available_quantity`` is positive
    and represents stock arriving at the CD.

    The MercadoLivreAPIClient.list_fulfillment_inbound_operations method
    hardcodes ``type=INBOUND_RECEPTION`` and would miss TRANSFER_DELIVERY,
    so we issue direct calls and filter client-side.
    """
    events: list[dict[str, Any]] = []
    now_utc = datetime.now(UTC)
    chunk_start = oldest.astimezone(UTC)

    while chunk_start < now_utc:
        chunk_end = min(chunk_start + timedelta(days=59), now_utc)
        offset = 0
        limit = 50
        while True:
            # Retry on 429/5xx with exponential backoff. The official client's
            # _request method has this baked in, but it hardcodes type=
            # INBOUND_RECEPTION; we need the type-less call so we have to
            # reimplement the retry budget here.
            attempts = 0
            max_attempts = 6
            backoff = 1.0
            while True:
                token = await token_service.get_valid_access_token()
                response = await http_client.get(
                    "https://api.mercadolibre.com/stock/fulfillment/operations/search",
                    headers={"Authorization": f"Bearer {token}"},
                    params={
                        "seller_id": ml._user_id,
                        "inventory_id": inventory_id,
                        "date_from": _format_ml_datetime(chunk_start),
                        "date_to": _format_ml_datetime(chunk_end),
                        "limit": limit,
                        "offset": offset,
                    },
                )
                if response.status_code == 429 or response.status_code >= 500:
                    attempts += 1
                    if attempts >= max_attempts:
                        response.raise_for_status()
                    await asyncio.sleep(min(backoff, 30.0))
                    backoff *= 2
                    continue
                response.raise_for_status()
                break
            data = response.json()
            results = data.get("results") or []
            # Filter to inbound-positive events only.
            for op in results:
                if op.get("type") not in INBOUND_EVENT_TYPES:
                    continue
                qty = _extract_received_qty(op)
                if qty > 0:
                    events.append(op)
            paging = data.get("paging") or {}
            fetched = offset + len(results)
            api_total = int(paging.get("total") or 0)
            if fetched >= api_total or not results:
                break
            offset += limit
        chunk_start = chunk_end + timedelta(milliseconds=1)
    return events


async def _lookup_tiny_ids(skus: set[str]) -> dict[str, int]:
    async with AsyncSessionLocal() as session:
        rows = (
            await session.execute(
                select(ProductORM.sku, ProductORM.tiny_id).where(ProductORM.sku.in_(list(skus)))
            )
        ).all()
        return dict(rows)


async def _lookup_cost_prices(skus: set[str]) -> dict[str, Decimal]:
    """Return ``products.prices.cost_price`` per SKU, used as a fallback when
    the BACKFILL entry omits the cost (passed as ``'0'``). Defaults to 0 if
    the product row lacks ``cost_price``.
    """
    async with AsyncSessionLocal() as session:
        rows = (
            await session.execute(
                select(ProductORM.sku, ProductORM.prices).where(ProductORM.sku.in_(list(skus)))
            )
        ).all()
        out: dict[str, Decimal] = {}
        for sku, prices in rows:
            raw = (prices or {}).get("cost_price") or 0
            try:
                out[sku] = Decimal(str(raw))
            except Exception:
                out[sku] = Decimal("0")
        return out


async def _existing_backfill_keys() -> set[tuple[str, int, datetime]]:
    """Return the set of (sku, quantity, transferred_at) tuples that already
    have a BACKFILL_TAG row, so we can skip them at the row level.

    Row-level idempotency lets a subsequent run add NEW rows for a SKU that
    already had earlier backfill rows — useful when more historical
    transfers surface later for the same product.
    """
    from sqlalchemy import literal

    async with AsyncSessionLocal() as session:
        from tiny_mirror.infrastructure.orm.models import FulfillmentTransferORM

        rows = (
            await session.execute(
                select(
                    FulfillmentTransferORM.product_sku,
                    FulfillmentTransferORM.quantity,
                    FulfillmentTransferORM.transferred_at,
                ).where(FulfillmentTransferORM.notes.like(literal(f"{BACKFILL_TAG}%")))
            )
        ).all()
        return {
            (sku, int(qty), ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC))
            for sku, qty, ts in rows
        }


def _parse_transfers() -> list[Transfer]:
    return [
        Transfer(
            sku=sku,
            tiny_id=0,  # filled later
            qty=qty,
            transferred_at=datetime.fromisoformat(ts).astimezone(UTC),
            cost=Decimal(cost),
        )
        for sku, qty, ts, cost in BACKFILL
    ]


def _classify_transfers(
    transfers: list[Transfer],
    events: list[dict[str, Any]],
) -> list[Decision]:
    """FIFO match transfers (oldest first) against events with chronology.

    An event can only fulfill a transfer if ``event.date_created >=
    transfer.transferred_at`` — units can't be delivered before they leave
    the warehouse. Older transfers get first dibs on their eligible events.

    Algorithm: for each transfer (oldest first), walk the events in
    chronological order, consume remaining qty from any event whose date
    is >= the transfer's date, until the transfer's quantity is covered or
    no more eligible events remain. ``received_at`` is the date of the
    event that finally satisfied the transfer.
    """
    transfers_sorted = sorted(transfers, key=lambda t: t.transferred_at)
    events_sorted = sorted(
        events,
        key=lambda e: _extract_received_at(e) or datetime.min.replace(tzinfo=UTC),
    )

    # Per-event remaining quantity — events get partially consumed across transfers.
    event_qty_remaining = [_extract_received_qty(e) for e in events_sorted]
    total_received_in_window = sum(event_qty_remaining)

    decisions: list[Decision] = []
    for t in transfers_sorted:
        needed = t.qty
        last_event_date: datetime | None = None
        for i, e in enumerate(events_sorted):
            if event_qty_remaining[i] <= 0:
                continue
            evt_date = _extract_received_at(e)
            if evt_date is None or evt_date < t.transferred_at:
                continue
            take = min(needed, event_qty_remaining[i])
            event_qty_remaining[i] -= take
            needed -= take
            if take > 0:
                last_event_date = evt_date
            if needed <= 0:
                break
        status = "received" if needed <= 0 else "pending"
        decisions.append(
            Decision(
                transfer=t,
                status=status,
                received_at=last_event_date if status == "received" else None,
                inventory_ids_used=[],
                total_received_in_window=total_received_in_window,
            )
        )
    return decisions


def _print_table(sku: str, decisions: list[Decision], inventory_ids: list[str]) -> None:
    print(f"\n=== {sku} ===")
    print(f"  inventory_ids checked: {inventory_ids or '(none — SKU has no FL listing)'}")
    if decisions:
        total_recv = decisions[0].total_received_in_window
        total_qty = sum(d.transfer.qty for d in decisions)
        print(
            f"  total INBOUND_RECEPTION qty in window: {total_recv} "
            f"vs total transfer qty: {total_qty}"
        )
    print(f"  {'TRANSFERRED':<22} {'QTY':>5} {'STATUS':<10} {'RECEIVED_AT':<22}")
    for d in decisions:
        recv = d.received_at.strftime("%Y-%m-%d %H:%M UTC") if d.received_at else "—"
        print(
            f"  {d.transfer.transferred_at.strftime('%Y-%m-%d %H:%M UTC'):<22} "
            f"{d.transfer.qty:>5}  {d.status:<10} {recv}"
        )


async def _run(apply: bool) -> None:
    print("Loading settings…")
    if not settings.ml_client_id:
        raise RuntimeError("ML_CLIENT_ID is empty — script must run on the VPS")

    transfers = _parse_transfers()

    # Row-level idempotency: skip only the exact (sku, qty, transferred_at)
    # tuples already inserted by a previous BACKFILL run.
    already = await _existing_backfill_keys()
    pre_count = len(transfers)
    transfers = [t for t in transfers if (t.sku, t.qty, t.transferred_at) not in already]
    skipped = pre_count - len(transfers)
    if skipped:
        print(f"⚠ Skipping {skipped} rows that already exist with the {BACKFILL_TAG} tag.")
    skus = {t.sku for t in transfers}
    if not transfers:
        print("Nothing left to backfill.")
        return

    tiny_ids = await _lookup_tiny_ids(skus)
    missing = skus - set(tiny_ids.keys())
    if missing:
        raise RuntimeError(f"SKUs not found in products: {sorted(missing)}")

    # Fallback costs from products.prices.cost_price for entries that came
    # in with cost == 0 in the BACKFILL list (user didn't have the value).
    fallback_costs = await _lookup_cost_prices(skus)
    for t in transfers:
        t.tiny_id = tiny_ids[t.sku]
        if t.cost <= 0:
            t.cost = fallback_costs.get(t.sku, Decimal("0"))

    await initialize_redis()
    http_client = httpx.AsyncClient(timeout=30.0)
    tokens = MercadoLivreTokenService(
        session_factory=AsyncSessionLocal,
        redis_client=get_redis(),
        http_client=http_client,
        ml_client_id=settings.ml_client_id,
        ml_client_secret=settings.ml_client_secret,
        ml_initial_refresh_token=settings.ml_refresh_token,
    )
    ml = MercadoLivreAPIClient(
        token_service=tokens,
        http_client=http_client,
        ml_user_id=settings.ml_user_id,
    )

    all_decisions: list[tuple[str, list[Decision], list[str]]] = []
    try:
        for sku in sorted(skus):
            sku_transfers = [t for t in transfers if t.sku == sku]
            inventory_ids = await _gather_inventory_ids(sku)

            # Gather events across ALL inventories for the SKU.
            oldest = min(t.transferred_at for t in sku_transfers)
            all_events: list[dict[str, Any]] = []
            for inv_id in inventory_ids:
                evts = await _list_reception_events(
                    ml, inv_id, oldest, http_client=http_client, token_service=tokens
                )
                all_events.extend(evts)

            decisions = _classify_transfers(sku_transfers, all_events)
            for d in decisions:
                d.inventory_ids_used = inventory_ids
            all_decisions.append((sku, decisions, inventory_ids))
            _print_table(sku, decisions, inventory_ids)
    finally:
        await http_client.aclose()
        await close_redis()

    print("\n" + ("=" * 60))
    print("SUMMARY")
    total_pending = sum(1 for _, ds, _ in all_decisions for d in ds if d.status == "pending")
    total_received = sum(1 for _, ds, _ in all_decisions for d in ds if d.status == "received")
    total_rows = total_pending + total_received
    print(f"  rows that would be inserted: {total_rows}")
    print(f"    pending:  {total_pending}")
    print(f"    received: {total_received}")

    if not apply:
        print("\nDry-run only. Re-run with --apply to commit.")
        return

    print("\nApplying…")
    inserted = 0
    async with AsyncSessionLocal() as session:
        repo = FulfillmentTransferRepository(session)
        for _sku, decisions, _inv in all_decisions:
            for d in decisions:
                t = d.transfer
                notes = (
                    f"{BACKFILL_TAG} Tiny saida {t.transferred_at.strftime('%Y-%m-%d %H:%M UTC')}"
                    f" - operator did transfer in Tiny UI before the webhook delta path was deployed."
                )
                row = await repo.create(
                    product_tiny_id=t.tiny_id,
                    product_sku=t.sku,
                    quantity=t.qty,
                    cost_per_unit=t.cost,
                    transferred_at=t.transferred_at,
                    notes=notes,
                    source="manual",
                )
                if d.status == "received":
                    await repo.mark_received(row.id, d.received_at or datetime.now(UTC))
                inserted += 1
        await session.commit()
    print(f"Inserted {inserted} rows.")
    print("Refresh mv_coverage with: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_coverage;")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Insert rows; omit for dry-run.")
    args = parser.parse_args()
    asyncio.run(_run(apply=args.apply))


if __name__ == "__main__":
    main()
