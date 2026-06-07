"""Unit tests for FulfillmentReceptionService.

Covers:
- The pure ``_fifo_match_with_chronology`` matcher (FIFO + date guard).
- Helper extractors (``_extract_received_qty``, ``_extract_received_at``,
  ``_format_ml_datetime``).
- The full ``scan_and_reconcile`` flow, mocking AsyncSessionLocal +
  FulfillmentTransferRepository + ML client so we exercise the multi-
  inventory enumeration and the INBOUND_RECEPTION + TRANSFER_DELIVERY
  filtering without a live DB or ML round-trip.

Mock payloads use the real ML operations-search response shape captured
from production (2026-05).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.fulfillment_reception_service import (
    FulfillmentReceptionService,
    _extract_received_at,
    _extract_received_qty,
    _fifo_match_with_chronology,
    _format_ml_datetime,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Real-shape sample payloads
# ---------------------------------------------------------------------------
def _event(
    qty: int,
    *,
    event_type: str = "INBOUND_RECEPTION",
    date_iso: str = "2026-05-10T14:30:00Z",
    inbound_id: str | None = "68000000",
) -> dict:
    """Build a representative ML operation payload for tests.

    ``inbound_id``: default value mimics the real-world TRANSFER_DELIVERY
    shape from the 2026-06-07 audit (every TRANSFER_DELIVERY in production
    carries inbound_id). Pass ``None`` to simulate an event without the
    external_references link — the discriminator must reject it.
    """
    refs: list[dict] = []
    if inbound_id is not None:
        refs.append({"type": "inbound_id", "value": inbound_id})
    return {
        "id": 1000000000000000000 + qty,
        "type": event_type,
        "date_created": date_iso,
        "detail": {"available_quantity": qty, "not_available_detail": []},
        "result": {"total": qty, "available_quantity": qty},
        "external_references": refs,
    }


_EMPTY_PAYLOAD = {"paging": {"total": 0}, "results": []}


# ---------------------------------------------------------------------------
# Helpers — pure extraction tests
# ---------------------------------------------------------------------------
class TestExtractReceivedQty:
    def test_inbound_reception_shape(self) -> None:
        assert _extract_received_qty(_event(10)) == 10

    def test_transfer_delivery_shape(self) -> None:
        assert _extract_received_qty(_event(1, event_type="TRANSFER_DELIVERY")) == 1

    def test_fallback_quantities_received(self) -> None:
        assert _extract_received_qty({"quantities": {"received": 7}}) == 7

    def test_fallback_quantity_flat(self) -> None:
        assert _extract_received_qty({"quantity": 5}) == 5

    def test_missing_fields_returns_zero(self) -> None:
        assert _extract_received_qty({"type": "INBOUND_RECEPTION"}) == 0

    def test_malformed_value_returns_zero(self) -> None:
        assert _extract_received_qty({"detail": {"available_quantity": "not_a_number"}}) == 0

    def test_zero_available_quantity(self) -> None:
        assert _extract_received_qty({"detail": {"available_quantity": 0}}) == 0

    def test_negative_quantity_is_preserved(self) -> None:
        # Reservations/sales are negative; the caller is responsible for
        # filtering. The extractor must not convert them to 0.
        assert _extract_received_qty(_event(-3)) == -3


class TestExtractReceivedAt:
    def test_zulu_form(self) -> None:
        out = _extract_received_at(_event(1, date_iso="2026-05-14T12:30:45Z"))
        assert out == datetime(2026, 5, 14, 12, 30, 45, tzinfo=UTC)

    def test_with_offset(self) -> None:
        out = _extract_received_at({"date_created": "2026-05-14T09:30:45-03:00"})
        assert out == datetime(2026, 5, 14, 12, 30, 45, tzinfo=UTC)

    def test_missing_returns_none(self) -> None:
        assert _extract_received_at({}) is None

    def test_malformed_returns_none(self) -> None:
        assert _extract_received_at({"date_created": "not-a-date"}) is None


class TestFormatMlDatetime:
    def test_utc_aware(self) -> None:
        dt = datetime(2026, 5, 14, 12, 30, 45, tzinfo=UTC)
        assert _format_ml_datetime(dt) == "2026-05-14T12:30:45.000Z"

    def test_converts_other_timezone(self) -> None:
        from datetime import timezone

        dt = datetime(2026, 5, 14, 9, 30, 45, tzinfo=timezone(timedelta(hours=-3)))
        assert _format_ml_datetime(dt) == "2026-05-14T12:30:45.000Z"


# ---------------------------------------------------------------------------
# FIFO + chronology matcher (pure function)
# ---------------------------------------------------------------------------
def _transfer(
    transfer_id: int,
    qty: int,
    days_ago: int,
    *,
    quantity_received: int = 0,
    last_event_at: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.id = transfer_id
    row.quantity = qty
    row.quantity_received = quantity_received
    row.transferred_at = datetime.now(UTC) - timedelta(days=days_ago)
    row.last_event_at = last_event_at
    return row


class TestFifoMatchWithChronology:
    def test_single_transfer_single_event_exact(self) -> None:
        t = _transfer(1, 10, days_ago=2)
        evt = _event(10, date_iso=(t.transferred_at + timedelta(hours=1)).isoformat())
        decisions = _fifo_match_with_chronology([t], [evt])
        assert decisions[0].transfer_id == 1
        assert decisions[0].delta_units == 10
        assert decisions[0].is_full is True
        assert decisions[0].last_event_at is not None

    def test_event_before_transfer_does_not_credit(self) -> None:
        """The chronology guard: an event whose date_created is BEFORE the
        transfer's transferred_at can never fulfill it.
        """
        t = _transfer(1, 10, days_ago=2)
        evt = _event(10, date_iso=(datetime.now(UTC) - timedelta(days=5)).isoformat())
        decisions = _fifo_match_with_chronology([t], [evt])
        assert decisions[0].delta_units == 0
        assert decisions[0].is_full is False

    def test_partial_event_qty_carries_over_to_next_transfer(self) -> None:
        """An event partially consumed by the older transfer leaves remainder
        for the next FIFO transfer."""
        t1 = _transfer(1, 6, days_ago=5)
        t2 = _transfer(2, 4, days_ago=4)
        evt = _event(10, date_iso=(datetime.now(UTC) - timedelta(days=3)).isoformat())
        decisions = _fifo_match_with_chronology([t1, t2], [evt])
        assert all(d.is_full for d in decisions)
        assert decisions[0].delta_units == 6
        assert decisions[1].delta_units == 4

    def test_partial_reception_keeps_pending(self) -> None:
        """Transfer of 11 + 2 events of 1 each → partial: delta=2, not full."""
        t = _transfer(1, 11, days_ago=5)
        evts = [
            _event(
                1,
                event_type="TRANSFER_DELIVERY",
                date_iso=(datetime.now(UTC) - timedelta(days=2)).isoformat(),
            ),
            _event(
                1,
                event_type="TRANSFER_DELIVERY",
                date_iso=(datetime.now(UTC) - timedelta(days=1)).isoformat(),
            ),
        ]
        decisions = _fifo_match_with_chronology([t], evts)
        assert decisions[0].delta_units == 2
        assert decisions[0].is_full is False
        assert decisions[0].last_event_at is not None

    def test_partial_credit_resumes_from_prior_quantity_received(self) -> None:
        """A second scan picks up where the first left off — already-credited
        units don't double-count, only new events add to delta_units."""
        # Transfer of 11, already 6 credited from a prior scan.
        t = _transfer(1, 11, days_ago=5, quantity_received=6)
        # New event delivers the remaining 5.
        evt = _event(
            5,
            event_type="TRANSFER_DELIVERY",
            date_iso=(datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        )
        decisions = _fifo_match_with_chronology([t], [evt])
        assert decisions[0].delta_units == 5
        assert decisions[0].is_full is True

    def test_transfer_delivery_events_counted(self) -> None:
        t = _transfer(1, 3, days_ago=2)
        evts = [
            _event(
                1,
                event_type="TRANSFER_DELIVERY",
                date_iso=(datetime.now(UTC) - timedelta(days=1, hours=h)).isoformat(),
            )
            for h in (1, 2, 3)
        ]
        decisions = _fifo_match_with_chronology([t], evts)
        assert decisions[0].is_full is True

    def test_three_transfers_FIFO_oldest_first(self) -> None:
        """3 transfers of 5 each + 1 event of 11 → 2 oldest received, newest
        pending. The newest still gets a partial credit of 1."""
        ts = [
            _transfer(99, 5, days_ago=1),  # newest
            _transfer(11, 5, days_ago=5),  # oldest
            _transfer(55, 5, days_ago=3),  # middle
        ]
        evt = _event(11, date_iso=(datetime.now(UTC)).isoformat())
        decisions = _fifo_match_with_chronology(ts, [evt])
        by_id = {d.transfer_id: d for d in decisions}
        assert by_id[11].is_full is True
        assert by_id[55].is_full is True
        assert by_id[99].is_full is False
        assert by_id[99].delta_units == 1  # leftover unit credited

    def test_last_event_at_is_last_event_consumed(self) -> None:
        t = _transfer(1, 8, days_ago=5)
        evt1 = _event(3, date_iso=(datetime.now(UTC) - timedelta(days=4)).isoformat())
        evt2_date = (datetime.now(UTC) - timedelta(days=2)).isoformat()
        evt2 = _event(5, date_iso=evt2_date)
        decisions = _fifo_match_with_chronology([t], [evt1, evt2])
        assert decisions[0].is_full is True
        assert decisions[0].last_event_at == _extract_received_at(evt2)

    # Bug 3 fix (2026-06-05): events at or before t.last_event_at must NOT
    # be re-credited. Without this guard, every subsequent run inflates
    # quantity_received by the same 3 units (proven on BUB-ASPR-NAS-ESTJ
    # transfer #279: 3 real events → received=10/13 over 3-4 runs).
    def test_last_event_at_idempotency_skips_already_seen_events(self) -> None:
        last_run = datetime.now(UTC) - timedelta(hours=6)
        # Transfer credited up to last_run on a prior scan.
        t = _transfer(
            1,
            10,
            days_ago=5,
            quantity_received=3,
            last_event_at=last_run,
        )
        # Re-supplying the SAME event that was already credited on the
        # prior run (date BEFORE last_run): must NOT be credited again.
        already_seen = _event(
            3,
            date_iso=(last_run - timedelta(hours=1)).isoformat(),
        )
        decisions = _fifo_match_with_chronology([t], [already_seen])
        assert decisions[0].delta_units == 0
        assert decisions[0].is_full is False

    def test_last_event_at_admits_strictly_newer_events(self) -> None:
        last_run = datetime.now(UTC) - timedelta(hours=6)
        t = _transfer(
            1,
            10,
            days_ago=5,
            quantity_received=3,
            last_event_at=last_run,
        )
        # A genuinely new event strictly AFTER last_run: must be credited.
        new_event = _event(
            2,
            date_iso=(last_run + timedelta(hours=2)).isoformat(),
        )
        decisions = _fifo_match_with_chronology([t], [new_event])
        assert decisions[0].delta_units == 2
        assert decisions[0].is_full is False

    def test_last_event_at_at_boundary_is_not_credited(self) -> None:
        """Event with date_created EXACTLY at last_event_at is treated as the
        already-credited one — skipped. Avoids replay at second precision."""
        last_run = datetime.now(UTC) - timedelta(hours=6)
        t = _transfer(
            1,
            10,
            days_ago=5,
            quantity_received=3,
            last_event_at=last_run,
        )
        same_instant = _event(2, date_iso=last_run.isoformat())
        decisions = _fifo_match_with_chronology([t], [same_instant])
        assert decisions[0].delta_units == 0


# ---------------------------------------------------------------------------
# Seller-inbound discriminator (2026-06-07 audit on RON-COLL-PRE + 49 other
# inventories, n=2019 TRANSFER_DELIVERY events): the 2026-06-05 restriction
# to INBOUND_RECEPTION only credited 0 receptions despite ML clearly
# receiving stock — the empirical channel ML uses for unit-by-unit seller
# inbound reception is TRANSFER_DELIVERY with external_references.type ==
# "inbound_id". Pure internal warehouse moves (the doc's TRANSFER_DELIVERY)
# use TRANSFER_RESERVATION (negative qty, empty refs) instead.
#
# These tests pin the discriminator so a doc-driven over-correction
# (like 2026-06-05's) can't silently break it.
# ---------------------------------------------------------------------------
class TestInboundEventTypesContract:
    def test_inbound_reception_is_eligible(self) -> None:
        from tiny_mirror.services.fulfillment_reception_service import (
            INBOUND_EVENT_TYPES,
        )

        assert "INBOUND_RECEPTION" in INBOUND_EVENT_TYPES

    def test_transfer_delivery_is_eligible_for_discrimination(self) -> None:
        """TRANSFER_DELIVERY must reach the discriminator (see
        _is_seller_inbound_event); the qualifier is the inbound_id ref,
        not the type itself."""
        from tiny_mirror.services.fulfillment_reception_service import (
            INBOUND_EVENT_TYPES,
        )

        assert "TRANSFER_DELIVERY" in INBOUND_EVENT_TYPES


class TestIsSellerInboundEvent:
    """Pins the empirical discriminator from the 2026-06-07 audit."""

    def test_inbound_reception_always_counts(self) -> None:
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        assert _is_seller_inbound_event({"type": "INBOUND_RECEPTION", "external_references": []})

    def test_transfer_delivery_with_inbound_id_counts(self) -> None:
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        op = {
            "type": "TRANSFER_DELIVERY",
            "external_references": [{"type": "inbound_id", "value": "68422036"}],
        }
        assert _is_seller_inbound_event(op)

    def test_transfer_delivery_without_inbound_id_rejected(self) -> None:
        """No inbound_id → ML-internal move (hypothetical for our seller,
        never observed in 2019/2019 sample, but guard exists)."""
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        op = {"type": "TRANSFER_DELIVERY", "external_references": []}
        assert not _is_seller_inbound_event(op)

    def test_transfer_delivery_with_unrelated_ref_rejected(self) -> None:
        """Only ``type=inbound_id`` counts. Other ref types don't make
        the event a seller inbound."""
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        op = {
            "type": "TRANSFER_DELIVERY",
            "external_references": [{"type": "shipment_id", "value": "999"}],
        }
        assert not _is_seller_inbound_event(op)

    def test_transfer_reservation_rejected(self) -> None:
        """TRANSFER_RESERVATION is ML's internal-move signal. Even though
        external_references is empty, it's still rejected because the type
        itself isn't in our eligible set."""
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        op = {"type": "TRANSFER_RESERVATION", "external_references": []}
        assert not _is_seller_inbound_event(op)

    def test_sale_confirmation_rejected(self) -> None:
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        assert not _is_seller_inbound_event(
            {"type": "SALE_CONFIRMATION", "external_references": []}
        )

    def test_missing_external_references_rejected(self) -> None:
        """Defensive: TRANSFER_DELIVERY payload missing the key entirely
        is treated as 'no inbound_id' → rejected."""
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        assert not _is_seller_inbound_event({"type": "TRANSFER_DELIVERY"})

    def test_malformed_ref_entry_does_not_crash(self) -> None:
        """A non-dict element in external_references (defensive) must not
        crash the discriminator — it just doesn't match."""
        from tiny_mirror.services.fulfillment_reception_service import (
            _is_seller_inbound_event,
        )

        op = {
            "type": "TRANSFER_DELIVERY",
            "external_references": ["not a dict", None, {"type": "inbound_id", "value": "1"}],
        }
        assert _is_seller_inbound_event(op)


# ---------------------------------------------------------------------------
# Service tests — scan_and_reconcile with mocks
# ---------------------------------------------------------------------------
def _make_transfer_orm(
    transfer_id: int,
    sku: str,
    quantity: int,
    days_ago: int = 1,
    product_tiny_id: int = 971992238,
    quantity_received: int = 0,
    last_event_at: datetime | None = None,
) -> MagicMock:
    row = MagicMock()
    row.id = transfer_id
    row.product_sku = sku
    row.quantity = quantity
    row.quantity_received = quantity_received
    row.product_tiny_id = product_tiny_id
    row.transferred_at = datetime.now(UTC) - timedelta(days=days_ago)
    row.last_event_at = last_event_at
    return row


def _listing_row(sku: str, mlb_id: str, inventory_id: str | None, has_variations: bool) -> tuple:
    return (sku, mlb_id, inventory_id, has_variations)


def _patch_session(
    pending_rows: list,
    listing_rows: list[tuple] | None = None,
    variation_rows: list[tuple] | None = None,
):
    """Mock AsyncSessionLocal so each ``session.execute(...)`` returns the
    next pre-canned result. The scan issues at most 2 execute calls per
    invocation: (1) main listings, (2) variations if any has_variations=True.
    """
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    mock_session.commit = AsyncMock()

    listing_rows = listing_rows or []
    variation_rows = variation_rows or []

    # Order of session.execute() in scan_and_reconcile:
    # 1. _cancel_non_fulfillment_pending raw SQL → returns no rows
    # 2. main MLListingORM listings query
    # 3. variations query (only if at least one has_variations=True; we add
    #    it unconditionally — extra unused side_effects are harmless)
    cancel_result = MagicMock()
    cancel_result.all = MagicMock(return_value=[])
    listing_result = MagicMock()
    listing_result.all = MagicMock(return_value=listing_rows)
    variation_result = MagicMock()
    variation_result.all = MagicMock(return_value=variation_rows)

    mock_session.execute = AsyncMock(side_effect=[cancel_result, listing_result, variation_result])

    repo_mock = AsyncMock()
    repo_mock.list_all = AsyncMock(return_value=(pending_rows, len(pending_rows)))
    repo_mock.mark_received = AsyncMock(return_value=MagicMock())
    repo_mock.apply_partial_reception = AsyncMock(return_value=MagicMock())
    repo_mock.mark_cancelled = AsyncMock(return_value=MagicMock())

    return mock_session, repo_mock


@pytest.fixture
def ml_client() -> AsyncMock:
    client = AsyncMock()
    client.list_fulfillment_operations = AsyncMock(return_value=_EMPTY_PAYLOAD)
    return client


class TestReconciliation:
    @pytest.mark.asyncio
    async def test_no_pending_transfers_short_circuits(self, ml_client: AsyncMock) -> None:
        service = FulfillmentReceptionService(ml_client=ml_client)
        mock_session, repo_mock = _patch_session([])

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert result.skus_scanned == 0
        assert result.transfers_received == 0
        ml_client.list_fulfillment_operations.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_transfer_inbound_reception_match(self, ml_client: AsyncMock) -> None:
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-A", 10, days_ago=2)]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-A", "MLB1", "INV-A", False)],
        )
        ml_client.list_fulfillment_operations = AsyncMock(
            return_value={
                "paging": {"total": 1},
                "results": [
                    _event(10, date_iso=(datetime.now(UTC) - timedelta(hours=1)).isoformat())
                ],
            }
        )

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert result.skus_scanned == 1
        assert result.transfers_received == 1
        assert repo_mock.apply_partial_reception.call_count == 1

    @pytest.mark.asyncio
    async def test_transfer_delivery_with_inbound_id_credits(self, ml_client: AsyncMock) -> None:
        """TRANSFER_DELIVERY with external_references.type=inbound_id IS
        seller inbound reception (empirical: 2019/2019 such events in the
        2026-06-07 audit). Each unit credits the pending transfer FIFO.
        """
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-A", 3, days_ago=2)]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-A", "MLB1", "INV-A", False)],
        )
        ml_client.list_fulfillment_operations = AsyncMock(
            return_value={
                "paging": {"total": 3},
                "results": [
                    _event(
                        1,
                        event_type="TRANSFER_DELIVERY",
                        date_iso=(datetime.now(UTC) - timedelta(hours=h)).isoformat(),
                        inbound_id=f"6800000{h}",  # default would suffice; explicit for clarity
                    )
                    for h in (1, 2, 3)
                ],
            }
        )

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        # 3 events of 1 unit, transfer of 3 → fully received.
        assert result.transfers_received == 1

    @pytest.mark.asyncio
    async def test_transfer_delivery_without_inbound_id_does_not_credit(
        self, ml_client: AsyncMock
    ) -> None:
        """TRANSFER_DELIVERY WITHOUT inbound_id ref is a hypothetical
        ML-internal warehouse move (never observed in our 2019-event
        production sample but guarded). Must not credit the pending
        transfer.
        """
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-A", 3, days_ago=2)]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-A", "MLB1", "INV-A", False)],
        )
        ml_client.list_fulfillment_operations = AsyncMock(
            return_value={
                "paging": {"total": 3},
                "results": [
                    _event(
                        1,
                        event_type="TRANSFER_DELIVERY",
                        date_iso=(datetime.now(UTC) - timedelta(hours=h)).isoformat(),
                        inbound_id=None,  # no ref → discriminator rejects
                    )
                    for h in (1, 2, 3)
                ],
            }
        )

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert result.transfers_received == 0

    @pytest.mark.asyncio
    async def test_non_inbound_event_types_filtered_out(self, ml_client: AsyncMock) -> None:
        """SALE_CONFIRMATION / TRANSFER_RESERVATION are negative and must not
        contribute to received quantity."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-A", 5, days_ago=2)]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-A", "MLB1", "INV-A", False)],
        )
        ml_client.list_fulfillment_operations = AsyncMock(
            return_value={
                "paging": {"total": 2},
                "results": [
                    _event(-5, event_type="SALE_CONFIRMATION"),
                    _event(-3, event_type="TRANSFER_RESERVATION"),
                ],
            }
        )

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert result.transfers_received == 0

    @pytest.mark.asyncio
    async def test_sku_without_inventory_id_is_skipped(self, ml_client: AsyncMock) -> None:
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-UNK", 10)]
        mock_session, repo_mock = _patch_session(transfers, listing_rows=[])

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert "SKU-UNK" in result.skus_with_no_inventory
        ml_client.list_fulfillment_operations.assert_not_called()

    @pytest.mark.asyncio
    async def test_multi_inventory_enumerates_variations(self, ml_client: AsyncMock) -> None:
        """A SKU whose listing has variations must have ALL variation inventories
        queried, and reception events summed across them."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-V", 8, days_ago=2)]
        # Main row: has_variations=True. Variations: two distinct inventories.
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-V", "MLB-VAR", None, True)],
            variation_rows=[("SKU-V", "INV-V1"), ("SKU-V", "INV-V2")],
        )

        # Each inventory receives 4 — together they cover the 8-unit transfer.
        async def _by_inventory(*, inventory_id: str, **_kwargs) -> dict:
            return {
                "paging": {"total": 1},
                "results": [
                    _event(
                        4,
                        date_iso=(datetime.now(UTC) - timedelta(hours=1)).isoformat(),
                    )
                ],
            }

        ml_client.list_fulfillment_operations = AsyncMock(side_effect=_by_inventory)

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert result.transfers_received == 1
        # Both inventories queried, at least one call per inventory.
        inventory_ids_called = {
            c.kwargs["inventory_id"] for c in ml_client.list_fulfillment_operations.call_args_list
        }
        assert inventory_ids_called == {"INV-V1", "INV-V2"}

    @pytest.mark.asyncio
    async def test_chronology_guard_prevents_pre_transfer_credit(
        self, ml_client: AsyncMock
    ) -> None:
        """A reception event dated BEFORE the transfer must not be credited to it,
        even though the date window starts at the transfer date.
        """
        service = FulfillmentReceptionService(ml_client=ml_client)
        # The transfer happened 1 day ago; the ML event is 5 days ago.
        # (Could legitimately come back from the ML API if a previous transfer
        # was older than this list and we're querying a wide window via the
        # script — but in the cron, oldest_transfer_date == this transfer.)
        transfers = [_make_transfer_orm(1, "SKU-A", 5, days_ago=1)]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-A", "MLB1", "INV-A", False)],
        )
        ml_client.list_fulfillment_operations = AsyncMock(
            return_value={
                "paging": {"total": 1},
                "results": [
                    _event(5, date_iso=(datetime.now(UTC) - timedelta(days=5)).isoformat()),
                ],
            }
        )

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        # Event was pre-transfer → transfer stays pending.
        assert result.transfers_received == 0

    @pytest.mark.asyncio
    async def test_ml_api_error_does_not_stop_other_skus(self, ml_client: AsyncMock) -> None:
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [
            _make_transfer_orm(1, "SKU-A", 5, days_ago=2),
            _make_transfer_orm(2, "SKU-B", 5, days_ago=2),
        ]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[
                _listing_row("SKU-A", "MLB-A", "INV-A", False),
                _listing_row("SKU-B", "MLB-B", "INV-B", False),
            ],
        )

        async def _side(*, inventory_id: str, **_kwargs) -> dict:
            if inventory_id == "INV-A":
                raise RuntimeError("ML API down")
            return {
                "paging": {"total": 1},
                "results": [
                    _event(5, date_iso=(datetime.now(UTC) - timedelta(hours=1)).isoformat()),
                ],
            }

        ml_client.list_fulfillment_operations = AsyncMock(side_effect=_side)

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            result = await service.scan_and_reconcile()

        assert any("SKU-A" in err for err in result.errors)
        assert result.transfers_received == 1

    @pytest.mark.asyncio
    async def test_old_transfer_chunks_into_60d_windows(self, ml_client: AsyncMock) -> None:
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer_orm(1, "SKU-OLD", 5, days_ago=120)]
        mock_session, repo_mock = _patch_session(
            transfers,
            listing_rows=[_listing_row("SKU-OLD", "MLB-OLD", "INV-OLD", False)],
        )

        call_count = 0

        async def _track(**_kwargs) -> dict:
            nonlocal call_count
            call_count += 1
            return _EMPTY_PAYLOAD

        ml_client.list_fulfillment_operations = AsyncMock(side_effect=_track)

        with (
            patch(
                "tiny_mirror.services.fulfillment_reception_service.AsyncSessionLocal",
                return_value=mock_session,
            ),
            patch(
                "tiny_mirror.services.fulfillment_reception_service.FulfillmentTransferRepository",
                return_value=repo_mock,
            ),
        ):
            await service.scan_and_reconcile()

        assert call_count >= 2, f"Expected chunked calls for 120-day-old transfer; got {call_count}"
