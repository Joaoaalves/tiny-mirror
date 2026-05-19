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
) -> dict:
    return {
        "id": 1000000000000000000 + qty,
        "type": event_type,
        "date_created": date_iso,
        "detail": {"available_quantity": qty, "not_available_detail": []},
        "result": {"total": qty, "available_quantity": qty},
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
def _transfer(transfer_id: int, qty: int, days_ago: int) -> MagicMock:
    row = MagicMock()
    row.id = transfer_id
    row.quantity = qty
    row.transferred_at = datetime.now(UTC) - timedelta(days=days_ago)
    return row


class TestFifoMatchWithChronology:
    def test_single_transfer_single_event_exact(self) -> None:
        t = _transfer(1, 10, days_ago=2)
        evt = _event(10, date_iso=(t.transferred_at + timedelta(hours=1)).isoformat())
        decisions = _fifo_match_with_chronology([t], [evt])
        assert decisions[0].transfer_id == 1
        assert decisions[0].received_at is not None

    def test_event_before_transfer_does_not_credit(self) -> None:
        """The chronology guard: an event whose date_created is BEFORE the
        transfer's transferred_at can never fulfill it.
        """
        t = _transfer(1, 10, days_ago=2)
        # Event 5 days ago — before the transfer 2 days ago.
        evt = _event(10, date_iso=(datetime.now(UTC) - timedelta(days=5)).isoformat())
        decisions = _fifo_match_with_chronology([t], [evt])
        assert decisions[0].received_at is None

    def test_partial_event_qty_carries_over_to_next_transfer(self) -> None:
        """An event partially consumed by the older transfer leaves remainder
        for the next FIFO transfer."""
        t1 = _transfer(1, 6, days_ago=5)
        t2 = _transfer(2, 4, days_ago=4)
        evt = _event(10, date_iso=(datetime.now(UTC) - timedelta(days=3)).isoformat())
        decisions = _fifo_match_with_chronology([t1, t2], [evt])
        assert all(d.received_at is not None for d in decisions)

    def test_transfer_delivery_events_counted(self) -> None:
        """Per the matcher's contract, callers pre-filter to inbound types.
        Make sure a TRANSFER_DELIVERY-shaped event still fulfills the math.
        """
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
        assert decisions[0].received_at is not None

    def test_three_transfers_FIFO_oldest_first(self) -> None:
        """3 transfers of 5 each + 1 event of 11 → 2 oldest received, newest pending."""
        # Pass them out of date order — matcher must sort.
        ts = [
            _transfer(99, 5, days_ago=1),  # newest
            _transfer(11, 5, days_ago=5),  # oldest
            _transfer(55, 5, days_ago=3),  # middle
        ]
        evt = _event(11, date_iso=(datetime.now(UTC)).isoformat())
        decisions = _fifo_match_with_chronology(ts, [evt])
        by_id = {d.transfer_id: d.received_at for d in decisions}
        assert by_id[11] is not None
        assert by_id[55] is not None
        assert by_id[99] is None  # 1 unit left, needs 5

    def test_received_at_is_last_event_consumed(self) -> None:
        """The persisted received_at must reflect the event that closed the
        transfer (not the first event consumed)."""
        t = _transfer(1, 8, days_ago=5)
        evt1 = _event(3, date_iso=(datetime.now(UTC) - timedelta(days=4)).isoformat())
        evt2_date = (datetime.now(UTC) - timedelta(days=2)).isoformat()
        evt2 = _event(5, date_iso=evt2_date)
        decisions = _fifo_match_with_chronology([t], [evt1, evt2])
        assert decisions[0].received_at is not None
        assert decisions[0].received_at == _extract_received_at(evt2)


# ---------------------------------------------------------------------------
# Service tests — scan_and_reconcile with mocks
# ---------------------------------------------------------------------------
def _make_transfer_orm(
    transfer_id: int,
    sku: str,
    quantity: int,
    days_ago: int = 1,
    product_tiny_id: int = 971992238,
) -> MagicMock:
    row = MagicMock()
    row.id = transfer_id
    row.product_sku = sku
    row.quantity = quantity
    row.product_tiny_id = product_tiny_id
    row.transferred_at = datetime.now(UTC) - timedelta(days=days_ago)
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

    listing_result = MagicMock()
    listing_result.all = MagicMock(return_value=listing_rows)
    variation_result = MagicMock()
    variation_result.all = MagicMock(return_value=variation_rows)

    mock_session.execute = AsyncMock(side_effect=[listing_result, variation_result])

    repo_mock = AsyncMock()
    repo_mock.list_all = AsyncMock(return_value=(pending_rows, len(pending_rows)))
    repo_mock.mark_received = AsyncMock(return_value=MagicMock())

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
        assert repo_mock.mark_received.call_count == 1

    @pytest.mark.asyncio
    async def test_transfer_delivery_events_are_counted(self, ml_client: AsyncMock) -> None:
        """TRANSFER_DELIVERY (unit-by-unit flow) must reconcile pending transfers
        the same as INBOUND_RECEPTION — the regression fix."""
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

        assert result.transfers_received == 1

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
