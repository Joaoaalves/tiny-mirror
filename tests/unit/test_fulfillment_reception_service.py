"""Unit tests for FulfillmentReceptionService.

Mock payloads use the REAL ML INBOUND_RECEPTION response shape captured from
production (2026-05): ``detail.available_quantity`` per event, summed across
events in the date range. See _MULTI_EVENT_PAYLOAD below for an exact copy
of a 3-event reception cycle (16 units inbound split into 2 + 1 + 6).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.fulfillment_reception_service import (
    FulfillmentReceptionService,
    _extract_received_qty,
    _format_ml_datetime,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Real captured ML payloads (sanitized — same shape, fake IDs/quantities)
# ---------------------------------------------------------------------------
# Single complete event: 1 transfer of 10 → 1 INBOUND_RECEPTION of 10
_SINGLE_EVENT_PAYLOAD = {
    "paging": {"total": 1, "limit": None, "scroll": None},
    "results": [
        {
            "id": 1000000000000000001,
            "seller_id": 227584372,
            "date_created": "2026-05-10T14:30:00Z",
            "type": "INBOUND_RECEPTION",
            "detail": {"available_quantity": 10, "not_available_detail": []},
            "result": {
                "total": 10,
                "available_quantity": 10,
                "not_available_quantity": 0,
                "not_available_detail": [],
            },
            "external_references": [{"type": "inbound_id", "value": "70000001"}],
            "inventory_id": "TEST123",
        }
    ],
}

# Multi-event single inbound: 3 events summing to 9 (covers a transfer of 9)
_MULTI_EVENT_PAYLOAD = {
    "paging": {"total": 3, "limit": None, "scroll": None},
    "results": [
        {
            "id": 1000000000000000002,
            "seller_id": 227584372,
            "date_created": "2026-05-10T11:27:11Z",
            "type": "INBOUND_RECEPTION",
            "detail": {"available_quantity": 2, "not_available_detail": []},
            "result": {"total": 9, "available_quantity": 2, "not_available_quantity": 0},
            "external_references": [{"type": "inbound_id", "value": "70000002"}],
            "inventory_id": "TEST123",
        },
        {
            "id": 1000000000000000003,
            "seller_id": 227584372,
            "date_created": "2026-05-10T12:43:45Z",
            "type": "INBOUND_RECEPTION",
            "detail": {"available_quantity": 1, "not_available_detail": []},
            "result": {"total": 9, "available_quantity": 3, "not_available_quantity": 0},
            "external_references": [{"type": "inbound_id", "value": "70000002"}],
            "inventory_id": "TEST123",
        },
        {
            "id": 1000000000000000004,
            "seller_id": 227584372,
            "date_created": "2026-05-11T02:04:25Z",
            "type": "INBOUND_RECEPTION",
            "detail": {"available_quantity": 6, "not_available_detail": []},
            "result": {"total": 9, "available_quantity": 9, "not_available_quantity": 0},
            "external_references": [{"type": "inbound_id", "value": "70000002"}],
            "inventory_id": "TEST123",
        },
    ],
}

_EMPTY_PAYLOAD = {"paging": {"total": 0}, "results": []}


# ---------------------------------------------------------------------------
# Helper: extraction tests
# ---------------------------------------------------------------------------
class TestExtractReceivedQty:
    def test_real_ml_shape_detail_available(self) -> None:
        op = _SINGLE_EVENT_PAYLOAD["results"][0]
        assert _extract_received_qty(op) == 10

    def test_real_ml_shape_partial_event(self) -> None:
        op = _MULTI_EVENT_PAYLOAD["results"][0]
        assert _extract_received_qty(op) == 2  # detail.available_quantity, not result.total

    def test_fallback_quantities_received(self) -> None:
        assert _extract_received_qty({"quantities": {"received": 7}}) == 7

    def test_fallback_quantity_flat(self) -> None:
        assert _extract_received_qty({"quantity": 5}) == 5

    def test_missing_fields_returns_zero(self) -> None:
        assert _extract_received_qty({"type": "INBOUND_RECEPTION"}) == 0

    def test_malformed_value_returns_zero(self) -> None:
        assert _extract_received_qty({"detail": {"available_quantity": "not_a_number"}}) == 0

    def test_zero_available_quantity(self) -> None:
        op = {"detail": {"available_quantity": 0}}
        assert _extract_received_qty(op) == 0


class TestFormatMlDatetime:
    def test_utc_aware(self) -> None:
        dt = datetime(2026, 5, 14, 12, 30, 45, tzinfo=UTC)
        assert _format_ml_datetime(dt) == "2026-05-14T12:30:45.000Z"

    def test_converts_other_timezone(self) -> None:
        from datetime import timezone

        dt = datetime(2026, 5, 14, 9, 30, 45, tzinfo=timezone(timedelta(hours=-3)))
        # -03:00 → UTC = +12:30:45 → 2026-05-14T12:30:45
        assert _format_ml_datetime(dt) == "2026-05-14T12:30:45.000Z"


# ---------------------------------------------------------------------------
# Service test fixtures
# ---------------------------------------------------------------------------
def _make_transfer(
    transfer_id: int,
    sku: str,
    quantity: int,
    days_ago: int = 1,
    product_tiny_id: int = 971992238,
) -> MagicMock:
    """Build a fake FulfillmentTransferORM row."""
    row = MagicMock()
    row.id = transfer_id
    row.product_sku = sku
    row.quantity = quantity
    row.product_tiny_id = product_tiny_id
    row.transferred_at = datetime.now(UTC) - timedelta(days=days_ago)
    return row


@pytest.fixture
def ml_client() -> AsyncMock:
    client = AsyncMock()
    client.list_fulfillment_inbound_operations = AsyncMock(return_value=_EMPTY_PAYLOAD)
    return client


def _patch_session(pending_rows: list, inventory_map: dict[str, str]):
    """Return a context manager that patches AsyncSessionLocal for the service."""
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=None)
    mock_session.commit = AsyncMock()

    # Two session uses: (1) load pending transfers, (2) load inventory_ids.
    # For each `session.execute(...)` we return a mock with rows that match.
    inventory_rows = [MagicMock(sku=sku, inventory_id=inv) for sku, inv in inventory_map.items()]
    inventory_result = MagicMock()
    inventory_result.__iter__ = lambda self: iter(inventory_rows)
    mock_session.execute = AsyncMock(return_value=inventory_result)

    repo_mock = AsyncMock()
    repo_mock.list_all = AsyncMock(return_value=(pending_rows, len(pending_rows)))
    repo_mock.mark_received = AsyncMock(return_value=MagicMock())

    return mock_session, repo_mock


# ---------------------------------------------------------------------------
# Reconciliation scenarios
# ---------------------------------------------------------------------------
class TestReconciliation:
    @pytest.mark.asyncio
    async def test_no_pending_transfers_short_circuits(self, ml_client: AsyncMock) -> None:
        service = FulfillmentReceptionService(ml_client=ml_client)
        mock_session, repo_mock = _patch_session([], {})

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
        ml_client.list_fulfillment_inbound_operations.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_transfer_single_reception_exact_match(self, ml_client: AsyncMock) -> None:
        """1 transfer of 10 + 1 INBOUND_RECEPTION of 10 → marked received."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer(1, "SKU-A", 10, days_ago=1)]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-A": "INV-A"})
        ml_client.list_fulfillment_inbound_operations = AsyncMock(
            return_value=_SINGLE_EVENT_PAYLOAD
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
        assert repo_mock.mark_received.call_args.args[0] == 1

    @pytest.mark.asyncio
    async def test_two_transfers_one_inbound_sum_covers_both(self, ml_client: AsyncMock) -> None:
        """2 transfers of 5 each + INBOUND_RECEPTION events summing to 9 → only first covered."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [
            _make_transfer(1, "SKU-A", 5, days_ago=3),  # oldest
            _make_transfer(2, "SKU-A", 5, days_ago=1),
        ]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-A": "INV-A"})
        # Multi-event payload sums to 2+1+6=9 → covers transfer #1 (5) only
        ml_client.list_fulfillment_inbound_operations = AsyncMock(return_value=_MULTI_EVENT_PAYLOAD)

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

        # 9 received, FIFO: T1=5 consumed (remaining=4), T2=5 not covered → only T1 marked
        assert result.transfers_received == 1
        marked_ids = [c.args[0] for c in repo_mock.mark_received.call_args_list]
        assert marked_ids == [1]  # oldest first

    @pytest.mark.asyncio
    async def test_partial_reception_keeps_transfer_pending(self, ml_client: AsyncMock) -> None:
        """1 transfer of 10 + INBOUND_RECEPTION events summing to 8 → stays pending."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer(1, "SKU-A", 10, days_ago=1)]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-A": "INV-A"})

        partial_payload = {
            "paging": {"total": 2},
            "results": [
                {"detail": {"available_quantity": 5}, "type": "INBOUND_RECEPTION"},
                {"detail": {"available_quantity": 3}, "type": "INBOUND_RECEPTION"},
            ],
        }
        ml_client.list_fulfillment_inbound_operations = AsyncMock(return_value=partial_payload)

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
        repo_mock.mark_received.assert_not_called()

    @pytest.mark.asyncio
    async def test_sku_without_inventory_id_is_skipped(self, ml_client: AsyncMock) -> None:
        """Transfer for a SKU not in ml_listings is recorded in skus_with_no_inventory."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer(1, "SKU-UNKNOWN", 10, days_ago=1)]
        # inventory_map deliberately empty
        mock_session, repo_mock = _patch_session(transfers, {})

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

        assert "SKU-UNKNOWN" in result.skus_with_no_inventory
        assert result.transfers_received == 0
        ml_client.list_fulfillment_inbound_operations.assert_not_called()

    @pytest.mark.asyncio
    async def test_ml_api_error_recorded_does_not_stop_others(self, ml_client: AsyncMock) -> None:
        """An API error for one SKU is logged in errors but doesn't crash the scan."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [
            _make_transfer(1, "SKU-A", 5, days_ago=1),
            _make_transfer(2, "SKU-B", 5, days_ago=1),
        ]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-A": "INV-A", "SKU-B": "INV-B"})

        # SKU-A's API call raises; SKU-B succeeds with a matching reception
        async def _api_side_effect(inventory_id: str, **_kwargs) -> dict:
            if inventory_id == "INV-A":
                raise RuntimeError("ML API down")
            return {
                "paging": {"total": 1},
                "results": [{"detail": {"available_quantity": 5}}],
            }

        ml_client.list_fulfillment_inbound_operations = AsyncMock(side_effect=_api_side_effect)

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
        assert result.transfers_received == 1  # SKU-B still processed

    @pytest.mark.asyncio
    async def test_no_inbound_events_keeps_pending(self, ml_client: AsyncMock) -> None:
        """Empty ML response → no transfers marked, no errors."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer(1, "SKU-A", 5, days_ago=1)]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-A": "INV-A"})

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
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_old_transfer_chunks_into_60d_windows(self, ml_client: AsyncMock) -> None:
        """A transfer from 120 days ago triggers multiple chunked API calls (60d max)."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer(1, "SKU-OLD", 5, days_ago=120)]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-OLD": "INV-OLD"})

        call_count = 0

        async def _track(**_kwargs) -> dict:
            nonlocal call_count
            call_count += 1
            return _EMPTY_PAYLOAD

        ml_client.list_fulfillment_inbound_operations = AsyncMock(side_effect=_track)

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

        # 120 days at 59-day chunks → at least 2 windows (chunks: 0..59, 59..118, 118..120)
        assert call_count >= 2, f"Expected chunked calls for 120-day-old transfer; got {call_count}"

    @pytest.mark.asyncio
    async def test_fifo_marks_oldest_first_across_multiple_transfers(
        self, ml_client: AsyncMock
    ) -> None:
        """3 transfers of 5 each + reception of 11 → marks the 2 oldest (sum 10), leaves newest pending."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        # Intentionally out of order — service must sort by transferred_at
        transfers = [
            _make_transfer(99, "SKU-A", 5, days_ago=1),  # newest
            _make_transfer(11, "SKU-A", 5, days_ago=5),  # oldest
            _make_transfer(55, "SKU-A", 5, days_ago=3),  # middle
        ]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-A": "INV-A"})

        ml_client.list_fulfillment_inbound_operations = AsyncMock(
            return_value={
                "paging": {"total": 1},
                "results": [{"detail": {"available_quantity": 11}}],
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

        # 11 received: T11(5) → 6 left, T55(5) → 1 left, T99(5) → not enough
        assert result.transfers_received == 2
        marked_ids = [c.args[0] for c in repo_mock.mark_received.call_args_list]
        assert marked_ids == [11, 55]

    @pytest.mark.asyncio
    async def test_old_unprocessed_transfer_before_feature_is_detected(
        self, ml_client: AsyncMock
    ) -> None:
        """A transfer registered manually before the feature existed (e.g. 30 days ago)
        is still detected and marked received as long as ML has an INBOUND_RECEPTION
        within the 60-day window."""
        service = FulfillmentReceptionService(ml_client=ml_client)
        transfers = [_make_transfer(42, "SKU-LEGACY", 16, days_ago=30)]
        mock_session, repo_mock = _patch_session(transfers, {"SKU-LEGACY": "INV-LEG"})

        # ML returns a reception inside the window matching the 16 units
        ml_client.list_fulfillment_inbound_operations = AsyncMock(
            return_value={
                "paging": {"total": 2},
                "results": [
                    {"detail": {"available_quantity": 10}, "date_created": "2026-04-20T00:00:00Z"},
                    {"detail": {"available_quantity": 6}, "date_created": "2026-04-25T00:00:00Z"},
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
        assert repo_mock.mark_received.call_args.args[0] == 42

    @pytest.mark.asyncio
    async def test_inbound_event_predating_transfer_does_not_double_count(
        self, ml_client: AsyncMock
    ) -> None:
        """If we ask ML for INBOUND_RECEPTION starting at the OLDEST transfer date,
        only events after that date should contribute. The service relies on the
        ML API filter (date_from). We verify that the date_from passed matches
        the oldest transfer date — this is the guarantee against double-counting
        receptions from before any pending transfer existed.
        """
        service = FulfillmentReceptionService(ml_client=ml_client)
        oldest_date = datetime.now(UTC) - timedelta(days=10)
        t = MagicMock()
        t.id = 1
        t.product_sku = "SKU-A"
        t.quantity = 5
        t.product_tiny_id = 1
        t.transferred_at = oldest_date

        mock_session, repo_mock = _patch_session([t], {"SKU-A": "INV-A"})

        captured: dict[str, str] = {}

        async def _capture(**kwargs) -> dict:
            captured["date_from"] = kwargs["date_from"]
            return _EMPTY_PAYLOAD

        ml_client.list_fulfillment_inbound_operations = AsyncMock(side_effect=_capture)

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

        # date_from must match the oldest transfer date (10 days ago), formatted ISO-Z
        expected_prefix = oldest_date.strftime("%Y-%m-%dT%H:%M:%S")
        assert captured["date_from"].startswith(expected_prefix), (
            f"date_from {captured['date_from']!r} should start with oldest transfer "
            f"{expected_prefix!r}; ML filter is our defense against double-counting old receptions."
        )
