"""Unit tests for :mod:`tiny_mirror.services.ml_order_sync_service`.

ML HTTP is always mocked (httpx.MockTransport); the DB is patched.
Payload shapes mirror live /orders/search and /shipments/{id}/costs
responses read on 2026-09-30.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.services import ml_order_sync_service as mod
from tiny_mirror.services.ml_order_sync_service import (
    MLOrderSyncService,
    parse_order,
    parse_order_items,
    parse_shipment_costs,
)

pytestmark = pytest.mark.unit

SELLER = "227584372"


def _order(order_id: int, **overrides: Any) -> dict[str, Any]:
    order: dict[str, Any] = {
        "id": order_id,
        "pack_id": None,
        "status": "paid",
        "date_created": "2026-09-30T11:38:37.000-04:00",
        "date_closed": "2026-09-30T11:38:40.000-04:00",
        "last_updated": "2026-09-30T12:00:00.000-04:00",
        "buyer": {"id": 555, "nickname": "COMPRADOR"},
        "total_amount": 38.9,
        "paid_amount": 38.9,
        "shipping": {"id": 48136640657},
        "cancel_detail": None,
        "tags": ["paid", "not_delivered"],
        "order_items": [
            {
                "item": {"id": "MLB3351786643", "variation_id": None, "seller_sku": "SKU-1"},
                "quantity": 3,
                "unit_price": 12.97,
                "sale_fee": 5.52,
                "listing_type_id": "gold_special",
            }
        ],
    }
    order.update(overrides)
    return order


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_parse_order_keeps_exact_time_and_links_to_tiny_by_order_id() -> None:
    row = parse_order(_order(2000018719000620))

    assert row is not None
    assert row["tiny_ref"] == "2000018719000620"
    assert row["date_created"] == datetime(2026, 9, 30, 15, 38, 37, tzinfo=UTC)
    assert row["buyer_id"] == 555
    assert row["total_amount"] == Decimal("38.90")
    assert row["shipping_id"] == 48136640657
    assert row["cancel_detail"] is None


def test_parse_order_links_packs_by_pack_id() -> None:
    row = parse_order(_order(2000018719084930, pack_id=2000015279176039))

    assert row is not None
    assert row["pack_id"] == 2000015279176039
    assert row["tiny_ref"] == "2000015279176039"


def test_parse_order_keeps_cancellation_reason() -> None:
    cancel = {"group": "buyer", "code": "buyer_cancel", "description": "Arrependimento"}
    row = parse_order(_order(1, status="cancelled", cancel_detail=cancel))

    assert row is not None
    assert row["status"] == "cancelled"
    assert row["cancel_detail"] == cancel


@pytest.mark.parametrize(
    "overrides", [{"id": None}, {"date_created": None}, {"date_created": "ontem"}]
)
def test_parse_order_rejects_unusable_rows(overrides: dict[str, Any]) -> None:
    assert parse_order(_order(1, **overrides)) is None


def test_parse_order_items_keeps_per_unit_fee() -> None:
    rows = parse_order_items(_order(7))

    assert rows == [
        {
            "order_id": 7,
            "mlb_id": "MLB3351786643",
            "variation_id": None,
            "seller_sku": "SKU-1",
            "quantity": 3,
            "unit_price": Decimal("12.97"),
            "sale_fee": Decimal("5.52"),
            "listing_type_id": "gold_special",
        }
    ]


def test_parse_order_items_skips_lines_without_mlb() -> None:
    raw = _order(7, order_items=[{"item": {}, "quantity": 1}])

    assert parse_order_items(raw) == []


def test_parse_shipment_costs_takes_the_seller_entry() -> None:
    costs = {
        "gross_amount": 52.72,
        "receiver": {"cost": 0, "save": 42.79},
        "senders": [
            {"user_id": 1, "cost": 99, "save": 0},
            {"user_id": int(SELLER), "cost": 6.95, "save": 2.98},
        ],
    }

    row = parse_shipment_costs(48136640657, costs, SELLER, "fulfillment")

    assert row["seller_cost"] == Decimal("6.95")
    assert row["seller_save"] == Decimal("2.98")
    assert row["buyer_cost"] == Decimal("0.00")
    assert row["list_cost"] == Decimal("52.72")
    assert row["logistic_type"] == "fulfillment"


def test_parse_shipment_costs_without_senders_leaves_seller_fields_empty() -> None:
    row = parse_shipment_costs(1, {"receiver": {}}, SELLER, None)

    assert row["seller_cost"] is None and row["seller_save"] is None


# ---------------------------------------------------------------------------
# Sync flow (HTTP mocked)
# ---------------------------------------------------------------------------
def _service(handler: Any) -> MLOrderSyncService:
    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="tok")
    tok.handle_unauthorized = AsyncMock(return_value="tok2")
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return MLOrderSyncService(token_service=tok, http_client=http, ml_user_id=SELLER)


def _handler(orders_by_day: dict[str, list[dict[str, Any]]], calls: list[str]) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/orders/search":
            day = request.url.params["order.date_created.from"][:10]
            results = orders_by_day.get(day, [])
            offset = int(request.url.params["offset"])
            page = results[offset : offset + 50]
            return httpx.Response(200, json={"results": page, "paging": {"total": len(results)}})
        if request.url.path.endswith("/costs"):
            return httpx.Response(
                200,
                json={
                    "gross_amount": 30,
                    "receiver": {"cost": 0},
                    "senders": [{"user_id": int(SELLER), "cost": 7.5, "save": 1}],
                },
            )
        if request.url.path.startswith("/shipments/"):
            return httpx.Response(200, json={"logistic_type": "cross_docking"})
        return httpx.Response(404)

    return handler


async def test_sync_collects_every_status_and_fetches_new_shipments() -> None:
    today = datetime.now(UTC).date()
    orders = {
        today.isoformat(): [
            _order(1, shipping={"id": 10}),
            _order(2, status="cancelled", shipping={"id": 20}),
        ],
        (today - timedelta(days=1)).isoformat(): [_order(3, shipping={"id": 10})],
    }
    calls: list[str] = []
    service = _service(_handler(orders, calls))
    service._shipments_to_refresh = AsyncMock(return_value=[10, 20])  # type: ignore[method-assign]
    service._persist = AsyncMock()  # type: ignore[method-assign]

    stats = await service.sync(days=2)

    assert stats["orders"] == 3
    assert stats["cancelled"] == 1
    assert stats["shipments_fetched"] == 2
    persisted_orders, persisted_items, persisted_ships = service._persist.await_args.args
    assert sorted(o["order_id"] for o in persisted_orders) == [1, 2, 3]
    assert set(persisted_items) == {1, 2, 3}
    assert {s["shipment_id"]: s["seller_cost"] for s in persisted_ships} == {
        10: Decimal("7.50"),
        20: Decimal("7.50"),
    }
    assert calls.count("/orders/search") == 2


async def test_sync_paginates_a_busy_day() -> None:
    today = datetime.now(UTC).date().isoformat()
    calls: list[str] = []
    service = _service(_handler({today: [_order(i) for i in range(1, 121)]}, calls))
    service._shipments_to_refresh = AsyncMock(return_value=[])  # type: ignore[method-assign]
    service._persist = AsyncMock()  # type: ignore[method-assign]

    stats = await service.sync(days=1)

    assert stats["orders"] == 120
    assert calls.count("/orders/search") == 3


async def test_failed_shipment_is_retried_next_run_not_stored() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/orders/search":
            return httpx.Response(200, json={"results": [_order(1)], "paging": {"total": 1}})
        return httpx.Response(429)

    service = _service(handler)
    service._shipments_to_refresh = AsyncMock(return_value=[48136640657])  # type: ignore[method-assign]
    service._persist = AsyncMock()  # type: ignore[method-assign]

    stats = await service.sync(days=1)

    assert stats["shipments_failed"] == 1
    assert service._persist.await_args.args[2] == []


async def test_expired_token_is_refreshed_once() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.headers["Authorization"])
        if request.headers["Authorization"] == "Bearer tok":
            return httpx.Response(401)
        return httpx.Response(200, json={"results": [], "paging": {"total": 0}})

    service = _service(handler)

    assert await service._fetch_day(datetime.now(UTC).date()) == []
    assert calls == ["Bearer tok", "Bearer tok2"]
    service._tok.handle_unauthorized.assert_awaited_once()


# ---------------------------------------------------------------------------
# Shipment refresh selection (DB patched)
# ---------------------------------------------------------------------------
def _session_returning(rows: list[tuple[int, datetime]]) -> Any:
    @asynccontextmanager
    async def factory() -> Any:
        session = MagicMock()
        result = MagicMock()
        result.all.return_value = rows
        session.execute = AsyncMock(return_value=result)
        yield session

    return factory


async def test_shipments_to_refresh_skips_unchanged_known_shipments() -> None:
    t0 = datetime(2026, 9, 30, 12, tzinfo=UTC)
    orders = {
        1: {"shipping_id": 10, "last_updated": t0},  # known, fetched after -> skip
        2: {"shipping_id": 20, "last_updated": t0},  # known, order changed later -> fetch
        3: {"shipping_id": 30, "last_updated": t0},  # never fetched -> fetch
        4: {"shipping_id": None, "last_updated": t0},  # no shipment
    }
    known = [(10, t0 + timedelta(hours=1)), (20, t0 - timedelta(hours=1))]
    service = _service(lambda r: httpx.Response(404))

    with patch.object(mod, "AsyncSessionLocal", _session_returning(known)):
        assert sorted(await service._shipments_to_refresh(orders)) == [20, 30]


async def test_shipments_to_refresh_without_shipments_skips_the_db() -> None:
    service = _service(lambda r: httpx.Response(404))

    with patch.object(mod, "AsyncSessionLocal", side_effect=AssertionError("no DB call")):
        assert await service._shipments_to_refresh({1: {"shipping_id": None}}) == []
