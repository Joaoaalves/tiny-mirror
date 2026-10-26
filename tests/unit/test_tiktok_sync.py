"""Unit tests for the TikTok Shop integration (client + TikTokSyncService).

HTTP mocked (httpx.MockTransport), DB patched. Payload shapes mirror live
TikTok Shop 202309 responses read on 2026-10-26.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.infrastructure.external import tiktok_shop_client as client_mod
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.infrastructure.external.tiktok_shop_client import (
    ORDERS_SEARCH,
    PRODUCTS_SEARCH,
    TikTokAPIError,
    TikTokShopClient,
    TikTokTokenExpired,
    signature,
)
from tiny_mirror.scheduler import jobs as jobs_mod
from tiny_mirror.services import tiktok_sync_service as svc_mod
from tiny_mirror.services.tiktok_sync_service import (
    TikTokSyncService,
    detail_is_stale,
    parse_order,
    parse_product,
    stored_details,
)

pytestmark = pytest.mark.unit


def _creds(tmp_path: Path, expires_in: int = 3600) -> MarketplaceCredentials:
    f = tmp_path / "creds.json"
    f.write_text(
        json.dumps(
            {
                "tiktok_shop": {
                    "app_key": "ak",
                    "app_secret": "sec",
                    "shop_id": "7496203764540869177",
                    "shop_cipher": "ROW_cipher",
                    "access_token": "tok",
                    "expires_at": int(time.time()) + expires_in,
                }
            }
        )
    )
    return MarketplaceCredentials(str(f))


def _client(tmp_path: Path, handler: Any, expires_in: int = 3600) -> TikTokShopClient:
    return TikTokShopClient(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)), _creds(tmp_path, expires_in)
    )


def _ok(data: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "message": "Success", "data": data})


ORDER = {
    "id": "586807550558046120",
    "status": "AWAITING_SHIPMENT",
    "create_time": 1792900000,
    "update_time": 1792900600,
    "fulfillment_type": "FULFILLMENT_BY_SELLER",
    "payment": {"currency": "BRL", "total_amount": "89.90", "sub_total": "79.90"},
    "line_items": [{"seller_sku": "DGS-SPM86"}],
    "recipient_address": {"full_address": "Rua X, 1"},
    "buyer_email": "x@y.z",
    "buyer_message": "oi",
    "cpf": "000.000.000-00",
    "cpf_name": "Fulano",
    "user_id": "123",
}

PRODUCT = {
    "id": "1729000000000000001",
    "status": "ACTIVATE",
    "title": "Suporte TV Fixo",
    "create_time": 1790000000,
    "update_time": 1792000000,
    "skus": [
        {
            "id": "1729000000000000101",
            "seller_sku": "DGS-SPM86",
            "price": {"currency": "BRL", "tax_exclusive_price": "79.9"},
            "inventory": [{"quantity": 10, "warehouse_id": "w1"}, {"quantity": 5}],
        },
        {
            "id": "1729000000000000102",
            "seller_sku": "",
            "price": {"currency": "BRL", "tax_exclusive_price": "99.90"},
            "inventory": [],
        },
    ],
}

DETAIL = {
    "id": PRODUCT["id"],
    "status": "ACTIVATE",
    "title": "Suporte TV Fixo",
    "update_time": 1792000000,
    "brand": {"id": "1", "name": "RT AT"},
    "main_images": [
        {"thumb_urls": ["https://img/main-300.jpeg"], "urls": ["https://img/main.jpeg"]}
    ],
    "package_weight": {"unit": "KILOGRAM", "value": "1.2"},
    "skus": [
        {
            "id": "1729000000000000101",
            "seller_sku": "DGS-SPM86",
            "price": {"currency": "BRL", "tax_exclusive_price": "70.00"},
            "inventory": [{"quantity": 1}],
            "identifier_code": {"code": "7908812704280", "type": "GTIN"},
            "sales_attributes": [
                {
                    "name": "Cor",
                    "value_name": "Preto",
                    "sku_img": {"thumb_urls": ["https://img/preto-300.jpeg"]},
                }
            ],
        }
    ],
}


def _fresh(update_time: int = 1792000000, days_old: int = 0) -> dict[str, Any]:
    synced = (datetime.now(UTC) - timedelta(days=days_old)).isoformat()
    return {**DETAIL, "_detail": {"update_time": update_time, "synced_at": synced}}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
def test_signature_matches_tiktok_202309_formula() -> None:
    query = {"timestamp": "1700000000", "app_key": "ak", "sign": "x", "access_token": "t"}
    base = 'sec/order/202309/orders/searchapp_keyaktimestamp1700000000{"a":1}sec'
    expected = hmac.new(b"sec", base.encode(), hashlib.sha256).hexdigest()

    assert signature("sec", ORDERS_SEARCH, query, '{"a":1}') == expected


async def test_client_signs_posts_and_returns_data(tmp_path: Path) -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["params"] = dict(request.url.params)
        seen["token"] = request.headers["x-tts-access-token"]
        seen["body"] = request.content.decode()
        return _ok({"ok": 1})

    data = await _client(tmp_path, handler).search(ORDERS_SEARCH, {"page_size": 1}, {"a": 1})

    assert data == {"ok": 1}
    p = seen["params"]
    assert seen["method"] == "POST" and seen["token"] == "tok" and seen["body"] == '{"a":1}'
    assert p["app_key"] == "ak" and p["shop_cipher"] == "ROW_cipher" and p["page_size"] == "1"
    assert "access_token" not in p
    assert p["sign"] == signature("sec", ORDERS_SEARCH, p, '{"a":1}')


async def test_only_read_paths_are_callable(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return _ok({})

    with pytest.raises(ValueError):
        await _client(tmp_path, handler).search(
            "/product/202309/products/1/inventory/update", {}, {}
        )
    assert calls == []


async def test_get_product_is_a_signed_get_without_body(tmp_path: Path) -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        seen["body"] = request.content
        return _ok(DETAIL)

    data = await _client(tmp_path, handler).get_product(PRODUCT["id"])

    path = f"/product/202309/products/{PRODUCT['id']}"
    assert data["main_images"] == DETAIL["main_images"]
    assert seen["method"] == "GET" and seen["path"] == path and seen["body"] == b""
    assert seen["params"]["sign"] == signature("sec", path, seen["params"], "")


async def test_get_product_rejects_non_numeric_ids(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return _ok({})

    for bad in ("1/inventory/update", "", "abc"):
        with pytest.raises(ValueError):
            await _client(tmp_path, handler).get_product(bad)
    assert calls == []


async def test_expired_exported_token_is_never_refreshed(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return _ok({})

    with pytest.raises(TikTokTokenExpired):
        await _client(tmp_path, handler, expires_in=10).search(ORDERS_SEARCH, {}, {})
    assert calls == []  # no auth call, no API call


async def test_api_error_is_raised(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": 105002, "message": "invalid sign"})

    with pytest.raises(TikTokAPIError) as exc:
        await _client(tmp_path, handler).search(ORDERS_SEARCH, {}, {})
    assert exc.value.code == 105002


async def test_rate_limit_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(client_mod.asyncio, "sleep", fake_sleep)
    answers = iter([httpx.Response(429, json={"code": 36009004}), _ok({"ok": 1})])

    assert await _client(tmp_path, lambda r: next(answers)).search(ORDERS_SEARCH, {}, {}) == {
        "ok": 1
    }
    assert len(sleeps) == 1


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_parse_order_strips_buyer_pii() -> None:
    row = parse_order(ORDER)

    assert row is not None
    assert row["order_id"] == "586807550558046120"
    assert row["created_at"] == datetime.fromtimestamp(1792900000, tz=UTC)
    assert row["last_updated"] == datetime.fromtimestamp(1792900600, tz=UTC)
    assert row["total_amount"] == Decimal("89.90") and row["currency"] == "BRL"
    assert row["status"] == "AWAITING_SHIPMENT"
    assert row["fulfilled_by"] == "FULFILLMENT_BY_SELLER"
    for key in ("recipient_address", "buyer_email", "buyer_message", "cpf", "cpf_name", "user_id"):
        assert key not in row["raw"]
    assert row["raw"]["line_items"] == ORDER["line_items"]


def test_parse_order_without_id_or_time_is_skipped() -> None:
    assert parse_order({**ORDER, "id": None}) is None
    assert parse_order({**ORDER, "create_time": None}) is None


def test_parse_product_one_row_per_sku() -> None:
    rows = parse_product(PRODUCT)

    assert [(r["variation_id"], r["sku"], r["stock"], r["price"]) for r in rows] == [
        ("1729000000000000101", "DGS-SPM86", 15, Decimal("79.90")),
        ("1729000000000000102", None, 0, Decimal("99.90")),
    ]
    assert all(r["listing_id"] == PRODUCT["id"] and r["is_active"] for r in rows)
    assert "skus" not in rows[0]["raw"]["product"]


def test_parse_product_merges_detail_but_keeps_fresh_price_and_stock() -> None:
    rows = parse_product(PRODUCT, _fresh())
    first = rows[0]

    assert first["price"] == Decimal("79.90") and first["stock"] == 15  # from the search
    assert first["raw"]["sku"]["sales_attributes"][0]["value_name"] == "Preto"
    assert first["raw"]["sku"]["identifier_code"]["code"] == "7908812704280"
    assert first["raw"]["sku"]["inventory"] == PRODUCT["skus"][0]["inventory"]
    assert first["raw"]["product"]["main_images"] == DETAIL["main_images"]
    assert first["raw"]["product"]["brand"]["name"] == "RT AT"
    assert "skus" not in first["raw"]["product"] and "_detail" not in first["raw"]["product"]
    assert first["raw"]["detail"]["update_time"] == 1792000000
    # a SKU the detail doesn't know still gets the product-level detail
    assert "sales_attributes" not in rows[1]["raw"]["sku"]
    assert rows[1]["raw"]["product"]["main_images"] == DETAIL["main_images"]


def test_parse_product_without_detail_has_no_detail_marker() -> None:
    assert all("detail" not in r["raw"] for r in parse_product(PRODUCT))


def test_detail_is_stale_rules() -> None:
    assert detail_is_stale(PRODUCT, None)  # never fetched
    assert not detail_is_stale(PRODUCT, _fresh())
    assert detail_is_stale(PRODUCT, _fresh(update_time=1))  # product changed
    assert detail_is_stale(PRODUCT, _fresh(days_old=8))  # too old
    assert detail_is_stale(PRODUCT, {**DETAIL, "_detail": {"update_time": 1792000000}})


async def test_stored_details_rebuilds_product_from_sku_rows() -> None:
    raw_a = parse_product(PRODUCT, _fresh())
    rows = [(r["listing_id"], r["raw"]) for r in raw_a] + [("9", {"product": {}, "sku": {}})]
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(all=lambda: rows))
    ctx = MagicMock(__aenter__=AsyncMock(return_value=session), __aexit__=AsyncMock())

    with patch.object(svc_mod, "AsyncSessionLocal", return_value=ctx):
        out = await stored_details()

    assert set(out) == {PRODUCT["id"]}  # the row without detail is ignored
    d = out[PRODUCT["id"]]
    assert d["_detail"]["update_time"] == 1792000000
    assert d["main_images"] == DETAIL["main_images"] and len(d["skus"]) == 2
    assert not detail_is_stale(PRODUCT, d)
    assert parse_product(PRODUCT, d)[0]["raw"] == raw_a[0]["raw"]


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
async def test_orders_follow_page_token(tmp_path: Path) -> None:
    calls: list[tuple[dict[str, str], dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        calls.append((params, json.loads(request.content)))
        if params.get("page_token") == "p2":
            return _ok({"orders": [{**ORDER, "id": "B"}, {**ORDER, "id": "A"}]})
        return _ok({"orders": [{**ORDER, "id": "A"}], "next_page_token": "p2"})

    with patch.object(svc_mod, "upsert", AsyncMock()) as upsert:
        stats = await TikTokSyncService(_client(tmp_path, handler)).sync_orders(updated_hours=3)

    assert [p.get("page_token") for p, _ in calls] == [None, "p2"]
    assert all(p["sort_field"] == "update_time" and p["page_size"] == "100" for p, _ in calls)
    body = calls[0][1]
    assert body["update_time_lt"] - body["update_time_ge"] == 3 * 3600
    assert stats == {"orders": 2, "mode": "update_time"}
    assert sorted(r["order_id"] for r in upsert.await_args.args[1]) == ["A", "B"]


async def test_orders_backfill_filters_by_create_time(tmp_path: Path) -> None:
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return _ok({"orders": []})

    with patch.object(svc_mod, "upsert", AsyncMock()):
        stats = await TikTokSyncService(_client(tmp_path, handler)).sync_orders(created_days=2)

    assert set(bodies[0]) == {"create_time_ge", "create_time_lt"}
    assert stats == {"orders": 0, "mode": "create_time"}


async def test_listings_skip_deleted_and_prune(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return _ok({"update_time": 1792000000})
        assert request.url.path == PRODUCTS_SEARCH
        return _ok(
            {
                "products": [
                    PRODUCT,
                    {**PRODUCT, "id": "2", "status": "DELETED"},
                    {**PRODUCT, "id": "3", "status": "FREEZE", "skus": [PRODUCT["skus"][0]]},
                ]
            }
        )

    with (
        patch.object(svc_mod, "stored_details", AsyncMock(return_value={})),
        patch.object(svc_mod, "upsert", AsyncMock()),
        patch.object(svc_mod, "prune_listings", AsyncMock(return_value=4)) as prune,
    ):
        stats = await TikTokSyncService(_client(tmp_path, handler)).sync_listings()

    assert stats == {
        "products": 2,
        "listings": 3,
        "active": 2,
        "removed": 4,
        "details_fetched": 2,
        "details_failed": 0,
    }
    assert prune.await_args.args == (
        "tiktok",
        {
            (PRODUCT["id"], "1729000000000000101"),
            (PRODUCT["id"], "1729000000000000102"),
            ("3", "1729000000000000101"),
        },
    )


async def test_empty_listings_never_prune(tmp_path: Path) -> None:
    with (
        patch.object(svc_mod, "stored_details", AsyncMock(return_value={})),
        patch.object(svc_mod, "upsert", AsyncMock()),
        patch.object(svc_mod, "prune_listings", AsyncMock()) as prune,
    ):
        await TikTokSyncService(_client(tmp_path, lambda r: _ok({"products": []}))).sync_listings()
    prune.assert_not_awaited()


async def test_listings_fetch_detail_only_for_new_or_changed_products(tmp_path: Path) -> None:
    gets: list[str] = []
    unchanged = {**PRODUCT, "id": "11"}
    changed = {**PRODUCT, "id": "22", "update_time": 1792999999}
    new = {**PRODUCT, "id": "33"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            pid = request.url.path.rsplit("/", 1)[1]
            gets.append(pid)
            if pid == "33":
                return httpx.Response(200, json={"code": 12052700, "message": "boom"})
            return _ok({**DETAIL, "id": pid, "update_time": 1792999999})
        return _ok({"products": [unchanged, changed, new]})

    stored = {"11": {**_fresh(), "id": "11"}, "22": {**_fresh(), "id": "22"}}
    with (
        patch.object(svc_mod, "stored_details", AsyncMock(return_value=stored)),
        patch.object(svc_mod, "upsert", AsyncMock()) as upsert,
        patch.object(svc_mod, "prune_listings", AsyncMock(return_value=0)),
    ):
        stats = await TikTokSyncService(_client(tmp_path, handler)).sync_listings()

    assert sorted(gets) == ["22", "33"]  # "11" reuses its stored detail
    assert stats["details_fetched"] == 1 and stats["details_failed"] == 1
    first_sku = PRODUCT["skus"][0]["id"]
    by_id = {
        r["listing_id"]: r for r in upsert.await_args.args[1] if r["variation_id"] == first_sku
    }
    assert by_id["11"]["raw"]["detail"] == stored["11"]["_detail"]
    assert by_id["22"]["raw"]["detail"]["update_time"] == 1792999999
    assert "detail" not in by_id["33"]["raw"]  # failed: stays without detail, retried next run
    assert by_id["33"]["price"] == Decimal("79.90")


async def test_job_skips_quietly_when_exported_token_expired(tmp_path: Path) -> None:
    service = TikTokSyncService(_client(tmp_path, lambda r: _ok({}), expires_in=10))

    with patch.object(jobs_mod, "tiktok_service", return_value=service):
        await jobs_mod.tiktok_sync_job(object(), "orders")  # must not raise


def test_service_is_none_without_exported_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "creds.json"
    f.write_text(json.dumps({"shopee_seller": {}}))
    monkeypatch.setattr(jobs_mod.settings, "marketplace_credentials_file", str(f))

    assert jobs_mod.tiktok_service(object()) is None
    assert jobs_mod.tiktok_service(None) is None
