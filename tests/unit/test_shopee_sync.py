"""Unit tests for the Shopee integration (client + ShopeeSyncService).

HTTP mocked (httpx.MockTransport), DB patched. Payload shapes mirror live
Shopee v2 responses read on 2026-09-30.
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
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tiny_mirror.infrastructure.external import shopee_client as client_mod
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.infrastructure.external.shopee_client import (
    ShopeeAPIError,
    ShopeeClient,
    ShopeeTokenExpired,
    signature,
)
from tiny_mirror.scheduler import jobs as jobs_mod
from tiny_mirror.services import shopee_sync_service as svc_mod
from tiny_mirror.services.shopee_sync_service import (
    ShopeeSyncService,
    _windows,
    parse_item,
    parse_models,
    parse_order,
)

pytestmark = pytest.mark.unit

SHOP = 1157287748


def _creds(tmp_path: Path, expires_in: int = 3600) -> MarketplaceCredentials:
    f = tmp_path / "creds.json"
    f.write_text(
        json.dumps(
            {
                "shopee_seller": {
                    "partner_id": 2032218,
                    "partner_key": "pk",
                    "shop_id": SHOP,
                    "access_token": "tok",
                    "expires_at": int(time.time()) + expires_in,
                }
            }
        )
    )
    return MarketplaceCredentials(str(f))


def _client(tmp_path: Path, handler: Any, expires_in: int = 3600) -> ShopeeClient:
    return ShopeeClient(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)), _creds(tmp_path, expires_in)
    )


ORDER = {
    "order_sn": "261001E681V7W8",
    "create_time": 1790788550,
    "update_time": 1790788738,
    "order_status": "PROCESSED",
    "total_amount": 140.22,
    "currency": "BRL",
    "fulfillment_flag": "fulfilled_by_local_seller",
    "cancel_reason": "",
}

ITEM = {
    "item_id": 58214144371,
    "item_sku": "DGS-SPM86",
    "item_status": "NORMAL",
    "item_name": "Suporte TV Fixo",
    "has_model": False,
    "description": "long text that must not be stored",
    "price_info": [{"currency": "BRL", "original_price": 133.17, "current_price": 79.91}],
    "stock_info_v2": {"summary_info": {"total_reserved_stock": 1, "total_available_stock": 161}},
}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
def test_signature_matches_shopee_v2_formula() -> None:
    expected = hmac.new(
        b"pk", f"2032218/api/v2/x1700000000tok{SHOP}".encode(), hashlib.sha256
    ).hexdigest()

    assert signature("pk", 2032218, "/api/v2/x", 1700000000, "tok", SHOP) == expected


async def test_client_signs_and_returns_response(tmp_path: Path) -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(dict(request.url.params))
        return httpx.Response(200, json={"error": "", "response": {"ok": 1}})

    assert await _client(tmp_path, handler).get("/api/v2/a", {"page_size": 1}) == {"ok": 1}
    assert seen["shop_id"] == str(SHOP) and seen["access_token"] == "tok"
    assert seen["sign"] == signature(
        "pk", 2032218, "/api/v2/a", int(seen["timestamp"]), "tok", SHOP
    )


async def test_expired_exported_token_is_never_refreshed(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(200, json={})

    with pytest.raises(ShopeeTokenExpired):
        await _client(tmp_path, handler, expires_in=10).get("/api/v2/a", {})
    assert calls == []  # no auth call, no API call


async def test_api_error_is_raised(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "error_permission", "message": "no"})

    with pytest.raises(ShopeeAPIError) as exc:
        await _client(tmp_path, handler).get("/api/v2/a", {})
    assert exc.value.error == "error_permission"


async def test_rate_limit_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(client_mod.asyncio, "sleep", fake_sleep)
    answers = iter(
        [
            httpx.Response(429, json={"error": "error_rate_limit"}),
            httpx.Response(200, json={"response": {"ok": 1}}),
        ]
    )

    assert await _client(tmp_path, lambda r: next(answers)).get("/api/v2/a", {}) == {"ok": 1}
    assert len(sleeps) == 1


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_windows_cover_the_range_without_gaps_or_exceeding_the_cap() -> None:
    end = datetime(2026, 9, 30, tzinfo=UTC)
    for days in (1, 14, 15, 90, 91):
        windows = _windows(end - timedelta(days=days), end)
        assert windows[0][0] == end - timedelta(days=days) and windows[-1][1] == end
        assert all(b - a <= timedelta(days=14) for a, b in windows)
        assert all(windows[i][1] == windows[i + 1][0] for i in range(len(windows) - 1))


def test_parse_order_converts_unix_time() -> None:
    row = parse_order(ORDER)

    assert row is not None
    assert row["order_id"] == "261001E681V7W8"
    assert row["created_at"] == datetime.fromtimestamp(1790788550, tz=UTC)
    assert (row["status"], row["total_amount"]) == ("PROCESSED", Decimal("140.22"))
    assert row["fulfilled_by"] == "fulfilled_by_local_seller"


def test_parse_item_without_models() -> None:
    row = parse_item(ITEM, SHOP)

    assert (row["listing_id"], row["variation_id"], row["sku"]) == ("58214144371", "", "DGS-SPM86")
    assert (row["price"], row["stock"], row["is_active"]) == (Decimal("79.91"), 161, True)
    assert row["url"] == f"https://shopee.com.br/product/{SHOP}/58214144371"
    assert "description" not in row["raw"]["item"]


def test_parse_models_one_row_per_variation() -> None:
    item = {**ITEM, "has_model": True, "item_name": "Vaporizador"}
    models = [
        {
            "model_id": 149554737711,
            "model_sku": "INT-TOP-VAPORCL-220V",
            "model_name": "220V",
            "model_status": "MODEL_NORMAL",
            "price_info": [{"current_price": 365.91}],
            "stock_info_v2": {"summary_info": {"total_available_stock": 12}},
        },
        {"model_id": 2, "model_sku": "X-110V", "model_status": "MODEL_UNAVAILABLE"},
    ]

    rows = parse_models(item, models, SHOP)

    assert [(r["variation_id"], r["sku"], r["is_active"]) for r in rows] == [
        ("149554737711", "INT-TOP-VAPORCL-220V", True),
        ("2", "X-110V", False),
    ]
    assert rows[0]["title"] == "Vaporizador - 220V"


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
async def test_orders_follow_cursor_and_batch_details(tmp_path: Path) -> None:
    sns = [f"SN{i:03d}" for i in range(120)]
    calls: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        calls.append((request.url.path, params))
        if request.url.path.endswith("get_order_list"):
            start = int(params["cursor"] or 0)
            page = sns[start : start + 100]
            more = start + 100 < len(sns)
            return httpx.Response(
                200,
                json={
                    "response": {
                        "order_list": [{"order_sn": s} for s in page],
                        "more": more,
                        "next_cursor": str(start + 100),
                    }
                },
            )
        batch = params["order_sn_list"].split(",")
        return httpx.Response(
            200, json={"response": {"order_list": [{**ORDER, "order_sn": s} for s in batch]}}
        )

    with patch.object(svc_mod, "upsert", AsyncMock()) as upsert:
        stats = await ShopeeSyncService(_client(tmp_path, handler)).sync_orders(updated_hours=3)

    list_calls = [p for path, p in calls if path.endswith("get_order_list")]
    detail_calls = [p for path, p in calls if path.endswith("get_order_detail")]
    assert [p["time_range_field"] for p in list_calls] == ["update_time", "update_time"]
    assert [len(p["order_sn_list"].split(",")) for p in detail_calls] == [50, 50, 20]
    assert stats == {"orders": 120, "mode": "update_time"}
    assert len(upsert.await_args.args[1]) == 120


async def test_listings_expand_models_and_prune(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path, params = request.url.path, dict(request.url.params)
        if path.endswith("get_item_list"):
            items = [{"item_id": 1}, {"item_id": 2}] if params["item_status"] == "NORMAL" else []
            return httpx.Response(200, json={"response": {"item": items, "has_next_page": False}})
        if path.endswith("get_item_base_info"):
            return httpx.Response(
                200,
                json={
                    "response": {
                        "item_list": [
                            {**ITEM, "item_id": 1},
                            {**ITEM, "item_id": 2, "has_model": True},
                        ]
                    }
                },
            )
        return httpx.Response(
            200,
            json={
                "response": {
                    "model": [
                        {"model_id": 10, "model_sku": "A"},
                        {"model_id": 11, "model_sku": "B"},
                    ]
                }
            },
        )

    with (
        patch.object(svc_mod, "upsert", AsyncMock()),
        patch.object(svc_mod, "prune_listings", AsyncMock(return_value=3)) as prune,
    ):
        stats = await ShopeeSyncService(_client(tmp_path, handler)).sync_listings()

    assert stats == {"items": 2, "listings": 3, "active": 3, "removed": 3}
    assert prune.await_args.args == ("shopee", {("1", ""), ("2", "10"), ("2", "11")})


async def test_job_skips_quietly_when_exported_token_expired(tmp_path: Path) -> None:
    service = ShopeeSyncService(_client(tmp_path, lambda r: httpx.Response(200), expires_in=10))

    with patch.object(jobs_mod, "shopee_service", return_value=service):
        await jobs_mod.shopee_sync_job(object(), "orders")  # must not raise
