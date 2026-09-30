"""Unit tests for the Amazon integration (credentials reader, SP-API client,
AmazonSyncService) and the OpenClaw credentials exporter.

HTTP is mocked (httpx.MockTransport), DB patched. Payload shapes mirror live
SP-API responses read on 2026-09-30 (orders 2026-01-01, listings 2021-08-01).
"""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tiny_mirror.infrastructure.external import amazon_spapi_client as client_mod
from tiny_mirror.infrastructure.external.amazon_spapi_client import (
    AmazonNotConfigured,
    AmazonSPAPIClient,
)
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.services import amazon_sync_service as svc_mod
from tiny_mirror.services.amazon_sync_service import (
    AmazonSyncService,
    parse_listing,
    parse_order,
)

pytestmark = pytest.mark.unit

CREDS = {
    "amazon_spapi": {
        "client_id": "cid",
        "client_secret": "sec",
        "refresh_token": "rt",
        "seller_id": "SELLER",
        "marketplace_id": "A2Q3Y263D00KWC",
    }
}

ORDER = {
    "orderId": "701-1260995-4070629",
    "createdTime": "2026-09-29T10:13:51.370Z",
    "lastUpdatedTime": "2026-09-29T12:00:00Z",
    "salesChannel": {"marketplaceId": "A2Q3Y263D00KWC", "channelName": "AMAZON"},
    "fulfillment": {"fulfillmentStatus": "SHIPPED", "fulfilledBy": "MERCHANT"},
    "proceeds": {"grandTotal": {"amount": "56.90", "currencyCode": "BRL"}},
}

LISTING = {
    "sku": "5U-NIT-PT-RSP-TRV-500-N",
    "summaries": [
        {
            "asin": "B0GWNK16DD",
            "status": ["DISCOVERABLE", "BUYABLE"],
            "itemName": "Kit 1, 5, 7 ou 10 Potes Marmita Hermética",
        }
    ],
    "offers": [{"price": {"amount": "56.9"}}],
    "fulfillmentAvailability": [{"fulfillmentChannelCode": "DEFAULT", "quantity": 27}],
}


def _creds_file(tmp_path: Path, data: dict[str, Any] = CREDS) -> MarketplaceCredentials:
    f = tmp_path / "creds.json"
    f.write_text(json.dumps(data))
    return MarketplaceCredentials(str(f))


# ---------------------------------------------------------------------------
# MarketplaceCredentials
# ---------------------------------------------------------------------------
def test_credentials_reload_when_file_changes(tmp_path: Path) -> None:
    creds = _creds_file(tmp_path, {"amazon_spapi": {"seller_id": "A"}})
    assert creds.section("amazon_spapi") == {"seller_id": "A"}

    f = tmp_path / "creds.json"
    f.write_text(json.dumps({"amazon_spapi": {"seller_id": "B"}}))
    os.utime(f, (1, 1))  # force a different mtime

    assert creds.section("amazon_spapi") == {"seller_id": "B"}


def test_credentials_missing_file_or_section_is_none(tmp_path: Path) -> None:
    assert MarketplaceCredentials(str(tmp_path / "nope.json")).section("amazon_spapi") is None
    assert MarketplaceCredentials("").section("amazon_spapi") is None
    assert _creds_file(tmp_path, {}).section("amazon_spapi") is None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_parse_order_keeps_exact_time_status_and_total() -> None:
    row = parse_order(ORDER)

    assert row is not None
    assert row["channel"] == "amazon"
    assert row["order_id"] == "701-1260995-4070629"
    assert row["created_at"] == datetime(2026, 9, 29, 10, 13, 51, 370000, tzinfo=UTC)
    assert (row["status"], row["fulfilled_by"]) == ("SHIPPED", "MERCHANT")
    assert (row["total_amount"], row["currency"]) == (Decimal("56.90"), "BRL")


def test_parse_order_without_time_is_skipped() -> None:
    assert parse_order({"orderId": "1"}) is None


def test_parse_listing_active_when_buyable() -> None:
    row = parse_listing(LISTING)

    assert row is not None
    assert row["listing_id"] == row["sku"] == "5U-NIT-PT-RSP-TRV-500-N"
    assert row["variation_id"] == ""
    assert row["external_id"] == "B0GWNK16DD"
    assert row["is_active"] is True
    assert row["price"] == Decimal("56.90")
    assert row["stock"] == 27
    assert row["url"] == "https://www.amazon.com.br/dp/B0GWNK16DD"


def test_parse_listing_discoverable_only_is_inactive() -> None:
    raw = {**LISTING, "summaries": [{"asin": "X", "status": ["DISCOVERABLE"]}], "offers": []}
    row = parse_listing(raw)

    assert row is not None and row["is_active"] is False and row["price"] is None


# ---------------------------------------------------------------------------
# SP-API client
# ---------------------------------------------------------------------------
def _client(tmp_path: Path, handler: Any) -> AmazonSPAPIClient:
    return AmazonSPAPIClient(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)), _creds_file(tmp_path)
    )


async def test_client_caches_lwa_token_and_refreshes_on_401(tmp_path: Path) -> None:
    lwa_calls: list[int] = []
    api_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.amazon.com":
            lwa_calls.append(1)
            return httpx.Response(
                200, json={"access_token": f"t{len(lwa_calls)}", "expires_in": 3600}
            )
        api_calls.append(request.headers["x-amz-access-token"])
        if request.headers["x-amz-access-token"] == "t1" and len(api_calls) == 2:
            return httpx.Response(401)
        return httpx.Response(200, json={"ok": True})

    client = _client(tmp_path, handler)
    await client.get("/a")
    await client.get("/b")

    assert api_calls == ["t1", "t1", "t2"]  # cached, then refreshed once on 401
    assert len(lwa_calls) == 2


async def test_client_retries_429(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(client_mod.asyncio, "sleep", fake_sleep)
    answers = iter([429, 200])

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.amazon.com":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 3600})
        return httpx.Response(next(answers), json={"ok": True}, headers={"Retry-After": "2"})

    assert await _client(tmp_path, handler).get("/a") == {"ok": True}
    assert sleeps == [2.0]


def test_client_without_credentials_raises(tmp_path: Path) -> None:
    client = AmazonSPAPIClient(httpx.AsyncClient(), MarketplaceCredentials(str(tmp_path / "x")))

    with pytest.raises(AmazonNotConfigured):
        client.credentials()


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
async def test_orders_pagination_repeats_filters_and_included_data(tmp_path: Path) -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.amazon.com":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 3600})
        seen.append(dict(request.url.params))
        if "paginationToken" not in request.url.params:
            return httpx.Response(200, json={"orders": [ORDER], "pagination": {"nextToken": "N1"}})
        return httpx.Response(
            200, json={"orders": [{**ORDER, "orderId": "702-1"}], "pagination": {}}
        )

    service = AmazonSyncService(_client(tmp_path, handler))
    with patch.object(svc_mod, "_upsert", AsyncMock()) as upsert:
        stats = await service.sync_orders(updated_hours=3)

    assert stats == {"orders": 2, "mode": "updated"}
    assert "lastUpdatedAfter" in seen[0] and "createdAfter" not in seen[0]
    assert seen[1]["paginationToken"] == "N1"
    assert seen[1]["includedData"] == seen[0]["includedData"] == "FULFILLMENT,PROCEEDS,CANCELLATION"
    assert seen[1]["lastUpdatedAfter"] == seen[0]["lastUpdatedAfter"]
    assert len(upsert.await_args.args[1]) == 2


async def test_backfill_uses_created_after(tmp_path: Path) -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.amazon.com":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 3600})
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={"orders": [], "pagination": {}})

    with patch.object(svc_mod, "_upsert", AsyncMock()):
        stats = await AmazonSyncService(_client(tmp_path, handler)).sync_orders(created_days=90)

    assert "createdAfter" in seen[0] and "lastUpdatedAfter" not in seen[0]
    assert stats["mode"] == "created"


async def test_listings_full_pass_prunes_missing_skus(tmp_path: Path) -> None:
    pages = iter(
        [
            {"items": [LISTING], "pagination": {"nextToken": "P2"}},
            {"items": [{**LISTING, "sku": "OTHER"}], "pagination": {}},
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.amazon.com":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 3600})
        assert request.url.path == "/listings/2021-08-01/items/SELLER"
        return httpx.Response(200, json=next(pages))

    with (
        patch.object(svc_mod, "_upsert", AsyncMock()),
        patch.object(svc_mod, "_prune_listings", AsyncMock(return_value=4)) as prune,
    ):
        stats = await AmazonSyncService(_client(tmp_path, handler)).sync_listings()

    assert stats == {"listings": 2, "active": 2, "removed": 4, "complete": True}
    assert prune.await_args.args[0] == {("5U-NIT-PT-RSP-TRV-500-N", ""), ("OTHER", "")}


# ---------------------------------------------------------------------------
# Credentials exporter (deploy/export_marketplace_credentials.py)
# ---------------------------------------------------------------------------
def _load_exporter() -> Any:
    path = Path(__file__).resolve().parents[2] / "deploy" / "export_marketplace_credentials.py"
    spec = importlib.util.spec_from_file_location("export_mp_creds", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exporter_copies_only_what_tiny_mirror_needs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exporter = _load_exporter()
    (tmp_path / "accounts.json").write_text(
        json.dumps({**CREDS, "amazon_ads": {"client_id": "ads"}, "tiktok_ads": {"secret": "x"}})
    )
    (tmp_path / "shopee.json").write_text(
        json.dumps(
            {
                "apps": {
                    "seller": {"partner_id": 2032218, "partner_key": "pk", "status": "live"},
                    "ads": {"partner_id": 2032307, "partner_key": "ak"},
                },
                "shops": {
                    "ads:1157287748": {
                        "shop_id": 1157287748,
                        "access_token": "ads-token",
                        "expires_at": 1,
                    },
                    "seller:1157287748": {
                        "shop_id": 1157287748,
                        "access_token": "seller-token",
                        "refresh_token": "never-exported",
                        "expires_at": 1_759_250_000_000,
                    },
                },
            }
        )
    )
    monkeypatch.setattr(exporter, "SRC", str(tmp_path))

    out = exporter.build()

    assert out["amazon_spapi"] == CREDS["amazon_spapi"]
    assert out["shopee_seller"] == {
        "partner_id": 2032218,
        "partner_key": "pk",
        "shop_id": 1157287748,
        "access_token": "seller-token",
        "expires_at": 1_759_250_000,
    }
    assert "refresh_token" not in json.dumps(out["shopee_seller"])
    assert "amazon_ads" not in out and "tiktok_ads" not in out
