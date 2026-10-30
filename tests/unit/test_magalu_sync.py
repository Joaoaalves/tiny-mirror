"""Unit tests for the Magalu integration (client + MagaluSyncService + exporter).

HTTP mocked (httpx.MockTransport), DB patched. Payload shapes follow the
official OpenAPI specs (developers.magalu.com/apis/orders|products, 2026-10-30).
"""

from __future__ import annotations

import importlib.util
import json
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from tiny_mirror.infrastructure.external import magalu_client as client_mod
from tiny_mirror.infrastructure.external.magalu_client import (
    ORDERS,
    SKUS,
    MagaluAPIError,
    MagaluClient,
    MagaluNotConfigured,
    MagaluTokenExpired,
)
from tiny_mirror.infrastructure.external.marketplace_credentials import MarketplaceCredentials
from tiny_mirror.scheduler import jobs as jobs_mod
from tiny_mirror.services import magalu_sync_service as svc_mod
from tiny_mirror.services.magalu_sync_service import (
    MAGALU_CHANNEL_ID,
    MagaluSyncService,
    cents,
    parse_order,
    parse_sku,
    stored_offers,
    strip_pii,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_waits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_mod, "_MIN_INTERVAL", 0.0)
    monkeypatch.setattr(client_mod, "_backoff", lambda attempt, retry_after: 0.0)


def _creds(tmp_path: Path, expires_in: int = 3600, section: bool = True) -> MarketplaceCredentials:
    f = tmp_path / "creds.json"
    data: dict[str, Any] = {}
    if section:
        data["magalu"] = {
            "access_token": "tok",
            "expires_at": int(time.time()) + expires_in,
            "seller_id": "offshop",
            "tenant_id": "GENPUB.x",
            "channel_id": MAGALU_CHANNEL_ID,
        }
    f.write_text(json.dumps(data))
    return MarketplaceCredentials(str(f))


def _client(tmp_path: Path, handler: Any, expires_in: int = 3600) -> MagaluClient:
    return MagaluClient(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)), _creds(tmp_path, expires_in)
    )


def _page(results: list[dict[str, Any]], nxt: str | None = None) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "meta": {"page": {"count": len(results)}, "links": {"next": nxt}},
            "results": results,
        },
    )


ORDER: dict[str, Any] = {
    "id": "3f1c2b9a-0000-4000-8000-000000000001",
    "code": "1582570110078547",
    "status": "approved",
    "channel": {"id": MAGALU_CHANNEL_ID},
    "created_at": "2026-10-30T11:58:00Z",
    "purchased_at": "2026-10-30T12:00:00Z",
    "approved_at": "2026-10-30T12:05:00Z",
    "updated_at": "2026-10-30T13:00:00Z",
    "amounts": {
        "total": 8999,
        "normalizer": 100,
        "currency": "BRL",
        "freight": {"total": 1500, "normalizer": 100},
    },
    "customer": {
        "name": "Fulano",
        "document_number": "12345678909",
        "email": "x@y.z",
        "birth_date": "1990-01-01",
        "phones": [{"number": "999"}],
    },
    "payments": [{"method": "credit_card", "amount": 8999, "normalizer": 100}],
    "deliveries": [
        {
            "code": "D1",
            "status": "approved",
            "shipping": {
                "provider": {"name": "Magalu Entregas"},
                "recipient": {"name": "Fulano", "address": {"street": "Rua X"}},
            },
            "items": [
                {
                    "quantity": 1,
                    "info": {"sku": "DGS-SPM86", "name": "Suporte"},
                    "unit_price": {"value": 8999, "normalizer": 100},
                }
            ],
        }
    ],
}

SKU: dict[str, Any] = {
    "sku": "DGS-SPM86",
    "title": "Suporte TV Fixo",
    "status": "PUBLISHED",
    "active": True,
    "fulfillment": False,
    "group": {"id": "G1", "main_variation": True},
    "url_marketplace": [
        {"channel": "magalu", "url": "https://www.magazineluiza.com.br/suporte/p/kd12345ab6/ud/"}
    ],
}
PRICES = {
    "results": [
        {"channel": {"id": "other"}, "price": 1, "normalizer": 100},
        {
            "channel": {"id": MAGALU_CHANNEL_ID},
            "price": 7990,
            "list_price": 9990,
            "normalizer": 100,
        },
    ]
}
STOCKS = {
    "results": [
        {"channel": {"id": MAGALU_CHANNEL_ID}, "quantity": 7, "type": "on_seller"},
        {"channel": {"id": MAGALU_CHANNEL_ID}, "quantity": 3, "type": "on_seller"},
    ]
}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
async def test_client_gets_with_bearer_and_returns_body(tmp_path: Path) -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["auth"] = request.headers["Authorization"]
        return _page([ORDER])

    body = await _client(tmp_path, handler).get(ORDERS, {"_limit": 1, "code": None})

    assert body["results"][0]["code"] == ORDER["code"]
    assert seen["method"] == "GET" and seen["auth"] == "Bearer tok"
    assert seen["url"] == "https://api.magalu.com/seller/v1/orders?_limit=1"


async def test_only_read_paths_are_callable(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return _page([])

    client = _client(tmp_path, handler)
    for path in ("/seller/v1/promotions/1/subscriptions", "/oauth/token", "/seller/v1/orders/../x"):
        with pytest.raises(ValueError):
            await client.get(path)
    with pytest.raises(ValueError):
        await client.get_price("bad sku with spaces")
    assert calls == []


async def test_client_never_calls_with_expired_or_missing_token(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("must not call")

    with pytest.raises(MagaluTokenExpired):
        await _client(tmp_path, handler, expires_in=10).get(ORDERS)
    empty = MagaluClient(
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        _creds(tmp_path, section=False),
    )
    with pytest.raises(MagaluNotConfigured):
        await empty.get(ORDERS)


async def test_401_means_token_refused_not_retried(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401, json={"slug": "UNAUTHORIZED", "message": "Unauthorized"})

    with pytest.raises(MagaluTokenExpired):
        await _client(tmp_path, handler).get(ORDERS)
    assert calls == 1


async def test_429_and_5xx_are_retried_then_succeed(tmp_path: Path) -> None:
    statuses = [429, 503, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        code = statuses.pop(0)
        return _page([ORDER]) if code == 200 else httpx.Response(code, json={"slug": "X"})

    body = await _client(tmp_path, handler).get(ORDERS)
    assert body["results"] and statuses == []


async def test_persistent_429_gives_up_and_4xx_carries_slug(tmp_path: Path) -> None:
    with pytest.raises(MagaluAPIError) as too_many:
        await _client(tmp_path, lambda r: httpx.Response(429, json={})).get(ORDERS)
    assert too_many.value.status == 429

    def forbidden(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"slug": "FORBIDDEN", "message": "Forbidden"})

    with pytest.raises(MagaluAPIError) as exc:
        await _client(tmp_path, forbidden).get(SKUS)
    assert exc.value.status == 403 and exc.value.slug == "FORBIDDEN"


async def test_pages_walk_offsets_until_short_page(tmp_path: Path) -> None:
    offsets: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        offset = request.url.params["_offset"]
        offsets.append(offset)
        n = 2 if offset in ("0", "2") else 1
        return _page([{"code": f"{offset}-{i}"} for i in range(n)], nxt="next")

    rows = await _client(tmp_path, handler).pages(ORDERS, {"a": "b"}, 2)
    assert offsets == ["0", "2", "4"] and len(rows) == 5


async def test_pages_stop_when_next_link_is_empty(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return _page([{"code": "1"}, {"code": "2"}], nxt=None)

    assert len(await _client(tmp_path, handler).pages(ORDERS, {}, 2)) == 2
    assert calls == 1


async def test_pages_refuse_to_pass_the_offset_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_mod, "MAX_OFFSET", 3)
    full = _page([{"code": "1"}, {"code": "2"}], nxt="x")
    with pytest.raises(MagaluAPIError) as exc:
        await _client(tmp_path, lambda r: full).pages(ORDERS, {}, 2)
    assert exc.value.slug == "OFFSET_CEILING"


async def test_sku_with_slash_is_path_encoded(tmp_path: Path) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.raw_path.decode())
        return httpx.Response(200, json={"results": []})

    await _client(tmp_path, handler).get_price("KIT/2")
    assert paths == ["/seller/v1/portfolios/prices/KIT%2F2"]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def test_cents_divides_by_normalizer() -> None:
    assert cents(8999, 100) == Decimal("89.99")
    assert cents(8999) == Decimal("89.99")
    assert cents("12345", 1000) == Decimal("12.35")
    assert cents(None) is None and cents("x") is None
    assert cents(150, 0) == Decimal("1.50")  # missing/zero normalizer = cents


def test_parse_order_uses_tiny_number_reais_and_drops_pii() -> None:
    row = parse_order(ORDER)

    assert row is not None
    assert row["channel"] == "magalu"
    assert row["order_id"] == "LU-1582570110078547"  # = orders.ecommerce_order_number
    assert row["created_at"] == datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
    assert row["last_updated"] == datetime(2026, 10, 30, 13, 0, tzinfo=UTC)
    assert row["status"] == "approved"
    assert row["total_amount"] == Decimal("89.99") and row["currency"] == "BRL"
    raw = json.dumps(row["raw"])
    for pii in ("Fulano", "12345678909", "x@y.z", "1990-01-01", "Rua X", '"999"'):
        assert pii not in raw
    assert row["raw"]["code"] == ORDER["code"] and row["raw"]["id"] == ORDER["id"]
    assert row["raw"]["deliveries"][0]["items"][0]["info"]["sku"] == "DGS-SPM86"


def test_parse_order_falls_back_to_created_at_and_rejects_incomplete() -> None:
    row = parse_order({**ORDER, "purchased_at": None})
    assert row is not None and row["created_at"] == datetime(2026, 10, 30, 11, 58, tzinfo=UTC)
    assert parse_order({**ORDER, "code": ""}) is None
    assert parse_order({"code": "1"}) is None


def test_strip_pii_is_recursive() -> None:
    assert strip_pii({"a": [{"customer": 1, "b": {"email": 2, "c": 3}}]}) == {
        "a": [{"b": {"c": 3}}]
    }


def test_parse_sku_takes_magalu_channel_price_and_sums_stock() -> None:
    row = parse_sku(SKU, {"prices": PRICES["results"], "stocks": STOCKS["results"]})

    assert row is not None
    assert row["listing_id"] == row["sku"] == "DGS-SPM86" and row["variation_id"] == ""
    assert row["external_id"] == "kd12345ab6"
    assert row["price"] == Decimal("79.90") and row["stock"] == 10
    assert row["is_active"] is True
    assert row["url"].startswith("https://www.magazineluiza.com.br/")
    assert row["raw"]["offer"]["prices"] == PRICES["results"]


def test_parse_sku_status_and_missing_offer() -> None:
    row = parse_sku({**SKU, "status": "BLOCKED", "url_marketplace": []}, None)
    assert row is not None
    assert row["is_active"] is False and row["price"] is None and row["stock"] is None
    assert row["url"] is None and row["external_id"] is None and "offer" not in row["raw"]
    inactive = parse_sku({**SKU, "active": False}, None)
    assert inactive is not None and inactive["is_active"] is False
    assert parse_sku({**SKU, "sku": ""}, None) is None


async def test_stored_offers_reads_offer_from_raw() -> None:
    rows = [("A", {"sku": {}, "offer": {"prices": [1]}}), ("B", {"sku": {}}), ("C", None)]
    session = MagicMock()
    session.execute = AsyncMock(return_value=MagicMock(all=lambda: rows))
    ctx = MagicMock(__aenter__=AsyncMock(return_value=session), __aexit__=AsyncMock())

    with patch.object(svc_mod, "AsyncSessionLocal", return_value=ctx):
        assert await stored_offers() == {"A": {"prices": [1]}}


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------
async def test_orders_incremental_filters_by_updated_at(tmp_path: Path) -> None:
    params: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params.append(dict(request.url.params))
        return _page([ORDER, {**ORDER, "code": "2"}, ORDER])

    with patch.object(svc_mod, "upsert", AsyncMock()) as upsert:
        stats = await MagaluSyncService(_client(tmp_path, handler)).sync_orders(updated_hours=3)

    p = params[0]
    gte = datetime.fromisoformat(p["updated_at__gte"])
    lte = datetime.fromisoformat(p["updated_at__lte"])
    assert (lte - gte).total_seconds() == 3 * 3600
    assert p["_limit"] == "50" and p["_offset"] == "0" and p["_sort"] == "purchased_at:asc"
    assert stats == {"orders": 2, "mode": "updated_at"}
    assert upsert.await_args.args[2] == ["channel", "order_id"]
    assert sorted(r["order_id"] for r in upsert.await_args.args[1]) == [
        "LU-1582570110078547",
        "LU-2",
    ]


async def test_orders_backfill_is_one_window_per_day(tmp_path: Path) -> None:
    params: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params.append(dict(request.url.params))
        return _page([])

    with patch.object(svc_mod, "upsert", AsyncMock()):
        stats = await MagaluSyncService(_client(tmp_path, handler)).sync_orders(created_days=3)

    assert len(params) == 3
    assert all("purchased_at__gte" in p and "updated_at__gte" not in p for p in params)
    spans = [
        datetime.fromisoformat(p["purchased_at__lte"])
        - datetime.fromisoformat(p["purchased_at__gte"])
        for p in params
    ]
    assert all(s.total_seconds() == 86400 for s in spans)
    assert stats == {"orders": 0, "mode": "purchased_at"}


async def test_listings_join_offers_skip_deleted_and_prune(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append(path)
        if path == SKUS:
            return _page(
                [
                    SKU,
                    {**SKU, "sku": "GONE", "status": "DELETED"},
                    {**SKU, "sku": "REVIEW", "status": "UNDER_REVIEW"},
                ]
            )
        if "/prices/" in path:
            return httpx.Response(200, json=PRICES)
        return httpx.Response(200, json=STOCKS)

    with (
        patch.object(svc_mod, "stored_offers", AsyncMock(return_value={})),
        patch.object(svc_mod, "upsert", AsyncMock()) as upsert,
        patch.object(svc_mod, "prune_listings", AsyncMock(return_value=1)) as prune,
    ):
        stats = await MagaluSyncService(_client(tmp_path, handler)).sync_listings()

    assert stats == {"skus": 2, "listings": 2, "active": 1, "removed": 1, "offers_failed": 0}
    assert not any("GONE" in c for c in calls)
    assert prune.await_args.args == ("magalu", {("DGS-SPM86", ""), ("REVIEW", "")})
    rows = {r["listing_id"]: r for r in upsert.await_args.args[1]}
    assert rows["REVIEW"]["price"] == Decimal("79.90") and rows["REVIEW"]["is_active"] is False


async def test_failed_offer_keeps_stored_values(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == SKUS:
            return _page([SKU, {**SKU, "sku": "NEW"}])
        return httpx.Response(404, json={"slug": "NOT_FOUND", "message": "x"})

    stored = {"DGS-SPM86": {"prices": PRICES["results"], "stocks": STOCKS["results"]}}
    with (
        patch.object(svc_mod, "stored_offers", AsyncMock(return_value=stored)),
        patch.object(svc_mod, "upsert", AsyncMock()) as upsert,
        patch.object(svc_mod, "prune_listings", AsyncMock(return_value=0)),
    ):
        stats = await MagaluSyncService(_client(tmp_path, handler)).sync_listings()

    assert stats["offers_failed"] == 2
    rows = {r["listing_id"]: r for r in upsert.await_args.args[1]}
    assert rows["DGS-SPM86"]["price"] == Decimal("79.90") and rows["DGS-SPM86"]["stock"] == 10
    assert rows["NEW"]["price"] is None and rows["NEW"]["stock"] is None


async def test_empty_listings_never_prune(tmp_path: Path) -> None:
    with (
        patch.object(svc_mod, "stored_offers", AsyncMock(return_value={})),
        patch.object(svc_mod, "upsert", AsyncMock()),
        patch.object(svc_mod, "prune_listings", AsyncMock()) as prune,
    ):
        await MagaluSyncService(_client(tmp_path, lambda r: _page([]))).sync_listings()
    prune.assert_not_awaited()


async def test_job_skips_quietly_when_exported_token_expired(tmp_path: Path) -> None:
    service = MagaluSyncService(_client(tmp_path, lambda r: _page([]), expires_in=10))

    with patch.object(jobs_mod, "magalu_service", return_value=service):
        await jobs_mod.magalu_sync_job(object(), "orders")  # must not raise
        await jobs_mod.magalu_sync_job(object(), "listings")


def test_service_is_none_without_exported_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    f = tmp_path / "creds.json"
    f.write_text(json.dumps({"tiktok_shop": {}}))
    monkeypatch.setattr(jobs_mod.settings, "marketplace_credentials_file", str(f))

    assert jobs_mod.magalu_service(object()) is None
    assert jobs_mod.magalu_service(None) is None
    f.write_text(json.dumps({"magalu": {"access_token": "t", "expires_at": 1}}))
    assert isinstance(jobs_mod.magalu_service(object()), MagaluSyncService)


# ---------------------------------------------------------------------------
# Credentials exporter (deploy/export_marketplace_credentials.py)
# ---------------------------------------------------------------------------
def _load_exporter() -> Any:
    path = Path(__file__).resolve().parents[2] / "deploy" / "export_marketplace_credentials.py"
    spec = importlib.util.spec_from_file_location("export_mp_creds_magalu", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_sources(tmp_path: Path, magalu: dict[str, Any] | None) -> None:
    (tmp_path / "accounts.json").write_text(json.dumps({}))
    (tmp_path / "shopee.json").write_text(json.dumps({}))
    if magalu is not None:
        (tmp_path / "magalu.json").write_text(json.dumps(magalu))


MAGALU_CONF: dict[str, Any] = {
    "client_id": "cid",
    "client_secret": "never-exported-secret",
    "redirect_uri": "https://lojaoffshop.com.br/magalu/callback",
    "tenant_id": "GENPUB.conf",
    "seller_id": "offshop",
    "channel_id": MAGALU_CHANNEL_ID,
    "status": "authorized",
    "auth": {
        "access_token": "mg-token",
        "refresh_token": "never-exported-refresh",
        "expires_at": 1_793_000_000_000,
        "scope": ["open:order-order-seller:read"],
    },
    "seller": {"id": "offshop", "tenant_id": "GENPUB.live", "channel_id": MAGALU_CHANNEL_ID},
}


def test_exporter_copies_magalu_token_without_refresh_token_or_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exporter = _load_exporter()
    _write_sources(tmp_path, MAGALU_CONF)
    monkeypatch.setattr(exporter, "SRC", str(tmp_path))

    out = exporter.build()

    assert out["magalu"] == {
        "access_token": "mg-token",
        "expires_at": 1_793_000_000,
        "seller_id": "offshop",
        "tenant_id": "GENPUB.live",
        "channel_id": MAGALU_CHANNEL_ID,
    }
    dumped = json.dumps(out)
    assert "never-exported" not in dumped


@pytest.mark.parametrize(
    "conf",
    [None, {**MAGALU_CONF, "auth": {}}, {k: v for k, v in MAGALU_CONF.items() if k != "auth"}],
)
def test_exporter_skips_magalu_until_authorized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conf: dict[str, Any] | None
) -> None:
    exporter = _load_exporter()
    _write_sources(tmp_path, conf)
    monkeypatch.setattr(exporter, "SRC", str(tmp_path))

    assert "magalu" not in exporter.build()
