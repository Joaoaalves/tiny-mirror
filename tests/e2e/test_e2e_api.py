"""End-to-end coverage for stage 10 — REST API surface.

The tests boot a fresh FastAPI app per test (without the lifespan, since
the live_* fixtures already initialize the database, redis and rabbitmq)
and drive it through ``httpx.AsyncClient`` over an ASGITransport. The
shared ``http_client`` and ``queue_publisher`` that the lifespan would
populate on ``app.state`` are filled in here from the live fixtures.

We exercise the real handlers, real Postgres, real Redis and real
RabbitMQ — only the lifespan boot is bypassed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import delete

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import (
    OrderItemORM,
    OrderORM,
    ProductORM,
    SyncLogORM,
)
from tiny_mirror.main import create_app
from tiny_mirror.queue.publisher import QueuePublisher

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture
async def http_client(
    live_db: None,
    live_redis: None,
    live_rabbitmq: QueuePublisher,
    live_http_client: httpx.AsyncClient,
) -> AsyncIterator[httpx.AsyncClient]:
    """Serve the real app over ASGITransport, with app.state populated
    from the live infrastructure fixtures (no lifespan run)."""
    app = create_app()
    app.state.http_client = live_http_client
    app.state.queue_publisher = live_rabbitmq

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------
SENTINEL_PRODUCT_ID = 90100001
SENTINEL_ORDER_ID = 90100100


@pytest_asyncio.fixture
async def seeded_product() -> AsyncIterator[None]:
    async with AsyncSessionLocal() as session:
        session.add(
            ProductORM(
                tiny_id=SENTINEL_PRODUCT_ID,
                sku="API-TEST-1",
                description="API-Test product",
                type="P",
                situation="A",
                synced_at=datetime.now(UTC),
                prices={"price": 10.0, "promotional_price": None},
                unit="UN",
            )
        )
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(ProductORM).where(ProductORM.tiny_id == SENTINEL_PRODUCT_ID)
        )
        await session.commit()


@pytest_asyncio.fixture
async def seeded_order() -> AsyncIterator[None]:
    async with AsyncSessionLocal() as session:
        session.add(
            ProductORM(
                tiny_id=SENTINEL_PRODUCT_ID,
                sku="API-TEST-1",
                description="API-Test product",
                type="P",
                situation="A",
                synced_at=datetime.now(UTC),
                prices={},
            )
        )
        session.add(
            OrderORM(
                tiny_id=SENTINEL_ORDER_ID,
                order_number=SENTINEL_ORDER_ID,
                customer={"name": "API Test"},
                situation=3,
                order_date=datetime.now(UTC).date(),
                ecommerce_name="API-TEST-CHANNEL",
                synced_at=datetime.now(UTC),
            )
        )
        await session.flush()
        session.add(
            OrderItemORM(
                order_tiny_id=SENTINEL_ORDER_ID,
                product_tiny_id=SENTINEL_PRODUCT_ID,
                product_sku="API-TEST-1",
                product_type="P",
                product_description="API-Test product",
                quantity=Decimal("2"),
                unit_value=Decimal("10"),
            )
        )
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(OrderORM).where(OrderORM.tiny_id == SENTINEL_ORDER_ID)
        )
        await session.execute(
            delete(ProductORM).where(ProductORM.tiny_id == SENTINEL_PRODUCT_ID)
        )
        await session.commit()


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
async def test_health_returns_ok_when_all_components_up(
    http_client: httpx.AsyncClient,
) -> None:
    response = await http_client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert "components" in body
    assert body["components"]["database"] == "ok"
    assert body["components"]["redis"] == "ok"
    assert body["components"]["rabbitmq"] == "ok"


async def test_request_id_header_is_set_on_every_response(
    http_client: httpx.AsyncClient,
) -> None:
    response = await http_client.get("/health")
    assert "x-request-id" in {h.lower() for h in response.headers}


async def test_request_id_is_propagated_when_supplied_by_client(
    http_client: httpx.AsyncClient,
) -> None:
    response = await http_client.get(
        "/health", headers={"X-Request-Id": "test-fixed-id"}
    )
    assert response.headers.get("X-Request-Id") == "test-fixed-id"


# ---------------------------------------------------------------------------
# /products
# ---------------------------------------------------------------------------
async def test_products_list_returns_pagination_envelope(
    http_client: httpx.AsyncClient, seeded_product: None
) -> None:
    response = await http_client.get(
        "/products", params={"sku": "API-TEST", "page_size": 10}
    )
    assert response.status_code == 200
    body = response.json()
    assert "items" in body and "pagination" in body
    assert body["pagination"]["total"] >= 1
    skus = [item["sku"] for item in body["items"]]
    assert "API-TEST-1" in skus


async def test_products_list_rejects_page_size_above_100(
    http_client: httpx.AsyncClient,
) -> None:
    response = await http_client.get("/products", params={"page_size": 200})
    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "validation_error"


async def test_products_list_filters_by_situation(
    http_client: httpx.AsyncClient, seeded_product: None
) -> None:
    # situation=I — our seeded product is 'A', so it must NOT show up.
    response = await http_client.get(
        "/products", params={"situation": "I", "page_size": 100}
    )
    assert response.status_code == 200
    skus = [item["sku"] for item in response.json()["items"]]
    assert "API-TEST-1" not in skus


async def test_product_detail_returns_kit_components_and_buckets(
    http_client: httpx.AsyncClient, seeded_product: None
) -> None:
    response = await http_client.get(f"/products/{SENTINEL_PRODUCT_ID}")
    assert response.status_code == 200
    body = response.json()
    assert body["tiny_id"] == SENTINEL_PRODUCT_ID
    assert body["sku"] == "API-TEST-1"
    assert isinstance(body["kit_components"], list)
    assert isinstance(body["sale_buckets_90d"], list)


async def test_product_detail_returns_404_for_unknown_id(
    http_client: httpx.AsyncClient,
) -> None:
    response = await http_client.get("/products/9999999999")
    assert response.status_code == 404
    body = response.json()
    assert body["error"] == "http_error" or body["error"] == "not_found"


# ---------------------------------------------------------------------------
# /orders
# ---------------------------------------------------------------------------
async def test_orders_list_returns_pagination_envelope(
    http_client: httpx.AsyncClient, seeded_order: None
) -> None:
    response = await http_client.get(
        "/orders",
        params={"ecommerce_name": "API-TEST", "page_size": 10},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["pagination"]["total"] >= 1
    numbers = [o["order_number"] for o in body["items"]]
    assert SENTINEL_ORDER_ID in numbers


async def test_orders_list_does_not_include_items(
    http_client: httpx.AsyncClient, seeded_order: None
) -> None:
    response = await http_client.get(
        "/orders", params={"ecommerce_name": "API-TEST"}
    )
    body = response.json()
    assert body["items"], "fixture should produce at least one order"
    # OrderListItem schema must not expose the items array.
    assert "items" in body, "wrapper key"
    for o in body["items"]:
        assert "items" not in o or o.get("items") is None or o["items"] == []


async def test_orders_detail_includes_items(
    http_client: httpx.AsyncClient, seeded_order: None
) -> None:
    response = await http_client.get(f"/orders/{SENTINEL_ORDER_ID}")
    assert response.status_code == 200
    body = response.json()
    assert body["tiny_id"] == SENTINEL_ORDER_ID
    assert isinstance(body["items"], list) and len(body["items"]) == 1
    item = body["items"][0]
    assert item["product_sku"] == "API-TEST-1"
    assert item["quantity"] == 2.0
    assert item["unit_value"] == 10.0


async def test_orders_detail_404_for_unknown_id(
    http_client: httpx.AsyncClient,
) -> None:
    response = await http_client.get("/orders/9999999999")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# /sync/*
# ---------------------------------------------------------------------------
async def test_sync_products_returns_202_and_publishes_message(
    http_client: httpx.AsyncClient,
) -> None:
    from tiny_mirror.rabbitmq import get_channel

    channel = get_channel()
    queue = await channel.get_queue("tiny.sync.products.full")
    while True:
        leftover = await queue.get(no_ack=True, fail=False)
        if leftover is None:
            break

    response = await http_client.post("/sync/products")
    assert response.status_code == 202
    body = response.json()
    assert body["sync_log_id"] > 0
    assert "Product sync triggered" in body["message"]

    msg = await queue.get(no_ack=True, fail=False)
    assert msg is not None, "sync trigger must publish a message"
    import json

    payload = json.loads(msg.body.decode("utf-8"))
    assert payload["sync_log_id"] == body["sync_log_id"]
    assert payload["triggered_by"] == "manual"


async def test_sync_orders_validates_date_range(
    http_client: httpx.AsyncClient,
) -> None:
    # date_from without date_to -> 422
    response = await http_client.post(
        "/sync/orders", json={"date_from": "2025-01-01"}
    )
    assert response.status_code == 422

    # date_from > date_to -> 422
    response = await http_client.post(
        "/sync/orders",
        json={"date_from": "2025-02-01", "date_to": "2025-01-01"},
    )
    assert response.status_code == 422


async def test_sync_orders_with_date_range_publishes_historical_message(
    http_client: httpx.AsyncClient,
) -> None:
    from tiny_mirror.rabbitmq import get_channel

    channel = get_channel()
    queue = await channel.get_queue("tiny.sync.orders.full")
    while True:
        leftover = await queue.get(no_ack=True, fail=False)
        if leftover is None:
            break

    response = await http_client.post(
        "/sync/orders",
        json={"date_from": "2025-01-01", "date_to": "2025-01-07"},
    )
    assert response.status_code == 202
    body = response.json()

    msg = await queue.get(no_ack=True, fail=False)
    assert msg is not None
    import json

    payload = json.loads(msg.body.decode("utf-8"))
    assert payload["is_historical"] is True
    assert payload["date_from"] == "2025-01-01"
    assert payload["date_to"] == "2025-01-07"
    assert payload["sync_log_id"] == body["sync_log_id"]


async def test_sync_stock_returns_202(http_client: httpx.AsyncClient) -> None:
    response = await http_client.post("/sync/stock")
    assert response.status_code == 202
    assert response.json()["sync_log_id"] > 0


# ---------------------------------------------------------------------------
# /sync/logs
# ---------------------------------------------------------------------------
async def test_sync_logs_filters_by_status(
    http_client: httpx.AsyncClient,
) -> None:
    # Create one running and one failed log so the filter has something to
    # discriminate.
    async with AsyncSessionLocal() as session:
        session.add(
            SyncLogORM(
                sync_type="products",
                status="failed",
                items_processed=0,
                items_failed=1,
                error_message="api-test",
            )
        )
        session.add(
            SyncLogORM(
                sync_type="products",
                status="running",
                items_processed=0,
                items_failed=0,
            )
        )
        await session.commit()

    try:
        response = await http_client.get("/sync/logs", params={"status": "failed"})
        assert response.status_code == 200
        body = response.json()
        assert body["pagination"]["total"] >= 1
        for item in body["items"]:
            assert item["status"] == "failed"
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(SyncLogORM).where(SyncLogORM.error_message == "api-test")
            )
            await session.execute(
                delete(SyncLogORM).where(
                    (SyncLogORM.status == "running")
                    & (SyncLogORM.items_processed == 0)
                    & (SyncLogORM.items_failed == 0)
                )
            )
            await session.commit()
