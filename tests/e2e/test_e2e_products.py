"""End-to-end coverage for stage 04 (products endpoints) and stage 06
(product sync into Postgres).

Tests assume:
- live Postgres / Redis / RabbitMQ via docker-compose
- a working refresh token in .env (TokenService bootstraps automatically)
- ``E2E_TINY_TEST_PRODUCT_ID`` set to a known product id
- ``E2E_TINY_TEST_KIT_ID`` set to a known tipo=K product (kit tests skip
  individually if not provided)
"""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyNotFoundException
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.orm.models import ProductORM, SyncLogORM
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.rabbitmq import get_channel
from tiny_mirror.services.product_sync_service import ProductSyncService

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Stage 04 — TinyAPIClient products endpoints
# ---------------------------------------------------------------------------
async def test_list_products_returns_valid_structure(
    live_tiny_client: TinyAPIClient,
) -> None:
    response = await live_tiny_client.list_products(situation="A", limit=1)

    assert "itens" in response and isinstance(response["itens"], list)
    assert "paginacao" in response
    assert response["paginacao"].get("total", 0) >= 1, (
        "expected at least one active product in the live Tiny account"
    )

    item = response["itens"][0]
    for required in ("id", "sku", "descricao", "tipo", "situacao"):
        assert required in item, f"missing field {required!r} in list item"


async def test_get_product_returns_complete_detail(
    live_tiny_client: TinyAPIClient,
    e2e_product_id: int,
) -> None:
    detail = await live_tiny_client.get_product(e2e_product_id)

    for required in ("id", "sku", "descricao", "tipo", "situacao", "precos"):
        assert required in detail, f"missing field {required!r} in detail"
    assert int(detail["id"]) == e2e_product_id


async def test_get_kit_product_has_kit_array(
    live_tiny_client: TinyAPIClient,
    e2e_kit_id: int,
) -> None:
    detail = await live_tiny_client.get_product(e2e_kit_id)

    assert detail["tipo"] == "K", "fixture E2E_TINY_TEST_KIT_ID must point at a kit"
    assert isinstance(detail.get("kit"), list)
    assert len(detail["kit"]) >= 1
    first = detail["kit"][0]
    assert "produto" in first and "quantidade" in first


async def test_get_product_unknown_id_raises_not_found(
    live_tiny_client: TinyAPIClient,
) -> None:
    with pytest.raises(TinyNotFoundException) as excinfo:
        await live_tiny_client.get_product(1)

    assert excinfo.value.resource_type == "produto"
    assert str(excinfo.value.resource_id) == "1"


# ---------------------------------------------------------------------------
# Stage 06 — ProductSyncService persistence end-to-end
# ---------------------------------------------------------------------------
async def test_process_product_item_persists_row(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_product_id: int,
) -> None:
    service = ProductSyncService(
        tiny_client=live_tiny_client, queue_publisher=live_rabbitmq
    )
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("products")

    await service.process_product_item(e2e_product_id, sync_log_id)

    async with AsyncSessionLocal() as session:
        row = await PostgreSQLProductRepository(session).get_by_tiny_id(
            e2e_product_id
        )
    assert row is not None
    assert int(row["tiny_id"]) == e2e_product_id
    assert row["sku"], "sku must be persisted"
    assert row["type"] in ("P", "S", "K")


async def test_process_product_item_is_idempotent(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_product_id: int,
) -> None:
    service = ProductSyncService(
        tiny_client=live_tiny_client, queue_publisher=live_rabbitmq
    )
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("products")

    # Two consecutive calls must not produce two rows.
    await service.process_product_item(e2e_product_id, sync_log_id)
    await service.process_product_item(e2e_product_id, sync_log_id)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(func.count(ProductORM.tiny_id)).where(
                ProductORM.tiny_id == e2e_product_id
            )
        )
        assert int(result.scalar_one()) == 1


async def test_process_product_item_for_kit_writes_components(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
    e2e_kit_id: int,
) -> None:
    service = ProductSyncService(
        tiny_client=live_tiny_client, queue_publisher=live_rabbitmq
    )
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("products")

    await service.process_product_item(e2e_kit_id, sync_log_id)

    async with AsyncSessionLocal() as session:
        repo = PostgreSQLProductRepository(session)
        kit_row = await repo.get_by_tiny_id(e2e_kit_id)
        components = await repo.get_kit_components(e2e_kit_id)

    assert kit_row is not None and kit_row["type"] == "K"
    assert components, "kit must have at least one component"
    first = components[0]
    assert first["component_sku"]
    assert float(first["quantity"]) > 0


async def test_run_full_sync_publishes_one_message_per_active_product(
    live_tiny_client: TinyAPIClient,
    live_rabbitmq: QueuePublisher,
) -> None:
    """Full sync must paginate the active catalog and publish exactly one
    products.item message per active product. We drain the queue before
    and after so the count is unambiguous.
    """
    channel = get_channel()
    queue = await channel.get_queue("tiny.sync.products.item")

    # Drain anything left from previous runs.
    while True:
        leftover = await queue.get(no_ack=True, fail=False)
        if leftover is None:
            break

    service = ProductSyncService(
        tiny_client=live_tiny_client, queue_publisher=live_rabbitmq
    )
    async with AsyncSessionLocal() as session:
        sync_log_id = await SyncLogRepository(session).create_sync_log("products")

    await service.run_full_sync(sync_log_id)

    # Cross-check expected count via the same listing endpoint.
    head = await live_tiny_client.list_products(situation="A", limit=1)
    expected = int(head.get("paginacao", {}).get("total", 0))

    drained = 0
    while True:
        msg = await queue.get(no_ack=True, fail=False)
        if msg is None:
            break
        drained += 1

    assert drained == expected, (
        f"expected {expected} product fan-out messages, drained {drained}"
    )

    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                select(SyncLogORM).where(SyncLogORM.id == sync_log_id)
            )
        ).scalar_one()
    metadata = row.sync_metadata or {}
    assert metadata.get("total_enqueued") == expected
