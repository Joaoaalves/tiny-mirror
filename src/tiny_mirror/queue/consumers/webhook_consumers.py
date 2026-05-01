"""Webhook payload consumers.

The ``OrderWebhookConsumer`` mirrors Tiny's order-status notifications
into the local store and then fans out an incremental stock refresh
plus a sale-bucket recompute for the order's date. The
``StockWebhookConsumer`` simply re-fetches the full stock for the
notified product (the webhook payload only carries one of the
balances).

Both consumers honor the ``request_id`` propagated from the inbound
HTTP request so that the receiver-side log line and the consumer-side
processing log can be correlated end-to-end.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from aio_pika.abc import AbstractChannel

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.order_sync_service import OrderSyncService
from tiny_mirror.services.sale_bucket_service import SaleBucketService
from tiny_mirror.services.stock_sync_service import StockSyncService

logger = structlog.get_logger(__name__)


def _bind_request_id(message_body: dict[str, Any]) -> None:
    request_id = message_body.get("request_id")
    if request_id:
        structlog.contextvars.bind_contextvars(request_id=request_id)


class OrderWebhookConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.webhooks.orders"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        order_sync_service: OrderSyncService,
        stock_sync_service: StockSyncService,
        sale_bucket_service: SaleBucketService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._orders = order_sync_service
        self._stock = stock_sync_service
        self._buckets = sale_bucket_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        _bind_request_id(message_body)

        dados = message_body["dados"]
        order_tiny_id = int(dados["id_venda_tiny"])
        situacao = dados.get("situacao")

        logger.info(
            "Processing order webhook",
            order_tiny_id=order_tiny_id,
            situacao=situacao,
        )

        # Fetch + map + persist order and items via the existing service so
        # the upsert behavior stays in one place.
        await self._orders.process_order_item(order_tiny_id, sync_log_id=None)

        # Now read back the persisted order to drive the fan-out.
        async with AsyncSessionLocal() as session:
            persisted = await PostgreSQLOrderRepository(session).get_by_tiny_id(
                order_tiny_id
            )
        if persisted is None:
            # process_order_item logged "Order not found" already; nothing
            # else to fan out.
            return

        items = persisted.get("items", []) or []

        # Fan out an incremental stock refresh for every product touched by
        # the order. NULL product_tiny_id means we never synced that product
        # — skip; the daily sync will pick it up.
        product_ids = {
            int(item["product_tiny_id"])
            for item in items
            if item.get("product_tiny_id") is not None
        }
        for pid in product_ids:
            await self._publisher.publish_sync_message(
                "stock.item",
                {
                    "product_tiny_id": pid,
                    "sync_log_id": None,
                    "published_at": datetime.now(UTC).isoformat(),
                },
            )

        # Recompute the day's sale buckets so analytics catch the change.
        order_date = persisted["order_date"]
        await self._publisher.publish_sync_message(
            "buckets.refresh",
            {
                "date_from": order_date.isoformat(),
                "date_to": order_date.isoformat(),
                "triggered_by": "webhook",
                "published_at": datetime.now(UTC).isoformat(),
            },
        )

        logger.info(
            "Order webhook processed",
            order_tiny_id=order_tiny_id,
            order_number=persisted.get("order_number"),
            items_count=len(items),
            stock_products_queued=len(product_ids),
        )


class StockWebhookConsumer(BaseConsumer):
    QUEUE_NAME = "tiny.webhooks.stock"

    def __init__(
        self,
        channel: AbstractChannel,
        queue_publisher: QueuePublisher,
        stock_sync_service: StockSyncService,
    ) -> None:
        super().__init__(channel, queue_publisher)
        self._stock = stock_sync_service

    async def handle(self, message_body: dict[str, Any]) -> None:
        _bind_request_id(message_body)

        dados = message_body["dados"]
        product_tiny_id = int(dados["id_produto"])
        sku = dados.get("sku")

        logger.info(
            "Processing stock webhook",
            product_tiny_id=product_tiny_id,
            sku=sku,
            balance=dados.get("saldo"),
        )

        # Always re-fetch the full payload from Tiny — the webhook only
        # carries one balance, but we need the deposit breakdown too.
        await self._stock.process_stock_item(product_tiny_id, sync_log_id=None)
