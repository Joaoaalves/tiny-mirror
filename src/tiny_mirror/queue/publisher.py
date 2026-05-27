"""Single entry point for publishing messages onto RabbitMQ exchanges.

No other module is allowed to call ``aio_pika`` directly to publish — every
producer (scheduler jobs, webhook handlers, manual triggers, fan-outs from
consumers) goes through :class:`QueuePublisher` so persistence, JSON
serialization and routing-key mapping stay consistent.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

import aio_pika
import structlog
from aio_pika.abc import AbstractChannel

from tiny_mirror.exceptions import QueueException
from tiny_mirror.queue.topology import EXCHANGE_MAIN

logger = structlog.get_logger(__name__)


class QueuePublisher:
    # Logical name -> (exchange, routing key). The 'webhooks.*' entries route
    # through the same tiny.sync exchange (topic) — keeping a single exchange
    # simplifies bindings and lets us reuse the dead-letter wiring.
    ROUTING_MAP: ClassVar[dict[str, tuple[str, str]]] = {
        "products.full": (EXCHANGE_MAIN, "sync.products.full"),
        "products.item": (EXCHANGE_MAIN, "sync.products.item"),
        "orders.full": (EXCHANGE_MAIN, "sync.orders.full"),
        "orders.item": (EXCHANGE_MAIN, "sync.orders.item"),
        "stock.full": (EXCHANGE_MAIN, "sync.stock.full"),
        "stock.item": (EXCHANGE_MAIN, "sync.stock.item"),
        "buckets.refresh": (EXCHANGE_MAIN, "sync.buckets.refresh"),
        "invoices.full": (EXCHANGE_MAIN, "sync.invoices.full"),
        "stock_history.full": (EXCHANGE_MAIN, "sync.stock_history.full"),
        "purchase_orders.full": (EXCHANGE_MAIN, "sync.purchase_orders.full"),
        "ml_listings.full": (EXCHANGE_MAIN, "sync.ml_listings.full"),
        "ml_fl_stock.full": (EXCHANGE_MAIN, "sync.ml_fl_stock.full"),
        "webhooks.orders": (EXCHANGE_MAIN, "webhooks.orders"),
        "webhooks.stock": (EXCHANGE_MAIN, "webhooks.stock"),
    }

    def __init__(self, channel: AbstractChannel) -> None:
        self._channel = channel

    async def publish(
        self,
        exchange_name: str,
        routing_key: str,
        message: dict[str, Any],
    ) -> None:
        try:
            body = json.dumps(message, ensure_ascii=False, default=str).encode("utf-8")
            amqp_message = aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                content_encoding="utf-8",
            )
            exchange = await self._channel.get_exchange(exchange_name)
            await exchange.publish(amqp_message, routing_key=routing_key)
        except Exception as exc:
            logger.error(
                "Failed to publish message",
                exchange=exchange_name,
                routing_key=routing_key,
                error=str(exc),
            )
            raise QueueException(
                f"Failed to publish to {exchange_name}/{routing_key}: {exc}",
                queue_name=exchange_name,
                routing_key=routing_key,
            ) from exc

        logger.debug(
            "Message published",
            exchange=exchange_name,
            routing_key=routing_key,
            message_type=message.get("tipo", "unknown"),
        )

    async def publish_sync_message(self, queue_type: str, payload: dict[str, Any]) -> None:
        if queue_type not in self.ROUTING_MAP:
            raise QueueException(f"Unknown queue type: {queue_type}")
        exchange, routing_key = self.ROUTING_MAP[queue_type]
        await self.publish(exchange, routing_key, payload)
