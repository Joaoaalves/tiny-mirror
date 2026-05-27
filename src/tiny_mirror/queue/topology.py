"""RabbitMQ topology: exchanges, queues, dead-letter routing.

Idempotent — safe to call on every startup. Declares the two exchanges
(``tiny.sync`` topic + ``tiny.dlx`` direct), all main queues with their
dead-letter arguments, matching dead-letter queues, and binds everything together.
"""

from __future__ import annotations

import aio_pika
import structlog
from aio_pika.abc import AbstractChannel

logger = structlog.get_logger(__name__)

EXCHANGE_MAIN = "tiny.sync"
EXCHANGE_DLX = "tiny.dlx"

# (queue name, routing key on tiny.sync)
QUEUE_BINDINGS: tuple[tuple[str, str], ...] = (
    ("tiny.sync.products.full", "sync.products.full"),
    ("tiny.sync.products.item", "sync.products.item"),
    ("tiny.sync.orders.full", "sync.orders.full"),
    ("tiny.sync.orders.item", "sync.orders.item"),
    ("tiny.sync.stock.full", "sync.stock.full"),
    ("tiny.sync.stock.item", "sync.stock.item"),
    ("tiny.sync.buckets.refresh", "sync.buckets.refresh"),
    ("tiny.sync.invoices.full", "sync.invoices.full"),
    ("tiny.sync.stock_history.full", "sync.stock_history.full"),
    ("tiny.sync.purchase_orders.full", "sync.purchase_orders.full"),
    ("tiny.sync.ml_listings.full", "sync.ml_listings.full"),
    ("tiny.sync.ml_fl_stock.full", "sync.ml_fl_stock.full"),
    ("tiny.sync.fl_stock_correction.full", "sync.fl_stock_correction.full"),
    ("tiny.sync.phantom_detection.full", "sync.phantom_detection.full"),
    ("tiny.webhooks.orders", "webhooks.orders"),
    ("tiny.webhooks.stock", "webhooks.stock"),
)


def dlq_name(queue: str) -> str:
    return f"{queue}.dlq"


async def setup_topology(channel: AbstractChannel) -> None:
    dlx = await channel.declare_exchange(
        EXCHANGE_DLX,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
        auto_delete=False,
    )
    main = await channel.declare_exchange(
        EXCHANGE_MAIN,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
        auto_delete=False,
    )

    for queue_name, routing_key in QUEUE_BINDINGS:
        dlq = await channel.declare_queue(
            dlq_name(queue_name),
            durable=True,
            arguments={},
        )
        await dlq.bind(dlx, routing_key=dlq_name(queue_name))

        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": EXCHANGE_DLX,
                "x-dead-letter-routing-key": dlq_name(queue_name),
            },
        )
        await queue.bind(main, routing_key=routing_key)

    logger.info(
        "RabbitMQ topology declared",
        main_exchange=EXCHANGE_MAIN,
        dlx_exchange=EXCHANGE_DLX,
        queues=len(QUEUE_BINDINGS),
    )
