"""Boot routine for the queue subsystem — separated from ``queue/__init__``
so that other modules can import ``queue.publisher`` without dragging in
the consumer/service stack (which would cause an import cycle).
"""

from __future__ import annotations

import structlog
from aio_pika.abc import AbstractChannel

from tiny_mirror.queue.base_consumer import BaseConsumer
from tiny_mirror.queue.consumers.bucket_consumer import BucketRefreshConsumer
from tiny_mirror.queue.consumers.invoice_consumers import InvoiceFullSyncConsumer
from tiny_mirror.queue.consumers.order_consumers import (
    OrderFullSyncConsumer,
    OrderItemConsumer,
)
from tiny_mirror.queue.consumers.product_consumers import (
    ProductFullSyncConsumer,
    ProductItemConsumer,
)
from tiny_mirror.queue.consumers.purchase_order_consumers import (
    PurchaseOrderFullSyncConsumer,
    StockHistoryFullSyncConsumer,
)
from tiny_mirror.queue.consumers.stock_consumers import (
    StockFullSyncConsumer,
    StockItemConsumer,
)
from tiny_mirror.queue.consumers.webhook_consumers import (
    OrderWebhookConsumer,
    StockWebhookConsumer,
)
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.services.invoice_sync_service import InvoiceSyncService
from tiny_mirror.services.order_sync_service import OrderSyncService
from tiny_mirror.services.product_sync_service import ProductSyncService
from tiny_mirror.services.purchase_order_sync_service import PurchaseOrderSyncService
from tiny_mirror.services.sale_bucket_service import SaleBucketService
from tiny_mirror.services.stock_history_sync_service import StockHistorySyncService
from tiny_mirror.services.stock_sync_service import StockSyncService

logger = structlog.get_logger(__name__)


async def start_consumers(
    channel: AbstractChannel,
    *,
    queue_publisher: QueuePublisher,
    product_sync: ProductSyncService,
    order_sync: OrderSyncService,
    stock_sync: StockSyncService,
    sale_buckets: SaleBucketService,
    invoice_sync: InvoiceSyncService,
    stock_history_sync: StockHistorySyncService,
    purchase_order_sync: PurchaseOrderSyncService,
) -> list[BaseConsumer]:
    """Register every consumer on the shared channel and return the instances.

    aio-pika's ``queue.consume`` registers the callback and returns
    immediately; the connection's I/O loop dispatches messages, so we don't
    need to keep an asyncio task per consumer alive. On shutdown, closing
    the channel/connection deregisters every consumer automatically.
    """
    consumers: list[BaseConsumer] = [
        ProductFullSyncConsumer(channel, queue_publisher, product_sync),
        ProductItemConsumer(channel, queue_publisher, product_sync),
        OrderFullSyncConsumer(channel, queue_publisher, order_sync),
        OrderItemConsumer(channel, queue_publisher, order_sync),
        StockFullSyncConsumer(channel, queue_publisher, stock_sync),
        StockItemConsumer(channel, queue_publisher, stock_sync),
        BucketRefreshConsumer(channel, queue_publisher, sale_buckets),
        InvoiceFullSyncConsumer(channel, queue_publisher, invoice_sync),
        StockHistoryFullSyncConsumer(channel, queue_publisher, stock_history_sync),
        PurchaseOrderFullSyncConsumer(channel, queue_publisher, purchase_order_sync),
        OrderWebhookConsumer(channel, queue_publisher, order_sync, sale_buckets),
        StockWebhookConsumer(channel, queue_publisher, stock_sync),
    ]
    for consumer in consumers:
        await consumer.start_consuming()
    logger.info("All consumers started", count=len(consumers))
    return consumers
