"""FastAPI application factory and lifespan handler.

Production servers (gunicorn/uvicorn) import :data:`app` directly via
``src.tiny_mirror.main:app``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import structlog
from fastapi import FastAPI

from tiny_mirror.api.error_handlers import register_error_handlers
from tiny_mirror.api.middleware import RequestIdMiddleware, RequestLoggingMiddleware
from tiny_mirror.api.routers.health import router as health_router
from tiny_mirror.api.routers.orders import router as orders_router
from tiny_mirror.api.routers.products import router as products_router
from tiny_mirror.api.routers.sync import router as sync_router
from tiny_mirror.api.routers.webhooks import router as webhooks_router
from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal, close_database, initialize_database
from tiny_mirror.infrastructure.external.mercadolivre_client import MercadoLivreAPIClient
from tiny_mirror.infrastructure.external.rate_limiter import RateLimiter
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.external.tiny_v2_client import TinyV2Client
from tiny_mirror.logging_config import configure_logging
from tiny_mirror.queue.bootstrap import start_consumers
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.queue.topology import setup_topology
from tiny_mirror.rabbitmq import close_rabbitmq, get_channel, initialize_rabbitmq
from tiny_mirror.redis_client import close_redis, get_redis, initialize_redis
from tiny_mirror.scheduler.jobs import (
    check_and_trigger_initial_sync,
    setup_scheduler,
    shutdown_scheduler,
)
from tiny_mirror.services.invoice_sync_service import InvoiceSyncService
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService
from tiny_mirror.services.order_sync_service import OrderSyncService
from tiny_mirror.services.product_sync_service import ProductSyncService
from tiny_mirror.services.purchase_order_sync_service import PurchaseOrderSyncService
from tiny_mirror.services.sale_bucket_service import SaleBucketService
from tiny_mirror.services.stock_history_sync_service import StockHistorySyncService
from tiny_mirror.services.stock_sync_service import StockSyncService
from tiny_mirror.services.token_service import TokenService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Run startup and shutdown routines for every external dependency."""
    logger = structlog.get_logger(__name__)
    structlog.contextvars.bind_contextvars(service="tiny-mirror")
    logger.info("Starting tiny-mirror service", env=settings.app_env)

    scheduler: Any = None
    app.state.consumers = []

    try:
        await initialize_database()
        await initialize_redis()
        await initialize_rabbitmq()

        channel = get_channel()
        await setup_topology(channel)

        app.state.http_client = httpx.AsyncClient(timeout=30.0)
        app.state.queue_publisher = QueuePublisher(channel)

        token_service = TokenService(
            session_factory=AsyncSessionLocal,
            redis_client=get_redis(),
            http_client=app.state.http_client,
            tiny_client_id=settings.tiny_client_id,
            tiny_client_secret=settings.tiny_client_secret,
            tiny_initial_refresh_token=settings.tiny_refresh_token,
        )
        await token_service.validate_on_startup()
        app.state.token_service = token_service

        tiny_client = TinyAPIClient(
            token_service=token_service,
            rate_limiter=RateLimiter(get_redis()),
            http_client=app.state.http_client,
        )
        app.state.tiny_client = tiny_client

        tiny_v2_client = TinyV2Client(
            token=settings.tiny_v2_token,
            http_client=app.state.http_client,
        )

        # Mercado Livre overlay — optional. When ML_CLIENT_ID is set, every
        # per-product stock sync also pulls Full ML available_quantity from
        # the ML API and overwrites the (unreliable) Tiny "Full Mercado
        # Livre" deposit row in stock_deposits.
        ml_api_client: MercadoLivreAPIClient | None = None
        if settings.ml_client_id:
            ml_token_service = MercadoLivreTokenService(
                session_factory=AsyncSessionLocal,
                redis_client=get_redis(),
                http_client=app.state.http_client,
                ml_client_id=settings.ml_client_id,
                ml_client_secret=settings.ml_client_secret,
                ml_initial_refresh_token=settings.ml_refresh_token,
            )
            await ml_token_service.validate_on_startup()
            app.state.ml_token_service = ml_token_service

            ml_api_client = MercadoLivreAPIClient(
                token_service=ml_token_service,
                http_client=app.state.http_client,
                ml_user_id=settings.ml_user_id,
            )
            logger.info("Mercado Livre overlay enabled", ml_user_id=settings.ml_user_id)
        else:
            logger.info("ML_CLIENT_ID not set; Mercado Livre overlay disabled")

        # Stages 08-09 still ship stub services that raise
        # NotImplementedError; their messages go to their DLQs until the
        # matching stage lands.
        invoice_sync = InvoiceSyncService(
            tiny_client=tiny_client,
            queue_publisher=app.state.queue_publisher,
        )

        app.state.consumers = await start_consumers(
            channel,
            queue_publisher=app.state.queue_publisher,
            product_sync=ProductSyncService(
                tiny_client=tiny_client,
                queue_publisher=app.state.queue_publisher,
            ),
            order_sync=OrderSyncService(
                tiny_client=tiny_client,
                queue_publisher=app.state.queue_publisher,
                invoice_sync=invoice_sync,
            ),
            stock_sync=StockSyncService(
                tiny_client=tiny_client,
                queue_publisher=app.state.queue_publisher,
                ml_client=ml_api_client,
            ),
            sale_buckets=SaleBucketService(),
            invoice_sync=invoice_sync,
            stock_history_sync=StockHistorySyncService(tiny_v2=tiny_v2_client),
            purchase_order_sync=PurchaseOrderSyncService(tiny_client=tiny_client),
        )

        scheduler = setup_scheduler(app)
        app.state.scheduler = scheduler

        await check_and_trigger_initial_sync(app)

        logger.info("tiny-mirror started successfully")

        yield
    finally:
        logger.info("Shutting down tiny-mirror service")
        shutdown_scheduler(scheduler)
        http_client: httpx.AsyncClient | None = getattr(app.state, "http_client", None)
        if http_client is not None:
            await http_client.aclose()
        await close_rabbitmq()
        await close_redis()
        await close_database()
        logger.info("tiny-mirror stopped")


def create_app() -> FastAPI:
    configure_logging(settings.log_level)

    app = FastAPI(
        title="tiny-mirror",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.is_development else None,
        redoc_url=None,
    )

    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(RequestIdMiddleware)

    register_error_handlers(app)

    app.include_router(health_router)
    app.include_router(products_router, prefix="/products", tags=["Products"])
    app.include_router(orders_router, prefix="/orders", tags=["Orders"])
    app.include_router(sync_router, prefix="/sync", tags=["Sync"])
    app.include_router(webhooks_router, prefix="/webhooks", tags=["Webhooks"])

    return app


app = create_app()
