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
from tiny_mirror.infrastructure.repositories.token_repository import (
    PostgreSQLTokenRepository,
)
from tiny_mirror.logging_config import configure_logging
from tiny_mirror.queue.topology import setup_topology
from tiny_mirror.rabbitmq import close_rabbitmq, get_channel, initialize_rabbitmq
from tiny_mirror.redis_client import close_redis, get_redis, initialize_redis
from tiny_mirror.scheduler.jobs import setup_scheduler, shutdown_scheduler
from tiny_mirror.services.token_service import TokenService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Run startup and shutdown routines for every external dependency."""
    logger = structlog.get_logger(__name__)
    structlog.contextvars.bind_contextvars(service="tiny-mirror")
    logger.info("Starting tiny-mirror service", env=settings.app_env)

    scheduler: Any = None
    consumer_tasks: list[Any] = []

    try:
        await initialize_database()
        await initialize_redis()
        await initialize_rabbitmq()
        await setup_topology(get_channel())

        app.state.http_client = httpx.AsyncClient(timeout=30.0)

        async with AsyncSessionLocal() as session:
            token_service = TokenService(
                token_repository=PostgreSQLTokenRepository(session),
                redis_client=get_redis(),
                http_client=app.state.http_client,
                tiny_client_id=settings.tiny_client_id,
                tiny_client_secret=settings.tiny_client_secret,
                tiny_initial_refresh_token=settings.tiny_refresh_token,
            )
            await token_service.validate_on_startup()

        # consumer tasks and historical-sync trigger land in stage 05+.
        scheduler = setup_scheduler(app)
        logger.info("tiny-mirror started successfully")

        yield
    finally:
        logger.info("Shutting down tiny-mirror service")
        shutdown_scheduler(scheduler)
        for task in consumer_tasks:
            task.cancel()
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
