"""Reusable FastAPI dependencies.

Each ``get_*`` factory below resolves into one ``Depends(...)`` chain:
session -> repos / publisher -> services. Routes consume them directly,
keeping handlers free of construction logic.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import httpx
import redis.asyncio as redis
from aio_pika.abc import AbstractChannel
from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.config import settings
from tiny_mirror.database import AsyncSessionLocal, get_async_session
from tiny_mirror.infrastructure.external.rate_limiter import RateLimiter
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.repositories.order_repository import (
    PostgreSQLOrderRepository,
)
from tiny_mirror.infrastructure.repositories.product_repository import (
    PostgreSQLProductRepository,
)
from tiny_mirror.infrastructure.repositories.sale_bucket_repository import (
    PostgreSQLSaleBucketRepository,
)
from tiny_mirror.infrastructure.repositories.stock_repository import (
    PostgreSQLStockRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.queue.publisher import QueuePublisher
from tiny_mirror.rabbitmq import get_channel
from tiny_mirror.redis_client import get_redis
from tiny_mirror.services.order_sync_service import OrderSyncService
from tiny_mirror.services.product_sync_service import ProductSyncService
from tiny_mirror.services.sale_bucket_service import SaleBucketService
from tiny_mirror.services.stock_sync_service import StockSyncService
from tiny_mirror.services.token_service import TokenService


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an :class:`AsyncSession` per request."""
    async for session in get_async_session():
        yield session


def get_http_client(request: Request) -> httpx.AsyncClient:
    """Return the shared ``httpx.AsyncClient`` created in the lifespan."""
    return request.app.state.http_client  # type: ignore[no-any-return]


def get_redis_client() -> redis.Redis:
    return get_redis()


def get_rabbitmq_channel() -> AbstractChannel:
    return get_channel()


def get_queue_publisher(request: Request) -> QueuePublisher:
    """Reuse the publisher created in the lifespan when available."""
    publisher = getattr(request.app.state, "queue_publisher", None)
    if publisher is None:
        publisher = QueuePublisher(get_channel())
    return publisher  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# Repositories
# ---------------------------------------------------------------------------
def get_product_repository(
    session: AsyncSession = Depends(db_session),
) -> PostgreSQLProductRepository:
    return PostgreSQLProductRepository(session)


def get_order_repository(
    session: AsyncSession = Depends(db_session),
) -> PostgreSQLOrderRepository:
    return PostgreSQLOrderRepository(session)


def get_stock_repository(
    session: AsyncSession = Depends(db_session),
) -> PostgreSQLStockRepository:
    return PostgreSQLStockRepository(session)


def get_sale_bucket_repository(
    session: AsyncSession = Depends(db_session),
) -> PostgreSQLSaleBucketRepository:
    return PostgreSQLSaleBucketRepository(session)


def get_sync_log_repository(
    session: AsyncSession = Depends(db_session),
) -> SyncLogRepository:
    return SyncLogRepository(session)


# ---------------------------------------------------------------------------
# External clients
# ---------------------------------------------------------------------------
def get_token_service(
    redis_client: redis.Redis = Depends(get_redis_client),
    http_client: httpx.AsyncClient = Depends(get_http_client),
) -> TokenService:
    return TokenService(
        session_factory=AsyncSessionLocal,
        redis_client=redis_client,
        http_client=http_client,
        tiny_client_id=settings.tiny_client_id,
        tiny_client_secret=settings.tiny_client_secret,
        tiny_initial_refresh_token=settings.tiny_refresh_token,
    )


def get_rate_limiter(
    redis_client: redis.Redis = Depends(get_redis_client),
) -> RateLimiter:
    return RateLimiter(redis_client)


def get_tiny_client(
    token_service: TokenService = Depends(get_token_service),
    rate_limiter: RateLimiter = Depends(get_rate_limiter),
    http_client: httpx.AsyncClient = Depends(get_http_client),
) -> TinyAPIClient:
    return TinyAPIClient(
        token_service=token_service,
        rate_limiter=rate_limiter,
        http_client=http_client,
    )


# ---------------------------------------------------------------------------
# Services (sync / business)
# ---------------------------------------------------------------------------
def get_product_sync_service(
    tiny_client: TinyAPIClient = Depends(get_tiny_client),
    queue_publisher: QueuePublisher = Depends(get_queue_publisher),
) -> ProductSyncService:
    return ProductSyncService(
        tiny_client=tiny_client, queue_publisher=queue_publisher
    )


def get_order_sync_service(
    tiny_client: TinyAPIClient = Depends(get_tiny_client),
    queue_publisher: QueuePublisher = Depends(get_queue_publisher),
) -> OrderSyncService:
    return OrderSyncService(
        tiny_client=tiny_client, queue_publisher=queue_publisher
    )


def get_stock_sync_service(
    tiny_client: TinyAPIClient = Depends(get_tiny_client),
    queue_publisher: QueuePublisher = Depends(get_queue_publisher),
) -> StockSyncService:
    return StockSyncService(
        tiny_client=tiny_client, queue_publisher=queue_publisher
    )


def get_sale_bucket_service() -> SaleBucketService:
    return SaleBucketService()
