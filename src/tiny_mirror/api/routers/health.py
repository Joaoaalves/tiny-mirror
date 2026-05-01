"""Health-check endpoints used by load balancers and monitoring."""

from __future__ import annotations

from datetime import UTC, datetime

import redis.asyncio as redis
from aio_pika.abc import AbstractChannel
from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import (
    db_session,
    get_rabbitmq_channel,
    get_redis_client,
)
from tiny_mirror.api.schemas import HealthResponse
from tiny_mirror.config import settings

router = APIRouter(tags=["Health"])

VERSION = "1.0.0"


@router.get("/health")
async def health(
    session: AsyncSession = Depends(db_session),
    redis_client: redis.Redis = Depends(get_redis_client),
    channel: AbstractChannel = Depends(get_rabbitmq_channel),
) -> JSONResponse:
    """Aggregate health probe — checks Postgres, Redis and RabbitMQ.

    All three checks run sequentially because the cost is small (~ms) and
    parallelizing buys very little. Any failure flips the overall status
    to ``degraded`` and the response code to 503.
    """
    components: dict[str, str] = {}
    overall_ok = True

    try:
        await session.execute(text("SELECT 1"))
        components["database"] = "ok"
    except Exception as exc:  # pragma: no cover — exercised in failure tests
        overall_ok = False
        components["database"] = f"error: {exc}"

    try:
        await redis_client.ping()
        components["redis"] = "ok"
    except Exception as exc:  # pragma: no cover
        overall_ok = False
        components["redis"] = f"error: {exc}"

    try:
        if channel.is_closed:
            raise RuntimeError("RabbitMQ channel is closed")
        components["rabbitmq"] = "ok"
    except Exception as exc:  # pragma: no cover
        overall_ok = False
        components["rabbitmq"] = f"error: {exc}"

    body = HealthResponse(
        status="ok" if overall_ok else "degraded",
        timestamp=datetime.now(UTC),
        version=VERSION,
        environment=settings.app_env,
        components=components,
    ).model_dump(mode="json")

    return JSONResponse(
        status_code=status.HTTP_200_OK
        if overall_ok
        else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=body,
    )


@router.get("/ready")
async def ready() -> dict[str, str]:
    """Liveness probe — always 200 if the process is running."""
    return {"status": "ok"}
