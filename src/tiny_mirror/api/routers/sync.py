"""Manual sync trigger endpoints + sync_logs query."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import (
    db_session,
    get_queue_publisher,
    get_sync_log_repository,
)
from tiny_mirror.api.schemas import (
    PaginationResponse,
    SyncLogListResponse,
    SyncLogResponse,
    SyncTriggerResponse,
)
from tiny_mirror.infrastructure.orm.models import SyncLogORM
from tiny_mirror.infrastructure.repositories.sync_log_repository import (
    SyncLogRepository,
)
from tiny_mirror.queue.publisher import QueuePublisher

router = APIRouter()


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------
class OrdersSyncRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lookback_hours: int | None = Field(default=None, ge=1, le=720)
    date_from: date | None = None
    date_to: date | None = None

    @model_validator(mode="after")
    def _validate_window(self) -> OrdersSyncRequest:
        if (self.date_from is None) != (self.date_to is None):
            raise ValueError("date_from and date_to must be supplied together")
        if (
            self.date_from is not None
            and self.date_to is not None
            and self.date_from > self.date_to
        ):
            raise ValueError("date_from must be <= date_to")
        if self.date_from is not None and self.lookback_hours is not None:
            raise ValueError("lookback_hours and date range are mutually exclusive")
        return self


# ---------------------------------------------------------------------------
# Trigger endpoints
# ---------------------------------------------------------------------------
@router.post(
    "/products",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SyncTriggerResponse,
)
async def sync_products(
    sync_logs: SyncLogRepository = Depends(get_sync_log_repository),
    publisher: QueuePublisher = Depends(get_queue_publisher),
) -> SyncTriggerResponse:
    sync_log_id = await sync_logs.create_sync_log(
        "products", metadata={"triggered_by": "manual"}
    )
    await publisher.publish_sync_message(
        "products.full",
        {
            "triggered_by": "manual",
            "sync_log_id": sync_log_id,
            "published_at": datetime.now(UTC).isoformat(),
        },
    )
    return SyncTriggerResponse(
        message="Product sync triggered", sync_log_id=sync_log_id
    )


@router.post(
    "/orders",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SyncTriggerResponse,
)
async def sync_orders(
    body: OrdersSyncRequest = Body(default_factory=OrdersSyncRequest),
    sync_logs: SyncLogRepository = Depends(get_sync_log_repository),
    publisher: QueuePublisher = Depends(get_queue_publisher),
) -> SyncTriggerResponse:
    metadata: dict = {"triggered_by": "manual"}
    if body.date_from is not None and body.date_to is not None:
        metadata["date_from"] = body.date_from.isoformat()
        metadata["date_to"] = body.date_to.isoformat()
    elif body.lookback_hours is not None:
        metadata["lookback_hours"] = body.lookback_hours

    sync_log_id = await sync_logs.create_sync_log("orders", metadata=metadata)

    payload: dict
    if body.date_from is not None and body.date_to is not None:
        payload = {
            "is_historical": True,
            "date_from": body.date_from.isoformat(),
            "date_to": body.date_to.isoformat(),
            "sync_log_id": sync_log_id,
            "lookback_hours": None,
            "published_at": datetime.now(UTC).isoformat(),
        }
    else:
        payload = {
            "is_historical": False,
            "lookback_hours": body.lookback_hours or 2,
            "sync_log_id": sync_log_id,
            "date_from": None,
            "date_to": None,
            "published_at": datetime.now(UTC).isoformat(),
        }
    await publisher.publish_sync_message("orders.full", payload)

    return SyncTriggerResponse(
        message="Order sync triggered", sync_log_id=sync_log_id
    )


@router.post(
    "/stock",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=SyncTriggerResponse,
)
async def sync_stock(
    sync_logs: SyncLogRepository = Depends(get_sync_log_repository),
    publisher: QueuePublisher = Depends(get_queue_publisher),
) -> SyncTriggerResponse:
    sync_log_id = await sync_logs.create_sync_log(
        "stock", metadata={"triggered_by": "manual"}
    )
    await publisher.publish_sync_message(
        "stock.full",
        {
            "sync_log_id": sync_log_id,
            "published_at": datetime.now(UTC).isoformat(),
        },
    )
    return SyncTriggerResponse(
        message="Stock sync triggered", sync_log_id=sync_log_id
    )


# ---------------------------------------------------------------------------
# Query: GET /sync/logs
# ---------------------------------------------------------------------------
@router.get("/logs", response_model=SyncLogListResponse)
async def list_sync_logs(
    sync_type: Annotated[
        Literal["products", "orders", "stock", "sale_buckets", "token_rotation"]
        | None,
        Query(),
    ] = None,
    log_status: Annotated[
        Literal["running", "completed", "failed"] | None,
        Query(alias="status"),
    ] = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    session: AsyncSession = Depends(db_session),
) -> SyncLogListResponse:
    filters = []
    if sync_type is not None:
        filters.append(SyncLogORM.sync_type == sync_type)
    if log_status is not None:
        filters.append(SyncLogORM.status == log_status)

    base_query = select(SyncLogORM)
    for clause in filters:
        base_query = base_query.where(clause)

    list_query = (
        base_query.order_by(SyncLogORM.started_at.desc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    )
    count_query = select(func.count(SyncLogORM.id))
    for clause in filters:
        count_query = count_query.where(clause)

    # See products.list_products: same session can't run queries concurrently.
    items_result = await session.execute(list_query)
    count_result = await session.execute(count_query)

    rows = items_result.scalars().all()
    total = int(count_result.scalar_one())
    total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 0

    if total > 0 and page > total_pages:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Page out of range"
        )

    items = [
        SyncLogResponse(
            id=int(row.id),
            sync_type=row.sync_type,
            status=row.status,
            started_at=row.started_at,
            completed_at=row.completed_at,
            items_processed=int(row.items_processed),
            items_failed=int(row.items_failed),
            error_message=row.error_message,
            metadata=row.sync_metadata,
        )
        for row in rows
    ]
    return SyncLogListResponse(
        items=items,
        pagination=PaginationResponse(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )
