"""Fulfillment transfer endpoints."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field

from tiny_mirror.api.dependencies import get_tiny_client
from tiny_mirror.api.schemas import PaginationResponse
from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.repositories.fulfillment_transfer_repository import (
    FulfillmentTransferRepository,
)
from tiny_mirror.services.fulfillment_transfer_service import (
    FulfillmentTransferService,
    InsufficientStockError,
    ProductNotFoundError,
)

router = APIRouter()
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class TransferRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sku: str = Field(..., description="Product SKU to transfer")
    quantity: int = Field(..., gt=0, description="Number of units to transfer to Full ML")


class TransferResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    product_tiny_id: int
    product_sku: str
    quantity: int
    cost_per_unit: Decimal
    transferred_at: datetime
    status: str


class TransferListResponse(BaseModel):
    items: list[TransferResponse]
    pagination: PaginationResponse


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@router.post(
    "/transfer",
    status_code=status.HTTP_201_CREATED,
    response_model=TransferResponse,
    summary="Transfer units from Galpão to Full ML",
)
async def create_transfer(
    body: TransferRequest,
    tiny_client: TinyAPIClient = Depends(get_tiny_client),
) -> TransferResponse:
    """Transfer `quantity` units of `sku` from Galpão (deposit 851264346) to
    Full ML (deposit 912048995) via Tiny ERP API, then record the transfer
    as pending until ML INBOUND_RECEPTION confirms arrival.

    Returns the created transfer record with status=pending.
    """
    service = FulfillmentTransferService(tiny_client=tiny_client)
    try:
        result = await service.transfer_to_full(sku=body.sku, quantity=body.quantity)
    except ProductNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Product not found: {exc.sku}",
        ) from exc
    except InsufficientStockError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Insufficient Galpão stock for {exc.sku}: "
                f"requested {exc.requested}, available {exc.available}"
            ),
        ) from exc

    return TransferResponse(
        id=result.id,
        product_tiny_id=result.product_tiny_id,
        product_sku=result.product_sku,
        quantity=result.quantity,
        cost_per_unit=result.cost_per_unit,
        transferred_at=result.transferred_at,
        status=result.status,
    )


@router.get(
    "/transfers",
    response_model=TransferListResponse,
    summary="List fulfillment transfers",
)
async def list_transfers(
    sku: str | None = Query(default=None, description="Filter by SKU"),
    transfer_status: Literal["pending", "received", "cancelled"] | None = Query(
        default=None, alias="status"
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
) -> TransferListResponse:
    """List fulfillment transfer records, optionally filtered by SKU or status."""
    import math

    async with AsyncSessionLocal() as session:
        repo = FulfillmentTransferRepository(session)
        rows, total = await repo.list_all(
            sku=sku,
            status=transfer_status,
            limit=page_size,
            offset=(page - 1) * page_size,
        )

    total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 0
    items = [
        TransferResponse(
            id=row.id,
            product_tiny_id=row.product_tiny_id,
            product_sku=row.product_sku,
            quantity=row.quantity,
            cost_per_unit=row.cost_per_unit,
            transferred_at=row.transferred_at,
            status=row.status,
        )
        for row in rows
    ]
    return TransferListResponse(
        items=items,
        pagination=PaginationResponse(
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
        ),
    )
