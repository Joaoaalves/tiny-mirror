"""Manual sync trigger endpoints — implemented in stage 10."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter()


@router.post("/products")
async def sync_products() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "Not implemented yet")


@router.post("/orders")
async def sync_orders() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "Not implemented yet")


@router.post("/stock")
async def sync_stock() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "Not implemented yet")
