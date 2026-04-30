"""Order endpoints — implemented in stage 10."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter()


@router.get("")
async def list_orders() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "Not implemented yet")
