"""Webhook receivers — implemented in stage 11."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter()


@router.post("/tiny")
async def tiny_webhook() -> None:
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED, "Not implemented yet")
