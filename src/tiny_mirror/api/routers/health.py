"""Health-check endpoints used by load balancers and monitoring."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe — always returns ``200`` if the process is up."""
    return {"status": "ok"}


@router.get("/ready")
async def ready() -> dict[str, str]:
    """Readiness probe — placeholder until dependency checks land in stage 10."""
    return {"status": "ok"}
