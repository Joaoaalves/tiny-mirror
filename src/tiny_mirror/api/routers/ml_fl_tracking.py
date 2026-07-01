"""Estoque Full — API router.

Read-only towards Tiny/ML; only our workflow tables are mutated. Three tabs:

- ``GET /novos``      — anúncios fulfillment com cobertura > 30d (candidatos).
- ``GET /ignored`` / ``GET /removed`` — sub-listas de dispensados na aba Novos.
- ``GET /tracking``   — aba Acompanhamento (snapshot + valores atuais + timeline).
- ``GET /finalized``  — aba Finalizado.

Transições: track / annotate / finalize / remove / dismiss / restore.
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.api.dependencies import db_session
from tiny_mirror.config import settings
from tiny_mirror.services.ml_fl_tracking_service import MLFlTrackingService

logger = structlog.get_logger(__name__)

router = APIRouter()


def _service(session: AsyncSession) -> MLFlTrackingService:
    return MLFlTrackingService(session, ignore_days=settings.ml_fl_ignore_days)


# ── schemas ──────────────────────────────────────────────────────────────────
class DismissIn(BaseModel):
    mlb_id: str
    kind: str = Field(pattern="^(ignore|remove)$")
    actor: str | None = None


class RestoreIn(BaseModel):
    mlb_id: str


class ActorIn(BaseModel):
    actor: str | None = None


class AnnotateIn(BaseModel):
    author: str | None = None
    note: str = Field(min_length=1)


# ── reads ────────────────────────────────────────────────────────────────────
@router.get("/novos")
async def get_novos(session: AsyncSession = Depends(db_session)) -> list[dict[str, Any]]:
    return await _service(session).list_novos()


@router.get("/ignored")
async def get_ignored(session: AsyncSession = Depends(db_session)) -> list[dict[str, Any]]:
    return await _service(session).list_dismissed("ignore")


@router.get("/removed")
async def get_removed(session: AsyncSession = Depends(db_session)) -> list[dict[str, Any]]:
    return await _service(session).list_dismissed("remove")


@router.get("/tracking")
async def get_tracking(session: AsyncSession = Depends(db_session)) -> list[dict[str, Any]]:
    svc = _service(session)
    rows = await svc.list_tracking("tracking")
    return await svc.enrich_results(rows)


@router.get("/finalized")
async def get_finalized(session: AsyncSession = Depends(db_session)) -> list[dict[str, Any]]:
    return await _service(session).list_tracking("finalized")


# ── transitions ──────────────────────────────────────────────────────────────
@router.post("/dismiss")
async def dismiss(body: DismissIn, session: AsyncSession = Depends(db_session)) -> dict[str, Any]:
    logger.info("fl_tracking.dismiss", mlb_id=body.mlb_id, kind=body.kind, actor=body.actor)
    return await _service(session).dismiss(body.mlb_id, kind=body.kind, created_by=body.actor)


@router.post("/restore")
async def restore(body: RestoreIn, session: AsyncSession = Depends(db_session)) -> dict[str, bool]:
    ok = await _service(session).restore(body.mlb_id)
    return {"restored": ok}


@router.post("/track/{mlb_id}")
async def track(
    mlb_id: str, body: ActorIn, session: AsyncSession = Depends(db_session)
) -> dict[str, Any]:
    logger.info("fl_tracking.track", mlb_id=mlb_id, actor=body.actor)
    try:
        return await _service(session).track(mlb_id, moved_by=body.actor)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/trackings/{tracking_id}/annotate")
async def annotate(
    tracking_id: int, body: AnnotateIn, session: AsyncSession = Depends(db_session)
) -> dict[str, Any]:
    try:
        return await _service(session).annotate(tracking_id, author=body.author, note=body.note)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/trackings/{tracking_id}/finalize")
async def finalize(
    tracking_id: int, body: ActorIn, session: AsyncSession = Depends(db_session)
) -> dict[str, Any]:
    logger.info("fl_tracking.finalize", tracking_id=tracking_id, actor=body.actor)
    try:
        return await _service(session).finalize(tracking_id, finalized_by=body.actor)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/trackings/{tracking_id}")
async def remove_tracking(
    tracking_id: int, session: AsyncSession = Depends(db_session)
) -> dict[str, bool]:
    ok = await _service(session).remove_tracking(tracking_id)
    if not ok:
        raise HTTPException(status_code=404, detail="acompanhamento não encontrado")
    return {"removed": ok}
