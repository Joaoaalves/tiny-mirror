"""Espelho AS-IS das promoções do Mercado Livre.

FATO, não decisão: pega o que o ``GET /seller-promotions/items/{MLB}`` retorna e
grava cru em ``ml_promotions`` — sem cap, sem piso, sem preço inventado. O motor
de decisão (cap/piso/margem) é outro sistema (``ml_promo_decisions``) e não passa
por aqui.

Mantém o espelho = estado ATUAL do ML: a cada sync de um anúncio, faz upsert das
promos retornadas e APAGA as que o ML não retorna mais (campanha encerrada).
Alimentado por: webhook do ML (tempo real, por MLB), reconcile diário, e as
nossas próprias ações de escrita.
"""

from __future__ import annotations

from typing import Any

import structlog
from sqlalchemy import delete, func, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLPromotionORM
from tiny_mirror.services.ml_promotion_service import MLPromotionService, _parse_iso_dt, _to_dec

logger = structlog.get_logger(__name__)


def promo_to_row(mlb_id: str, sku: str | None, p: dict[str, Any]) -> dict[str, Any]:
    """Mapeia uma promo crua do ML para uma linha de ``ml_promotions``. Puro
    (sem I/O) — testável. ``promo_key`` = promotion_id quando existe, senão o
    type (seller PRICE_DISCOUNT não tem id)."""
    promo_id = p.get("id")
    ptype = (p.get("type") or "?")[:40]
    key = (str(promo_id) if promo_id else ptype)[:80]
    return {
        "mlb_id": mlb_id,
        "sku": sku,
        "promo_key": key,
        "promotion_id": str(promo_id)[:40] if promo_id else None,
        "promotion_type": ptype,
        "sub_type": (str(p["sub_type"])[:40] if p.get("sub_type") else None),
        "status": (str(p.get("status") or "")[:20]),
        "price": _to_dec(p.get("price")),
        "original_price": _to_dec(p.get("original_price")),
        "suggested_price": _to_dec(p.get("suggested_discounted_price")),
        "min_price": _to_dec(p.get("min_discounted_price")),
        "max_price": _to_dec(p.get("max_discounted_price")),
        "seller_percentage": _to_dec(p.get("seller_percentage")),
        "meli_percentage": _to_dec(p.get("meli_percentage")),
        "offer_id": (
            str(p.get("offer_id") or p.get("ref_id"))[:80]
            if (p.get("offer_id") or p.get("ref_id"))
            else None
        ),
        "name": p.get("name"),
        "start_date": _parse_iso_dt(p.get("start_date")),
        "finish_date": _parse_iso_dt(p.get("finish_date")),
        "stock": p.get("stock") if isinstance(p.get("stock"), dict) else None,
        "raw": p,
    }


class PromotionMirrorService:
    def __init__(self, ml_service: MLPromotionService) -> None:
        self._ml = ml_service

    async def sync_mlb(self, session: AsyncSession, mlb_id: str, sku: str | None = None) -> int:
        """Sincroniza o espelho de UM anúncio: busca eligible no ML e reconcilia
        ``ml_promotions``. Retorna o nº de promos espelhadas (0 limpa o anúncio)."""
        promos = await self._ml.fetch_eligible_promos(mlb_id)
        n = await self._reconcile_mlb(session, mlb_id, sku, promos, {})
        await session.commit()
        return n

    async def sync_all(self, session: AsyncSession) -> dict[str, int]:
        """Reconcilia TODOS os anúncios ativos E pausados. Pausado conta porque a
        promoção pode continuar rodando no ML mesmo com o anúncio pausado, e a UI
        os mostra no filtro 'Pausados'. Reconcile diário de segurança."""
        rows = (
            await session.execute(
                text("SELECT mlb_id, sku FROM ml_listings WHERE status IN ('active', 'paused')")
            )
        ).all()
        stats = {"mlbs": 0, "promos": 0, "errors": 0}
        # Cache de datas de campanha por promotion_id — campanhas são poucas e
        # compartilhadas por muitos anúncios, então busca cada uma UMA vez.
        date_cache: dict[str, tuple[Any, Any]] = {}
        for mlb_id, sku in rows:
            stats["mlbs"] += 1
            try:
                promos = await self._ml.fetch_eligible_promos(mlb_id)
                stats["promos"] += await self._reconcile_mlb(
                    session, mlb_id, sku, promos, date_cache
                )
            except Exception as exc:  # pragma: no cover — rede
                stats["errors"] += 1
                logger.debug("promotion_mirror_sync_failed", mlb_id=mlb_id, error=str(exc))
        await session.commit()
        logger.info("promotion_mirror_sync_all_done", **stats)
        return stats

    async def _reconcile_mlb(
        self,
        session: AsyncSession,
        mlb_id: str,
        sku: str | None,
        promos: list[dict[str, Any]],
        date_cache: dict[str, tuple[Any, Any]],
    ) -> int:
        """Upsert das promos vistas + DELETE das ausentes (mirror = estado atual
        do ML). NÃO commita (o caller decide o escopo da transação)."""
        if sku is None:
            sku = (
                await session.execute(
                    text("SELECT sku FROM ml_listings WHERE mlb_id = :m"), {"m": mlb_id}
                )
            ).scalar_one_or_none()

        seen: set[str] = set()
        for p in promos:
            row = promo_to_row(mlb_id, sku, p)
            # Co-participação (SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN) não traz
            # datas no elegível — completa pela campanha (cacheado por promo_id).
            if row["promotion_id"] and row["start_date"] is None:
                pid = row["promotion_id"]
                if pid not in date_cache:
                    date_cache[pid] = await self._ml.fetch_promotion_dates(
                        pid, row["promotion_type"]
                    )
                row["start_date"], row["finish_date"] = date_cache[pid]
            seen.add(row["promo_key"])
            stmt = (
                pg_insert(MLPromotionORM)
                .values(**row, last_seen_at=func.now(), updated_at=func.now())
                .on_conflict_do_update(
                    index_elements=["mlb_id", "promo_key"],
                    set_={
                        **{k: row[k] for k in row if k not in ("mlb_id", "promo_key")},
                        "last_seen_at": func.now(),
                        "updated_at": func.now(),
                    },
                )
            )
            await session.execute(stmt)

        # Apaga as promos que o ML não retorna mais para este anúncio.
        if seen:
            await session.execute(
                delete(MLPromotionORM).where(
                    MLPromotionORM.mlb_id == mlb_id,
                    MLPromotionORM.promo_key.notin_(seen),
                )
            )
        else:
            await session.execute(delete(MLPromotionORM).where(MLPromotionORM.mlb_id == mlb_id))
        return len(seen)
