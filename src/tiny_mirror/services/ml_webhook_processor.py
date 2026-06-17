"""Processador das notificações push do Mercado Livre (fase 1b).

O endpoint de webhook só GRAVA a notificação (ack rápido). Este processador, que
roda num job frequente, lê as notificações pendentes, descobre QUAIS anúncios
foram afetados e re-sincroniza as promoções deles — assim Disponíveis/Inscritas
ficam quase em tempo real, sem esperar o cron diário.

Tratamos o corpo da notificação como NÃO confiável: ele só diz "olha esse
anúncio"; os dados reais vêm do ``generate_pending_decisions`` (que consulta a
API autenticada do ML). Idempotente: re-sincronizar o mesmo SKU duas vezes é
inofensivo, então duplicatas/reenvios do ML não causam problema.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLWebhookNotificationORM
from tiny_mirror.services.ml_promotion_service import ML_API_BASE, MLPromotionService

logger = structlog.get_logger(__name__)

_MLB_RE = re.compile(r"MLB\d{6,}")

# Tópicos de PROMOÇÃO que disparam re-sync. O mesmo callback recebe MUITOS outros
# tópicos (orders_v2, shipments, items, price_suggestion, ...) que NÃO são nossa
# conta aqui — esses são marcados 'ignored' (sem re-sync) pra não bater no ML à
# toa. Set explícito + fallback por palavra-chave (offer/candidate/competition),
# que nenhum tópico não-promo visto contém.
_PROMO_TOPICS = frozenset(
    {
        "public_offers",
        "public_candidates",
        "catalog_item_competition_status",
        "marketplace_item_competition_status",
    }
)
_PROMO_KEYWORDS = ("offer", "candidate", "competition", "promotion")


def _is_promo_topic(topic: str) -> bool:
    t = (topic or "").lower()
    return t in _PROMO_TOPICS or any(k in t for k in _PROMO_KEYWORDS)


def _is_competition_topic(topic: str) -> bool:
    """Buy-box do catálogo (ganhando/perdendo) → refresh de catálogo, não promo."""
    return "competition" in (topic or "").lower()


class WebhookProcessor:
    """Drena ``ml_webhook_notifications`` re-sincronizando os anúncios afetados."""

    def __init__(
        self,
        *,
        promotion_service: MLPromotionService,
        catalog_service: Any,
        token_service: Any,
        http_client: Any,
    ) -> None:
        self._svc = promotion_service
        self._catalog = catalog_service
        self._token = token_service
        self._http = http_client

    async def process_pending(
        self, session: AsyncSession, *, batch_limit: int = 200, retention_days: int = 7
    ) -> dict[str, int]:
        pending = list(
            (
                await session.execute(
                    select(MLWebhookNotificationORM)
                    .where(MLWebhookNotificationORM.status == "pending")
                    .order_by(MLWebhookNotificationORM.received_at.asc())
                    .limit(batch_limit)
                )
            ).scalars()
        )
        stats = {
            "pending": len(pending),
            "skus_synced": 0,
            "catalog_refreshed": 0,
            "processed": 0,
            "errors": 0,
            "unresolved": 0,
            "ignored": 0,
        }
        if not pending:
            stats["deleted_old"] = await self._cleanup(session, retention_days)
            return stats

        ignored_ids = [n.id for n in pending if not _is_promo_topic(n.topic)]
        promo = [n for n in pending if _is_promo_topic(n.topic)]

        # 1) PRÉ-RESOLVE tudo (só leituras, sem commit) — captura em dados puros
        #    pra não tocar nos objetos ORM depois que os commits abaixo os expiram.
        #    kind: 'comp' (buy-box → refresh de catálogo) | 'promo' (oferta/candidato
        #    → re-sync de promoções).
        resolved: list[dict[str, Any]] = []
        for n in promo:
            kind = "comp" if _is_competition_topic(n.topic) else "promo"
            mlb = n.mlb_id or await self._resolve_mlb(n.resource)
            sku: str | None = None
            if mlb:
                sku = (
                    await session.execute(
                        text("SELECT sku FROM ml_listings WHERE mlb_id = :m"), {"m": mlb}
                    )
                ).scalar_one_or_none()
            resolved.append({"id": n.id, "kind": kind, "mlb": mlb, "sku": sku})

        # 2a) Ofertas/candidatos → re-sync de promoções por SKU (dedupe).
        promo_skus = {r["sku"] for r in resolved if r["kind"] == "promo" and r["sku"]}
        synced_ok: set[str] = set()
        for sku in promo_skus:
            try:
                await self._svc.generate_pending_decisions(
                    session, only_sku=sku, refresh_active_prices=True
                )
                synced_ok.add(sku)
                stats["skus_synced"] += 1
            except Exception as exc:  # um SKU ruim não trava os outros
                logger.error("ml_webhook.resync_failed", sku=sku, error=str(exc))

        # 2b) Buy-box (competition) → refresh de catálogo por MLB (dedupe).
        comp_mlbs = {r["mlb"]: r["sku"] for r in resolved if r["kind"] == "comp" and r["mlb"]}
        refreshed_ok: set[str] = set()
        for mlb, sku in comp_mlbs.items():
            try:
                st = await self._catalog.refresh_one(session, mlb, sku)
                if st is not None:
                    refreshed_ok.add(mlb)
                    stats["catalog_refreshed"] += 1
            except Exception as exc:
                logger.error("ml_webhook.catalog_refresh_failed", mlb=mlb, error=str(exc))

        # 3) Classifica cada notificação pelo desfecho do seu alvo.
        processed_ids: list[int] = []
        unresolved_ids: list[int] = []
        failed_ids: list[int] = []
        for r in resolved:
            ok = (r["kind"] == "promo" and r["sku"] in synced_ok) or (
                r["kind"] == "comp" and r["mlb"] in refreshed_ok
            )
            key = r["sku"] if r["kind"] == "promo" else r["mlb"]
            if key is None:
                unresolved_ids.append(r["id"])
            elif ok:
                processed_ids.append(r["id"])
            else:
                failed_ids.append(r["id"])

        now = datetime.now(UTC)
        for ids, st, note in (
            (processed_ids, "processed", None),
            (failed_ids, "error", "re-sync/refresh falhou"),
            (unresolved_ids, "error", "MLB/SKU não resolvido a partir da notificação"),
            (ignored_ids, "ignored", "tópico não-promo"),
        ):
            if ids:
                await session.execute(
                    update(MLWebhookNotificationORM)
                    .where(MLWebhookNotificationORM.id.in_(ids))
                    .values(status=st, processed_at=now, note=note)
                )
        await session.commit()
        stats["processed"] = len(processed_ids)
        stats["errors"] = len(failed_ids)
        stats["unresolved"] = len(unresolved_ids)
        stats["ignored"] = len(ignored_ids)
        stats["deleted_old"] = await self._cleanup(session, retention_days)
        logger.info("ml_webhook.process_done", **stats)
        return stats

    async def _cleanup(self, session: AsyncSession, retention_days: int) -> int:
        """Apaga notificações já resolvidas mais velhas que ``retention_days`` —
        mantém a tabela enxuta (o ML manda MUITA notificação). Nunca toca em
        pending. Retorna quantas removeu."""
        if retention_days <= 0:
            return 0
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        res = await session.execute(
            text(
                "DELETE FROM ml_webhook_notifications "
                "WHERE status <> 'pending' AND received_at < :c"
            ),
            {"c": cutoff},
        )
        await session.commit()
        return int(res.rowcount or 0)  # type: ignore[attr-defined]

    async def _resolve_mlb(self, resource: str) -> str | None:
        """Quando o resource não traz o MLB no texto (ex.: candidatos), faz um GET
        autenticado no resource e extrai o MLB da resposta. Best-effort."""
        if not resource or not resource.startswith("/"):
            return None
        m = _MLB_RE.search(resource)
        if m:
            return m.group(0)
        try:
            token = await self._token.get_valid_access_token()
            url = f"{ML_API_BASE}{resource}"
            resp = await self._http.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
            )
            if resp.status_code == 401:
                token = await self._token.handle_unauthorized()
                resp = await self._http.get(
                    url, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
                )
            if resp.status_code == 200:
                hit = _MLB_RE.search(resp.text or "")
                return hit.group(0) if hit else None
        except Exception as exc:  # pragma: no cover — rede
            logger.warning("ml_webhook.resolve_failed", resource=resource, error=str(exc))
        return None
