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
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLWebhookNotificationORM
from tiny_mirror.services.ml_promotion_service import ML_API_BASE, MLPromotionService

logger = structlog.get_logger(__name__)

_MLB_RE = re.compile(r"MLB\d{6,}")


class WebhookProcessor:
    """Drena ``ml_webhook_notifications`` re-sincronizando os anúncios afetados."""

    def __init__(
        self,
        *,
        promotion_service: MLPromotionService,
        token_service: Any,
        http_client: Any,
    ) -> None:
        self._svc = promotion_service
        self._token = token_service
        self._http = http_client

    async def process_pending(
        self, session: AsyncSession, *, batch_limit: int = 200
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
            "processed": 0,
            "errors": 0,
            "unresolved": 0,
        }
        if not pending:
            return stats

        # 1) Resolve (id da notificação → SKU). Lê tudo do ORM AGORA, antes de
        #    qualquer commit do generate (que expira os objetos).
        items: list[tuple[int, str | None]] = []
        skus: set[str] = set()
        for n in pending:
            mlb = n.mlb_id or await self._resolve_mlb(n.resource)
            sku: str | None = None
            if mlb:
                sku = (
                    await session.execute(
                        text("SELECT sku FROM ml_listings WHERE mlb_id = :m"), {"m": mlb}
                    )
                ).scalar_one_or_none()
            items.append((n.id, sku))
            if sku:
                skus.add(sku)

        # 2) Re-sincroniza cada SKU afetado UMA vez (dedupe). refresh_active_prices
        #    pra atualizar preço das started já conhecidas também.
        synced_ok: set[str] = set()
        for sku in skus:
            try:
                await self._svc.generate_pending_decisions(
                    session, only_sku=sku, refresh_active_prices=True
                )
                synced_ok.add(sku)
                stats["skus_synced"] += 1
            except Exception as exc:  # um SKU ruim não trava os outros
                logger.error("ml_webhook.resync_failed", sku=sku, error=str(exc))

        # 3) Marca as notificações pelo desfecho do SEU SKU.
        processed_ids = [i for i, s in items if s is not None and s in synced_ok]
        unresolved_ids = [i for i, s in items if s is None]
        failed_ids = [i for i, s in items if s is not None and s not in synced_ok]
        now = datetime.now(UTC)
        for ids, st, note in (
            (processed_ids, "processed", None),
            (failed_ids, "error", "resync falhou"),
            (unresolved_ids, "error", "MLB/SKU não resolvido a partir da notificação"),
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
        logger.info("ml_webhook.process_done", **stats)
        return stats

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
