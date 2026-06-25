"""Re-subscribe queue processor.

Raising a promotion price on the ML has no in-place edit: the app SAI (DELETE)
and REENTRA (POST). But the ML can take a while to re-suggest the campaign as a
candidate, so the immediate re-enroll sometimes fails and the listing is left at
the FULL price, with no promotion. The modify endpoint enqueues an
``ml_promo_resubscribe_jobs`` row in that case; this poller checks the listing's
eligible promos each tick and re-enrolls at the target price as soon as the
offer reappears as a candidate.

The poller is intentionally idempotent and self-healing:
- if the offer is already ``started`` again (operator re-did it, or a previous
  tick succeeded but the row wasn't closed) → mark done, no extra ML write;
- if the offer is not yet a candidate → bump the attempt and wait;
- on deadline / exhausted attempts → mark failed and raise an operator alert.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import MLPromoResubscribeJobORM
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLPromoActionRepository,
    MLPromoAlertRepository,
    MLPromoDecisionRepository,
    MLPromoResubscribeRepository,
)
from tiny_mirror.services.ml_promotion_service import MLPromotionService

logger = structlog.get_logger(__name__)

# Cadence between polls of a single waiting job. The scheduler cron can run more
# often than this; ``next_attempt_at`` is what actually gates a re-check, so a
# job is polled at most once per interval regardless of cron frequency.
DEFAULT_POLL_INTERVAL_SECONDS = 300


def _norm(s: Any) -> str:
    return (s or "").strip().upper()


def find_offer_by_status(
    promos: list[dict[str, Any]],
    promo_type: str,
    *,
    status: str,
    promo_id: str | None = None,
    strict: bool = False,
) -> dict[str, Any] | None:
    """First eligible-promos item matching ``type`` + ``status`` (case-insensitive).
    When ``promo_id`` is given, an exact id match is preferred but a same-type match
    still wins if the ML re-issued the offer under a new id (re-subscribe).

    ``strict=True`` disables that fallback — só casa o id EXATO. Necessário na
    MIGRAÇÃO de campanha: origem e destino são SELLER_CAMPAIGN DIFERENTES; sem strict,
    o check de "já ativo no destino" casava com a campanha de ORIGEM (started) e
    marcava 'already_active' sem inscrever de fato no destino."""
    pt = _norm(promo_type)
    st = (status or "").strip().lower()
    fallback: dict[str, Any] | None = None
    for p in promos:
        if _norm(p.get("type")) != pt:
            continue
        if (p.get("status") or "").strip().lower() != st:
            continue
        if promo_id and p.get("id") == promo_id:
            return p
        if fallback is None:
            fallback = p
    return None if strict else fallback


class ResubscribeService:
    """Drives ``ml_promo_resubscribe_jobs`` to completion."""

    def __init__(
        self,
        *,
        promotion_service: MLPromotionService,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
    ) -> None:
        self._svc = promotion_service
        self._poll_interval = poll_interval_seconds

    async def process_due(
        self,
        session: AsyncSession,
        *,
        now: datetime | None = None,
        batch_limit: int = 100,
    ) -> dict[str, int]:
        ref = now or datetime.now(UTC)
        repo = MLPromoResubscribeRepository(session)
        jobs = await repo.due(now=ref, limit=batch_limit)
        stats = {
            "due": len(jobs),
            "resubscribed": 0,
            "already_active": 0,
            "waiting": 0,
            "failed": 0,
            "deadline": 0,
            "errors": 0,
        }
        for job in jobs:
            try:
                outcome = await self._process_one(session, repo, job, ref)
                stats[outcome] = stats.get(outcome, 0) + 1
            except Exception as exc:  # one bad job must not stall the queue
                stats["errors"] += 1
                logger.error(
                    "promo.resubscribe_poll_error",
                    job_id=job.id,
                    mlb_id=job.mlb_id,
                    error=str(exc),
                )
                # Don't lose the row: push it to the next interval.
                try:
                    await repo.bump_attempt(
                        job,
                        next_attempt_at=ref + timedelta(seconds=self._poll_interval),
                        error=f"poll error: {exc}"[:500],
                    )
                except Exception:  # pragma: no cover — secondary failure
                    pass
            # Commit per job so progress survives a later job crashing.
            await session.commit()
        if stats["due"]:
            logger.info("promo.resubscribe_poll_done", **stats)
        return stats

    async def _process_one(
        self,
        session: AsyncSession,
        repo: MLPromoResubscribeRepository,
        job: MLPromoResubscribeJobORM,
        ref: datetime,
    ) -> str:
        log = logger.bind(
            job_id=job.id,
            mlb_id=job.mlb_id,
            sku=job.sku,
            promo_type=job.promo_type,
            op_id=job.op_id,
            target_price=float(job.target_price),
            attempt=job.attempts + 1,
        )

        deadline = job.deadline
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        if ref >= deadline:
            await repo.mark_failed(
                job,
                error="prazo esgotado — o ML não re-sugeriu a oferta a tempo",
            )
            await self._alert_giveup(session, job, "deadline")
            log.warning("promo.resubscribe_deadline")
            return "deadline"

        promos = await self._svc.fetch_eligible_promos(job.mlb_id)

        # Already re-enrolled (a prior tick won, or the operator redid it).
        strict = bool(getattr(job, "strict_promo_id", False))
        started = find_offer_by_status(
            promos, job.promo_type, status="started", promo_id=job.promo_id, strict=strict
        )
        if started is not None:
            await repo.mark_done(job)
            log.info("promo.resubscribe_already_active")
            return "already_active"

        candidate = find_offer_by_status(
            promos, job.promo_type, status="candidate", promo_id=job.promo_id, strict=strict
        )
        if candidate is None:
            await repo.bump_attempt(
                job,
                next_attempt_at=ref + timedelta(seconds=self._poll_interval),
                error="aguardando o ML re-sugerir a oferta",
            )
            outcome = "failed" if job.status == "failed" else "waiting"
            if outcome == "failed":
                await self._alert_giveup(session, job, "max_attempts")
            log.info("promo.resubscribe_waiting", outcome=outcome)
            return outcome

        # Offer is back — re-enroll at the target price.
        found_id = candidate.get("id") or job.promo_id
        if _norm(job.promo_type) == "PRICE_DISCOUNT" or not found_id:
            enter = await self._svc.create_price_discount(
                mlb_id=job.mlb_id, deal_price=float(job.target_price)
            )
        else:
            enter = await self._svc.modify_promotion(
                mlb_id=job.mlb_id,
                deal_price=float(job.target_price),
                promotion_id=found_id,
                promotion_type=job.promo_type,
            )
        sc = enter.get("status_code")
        if sc is not None and sc >= 400:
            await repo.bump_attempt(
                job,
                next_attempt_at=ref + timedelta(seconds=self._poll_interval),
                error=f"reentrada falhou: {str(enter.get('response'))[:400]}",
                status_code=sc,
            )
            outcome = "failed" if job.status == "failed" else "waiting"
            if outcome == "failed":
                await self._alert_giveup(session, job, "max_attempts")
            log.warning("promo.resubscribe_reenter_failed", ml_status_code=sc, outcome=outcome)
            return outcome

        await repo.mark_done(job)
        await MLPromoActionRepository(session).log(
            sku=job.sku,
            mlb_id=job.mlb_id,
            action="resubscribe_promotion",
            promo_type=job.promo_type,
            promo_id=found_id,
            price_after=Decimal(job.target_price),
            reason="re-inscrição automática concluída pela fila (oferta voltou a ser candidata)",
            ml_response=enter,
            decided_by=job.decided_by or "resubscribe-queue",
        )
        # Reaparece em 'Inscritas' com o novo preço antes do re-sync diário (a
        # linha 'started' foi expirada quando a re-inscrição foi agendada).
        await MLPromoDecisionRepository(session).restore_started_price(
            mlb_id=job.mlb_id,
            new_price=Decimal(job.target_price),
            promo_id=job.promo_id,
            promo_type=job.promo_type,
        )
        log.info("promo.resubscribe_done", ml_status_code=sc)
        return "resubscribed"

    async def _alert_giveup(
        self, session: AsyncSession, job: MLPromoResubscribeJobORM, why: str
    ) -> None:
        reason = "prazo esgotado" if why == "deadline" else "tentativas esgotadas"
        await MLPromoAlertRepository(session).create(
            sku=job.sku,
            mlb_id=job.mlb_id,
            kind="resubscribe_failed",
            message=(
                f"Re-inscrição automática falhou ({reason}) para {job.mlb_id} "
                f"({job.promo_type}). O anúncio pode estar a preço CHEIO, sem promoção. "
                f"Alvo era R$ {float(job.target_price):.2f}."
            ),
            data={
                "job_id": job.id,
                "promo_type": job.promo_type,
                "promo_id": job.promo_id,
                "target_price": float(job.target_price),
                "attempts": job.attempts,
                "why": why,
                "op_id": job.op_id,
            },
        )
