"""Estoque Full — service layer.

Turns the per-MLB metrics into the rows each tab shows (Novos / Acompanhamento
/ Finalizado) and runs the workflow transitions (acompanhar, anotar, finalizar,
ignorar, remover, restaurar). 100% read-only towards Tiny/ML — only our own
workflow tables are written here.

Pure helpers (ABC curve, status label, promo %, coverage) live at module level
so they're unit-testable without a DB.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.repositories.ml_fl_tracking_repository import (
    MLFlTrackingRepository,
)

# mv_coverage.status_base -> rótulo PT exibido na coluna de status.
_STATUS_LABEL = {
    "zombie": "Zumbi",
    "discontinue": "Descontinuado",
    "slow": "Lento",
    "declining": "Declinando",
    "clearance": "Queima",
    "rupture": "Ruptura",
    "pending": "Monitorar",
}
_NEW_PRODUCT_DAYS = 30


def compute_abc(metrics: list[dict[str, Any]]) -> dict[str, str]:
    """Curva ABC (Pareto por receita 90d): A<=80%, B<=95%, C o resto.

    Ranqueia TODOS os anúncios fulfillment. Receita 0 (ou universo sem receita)
    cai em C. Retorna ``{mlb_id: 'A'|'B'|'C'}``.
    """
    ranked = sorted(metrics, key=lambda m: float(m.get("rev_90d") or 0), reverse=True)
    total = sum(float(m.get("rev_90d") or 0) for m in ranked)
    out: dict[str, str] = {}
    if total <= 0:
        return {m["mlb_id"]: "C" for m in ranked}
    cum = 0.0
    for m in ranked:
        rev = float(m.get("rev_90d") or 0)
        if rev <= 0:
            out[m["mlb_id"]] = "C"
            continue
        # Classify by the cumulative share BEFORE this item, so the top item
        # (and each boundary-straddling item) lands in the richer class — the
        # standard ABC convention. A: prev < 80%, B: prev < 95%, C: rest.
        prev_pct = cum / total
        out[m["mlb_id"]] = "A" if prev_pct < 0.80 else ("B" if prev_pct < 0.95 else "C")
        cum += rev
    return out


def classify_status(status_base: str | None, age_days: int | None) -> str:
    """Rótulo de status. Produto recém-criado (< 30d) vira 'Novo' e tem
    precedência sobre a classificação de coverage."""
    if age_days is not None and age_days < _NEW_PRODUCT_DAYS:
        return "Novo"
    return _STATUS_LABEL.get(status_base or "", "Monitorar")


def promo_pct(original: Decimal | float | None, price: Decimal | float | None) -> float | None:
    """% de desconto total da promoção ativa. None se não houver promo válida."""
    if original is None or price is None:
        return None
    o, p = float(original), float(price)
    if o <= 0 or p >= o:
        return None
    return round((o - p) / o * 100, 1)


def coverage_days(stock_full: int, sold_30d: int) -> float | None:
    """Cobertura FL em dias. Sem vendas (ritmo 0) => None (cobertura infinita)."""
    rate = sold_30d / 30.0
    if rate <= 0:
        return None
    return round(stock_full / rate, 1)


def _age_days(created_at: datetime | None, today: datetime) -> int | None:
    if created_at is None:
        return None
    return (today.date() - created_at.date()).days


def _build_row(m: dict[str, Any], curve: str, today: datetime) -> dict[str, Any]:
    """Monta a linha exibível (Novos e base de snapshot) a partir da métrica."""
    stock_full = int(m.get("stock_full") or 0)
    sold_30d = int(m.get("sold_30d") or 0)
    daily_rate = round(sold_30d / 30.0, 2)
    cov = coverage_days(stock_full, sold_30d)
    pct = promo_pct(m.get("promo_original"), m.get("promo_price"))
    age = _age_days(m.get("product_created_at"), today)
    return {
        "mlb_id": m["mlb_id"],
        "sku": m.get("sku"),
        "title": m.get("title"),
        "permalink": m.get("permalink"),
        "curve": curve,
        "status": classify_status(m.get("status_base"), age),
        "status_base": m.get("status_base"),
        "stock_full": stock_full,
        "stock_galpao": int(m.get("stock_galpao") or 0),
        "sold_30d": sold_30d,
        "daily_rate_30d": daily_rate,
        "surplus": stock_full - sold_30d,
        "coverage_days": cov,
        "promo_pct": pct,
        "promo_type": m.get("promo_type"),
        "promo_seller_pct": (
            float(m["promo_seller_pct"]) if m.get("promo_seller_pct") is not None else None
        ),
        "rev_90d": float(m.get("rev_90d") or 0),
    }


def _qualifies_novos(row: dict[str, Any]) -> bool:
    """Novos = anúncio FL com estoque e cobertura > 30d (ou sem vendas)."""
    if row["stock_full"] <= 0:
        return False
    cov = row["coverage_days"]
    return cov is None or cov > 30


class MLFlTrackingService:
    def __init__(self, session: AsyncSession, *, ignore_days: int = 7) -> None:
        self._session = session
        self._repo = MLFlTrackingRepository(session)
        self._ignore_days = ignore_days

    async def _metrics_by_mlb(self) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        metrics = await self._repo.fetch_metrics()
        abc = compute_abc(metrics)
        return {m["mlb_id"]: m for m in metrics}, abc

    # ── NOVOS ────────────────────────────────────────────────────────────────
    async def list_novos(self) -> list[dict[str, Any]]:
        today = datetime.now(UTC)
        by_mlb, abc = await self._metrics_by_mlb()
        tracked = await self._repo.active_tracking_mlbs()
        dismissed = await self._active_dismissed_mlbs(today)
        rows = []
        for mlb, m in by_mlb.items():
            if mlb in tracked or mlb in dismissed:
                continue
            row = _build_row(m, abc.get(mlb, "C"), today)
            if _qualifies_novos(row):
                rows.append(row)
        rows.sort(key=lambda r: r["surplus"], reverse=True)
        return rows

    async def list_dismissed(self, kind: str) -> list[dict[str, Any]]:
        """Ignorados (kind='ignore', ativos) ou Removidos (kind='remove')."""
        today = datetime.now(UTC)
        by_mlb, abc = await self._metrics_by_mlb()
        out = []
        for d in await self._repo.list_dismissals():
            if d.kind != kind:
                continue
            if kind == "ignore" and (d.until is None or d.until <= today):
                continue  # ignore expirado — some da lista de ignorados
            m = by_mlb.get(d.mlb_id)
            row = (
                _build_row(m, abc.get(d.mlb_id, "C"), today)
                if m
                else {
                    "mlb_id": d.mlb_id,
                    "sku": d.sku,
                }
            )
            row["dismissed_kind"] = d.kind
            row["dismissed_until"] = d.until.isoformat() if d.until else None
            row["dismissed_by"] = d.created_by
            row["dismissed_at"] = d.created_at.isoformat() if d.created_at else None
            out.append(row)
        return out

    async def _active_dismissed_mlbs(self, now: datetime) -> set[str]:
        out = set()
        for d in await self._repo.list_dismissals():
            if d.kind == "remove":
                out.add(d.mlb_id)
            elif d.kind == "ignore" and d.until is not None and d.until > now:
                out.add(d.mlb_id)
        return out

    async def dismiss(self, mlb_id: str, *, kind: str, created_by: str | None) -> dict[str, Any]:
        now = datetime.now(UTC)
        by_mlb, _ = await self._metrics_by_mlb()
        sku = (by_mlb.get(mlb_id) or {}).get("sku")
        d = await self._repo.upsert_dismissal(
            mlb_id,
            kind=kind,
            sku=sku,
            created_by=created_by,
            ignore_days=self._ignore_days,
            now=now,
        )
        await self._session.commit()
        return {
            "mlb_id": d.mlb_id,
            "kind": d.kind,
            "until": d.until.isoformat() if d.until else None,
        }

    async def restore(self, mlb_id: str) -> bool:
        ok = await self._repo.delete_dismissal(mlb_id)
        await self._session.commit()
        return ok

    # ── ACOMPANHAR ───────────────────────────────────────────────────────────
    async def track(self, mlb_id: str, *, moved_by: str | None) -> dict[str, Any]:
        today = datetime.now(UTC)
        by_mlb, abc = await self._metrics_by_mlb()
        m = by_mlb.get(mlb_id)
        if m is None:
            raise ValueError(f"MLB {mlb_id} não é um anúncio fulfillment conhecido")
        existing = await self._repo.get_active_by_mlb(mlb_id)
        if existing is not None:
            return self._tracking_public(existing)
        row = _build_row(m, abc.get(mlb_id, "C"), today)
        tracking = await self._repo.create_tracking(
            mlb_id=mlb_id,
            sku=row["sku"],
            status="tracking",
            moved_by=moved_by,
            initial_stock_full=row["stock_full"],
            initial_stock_galpao=row["stock_galpao"],
            initial_daily_rate_30d=Decimal(str(row["daily_rate_30d"])),
            initial_promo_pct=(
                Decimal(str(row["promo_pct"])) if row["promo_pct"] is not None else None
            ),
            initial_snapshot=row,
        )
        await self._repo.add_event(
            tracking.id,
            event_type="status_change",
            author=moved_by,
            note="Enviado para acompanhamento",
            payload={"from": "novos", "to": "tracking"},
        )
        # Dismissal (se existir) deixa de fazer sentido — o anúncio agora é acompanhado.
        await self._repo.delete_dismissal(mlb_id)
        await self._session.commit()
        return self._tracking_public(tracking)

    async def annotate(self, tracking_id: int, *, author: str | None, note: str) -> dict[str, Any]:
        tracking = await self._repo.get_tracking(tracking_id)
        if tracking is None:
            raise ValueError("acompanhamento não encontrado")
        ev = await self._repo.add_event(
            tracking_id, event_type="annotation", author=author, note=note
        )
        await self._session.commit()
        return {
            "id": ev.id,
            "tracking_id": tracking_id,
            "event_type": ev.event_type,
            "author": ev.author,
            "note": ev.note,
            "created_at": ev.created_at.isoformat() if ev.created_at else None,
        }

    async def finalize(self, tracking_id: int, *, finalized_by: str | None) -> dict[str, Any]:
        today = datetime.now(UTC)
        tracking = await self._repo.get_tracking(tracking_id)
        if tracking is None:
            raise ValueError("acompanhamento não encontrado")
        if tracking.status == "finalized":
            return self._tracking_public(tracking)
        by_mlb, abc = await self._metrics_by_mlb()
        m = by_mlb.get(tracking.mlb_id)
        row = _build_row(m, abc.get(tracking.mlb_id, "C"), today) if m else None

        tracking.status = "finalized"
        tracking.finalized_at = today
        tracking.finalized_by = finalized_by
        if row is not None:
            tracking.final_stock_full = row["stock_full"]
            tracking.final_daily_rate_30d = Decimal(str(row["daily_rate_30d"]))
            tracking.final_promo_pct = (
                Decimal(str(row["promo_pct"])) if row["promo_pct"] is not None else None
            )
            tracking.final_snapshot = row
        tracking.result_summary = _result_summary(tracking, row, today)
        await self._repo.add_event(
            tracking_id,
            event_type="status_change",
            author=finalized_by,
            note="Finalizado",
            payload={"from": "tracking", "to": "finalized"},
        )
        await self._session.commit()
        return self._tracking_public(tracking)

    async def remove_tracking(self, tracking_id: int) -> bool:
        ok = await self._repo.delete_tracking(tracking_id)
        await self._session.commit()
        return ok

    # ── LISTAGENS acompanhamento/finalizados ─────────────────────────────────
    async def list_tracking(self, status: str) -> list[dict[str, Any]]:
        today = datetime.now(UTC)
        by_mlb, abc = await self._metrics_by_mlb()
        trackings = await self._repo.list_tracking(status)
        events_by = await self._repo.list_events_for([t.id for t in trackings])
        out = []
        for t in trackings:
            m = by_mlb.get(t.mlb_id)
            cur = _build_row(m, abc.get(t.mlb_id, "C"), today) if m else None
            events = events_by.get(t.id, [])
            out.append(self._acompanhamento_row(t, cur, events, today))
        return out

    def _acompanhamento_row(
        self,
        t: Any,
        cur: dict[str, Any] | None,
        events: list[Any],
        today: datetime,
    ) -> dict[str, Any]:
        last_event = events[-1] if events else None
        last_change_at = last_event.created_at if last_event else t.moved_at
        base = self._tracking_public(t)
        base.update(
            {
                "current": cur,
                "last_change_at": last_change_at.isoformat() if last_change_at else None,
                "timeline": [
                    {
                        "id": e.id,
                        "event_type": e.event_type,
                        "author": e.author,
                        "note": e.note,
                        "created_at": e.created_at.isoformat() if e.created_at else None,
                    }
                    for e in events
                ],
                # "resultado desde o primeiro dia" e "após a última alteração"
                # são calculados no endpoint (precisam de query extra por MLB).
            }
        )
        return base

    def _tracking_public(self, t: Any) -> dict[str, Any]:
        return {
            "id": t.id,
            "mlb_id": t.mlb_id,
            "sku": t.sku,
            "status": t.status,
            "moved_at": t.moved_at.isoformat() if t.moved_at else None,
            "moved_by": t.moved_by,
            "initial_stock_full": t.initial_stock_full,
            "initial_stock_galpao": t.initial_stock_galpao,
            "initial_daily_rate_30d": (
                float(t.initial_daily_rate_30d) if t.initial_daily_rate_30d is not None else None
            ),
            "initial_promo_pct": (
                float(t.initial_promo_pct) if t.initial_promo_pct is not None else None
            ),
            "initial_snapshot": t.initial_snapshot,
            "finalized_at": t.finalized_at.isoformat() if t.finalized_at else None,
            "finalized_by": t.finalized_by,
            "final_stock_full": t.final_stock_full,
            "final_daily_rate_30d": (
                float(t.final_daily_rate_30d) if t.final_daily_rate_30d is not None else None
            ),
            "final_promo_pct": (
                float(t.final_promo_pct) if t.final_promo_pct is not None else None
            ),
            "final_snapshot": t.final_snapshot,
            "result_summary": t.result_summary,
        }

    async def enrich_results(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Preenche 'resultado desde o primeiro dia' e 'após a última alteração'
        (média de vendas/dia por MLB entre datas). Feito fora do _build para
        agrupar as queries por MLB."""
        for r in rows:
            mlb = r["mlb_id"]
            moved_at = _parse_iso(r.get("moved_at"))
            last_change = _parse_iso(r.get("last_change_at")) or moved_at
            if moved_at is not None:
                r["result_since_start"] = await self._avg_since(mlb, moved_at)
            if last_change is not None:
                r["result_since_last_change"] = await self._avg_since(mlb, last_change)
        return rows

    async def _avg_since(self, mlb_id: str, since: datetime) -> dict[str, Any]:
        units = await self._repo.units_sold_since(mlb_id, since)
        days = max((datetime.now(UTC).date() - since.date()).days, 1)
        return {"units": units, "days": days, "avg_per_day": round(units / days, 2)}


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _result_summary(t: Any, cur: dict[str, Any] | None, today: datetime) -> str:
    """Resumo automático da aba Finalizado."""
    days = max((today.date() - t.moved_at.date()).days, 0) if t.moved_at else 0
    parts = [f"{days}d de acompanhamento"]
    if t.initial_stock_full is not None and cur is not None:
        delta = cur["stock_full"] - t.initial_stock_full
        parts.append(f"estoque FL {t.initial_stock_full}→{cur['stock_full']} ({delta:+d})")
    i_rate = float(t.initial_daily_rate_30d) if t.initial_daily_rate_30d is not None else None
    f_rate = cur["daily_rate_30d"] if cur is not None else None
    if i_rate is not None and f_rate is not None:
        parts.append(f"ritmo {i_rate:.2f}→{f_rate:.2f}/d")
    i_promo = float(t.initial_promo_pct) if t.initial_promo_pct is not None else 0.0
    f_promo = cur["promo_pct"] if (cur and cur.get("promo_pct") is not None) else 0.0
    parts.append(f"promo {i_promo:.0f}%→{f_promo:.0f}%")
    return "; ".join(parts)
