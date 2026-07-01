"""ML sales sync — vendas por anúncio (MLB) por dia, do Mercado Livre.

Os pedidos do Tiny só guardam o SKU. Pra dividir vendas por anúncio (o que
importa pras promoções) buscamos da ML Orders API (``/orders/search``), que
traz ``order_items[].item.id`` (MLB) + seller_sku + quantity + date_created.
Agrega por (mlb_id, dia) e faz upsert em ``ml_sales_daily``.

Janela por dia (o ``offset`` da busca do ML satura em 10k; por dia o volume
fica bem abaixo disso). Conta só pedidos pagos/enviados/entregues.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLSalesDailyORM

logger = structlog.get_logger(__name__)

_VALID_STATUSES = {"paid", "shipped", "delivered", "partially_paid"}
_PAGE = 50
_API = "https://api.mercadolibre.com/orders/search"
_SHIP_API = "https://api.mercadolibre.com/shipments"
# Quantos shipments buscar em paralelo (o logistic_type não vem no pedido).
_SHIP_CONCURRENCY = 8


class MLSalesSyncService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient, ml_user_id: str) -> None:
        self._tok = token_service
        self._http = http_client
        self._uid = ml_user_id

    async def _auth(self) -> dict[str, str]:
        access = await self._tok.get_valid_access_token()
        return {"Authorization": f"Bearer {access}"}

    async def _fetch_day(self, day: date) -> dict[tuple[str, date], dict[str, Any]]:
        """Agrega vendas (mlb, dia) -> {qty, revenue, full_qty, full_revenue, sku}.

        ``full_*`` é a parcela despachada por Full (``shipment.logistic_type ==
        'fulfillment'``) — o que o painel Full do ML conta em "Vendas 30 dias". O
        ``logistic_type`` não vem no pedido, então buscamos o shipment de cada
        pedido (concorrente, limitado).
        """
        frm = f"{day.isoformat()}T00:00:00.000-00:00"
        to = f"{day.isoformat()}T23:59:59.999-00:00"
        # (odate, shipment_id, [(mlb, qty, unit_price, seller_sku), ...])
        orders: list[tuple[date, str | None, list[tuple[str, int, float, str | None]]]] = []
        offset = 0
        while True:
            headers = await self._auth()
            params: dict[str, str | int] = {
                "seller": self._uid,
                "order.date_created.from": frm,
                "order.date_created.to": to,
                "sort": "date_asc",
                "offset": offset,
                "limit": _PAGE,
            }
            r = await self._http.get(_API, headers=headers, params=params)
            if r.status_code == 401:
                # The cached token is the one that just got rejected — force a
                # refresh instead of re-reading the same token from Redis.
                access = await self._tok.handle_unauthorized()
                headers = {"Authorization": f"Bearer {access}"}
                r = await self._http.get(_API, headers=headers, params=params)
            r.raise_for_status()
            data = r.json()
            results = data.get("results") or []
            for o in results:
                if (o.get("status") or "") not in _VALID_STATUSES:
                    continue
                created = str(o.get("date_created") or "")[:10]
                try:
                    odate = date.fromisoformat(created)
                except ValueError:
                    odate = day
                sid = (o.get("shipping") or {}).get("id")
                items: list[tuple[str, int, float, str | None]] = []
                for it in o.get("order_items") or []:
                    item = it.get("item") or {}
                    mlb = item.get("id")
                    if not mlb:
                        continue
                    qty = int(it.get("quantity") or 0)
                    if qty <= 0:
                        continue
                    # unit_price = preço efetivamente cobrado por unidade (já com
                    # a promo aplicada). Receita da linha = unit_price * quantity.
                    try:
                        unit_price = float(it.get("unit_price") or 0)
                    except (TypeError, ValueError):
                        unit_price = 0.0
                    items.append((mlb, qty, unit_price, item.get("seller_sku")))
                if items:
                    orders.append((odate, str(sid) if sid else None, items))
            if not results:
                break
            total = (data.get("paging") or {}).get("total")
            offset += _PAGE
            # Missing/zero paging metadata must not truncate a non-empty page;
            # keep going until an empty page (or the 10k offset hard cap).
            if (total is not None and int(total) > 0 and offset >= int(total)) or offset >= 10000:
                break

        # Classifica Full vs não-Full pelo logistic_type do shipment.
        ship_ids = {sid for _, sid, _ in orders if sid}
        ship_type = await self._fetch_shipment_types(ship_ids)

        agg: dict[tuple[str, date], dict[str, Any]] = {}
        for odate, sid, items in orders:
            is_full = ship_type.get(sid) == "fulfillment" if sid else False
            for mlb, qty, unit_price, seller_sku in items:
                key = (mlb, odate)
                cur = agg.setdefault(
                    key,
                    {"qty": 0, "sku": None, "revenue": 0.0, "full_qty": 0, "full_revenue": 0.0},
                )
                cur["qty"] += qty
                cur["revenue"] += unit_price * qty
                if is_full:
                    cur["full_qty"] += qty
                    cur["full_revenue"] += unit_price * qty
                if not cur["sku"]:
                    cur["sku"] = seller_sku
        return agg

    async def _fetch_shipment_types(self, shipment_ids: set[str]) -> dict[str, str | None]:
        """``{shipment_id -> logistic_type}``. Concorrente e limitado; um shipment
        que falhe vira ``None`` (tratado como não-Full)."""
        if not shipment_ids:
            return {}
        sem = asyncio.Semaphore(_SHIP_CONCURRENCY)
        out: dict[str, str | None] = {}

        async def one(sid: str) -> None:
            url = f"{_SHIP_API}/{sid}"
            async with sem:
                try:
                    headers = await self._auth()
                    r = await self._http.get(url, headers=headers)
                    if r.status_code == 401:
                        access = await self._tok.handle_unauthorized()
                        r = await self._http.get(url, headers={"Authorization": f"Bearer {access}"})
                    out[sid] = r.json().get("logistic_type") if r.status_code == 200 else None
                except Exception:  # pragma: no cover — network noise
                    out[sid] = None

        await asyncio.gather(*(one(s) for s in shipment_ids))
        return out

    async def backfill(self, days: int = 90) -> dict[str, Any]:
        """Reconstrói ml_sales_daily dos últimos ``days`` dias (incl. hoje)."""
        logger.info("ml_sales backfill started", days=days)
        today = datetime.now(UTC).date()
        start = today - timedelta(days=days - 1)
        # Agrega globalmente por (mlb, dia) — a data do pedido pode cair fora da
        # janela do dia buscado (fuso), então a mesma chave pode aparecer em
        # dois dias; somamos pra não duplicar no upsert.
        total_agg: dict[tuple[str, date], dict[str, Any]] = {}
        days_done = 0
        for i in range(days):
            day = start + timedelta(days=i)
            try:
                agg = await self._fetch_day(day)
            except Exception as exc:  # pragma: no cover — network noise
                logger.warning("ml_sales day failed", day=day.isoformat(), error=str(exc))
                continue
            for key, v in agg.items():
                cur = total_agg.setdefault(
                    key,
                    {"qty": 0, "sku": None, "revenue": 0.0, "full_qty": 0, "full_revenue": 0.0},
                )
                cur["qty"] += v["qty"]
                cur["revenue"] += v.get("revenue", 0.0)
                cur["full_qty"] += v.get("full_qty", 0)
                cur["full_revenue"] += v.get("full_revenue", 0.0)
                if not cur["sku"]:
                    cur["sku"] = v["sku"]
            days_done += 1
        rows: list[dict[str, Any]] = [
            {
                "mlb_id": mlb,
                "sale_date": odate,
                "sku": v["sku"],
                "qty": v["qty"],
                "revenue": round(v["revenue"], 2),
                "full_qty": v["full_qty"],
                "full_revenue": round(v["full_revenue"], 2),
            }
            for (mlb, odate), v in total_agg.items()
            if odate >= start
        ]

        # Substitui a janela inteira em uma transação (idempotente).
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text

            await session.execute(
                text("DELETE FROM ml_sales_daily WHERE sale_date >= :start"), {"start": start}
            )
            if rows:
                # chunked insert para não estourar parâmetros
                for i in range(0, len(rows), 500):
                    chunk = rows[i : i + 500]
                    stmt = pg_insert(MLSalesDailyORM).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["mlb_id", "sale_date"],
                        set_={
                            "qty": stmt.excluded.qty,
                            "sku": stmt.excluded.sku,
                            "revenue": stmt.excluded.revenue,
                            "full_qty": stmt.excluded.full_qty,
                            "full_revenue": stmt.excluded.full_revenue,
                        },
                    )
                    await session.execute(stmt)
            await session.commit()

        stats = {
            "days_requested": days,
            "days_done": days_done,
            "rows": len(rows),
            "units": sum(r["qty"] for r in rows),
            "mlbs": len({r["mlb_id"] for r in rows}),
        }
        logger.info("ml_sales backfill done", **stats)
        return stats
