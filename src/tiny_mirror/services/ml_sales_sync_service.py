"""ML sales sync — vendas por anúncio (MLB) por dia, do Mercado Livre.

Os pedidos do Tiny só guardam o SKU. Pra dividir vendas por anúncio (o que
importa pras promoções) buscamos da ML Orders API (``/orders/search``), que
traz ``order_items[].item.id`` (MLB) + seller_sku + quantity + date_created.
Agrega por (mlb_id, dia) e faz upsert em ``ml_sales_daily``.

Janela por dia (o ``offset`` da busca do ML satura em 10k; por dia o volume
fica bem abaixo disso). Conta só pedidos pagos/enviados/entregues.
"""

from __future__ import annotations

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


class MLSalesSyncService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient, ml_user_id: str) -> None:
        self._tok = token_service
        self._http = http_client
        self._uid = ml_user_id

    async def _auth(self) -> dict[str, str]:
        access = await self._tok.get_valid_access_token()
        return {"Authorization": f"Bearer {access}"}

    async def _fetch_day(self, day: date) -> dict[tuple[str, date], dict[str, Any]]:
        """Agrega vendas (mlb, dia) -> {qty, sku} de um único dia."""
        frm = f"{day.isoformat()}T00:00:00.000-00:00"
        to = f"{day.isoformat()}T23:59:59.999-00:00"
        agg: dict[tuple[str, date], dict[str, Any]] = {}
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
                headers = await self._auth()
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
                for it in o.get("order_items") or []:
                    item = it.get("item") or {}
                    mlb = item.get("id")
                    if not mlb:
                        continue
                    qty = int(it.get("quantity") or 0)
                    if qty <= 0:
                        continue
                    key = (mlb, odate)
                    cur = agg.setdefault(key, {"qty": 0, "sku": None})
                    cur["qty"] += qty
                    if not cur["sku"]:
                        cur["sku"] = item.get("seller_sku")
            total = (data.get("paging") or {}).get("total", 0)
            offset += _PAGE
            if offset >= total or offset >= 10000 or not results:
                break
        return agg

    async def backfill(self, days: int = 90) -> dict[str, Any]:
        """Reconstrói ml_sales_daily dos últimos ``days`` dias (incl. hoje)."""
        logger.info("ml_sales backfill started", days=days)
        today = datetime.now(UTC).date()
        start = today - timedelta(days=days - 1)
        rows: list[dict[str, Any]] = []
        days_done = 0
        for i in range(days):
            day = start + timedelta(days=i)
            try:
                agg = await self._fetch_day(day)
            except Exception as exc:  # pragma: no cover — network noise
                logger.warning("ml_sales day failed", day=day.isoformat(), error=str(exc))
                continue
            for (mlb, odate), v in agg.items():
                rows.append({"mlb_id": mlb, "sale_date": odate, "sku": v["sku"], "qty": v["qty"]})
            days_done += 1

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
                        set_={"qty": stmt.excluded.qty, "sku": stmt.excluded.sku},
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
