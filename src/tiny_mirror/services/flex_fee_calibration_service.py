"""Flex fee calibration — refresh ``ml_flex_fee_calibration`` from settled orders.

For **Flex / non-fulfillment** listings the spreadsheet commission and the
generic freight bands are wrong (see [[margin-model-fees]]). This service
re-derives, per Flex MLB, what ML actually charged from the last ``days`` of
settled orders:

  real_comm_pct          = median(order_item.sale_fee / unit_price * 100)
  freight_per_unit_<79/≥79 = mean(shipment senders.cost / qty), split at R$79
  payback_per_unit_<79/≥79 = mean(shipment senders.save / qty), split at R$79

``sale_fee`` is per UNIT; ``senders.cost``/``senders.save`` are per SHIPMENT
(÷ qty). Fulfillment is never touched. Every ACTIVE Flex listing gets a row —
those without their own sales/freight fall back to the global Flex mean so the
margin model never silently reverts them to the (wrong) fulfillment bands.

Runs weekly from the scheduler; also exposed via POST /ml-promotions/calibrate-flex.
"""

from __future__ import annotations

import asyncio
import statistics as st
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import structlog
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import (
    MLCostsSnapshotORM,
    MLFlexFeeCalibrationORM,
    MLListingORM,
)

logger = structlog.get_logger(__name__)

_API = "https://api.mercadolibre.com"
_REP_ZIP = "01310100"  # Av. Paulista, SP — CEP representativo p/ estimar frete dos sem-venda
_PAGE = 50
_VALID = {"paid", "shipped", "delivered", "partially_paid"}


class FlexFeeCalibrationService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient, ml_user_id: str) -> None:
        self._tok = token_service
        self._http = http_client
        self._uid = ml_user_id

    async def _auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {await self._tok.get_valid_access_token()}"}

    async def _get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        r = await self._http.get(url, headers=await self._auth(), params=params, timeout=30.0)
        if r.status_code == 401:
            # The cached token is the one that just got rejected — force a
            # refresh instead of re-reading the same token from Redis.
            access = await self._tok.handle_unauthorized()
            headers = {"Authorization": f"Bearer {access}"}
            r = await self._http.get(url, headers=headers, params=params, timeout=30.0)
        if r.status_code >= 400:
            return None
        return r.json()

    async def _seller_freight_estimate(self, mlb: str) -> float | None:
        """Frete do vendedor pela opção GRÁTIS no CEP representativo (SP), IGUAL ao
        Mercado Turbo: ``GET /items/{mlb}/shipping_options`` → MAIOR ``list_cost`` entre as
        opções com ``cost==0`` (a entrega padrão a domicílio; o ML pode listar uma agência
        mais barata, mas a referência usa a cheia). Ex.: MLB6447789292 → 14,45 (= Turbo).
        Retorna ``0.0`` se o anúncio não tem frete grátis no CEP (vendedor não paga frete);
        ``None`` só em FALHA de API (o caller cai pra média global)."""
        d = await self._get(f"{_API}/items/{mlb}/shipping_options", {"zip_code": _REP_ZIP})
        if not isinstance(d, dict) or "options" not in d:
            return None
        free = [
            float(o.get("list_cost"))
            for o in (d.get("options") or [])
            if (o.get("cost") or 0) == 0 and o.get("list_cost") is not None
        ]
        return round(max(free), 2) if free else 0.0

    async def _orders_for_day(self, day: date) -> list[dict[str, Any]]:
        """Return raw order-item rows for one day: mlb, qty, unit_price, sale_fee, shipping_id."""
        frm = f"{day.isoformat()}T00:00:00.000-00:00"
        to = f"{day.isoformat()}T23:59:59.999-00:00"
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            data = await self._get(
                f"{_API}/orders/search",
                {
                    "seller": self._uid,
                    "order.date_created.from": frm,
                    "order.date_created.to": to,
                    "sort": "date_asc",
                    "offset": offset,
                    "limit": _PAGE,
                },
            )
            results = (data or {}).get("results") or []
            for o in results:
                if (o.get("status") or "") not in _VALID:
                    continue
                shipping_id = (o.get("shipping") or {}).get("id")
                for it in o.get("order_items") or []:
                    mlb = (it.get("item") or {}).get("id")
                    price = it.get("unit_price")
                    sale_fee = it.get("sale_fee")
                    qty = int(it.get("quantity") or 1) or 1
                    if not mlb or price is None or sale_fee is None or price <= 0:
                        continue
                    rows.append(
                        {
                            "mlb": mlb,
                            "qty": qty,
                            "price": float(price),
                            "sale_fee": float(sale_fee),
                            "shipping_id": shipping_id,
                        }
                    )
            if not results:
                break
            total = ((data or {}).get("paging") or {}).get("total")
            offset += _PAGE
            # Missing/zero paging metadata must not truncate a non-empty page;
            # keep going until an empty page (or the 10k offset hard cap).
            if (total is not None and int(total) > 0 and offset >= int(total)) or offset >= 10000:
                break
        return rows

    async def recalibrate(self, *, days: int = 90, max_shipments: int = 3000) -> dict[str, Any]:
        logger.info("flex_fee_calibration started", days=days, max_shipments=max_shipments)
        today = datetime.now(UTC).date()
        start = today - timedelta(days=days - 1)

        # --- logistic_type per MLB (Flex = non-fulfillment) -----------------
        async with AsyncSessionLocal() as session:
            logi_rows = (
                await session.execute(
                    select(MLListingORM.mlb_id, MLListingORM.logistic_type, MLListingORM.status)
                )
            ).all()
            sku_rows = (
                await session.execute(select(MLCostsSnapshotORM.mlb_id, MLCostsSnapshotORM.sku))
            ).all()
        logi = {r[0]: r[1] for r in logi_rows}
        status = {r[0]: r[2] for r in logi_rows}
        sku_by_mlb = {r[0]: r[1] for r in sku_rows}

        def is_flex(mlb: str) -> bool:
            lt = logi.get(mlb)
            return lt is not None and lt != "fulfillment"

        # --- pull orders, keep only Flex MLBs -------------------------------
        order_rows: list[dict[str, Any]] = []
        for i in range(days):
            day = start + timedelta(days=i)
            try:
                day_rows = await self._orders_for_day(day)
            except Exception as exc:  # pragma: no cover — network noise
                logger.warning("flex_calib day failed", day=day.isoformat(), error=str(exc))
                continue
            order_rows.extend(r for r in day_rows if is_flex(r["mlb"]))

        by: dict[str, dict[str, Any]] = {}
        for r in order_rows:
            d = by.setdefault(
                r["mlb"], {"rates": [], "fr_lt": [], "fr_ge": [], "pb_lt": [], "pb_ge": []}
            )
            d["rates"].append(r["sale_fee"] / r["price"] * 100)

        # --- FRETE: padronizado no shipping_options do ML (igual ao Mercado Turbo) pra
        # TODOS os anúncios Flex ativos — não mais o frete-por-envio. A cota do ML é o que
        # o Turbo usa (pode superestimar vs o cobrado real, mas o operador quer bater com a
        # ferramenta). A comissão segue do real das vendas (real_comm_pct).
        active_flex = [mlb for mlb in logi if status.get(mlb) == "active" and is_flex(mlb)]
        _fsem = asyncio.Semaphore(8)

        async def _est(mlb: str) -> tuple[str, float | None]:
            async with _fsem:
                return mlb, await self._seller_freight_estimate(mlb)

        freight_by = dict(await asyncio.gather(*[_est(m) for m in active_flex]))
        ok_vals = [v for v in freight_by.values() if v is not None]
        fb_freight = round(st.mean(ok_vals), 2) if ok_vals else 0.0  # falha de API → média
        n_freight_api = len(ok_vals)

        # --- assemble: 1 linha por anúncio Flex ATIVO — frete do shipping_options pra
        # todos (igual ao Turbo); comissão real das vendas quando houver. A cota do frete
        # independe do preço, então vale pras duas faixas (lt79/ge79). payback=0 (a cota já
        # é o bruto que o vendedor paga; o subsídio do ML é o da promo, via meli_banca).
        values: list[dict[str, Any]] = []
        for mlb in active_flex:
            fr = freight_by.get(mlb)
            fr = fr if fr is not None else fb_freight
            rates = by.get(mlb, {}).get("rates", [])
            values.append(
                {
                    "mlb_id": mlb,
                    "sku": sku_by_mlb.get(mlb),
                    "n_sales": len(rates),
                    "real_comm_pct": round(st.median(rates), 2) if rates else None,
                    "freight_per_unit_lt79": fr,
                    "freight_per_unit_ge79": fr,
                    "payback_per_unit_lt79": 0.0,
                    "payback_per_unit_ge79": 0.0,
                    "n_freight_lt79": 0,
                    "n_freight_ge79": 0,
                }
            )

        async with AsyncSessionLocal() as session:
            await session.execute(text("DELETE FROM ml_flex_fee_calibration"))
            for j in range(0, len(values), 500):
                chunk = values[j : j + 500]
                stmt = pg_insert(MLFlexFeeCalibrationORM).values(chunk)
                await session.execute(stmt)
            await session.commit()

        stats = {
            "days": days,
            "active_flex_listings": len(active_flex),
            "flex_mlbs_with_sales": len(by),
            "freight_from_ml_endpoint": n_freight_api,
            "freight_api_fallback_mean": fb_freight,
            "rows_written": len(values),
        }
        logger.info("flex_fee_calibration done", **stats)
        return stats
