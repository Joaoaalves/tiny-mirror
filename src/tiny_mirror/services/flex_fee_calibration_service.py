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
            r = await self._http.get(url, headers=await self._auth(), params=params, timeout=30.0)
        if r.status_code >= 400:
            return None
        return r.json()

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
            total = ((data or {}).get("paging") or {}).get("total", 0)
            offset += _PAGE
            if offset >= total or offset >= 10000 or not results:
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

        # --- freight: fetch shipment costs, >=79 first then <79 (capped) ----
        seen_ship: dict[str, dict[str, Any] | None] = {}
        sid_price: dict[str, float] = {}
        for r in order_rows:
            sid = r["shipping_id"]
            if sid:
                sid_price[sid] = max(sid_price.get(sid, 0.0), r["price"])
        ordered_sids = sorted(sid_price, key=lambda s: 0 if sid_price[s] >= 79 else 1)
        glob_lt: list[float] = []
        glob_ge: list[float] = []
        n_ship = 0
        for sid in ordered_sids:
            if n_ship >= max_shipments:
                break
            c = await self._get(f"{_API}/shipments/{sid}/costs")
            n_ship += 1
            if c is None:
                seen_ship[sid] = None
                continue
            snd = (c.get("senders") or [{}])[0]
            seen_ship[sid] = {
                "cost": float(snd.get("cost") or 0),
                "save": float(snd.get("save") or 0),
            }
        for r in order_rows:
            sc = seen_ship.get(r["shipping_id"])
            if not sc:
                continue
            per_cost = sc["cost"] / r["qty"]
            per_save = sc["save"] / r["qty"]
            d = by[r["mlb"]]
            if r["price"] >= 79:
                d["fr_ge"].append(per_cost)
                d["pb_ge"].append(per_save)
                glob_ge.append(per_cost)
            else:
                d["fr_lt"].append(per_cost)
                d["pb_lt"].append(per_save)
                glob_lt.append(per_cost)

        fb_lt = round(st.mean(glob_lt), 2) if glob_lt else 0.0
        fb_ge = round(st.mean(glob_ge), 2) if glob_ge else 0.0

        # --- assemble per-MLB rows (+ all active Flex with fallback) --------
        values: list[dict[str, Any]] = []
        for mlb, d in by.items():
            values.append(
                {
                    "mlb_id": mlb,
                    "sku": sku_by_mlb.get(mlb),
                    "n_sales": len(d["rates"]),
                    "real_comm_pct": round(st.median(d["rates"]), 2) if d["rates"] else None,
                    "freight_per_unit_lt79": round(st.mean(d["fr_lt"]), 2) if d["fr_lt"] else fb_lt,
                    "freight_per_unit_ge79": round(st.mean(d["fr_ge"]), 2) if d["fr_ge"] else fb_ge,
                    "payback_per_unit_lt79": round(st.mean(d["pb_lt"]), 2) if d["pb_lt"] else 0.0,
                    "payback_per_unit_ge79": round(st.mean(d["pb_ge"]), 2) if d["pb_ge"] else 0.0,
                    "n_freight_lt79": len(d["fr_lt"]),
                    "n_freight_ge79": len(d["fr_ge"]),
                }
            )
        # Active Flex listings with no sales in window → global fallback row so
        # they don't silently fall back to the wrong fulfillment-style bands.
        # real_comm_pct stays NULL (no data) → override keeps snapshot commission.
        covered = {v["mlb_id"] for v in values}
        n_fallback = 0
        for mlb in logi:
            if mlb in covered or status.get(mlb) != "active" or not is_flex(mlb):
                continue
            n_fallback += 1
            values.append(
                {
                    "mlb_id": mlb,
                    "sku": sku_by_mlb.get(mlb),
                    "n_sales": 0,
                    "real_comm_pct": None,
                    "freight_per_unit_lt79": fb_lt,
                    "freight_per_unit_ge79": fb_ge,
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
            "flex_mlbs_with_sales": len(by),
            "flex_mlbs_fallback": n_fallback,
            "rows_written": len(values),
            "shipments_fetched": n_ship,
            "fallback_freight_lt79": fb_lt,
            "fallback_freight_ge79": fb_ge,
        }
        logger.info("flex_fee_calibration done", **stats)
        return stats
