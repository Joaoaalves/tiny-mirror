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
from typing import Any, ClassVar

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

    # Grade de preços p/ mapear a escada de comissão. Densa nas quebras conhecidas
    # (o "vale" de tarifa reduzida costuma abrir em ~100-150 e fechar em ~500-800).
    _FEE_GRID: ClassVar[list[int]] = [
        12,
        15,
        20,
        25,
        30,
        40,
        50,
        60,
        70,
        79,
        80,
        90,
        100,
        110,
        120,
        130,
        140,
        150,
        175,
        200,
        250,
        300,
        350,
        400,
        450,
        500,
        550,
        600,
        700,
        800,
        900,
        1000,
        1200,
        1500,
        2000,
        3000,
        5000,
    ]

    @staticmethod
    def _parse_dims(attrs: list[dict[str, Any]]) -> str | None:
        """``"AxLxC,gramas"`` a partir dos atributos do item — preferindo as medidas
        declaradas pelo vendedor (SELLER_PACKAGE_*, as que o Mercado Turbo usa no
        tooltip), com fallback nas certificadas (PACKAGE_*). None se incompleto."""

        def num(vid: str) -> float | None:
            for a in attrs:
                if a.get("id") == vid:
                    raw = str(a.get("value_name") or "")
                    try:
                        return float(raw.split()[0].replace(",", "."))
                    except (ValueError, IndexError):
                        return None
            return None

        for prefix in ("SELLER_PACKAGE_", "PACKAGE_"):
            h = num(f"{prefix}HEIGHT")
            w = num(f"{prefix}WIDTH")
            ln = num(f"{prefix}LENGTH")
            g = num(f"{prefix}WEIGHT")
            if None not in (h, w, ln, g):
                return f"{h:g}x{w:g}x{ln:g},{g:g}"
        return None

    async def _item_meta(self, mlbs: list[str]) -> dict[str, dict[str, Any]]:
        """``{mlb: {cat, lt, dims}}`` via o multiget de items (1 chamada por 20)."""
        out: dict[str, dict[str, Any]] = {}
        for i in range(0, len(mlbs), 20):
            data = await self._get(
                f"{_API}/items",
                {
                    "ids": ",".join(mlbs[i : i + 20]),
                    "attributes": "id,category_id,listing_type_id,attributes",
                },
            )
            for e in data or []:
                b = e.get("body") or {}
                mid = b.get("id")
                if not mid:
                    continue
                out[mid] = {
                    "cat": b.get("category_id"),
                    "lt": b.get("listing_type_id"),
                    "dims": self._parse_dims(b.get("attributes") or []),
                }
        return out

    # Faixas de preço da tabela de frete do ML (mesmas da planilha/Mercado Turbo);
    # sondamos a calculadora com um preço no MIOLO de cada faixa.
    _FREIGHT_BRACKETS: ClassVar[list[tuple[float, float | None, float]]] = [
        (0, 18.99, 12),
        (19, 48.99, 30),
        (49, 78.99, 60),
        (79, 99.99, 85),
        (100, 119.99, 110),
        (120, 149.99, 135),
        (150, 199.99, 175),
        (200, None, 250),
    ]

    async def _freight_schedule(self, dims: str, lt: str | None) -> list[dict[str, Any]] | None:
        """Tabela de frete do vendedor por faixa de preço para um pacote ``dims``,
        via a calculadora do ML (``/users/{uid}/shipping_options/free`` com
        ``item_price``) — o MESMO frete que o Mercado Turbo exibe (validado 7/7
        exato). Uma sonda por faixa; ``None`` se nenhuma responder."""
        bands: list[dict[str, Any]] = []
        for lo, hi, probe in self._FREIGHT_BRACKETS:
            d = await self._get(
                f"{_API}/users/{self._uid}/shipping_options/free",
                {
                    "dimensions": dims,
                    "item_price": probe,
                    "listing_type_id": lt or "gold_special",
                    "mode": "me2",
                    "condition": "new",
                },
            )
            cost = (((d or {}).get("coverage") or {}).get("all_country") or {}).get("list_cost")
            if cost is None:
                continue
            bands.append({"min": lo, "max": hi, "cost": round(float(cost), 2)})
        if not bands:
            return None
        # faixas contíguas de mesmo custo colapsam numa só (menos ruído no JSON)
        merged: list[dict[str, Any]] = [bands[0]]
        for b in bands[1:]:
            if b["cost"] == merged[-1]["cost"]:
                merged[-1]["max"] = b["max"]
            else:
                merged.append(b)
        return merged

    async def _commission_schedule(self, cat: str, lt: str) -> list[dict[str, Any]] | None:
        """Nominal ML fee schedule for a (category, listing_type), probed from
        ``/sites/MLB/listing_prices`` across a price grid and compressed into
        ``[{min, max, pct}]`` bands (inclusive ends, last ``max=None``). This is
        the fee Mercado Turbo charges (``sale_fee = pct x price``, fixed_fee 0 for
        our categories). Returns ``None`` on total API failure."""
        pts: list[tuple[float, float]] = []
        for price in self._FEE_GRID:
            d = await self._get(
                f"{_API}/sites/MLB/listing_prices",
                {"price": price, "category_id": cat, "listing_type_id": lt},
            )
            if not isinstance(d, dict):
                continue
            det = d.get("sale_fee_details") or {}
            pct = det.get("percentage_fee")
            if pct is None:
                sf = d.get("sale_fee_amount")
                pct = round(float(sf) / price * 100, 2) if sf and price else None
            if pct is not None:
                pts.append((float(price), round(float(pct), 2)))
        if not pts:
            return None
        # comprime pontos consecutivos de mesma % numa banda; a quebra fica no
        # ponto médio entre a última amostra de uma % e a primeira da próxima.
        bands: list[dict[str, Any]] = []
        seg_lo: float = 0.0
        cur_pct = pts[0][1]
        prev_price = pts[0][0]
        for pr, pc in pts[1:]:
            if pc != cur_pct:
                boundary = round((prev_price + pr) / 2, 2)
                bands.append({"min": seg_lo, "max": boundary, "pct": cur_pct})
                seg_lo = boundary
                cur_pct = pc
            prev_price = pr
        bands.append({"min": seg_lo, "max": None, "pct": cur_pct})
        return bands

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

        # --- FRETE flat (fallback Flex): cota do shipping_options no preço atual —
        # usada só quando o item não tem dimensões pra tabela por faixa.
        active_flex = [mlb for mlb in logi if status.get(mlb) == "active" and is_flex(mlb)]
        # Fulfillment entra SÓ pro frete por faixa (calculadora) — comissão/custo do
        # FULL seguem intocados (planilha), por decisão do operador (2026-07-08:
        # frete de TODOS pela calculadora; a cerca segue valendo pro resto).
        active_full = [
            mlb for mlb in logi if status.get(mlb) == "active" and logi.get(mlb) == "fulfillment"
        ]
        _fsem = asyncio.Semaphore(8)

        async def _est(mlb: str) -> tuple[str, float | None]:
            async with _fsem:
                return mlb, await self._seller_freight_estimate(mlb)

        freight_by = dict(await asyncio.gather(*[_est(m) for m in active_flex]))
        ok_vals = [v for v in freight_by.values() if v is not None]
        fb_freight = round(st.mean(ok_vals), 2) if ok_vals else 0.0  # falha de API → média
        n_freight_api = len(ok_vals)

        # --- metadados por item (1 multiget): categoria+tipo (p/ comissão) e
        # dimensões do pacote (p/ frete). Full incluso (frete por faixa).
        meta = await self._item_meta(active_flex + active_full)

        # --- COMISSÃO NOMINAL: escada de tarifa por (categoria, tipo de anúncio),
        # sondada do listing_prices (= o que o Mercado Turbo cobra). A % NÃO é
        # constante no preço, então guardamos bandas por MLB. Poucas (cat,lt)
        # distintas → sonda uma vez cada e reaproveita em todos os MLBs.
        # Vale pra TODOS (Flex e fulfillment — operador autorizou 2026-07-08; a
        # planilha do FULL divergiu da nominal). Só o CUSTO segue intocável.
        cat_lt = {m: (v["cat"], v["lt"]) for m, v in meta.items() if v.get("cat") and v.get("lt")}
        distinct_pairs = sorted(set(cat_lt.values()))
        _csem = asyncio.Semaphore(6)

        async def _sched(
            pair: tuple[str, str],
        ) -> tuple[tuple[str, str], list[dict[str, Any]] | None]:
            async with _csem:
                return pair, await self._commission_schedule(pair[0], pair[1])

        sched_by_pair = dict(await asyncio.gather(*[_sched(p) for p in distinct_pairs]))
        n_sched_ok = sum(1 for v in sched_by_pair.values() if v)

        # --- FRETE POR FAIXA: a calculadora do ML dá o frete do vendedor por
        # (dimensões, faixa de preço) — o flat da cota só vale na faixa em que o
        # anúncio vende HOJE (uma caixa 56L custa 11,75 <79 e 48,55 >=79). Sondamos
        # por (dims, lt) distintos e reaproveitamos (mesma caixa → mesma tabela).
        freight_keys = sorted({(v["dims"], v.get("lt")) for v in meta.values() if v.get("dims")})

        async def _fsched(
            key: tuple[str, str | None],
        ) -> tuple[tuple[str, str | None], list[dict[str, Any]] | None]:
            async with _csem:
                return key, await self._freight_schedule(key[0], key[1])

        fsched_by_key = dict(await asyncio.gather(*[_fsched(k) for k in freight_keys]))
        n_fsched_ok = sum(1 for v in fsched_by_key.values() if v)

        # --- assemble — 1 linha por anúncio ATIVO:
        #   Flex: frete da calculadora por faixa (fallback: cota flat) + comissão
        #     da escada nominal (fallback = mediana real das vendas).
        #   Fulfillment: freight_bands + commission_bands nominais (o CUSTO do FULL
        #     fica da planilha, intocável); sem banda nenhuma → sem linha.
        # payback=0 (a tabela já é o bruto que o vendedor paga; o subsídio do ML é
        # o da promo, via meli_banca).
        values: list[dict[str, Any]] = []
        for mlb in active_flex:
            fr = freight_by.get(mlb)
            fr = fr if fr is not None else fb_freight
            rates = by.get(mlb, {}).get("rates", [])
            comm_bands = sched_by_pair.get(cat_lt[mlb]) if mlb in cat_lt else None
            mm = meta.get(mlb) or {}
            fr_bands = fsched_by_key.get((mm["dims"], mm.get("lt"))) if mm.get("dims") else None
            values.append(
                {
                    "mlb_id": mlb,
                    "sku": sku_by_mlb.get(mlb),
                    "n_sales": len(rates),
                    "real_comm_pct": round(st.median(rates), 2) if rates else None,
                    "commission_bands": comm_bands,
                    "freight_bands": fr_bands,
                    "freight_per_unit_lt79": fr,
                    "freight_per_unit_ge79": fr,
                    "payback_per_unit_lt79": 0.0,
                    "payback_per_unit_ge79": 0.0,
                    "n_freight_lt79": 0,
                    "n_freight_ge79": 0,
                }
            )
        n_full_banded = 0
        for mlb in active_full:
            mm = meta.get(mlb) or {}
            fr_bands = fsched_by_key.get((mm["dims"], mm.get("lt"))) if mm.get("dims") else None
            comm_bands = sched_by_pair.get(cat_lt[mlb]) if mlb in cat_lt else None
            if not fr_bands and not comm_bands:
                continue  # sem banda nenhuma → fulfillment fica 100% na planilha
            n_full_banded += 1
            values.append(
                {
                    "mlb_id": mlb,
                    "sku": sku_by_mlb.get(mlb),
                    "n_sales": 0,
                    "real_comm_pct": None,  # fallback do FULL = comissão da planilha
                    "commission_bands": comm_bands,
                    "freight_bands": fr_bands,
                    "freight_per_unit_lt79": None,
                    "freight_per_unit_ge79": None,
                    "payback_per_unit_lt79": None,
                    "payback_per_unit_ge79": None,
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
            "commission_pairs_probed": len(distinct_pairs),
            "commission_schedules_ok": n_sched_ok,
            "mlbs_with_commission_bands": sum(1 for v in values if v["commission_bands"]),
            "freight_dims_probed": len(freight_keys),
            "freight_schedules_ok": n_fsched_ok,
            "mlbs_with_freight_bands": sum(1 for v in values if v["freight_bands"]),
            "active_full_listings": len(active_full),
            "full_freight_banded": n_full_banded,
            "rows_written": len(values),
        }
        logger.info("flex_fee_calibration done", **stats)
        return stats
