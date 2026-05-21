"""ML promotion automation service.

Wraps three external surfaces:
- Mercado Livre seller-promotions API (read eligible, future write apply)
- Google Apps Script costs endpoint (planilha MERCADO LIVRE)
- ml_listings table (which MLBs belong to which SKU)

Exposes the decision algorithm as a pure function (``decide_for_item``) that the
router / cron can call without side effects, plus orchestration methods that
persist snapshots and audit-log entries.

This service deliberately does NOT call POST/DELETE on Mercado Livre yet.
Apply is implemented as dry-run only until the operator flips the flag in
production. The action log records dry_run=True to make this explicit.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.config import settings as _settings
from tiny_mirror.infrastructure.repositories.ml_listing_repository import (
    MLListingRepository,
)
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLCostsSnapshotRepository,
    MLPromoActionRepository,
    MLPromoAlertRepository,
    MLPromoCapRepository,
)
from tiny_mirror.services.mercadolivre_token_service import MercadoLivreTokenService

logger = structlog.get_logger(__name__)

ML_API_BASE = "https://api.mercadolibre.com"


# ===========================================================================
# Decision algorithm — pure
# ===========================================================================
@dataclass
class FreightOpt:
    from_price: float
    to_price: float
    price_drop: float
    current_freight: float
    lower_freight: float
    freight_savings: float
    net_gain: float


@dataclass
class Decision:
    action: str  # 'keep', 'activate_candidate', 'create_price_discount', 'skip', 'no_data'
    reason: str
    floor_price: float | None = None
    current_total_pct: float | None = None
    current_seller_pct: float | None = None
    current_meli_pct: float | None = None
    current_price: float | None = None
    current_promo_type: str | None = None
    current_promo_id: str | None = None
    target_total_pct: float | None = None
    target_seller_pct: float | None = None
    target_meli_pct: float | None = None
    target_price: float | None = None
    target_promo_type: str | None = None
    target_promo_id: str | None = None
    target_promo_name: str | None = None
    floor_violated: bool = False
    freight_opt: FreightOpt | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    # Catalog-listing (buy-box) context. Populated only when the caller
    # provides price_to_win info from ML's /items/{MLB}/price_to_win.
    catalog_status: str | None = None  # "winning" | "losing" | "not_listed" | None
    price_to_win: float | None = None
    visit_share: str | None = None
    still_losing: bool = False  # True when price_to_win < floor and we cap'd at floor


def _compute_pct(orig: float | None, price: float | None) -> float | None:
    if not orig or orig <= 0 or price is None:
        return None
    return round((orig - price) / orig * 100, 1)


def _freight_band_for(price: float, bands: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    if not bands:
        return None
    for b in bands:
        mx = b.get("max")
        if b.get("min") is not None and (mx is None or b["min"] <= price <= mx):
            return b
    return None


def _freight_opt_check(
    target_price: float, bands: list[dict[str, Any]] | None
) -> FreightOpt | None:
    """If dropping the price by a few cents crosses into the band below, and the
    freight saving exceeds the price drop, return a FreightOpt suggestion."""
    if not bands or target_price <= 0:
        return None
    current = _freight_band_for(target_price, bands)
    if current is None:
        return None
    idx = bands.index(current)
    if idx == 0:
        return None  # already in lowest band
    prev = bands[idx - 1]
    new_price = prev.get("max")
    if new_price is None or new_price >= target_price:
        return None
    price_drop = round(target_price - new_price, 2)
    cur_cost = current.get("cost") or 0
    prev_cost = prev.get("cost") or 0
    freight_savings = round(cur_cost - prev_cost, 2)
    if freight_savings > price_drop:
        return FreightOpt(
            from_price=target_price,
            to_price=new_price,
            price_drop=price_drop,
            current_freight=cur_cost,
            lower_freight=prev_cost,
            freight_savings=freight_savings,
            net_gain=round(freight_savings - price_drop, 2),
        )
    return None


def _annotate_catalog(decision: Decision, ptw: dict[str, Any] | None) -> Decision:
    """Attach catalog buy-box context to a Decision (read-only)."""
    if not ptw or not isinstance(ptw, dict):
        return decision
    decision.catalog_status = ptw.get("status")
    decision.visit_share = ptw.get("visit_share")
    raw_ptw = ptw.get("price_to_win")
    decision.price_to_win = float(raw_ptw) if raw_ptw is not None else None
    # still_losing = engine output (target_price or current_price) > price_to_win
    if decision.catalog_status == "losing" and decision.price_to_win is not None:
        effective_price = decision.target_price or decision.current_price
        if effective_price is not None and effective_price > decision.price_to_win + 0.005:
            decision.still_losing = True
    return decision


def decide_for_item(
    *,
    promos: list[dict[str, Any]],
    costs: dict[str, Any] | None,
    cap_seller_pct: float,
    margin_floor_price: float | None = None,
    freight_band_opt_enabled: bool = True,
    excluded_types: Iterable[str] = (),
    price_to_win_info: dict[str, Any] | None = None,
) -> Decision:
    """Pure decision function. Inputs:

    - ``promos``     — raw list from GET /seller-promotions/items/{MLB}
    - ``costs``      — dict from GAS endpoint (or None if unavailable)
    - ``cap_seller_pct`` — user-set ceiling on seller share %
    - ``margin_floor_price`` — explicit floor; if None, falls back to costs.sheet_promo_price
    - ``freight_band_opt_enabled`` — opt-in for the 1-cent-down freight trick
    - ``excluded_types`` — list of promotion types to ignore entirely
    - ``price_to_win_info`` — optional dict from /items/{MLB}/price_to_win:
        ``{ current_price, price_to_win, status, visit_share }``.
        When provided, applies the catalog-aware policy:
          • status="winning" AND visit_share="maximum"
            → no new discount; keep_winning.
          • status="losing" AND price_to_win >= floor
            → cap still rules. If cap can't reach price_to_win, decision
              proceeds at the cap-limited price; ``still_losing`` flag set.
          • status="losing" AND price_to_win < floor
            → cap to the floor (cap is inviolable). ``still_losing=True``.

    **INVARIANT (cap is inviolable)**:
        target_price >= max(margin_floor_price, list_price * (1 - cap/100))
    The price_to_win signal can only RAISE the target_price, never lower it.

    Policy: never removes a ``started`` promotion (cap only blocks creates/upgrades).
    """
    excluded = set(excluded_types)

    # ---- catalog-aware short-circuit: winning + maximum share ------------
    ptw = price_to_win_info or {}
    cat_status = ptw.get("status") if isinstance(ptw, dict) else None
    cat_visit_share = ptw.get("visit_share") if isinstance(ptw, dict) else None
    cat_ptw = ptw.get("price_to_win") if isinstance(ptw, dict) else None

    if cat_status == "winning" and cat_visit_share == "maximum":
        return Decision(
            action="keep_winning",
            reason=(
                f"vencendo o catalogo com visit_share=maximum; "
                f"current_price=R$ {ptw.get('current_price')}, "
                f"price_to_win=R$ {cat_ptw}"
            ),
            current_price=float(ptw["current_price"])
            if ptw.get("current_price") is not None
            else None,
            catalog_status=cat_status,
            price_to_win=float(cat_ptw) if cat_ptw is not None else None,
            visit_share=cat_visit_share,
        )

    # ---- pre-process promos ------------------------------------------------
    started: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    orig_price: float | None = None

    for p in promos:
        if p.get("type") in excluded:
            continue
        price = p.get("price") or 0
        op = p.get("original_price") or orig_price or 0
        if op and orig_price is None:
            orig_price = op
        meli_pct = p.get("meli_percentage") or 0
        total_pct = _compute_pct(op, price) if price > 0 else None
        seller_pct = (total_pct - meli_pct) if total_pct is not None else None
        p["_total_pct"] = total_pct
        p["_seller_pct"] = seller_pct
        p["_meli_pct"] = meli_pct
        p["_price"] = price
        if p.get("status") == "started":
            started.append(p)
        elif p.get("status") == "candidate":
            candidates.append(p)

    # If we still don't have an original price, try from costs
    if not orig_price and costs:
        orig_price = costs.get("listPrice")

    if not orig_price:
        return _annotate_catalog(
            Decision(action="no_data", reason="no original price from ML or sheet"),
            price_to_win_info,
        )

    # ---- floor price -------------------------------------------------------
    floor_price = margin_floor_price
    if floor_price is None and costs:
        floor_price = costs.get("promoPrice")
    floor_price = float(floor_price) if floor_price else None

    # ---- best started ------------------------------------------------------
    started_with_pct = [p for p in started if p["_total_pct"] is not None]
    started_with_pct.sort(key=lambda p: p["_total_pct"], reverse=True)
    best_started = started_with_pct[0] if started_with_pct else None
    cur_total_pct = best_started["_total_pct"] if best_started else 0

    # ---- best candidate respecting cap + floor ----------------------------
    best_cand = None
    best_cand_total_pct = 0.0
    best_cand_target_price = None
    for p in candidates:
        min_price = p.get("min_discounted_price")
        if not min_price:
            continue
        meli_pct = p.get("meli_percentage") or 0
        # Highest total% the cap allows (cap is on seller; ML banca extra is bonus)
        max_total_seller_cap = cap_seller_pct + meli_pct
        max_total_ml_min = _compute_pct(orig_price, min_price) or 0
        achievable_total = min(max_total_seller_cap, max_total_ml_min)
        target_price = round(orig_price * (1 - achievable_total / 100), 2)
        # Floor constraint
        if floor_price is not None and target_price < floor_price:
            achievable_total = _compute_pct(orig_price, floor_price) or 0
            target_price = floor_price
        if achievable_total > best_cand_total_pct:
            best_cand_total_pct = achievable_total
            best_cand = p
            best_cand_target_price = target_price

    # ---- decision ----------------------------------------------------------
    floor_violated = bool(
        best_started and floor_price is not None and best_started["_price"] < floor_price
    )

    # Activate candidate when it materially beats current
    if best_cand and best_cand_total_pct > cur_total_pct + 1:
        target_price = best_cand_target_price or 0
        freight_opt = None
        if freight_band_opt_enabled and costs and costs.get("freightBands"):
            opt = _freight_opt_check(target_price, costs["freightBands"])
            if opt:
                # Re-validate floor
                if floor_price is None or opt.to_price >= floor_price:
                    target_price = opt.to_price
                    freight_opt = opt
                    best_cand_total_pct = (
                        _compute_pct(orig_price, target_price) or best_cand_total_pct
                    )
        meli = best_cand.get("meli_percentage") or 0
        return _annotate_catalog(
            Decision(
                action="activate_candidate",
                reason=(
                    f"candidate -{best_cand_total_pct:.1f}% "
                    f"(seller -{best_cand_total_pct - meli:.1f}%) vs atual -{cur_total_pct:.1f}%"
                ),
                floor_price=floor_price,
                current_total_pct=cur_total_pct,
                current_seller_pct=best_started["_seller_pct"] if best_started else None,
                current_meli_pct=best_started["_meli_pct"] if best_started else None,
                current_price=best_started["_price"] if best_started else None,
                current_promo_type=best_started.get("type") if best_started else None,
                current_promo_id=best_started.get("id") if best_started else None,
                target_total_pct=best_cand_total_pct,
                target_seller_pct=best_cand_total_pct - meli,
                target_meli_pct=meli,
                target_price=target_price,
                target_promo_type=best_cand.get("type"),
                target_promo_id=best_cand.get("id"),
                target_promo_name=best_cand.get("name"),
                floor_violated=floor_violated,
                freight_opt=freight_opt,
            ),
            price_to_win_info,
        )

    # Keep current
    if best_started:
        return _annotate_catalog(
            Decision(
                action="keep",
                reason="já tem a melhor promo ativa dentro do cap (política: nunca derruba)",
                floor_price=floor_price,
                current_total_pct=cur_total_pct,
                current_seller_pct=best_started["_seller_pct"],
                current_meli_pct=best_started["_meli_pct"],
                current_price=best_started["_price"],
                current_promo_type=best_started.get("type"),
                current_promo_id=best_started.get("id"),
                floor_violated=floor_violated,
            ),
            price_to_win_info,
        )

    # Fallback: PRICE_DISCOUNT respecting floor
    if cap_seller_pct > 0:
        cap_target_price = round(orig_price * (1 - cap_seller_pct / 100), 2)
        target_price = max(cap_target_price, floor_price) if floor_price else cap_target_price
        actual_pct = _compute_pct(orig_price, target_price) or 0
        freight_opt = None
        if freight_band_opt_enabled and costs and costs.get("freightBands"):
            opt = _freight_opt_check(target_price, costs["freightBands"])
            if opt and (floor_price is None or opt.to_price >= floor_price):
                target_price = opt.to_price
                freight_opt = opt
                actual_pct = _compute_pct(orig_price, target_price) or actual_pct
        return _annotate_catalog(
            Decision(
                action="create_price_discount",
                reason=f"sem promo ativa; criar PRICE_DISCOUNT -{actual_pct:.1f}% (piso: R$ {floor_price})",
                floor_price=floor_price,
                target_total_pct=actual_pct,
                target_seller_pct=actual_pct,
                target_meli_pct=0,
                target_price=target_price,
                target_promo_type="PRICE_DISCOUNT",
                freight_opt=freight_opt,
            ),
            price_to_win_info,
        )

    return _annotate_catalog(
        Decision(action="skip", reason="sem cap configurado"), price_to_win_info
    )


# ===========================================================================
# Service — fetch + persist + decide + log
# ===========================================================================
class MLPromotionService:
    def __init__(
        self,
        *,
        token_service: MercadoLivreTokenService,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._token_service = token_service
        self._http = http_client

    # -- ML promotions ----------------------------------------------------
    async def fetch_eligible_promos(self, mlb_id: str) -> list[dict[str, Any]]:
        token = await self._token_service.get_valid_access_token()
        resp = await self._http.get(
            f"{ML_API_BASE}/seller-promotions/items/{mlb_id}",
            params={"app_version": "v2"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=15.0,
        )
        if resp.status_code == 401:
            token = await self._token_service.handle_unauthorized()
            resp = await self._http.get(
                f"{ML_API_BASE}/seller-promotions/items/{mlb_id}",
                params={"app_version": "v2"},
                headers={"Authorization": f"Bearer {token}"},
                timeout=15.0,
            )
        if resp.status_code >= 400:
            logger.warning(
                "ml_promo_fetch_failed",
                mlb_id=mlb_id,
                status=resp.status_code,
                body=resp.text[:300],
            )
            return []
        body = resp.json()
        return body if isinstance(body, list) else []

    # -- Price-to-win (catalog buy-box info from ML) ----------------------
    async def fetch_price_to_win(self, mlb_id: str) -> dict[str, Any] | None:
        """Pull catalog-listing competitive info for an MLB.

        Returns the raw ML payload from ``/items/{MLB}/price_to_win``:
            { item_id, current_price, price_to_win, status,
              visit_share, winner: {item_id, price}, catalog_product_id, ... }

        Returns None on HTTP error, on non-catalog items (404), or on
        token failure. The decision engine treats absent info as "no signal"
        and falls back to the cap-only policy.
        """
        token = await self._token_service.get_valid_access_token()
        url = f"{ML_API_BASE}/items/{mlb_id}/price_to_win"
        try:
            resp = await self._http.get(
                url, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
            )
            if resp.status_code == 401:
                token = await self._token_service.handle_unauthorized()
                resp = await self._http.get(
                    url, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
                )
            if resp.status_code >= 400:
                # 404 here is normal for non-catalog items; only log other errors.
                if resp.status_code != 404:
                    logger.debug(
                        "price_to_win_fetch_non_2xx",
                        mlb_id=mlb_id,
                        status=resp.status_code,
                    )
                return None
            body = resp.json()
            return body if isinstance(body, dict) else None
        except (httpx.RequestError, json.JSONDecodeError) as e:
            logger.warning("price_to_win_fetch_failed", mlb_id=mlb_id, error=str(e))
            return None

    # -- GAS costs --------------------------------------------------------
    async def fetch_gas_costs(self, mlb_id: str) -> dict[str, Any] | None:
        """Single-MLB cost lookup via the unified GAS endpoint.

        Prefer ``CostRefreshService.refresh_all_from_bulk`` for batch use
        — this method exists for ad-hoc one-offs only.
        """
        if not _settings.gas_base_url or not _settings.gas_token:
            return {"error": "GAS not configured"}
        try:
            resp = await self._http.get(
                _settings.gas_base_url,
                params={
                    "action": "cost",
                    "mlbid": mlb_id,
                    "token": _settings.gas_token,
                },
                timeout=_settings.gas_http_timeout_seconds,
                follow_redirects=True,
            )
            if resp.status_code >= 400:
                return {"error": f"HTTP {resp.status_code}"}
            body = resp.json()
            if isinstance(body, dict):
                return body
            return None
        except (httpx.RequestError, json.JSONDecodeError) as e:
            logger.warning("gas_fetch_failed", mlb_id=mlb_id, error=str(e))
            return {"error": str(e)}

    # -- Snapshot persist -------------------------------------------------
    async def refresh_costs_for_mlb(
        self, session: AsyncSession, mlb_id: str
    ) -> dict[str, Any] | None:
        snapshot_repo = MLCostsSnapshotRepository(session)
        body = await self.fetch_gas_costs(mlb_id)
        if not body:
            await snapshot_repo.upsert(
                mlb_id=mlb_id,
                sku="",
                active_on_sheet=False,
                base_cost=None,
                commission_pct=None,
                commission_label=None,
                list_price=None,
                sheet_promo_price=None,
                sheet_discount_pct=None,
                sheet_margin_pct=None,
                sheet_margin_value=None,
                freight_bands=None,
                fetch_error="no response",
            )
            return None
        if "error" in body:
            await snapshot_repo.upsert(
                mlb_id=mlb_id,
                sku="",
                active_on_sheet=False,
                base_cost=None,
                commission_pct=None,
                commission_label=None,
                list_price=None,
                sheet_promo_price=None,
                sheet_discount_pct=None,
                sheet_margin_pct=None,
                sheet_margin_value=None,
                freight_bands=None,
                fetch_error=str(body["error"])[:500],
            )
            return body
        # Persist
        await snapshot_repo.upsert(
            mlb_id=mlb_id,
            sku=body.get("sku") or "",
            active_on_sheet=bool(body.get("active")),
            base_cost=Decimal(str(body["baseCost"])) if body.get("baseCost") is not None else None,
            commission_pct=Decimal(str(body["commissionPct"]))
            if body.get("commissionPct") is not None
            else None,
            commission_label=body.get("commissionLabel"),
            list_price=Decimal(str(body["listPrice"]))
            if body.get("listPrice") is not None
            else None,
            sheet_promo_price=Decimal(str(body["promoPrice"]))
            if body.get("promoPrice") is not None
            else None,
            sheet_discount_pct=Decimal(str(body["discountPct"]))
            if body.get("discountPct") is not None
            else None,
            sheet_margin_pct=Decimal(str(body["currentMarginPct"]))
            if body.get("currentMarginPct") is not None
            else None,
            sheet_margin_value=Decimal(str(body["currentMarginValue"]))
            if body.get("currentMarginValue") is not None
            else None,
            freight_bands=body.get("freightBands"),
            fetch_error=None,
        )
        return body

    # -- Decision (with persistence side-effects) -------------------------
    async def evaluate_sku(
        self,
        session: AsyncSession,
        sku: str,
        *,
        dry_run: bool = True,
        actor: str = "cron",
    ) -> list[dict[str, Any]]:
        """Evaluate one SKU end-to-end. Returns a list of decisions (one per MLB).

        Always dry_run=True for now until ML write is enabled.
        """
        caps = MLPromoCapRepository(session)
        actions = MLPromoActionRepository(session)
        alerts = MLPromoAlertRepository(session)
        listings = MLListingRepository(session)

        cap = await caps.get(sku)
        if cap is None:
            return []  # not configured

        # Find active MLBs for this SKU
        mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
        results: list[dict[str, Any]] = []
        for mlb_id in mlb_ids:
            promos = await self.fetch_eligible_promos(mlb_id)
            costs = await self.fetch_gas_costs(mlb_id)
            decision = decide_for_item(
                promos=promos,
                costs=costs,
                cap_seller_pct=float(cap.max_seller_share_pct),
                margin_floor_price=float(cap.margin_floor_price)
                if cap.margin_floor_price
                else None,
                freight_band_opt_enabled=cap.freight_band_opt,
                excluded_types=cap.excluded_promo_types or (),
            )

            # Audit
            await actions.log(
                sku=sku,
                mlb_id=mlb_id,
                action=decision.action,
                promo_type=decision.target_promo_type or decision.current_promo_type,
                promo_id=decision.target_promo_id or decision.current_promo_id,
                price_before=Decimal(str(decision.current_price))
                if decision.current_price
                else None,
                price_after=Decimal(str(decision.target_price)) if decision.target_price else None,
                total_pct=Decimal(str(decision.target_total_pct))
                if decision.target_total_pct is not None
                else None,
                seller_pct=Decimal(str(decision.target_seller_pct))
                if decision.target_seller_pct is not None
                else None,
                meli_pct=Decimal(str(decision.target_meli_pct))
                if decision.target_meli_pct is not None
                else None,
                reason=decision.reason,
                ml_response=None,
                dry_run=dry_run,
            )

            # Alerts
            if decision.floor_violated:
                await alerts.create(
                    sku=sku,
                    mlb_id=mlb_id,
                    kind="floor_violation",
                    message=(
                        f"Promo started -{decision.current_total_pct}% R$ {decision.current_price} "
                        f"abaixo do piso R$ {decision.floor_price}"
                    ),
                    data={
                        "current_total_pct": decision.current_total_pct,
                        "current_price": decision.current_price,
                        "floor_price": decision.floor_price,
                    },
                )
            if decision.freight_opt:
                await alerts.create(
                    sku=sku,
                    mlb_id=mlb_id,
                    kind="freight_opt_pending",
                    message=(
                        f"Oportunidade: R$ {decision.freight_opt.from_price} → "
                        f"R$ {decision.freight_opt.to_price} (NET +R$ {decision.freight_opt.net_gain})"
                    ),
                    data={
                        "from_price": decision.freight_opt.from_price,
                        "to_price": decision.freight_opt.to_price,
                        "net_gain": decision.freight_opt.net_gain,
                    },
                )
            if costs is None or (isinstance(costs, dict) and costs.get("error")):
                await alerts.create(
                    sku=sku,
                    mlb_id=mlb_id,
                    kind="no_cost_data",
                    message=f"Sem dado de custo na planilha para {mlb_id}",
                    data={"raw_error": (costs or {}).get("error")},
                )

            results.append(
                {
                    "mlb_id": mlb_id,
                    "decision": decision,
                    "actor": actor,
                    "dry_run": dry_run,
                }
            )
        return results

    # -- Pure analysis (no persistence, no ML write, no GAS call) ------------
    async def analyze_sku_dry(
        self,
        session: AsyncSession,
        sku: str,
    ) -> list[dict[str, Any]]:
        """Like ``evaluate_sku`` but with zero side-effects.

        - Reads the cap and the cost SNAPSHOT from Postgres (no GAS call).
        - Calls ML /seller-promotions to fetch live eligible promos.
        - Runs the pure ``decide_for_item`` engine.
        - Does NOT write to ``ml_promo_actions`` or ``ml_promo_alerts``.
        - Does NOT call any ML write endpoint.

        Designed to be safe to run across the whole catalog every day for
        forecasting and trend analysis.
        """
        caps = MLPromoCapRepository(session)
        listings = MLListingRepository(session)
        snap_repo = MLCostsSnapshotRepository(session)

        cap = await caps.get(sku)
        if cap is None:
            return []

        mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
        results: list[dict[str, Any]] = []
        for mlb_id in mlb_ids:
            promos = await self.fetch_eligible_promos(mlb_id)
            snap = await snap_repo.get(mlb_id)
            costs = _snapshot_to_costs(snap) if snap else None
            # Catalog buy-box info. None when item is not in a catalog
            # listing (no competition) — the engine then falls back to
            # the cap-only policy.
            price_to_win_info = await self.fetch_price_to_win(mlb_id)
            decision = decide_for_item(
                promos=promos,
                costs=costs,
                cap_seller_pct=float(cap.max_seller_share_pct),
                margin_floor_price=float(cap.margin_floor_price)
                if cap.margin_floor_price
                else None,
                freight_band_opt_enabled=cap.freight_band_opt,
                excluded_types=cap.excluded_promo_types or (),
                price_to_win_info=price_to_win_info,
            )
            results.append({"mlb_id": mlb_id, "decision": decision})
        return results


def _snapshot_to_costs(snap: Any) -> dict[str, Any]:
    """Convert an ml_costs_snapshot row into the camelCase dict shape that
    ``decide_for_item`` expects (mirrors the GAS endpoint payload)."""

    def _f(v: Any) -> float | None:
        return None if v is None else float(v)

    return {
        "listPrice": _f(snap.list_price),
        "promoPrice": _f(snap.sheet_promo_price),
        "freightBands": snap.freight_bands,
    }
