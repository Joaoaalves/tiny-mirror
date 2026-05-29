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
from datetime import UTC, datetime
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

# Promo types that get extra exposure in ML's UI (interval-based deals).
# Boost is informational — surfaced in the decision so the UI/operator can
# weigh exposure vs margin. Not used for filtering.
EXPOSURE_BOOST_TYPES = frozenset({"DEAL", "DOD", "LIGHTNING"})
EXPOSURE_BOOST_FACTOR = 1.3

# Fixed-price types: ML sets the price; seller can't choose within an
# interval. Interval types let the seller pick a price in [min, max].
FIXED_PRICE_TYPES = frozenset(
    {
        "SMART",
        "PRICE_MATCHING",
        "MARKETPLACE_CAMPAIGN",
        "PRE_NEGOTIATED",
        "PIX",
        "VOLUME",
    }
)


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


# Statuses that mean "we are not on top of the catalog". still_losing
# semantics apply across all of them — the engine respects the cap and
# flags when the cap-limited price is above price_to_win.
_LOSING_STATUSES = frozenset({"losing", "competing"})


def _annotate_catalog(decision: Decision, ptw: dict[str, Any] | None) -> Decision:
    """Attach catalog buy-box context to a Decision (read-only).

    ``still_losing`` fires when our (cap-respected) effective price is
    higher than price_to_win and the catalog status is any of
    ``losing`` / ``competing`` / ``sharing_first_place`` (when not on
    maximum visit_share).
    """
    if not ptw or not isinstance(ptw, dict):
        return decision
    decision.catalog_status = ptw.get("status")
    decision.visit_share = ptw.get("visit_share")
    raw_ptw = ptw.get("price_to_win")
    decision.price_to_win = float(raw_ptw) if raw_ptw is not None else None

    if decision.price_to_win is None:
        return decision
    losing_like = decision.catalog_status in _LOSING_STATUSES or (
        decision.catalog_status == "sharing_first_place" and decision.visit_share != "maximum"
    )
    if losing_like:
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
    skip_when_winning: bool = False,
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

    if cat_status == "winning" and cat_visit_share == "maximum" and skip_when_winning:
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
    # Uses the unified score_candidate_promo helper so every promo type
    # (DEAL, SMART, PRICE_DISCOUNT, SELLER_COUPON_CAMPAIGN, UNHEALTHY_STOCK,
    # …) is evaluated against the same cap + floor envelope.
    best_cand = None
    best_cand_total_pct = 0.0
    best_cand_target_price = None
    for p in candidates:
        scored = score_candidate_promo(
            p,
            cap_seller_pct=cap_seller_pct,
            margin_floor_price=floor_price,
            list_price=orig_price,
        )
        # score agora sempre retorna dict pra promos parseables (com flag
        # accepted). Pulamos os negados aqui — quem decide ativar é só os
        # que passam no cap+floor. enumerate_activations_for_item registra
        # os negados pra UI.
        if scored is None or not scored.get("accepted"):
            continue
        achievable_total = float(scored["target_total_pct"] or 0)
        target_price = float(scored["target_price"] or 0)
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

        sku_caps = await caps.get_by_sku(sku)
        if not sku_caps:
            return []  # not configured
        cap_by_mlb = {c.mlb_id: c for c in sku_caps}

        # Find active MLBs for this SKU
        mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
        results: list[dict[str, Any]] = []
        for mlb_id in mlb_ids:
            cap = cap_by_mlb.get(mlb_id)
            if cap is None:
                continue  # listing has no cap row yet — skip silently
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

        sku_caps = await caps.get_by_sku(sku)
        if not sku_caps:
            return []
        cap_by_mlb = {c.mlb_id: c for c in sku_caps}

        # Catalog status comes from the DB (refreshed daily by
        # CatalogStatusSyncService). This keeps analyze_sku_dry fast and
        # idempotent — every analysis pass over the whole catalog runs in
        # seconds instead of minutes.
        from sqlalchemy import select as _select

        from tiny_mirror.infrastructure.orm.models import MLCatalogStatusORM

        mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
        catalog_by_mlb: dict[str, MLCatalogStatusORM] = {}
        if mlb_ids:
            result = await session.execute(
                _select(MLCatalogStatusORM).where(MLCatalogStatusORM.mlb_id.in_(mlb_ids))
            )
            for row in result.scalars().all():
                catalog_by_mlb[row.mlb_id] = row

        results: list[dict[str, Any]] = []
        for mlb_id in mlb_ids:
            cap = cap_by_mlb.get(mlb_id)
            if cap is None:
                continue  # listing without a cap row yet — skip silently
            promos = await self.fetch_eligible_promos(mlb_id)
            snap = await snap_repo.get(mlb_id)
            costs = _snapshot_to_costs(snap) if snap else None
            cat = catalog_by_mlb.get(mlb_id)
            price_to_win_info = _catalog_row_to_ptw(cat) if cat else None
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
                skip_when_winning=bool(getattr(cap, "skip_when_winning", False)),
            )
            eligible_count = count_eligible_candidates(
                promos=promos,
                cap_seller_pct=float(cap.max_seller_share_pct),
                margin_floor_price=float(cap.margin_floor_price)
                if cap.margin_floor_price
                else None,
                list_price=float(snap.list_price) if snap and snap.list_price else None,
                excluded_types=cap.excluded_promo_types or (),
            )
            results.append(
                {
                    "mlb_id": mlb_id,
                    "decision": decision,
                    "eligible_candidates_in_cap": eligible_count,
                }
            )
        return results

    # ------------------------------------------------------------------
    # Decisions queue (operator approval)
    # ------------------------------------------------------------------
    async def generate_pending_decisions(
        self,
        session: AsyncSession,
        *,
        only_sku: str | None = None,
        limit_skus: int | None = None,
    ) -> dict[str, Any]:
        """Walk every (sku, MLB) and write a pending decision per
        candidate promo that fits the cap+floor. Idempotent: existing
        rows in any status are skipped by the unique constraint.

        Returns aggregate counts so the cron / API can log them.
        """
        from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
            MLPromoDecisionRepository,
        )

        caps = MLPromoCapRepository(session)
        listings = MLListingRepository(session)
        snap_repo = MLCostsSnapshotRepository(session)
        decisions = MLPromoDecisionRepository(session)

        all_caps, _ = await caps.list_all(only_auto=None, limit=2000)
        caps_by_mlb = {c.mlb_id: c for c in all_caps}
        skus = sorted({c.sku for c in all_caps})
        if only_sku is not None:
            skus = [s for s in skus if s == only_sku]
        if limit_skus is not None:
            skus = skus[:limit_skus]

        stats = {
            "skus_scanned": 0,
            "mlbs_scanned": 0,
            "candidates_eligible": 0,
            "decisions_inserted": 0,
            "decisions_skipped_existing": 0,
            "decisions_denied_inserted": 0,
            "decisions_active_inserted": 0,
        }

        for sku in skus:
            stats["skus_scanned"] += 1
            mlb_ids = await listings.get_active_mlb_ids_for_sku(sku)
            for mlb_id in mlb_ids:
                cap = caps_by_mlb.get(mlb_id)
                if cap is None or cap.max_seller_share_pct == 0:
                    continue
                stats["mlbs_scanned"] += 1
                snap = await snap_repo.get(mlb_id)
                list_price = float(snap.list_price) if snap and snap.list_price else None
                try:
                    promos = await self.fetch_eligible_promos(mlb_id)
                except Exception as exc:  # pragma: no cover — network noise
                    logger.debug("decisions_fetch_failed", mlb_id=mlb_id, error=str(exc))
                    continue
                if list_price is None:
                    for p in promos:
                        if p.get("original_price"):
                            list_price = float(p["original_price"])
                            break
                if list_price is None:
                    continue

                entries = enumerate_activations_for_item(
                    promos=promos,
                    cap_seller_pct=float(cap.max_seller_share_pct),
                    margin_floor_price=float(cap.margin_floor_price)
                    if cap.margin_floor_price
                    else None,
                    list_price=list_price,
                    excluded_types=cap.excluded_promo_types or (),
                )
                for entry in entries:
                    entry_status = entry.get("status")
                    # Persistimos as 3 categorias:
                    #   - would_activate → status=pending (operador decide)
                    #   - already_active → status=ignored (só pra visibilidade)
                    #   - denied         → status=ignored (idem)
                    if entry_status == "would_activate":
                        db_status = "pending"
                        decision_kind = "would_activate"
                        stats["candidates_eligible"] += 1
                    elif entry_status == "already_active":
                        db_status = "ignored"
                        decision_kind = "already_active"
                    elif entry_status == "denied":
                        db_status = "ignored"
                        decision_kind = "denied"
                    else:
                        continue  # forma desconhecida

                    promo_id = entry.get("promo_id")
                    promo_key = (
                        promo_id if promo_id else f"CREATE-{entry.get('constraint') or 'unknown'}"
                    )
                    inserted = await decisions.insert_if_absent(
                        mlb_id=mlb_id,
                        sku=sku,
                        promo_key=str(promo_key)[:80],
                        promo_id=promo_id,
                        promo_type=entry.get("promo_type") or "?",
                        promo_name=entry.get("promo_name"),
                        decision_kind=decision_kind,
                        target_price=_to_dec(entry.get("target_price")),
                        target_total_pct=_to_dec(entry.get("target_total_pct")),
                        target_seller_pct=_to_dec(entry.get("target_seller_pct")),
                        meli_percentage=_to_dec(entry.get("meli_percentage")),
                        constraint_used=entry.get("constraint"),
                        list_price=Decimal(str(list_price)),
                        cap_pct=cap.max_seller_share_pct,
                        floor_price=cap.margin_floor_price,
                        reason=entry.get("reason") or "",
                        status=db_status,
                    )
                    if inserted is not None:
                        if decision_kind == "would_activate":
                            stats["decisions_inserted"] += 1
                        elif decision_kind == "denied":
                            stats["decisions_denied_inserted"] += 1
                        else:
                            stats["decisions_active_inserted"] += 1
                    else:
                        stats["decisions_skipped_existing"] += 1
        await session.commit()
        logger.info("decisions_generated", **stats)
        return stats

    async def expire_stale_decisions(
        self,
        session: AsyncSession,
        *,
        price_drift_pct: float | None = None,
        cap_drift_pct: float | None = None,
        floor_drift_pct: float | None = None,
        age_days: int | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Mark pending decisions as ``status='expired'`` when the inputs
        they were built on no longer match reality.

        A decision row carries a snapshot of ``list_price`` /
        ``cap_pct`` / ``floor_price`` at generation time. Hours later
        the daily recompute job may have moved any of those — the
        target_price the operator is about to approve would be wrong.
        We auto-expire instead of silently mutating: the operator can
        re-trigger generation to get fresh rows.

        Rules (any one trips → expire), in priority order so the
        recorded ``expired_reason`` is the *first* one that failed,
        even when multiple do:

        1. ``list_price_drift`` — current list_price moved by more than
           ``price_drift_pct`` % from the snapshot value.
        2. ``cap_changed``     — current cap moved by more than
           ``cap_drift_pct`` percentage points from the snapshot value.
        3. ``floor_changed``   — current floor_price moved by more than
           ``floor_drift_pct`` % from the snapshot value.
        4. ``stale_age``       — created_at older than ``age_days``.

        Thresholds default to the Settings values so the daily cron
        runs with the env-configured policy; explicit args exist for
        the manual API trigger and tests.
        """
        from tiny_mirror.config import settings
        from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
            MLPromoDecisionRepository,
        )

        price_pct = (
            price_drift_pct if price_drift_pct is not None else settings.promo_stale_price_drift_pct
        )
        cap_pct = cap_drift_pct if cap_drift_pct is not None else settings.promo_stale_cap_drift_pct
        floor_pct = (
            floor_drift_pct if floor_drift_pct is not None else settings.promo_stale_floor_drift_pct
        )
        max_age_days = age_days if age_days is not None else settings.promo_stale_age_days
        now = now or datetime.now(UTC)

        caps_repo = MLPromoCapRepository(session)
        snap_repo = MLCostsSnapshotRepository(session)
        decisions_repo = MLPromoDecisionRepository(session)

        # Pull pending rows. The dashboard has thousands but the cron
        # runs in one transaction; bound the page so we don't surprise
        # the DB on a runaway dataset. 5000 is comfortably above the
        # 2.1k current backlog.
        pending_rows, total_pending = await decisions_repo.list_(status="pending", limit=5000)

        by_reason: dict[str, int] = {
            "list_price_drift": 0,
            "cap_changed": 0,
            "floor_changed": 0,
            "stale_age": 0,
        }
        expired_total = 0

        for row in pending_rows:
            reason = self._stale_reason(
                row,
                snap=await snap_repo.get(row.mlb_id),
                cap=await caps_repo.get(row.mlb_id),
                now=now,
                price_drift_pct=price_pct,
                cap_drift_pct=cap_pct,
                floor_drift_pct=floor_pct,
                age_days=max_age_days,
            )
            if reason is None:
                continue
            updated = await decisions_repo.expire(row.id, reason=reason)
            if updated is not None:
                expired_total += 1
                by_reason[reason] = by_reason.get(reason, 0) + 1

        await session.commit()
        stats = {
            "total_pending_seen": total_pending,
            "expired_total": expired_total,
            "by_reason": by_reason,
        }
        logger.info("decisions_expired", **stats)
        return stats

    @staticmethod
    def _stale_reason(
        row: Any,
        *,
        snap: Any,
        cap: Any,
        now: datetime,
        price_drift_pct: float,
        cap_drift_pct: float,
        floor_drift_pct: float,
        age_days: int,
    ) -> str | None:
        """Return the first staleness reason that applies, or None."""
        # 1. list_price drift — compare snapshot list_price to current
        # ml_costs_snapshots list_price. Both nullable, so only fire
        # when both sides exist; a missing snapshot is "no signal" not
        # "stale".
        if row.list_price and snap is not None and snap.list_price:
            row_lp = float(row.list_price)
            cur_lp = float(snap.list_price)
            if row_lp > 0:
                drift = abs(cur_lp - row_lp) / row_lp * 100.0
                if drift > price_drift_pct:
                    return "list_price_drift"

        # 2. cap drift — absolute percentage points.
        if row.cap_pct is not None and cap is not None:
            row_cap = float(row.cap_pct)
            cur_cap = float(cap.max_seller_share_pct)
            if abs(cur_cap - row_cap) > cap_drift_pct:
                return "cap_changed"

        # 3. floor drift — relative %.
        if row.floor_price and cap is not None and cap.margin_floor_price is not None:
            row_floor = float(row.floor_price)
            cur_floor = float(cap.margin_floor_price)
            if row_floor > 0:
                drift = abs(cur_floor - row_floor) / row_floor * 100.0
                if drift > floor_drift_pct:
                    return "floor_changed"

        # 4. plain age. created_at is timezone-aware.
        if row.created_at is not None:
            age = (now - row.created_at).total_seconds() / 86400.0
            if age > age_days:
                return "stale_age"

        return None


def _to_dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def score_candidate_promo(
    p: dict[str, Any],
    *,
    cap_seller_pct: float,
    margin_floor_price: float | None,
    list_price: float,
) -> dict[str, Any] | None:
    """Uniformly score one CANDIDATE promo against the per-MLB cap + floor.

    ML's promo API returns candidates in five different shapes depending
    on the type:

    1. ``fixed_percentage``       — SELLER_COUPON_CAMPAIGN
    2. ``min_discounted_price``   — DEAL / DOD / LIGHTNING / PRICE_DISCOUNT
                                    / SELLER_CAMPAIGN (interval: seller
                                    picks any price in [min, max], with
                                    ``suggested_discounted_price`` as ML's
                                    recommendation for best exposure)
    3. ``price`` + ``seller_percentage`` — SMART / PRICE_MATCHING (ML sets
                                    the price; seller accepts or skips)
    4. ``seller_percentage`` only (no price) — UNHEALTHY_STOCK; we derive
       the target price from ``original_price * (1 - (seller% + meli%)/100)``
    5. ``price`` + ``original_price`` only — generic fallback

    Returns:
      - ``None`` when the promo doesn't match any known shape (data
        integrity issue) — caller should skip silently.
      - ``dict`` always with ``accepted: bool`` and ``denied_reason: str |
        None``. When accepted, the dict carries the activation plan
        (target_price, total_pct, etc). When denied, the same fields
        carry what WOULD have been — UI can show "would have been R$ X
        (-Y%) but blocked by floor R$ Z".

    Extra fields on every entry:
      - ``structure_type``: FIXED_PCT | FIXED_PRICE | INTERVAL | SUGG_ONLY
      - ``is_fixed_price``: True when ML pins the price (no seller choice)
      - ``exposure_boost``: 1.3 for DOD/LIGHTNING/DEAL (extra UI exposure),
        1.0 otherwise — informational, not used for filtering.
    """
    if list_price is None or list_price <= 0:
        return None
    floor = margin_floor_price or 0.0
    meli_pct = float(p.get("meli_percentage") or 0)
    cap_total = cap_seller_pct + meli_pct
    promo_type = (p.get("type") or "").upper()
    exposure_boost = EXPOSURE_BOOST_FACTOR if promo_type in EXPOSURE_BOOST_TYPES else 1.0

    def _copay_line(seller_pct: float) -> str:
        """Linha secundária mostrando quem paga quanto, quando ML co-banca."""
        if meli_pct > 0.01:
            return f" (seller paga {seller_pct:.1f}% · ML banca {meli_pct:.1f}%)"
        return ""

    # Portuguese labels for the cap_label machine token.
    cap_label_pt = {"total": "desconto total", "seller": "seller"}

    def _build(
        *,
        target_price: float,
        target_total_pct: float,
        target_seller_pct: float,
        constraint: str,
        reason: str,
        cap_check_value: float,
        cap_check_limit: float,
        cap_label: str,
        structure_type: str,
    ) -> dict[str, Any]:
        accepted = True
        denied_reason: str | None = None
        # Motivo só explica o "por que esse alvo" — preço/desconto já
        # aparecem na coluna "De → Por". Co-pay (quem paga quanto) entra
        # quando ML banca uma parte.
        ui_reason = reason + _copay_line(target_seller_pct)
        if cap_check_value > cap_check_limit + 0.01:
            accepted = False
            denied_reason = (
                f"cap_exceeded: {cap_label} {cap_check_value:.2f}% > "
                f"limite {cap_check_limit:.2f}%"
            )
            ui_reason = (
                f"Bloqueado pelo cap do {cap_label_pt.get(cap_label, cap_label)} — "
                f"desconto exigido {cap_check_value:.1f}% excede o limite {cap_check_limit:.1f}%"
            )
        elif target_price + 0.01 < floor:
            accepted = False
            denied_reason = f"floor_violation: R$ {target_price:.2f} < piso R$ {floor:.2f}"
            ui_reason = (
                f"Bloqueado pelo piso — preço alvo R$ {target_price:.2f} cairia "
                f"abaixo do piso de margem R$ {floor:.2f}"
            )
        return {
            "accepted": accepted,
            "denied_reason": denied_reason,
            "target_price": target_price,
            "target_total_pct": target_total_pct,
            "target_seller_pct": target_seller_pct,
            "meli_percentage": meli_pct,
            "constraint": constraint,
            "reason": ui_reason,
            "structure_type": structure_type,
            "is_fixed_price": structure_type in ("FIXED_PCT", "FIXED_PRICE"),
            "exposure_boost": exposure_boost,
        }

    # 1. fixed_percentage (cupons).
    fixed = p.get("fixed_percentage")
    if fixed is not None:
        fixed_f = float(fixed)
        target = round(list_price * (1 - fixed_f / 100), 2)
        return _build(
            target_price=target,
            target_total_pct=fixed_f,
            target_seller_pct=round(fixed_f - meli_pct, 2),
            constraint="fixed_percentage",
            reason="Cupom de desconto fixo (operador escolhe se aceita)",
            cap_check_value=fixed_f,
            cap_check_limit=cap_total,
            cap_label="total",
            structure_type="FIXED_PCT",
        )

    # 2. min_discounted_price (INTERVAL types: DEAL/DOD/LIGHTNING/etc).
    # The seller picks any price in [min, max] (max optional). We choose
    # the highest price that respects: ML's min, our margin floor, and
    # our cap_total. If ML publishes ``suggested_discounted_price`` and
    # it falls inside our admissible band, we prefer it (best exposure).
    min_price = p.get("min_discounted_price")
    if min_price is not None:
        min_f = float(min_price)
        max_raw = p.get("max_discounted_price")
        max_f = float(max_raw) if max_raw is not None else None
        sugg_raw = p.get("suggested_discounted_price")
        sugg_f = float(sugg_raw) if sugg_raw is not None else None
        target_at_cap = round(list_price * (1 - cap_total / 100), 2)

        # Lower bound: must respect ML min, our floor, and our cap.
        lower = max(min_f, floor, target_at_cap)
        # Upper bound: ML max if published, else list_price.
        upper = max_f if max_f is not None else list_price

        # If the admissible band is empty, deny — the would-have-been
        # target (target_price/total_pct) is still surfaced so the UI shows
        # what would have happened.
        if lower > upper + 0.01:
            target = round(lower, 2)
            total_pct = round((list_price - target) / list_price * 100, 2)
            seller_pct = round(total_pct - meli_pct, 2)
            if floor > upper + 0.01:
                code_reason = f"interval_empty: piso R$ {floor:.2f} > max R$ {upper:.2f}"
                ui_reason = (
                    f"Bloqueado pelo piso — seu piso R$ {floor:.2f} é mais alto "
                    f"que o máximo permitido pelo ML R$ {upper:.2f}"
                )
            elif target_at_cap > upper + 0.01:
                code_reason = (
                    f"interval_empty: cap_target R$ {target_at_cap:.2f} > max R$ {upper:.2f}"
                )
                ui_reason = (
                    f"Bloqueado pelo cap — preço com seu cap R$ {target_at_cap:.2f} "
                    f"é mais alto que o máximo do ML R$ {upper:.2f}"
                )
            else:
                code_reason = f"interval_empty: lower R$ {lower:.2f} > max R$ {upper:.2f}"
                ui_reason = (
                    f"Intervalo do ML não acomoda seus limites "
                    f"(mínimo possível R$ {lower:.2f} > máx ML R$ {upper:.2f})"
                )
            return {
                "accepted": False,
                "denied_reason": code_reason,
                "target_price": target,
                "target_total_pct": total_pct,
                "target_seller_pct": seller_pct,
                "meli_percentage": meli_pct,
                "constraint": "min_discounted_price",
                "reason": ui_reason,
                "structure_type": "INTERVAL",
                "is_fixed_price": False,
                "exposure_boost": exposure_boost,
            }

        # Prefer ML's suggested price when it lies inside our band.
        if sugg_f is not None and lower - 0.01 <= sugg_f <= upper + 0.01:
            target = round(sugg_f, 2)
            constraint = "suggested_within_interval"
            ui_reason = (
                "ML recomenda este preço dentro do intervalo permitido "
                "(melhor exposição da promoção)"
            )
        else:
            target = round(lower, 2)
            constraint = "min_discounted_price"
            if abs(target - min_f) < 0.01:
                ui_reason = "Mínimo aceito pelo ML — não dá pra baixar mais"
            elif abs(target - floor) < 0.01:
                ui_reason = (
                    "Seu piso de margem é o limite (sem ele, ML aceitaria " "preço mais baixo)"
                )
            elif abs(target - target_at_cap) < 0.01:
                ui_reason = (
                    "Seu cap de seller é o limite (sem ele, ML aceitaria " "preço mais baixo)"
                )
            else:
                ui_reason = "Preço alvo no piso permitido pelas suas regras"

        total_pct = round((list_price - target) / list_price * 100, 2)
        seller_pct = round(total_pct - meli_pct, 2)
        return {
            "accepted": True,
            "denied_reason": None,
            "target_price": target,
            "target_total_pct": total_pct,
            "target_seller_pct": seller_pct,
            "meli_percentage": meli_pct,
            "constraint": constraint,
            "reason": ui_reason + _copay_line(seller_pct),
            "structure_type": "INTERVAL",
            "is_fixed_price": False,
            "exposure_boost": exposure_boost,
        }

    # 2b. suggested_discounted_price only (no min/max published).
    sugg = p.get("suggested_discounted_price")
    if sugg is not None:
        sugg_f = float(sugg)
        total_pct = round((list_price - sugg_f) / list_price * 100, 2)
        return _build(
            target_price=sugg_f,
            target_total_pct=total_pct,
            target_seller_pct=round(total_pct - meli_pct, 2),
            constraint="suggested_discounted_price",
            reason="ML recomenda este preço para a campanha",
            cap_check_value=total_pct,
            cap_check_limit=cap_total,
            cap_label="total",
            structure_type="SUGG_ONLY",
        )

    # 3. price + seller_percentage (SMART / PRICE_MATCHING) — ML pins price.
    ml_price = p.get("price")
    seller_raw = p.get("seller_percentage")
    if ml_price is not None and seller_raw is not None:
        price_f = float(ml_price)
        seller_f = float(seller_raw)
        total_pct = round(seller_f + meli_pct, 2)
        return _build(
            target_price=price_f,
            target_total_pct=total_pct,
            target_seller_pct=seller_f,
            constraint="ml_priced",
            reason="ML fixa o preço (campanha Smart/PriceMatching) — operador só aceita ou pula",
            cap_check_value=seller_f,
            cap_check_limit=cap_seller_pct,
            cap_label="seller",
            structure_type="FIXED_PRICE",
        )

    # 4. seller_percentage only — derive target from original_price.
    if seller_raw is not None:
        orig = p.get("original_price") or list_price
        seller_f = float(seller_raw)
        total_pct = round(seller_f + meli_pct, 2)
        derived_price = round(float(orig) * (1 - total_pct / 100), 2)
        return _build(
            target_price=derived_price,
            target_total_pct=total_pct,
            target_seller_pct=seller_f,
            constraint="seller_pct_only",
            reason=f"ML pediu desconto de seller de -{seller_f:.1f}% (ex: estoque parado)",
            cap_check_value=seller_f,
            cap_check_limit=cap_seller_pct,
            cap_label="seller",
            structure_type="FIXED_PCT",
        )

    # 5. price + original_price only (generic fallback) — ML pins price.
    if ml_price is not None:
        orig = p.get("original_price")
        if orig:
            price_f = float(ml_price)
            total_pct = round((float(orig) - price_f) / float(orig) * 100, 2)
            return _build(
                target_price=price_f,
                target_total_pct=total_pct,
                target_seller_pct=round(total_pct - meli_pct, 2),
                constraint="price_only",
                reason="ML fixa o preço da campanha — operador só aceita ou pula",
                cap_check_value=total_pct,
                cap_check_limit=cap_total,
                cap_label="total",
                structure_type="FIXED_PRICE",
            )

    return None


def enumerate_activations_for_item(
    *,
    promos: list[dict[str, Any]],
    cap_seller_pct: float,
    margin_floor_price: float | None,
    list_price: float | None,
    excluded_types: Iterable[str] = (),
) -> list[dict[str, Any]]:
    """Return every promo the engine would *try to activate* under the
    "activate everything that fits" policy (2026-05-21).

    The same eligibility rules as ``count_eligible_candidates`` apply, but
    instead of a count this returns one dict per candidate so the caller
    can render them, persist plans, or call ML's activate endpoint per
    promotion. STARTED promos are returned with ``status='already_active'``
    so the caller sees the full coverage on the MLB at a glance.

    Each entry carries:
      - promo_id, promo_type, promo_name
      - status: "would_activate" | "already_active"
      - target_price: BRL the customer would pay if activated
      - target_total_pct: total discount (seller + meli)
      - target_seller_pct: seller share only
      - meli_percentage: ML's banca co-pay
      - constraint: which rule pinned the price (fixed_percentage /
        min_discounted_price / suggested_discounted_price)
      - reason: one-line human label.
    """
    if not promos or list_price is None or list_price <= 0:
        return []
    excluded = set(excluded_types)
    out: list[dict[str, Any]] = []
    for p in promos:
        if p.get("type") in excluded:
            continue
        status = (p.get("status") or "").lower()
        meli_pct = float(p.get("meli_percentage") or 0)
        promo_type = (p.get("type") or "").upper()
        # Structure inference for STARTED entries (no candidate fields):
        if promo_type in FIXED_PRICE_TYPES:
            started_structure = "FIXED_PRICE"
            started_fixed = True
        elif promo_type in EXPOSURE_BOOST_TYPES or promo_type in {
            "PRICE_DISCOUNT",
            "SELLER_CAMPAIGN",
        }:
            started_structure = "INTERVAL"
            started_fixed = False
        else:
            started_structure = "FIXED_PCT"
            started_fixed = True
        exposure_boost = EXPOSURE_BOOST_FACTOR if promo_type in EXPOSURE_BOOST_TYPES else 1.0

        # STARTED promos: report as already-active for the operator audit.
        if status == "started":
            original = float(p.get("original_price") or list_price)
            price = float(p.get("price") or 0)
            total_pct = (original - price) / original * 100 if original > 0 and price > 0 else None
            out.append(
                {
                    "promo_id": p.get("id"),
                    "promo_type": p.get("type"),
                    "promo_name": p.get("name"),
                    "status": "already_active",
                    "target_price": price or None,
                    "target_total_pct": total_pct,
                    "target_seller_pct": (total_pct - meli_pct) if total_pct is not None else None,
                    "meli_percentage": meli_pct,
                    "constraint": "started",
                    "reason": f"{p.get('type')} já STARTED -{total_pct:.1f}%"
                    if total_pct
                    else "STARTED",
                    "structure_type": started_structure,
                    "is_fixed_price": started_fixed,
                    "exposure_boost": exposure_boost,
                }
            )
            continue

        if status != "candidate":
            continue

        scored = score_candidate_promo(
            p,
            cap_seller_pct=cap_seller_pct,
            margin_floor_price=margin_floor_price,
            list_price=list_price,
        )
        if scored is None:
            # Forma não reconhecida (sem fixed/min/sugg/price+seller). Pulamos
            # silencioso — não temos como julgar.
            continue
        accepted = scored.get("accepted", False)
        # score_candidate_promo already writes the human PT reason for
        # both accepted and denied entries; denied_reason carries the
        # English machine code separately. Nothing to reshape here.
        out.append(
            {
                "promo_id": p.get("id"),
                "promo_type": p.get("type"),
                "promo_name": p.get("name"),
                "status": "would_activate" if accepted else "denied",
                **scored,
            }
        )
    return out


def count_eligible_candidates(
    *,
    promos: list[dict[str, Any]],
    cap_seller_pct: float,
    margin_floor_price: float | None,
    list_price: float | None,
    excluded_types: Iterable[str] = (),
) -> int:
    """Count how many CANDIDATE promos pass the cap + margin floor.

    Used by analyze-all to report the total number of promotions the
    engine would activate per MLB under the "activate everything that
    fits" policy from 2026-05-21. Pure function; no I/O.

    Rules:
      - status must be ``candidate``.
      - type not in ``excluded_types``.
      - One of: ``fixed_percentage`` <= cap_total
                ``min_discounted_price`` >= floor
                ``suggested_discounted_price`` >= floor and produces
                  total% <= cap_total
    """
    if not promos or list_price is None or list_price <= 0:
        return 0
    excluded = set(excluded_types)
    floor = margin_floor_price or 0.0
    count = 0
    for p in promos:
        if p.get("status") != "candidate":
            continue
        if p.get("type") in excluded:
            continue
        meli_pct = p.get("meli_percentage") or 0
        cap_total = cap_seller_pct + meli_pct

        fixed = p.get("fixed_percentage")
        if fixed is not None:
            if float(fixed) <= cap_total + 0.01:
                # Need to also respect the floor.
                target = list_price * (1 - float(fixed) / 100)
                if target + 0.01 >= floor:
                    count += 1
            continue

        min_price = p.get("min_discounted_price")
        if min_price is not None:
            # We can reach min_price only if it's at/above the floor.
            if float(min_price) + 0.01 >= floor:
                count += 1
            continue

        sugg = p.get("suggested_discounted_price")
        if sugg is not None:
            total_pct = (list_price - float(sugg)) / list_price * 100
            if total_pct <= cap_total + 0.01 and float(sugg) + 0.01 >= floor:
                count += 1
    return count


def _catalog_row_to_ptw(row: Any) -> dict[str, Any]:
    """Turn an MLCatalogStatusORM row into the dict shape decide_for_item
    expects from /items/{MLB}/price_to_win."""
    return {
        "status": row.status,
        "visit_share": row.visit_share,
        "price_to_win": float(row.price_to_win) if row.price_to_win is not None else None,
        "current_price": float(row.current_price) if row.current_price is not None else None,
        "winner": {
            "item_id": row.winner_item_id,
            "price": float(row.winner_price) if row.winner_price is not None else None,
        },
        "catalog_product_id": row.catalog_product_id,
    }


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
