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

import asyncio
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
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

# Quantas vezes reenviar uma escrita no ML após 429/502/503/504 (rate-limit /
# gateway transitório), com backoff respeitando Retry-After. Protege a criação
# em massa de promoções, que dispara muitas escritas em sequência.
_ML_WRITE_MAX_RATE_RETRIES = 3


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    """Lê o header Retry-After (segundos) de uma resposta 429/503. Ignora o
    formato HTTP-date (raro nessa API) e limita a 30s pra não travar a request."""
    raw = resp.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return min(max(float(raw), 0.0), 30.0)
    except ValueError:
        return None


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

# Co-participation campaigns (ML co-pays part of the discount). ML SETS the
# price — the seller can only ACCEPT (enrol) or leave; there is no seller
# price to send. Enrol/exit need the live ``offer_id`` (``ref_id`` of the
# candidate in the per-item GET), so these go through enroll_offer/exit_offer
# instead of the seller-driven create/modify/exit path.
CO_PARTICIPATION_TYPES = frozenset({"SMART", "PRICE_MATCHING", "MARKETPLACE_CAMPAIGN"})

# Editabilidade do PREÇO por tipo (regra de negócio do ML, conforme docs):
#  - IN-PLACE (têm "Modificar item" = PUT): DEAL, SELLER_CAMPAIGN.
#  - via REMOVER+RECRIAR (sem PUT, mas o vendedor define o preço): PRICE_DISCOUNT.
#  - NÃO editável: co-participação (preço definido pelo ML), cupom (desconto
#    FIXO da campanha) e DOD/LIGHTNING (sem operação de edição) → só dá pra
#    inscrever/sair.
EDITABLE_INPLACE_TYPES = frozenset({"DEAL", "SELLER_CAMPAIGN"})
PRICE_EDITABLE_TYPES = EDITABLE_INPLACE_TYPES | {"PRICE_DISCOUNT"}


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

    # -- ML promotions (write) --------------------------------------------
    async def _ml_write(
        self,
        method: str,
        mlb_id: str,
        *,
        op: str,
        body: dict[str, Any] | None = None,
        extra_params: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Single funnel for EVERY seller-promotions write (POST/DELETE).

        Logs the exact request (``op``, method, url, params, body — never the
        bearer token) and the full response (status, parsed body, elapsed_ms,
        and the parsed ML error envelope ``{message,error,status,cause}``) so a
        failed ML call can be reconstructed end-to-end from Seq without a
        screenshot. Handles the 401→refresh→retry dance. Never raises for
        operational errors — returns a uniform dict::

            {ok, status_code, response, raw, sent_body, elapsed_ms, error}

        where ``response`` is the parsed JSON when available (raw text
        otherwise) and ``error`` is one of None/"http"/"transport".
        """
        url = f"{ML_API_BASE}/seller-promotions/items/{mlb_id}"
        params: dict[str, str] = {"app_version": "v2", **(extra_params or {})}
        log = logger.bind(
            op=op,
            mlb_id=mlb_id,
            ml_method=method.upper(),
            ml_url=url,
            ml_params=params,
            ml_body=body,
        )
        # The request line carries the EXACT payload we hand to ML — this is
        # the single most useful thing when ML rejects a write.
        log.info("ml_write.request")

        async def _send(tok: str) -> httpx.Response:
            headers = {"Authorization": f"Bearer {tok}"}
            m = method.upper()
            if m == "DELETE":
                return await self._http.delete(url, params=params, headers=headers, timeout=timeout)
            if m == "PUT":
                return await self._http.put(
                    url, params=params, json=body, headers=headers, timeout=timeout
                )
            return await self._http.post(
                url, params=params, json=body, headers=headers, timeout=timeout
            )

        token = await self._token_service.get_valid_access_token()
        started = time.monotonic()
        retried_auth = False
        rate_retries = 0
        try:
            resp = await _send(token)
            if resp.status_code == 401:
                log.info("ml_write.unauthorized_refresh")
                token = await self._token_service.handle_unauthorized()
                resp = await _send(token)
                retried_auth = True
            # Rate-limit (429) e gateways transitórios (502/503/504): espera e
            # tenta de novo, respeitando Retry-After. Essencial pra criação em
            # massa, que dispara muitas escritas seguidas.
            while (
                resp.status_code in (429, 502, 503, 504)
                and rate_retries < _ML_WRITE_MAX_RATE_RETRIES
            ):
                delay = _retry_after_seconds(resp)
                if delay is None:
                    delay = float(2**rate_retries)  # 1s, 2s, 4s
                rate_retries += 1
                log.warning(
                    "ml_write.rate_limited",
                    status=resp.status_code,
                    retry_in_s=delay,
                    attempt=rate_retries,
                )
                await asyncio.sleep(delay)
                resp = await _send(token)
        except httpx.RequestError as exc:
            elapsed_ms = round((time.monotonic() - started) * 1000)
            log.warning(
                "ml_write.transport_error",
                error=str(exc),
                error_type=type(exc).__name__,
                elapsed_ms=elapsed_ms,
                retried_auth=retried_auth,
            )
            return {
                "ok": False,
                "status_code": None,
                "response": f"transport error: {exc!s}",
                "raw": str(exc),
                "sent_body": body,
                "elapsed_ms": elapsed_ms,
                "error": "transport",
            }

        elapsed_ms = round((time.monotonic() - started) * 1000)
        raw = resp.text or ""
        parsed: Any = None
        if resp.content:
            try:
                parsed = resp.json()
            except ValueError:
                parsed = None
        ok = 200 <= resp.status_code < 300

        fields: dict[str, Any] = {
            "status_code": resp.status_code,
            "elapsed_ms": elapsed_ms,
            "retried_auth": retried_auth,
            "rate_retries": rate_retries,
            # Full parsed body when JSON; bounded raw text otherwise. Seq keeps
            # the whole object, so we can read ML's exact answer.
            "ml_response": parsed if parsed is not None else raw[:8000],
        }
        if not ok and isinstance(parsed, dict):
            # ML error envelope — surface each field as its own property so it's
            # filterable in Seq (ml_error_code:"item_already_in_promotion", etc.)
            fields["ml_error_message"] = parsed.get("message")
            fields["ml_error_code"] = parsed.get("error")
            fields["ml_error_status"] = parsed.get("status")
            if parsed.get("cause"):
                fields["ml_error_cause"] = parsed.get("cause")

        if ok:
            log.info("ml_write.ok", **fields)
        else:
            log.warning("ml_write.failed", **fields)

        return {
            "ok": ok,
            "status_code": resp.status_code,
            "response": parsed if parsed is not None else raw,
            "raw": raw,
            "sent_body": body,
            "elapsed_ms": elapsed_ms,
            "error": None if ok else "http",
        }

    async def apply_decision_to_ml(
        self,
        *,
        decision: Any,
    ) -> dict[str, Any]:
        """POST a previously-approved decision to /seller-promotions.

        Last line of defence on the margin floor: even if a row somehow
        reaches us with ``target_price < floor_price`` (shouldn't
        happen — the override path is HARD-gated upstream) we refuse
        to POST. Promotions cannot leak below our floor under any
        circumstance.

        Behaviour by ``promo_type`` x ``decision_kind``:

        - ``DEAL`` / ``DOD`` / ``LIGHTNING`` / ``SELLER_CAMPAIGN`` /
          ``PRICE_DISCOUNT`` with ``decision_kind='would_activate'``:
          POST ``{promotion_id, promotion_type, deal_price}`` to enrol
          the listing in the existing campaign at our target price.

        - ``PRICE_DISCOUNT`` with ``decision_kind='create_price_discount'``:
          POST ``{promotion_type='PRICE_DISCOUNT', deal_price}`` with
          no promotion_id to create a new seller-driven discount.

        - ``SELLER_COUPON_CAMPAIGN``: POST
          ``{promotion_id, promotion_type, discount_percentage}`` —
          coupons are % off at checkout, not a fixed price.

        - Anything else (SMART, PRICE_MATCHING, VOLUME, etc.) returns
          ``status='skipped'`` without touching ML. Those types are
          either ML-managed (SMART) or not yet validated against the
          live API — we'd rather skip explicitly than send a guessed
          body shape and risk a hard reject.

        Returns ``{status, status_code, response}`` so the caller can
        persist the outcome on the decision row. Never raises for
        operational errors (timeout / 5xx / 4xx) — the caller commits
        the row either way and surfaces ``status='failed'`` to the UI
        so the operator can retry.
        """
        promo_type = decision.promo_type
        decision_kind = decision.decision_kind
        mlb_id = decision.mlb_id

        # --- HARD floor verification (defence in depth) ----------------
        # The override path in the router already 422s anything below
        # the floor, so a row reaching here SHOULD be safe. This guard
        # exists for everything else: rows generated by the cron and
        # never overridden, retries via /retry-ml, future bulk-approve
        # paths, etc. If the math says we'd POST a price under our own
        # margin floor, refuse — the row stays approved locally with
        # ml_apply_status='failed' and a clear reason in the response.
        floor_violation = self._floor_violation_reason(decision)
        if floor_violation is not None:
            logger.warning(
                "promo_apply refused — floor guard tripped",
                decision_id=decision.id,
                mlb_id=mlb_id,
                promo_type=promo_type,
                reason=floor_violation,
            )
            return {
                "status": "failed",
                "status_code": None,
                "response": (
                    f"executor recusou: {floor_violation}. "
                    f"Promoção NÃO foi enviada ao Mercado Livre."
                ),
            }

        body = self._build_apply_body(decision)
        if body is None:
            logger.info(
                "promo_apply skipped — no known body shape",
                decision_id=decision.id,
                mlb_id=mlb_id,
                promo_type=promo_type,
                decision_kind=decision_kind,
            )
            return {
                "status": "skipped",
                "status_code": None,
                "response": (
                    f"executor sem suporte para {promo_type}/{decision_kind}; "
                    "row foi aprovada localmente mas não enviada ao ML"
                ),
            }

        # All transport + 401-retry + rich request/response logging lives in
        # _ml_write. We persist a bounded TEXT snippet on the row
        # (ml_apply_response) so keep that contract here.
        result = await self._ml_write("POST", mlb_id, op="apply", body=body, timeout=20.0)
        snippet = (result["raw"] or "")[:2000]
        return {
            "status": "ok" if result["ok"] else "failed",
            "status_code": result["status_code"],
            "response": snippet,
        }

    @staticmethod
    def _floor_violation_reason(decision: Any) -> str | None:
        """Return a human-readable reason if this row must NOT be
        POSTed to ML for margin-safety; ``None`` if it's safe.

        Single invariant: the effective price the buyer will pay must
        be at or above our ``floor_price`` (tolerance half a cent).
        Implementations vary by promo_type:

        - SELLER_COUPON_CAMPAIGN: discount is a %, applied at
          checkout. Effective = ``list_price * (1 - total_pct / 100)``.
        - Everything else (DEAL/PRICE_DISCOUNT/etc.): the body carries
          ``deal_price`` directly, so effective = ``target_price``.

        Rules in any case:

        1. ``floor_price IS NULL`` — we don't know our margin floor
           (costs weren't loaded when the row was generated). Refuse:
           regenerate after the cost sync runs.
        2. ``effective < floor`` — refuse, would lose money.

        2026-06-05: na aprovação MANUAL o piso (CAP) deixa de bloquear — o
        operador decide (com dupla confirmação no front p/ margem negativa). Este
        guard fica DESLIGADO até a automação de aprovação entrar, quando o cap
        volta a ser exigido só nesse fluxo. Por isso retorna sempre None.
        """
        return None

    @staticmethod
    def _build_apply_body(decision: Any) -> dict[str, Any] | None:
        """Map a decision row → the JSON body for POST /seller-promotions.

        Returns ``None`` when the type/kind combo isn't supported by
        the executor; the caller treats that as ``status='skipped'``.

        The shapes here mirror what ML accepts on
        ``POST /seller-promotions/items/{MLB}?app_version=v2``. Each
        body is built from columns already on the row (no extra
        lookups) — the decision row IS the validated contract.
        """

        promo_type = (decision.promo_type or "").upper()
        decision_kind = (decision.decision_kind or "").lower()
        target = decision.target_price
        promo_id = decision.promo_id

        # SELLER_COUPON_CAMPAIGN — "Indicar itens para uma campanha": o cupom é
        # desconto FIXO da campanha aplicado no CHECKOUT, então NÃO se envia
        # preço nem %; o body é só {promotion_id, promotion_type}. (O item não
        # pode ser modificado depois — o valor/% é fixo da campanha.)
        if promo_type == "SELLER_COUPON_CAMPAIGN":
            if promo_id is None:
                return None
            return {"promotion_id": promo_id, "promotion_type": "SELLER_COUPON_CAMPAIGN"}

        # DEAL / SELLER_CAMPAIGN — inscrição numa campanha existente pelo id:
        # {promotion_id, promotion_type, deal_price} (top_deal_price é opcional).
        if promo_type in {"DEAL", "SELLER_CAMPAIGN"} and decision_kind == "would_activate":
            if promo_id is None or target is None:
                return None
            return {
                "promotion_id": promo_id,
                "promotion_type": promo_type,
                "deal_price": float(target),
            }

        # DOD (oferta do dia) — body é SÓ {deal_price, promotion_type}: SEM
        # promotion_id, SEM stock (lá o `stock` é informativo, não vai no POST).
        if promo_type == "DOD" and decision_kind == "would_activate":
            if target is None:
                return None
            return {"promotion_type": "DOD", "deal_price": float(target)}

        # LIGHTNING (relâmpago) — {deal_price, stock, promotion_type}: SEM
        # promotion_id; `stock` (qtd reservada pra oferta) é OBRIGATÓRIO no body.
        if promo_type == "LIGHTNING" and decision_kind == "would_activate":
            stock = getattr(decision, "stock_chosen", None)
            if target is None or not stock:
                return None
            return {
                "promotion_type": "LIGHTNING",
                "deal_price": float(target),
                "stock": int(stock),
            }

        # PRICE_DISCOUNT (desconto individual) — o ML EXIGE start_date +
        # finish_date em formato LOCAL (sem timezone; só a data conta). create =
        # sem promotion_id; would_activate = com promotion_id.
        if promo_type == "PRICE_DISCOUNT":
            if target is None:
                return None
            start, finish = _price_discount_default_dates()
            if decision_kind == "create_price_discount":
                return {
                    "promotion_type": "PRICE_DISCOUNT",
                    "deal_price": float(target),
                    "start_date": start,
                    "finish_date": finish,
                }
            if decision_kind == "would_activate" and promo_id is not None:
                return {
                    "promotion_id": promo_id,
                    "promotion_type": "PRICE_DISCOUNT",
                    "deal_price": float(target),
                    "start_date": start,
                    "finish_date": finish,
                }

        # SMART / PRICE_MATCHING / MARKETPLACE_CAMPAIGN / VOLUME /
        # PRE_NEGOTIATED — ML-managed or not validated yet. Skip.
        return None

    # -- Seller-driven price actions -------------------------------------
    @staticmethod
    def _find_offer(
        promos: list[dict[str, Any]],
        promotion_type: str,
        *,
        status: str | None = None,
        promotion_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Acha a oferta de um item (resposta do GET /seller-promotions/items)
        casando o ``type`` (e opcionalmente ``status``/``promotion_id``). Usado
        pra resolver o ``offer_id`` (``ref_id``) ao vivo nas campanhas de
        co-participação, onde o enrol/exit do ML exige esse id."""
        pt = (promotion_type or "").upper()
        for p in promos:
            if (p.get("type") or "").upper() != pt:
                continue
            if promotion_id and p.get("id") != promotion_id:
                continue
            if status and (p.get("status") or "").lower() != status:
                continue
            return p
        return None

    async def enroll_offer(
        self, *, mlb_id: str, promotion_type: str, promotion_id: str | None = None
    ) -> dict[str, Any]:
        """Inscreve (aceita) o item numa campanha de co-participação (SMART /
        PRICE_MATCHING / MARKETPLACE_CAMPAIGN). O ML define o preço — o vendedor
        só aceita —, então o body NÃO leva ``deal_price``: leva ``promotion_id``
        + ``promotion_type`` + ``offer_id``. O ``offer_id`` (o ``ref_id`` do
        candidato) é resolvido ao vivo do GET /seller-promotions/items, porque
        a decisão não o armazena.

        Devolve ``{status_code, response, sent_body}`` (nunca levanta em erro
        operacional) pro caller persistir o resultado."""
        promos = await self.fetch_eligible_promos(mlb_id)
        # Só inscreve um CANDIDATO de verdade. Os fallbacks antigos (qualquer
        # status) pegavam um offer já 'started' — e inscrever o que já está
        # inscrito faz o ML responder "Candidate not valid". Isso acontecia quando
        # o ML auto-inscrevia o SMART (candidate→started) e o espelho ainda
        # mostrava "Ativar".
        candidate = self._find_offer(
            promos, promotion_type, status="candidate", promotion_id=promotion_id
        ) or self._find_offer(promos, promotion_type, status="candidate")
        if candidate is None:
            # Já está ATIVA (started)? Trata como sucesso/no-op em vez de reenviar
            # (que daria "Candidate not valid").
            already = self._find_offer(
                promos, promotion_type, status="started", promotion_id=promotion_id
            ) or self._find_offer(promos, promotion_type, status="started")
            if already is not None:
                return {
                    "status_code": 200,
                    "response": {"status": "already_active", "message": "já ativa no ML"},
                    "sent_body": None,
                    "already_active": True,
                }
            return {
                "status_code": None,
                "response": (
                    f"nenhum candidato {promotion_type} para {mlb_id} — a oferta não "
                    "está mais disponível (o ML já a iniciou/retirou). Atualize a lista."
                ),
                "sent_body": None,
            }
        offer_ref = candidate.get("ref_id") or candidate.get("offer_id")
        pid = candidate.get("id") or promotion_id
        body: dict[str, Any] = {"promotion_type": (promotion_type or "").upper()}
        if pid:
            body["promotion_id"] = pid
        if offer_ref:
            body["offer_id"] = offer_ref

        result = await self._ml_write("POST", mlb_id, op="enroll_offer", body=body, timeout=30.0)
        return {
            "status_code": result["status_code"],
            "response": result["response"],
            "sent_body": body,
        }

    async def exit_offer(
        self, *, mlb_id: str, promotion_type: str, promotion_id: str | None = None
    ) -> dict[str, Any]:
        """Sai de uma campanha de co-participação (SMART/PRICE_MATCHING/...). O
        DELETE do ML para esses tipos exige ``promotion_id`` + ``offer_id`` além
        do ``promotion_type``; resolvemos ambos ao vivo da oferta ``started`` do
        item antes de chamar o DELETE."""
        promos = await self.fetch_eligible_promos(mlb_id)
        offer = (
            self._find_offer(promos, promotion_type, status="started", promotion_id=promotion_id)
            or self._find_offer(promos, promotion_type, promotion_id=promotion_id)
            or self._find_offer(promos, promotion_type)
        )
        offer_ref = (offer.get("ref_id") or offer.get("offer_id")) if offer else None
        pid = (offer.get("id") if offer else None) or promotion_id
        return await self.exit_promotion(
            mlb_id=mlb_id,
            promotion_type=promotion_type,
            promotion_id=pid,
            offer_id=offer_ref,
        )

    async def exit_promotion(
        self,
        *,
        mlb_id: str,
        promotion_type: str | None = None,
        promotion_id: str | None = None,
        offer_id: str | None = None,
    ) -> dict[str, Any]:
        """Remove o anúncio de uma promoção ativa.

        Chama DELETE /seller-promotions/items/{mlb_id}?promotion_type=X&app_version=v2.
        O preço volta ao list_price do anúncio. A doc do ML exige o
        ``promotion_type`` no DELETE (sem ele, o ML pode não saber qual oferta
        remover) — passamos quando conhecido. Campanhas de co-participação
        exigem também ``promotion_id`` + ``offer_id`` (passados por exit_offer).
        Não levanta exceção em erros operacionais — devolve {status_code,
        response} para o caller persistir.

        Nota (doc ML): ofertas LIGHTNING já *iniciadas* (started) NÃO podem ser
        removidas, apenas pausadas — o ML devolverá erro nesse caso.
        """
        extra: dict[str, str] = {}
        if promotion_type:
            extra["promotion_type"] = promotion_type
        if promotion_id:
            extra["promotion_id"] = promotion_id
        if offer_id:
            extra["offer_id"] = offer_id
        result = await self._ml_write(
            "DELETE", mlb_id, op="exit_promotion", extra_params=extra, timeout=30.0
        )
        return {"status_code": result["status_code"], "response": result["response"]}

    async def create_price_discount(
        self,
        *,
        mlb_id: str,
        deal_price: float,
        start_date: str | None = None,
        finish_date: str | None = None,
    ) -> dict[str, Any]:
        """Cria uma promoção PRICE_DISCOUNT direta para um MLB (sem campaign_id).

        Doc ML "Desconto individual": o body EXIGE ``start_date`` + ``finish_date``
        em formato LOCAL (sem timezone — o ML considera só a data: início/fim do
        dia). Default quando não informado: hoje → +30 dias.
        """
        if not start_date or not finish_date:
            ds, df = _price_discount_default_dates()
            start_date = start_date or ds
            finish_date = finish_date or df
        body = {
            "promotion_type": "PRICE_DISCOUNT",
            "deal_price": deal_price,
            "start_date": start_date,
            "finish_date": finish_date,
        }
        result = await self._ml_write(
            "POST", mlb_id, op="create_price_discount", body=body, timeout=30.0
        )
        return {"status_code": result["status_code"], "response": result["response"]}

    async def edit_promotion_price(
        self,
        *,
        mlb_id: str,
        deal_price: float,
        promotion_id: str | None = None,
        promotion_type: str = "PRICE_DISCOUNT",
    ) -> dict[str, Any]:
        """Altera, IN-PLACE, o preço de uma promoção JÁ inscrita — sem sair dela.

        Doc ML (Modify items): a edição de um item numa promoção é
        ``PUT /seller-promotions/items/{mlb_id}?app_version=v2`` com
        ``{deal_price, promotion_id, promotion_type}`` (≠ do POST, que é a
        INSCRIÇÃO). O ML só permite **BAIXAR** o preço in-place; subir devolve
        400 (preço fora do desconto sugerido) — por isso o endpoint roteia o
        "subir" por sair+reentrar. A regra de só-baixar é validada no front
        antes de chamar este método."""
        body: dict[str, Any] = {
            "promotion_type": promotion_type,
            "deal_price": deal_price,
        }
        if promotion_id is not None:
            body["promotion_id"] = promotion_id
        result = await self._ml_write(
            "PUT", mlb_id, op="edit_promotion_price", body=body, timeout=30.0
        )
        return {"status_code": result["status_code"], "response": result["response"]}

    async def modify_promotion(
        self,
        *,
        mlb_id: str,
        deal_price: float,
        promotion_id: str | None = None,
        promotion_type: str = "PRICE_DISCOUNT",
    ) -> dict[str, Any]:
        """Inscreve/RE-inscreve o anúncio numa campanha a um preço — ``POST``.

        Usado pelo caminho de "subir preço" (sair + reentrar): depois do
        DELETE, o item NÃO está mais na campanha, então reentrar é uma
        INSCRIÇÃO (``POST /seller-promotions/items/{mlb_id}`` com
        ``{promotion_id, promotion_type, deal_price}``), não uma edição
        in-place (essa é ``edit_promotion_price`` via PUT)."""
        body: dict[str, Any] = {
            "promotion_type": promotion_type,
            "deal_price": deal_price,
        }
        if promotion_id is not None:
            body["promotion_id"] = promotion_id
        result = await self._ml_write(
            "POST", mlb_id, op="modify_promotion", body=body, timeout=30.0
        )
        return {"status_code": result["status_code"], "response": result["response"]}

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

    async def fetch_item_price(self, mlb_id: str) -> dict[str, Any]:
        """Preço ATUAL do anúncio no ML (reflete a promo ativa) — ground truth pra
        confirmar que a tela bate com o ML. Read-only. ``price`` = preço de venda
        agora (com a promo, se houver); ``base_price`` = cheio."""
        data = await self._ml_get_json(
            f"{ML_API_BASE}/items/{mlb_id}",
            {"attributes": "price,base_price,original_price"},
            op="item_price",
        )
        return data if isinstance(data, dict) else {}

    async def _ml_get_json(self, url: str, params: dict[str, Any], *, op: str) -> Any:
        """Autenticated GET with one 401-refresh retry. Returns parsed JSON or
        None on >=400 (logged). Shared by the campaign-listing reads."""
        token = await self._token_service.get_valid_access_token()
        resp = await self._http.get(
            url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=20.0
        )
        if resp.status_code == 401:
            token = await self._token_service.handle_unauthorized()
            resp = await self._http.get(
                url, params=params, headers={"Authorization": f"Bearer {token}"}, timeout=20.0
            )
        if resp.status_code >= 400:
            logger.warning(
                "ml_get_failed", op=op, url=url, status=resp.status_code, body=resp.text[:300]
            )
            return None
        return resp.json()

    async def fetch_promotion_dates(
        self, promo_id: str, promo_type: str
    ) -> tuple[datetime | None, datetime | None]:
        """Busca (start_date, finish_date) de UMA campanha. O endpoint de
        elegíveis (``items/{MLB}``) NÃO manda datas pra co-participação
        (SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN); aqui pegamos da própria
        campanha via ``GET /seller-promotions/promotions/{id}``. Devolve
        (None, None) se a campanha não responder."""
        body = await self._ml_get_json(
            f"{ML_API_BASE}/seller-promotions/promotions/{promo_id}",
            {"promotion_type": promo_type, "app_version": "v2"},
            op="fetch_promotion_dates",
        )
        if not isinstance(body, dict):
            return None, None
        return _parse_iso_dt(body.get("start_date")), _parse_iso_dt(body.get("finish_date"))

    async def list_seller_campaigns(self) -> list[dict[str, Any]]:
        """Campanhas em que o vendedor pode INSCREVER anúncios em massa —
        SELLER_CAMPAIGN (campanha do vendedor) e DEAL (oferta do ML). Mesmo
        contrato de inscrição (``{promotion_id, promotion_type, deal_price}``).
        Exclui campanhas encerradas; mantém started/pending (entráveis)."""
        body = await self._ml_get_json(
            f"{ML_API_BASE}/seller-promotions/users/{_settings.ml_user_id}",
            {"app_version": "v2"},
            op="list_seller_campaigns",
        )
        results = (body or {}).get("results") or [] if isinstance(body, dict) else []
        out: list[dict[str, Any]] = []
        for p in results:
            if not isinstance(p, dict):
                continue
            if p.get("type") not in ("SELLER_CAMPAIGN", "DEAL"):
                continue
            if (p.get("status") or "").lower() in ("finished", "expired", "cancelled"):
                continue
            out.append(p)
        return out

    async def list_campaign_candidates(
        self, promotion_id: str, promotion_type: str, *, status: str = "candidate"
    ) -> list[dict[str, Any]]:
        """Itens de uma campanha por status (``candidate`` = elegíveis a entrar,
        ``started`` = já inscritos). Pagina até esgotar (ML retorna ``paging``).
        Cada candidato traz ``original_price``, ``min_discounted_price``,
        ``max_discounted_price`` e ``suggested_discounted_price``."""
        url = f"{ML_API_BASE}/seller-promotions/promotions/{promotion_id}/items"
        out: list[dict[str, Any]] = []
        offset = 0
        for _ in range(40):  # hard stop ~2000 itens
            body = await self._ml_get_json(
                url,
                {
                    "promotion_type": promotion_type,
                    "status": status,
                    "app_version": "v2",
                    "limit": 50,
                    "offset": offset,
                },
                op="list_campaign_candidates",
            )
            if not isinstance(body, dict):
                break
            page = body.get("results") or []
            out.extend(r for r in page if isinstance(r, dict))
            paging = body.get("paging") or {}
            total = int(paging.get("total") or 0)
            offset += 50
            if offset >= total or not page:
                break
        return out

    async def fetch_catalog_competitors(self, catalog_product_id: str) -> list[dict[str, Any]]:
        """Lista os anúncios concorrentes de um produto de catálogo, com preços.

        Chama ``GET /products/{catalog_product_id}/items`` — retorna todos os
        vendedores disputando o catálogo. Usado para sugerir um preço de
        recuperação de margem baseado na concorrência (o ``price_to_win`` da ML
        não revela o 2º colocado quando estamos ganhando). Devolve uma lista de
        ``{item_id, seller_id, price, free_shipping, logistic_type, condition,
        listing_type_id}``. Lista vazia em erro/sem dados.
        """
        token = await self._token_service.get_valid_access_token()
        url = f"{ML_API_BASE}/products/{catalog_product_id}/items"
        resp = await self._http.get(
            url, params={"limit": 50}, headers={"Authorization": f"Bearer {token}"}, timeout=15.0
        )
        if resp.status_code == 401:
            token = await self._token_service.handle_unauthorized()
            resp = await self._http.get(
                url,
                params={"limit": 50},
                headers={"Authorization": f"Bearer {token}"},
                timeout=15.0,
            )
        if resp.status_code >= 400:
            logger.warning(
                "ml_catalog_competitors_failed",
                catalog_product_id=catalog_product_id,
                status=resp.status_code,
                body=resp.text[:300],
            )
            return []
        results = (resp.json() or {}).get("results") or []
        out: list[dict[str, Any]] = []
        for r in results:
            shipping = r.get("shipping") or {}
            out.append(
                {
                    "item_id": r.get("item_id"),
                    "seller_id": r.get("seller_id"),
                    "price": r.get("price"),
                    "free_shipping": shipping.get("free_shipping"),
                    "logistic_type": shipping.get("logistic_type"),
                    "condition": r.get("condition"),
                    "listing_type_id": r.get("listing_type_id"),
                }
            )
        return out

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
            # Co-participação (SMART/PRICE_MATCHING/MARKETPLACE_CAMPAIGN): o
            # current_price é o PISO, não o preço de venda (que o ML define
            # dinâmico, mais alto). Comparar o piso com o nosso floor gera alerta
            # FALSO — o anúncio raramente vende no piso. Só alerta em promo de
            # preço FIXO (DEAL/SELLER_CAMPAIGN/PRICE_DISCOUNT/…), onde o
            # current_price é mesmo o preço de venda.
            if (
                decision.floor_violated
                and decision.current_promo_type not in CO_PARTICIPATION_TYPES
            ):
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
        refresh_active_prices: bool = False,
    ) -> dict[str, Any]:
        """Walk every (sku, MLB) and write a pending decision per
        candidate promo that fits the cap+floor. Idempotent: existing
        rows in any status are skipped by the unique constraint.

        ``refresh_active_prices``: when True, also force-update the cached price
        of EXISTING 'started' (active) rows to match ML's live value. The normal
        idempotent path skips them, so a promo whose price changed on ML (or via
        the app) since the last generate would otherwise stay stale until the
        next day. Only touches visibility-only started rows — never an operator's
        pending/approved decision.

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
            "started_expired": 0,
            "active_prices_refreshed": 0,
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
                # promo_keys das campanhas STARTED vistas AGORA neste MLB. Tudo
                # que estava 'started/ignored' no banco e não aparece mais aqui
                # = campanha encerrada no ML → expira (limpa promoções antigas
                # que ficavam "ativas" pra sempre, ex.: SMART/SELLER de maio).
                started_keys_now: set[str] = set()
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
                    # Parse finish_date from ISO string if present.
                    # ML returns e.g. "2026-06-08T00:00:00-03:00" or
                    # "2026-06-08T00:00:00" — fromisoformat handles both
                    # in Python 3.11+; strip trailing timezone offset
                    # manually for older runtimes.
                    finish_dt = _parse_iso_dt(entry.get("finish_date"))
                    start_dt = _parse_iso_dt(entry.get("start_date"))

                    key_now = str(promo_key)[:80]
                    if entry.get("constraint") == "started":
                        started_keys_now.add(key_now)

                    inserted = await decisions.insert_if_absent(
                        mlb_id=mlb_id,
                        sku=sku,
                        promo_key=key_now,
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
                        promo_finish_date=finish_dt,
                        promo_start_date=start_dt,
                        min_price=_to_dec(entry.get("min_price")),
                        max_price=_to_dec(entry.get("max_price")),
                        stock_min=entry.get("stock_min"),
                        stock_max=entry.get("stock_max"),
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
                        # Linha já existia mas a inserção idempotente NÃO a atualiza.
                        # Se o ML diz que está STARTED, RECONCILIA a linha pra
                        # 'started' com o preço vivo. Cobre dois casos críticos:
                        #  (a) campanha que o operador APROVOU — ficava com
                        #      constraint 'suggested_within_interval'/status
                        #      'approved' e SUMIA das Inscritas (que filtram
                        #      'started'), mesmo estando ativa no ML;
                        #  (b) mantém o preço das started em dia com o ML.
                        tp = _to_dec(entry.get("target_price"))
                        if entry.get("constraint") == "started" and tp is not None and tp > 0:
                            await decisions.upsert_started(
                                mlb_id=mlb_id,
                                sku=sku,
                                promo_type=entry.get("promo_type") or "?",
                                target_price=tp,
                                promo_id=promo_id,
                                promo_key=key_now,
                                list_price=Decimal(str(list_price)),
                                cap_pct=cap.max_seller_share_pct,
                                floor_price=cap.margin_floor_price,
                                promo_name=entry.get("promo_name"),
                                promo_start_date=start_dt,
                                promo_finish_date=finish_dt,
                                reason=entry.get("reason") or "",
                            )
                            stats["active_prices_refreshed"] += 1

                # MLB consultado com sucesso: expira as STARTED antigas que o ML
                # não retorna mais (campanha encerrada). Só roda pra MLBs que
                # chegaram aqui (fetch ok), nunca pros que deram `continue`.
                stats["started_expired"] += await decisions.expire_disappeared_started(
                    mlb_id=mlb_id, seen_promo_keys=started_keys_now
                )
        await session.commit()
        logger.info("decisions_generated", **stats)
        return stats

    async def reconcile_started_promos(
        self,
        session: AsyncSession,
        *,
        only_mlb: str | None = None,
    ) -> dict[str, Any]:
        """Varre TODOS os anúncios ATIVOS (independente de cap/SKU) e reconcilia
        as promoções STARTED do ML no espelho.

        Fecha o ponto cego do ``generate_pending_decisions``, que só varre SKUs
        COM cap — anúncios sem cap (ou sem SKU mapeado) tinham promoção ativa
        (ex.: SELLER_CAMPAIGN) que nunca virava linha 'started', sumindo das
        Inscritas e poluindo o "sem promoção". Aqui grava as started (upsert) e
        expira as que o ML não retorna mais. ``only_mlb`` restringe a um anúncio
        (usado pelo webhook em tempo real).
        """
        from sqlalchemy import text

        from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
            MLPromoDecisionRepository,
        )

        decisions = MLPromoDecisionRepository(session)
        snap_repo = MLCostsSnapshotRepository(session)

        if only_mlb:
            row = (
                await session.execute(
                    text(
                        "SELECT mlb_id, sku FROM ml_listings WHERE mlb_id = :m AND status = 'active'"
                    ),
                    {"m": only_mlb},
                )
            ).one_or_none()
            targets: list[tuple[str, str | None]] = [(row.mlb_id, row.sku)] if row else []
        else:
            targets = [
                (r.mlb_id, r.sku)
                for r in (
                    await session.execute(
                        text("SELECT mlb_id, sku FROM ml_listings WHERE status = 'active'")
                    )
                ).all()
            ]

        stats = {
            "mlbs_scanned": 0,
            "started_upserted": 0,
            "started_expired": 0,
            "errors": 0,
        }
        for mlb_id, sku in targets:
            stats["mlbs_scanned"] += 1
            try:
                promos = await self.fetch_eligible_promos(mlb_id)
            except Exception as exc:  # pragma: no cover — rede
                stats["errors"] += 1
                logger.debug("reconcile_started_fetch_failed", mlb_id=mlb_id, error=str(exc))
                continue

            list_price: float | None = None
            snap = await snap_repo.get(mlb_id)
            if snap and snap.list_price:
                list_price = float(snap.list_price)
            if list_price is None:
                for p in promos:
                    if p.get("original_price"):
                        list_price = float(p["original_price"])
                        break

            seen_keys: set[str] = set()
            for p in promos:
                if (p.get("status") or "").lower() != "started":
                    continue
                price = _to_dec(p.get("price"))
                if price is None or price <= 0:
                    continue
                promo_id = p.get("id")
                # Mesmo esquema de chave do generate: id quando existe, senão
                # 'CREATE-started' (seller PRICE_DISCOUNT não tem id).
                key = (str(promo_id) if promo_id else "CREATE-started")[:80]
                seen_keys.add(key)
                await decisions.upsert_started(
                    mlb_id=mlb_id,
                    sku=sku or mlb_id,
                    promo_type=(p.get("type") or "?"),
                    target_price=price,
                    promo_id=promo_id,
                    promo_key=key,
                    list_price=_to_dec(list_price),
                    promo_name=p.get("name"),
                    promo_start_date=_parse_iso_dt(p.get("start_date")),
                    promo_finish_date=_parse_iso_dt(p.get("finish_date")),
                    reason=f"{p.get('type')} STARTED (varredura de promoções ativas)",
                )
                stats["started_upserted"] += 1

            # Expira started antigas que o ML não retorna mais (campanha encerrada).
            stats["started_expired"] += await decisions.expire_disappeared_started(
                mlb_id=mlb_id, seen_promo_keys=seen_keys
            )
        await session.commit()
        logger.info("reconcile_started_done", only_mlb=only_mlb, **stats)
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

        from sqlalchemy import select

        from tiny_mirror.infrastructure.orm.models import MLListingORM

        caps_repo = MLPromoCapRepository(session)
        snap_repo = MLCostsSnapshotRepository(session)
        decisions_repo = MLPromoDecisionRepository(session)

        # Pull pending rows. The dashboard has thousands but the cron
        # runs in one transaction; bound the page so we don't surprise
        # the DB on a runaway dataset. 5000 is comfortably above the
        # 2.1k current backlog.
        pending_rows, total_pending = await decisions_repo.list_(status="pending", limit=5000)

        # Pre-load listing statuses in one query to avoid N+1.
        mlb_ids = list({r.mlb_id for r in pending_rows})
        listing_status_result = await session.execute(
            select(MLListingORM.mlb_id, MLListingORM.status).where(MLListingORM.mlb_id.in_(mlb_ids))
        )
        listing_status: dict[str, str] = {
            row[0]: (row[1] or "") for row in listing_status_result.all()
        }

        by_reason: dict[str, int] = {
            "list_price_drift": 0,
            "cap_changed": 0,
            "floor_changed": 0,
            "stale_age": 0,
            "listing_not_active": 0,
            "campaign_ended": 0,
        }
        expired_total = 0

        for row in pending_rows:
            cap = await caps_repo.get(row.mlb_id)
            # 0. MLB não está ativo (pausado, fechado, ou sem listing) →
            #    nunca vai ser possível aplicar a promoção.
            lst_status = listing_status.get(row.mlb_id)
            if lst_status != "active":
                reason: str | None = "listing_not_active"
            # 0b. Campanha com data de término já passou → decisão obsoleta.
            elif row.promo_finish_date is not None and row.promo_finish_date < now:
                reason = "campaign_ended"
            else:
                reason = self._stale_reason(
                    row,
                    snap=await snap_repo.get(row.mlb_id),
                    cap=cap,
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

    async def bulk_decide_pending(
        self,
        session: AsyncSession,
        *,
        action: str,
        promo_types: list[str] | None = None,
        min_delta_pct: float | None = None,
        max_delta_pct: float | None = None,
        skus: list[str] | None = None,
        dry_run: bool = True,
        decided_by: str | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        """Find every pending decision matching the filter set and
        flip its status. Idempotent: only rows still in ``pending``
        are touched, so a re-run after a partial failure picks up
        what was missed.

        Returns either a dry-run preview (``dry_run=True``) or the
        post-commit count (``dry_run=False``). Both shapes share:

          - ``matched``     int — rows passing the filter
          - ``by_type``     dict[str,int] — count per promo_type
          - ``sample``      list — first 5 (sku, mlb, type, target) so
            the UI can render a sanity-check before the operator
            commits
          - ``avg_delta_pct`` float|None — population average of
            (target - list) / list * 100; useful even for ignore so
            the operator sees how steep the discounts are

        The DRY-run shape additionally returns ``would_update`` (==
        matched). The WET shape returns ``updated`` which equals
        matched in the happy path; lower only when a concurrent
        request moved some rows out of pending between the filter and
        the UPDATE.

        Approve is rejected at the router layer. Bulk-approve would
        skip the per-row target_price / cap / floor revalidation so we
        forbid it; ``ignore`` and ``reject`` are pure DB transitions
        and safe.
        """
        from sqlalchemy import select, update

        from tiny_mirror.infrastructure.orm.models import MLPromoDecisionORM

        if action not in ("ignore", "reject"):
            raise ValueError(f"bulk action not allowed: {action}")

        # Build the filter once; reuse for the dry-run SELECT and the
        # commit UPDATE so the two see exactly the same row set.
        conditions = [MLPromoDecisionORM.status == "pending"]
        if promo_types:
            conditions.append(MLPromoDecisionORM.promo_type.in_(promo_types))
        if skus:
            conditions.append(MLPromoDecisionORM.sku.in_(skus))

        # Δ% filter is done in Python rather than SQL because the
        # decision row stores target_price and list_price as Numeric
        # nullables; expressing (a-b)/b in SQL with NULL handling and
        # divide-by-zero guards is messy. The dataset is bounded
        # (today 2k rows) so an in-process filter is fine.
        stmt = select(MLPromoDecisionORM).where(*conditions)
        rows = list((await session.execute(stmt)).scalars().all())

        matched_rows = [
            r for r in rows if self._row_passes_delta_range(r, min_delta_pct, max_delta_pct)
        ]

        by_type: dict[str, int] = {}
        deltas: list[float] = []
        for r in matched_rows:
            by_type[r.promo_type] = by_type.get(r.promo_type, 0) + 1
            d = self._row_delta_pct(r)
            if d is not None:
                deltas.append(d)
        avg_delta_pct = sum(deltas) / len(deltas) if deltas else None
        sample = [
            {
                "id": r.id,
                "sku": r.sku,
                "mlb_id": r.mlb_id,
                "promo_type": r.promo_type,
                "target_price": float(r.target_price) if r.target_price else None,
                "list_price": float(r.list_price) if r.list_price else None,
            }
            for r in matched_rows[:5]
        ]

        base = {
            "matched": len(matched_rows),
            "by_type": by_type,
            "avg_delta_pct": avg_delta_pct,
            "sample": sample,
        }

        if dry_run:
            logger.info(
                "decisions_bulk_dry_run",
                action=action,
                **{k: v for k, v in base.items() if k != "sample"},
            )
            return {**base, "would_update": len(matched_rows), "dry_run": True}

        # Commit path. UPDATE-by-id with the same filter set; the WHERE
        # still includes status='pending' so a row touched by another
        # request between the SELECT and the UPDATE is safely skipped.
        ids = [r.id for r in matched_rows]
        if not ids:
            return {**base, "updated": 0, "dry_run": False}

        upd_stmt = (
            update(MLPromoDecisionORM)
            .where(
                MLPromoDecisionORM.id.in_(ids),
                MLPromoDecisionORM.status == "pending",
            )
            .values(
                status=action,
                decided_at=datetime.now(UTC),
                decided_by=decided_by,
                notes=notes,
            )
            .execution_options(synchronize_session=False)
        )
        result = await session.execute(upd_stmt)
        await session.commit()
        updated = int(getattr(result, "rowcount", 0) or 0)
        logger.info(
            "decisions_bulk_committed",
            action=action,
            matched=len(matched_rows),
            updated=updated,
            decided_by=decided_by,
        )
        return {**base, "updated": updated, "dry_run": False}

    @staticmethod
    def _row_delta_pct(row: Any) -> float | None:
        """Return (target - list) / list * 100 for a decision row, or
        None when either side is missing or list is non-positive.
        Negative = discount, positive = price hike.
        """
        if row.target_price is None or row.list_price is None or float(row.list_price) <= 0:
            return None
        return (float(row.target_price) - float(row.list_price)) / float(row.list_price) * 100.0

    @classmethod
    def _row_passes_delta_range(
        cls,
        row: Any,
        min_delta_pct: float | None,
        max_delta_pct: float | None,
    ) -> bool:
        """Range gate used by the bulk-act filter. When neither bound
        is set the row passes unconditionally; otherwise a row whose
        Δ% can't be computed (missing target/list) is REJECTED — the
        operator asked for a Δ% slice and a row without one isn't in it.
        """
        if min_delta_pct is None and max_delta_pct is None:
            return True
        d = cls._row_delta_pct(row)
        if d is None:
            return False
        if min_delta_pct is not None and d < min_delta_pct:
            return False
        if max_delta_pct is not None and d > max_delta_pct:
            return False
        return True

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


def _price_discount_default_dates(days: int = 30) -> tuple[str, str]:
    """start_date/finish_date no formato LOCAL exigido pelo ML para
    PRICE_DISCOUNT (sem timezone — o ML usa só a data: início/fim do dia).
    Default: de hoje até +``days`` dias (horário de Brasília)."""
    try:
        from zoneinfo import ZoneInfo

        today = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    except Exception:  # pragma: no cover — tzdata ausente
        today = datetime.now(UTC).date()
    start = f"{today.isoformat()}T00:00:00"
    finish = f"{(today + timedelta(days=days)).isoformat()}T00:00:00"
    return start, finish


def _parse_iso_dt(raw: Any) -> datetime | None:
    """Parse a ML ISO timestamp ("2026-06-08T00:00:00-03:00" or naive) into an
    aware datetime (UTC when no offset). Returns None on missing/invalid."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


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

    # 1b. LIGHTNING (Oferta Relâmpago): o ML DETERMINA o teto (``offer.price``)
    # com base nas nossas vendas — não dá pra subir acima dele. O operador só
    # pode BAIXAR o preço, até o ``min_discounted_price``. O ``suggested`` vem
    # ACIMA do teto real e por isso é ignorado aqui. Reserva de estoque
    # obrigatória vem em ``offer.stock {min,max}``.
    if promo_type == "LIGHTNING" and p.get("price") is not None:
        ceiling = float(p["price"])
        min_raw = p.get("min_discounted_price")
        floor_min = float(min_raw) if min_raw is not None else ceiling
        target = round(ceiling, 2)
        total_pct = round((list_price - target) / list_price * 100, 2)
        stock = p.get("stock") or {}
        return {
            "accepted": True,
            "target_price": target,
            "target_total_pct": total_pct,
            "target_seller_pct": round(total_pct - meli_pct, 2),
            "meli_percentage": meli_pct,
            "constraint": "lightning_ceiling",
            "reason": "Teto definido pelo ML (com base nas suas vendas) — só dá pra baixar o preço",
            "structure_type": "INTERVAL",
            "is_fixed_price": False,
            "exposure_boost": exposure_boost,
            "min_price": round(floor_min, 2),
            "max_price": round(ceiling, 2),
            "stock_min": stock.get("min"),
            "stock_max": stock.get("max"),
        }

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
                "min_price": round(min_f, 2),
                "max_price": round(upper, 2),
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
            "min_price": round(min_f, 2),
            "max_price": round(upper, 2),
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
                    "finish_date": p.get("finish_date"),
                    "start_date": p.get("start_date"),
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
                "finish_date": p.get("finish_date"),
                "start_date": p.get("start_date"),
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
