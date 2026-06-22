"""Recomendador por similaridade — aprende com as decisões DO USUÁRIO.

Lê os rótulos de ouro (o que o operador fez): decisões explícitas
(``ml_promo_decisions`` approved/rejected/ignored) + ações na aba Promoções
(``ml_promo_actions``: entrar/alterar = positivo, sair = negativo). Cada exemplo
carrega o snapshot de features congelado no momento (``decision_context`` /
``context``): estoque/cobertura, disputa de catálogo, vendas, margem, desconto.

Pra uma promoção nova, acha os K vizinhos mais parecidos (mesmo tipo) e devolve
"entrar a R$X / pular" + confiança + o porquê. **Com gating**: se não há vizinhos
parecidos o suficiente, devolve ``None`` (a UI fica calada — sem fingir confiança
que não tem). Conforme o operador decide, o dataset cresce e mais segmentos passam
a ter recomendação. Interpretável de propósito — um modelo treinado entra depois,
quando o volume justificar.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Features numéricas usadas na distância. Cada uma vira z-score sobre a população
# de exemplos (nulos viram a média = distância neutra naquele eixo).
_NUMERIC_FEATURES = (
    "coverage_days",
    "margin_pct",
    "discount_pct",
    "sales_30d",
    "momentum",
    "catalog_rank",
    "price_to_win_gap",
    "cap_pct",
)

# Disputa de catálogo → posto numérico (quanto maior, melhor posicionado).
_CATALOG_RANK = {
    "winning": 3.0,
    "sharing_first_place": 2.0,
    "competing": 1.0,
    "losing": 0.0,
    "not_listed": -1.0,
}

_MIN_NEIGHBORS = 5  # abaixo disso a UI fica calada (sem confiança)
_K = 10
_MAX_DIST = 3.0  # vizinho além disso (em desvios-padrão) não conta


@dataclass
class Sample:
    promo_type: str
    label: int  # +1 entrou/aprovou · -1 pulou/saiu
    chosen_total_pct: float | None
    feats: dict[str, float | None]


@dataclass
class Recommendation:
    action: str  # "enter" | "skip" | "neutral"
    suggested_total_pct: float | None
    confidence: str  # "alta" | "média" | "baixa"
    n_neighbors: int
    n_enter: int
    why: str


def _catalog_rank(status: Any) -> float | None:
    if not status:
        return None
    return _CATALOG_RANK.get(str(status).lower())


def featurize(ctx: dict[str, Any]) -> dict[str, float | None]:
    """Extrai o vetor de features de um snapshot de contexto (cru → numérico).
    Aceita tanto o ``decision_context`` quanto o ``context`` da action — mesmas
    chaves (vêm do mesmo ``_build_decision_context``)."""

    def num(key: str) -> float | None:
        v = ctx.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    cur = num("current_price")
    ptw = num("price_to_win")
    gap = (cur - ptw) / cur if (cur and ptw and cur > 0) else None
    cov = num("coverage_days")
    return {
        "coverage_days": min(cov, 365.0) if cov is not None else None,
        "margin_pct": num("margin_pct"),
        "discount_pct": num("discount_pct"),
        # log1p amortece a cauda longa de vendas
        "sales_30d": math.log1p(num("sales_30d") or 0.0)
        if ctx.get("sales_30d") is not None
        else None,
        "momentum": num("momentum"),
        "catalog_rank": _catalog_rank(ctx.get("catalog_status")),
        "price_to_win_gap": gap,
        "cap_pct": num("cap_pct"),
    }


async def load_samples(session: AsyncSession) -> list[Sample]:
    """Carrega TODOS os exemplos rotulados pelo usuário (decisões + ações)."""
    out: list[Sample] = []

    dec_rows = (
        await session.execute(
            text(
                "SELECT promo_type, status, target_price, list_price, decision_context "
                "FROM ml_promo_decisions "
                "WHERE decided_by IS NOT NULL AND decision_context IS NOT NULL "
                "  AND status IN ('approved', 'rejected', 'ignored')"
            )
        )
    ).all()
    for promo_type, status, target_price, list_price, ctx in dec_rows:
        if not isinstance(ctx, dict):
            continue
        label = 1 if status == "approved" else -1
        pct = _pct(ctx.get("discount_pct"), target_price, list_price)
        out.append(Sample(promo_type or "?", label, pct, featurize(ctx)))

    # Ações da aba Promoções: entrar/alterar/recriar = positivo; sair = negativo.
    act_rows = (
        await session.execute(
            text(
                "SELECT promo_type, action, price_after, context "
                "FROM ml_promo_actions "
                "WHERE dry_run = false AND context IS NOT NULL "
                "  AND action IN ('enroll_offer','enroll_campaign_item',"
                "    'direct_create_price_discount','modify_promotion',"
                "    'resubscribe_promotion','exit_promotion','reprice','activate_smart')"
            )
        )
    ).all()
    for promo_type, action, price_after, ctx in act_rows:
        if not isinstance(ctx, dict):
            continue
        label = -1 if action == "exit_promotion" else 1
        pct = _pct(ctx.get("discount_pct"), price_after, ctx.get("list_price"))
        out.append(Sample(promo_type or "?", label, pct, featurize(ctx)))

    return out


def _pct(discount_pct: Any, price: Any, list_price: Any) -> float | None:
    try:
        if discount_pct is not None:
            return float(discount_pct)
        p, lp = float(price), float(list_price)
        return (1 - p / lp) * 100 if lp > 0 else None
    except (TypeError, ValueError, ZeroDivisionError):
        return None


@dataclass
class _Stats:
    mean: dict[str, float] = field(default_factory=dict)
    std: dict[str, float] = field(default_factory=dict)


def _fit_stats(samples: list[Sample]) -> _Stats:
    st = _Stats()
    for f in _NUMERIC_FEATURES:
        vals: list[float] = [v for s in samples if (v := s.feats.get(f)) is not None]
        if not vals:
            st.mean[f], st.std[f] = 0.0, 1.0
            continue
        m = sum(vals) / len(vals)
        var = sum((v - m) ** 2 for v in vals) / len(vals)
        st.mean[f] = m
        st.std[f] = math.sqrt(var) or 1.0
    return st


def _z(feats: dict[str, float | None], st: _Stats) -> dict[str, float]:
    # Nulo → 0 (= média): eixo neutro, não puxa distância pra nenhum lado.
    out: dict[str, float] = {}
    for f in _NUMERIC_FEATURES:
        v = feats.get(f)
        out[f] = ((v - st.mean[f]) / st.std[f]) if v is not None else 0.0
    return out


def _dist(a: dict[str, float], b: dict[str, float]) -> float:
    return math.sqrt(sum((a[f] - b[f]) ** 2 for f in _NUMERIC_FEATURES))


def recommend(
    cand_ctx: dict[str, Any], promo_type: str, samples: list[Sample]
) -> Recommendation | None:
    """Recomenda (ou None = calado) pra uma promoção candidata, com base nos
    vizinhos do MESMO tipo. None quando não há vizinhos parecidos o bastante."""
    same = [s for s in samples if s.promo_type == promo_type]
    if len(same) < _MIN_NEIGHBORS:
        return None
    st = _fit_stats(same)
    cz = _z(featurize(cand_ctx), st)
    scored = sorted(((_dist(cz, _z(s.feats, st)), s) for s in same), key=lambda x: x[0])
    neigh = [(d, s) for d, s in scored[:_K] if d <= _MAX_DIST]
    if len(neigh) < _MIN_NEIGHBORS:
        return None

    wsum = sum(1.0 / (1.0 + d) for d, _ in neigh)
    score = sum((1.0 / (1.0 + d)) * s.label for d, s in neigh) / wsum
    enter = [s for _, s in neigh if s.label == 1]
    n_enter = len(enter)
    n = len(neigh)

    pcts = sorted(s.chosen_total_pct for s in enter if s.chosen_total_pct is not None)
    suggested = pcts[len(pcts) // 2] if pcts else None

    if score >= 0.34:
        action = "enter"
    elif score <= -0.34:
        action = "skip"
    else:
        action = "neutral"

    agree = max(n_enter, n - n_enter) / n
    if n >= 8 and agree >= 0.75:
        confidence = "alta"
    elif n >= 6 and agree >= 0.6:
        confidence = "média"
    else:
        confidence = "baixa"

    why = f"{n_enter}/{n} decisões parecidas você entrou"
    if action == "skip":
        why = f"{n - n_enter}/{n} decisões parecidas você pulou/saiu"
    if suggested is not None and action == "enter":
        why += f" (~{suggested:.0f}% off)"

    return Recommendation(action, suggested, confidence, n, n_enter, why)
