"""Scrape da verdade do PAINEL de promoções do vendedor (ML).

A API oficial ``seller-promotions`` serve dados defasados ou ausentes para
promoções CANDIDATAS: sugestões de preço antigas, percentuais SMART que não
batem com o painel e campanhas que só existem no painel (verificado caso a
caso em 2026-07-10). A página ``/anuncios/lista/promos`` da central de
vendedores renderiza a verdade no HTML, num JSON embutido
(``__NORDIC_RENDERING_CTX__``): nome, vigência, desconto R$/%, preço final,
"você recebe", detalhe de custos (tarifa/envio) e a banca SMART
("Reduzimos R$ X das suas tarifas por cada venda").

Este serviço busca essas páginas com a SESSÃO WEB do vendedor (cookie jar
Netscape mantido aquecido pelo probe ``ml_panel_probe.sh`` no cron do host),
parseia o JSON e grava em ``ml_panel_promos``. O board sobrepõe esses valores
nas candidatas; inscritas ficam na API oficial (já exata, webhook ~1min).

Sem browser: a página responde a GET simples com cookies (validado por
replay). Se a sessão cair, o fetch detecta a tela de login e o sweep aborta
com ``login_wall=True`` — o probe do host alerta no Telegram.
"""

from __future__ import annotations

import http.cookiejar
import json
import re
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLPanelPromoORM

logger = structlog.get_logger(__name__)

_BASE = "https://www.mercadolivre.com.br/anuncios/lista/promos"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
_CTX_MARKER = "__NORDIC_RENDERING_CTX__"
_CTX_PREFIX = "_n.ctx.r="
_MLB_RE = re.compile(r"^MLB\d{9,}$")
_MONEY_RE = re.compile(r"-?R\$\s*([\d.]+(?:,\d{1,2})?)")
_PCT_RE = re.compile(r"\((\d+(?:[.,]\d+)?)%\)")
_REDUZIMOS_RE = re.compile(r"Reduzimos\s*R\$\s*([\d.]+(?:,\d{1,2})?)")
_VIGENCIA_RE = re.compile(r"\d+(?:/\w+)?\s*a\s*\d+/\w+")
_STATUS_CHIPS = {"ATIVA", "PROGRAMADA", "PAUSADA", "FINALIZADA"}
_PROMOID_RE = re.compile(r"promoId=([\w-]+)")
_PROMOTYPE_RE = re.compile(r"promotionType=([a-z_]+)")
# Tipo do painel (urlCallback) → promotion_type da seller-promotions API. Só os
# que ``apply_decision_to_ml`` sabe inscrever entram como enrolláveis; SMART é
# ML-managed (fluxo activate-smart) e cupom é escondido.
_PANEL_TYPE_MAP = {
    "tier": "DEAL",
    "seller_campaigns": "SELLER_CAMPAIGN",
    "price_discount": "PRICE_DISCOUNT",
    "smart_campaign": "SMART",
    "seller_coupon_campaigns_massive": "SELLER_COUPON_CAMPAIGN",
}


class PanelSessionExpired(Exception):
    """A resposta veio sem dados de promoção e com cara de tela de login."""


def _money(s: str | None) -> Decimal | None:
    if not s:
        return None
    m = _MONEY_RE.search(s.replace("\xa0", " "))
    if not m:
        return None
    try:
        return Decimal(m.group(1).replace(".", "").replace(",", "."))
    except InvalidOperation:  # pragma: no cover — defensive
        return None


@dataclass
class PanelPromo:
    mlb_id: str
    promo_name: str
    badge: str | None = None
    status_chip: str | None = None
    vigencia: str | None = None
    discount_value: Decimal | None = None
    discount_pct: Decimal | None = None
    final_price: Decimal | None = None
    you_receive: Decimal | None = None
    sale_fee: Decimal | None = None
    shipping_cost: Decimal | None = None
    listing_type_label: str | None = None
    meli_reduction: Decimal | None = None
    is_suggested: bool = False
    is_coupon: bool = False
    action_label: str | None = None
    # id + tipo (mapeado p/ a API) da campanha, extraídos do botão de ação do
    # painel — permitem INSCREVER via /promotions/enroll mesmo quando a
    # seller-promotions API não lista a campanha pro anúncio (panel-only).
    promo_id: str | None = None
    api_promo_type: str | None = None


# ---------------------------------------------------------------------- parse
def extract_ctx(html: str) -> Any:
    """JSON embutido do renderer (``_n.ctx.r={...}``) ou None."""
    s0 = html.find(_CTX_MARKER)
    if s0 < 0:
        return None
    inner = html[html.find(">", s0) + 1 : html.find("</script>", s0)]
    j = inner.find(_CTX_PREFIX)
    if j < 0:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(inner[j + len(_CTX_PREFIX) :].lstrip())
        return obj
    except json.JSONDecodeError:
        return None


def _walk(o: Any, path: tuple[Any, ...] = ()) -> Any:
    if isinstance(o, dict):
        for k, v in o.items():
            yield from _walk(v, (*path, k))
    elif isinstance(o, list):
        for n, v in enumerate(o):
            yield from _walk(v, (*path, n))
    else:
        yield path, o


def _texts_in_order(node: Any) -> list[tuple[str, str]]:
    """(chave, texto) na ordem do documento — base do parser posicional."""
    out: list[tuple[str, str]] = []

    def w(o: Any) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if k in (
                    "content",
                    "value",
                    "title",
                    "label",
                    "text",
                    "description",
                ) and isinstance(v, str):
                    out.append((k, v))
                else:
                    w(v)
        elif isinstance(o, list):
            for x in o:
                w(x)

    w(node)
    return out


def _parse_row(row: dict[str, Any], mlb_id: str) -> PanelPromo | None:
    """Uma linha de promoção (box ou collapsible) → PanelPromo.

    Colunas observadas: [0] nome (+badge), [1] vigência (+chip de estado),
    [2] desconto "R$ X" "(Y%)" (+"Desconto sugerido"), [3] preço final,
    [4] você recebe + detalhe de custos (+"Reduzimos R$ X"), [5] ação.
    Parser posicional por coluna com regex — resiliente a linhas a mais.
    """
    cols = row.get("columns") or []
    if not cols:
        return None
    p = PanelPromo(mlb_id=mlb_id, promo_name="", is_coupon=bool(row.get("isCoupon")))

    c0 = [v for _, v in _texts_in_order(cols[0])] if len(cols) > 0 else []
    if not c0:
        return None
    p.promo_name = c0[0].strip()
    if len(c0) > 1:
        p.badge = c0[1].strip() or None

    for _, v in _texts_in_order(cols[1]) if len(cols) > 1 else []:
        vv = v.strip()
        if vv in _STATUS_CHIPS:
            p.status_chip = vv
        elif _VIGENCIA_RE.search(vv):
            p.vigencia = vv

    if len(cols) > 2:
        c2 = [v for _, v in _texts_in_order(cols[2])]
        for v in c2:
            if p.discount_value is None and _MONEY_RE.search(v):
                p.discount_value = _money(v)
            m = _PCT_RE.search(v)
            if m and p.discount_pct is None:
                p.discount_pct = Decimal(m.group(1).replace(",", "."))
            if "sugerido" in v.lower():
                p.is_suggested = True

    if len(cols) > 3:
        for v in [v for _, v in _texts_in_order(cols[3])]:
            if _MONEY_RE.search(v):
                p.final_price = _money(v)
                break

    if len(cols) > 4:
        c4 = _texts_in_order(cols[4])
        joined = " ".join(v for _, v in c4)
        m = _REDUZIMOS_RE.search(joined.replace("\xa0", " "))
        if m:
            p.meli_reduction = _money("R$ " + m.group(1))
        # detalhe de custos: pares label→value
        last_label: str | None = None
        for k, v in c4:
            if k == "label":
                last_label = v.strip().lower()
            elif k == "value" and last_label is not None:
                val = _money(v)
                if val is not None:
                    if "tarifa" in last_label:
                        p.sale_fee = abs(val)
                    elif "envio" in last_label or "frete" in last_label:
                        p.shipping_cost = abs(val)
                    elif "recebe" in last_label and p.you_receive is None:
                        p.you_receive = val
                last_label = None
            elif k == "title" and "recebe" in v.lower():
                last_label = "recebe"
            elif k == "description" and p.sale_fee is not None and p.listing_type_label is None:
                vv = v.strip()
                # a descrição logo após a tarifa é o tipo de anúncio (Clássico/Premium)
                if vv and len(vv) < 30 and "valor" not in vv.lower():
                    p.listing_type_label = vv

    if len(cols) > 5:
        for k, v in _texts_in_order(cols[5]):
            if k == "text" and v.strip():
                p.action_label = v.strip()
                break
        # id + tipo da campanha: preferimos o urlCallback do botão, que traz os
        # DOIS juntos ("...promotionType=tier&...&promoId=P-MLB...") — evita casar
        # o tipo de um callback com o id de outro. Fallback: primeiros soltos.
        for _, v in _walk(cols[5]):
            if isinstance(v, str) and "promotionType=" in v and "promoId=" in v:
                mt = _PROMOTYPE_RE.search(v)
                mid = _PROMOID_RE.search(v)
                if mt:
                    p.api_promo_type = _PANEL_TYPE_MAP.get(mt.group(1))
                if mid:
                    p.promo_id = mid.group(1)
                break
        if p.promo_id is None:
            for _, v in _walk(cols[5]):
                if isinstance(v, str) and re.fullmatch(r"[A-Z]+-MLB\d+", v):
                    p.promo_id = v
                    break

    return p if p.promo_name else None


def parse_page(html: str, seller_mlb: str) -> list[PanelPromo]:
    """Todas as promoções (todas as linhas de todos os anúncios) da página.

    Vínculo linha→MLB: sobe do ``promotionList`` na árvore até o menor
    subtree que contenha exatamente 1 MLB de anúncio (validado 25/25).
    ``seller_mlb`` = "MLB"+user_id, excluído da busca.
    """
    ctx = extract_ctx(html)
    if ctx is None:
        return []

    def node_at(path: tuple[Any, ...]) -> Any:
        o = ctx
        for part in path:
            o = o[part]
        return o

    def find_pls(o: Any, path: tuple[Any, ...] = ()) -> Any:
        if isinstance(o, dict):
            if "promotionList" in o:
                yield path, o["promotionList"]
            for k, v in o.items():
                yield from find_pls(v, (*path, k))
        elif isinstance(o, list):
            for n, v in enumerate(o):
                yield from find_pls(v, (*path, n))

    out: list[PanelPromo] = []
    for path, pl in find_pls(ctx):
        mlb: str | None = None
        for up in range(1, 10):
            anc = path[:-up] if up <= len(path) else ()
            try:
                sub = node_at(anc)
            except Exception:
                continue
            mset = {
                v
                for _, v in _walk(sub)
                if isinstance(v, str) and _MLB_RE.match(v) and v != seller_mlb
            }
            if len(mset) == 1:
                mlb = next(iter(mset))
                break
            if len(mset) > 1:
                break  # subiu demais (pegou a página toda)
        if not mlb:
            continue
        for row in (pl.get("promotionBoxes") or []) + (pl.get("collapsibleRows") or []):
            if not isinstance(row, dict) or row.get("isEmptyState"):
                continue
            parsed = _parse_row(row, mlb)
            if parsed:
                out.append(parsed)
    return out


# ---------------------------------------------------------------------- fetch
class MLPanelScrapeService:
    def __init__(self, http_client: httpx.AsyncClient, cookie_jar_path: str, ml_user_id: str):
        self._http = http_client
        self._jar_path = cookie_jar_path
        self._seller_mlb = f"MLB{ml_user_id}"

    def _cookie_header(self) -> str:
        """Header Cookie a partir do jar Netscape (mantido/rotacionado pelo probe
        do host — aqui só LEMOS; escrever concorrentemente arriscaria corromper)."""
        jar = http.cookiejar.MozillaCookieJar(self._jar_path)
        jar.load(ignore_discard=True, ignore_expires=True)
        return "; ".join(f"{c.name}={c.value}" for c in jar)

    async def fetch_page(self, page: int, search: str | None = None) -> str:
        params: dict[str, Any] = {"page": page}
        if search:
            params["search"] = f" {search.lower()}"
        r = await self._http.get(
            _BASE,
            params=params,
            headers={
                "Cookie": self._cookie_header(),
                "User-Agent": _UA,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "pt-BR,pt;q=0.9",
            },
            timeout=45.0,
            follow_redirects=False,
        )
        if r.status_code in (301, 302, 303, 307, 308):
            raise PanelSessionExpired(
                f"redirect {r.status_code} -> {r.headers.get('location', '')[:120]}"
            )
        r.raise_for_status()
        html = r.text
        if "promotionList" not in html and _CTX_MARKER not in html:
            raise PanelSessionExpired("página sem dados de promoção (tela de login?)")
        return html

    async def _upsert(self, promos: list[PanelPromo]) -> None:
        by_mlb: dict[str, list[PanelPromo]] = {}
        for p in promos:
            by_mlb.setdefault(p.mlb_id, []).append(p)
        async with AsyncSessionLocal() as session:
            for mlb, rows in by_mlb.items():
                await session.execute(
                    text("DELETE FROM ml_panel_promos WHERE mlb_id = :m"), {"m": mlb}
                )
                # dedup por nome (o painel pode repetir a linha no box e no collapsible)
                seen: dict[str, PanelPromo] = {}
                for p in rows:
                    seen.setdefault(p.promo_name, p)
                await session.execute(
                    pg_insert(MLPanelPromoORM),
                    [asdict(p) for p in seen.values()],
                )
            await session.commit()

    async def refresh_mlb(self, mlb_id: str) -> dict[str, Any]:
        """On-demand: a verdade do painel pra UM anúncio (?search=mlb…)."""
        html = await self.fetch_page(1, search=mlb_id)
        promos = [p for p in parse_page(html, self._seller_mlb) if p.mlb_id == mlb_id.upper()]
        if promos:
            await self._upsert(promos)
        return {"mlb_id": mlb_id, "promos": len(promos)}

    async def _demote_stale_panel_enrolls(self) -> int:
        """Cura inscrições panel-only que o operador DESFEZ pelo painel do ML.

        A linha started criada pelo enroll panel-only tem ``enrolled_at`` (proteção
        anti-flap do eligible) e ``raw='{}'`` (nosso marcador de origem). Se o
        PAINEL — que é a fonte da campanha — voltou a oferecer "Participar" sem
        chip pra mesma (mlb, campanha), a inscrição morreu do lado do ML e a linha
        ficou órfã (incidente Inverno 2026-07-10: exit via painel → stale em
        Inscritas). Janela de 2h protege contra o lag do próprio painel logo após
        um enroll legítimo. Só toca linhas de origem panel-only; as demais são
        reconciliadas pelo sweep da API/webhooks."""
        async with AsyncSessionLocal() as session:
            res = await session.execute(
                text(
                    "DELETE FROM ml_promotions mp "
                    "WHERE mp.raw = '{}'::jsonb AND mp.enrolled_at IS NOT NULL "
                    "  AND mp.enrolled_at < now() - interval '2 hours' "
                    "  AND mp.status IN ('started', 'pending') "
                    "  AND EXISTS ("
                    "    SELECT 1 FROM ml_panel_promos pp "
                    "    WHERE pp.mlb_id = mp.mlb_id "
                    "      AND lower(trim(pp.promo_name)) = lower(trim(mp.name)) "
                    "      AND pp.scraped_at > now() - interval '2 hours' "
                    "      AND pp.status_chip IS NULL "
                    "      AND lower(COALESCE(pp.action_label, '')) LIKE 'particip%')"
                )
            )
            await session.commit()
            n = getattr(res, "rowcount", 0) or 0
            if n:
                logger.info("ml_panel_demoted_stale_enrolls", rows=n)
            return n

    async def run_sweep(self, max_pages: int = 40) -> dict[str, Any]:
        """Varre as páginas do painel (25 anúncios/página) e grava tudo."""
        pages = 0
        total_rows = 0
        mlbs: set[str] = set()
        for page in range(1, max_pages + 1):
            html = await self.fetch_page(page)
            promos = parse_page(html, self._seller_mlb)
            if not promos:
                break
            pages += 1
            total_rows += len(promos)
            mlbs.update(p.mlb_id for p in promos)
            await self._upsert(promos)
        # linhas de anúncios que saíram do painel (pausados/delistados) expiram
        async with AsyncSessionLocal() as session:
            gone = await session.execute(
                text("DELETE FROM ml_panel_promos WHERE scraped_at < now() - interval '48 hours'")
            )
            await session.commit()
        stats = {
            "pages": pages,
            "rows": total_rows,
            "mlbs": len(mlbs),
            "stale_deleted": getattr(gone, "rowcount", 0) or 0,
            "demoted_stale_enrolls": await self._demote_stale_panel_enrolls(),
        }
        logger.info("ml_panel_sweep_ok", **stats)
        return stats
