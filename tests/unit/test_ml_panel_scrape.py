"""Unit tests do parser do painel de promoções (ml_panel_scrape_service).

A fixture reproduz a estrutura REAL do JSON embutido (__NORDIC_RENDERING_CTX__
→ brickTree → promotionList{promotionBoxes,collapsibleRows}) observada em
2026-07-10, incluindo a banca SMART ("Reduzimos R$ X") em segments.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from tiny_mirror.services.ml_panel_scrape_service import (
    _money,
    extract_ctx,
    parse_page,
)

pytestmark = pytest.mark.unit

SELLER = "MLB227584372"


def _txt(content: str, secondary: str | None = None) -> dict:
    line: dict = {"primaryText": {"content": content}}
    if secondary is not None:
        line["secondaryText"] = {"content": secondary}
    return {"lines": [line]}


def _row(
    name: str,
    *,
    badge: str | None = None,
    vig: str = "7 a 12/jul",
    chip: str | None = None,
    desc_rs: str = "R$ 3",
    desc_pct: str = "(5%)",
    sugerido: bool = True,
    final: str = "R$ 56,86",
    recebe: str = "R$\xa042,57",
    tarifa: str = "-R$\xa06,54",
    envio: str = "-R$\xa07,75",
    reduzimos: str | None = None,
    action: str = "Participar",
    is_coupon: bool = False,
) -> dict:
    col0 = {"lines": [{"primaryText": {"content": name}}]}
    if badge:
        col0["lines"].append({"primaryText": {"content": badge}})
    col1 = {"lines": [{"primaryText": {"content": vig}}]}
    if chip:
        col1["lines"].append({"primaryText": {"content": chip}})
    col2 = {
        "lines": [{"primaryText": {"content": desc_rs}, "secondaryText": {"content": desc_pct}}]
    }
    if sugerido:
        col2["lines"].append({"primaryText": {"content": "Desconto sugerido"}})
    col4_lines = [
        {
            "type": "charges",
            "totalCharges": {
                "title": "Você recebe",
                "value": recebe,
                "detail": {
                    "rows": [
                        {"label": "Preço*", "value": final},
                        {"label": "Tarifa de venda", "value": tarifa, "description": "Clássico"},
                        {"label": "Custo de envio Full", "value": envio},
                        {"label": "Você recebe", "value": recebe},
                    ]
                },
            },
        }
    ]
    if reduzimos:
        col4_lines.append(
            {
                "segments": [
                    {"content": "Reduzimos "},
                    {"content": f"R$ {reduzimos}"},
                    {"content": " das suas tarifas por cada venda"},
                ]
            }
        )
    return {
        "isEmptyState": False,
        "isCoupon": is_coupon,
        "columns": [
            col0,
            col1,
            col2,
            _txt(final),
            {"lines": col4_lines},
            {"lines": [{"text": action}]},
        ],
    }


def _page(mlb: str, boxes: list, collapsible: list) -> str:
    ctx = {
        "appProps": {
            "pageProps": {
                "brickTree": {
                    "bricks": [
                        {
                            "data": {"tracks": [{"event_data": {"items": [{"item_id": mlb}]}}]},
                            "bricks": [
                                {
                                    "data": {
                                        "promotionList": {
                                            "promotionBoxes": boxes,
                                            "collapsibleRows": collapsible,
                                        }
                                    }
                                }
                            ],
                        },
                        {"data": {"seller": SELLER}},
                    ]
                }
            }
        }
    }
    return (
        '<html><script id="__NORDIC_RENDERING_CTX__" nonce="x">_n.ctx.r='
        + json.dumps(ctx, ensure_ascii=False)
        + ";_n.ctx.r.assets=1</script></html>"
    )


def test_money_parser() -> None:
    assert _money("R$ 56,86") == Decimal("56.86")
    assert _money("R$\xa042,57") == Decimal("42.57")
    assert _money("-R$\xa06,54") == Decimal("6.54")
    assert _money("R$ 19") == Decimal("19")
    assert _money("R$ 1.057,00") == Decimal("1057.00")
    assert _money("Combinar") is None


def test_extract_ctx_handles_trailing_js() -> None:
    html = _page("MLB3709682777", [], [])
    ctx = extract_ctx(html)
    assert ctx is not None and "appProps" in ctx


def test_parse_page_full_row_with_smart_reduction() -> None:
    smart = _row(
        "DESTAQUE JULHO",
        vig="1/jul a 1/ago",
        desc_rs="R$ 37,72",
        desc_pct="(63%)",
        final="R$ 22,14",
        recebe="R$\xa014,36",
        reduzimos="1,32",
        sugerido=False,
    )
    deal = _row("Julho de Ferias", chip=None)
    ativa = _row(
        "07.07 e Descontaco",
        badge="DESCONTAÇO",
        chip="ATIVA",
        action="Alterar",
        desc_rs="R$ 19,36",
        desc_pct="(32%)",
        final="R$ 40,50",
        sugerido=False,
    )
    html = _page("MLB3709682777", [smart, ativa], [deal])
    rows = parse_page(html, SELLER)
    assert {r.promo_name for r in rows} == {
        "DESTAQUE JULHO",
        "Julho de Ferias",
        "07.07 e Descontaco",
    }
    by = {r.promo_name: r for r in rows}

    d = by["DESTAQUE JULHO"]
    assert d.mlb_id == "MLB3709682777"
    assert d.final_price == Decimal("22.14")
    assert d.discount_value == Decimal("37.72")
    assert d.discount_pct == Decimal("63")
    assert d.meli_reduction == Decimal("1.32")  # banca SMART "Reduzimos R$ 1,32"
    assert d.you_receive == Decimal("14.36")
    assert d.sale_fee == Decimal("6.54")
    assert d.shipping_cost == Decimal("7.75")
    assert d.listing_type_label == "Clássico"

    f = by["Julho de Ferias"]
    assert f.final_price == Decimal("56.86")
    assert f.discount_pct == Decimal("5")
    assert f.is_suggested is True
    assert f.meli_reduction is None
    assert f.vigencia == "7 a 12/jul"

    a = by["07.07 e Descontaco"]
    assert a.status_chip == "ATIVA"
    assert a.badge == "DESCONTAÇO"
    assert a.action_label == "Alterar"
    assert a.final_price == Decimal("40.50")


def test_parse_page_skips_rows_without_mlb_link() -> None:
    # sem item_id no ancestral → linha não vinculável → descartada
    ctx = {"a": {"promotionList": {"promotionBoxes": [_row("X")], "collapsibleRows": []}}}
    html = '<script id="__NORDIC_RENDERING_CTX__">_n.ctx.r=' + json.dumps(ctx) + "</script>"
    assert parse_page(html, SELLER) == []
