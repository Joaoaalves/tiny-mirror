"""A URL do painel precisa apontar para a central de vendedores.

Em 2026-08-09 o ML migrou o painel de ``www.mercadolivre.com.br`` para
``vendedores.mercadolivre.com.br``. A URL antiga responde 302 e, como o
scrape não segue redirect, o serviço passou a receber a página vazia do
redirect — o painel ficou 4 dias sem dados e o probe do host reportou
"login wall" (falso positivo, a sessão estava válida).
"""

from __future__ import annotations

import pytest

from tiny_mirror.services.ml_panel_scrape_service import _BASE

pytestmark = pytest.mark.unit


def test_base_url_uses_seller_central_domain() -> None:
    assert _BASE.startswith("https://vendedores.mercadolivre.com.br/")


def test_base_url_is_not_the_redirecting_www_host() -> None:
    # www.* devolve 302 -> corpo sem __NORDIC_RENDERING_CTX__ -> scrape vazio
    assert "www.mercadolivre.com.br" not in _BASE


def test_base_url_points_to_promos_listing() -> None:
    assert _BASE.endswith("/anuncios/lista/promos")
