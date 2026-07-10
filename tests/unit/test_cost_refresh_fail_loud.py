"""Fail-loud guards do payload de custos da planilha Controle 4.0.

Contexto: o Apps Script lê a aba "Mercado Livre" por POSIÇÃO de coluna. Quando
inseriram uma coluna na aba GERAL (2026-07-08), o endpoint irmão passou a
devolver COD. FAB como se fosse SKU — JSON válido, valores errados, zero alarme.
``ml_costs_snapshot`` alimenta caps e margens, então gravar lixo é pior que
abortar. Estes testes travam esse comportamento.

Baseline real (2026-07-10, 515 itens): mlb inválido 0.4%, resto 0%.
"""

from __future__ import annotations

from typing import Any

import pytest

from tiny_mirror.services.cost_refresh_service import (
    CostRefreshError,
    _assert_payload_sane,
)

pytestmark = pytest.mark.unit


def _item(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "sku": "SKU-1",
        "baseCost": 19.15,
        "freightBands": [{"min": 0, "max": 18.99, "cost": 5.65}],
    }
    base.update(over)
    return base


def _payload(n: int, **over: Any) -> dict[str, Any]:
    return {f"MLB{5000000000 + i}": _item(**over) for i in range(n)}


def test_healthy_payload_passes_and_reports_zero_ratios() -> None:
    ratios = _assert_payload_sane(_payload(50))
    assert ratios == {
        "invalid_mlb": 0.0,
        "missing_sku": 0.0,
        "missing_cost": 0.0,
        "missing_bands": 0.0,
    }


def test_empty_payload_raises() -> None:
    with pytest.raises(CostRefreshError, match="0 items"):
        _assert_payload_sane({})


def test_tolerates_the_few_genuinely_malformed_cells_seen_in_prod() -> None:
    """Baseline real tem ~0.4% de células tipo 'MLB4078501557 / 4078501557'."""
    items = _payload(99)
    items["MLB4078501557 / 4078501557"] = _item()  # 1 de 100 = 1% < teto 5%
    ratios = _assert_payload_sane(items)
    assert ratios["invalid_mlb"] == pytest.approx(0.01)


def test_column_shift_turning_keys_into_cod_fab_aborts() -> None:
    """O modo de falha exato do incidente: a chave deixa de ser um MLB."""
    items = {f"COD-FAB-{i}": _item() for i in range(20)}
    with pytest.raises(CostRefreshError, match="não parece um MLB"):
        _assert_payload_sane(items)


def test_numeric_cod_fab_keys_also_abort() -> None:
    """No manual_status as chaves viraram '7551', '11851' — números puros."""
    items = {str(7000 + i): _item() for i in range(20)}
    with pytest.raises(CostRefreshError, match="não parece um MLB"):
        _assert_payload_sane(items)


def test_shifted_sku_column_aborts() -> None:
    items = _payload(20, sku="")
    with pytest.raises(CostRefreshError, match="sku vazio"):
        _assert_payload_sane(items)


def test_shifted_cost_column_aborts() -> None:
    items = _payload(20, baseCost=None)
    with pytest.raises(CostRefreshError, match="baseCost ausente"):
        _assert_payload_sane(items)


def test_zero_cost_counts_as_missing() -> None:
    items = _payload(20, baseCost=0)
    with pytest.raises(CostRefreshError, match="baseCost ausente"):
        _assert_payload_sane(items)


def test_missing_freight_bands_aborts() -> None:
    items = _payload(20, freightBands=[])
    with pytest.raises(CostRefreshError, match="freightBands ausente"):
        _assert_payload_sane(items)


def test_error_message_names_the_offenders_so_a_human_can_act() -> None:
    items = {f"BAD-{i}": _item() for i in range(20)}
    with pytest.raises(CostRefreshError) as exc:
        _assert_payload_sane(items)
    msg = str(exc.value)
    assert "20/20" in msg
    assert "BAD-0" in msg
    assert "Mercado Livre" in msg  # aponta a aba a conferir


def test_ratios_not_enforced_below_min_sample() -> None:
    """Com poucos itens a proporção é ruído: 1 célula ruim em 2 já daria 50%.
    O skip por linha (_VALID_MLB_RE) continua protegendo o banco."""
    items = {"MLB3884049149": _item(), "MLB4078501557 / 4078501557": _item()}
    ratios = _assert_payload_sane(items)  # não levanta
    assert ratios["invalid_mlb"] == pytest.approx(0.5)


def test_min_sample_boundary_enforces_at_twenty() -> None:
    items = {f"BAD-{i}": _item() for i in range(19)}
    assert _assert_payload_sane(items)["invalid_mlb"] == 1.0  # 19 itens: não enforça
    items["BAD-19"] = _item()
    with pytest.raises(CostRefreshError):  # 20 itens: enforça
        _assert_payload_sane(items)


def test_non_dict_rows_do_not_crash_the_guard() -> None:
    items: dict[str, Any] = dict(_payload(19))
    items["MLB9999999999"] = "linha corrompida"
    ratios = _assert_payload_sane(items)  # 1/20 sem sku/cost -> abaixo dos tetos
    assert ratios["invalid_mlb"] == 0.0
