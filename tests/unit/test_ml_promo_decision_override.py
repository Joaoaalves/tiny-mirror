"""Unit coverage for `_apply_target_override` in the ml_promotions router.

Validates that the server re-checks floor/cap when the operator
overrides ``target_price`` before approving. The check uses the
``floor_price`` / ``cap_pct`` / ``meli_percentage`` / ``list_price``
snapshot stored on the decision row at creation time (not the live
cap which may have moved since).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from tiny_mirror.api.routers.ml_promotions import _apply_target_override

pytestmark = pytest.mark.unit


def _row(
    list_price: Decimal | None = Decimal("50.00"),
    floor_price: Decimal | None = Decimal("40.00"),
    cap_pct: Decimal | None = Decimal("20.00"),
    meli_percentage: Decimal | None = Decimal("0"),
    target_price: Decimal | None = Decimal("45.00"),
) -> SimpleNamespace:
    return SimpleNamespace(
        list_price=list_price,
        floor_price=floor_price,
        cap_pct=cap_pct,
        meli_percentage=meli_percentage,
        target_price=target_price,
    )


def _repo(row: SimpleNamespace | None) -> MagicMock:
    repo = MagicMock()
    repo.get = AsyncMock(return_value=row)
    return repo


async def test_override_returns_recomputed_pcts_when_valid() -> None:
    repo = _repo(_row())
    out, warning = await _apply_target_override(
        repo, decision_id=1, override_price=Decimal("45.00")
    )
    assert out["target_price"] == Decimal("45.00")
    # (50 - 45) / 50 * 100 = 10.00
    assert out["target_total_pct"] == Decimal("10.00")
    # no co-pay → seller = total
    assert out["target_seller_pct"] == Decimal("10.00")
    assert warning is None


async def test_override_subtracts_meli_copay_from_seller_pct() -> None:
    repo = _repo(_row(meli_percentage=Decimal("3")))
    out, _ = await _apply_target_override(repo, decision_id=1, override_price=Decimal("45.00"))
    # total 10% - ML 3% = seller 7%
    assert out["target_seller_pct"] == Decimal("7.00")


async def test_override_below_floor_returns_422() -> None:
    """HARD floor: piso violado vira 422 (mudou em 2026-05-29 com o
    executor Phase 5). Nada que vá pro ML pode ficar abaixo do nosso
    piso de margem.
    """
    repo = _repo(_row(floor_price=Decimal("42.00")))
    with pytest.raises(HTTPException) as exc:
        await _apply_target_override(repo, decision_id=1, override_price=Decimal("41.00"))
    assert exc.value.status_code == 422
    assert "piso" in exc.value.detail
    assert "margem em risco" in exc.value.detail


async def test_override_down_without_floor_is_rejected() -> None:
    """Sem piso (custos ausentes) o operador NÃO pode forçar pra baixo.
    Pra cima continua permitido."""
    repo = _repo(_row(floor_price=None, target_price=Decimal("45.00")))
    # Pra cima do alvo gerado (45) → permitido.
    out, warning = await _apply_target_override(
        repo, decision_id=1, override_price=Decimal("46.00")
    )
    assert out["target_price"] == Decimal("46.00")
    assert warning is None
    # Pra baixo do alvo gerado → 422.
    with pytest.raises(HTTPException) as exc:
        await _apply_target_override(repo, decision_id=1, override_price=Decimal("40.00"))
    assert exc.value.status_code == 422
    assert "sem piso" in exc.value.detail


async def test_override_rejects_seller_cap_exceeded() -> None:
    """HARD cap: seller > cap_pct continua 422 (cap do canal ML)."""
    # cap 5% → seller% must stay ≤ 5%. For list=50 and target=40, total=20%.
    repo = _repo(_row(cap_pct=Decimal("5.00"), floor_price=None))
    with pytest.raises(HTTPException) as exc:
        await _apply_target_override(repo, decision_id=1, override_price=Decimal("40.00"))
    assert exc.value.status_code == 422
    assert "cap ML" in exc.value.detail


async def test_override_allows_equal_to_floor() -> None:
    repo = _repo(_row(floor_price=Decimal("42.00")))
    out, warning = await _apply_target_override(
        repo, decision_id=1, override_price=Decimal("42.00")
    )
    assert out["target_price"] == Decimal("42.00")
    assert warning is None


async def test_override_rejects_zero_or_negative() -> None:
    repo = _repo(_row())
    with pytest.raises(HTTPException):
        await _apply_target_override(repo, decision_id=1, override_price=Decimal("0"))


async def test_override_rejects_price_above_list() -> None:
    """Promoção nunca aumenta preço — target > list_price recusa 422."""
    repo = _repo(_row(list_price=Decimal("50.00")))
    with pytest.raises(HTTPException) as exc:
        await _apply_target_override(repo, decision_id=1, override_price=Decimal("55.00"))
    assert exc.value.status_code == 422
    assert "não pode aumentar" in exc.value.detail


async def test_override_allows_equal_to_list_price() -> None:
    """Edge case: target = list_price (desconto zero) é permitido — algumas
    campanhas SMART/PriceMatching ML define preço igual ao list."""
    repo = _repo(_row(list_price=Decimal("50.00"), cap_pct=None, floor_price=None))
    out, _ = await _apply_target_override(repo, decision_id=1, override_price=Decimal("50.00"))
    assert out["target_total_pct"] == Decimal("0.00")


async def test_override_404_when_decision_missing() -> None:
    repo = _repo(None)
    with pytest.raises(HTTPException) as exc:
        await _apply_target_override(repo, decision_id=999, override_price=Decimal("45"))
    assert exc.value.status_code == 404


async def test_override_422_when_list_price_missing() -> None:
    repo = _repo(_row(list_price=None))
    with pytest.raises(HTTPException) as exc:
        await _apply_target_override(repo, decision_id=1, override_price=Decimal("45"))
    assert exc.value.status_code == 422
    assert "list_price" in exc.value.detail
