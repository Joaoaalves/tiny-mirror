"""Smoke test for the side-effect-free analyze path.

Two guarantees we care about:
  1. analyze_sku_dry does NOT write to ml_promo_actions / ml_promo_alerts
     (it never instantiates those repos with the session).
  2. analyze_sku_dry never calls fetch_gas_costs (it reads the snapshot
     from Postgres instead).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiny_mirror.services.ml_promotion_service import (
    MLPromotionService,
    _snapshot_to_costs,
)

pytestmark = pytest.mark.unit


def _fake_cap() -> SimpleNamespace:
    return SimpleNamespace(
        sku="X",
        max_seller_share_pct=Decimal("30"),
        margin_floor_price=Decimal("39.90"),
        freight_band_opt=False,
        excluded_promo_types=[],
    )


def _fake_snapshot() -> SimpleNamespace:
    return SimpleNamespace(
        list_price=Decimal("57"),
        sheet_promo_price=Decimal("39.90"),
        freight_bands=[{"min": 0, "max": 1000, "cost": 5}],
    )


@pytest.mark.asyncio
async def test_analyze_sku_dry_skips_when_cap_missing() -> None:
    service = MLPromotionService(token_service=MagicMock(), http_client=MagicMock())
    session = MagicMock()
    with patch("tiny_mirror.services.ml_promotion_service.MLPromoCapRepository") as cap_repo_cls:
        cap_repo_cls.return_value.get = AsyncMock(return_value=None)
        out = await service.analyze_sku_dry(session, "no-cap-sku")
    assert out == []


@pytest.mark.asyncio
async def test_analyze_sku_dry_does_not_touch_actions_or_alerts() -> None:
    """The signature of analyze_sku_dry must never construct ActionRepo /
    AlertRepo — that's the side-effect-free guarantee."""
    service = MLPromotionService(token_service=MagicMock(), http_client=MagicMock())
    session = MagicMock()

    cap = _fake_cap()
    snap = _fake_snapshot()

    with (
        patch("tiny_mirror.services.ml_promotion_service.MLPromoCapRepository") as cap_repo_cls,
        patch("tiny_mirror.services.ml_promotion_service.MLListingRepository") as list_repo_cls,
        patch(
            "tiny_mirror.services.ml_promotion_service.MLCostsSnapshotRepository"
        ) as snap_repo_cls,
        patch(
            "tiny_mirror.services.ml_promotion_service.MLPromoActionRepository"
        ) as action_repo_cls,
        patch("tiny_mirror.services.ml_promotion_service.MLPromoAlertRepository") as alert_repo_cls,
        patch.object(MLPromotionService, "fetch_eligible_promos", new=AsyncMock(return_value=[])),
        patch.object(
            MLPromotionService,
            "fetch_gas_costs",
            new=AsyncMock(side_effect=AssertionError("GAS must not be called in dry path")),
        ),
        patch.object(MLPromotionService, "fetch_price_to_win", new=AsyncMock(return_value=None)),
    ):
        cap_repo_cls.return_value.get = AsyncMock(return_value=cap)
        list_repo_cls.return_value.get_active_mlb_ids_for_sku = AsyncMock(return_value=["MLB1"])
        snap_repo_cls.return_value.get = AsyncMock(return_value=snap)

        out = await service.analyze_sku_dry(session, "X")

    assert len(out) == 1
    action_repo_cls.assert_not_called()
    alert_repo_cls.assert_not_called()


def test_snapshot_to_costs_carries_floor_and_bands() -> None:
    snap = _fake_snapshot()
    out = _snapshot_to_costs(snap)
    assert out["listPrice"] == 57.0
    assert out["promoPrice"] == 39.9
    assert out["freightBands"][0]["cost"] == 5
