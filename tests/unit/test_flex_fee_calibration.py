"""Unit tests for the Flex fee-calibration override in margin math.

``_effective_fees`` must override commission + freight ONLY for Flex
(non-fulfillment) listings that have a calibration row. Fulfillment listings
and uncalibrated listings keep the snapshot values untouched.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from tiny_mirror.api.routers.ml_promotions import _effective_fees

pytestmark = pytest.mark.unit


class _Result:
    def __init__(self, val):
        self._val = val

    def scalar_one_or_none(self):
        return self._val


class _Session:
    """Minimal async session: execute() -> logistic_type, get() -> calib."""

    def __init__(self, logistic_type, calib):
        self._logistic = logistic_type
        self._calib = calib

    async def execute(self, _stmt):
        return _Result(self._logistic)

    async def get(self, _model, _key):
        return self._calib


SNAP = SimpleNamespace(
    commission_pct=Decimal("11.50"),
    freight_bands=[{"min": 0, "max": None, "cost": 16.85}],
)
CALIB = SimpleNamespace(
    real_comm_pct=Decimal("16.50"),
    freight_per_unit_lt79=Decimal("6.75"),
    freight_per_unit_ge79=Decimal("18.35"),
)


@pytest.mark.asyncio
async def test_fulfillment_is_never_overridden() -> None:
    comm, bands = await _effective_fees(_Session("fulfillment", CALIB), "MLB1", SNAP)
    assert comm == Decimal("11.50")
    assert bands == SNAP.freight_bands


@pytest.mark.asyncio
async def test_flex_with_calibration_overrides_to_2band_split() -> None:
    comm, bands = await _effective_fees(_Session("xd_drop_off", CALIB), "MLB1", SNAP)
    assert comm == Decimal("16.50")
    assert bands == [
        {"min": 0, "max": 78.99, "cost": 6.75},
        {"min": 79, "max": None, "cost": 18.35},
    ]


@pytest.mark.asyncio
async def test_flex_without_calibration_keeps_snapshot() -> None:
    comm, bands = await _effective_fees(_Session("self_service", None), "MLB1", SNAP)
    assert comm == Decimal("11.50")
    assert bands == SNAP.freight_bands


@pytest.mark.asyncio
async def test_unknown_logistic_keeps_snapshot() -> None:
    comm, bands = await _effective_fees(_Session(None, CALIB), "MLB1", SNAP)
    assert comm == Decimal("11.50")
    assert bands == SNAP.freight_bands
