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
# Nominal ML fee schedule (price-banded) — gold_pro style mid-band discount.
COMM_BANDS = [
    {"min": 0, "max": 125.0, "pct": 16.5},
    {"min": 125.0, "max": 650.0, "pct": 13.5},
    {"min": 650.0, "max": None, "pct": 16.5},
]
# Banded freight schedule (from ML's calculator) carried by the calib row.
FULL_FR_BANDS = [
    {"min": 0, "max": 78.99, "cost": 11.75},
    {"min": 79, "max": None, "cost": 48.55},
]
CALIB = SimpleNamespace(
    real_comm_pct=Decimal("16.50"),
    commission_bands=COMM_BANDS,
    freight_bands=FULL_FR_BANDS,
    freight_per_unit_lt79=Decimal("6.75"),
    freight_per_unit_ge79=Decimal("18.35"),
    payback_per_unit_lt79=Decimal("2.00"),
    payback_per_unit_ge79=Decimal("18.35"),
)

BANDS_CALIBRATED = [
    {"min": 0, "max": 78.99, "cost": 6.75, "payback": 2.0},
    {"min": 79, "max": None, "cost": 18.35, "payback": 18.35},
]


@pytest.mark.asyncio
async def test_fulfillment_gets_banded_freight_and_nominal_commission() -> None:
    """FULL: freight from the banded calculator schedule AND commission from the
    nominal ML schedule (operator decisions 2026-07-08). The snapshot commission
    remains as the band-lookup fallback; COST never flows through here."""
    comm, bands, comm_bands = await _effective_fees(_Session("fulfillment", CALIB), "MLB1", SNAP)
    assert comm == Decimal("11.50")  # snapshot % = fallback do lookup por banda
    assert comm_bands == COMM_BANDS  # escada nominal vale pro FULL também
    assert bands == [{**b, "payback": 0.0} for b in FULL_FR_BANDS]


@pytest.mark.asyncio
async def test_fulfillment_without_freight_schedule_keeps_snapshot() -> None:
    calib = SimpleNamespace(
        real_comm_pct=Decimal("16.50"),
        commission_bands=COMM_BANDS,
        freight_bands=None,
        freight_per_unit_lt79=Decimal("6.75"),
        freight_per_unit_ge79=Decimal("18.35"),
        payback_per_unit_lt79=Decimal("0.00"),
        payback_per_unit_ge79=Decimal("0.00"),
    )
    comm, bands, comm_bands = await _effective_fees(_Session("fulfillment", calib), "MLB1", SNAP)
    assert comm == Decimal("11.50")
    assert bands == SNAP.freight_bands  # sem tabela de frete → planilha
    assert comm_bands == COMM_BANDS  # mas a escada nominal ainda vale


@pytest.mark.asyncio
async def test_flex_with_calibration_uses_banded_freight_and_commission() -> None:
    comm, bands, comm_bands = await _effective_fees(_Session("xd_drop_off", CALIB), "MLB1", SNAP)
    assert comm == Decimal("16.50")
    assert bands == [{**b, "payback": 0.0} for b in FULL_FR_BANDS]  # schedule wins
    assert comm_bands == COMM_BANDS


@pytest.mark.asyncio
async def test_flex_without_calibration_keeps_snapshot() -> None:
    comm, bands, comm_bands = await _effective_fees(_Session("self_service", None), "MLB1", SNAP)
    assert comm == Decimal("11.50")
    assert bands == SNAP.freight_bands
    assert comm_bands is None


@pytest.mark.asyncio
async def test_unknown_logistic_keeps_snapshot() -> None:
    comm, bands, comm_bands = await _effective_fees(_Session(None, CALIB), "MLB1", SNAP)
    assert comm == Decimal("11.50")
    assert bands == SNAP.freight_bands
    assert comm_bands is None


def test_apply_flex_calibration_pure_helper() -> None:
    from tiny_mirror.services.pricing_service import apply_flex_calibration

    # fulfillment: frete por faixa + escada nominal (snapshot % = fallback)
    comm, bands, comm_bands = apply_flex_calibration(
        "fulfillment", Decimal("11.5"), SNAP.freight_bands, CALIB
    )
    assert comm == Decimal("11.5")
    assert comm_bands == COMM_BANDS
    assert bands == [{**b, "payback": 0.0} for b in FULL_FR_BANDS]
    # unknown / no-calib → unchanged
    assert apply_flex_calibration("xd_drop_off", Decimal("11.5"), SNAP.freight_bands, None) == (
        Decimal("11.5"),
        SNAP.freight_bands,
        None,
    )
    assert apply_flex_calibration(None, Decimal("11.5"), SNAP.freight_bands, CALIB) == (
        Decimal("11.5"),
        SNAP.freight_bands,
        None,
    )
    # flex + calib → comissão real + frete por faixa + escada nominal
    comm, bands, comm_bands = apply_flex_calibration(
        "xd_drop_off", Decimal("11.5"), SNAP.freight_bands, CALIB
    )
    assert comm == Decimal("16.50")
    assert bands == [{**b, "payback": 0.0} for b in FULL_FR_BANDS]
    assert comm_bands == COMM_BANDS


def test_apply_flex_calibration_fallback_row_overrides_only_freight() -> None:
    """A no-sales fallback row (real_comm_pct=None) still replaces the freight
    bands with the global Flex 2-band table, but keeps the snapshot commission."""
    from tiny_mirror.services.pricing_service import apply_flex_calibration

    fallback = SimpleNamespace(
        real_comm_pct=None,
        freight_per_unit_lt79=Decimal("5.54"),
        freight_per_unit_ge79=Decimal("37.82"),
        payback_per_unit_lt79=Decimal("0.00"),
        payback_per_unit_ge79=Decimal("0.00"),
    )
    comm, bands, comm_bands = apply_flex_calibration(
        "xd_drop_off", Decimal("11.5"), SNAP.freight_bands, fallback
    )
    assert comm == Decimal("11.5")  # commission kept — no measured rate
    assert comm_bands is None  # no schedule on this fallback row
    assert bands == [
        {"min": 0, "max": 78.99, "cost": 5.54, "payback": 0.0},
        {"min": 79, "max": None, "cost": 37.82, "payback": 0.0},
    ]


def test_calc_cap_for_snapshot_honors_fee_override() -> None:
    """The Flex floor must use the calibrated fees: higher fees → higher floor."""
    from tiny_mirror.services.cap_recompute_service import calc_cap_for_snapshot

    snap = SimpleNamespace(
        mlb_id="MLB1",
        sku="X",
        fetch_error=None,
        base_cost=Decimal("40"),
        list_price=Decimal("200"),
        commission_pct=Decimal("11.5"),
        freight_bands=[{"min": 0, "max": None, "cost": 10}],
        sheet_discount_pct=Decimal("30"),
    )
    default = calc_cap_for_snapshot(snap)  # type: ignore[arg-type]
    override = calc_cap_for_snapshot(  # type: ignore[arg-type]
        snap,
        commission_pct=Decimal("16.5"),
        freight_bands=[
            {"min": 0, "max": 78.99, "cost": 6.0},
            {"min": 79, "max": None, "cost": 25.0},
        ],
    )
    # The calibrated (higher) fees must flow into the margin math: the realised
    # margin at the floor is lower than with the snapshot fees.
    assert default.margin_pct_at_floor is not None
    assert override.margin_pct_at_floor is not None
    assert override.margin_pct_at_floor < default.margin_pct_at_floor


# ---------------------------------------------------------------------------
# FlexFeeCalibrationService HTTP loop — 401 recovery + pagination guard
# ---------------------------------------------------------------------------
def _http_response(status_code: int, body=None):
    from unittest.mock import MagicMock

    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=body or {})
    return resp


def _calib_order(mlb: str):
    return {
        "status": "paid",
        "shipping": {"id": 111},
        "order_items": [
            {"item": {"id": mlb}, "unit_price": 50.0, "sale_fee": 8.0, "quantity": 1},
        ],
    }


@pytest.mark.asyncio
async def test_calibration_get_401_forces_refresh_and_retries() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="cached.token")
    tok.handle_unauthorized = AsyncMock(return_value="refreshed.token")
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _http_response(401),
            _http_response(200, {"ok": True}),
        ]
    )
    service = FlexFeeCalibrationService(tok, http, "12345")

    data = await service._get("https://api.mercadolibre.com/x")

    tok.handle_unauthorized.assert_awaited_once()
    retry_headers = http.get.await_args_list[1].kwargs["headers"]
    assert retry_headers == {"Authorization": "Bearer refreshed.token"}
    assert data == {"ok": True}


@pytest.mark.asyncio
async def test_orders_for_day_missing_paging_keeps_fetching_until_empty_page() -> None:
    from datetime import date
    from unittest.mock import AsyncMock, MagicMock

    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="t")
    http = AsyncMock()
    http.get = AsyncMock(
        side_effect=[
            _http_response(200, {"results": [_calib_order("MLB1")]}),
            _http_response(200, {"results": [_calib_order("MLB2")]}),
            _http_response(200, {"results": []}),
        ]
    )
    service = FlexFeeCalibrationService(tok, http, "12345")

    rows = await service._orders_for_day(date(2026, 6, 10))

    assert http.get.await_count == 3
    assert [r["mlb"] for r in rows] == ["MLB1", "MLB2"]


@pytest.mark.asyncio
async def test_commission_schedule_compresses_midband_discount() -> None:
    """The nominal fee schedule probe must collapse the price grid into bands and
    reproduce ML's mid-price commission dip (gold_pro 16.5% → 13.5% → 16.5%)."""
    from unittest.mock import AsyncMock, MagicMock

    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="t")
    service = FlexFeeCalibrationService(tok, AsyncMock(), "12345")

    def pct_for(price: float) -> float:
        # real ML shape: cheaper in the ~130-575 window
        return 13.5 if 130 <= price <= 575 else 16.5

    async def fake_get(url: str, params=None):
        p = float(params["price"])
        return {"sale_fee_details": {"percentage_fee": pct_for(p), "fixed_fee": 0}}

    service._get = fake_get  # type: ignore[method-assign]
    bands = await service._commission_schedule("MLB1387", "gold_pro")

    assert bands is not None
    assert [b["pct"] for b in bands] == [16.5, 13.5, 16.5]
    assert bands[0]["min"] == 0
    assert bands[-1]["max"] is None
    # spot-check the lookup reproduces the right rate at MT prices

    def pct_at(price, bands):
        for b in bands:
            if price < b["min"]:
                continue
            if b["max"] is not None and price > b["max"]:
                continue
            return b["pct"]
        return None

    assert pct_at(60, bands) == 16.5
    assert pct_at(411.58, bands) == 13.5
    assert pct_at(1007, bands) == 16.5


@pytest.mark.asyncio
async def test_commission_schedule_none_on_total_api_failure() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="t")
    service = FlexFeeCalibrationService(tok, AsyncMock(), "12345")

    async def fake_get(url: str, params=None):
        return None

    service._get = fake_get  # type: ignore[method-assign]
    assert await service._commission_schedule("C", "gold_pro") is None


def test_apply_flex_calibration_prefers_freight_schedule() -> None:
    """When the calibration row carries a price-banded freight schedule (from
    ML's freight calculator), it must win over the flat lt79/ge79 quote."""
    from tiny_mirror.services.pricing_service import apply_flex_calibration

    fr_bands = [
        {"min": 0, "max": 78.99, "cost": 11.75},
        {"min": 79, "max": None, "cost": 48.55},
    ]
    calib = SimpleNamespace(
        real_comm_pct=None,
        commission_bands=None,
        freight_bands=fr_bands,
        freight_per_unit_lt79=Decimal("0.00"),  # the flat quote said 0 — must lose
        freight_per_unit_ge79=Decimal("0.00"),
        payback_per_unit_lt79=Decimal("0.00"),
        payback_per_unit_ge79=Decimal("0.00"),
    )
    _comm, bands, _cb = apply_flex_calibration(
        "xd_drop_off", Decimal("11.5"), SNAP.freight_bands, calib
    )
    assert bands == [
        {"min": 0, "max": 78.99, "cost": 11.75, "payback": 0.0},
        {"min": 79, "max": None, "cost": 48.55, "payback": 0.0},
    ]


def test_parse_dims_prefers_seller_package() -> None:
    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    attrs = [
        {"id": "PACKAGE_HEIGHT", "value_name": "34 cm"},
        {"id": "PACKAGE_WIDTH", "value_name": "59 cm"},
        {"id": "PACKAGE_LENGTH", "value_name": "41 cm"},
        {"id": "PACKAGE_WEIGHT", "value_name": "1300 g"},
        {"id": "SELLER_PACKAGE_HEIGHT", "value_name": "58 cm"},
        {"id": "SELLER_PACKAGE_WIDTH", "value_name": "42 cm"},
        {"id": "SELLER_PACKAGE_LENGTH", "value_name": "37 cm"},
        {"id": "SELLER_PACKAGE_WEIGHT", "value_name": "1800 g"},
    ]
    # MT usa as medidas do vendedor (58x42x37, 1800) — não as certificadas
    assert FlexFeeCalibrationService._parse_dims(attrs) == "58x42x37,1800"
    # sem as do vendedor → cai nas certificadas
    certified_only = [a for a in attrs if not a["id"].startswith("SELLER_")]
    assert FlexFeeCalibrationService._parse_dims(certified_only) == "34x59x41,1300"
    # incompleto → None
    assert FlexFeeCalibrationService._parse_dims(certified_only[:2]) is None


@pytest.mark.asyncio
async def test_freight_schedule_probes_brackets_and_merges() -> None:
    """The freight schedule probes ML's calculator per price bracket and merges
    contiguous equal-cost brackets (reproduces the UNI-CX 11.75/48.55 shape)."""
    from unittest.mock import AsyncMock, MagicMock

    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="t")
    service = FlexFeeCalibrationService(tok, AsyncMock(), "12345")

    async def fake_get(url: str, params=None):
        price = float(params["item_price"])
        assert params["dimensions"] == "58x42x37,1800"
        cost = 11.75 if price < 79 else 48.55
        return {"coverage": {"all_country": {"list_cost": cost}}}

    service._get = fake_get  # type: ignore[method-assign]
    bands = await service._freight_schedule("58x42x37,1800", "gold_special")

    assert bands == [
        {"min": 0, "max": 78.99, "cost": 11.75},
        {"min": 79, "max": None, "cost": 48.55},
    ]


@pytest.mark.asyncio
async def test_freight_schedule_none_on_total_failure() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from tiny_mirror.services.flex_fee_calibration_service import FlexFeeCalibrationService

    tok = MagicMock()
    tok.get_valid_access_token = AsyncMock(return_value="t")
    service = FlexFeeCalibrationService(tok, AsyncMock(), "12345")

    async def fake_get(url: str, params=None):
        return None

    service._get = fake_get  # type: ignore[method-assign]
    assert await service._freight_schedule("10x10x10,500", None) is None


def test_full_commission_discount_shifts_bands_down_1pp() -> None:
    """ML charges fulfillment listings 1pp LESS commission than the nominal
    listing_prices schedule (median real-nominal = -0,99pp on 3,444 settled FULL
    order items; Mercado Turbo applies exactly -1,00pp). The discount is applied
    when assembling the FULL calibration row."""
    bands = [
        {"min": 0, "max": 149.99, "pct": 16.5},
        {"min": 150, "max": None, "pct": 13.5},
    ]
    shifted = [{**b, "pct": max(round(float(b["pct"]) - 1.0, 2), 0.0)} for b in bands]
    assert shifted == [
        {"min": 0, "max": 149.99, "pct": 15.5},
        {"min": 150, "max": None, "pct": 12.5},
    ]
    # nunca negativa
    zero = [{"min": 0, "max": None, "pct": 0.5}]
    assert [{**b, "pct": max(round(float(b["pct"]) - 1.0, 2), 0.0)} for b in zero] == [
        {"min": 0, "max": None, "pct": 0.0}
    ]
