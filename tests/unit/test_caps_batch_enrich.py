"""Unit coverage for the batch caps enrichment and ``GET /caps/summary``.

``_enrich_cap_from`` is pure (all lookups come from the pre-loaded
``_CapEnrichment``), so it is tested with plain namespaces. The loader is
driven with a fake session to pin the fixed query budget: N caps cost 7
queries, not ~8 per cap. The live old-vs-new equivalence lives in
``tests/e2e/test_e2e_caps_batch_enrich.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from tiny_mirror.api.routers import ml_promotions as mp
from tiny_mirror.api.routers.ml_promotions import (
    _CapEnrichment,
    _enrich_cap_from,
    _enrich_caps,
    _load_cap_enrichment,
    caps_summary,
)
from tiny_mirror.infrastructure.orm.models import (
    MLCatalogStatusORM,
    MLCostsSnapshotORM,
    MLFlexFeeCalibrationORM,
    MLListingORM,
)

pytestmark = pytest.mark.unit

_BANDS = [{"min": 0, "max": None, "cost": 20.0}]


def _cap(mlb: str = "MLB1", sku: str = "SKU1", floor: str | None = "100") -> SimpleNamespace:
    return SimpleNamespace(
        mlb_id=mlb,
        sku=sku,
        max_seller_share_pct=Decimal("30"),
        margin_floor_price=Decimal(floor) if floor is not None else None,
        auto_apply=True,
        has_active_promo=False,
        active_promo_price=None,
        freight_band_opt=False,
        skip_when_winning=False,
        excluded_promo_types=[],
        notes=None,
        updated_by="op",
        updated_at=datetime(2026, 11, 25, tzinfo=UTC),
        manual_status=None,
    )


def _snap(base_cost: str | None = "40") -> SimpleNamespace:
    return SimpleNamespace(
        list_price=Decimal("150"),
        base_cost=Decimal(base_cost) if base_cost is not None else None,
        sheet_promo_price=Decimal("120"),
        commission_pct=Decimal("16"),
        freight_bands=_BANDS,
    )


def _listing(status: str = "active", logistic_type: str | None = "fulfillment") -> SimpleNamespace:
    return SimpleNamespace(
        logistic_type=logistic_type,
        status=status,
        available_quantity=7,
        title="Produto",
        thumbnail="http://img",
        permalink="http://ml",
        price=Decimal("150"),
        linked_mlb_ids=["MLB9"],
    )


def test_full_row_is_enriched_from_maps() -> None:
    data = _CapEnrichment(
        snapshots={"MLB1": _snap()},
        listings={"MLB1": _listing()},
        catalog={
            "MLB1": SimpleNamespace(
                catalog_listing=1,
                status="winning",
                visit_share="maximum",
                current_price=Decimal("140"),
                price_to_win=None,
                winner_price=Decimal("139"),
                competitors_sharing_first_place=2,
            )
        },
        manual_status={"SKU1": "queima"},
        stock={"SKU1": {"full_in_transfer": 3, "pending_full": 5, "warehouse_available": 11}},
    )
    out = _enrich_cap_from(data, _cap())
    assert out.base_cost == Decimal("40")
    assert out.commission_pct == Decimal("16")
    assert out.freight_bands == _BANDS  # fulfillment, no calibration → snapshot as-is
    assert out.commission_bands is None
    assert out.manual_status == "queima"
    assert out.has_active_listing is True
    assert out.linked_mlb_ids == ["MLB9"]
    assert out.full_available == 7 + 3
    assert out.full_in_transit == 5
    assert out.warehouse_available == 11
    assert out.catalog_listing is True and out.catalog_status == "winning"
    assert out.margin_at_floor_value is not None and out.margin_at_floor_pct is not None


def test_missing_rows_leave_defaults() -> None:
    out = _enrich_cap_from(_CapEnrichment(), _cap(floor=None))
    assert out.has_active_listing is False
    assert out.full_available == 0
    assert out.warehouse_available is None and out.full_in_transit is None
    assert out.base_cost is None and out.commission_pct is None
    assert out.margin_at_floor_value is None
    assert out.catalog_status is None and out.manual_status is None


def test_tiny_cost_only_fills_a_missing_sheet_cost() -> None:
    data = _CapEnrichment(
        snapshots={"MLB1": _snap(base_cost=None), "MLB2": _snap(base_cost="40")},
        tiny_cost={"SKU1": Decimal("33.5"), "SKU2": Decimal("99")},
    )
    assert _enrich_cap_from(data, _cap("MLB1", "SKU1")).base_cost == Decimal("33.5")
    assert _enrich_cap_from(data, _cap("MLB2", "SKU2")).base_cost == Decimal("40")


def test_flex_calibration_overrides_fees() -> None:
    calib = SimpleNamespace(
        commission_bands=[{"min": 0, "max": None, "pct": 12}],
        real_comm_pct=Decimal("13"),
        freight_bands=[{"min": 0, "max": None, "cost": 9.0}],
    )
    data = _CapEnrichment(
        snapshots={"MLB1": _snap()},
        listings={"MLB1": _listing(logistic_type="cross_docking")},
        calibrations={"MLB1": calib},
    )
    out = _enrich_cap_from(data, _cap())
    assert out.commission_pct == Decimal("13")
    assert out.freight_bands == [{"min": 0, "max": None, "cost": 9.0, "payback": 0.0}]
    assert out.commission_bands == calib.commission_bands


def test_inactive_listing_is_orphan() -> None:
    out = _enrich_cap_from(_CapEnrichment(listings={"MLB1": _listing("paused")}), _cap())
    assert out.has_active_listing is False
    assert out.listing_status == "paused"


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def scalars(self) -> list[Any]:
        return self._rows

    def all(self) -> list[Any]:
        return self._rows

    def mappings(self) -> _Result:
        return self

    def one(self) -> Any:
        return self._rows[0]


class _FakeSession:
    """Serves the loader's 7 statements; records every execute."""

    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def execute(self, stmt: Any, params: Any = None) -> _Result:
        self.calls.append(stmt)
        if hasattr(stmt, "column_descriptions"):
            entity = stmt.column_descriptions[0]["entity"]
            mlbs = ["MLB1", "MLB2", "MLB3"]
            if entity is MLListingORM:
                return _Result([SimpleNamespace(mlb_id=m, **vars(_listing())) for m in mlbs])
            if entity is MLCostsSnapshotORM:
                return _Result([SimpleNamespace(mlb_id=m, **vars(_snap())) for m in mlbs])
            if entity in (MLFlexFeeCalibrationORM, MLCatalogStatusORM):
                return _Result([])
            raise AssertionError(f"unexpected entity {entity}")
        sql = str(stmt)
        assert params is not None
        if "manual_status" in sql:
            assert "DISTINCT ON (sku)" in sql and "ORDER BY sku, ctid" in sql
            return _Result([(s, "normal") for s in params["skus"]])
        if "cost_price" in sql:
            assert "ORDER BY sku, ctid" in sql
            return _Result([])
        if "stock_deposits" in sql:
            return _Result(
                [
                    {"sku": s, "full_in_transfer": 1, "pending_full": 0, "warehouse_available": 2}
                    for s in params["skus"]
                ]
            )
        raise AssertionError(f"unexpected SQL {sql}")


@pytest.mark.asyncio
async def test_loader_uses_fixed_query_budget() -> None:
    session = _FakeSession()
    caps = [_cap("MLB1", "SKU1"), _cap("MLB2", "SKU1"), _cap("MLB3", "SKU2")]
    out = await _enrich_caps(session, caps)  # type: ignore[arg-type]
    assert len(session.calls) == 7
    assert [c.mlb_id for c in out] == ["MLB1", "MLB2", "MLB3"]
    assert all(c.manual_status == "normal" and c.full_available == 7 + 1 for c in out)


@pytest.mark.asyncio
async def test_loader_skips_everything_for_no_caps() -> None:
    session = _FakeSession()
    data = await _load_cap_enrichment(session, [])  # type: ignore[arg-type]
    assert session.calls == []
    assert data.listings == {} and data.stock == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("include_orphans", [False, True])
async def test_caps_summary_counts(include_orphans: bool) -> None:
    class _S:
        params: Any = None

        async def execute(self, stmt: Any, params: Any = None) -> _Result:
            _S.params = params
            assert "count(*) FILTER (WHERE c.auto_apply)" in str(stmt)
            return _Result([{"total": 736, "auto_apply": 12}])

    out = await caps_summary(include_orphans=include_orphans, session=_S())  # type: ignore[arg-type]
    assert out.model_dump() == {"total": 736, "auto_apply": 12}
    assert _S.params == {"include_orphans": include_orphans}


def test_summary_route_is_declared_before_the_mlb_id_route() -> None:
    paths = [getattr(r, "path", "") for r in mp.router.routes]
    prefix = mp.router.prefix
    summary = paths.index(f"{prefix}/caps/summary")
    by_id = paths.index(f"{prefix}/caps/{{mlb_id}}")
    assert summary < by_id
