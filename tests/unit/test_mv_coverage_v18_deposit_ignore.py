"""Unit coverage for the v18 mv_coverage migration (deposit-ignore immunity).

The views are Postgres-only SQL, so a unit test cannot execute them; the
live check lives in ``tests/e2e/test_e2e_mv_coverage_deposit_ignore.py``.
What we pin here is the *shape* that caused the 2026-09-11 incident: both
materialized views must never again filter ``stock_deposits`` with a bare
``WHERE NOT ignore`` before slicing the named deposits.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit

_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "alembic"
    / "versions"
    / "20260911_mv_coverage_v18_deposit_ignore.py"
)
_NAMED_DEPOSITS = ("Galpão", "Full Mercado Livre", "A Caminho")


@pytest.fixture(scope="module")
def migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("mv_coverage_v18", _MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stock_dep_where(sql: str) -> str:
    """Return the WHERE clause of the ``stock_dep`` CTE (between FROM and GROUP BY)."""
    match = re.search(r"FROM stock_deposits\s+(WHERE.*?)\s+GROUP BY product_tiny_id", sql, re.S)
    assert match is not None, "stock_dep CTE not found"
    return match.group(1)


def test_revision_chain(migration: ModuleType) -> None:
    assert migration.revision == "mv_coverage_v18_deposit_ignore"
    assert migration.down_revision == "panel_promo_id"


@pytest.mark.parametrize("attr", ["_CREATE_MV_COVERAGE", "_CREATE_MV_COVERAGE_FL_KITS"])
def test_named_deposits_bypass_ignore_flag(migration: ModuleType, attr: str) -> None:
    where = _stock_dep_where(getattr(migration, attr))
    assert where.startswith("WHERE NOT ignore")
    for name in _NAMED_DEPOSITS:
        assert f"OR deposit_name ILIKE '%{name}%'" in where, f"{attr}: {name} not exempt"


@pytest.mark.parametrize("attr", ["_CREATE_MV_COVERAGE", "_CREATE_MV_COVERAGE_FL_KITS"])
def test_no_bare_not_ignore_filter_left(migration: ModuleType, attr: str) -> None:
    sql = getattr(migration, attr)
    # The exact shape from v17 that dropped every flagged deposit row.
    assert re.search(r"WHERE NOT ignore\s+GROUP BY", sql) is None


@pytest.mark.parametrize("attr", ["_CREATE_MV_COVERAGE", "_CREATE_MV_COVERAGE_FL_KITS"])
def test_slices_still_keyed_by_deposit_name(migration: ModuleType, attr: str) -> None:
    sql = getattr(migration, attr)
    assert "ILIKE '%A Caminho%'\n        ), 0)::int AS stock_chegando" in sql
    assert "ILIKE '%Galpão%'\n        ), 0)::int AS stock_galpao" in sql
    assert "ILIKE '%Full Mercado Livre%'\n        ), 0)::int AS stock_full_ml" in sql
