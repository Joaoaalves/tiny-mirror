"""Unit coverage for the ``products_sku_index`` migration.

The index build itself needs Postgres (covered by
``tests/e2e/test_e2e_products_sku_index.py``). Here we pin the contract that
makes it safe in production: CONCURRENTLY, executed inside alembic's
autocommit block (never inside the migration transaction), chained after the
current head, and an INVALID leftover from a failed build is dropped first.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

pytestmark = pytest.mark.unit

_MIGRATION = (
    Path(__file__).resolve().parents[2] / "alembic" / "versions" / "20261125_products_sku_index.py"
)


@pytest.fixture(scope="module")
def migration() -> ModuleType:
    spec = importlib.util.spec_from_file_location("products_sku_index", _MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeResult:
    def __init__(self, row: Any) -> None:
        self._row = row

    def first(self) -> Any:
        return self._row


class _FakeOp:
    """Records each statement with whether it ran inside the autocommit block."""

    def __init__(self, invalid_leftover: bool) -> None:
        self.in_block = False
        self.executed: list[tuple[str, bool]] = []
        self._invalid = invalid_leftover

    def get_context(self) -> _FakeOp:
        return self

    @contextmanager
    def autocommit_block(self) -> Iterator[None]:
        self.in_block = True
        try:
            yield
        finally:
            self.in_block = False

    def get_bind(self) -> _FakeOp:
        return self

    def execute(self, stmt: Any, params: Any = None) -> _FakeResult:
        sql = str(stmt)
        if "pg_index" in sql:
            assert params == {"name": "ix_products_sku"}
            return _FakeResult((1,) if self._invalid else None)
        self.executed.append((sql, self.in_block))
        return _FakeResult(None)


def test_chains_after_mv_coverage_v19(migration: ModuleType) -> None:
    assert migration.revision == "products_sku_index"
    assert migration.down_revision == "mv_coverage_v19_slim"


def test_sql_is_concurrent_and_full_index(migration: ModuleType) -> None:
    assert migration.CREATE_SQL == (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_products_sku ON products (sku)"
    )
    # Full index: no partial predicate (the partial uq_products_sku already exists).
    assert "WHERE" not in migration.CREATE_SQL.upper()
    assert migration.DROP_SQL == "DROP INDEX CONCURRENTLY IF EXISTS ix_products_sku"


@pytest.mark.parametrize("invalid_leftover", [False, True])
def test_upgrade_runs_inside_autocommit_block(
    migration: ModuleType, monkeypatch: pytest.MonkeyPatch, invalid_leftover: bool
) -> None:
    fake = _FakeOp(invalid_leftover)
    monkeypatch.setattr(migration, "op", fake)
    migration.upgrade()
    expected = [migration.CREATE_SQL]
    if invalid_leftover:
        expected = [migration.DROP_SQL, migration.CREATE_SQL]
    assert [sql for sql, _ in fake.executed] == expected
    assert all(in_block for _, in_block in fake.executed)


def test_downgrade_drops_concurrently(
    migration: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeOp(invalid_leftover=False)
    monkeypatch.setattr(migration, "op", fake)
    migration.downgrade()
    assert fake.executed == [(migration.DROP_SQL, True)]
