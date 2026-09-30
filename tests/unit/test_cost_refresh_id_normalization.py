"""Unit tests for the spreadsheet MLB-id normalization in cost_refresh_service.

Backport of a prod hotfix (2026-09-13): cells of the MERCADO LIVRE tab may
repeat one listing ("MLB123 / 123") or name two ads of the same SKU
("MLB1 / MLB2"). The first normalizes to the single id; the second resolves
only through an operator-verified alias file (ML_COST_ID_ALIASES_PATH), kept
in deployment configuration, never in source.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tiny_mirror.services.cost_refresh_service import (
    CostRefreshError,
    _load_cost_id_aliases,
    _normalize_cost_ids,
)

pytestmark = pytest.mark.unit

A = "MLB4078501557"
B = "MLB4078501558"


def _alias(ids: list[str], target: str, sku: str = "KIT-X") -> dict[str, Any]:
    return {"ids": ids, "target": target, "sku": sku}


# ---------------------------------------------------------------------------
# _normalize_cost_ids
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw",
    [
        f"{A} / 4078501557",
        "4078501557",
        " mlb4078501557 ",
        f"{A}/{A}",
    ],
)
def test_single_listing_cells_normalize_to_the_id(raw: str) -> None:
    items, changed = _normalize_cost_ids({raw: {"sku": "S", "mlbId": raw}}, aliases=[])

    assert list(items) == [A]
    assert items[A]["mlbId"] == A
    assert changed == (raw != A)


def test_clean_ids_are_untouched() -> None:
    row = {"sku": "S"}
    items, changed = _normalize_cost_ids({A: row}, aliases=[])

    assert items == {A: row}
    assert changed == 0


def test_multi_ad_cell_without_alias_stays_invalid() -> None:
    raw = f"{A} / {B}"
    items, changed = _normalize_cost_ids({raw: {"sku": "KIT-X"}}, aliases=[])

    assert list(items) == [raw]
    assert changed == 0


def test_multi_ad_cell_resolves_through_verified_alias() -> None:
    raw = f"{A} / {B}"
    items, changed = _normalize_cost_ids(
        {raw: {"sku": "KIT-X", "mlbId": raw}}, aliases=[_alias([A, B], target=B)]
    )

    assert list(items) == [B]
    assert items[B]["mlbId"] == B
    assert changed == 1


def test_alias_is_ignored_when_the_row_sku_differs() -> None:
    raw = f"{A} / {B}"
    items, _ = _normalize_cost_ids(
        {raw: {"sku": "OTHER"}}, aliases=[_alias([A, B], target=B, sku="KIT-X")]
    )

    assert list(items) == [raw]


def test_conflicting_rows_for_the_same_id_abort_before_writes() -> None:
    with pytest.raises(CostRefreshError, match="Conflicting spreadsheet rows"):
        _normalize_cost_ids(
            {A: {"sku": "S", "baseCost": 10}, "4078501557": {"sku": "S", "baseCost": 11}},
            aliases=[],
        )


def test_identical_duplicate_rows_are_merged() -> None:
    items, changed = _normalize_cost_ids(
        {A: {"sku": "S", "baseCost": 10}, "4078501557": {"sku": "S", "baseCost": 10}},
        aliases=[],
    )

    assert items == {A: {"sku": "S", "baseCost": 10}}
    assert changed == 1


# ---------------------------------------------------------------------------
# _load_cost_id_aliases
# ---------------------------------------------------------------------------
def _write(tmp_path: Path, content: Any) -> str:
    path = tmp_path / "aliases.json"
    path.write_text(content if isinstance(content, str) else json.dumps(content))
    return str(path)


def test_no_alias_env_means_no_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ML_COST_ID_ALIASES_PATH", raising=False)

    assert _load_cost_id_aliases() == []


def test_valid_alias_file_loads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    aliases = [_alias([A, B], target=B)]
    monkeypatch.setenv("ML_COST_ID_ALIASES_PATH", _write(tmp_path, aliases))

    assert _load_cost_id_aliases() == aliases


@pytest.mark.parametrize(
    "content",
    [
        "{not json",
        {"ids": [A, B]},
        [{"ids": [A], "target": A, "sku": "S"}],
        [{"ids": [A, "MLB12"], "target": A, "sku": "S"}],
        [{"ids": [A, B], "target": "MLB999999999", "sku": "S"}],
        [{"ids": [A, B], "target": A, "sku": ""}],
        [_alias([A, B], target=A), _alias([B, A], target=B)],
    ],
    ids=[
        "invalid-json",
        "not-a-list",
        "single-id",
        "malformed-id",
        "target-outside-ids",
        "empty-sku",
        "duplicate-alias",
    ],
)
def test_invalid_alias_file_aborts_refresh(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, content: Any
) -> None:
    monkeypatch.setenv("ML_COST_ID_ALIASES_PATH", _write(tmp_path, content))

    with pytest.raises(CostRefreshError, match="Invalid cost ID alias configuration"):
        _load_cost_id_aliases()


def test_missing_alias_file_aborts_refresh(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ML_COST_ID_ALIASES_PATH", str(tmp_path / "missing.json"))

    with pytest.raises(CostRefreshError):
        _load_cost_id_aliases()
