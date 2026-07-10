"""Manual SKU status sync.

Pulls the operator's manual SKU classification from the Controle 4.0
spreadsheet (``GERAL`` tab, read via the Sheets API — see
``sheets_manual_status.py``) and writes it onto ``products.manual_status``.

Historically this came from an Apps Script Web App (``?action=manual_status``)
that read cell background colours. That script lived inside the spreadsheet,
unversioned, and broke silently when a column was inserted — so the source
moved into this repo.

Runs as a daily scheduler job. Downstream queima / reposição / FL crons
then filter ``WHERE manual_status IS NULL OR manual_status = 'normal'``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final, Protocol

import structlog
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.services.sheets_manual_status import SheetsManualStatusError

logger = structlog.get_logger(__name__)

VALID_STATUSES: Final[frozenset[str]] = frozenset({"queima", "analise", "normal"})


class ManualStatusSyncError(Exception):
    """Raised when the status source is unreachable or returns nothing usable."""


class ManualStatusSource(Protocol):
    """Anything that can produce ``{sku: manual_status}`` (see SheetsManualStatusFetcher)."""

    async def fetch_statuses(self) -> dict[str, str]: ...


class ManualStatusSyncService:
    """Fetches manual SKU classifications and upserts onto ``products``.

    Stateless aside from the injected source. Safe to instantiate per call.
    """

    def __init__(self, source: ManualStatusSource) -> None:
        self._source = source

    async def fetch(self) -> dict[str, str]:
        """Read the source and return a validated ``{sku: status}`` mapping.

        Raises ``ManualStatusSyncError`` if the source fails or yields nothing —
        never returns an empty dict, because ``apply()`` would then reset every
        SKU to ``'normal'``.
        """
        try:
            raw = await self._source.fetch_statuses()
        except SheetsManualStatusError as exc:
            raise ManualStatusSyncError(str(exc)) from exc

        out = {
            str(sku).strip(): status
            for sku, status in raw.items()
            if status in VALID_STATUSES and str(sku).strip()
        }
        if not out:
            raise ManualStatusSyncError("status source returned no valid SKUs")
        counts = {v: list(out.values()).count(v) for v in VALID_STATUSES}
        logger.info("manual_status_fetch_ok", total_skus=len(out), counts=counts)
        return out

    async def apply(
        self,
        session: AsyncSession,
        statuses: dict[str, str],
    ) -> dict[str, int]:
        """Write the fetched statuses to ``products``.

        Strategy: bulk UPDATE per status bucket (one statement per value
        in {queima, analise, normal}). Cheaper than per-row upserts and
        matches the scheduler-job pattern in the codebase.

        SKUs missing from the payload but present on ``products`` are
        explicitly reset to ``'normal'`` — once the operator clears a
        cell, the next sync must un-flag the SKU.
        """
        if not statuses:
            logger.warning("manual_status_apply_empty")
            return {
                "queima": 0,
                "analise": 0,
                "normal": 0,
                "cleared": 0,
                "unmatched_in_db": 0,
            }

        buckets: dict[str, list[str]] = {"queima": [], "analise": [], "normal": []}
        for sku, status in statuses.items():
            buckets[status].append(sku)

        now = datetime.now(UTC)
        stats: dict[str, int] = {"queima": 0, "analise": 0, "normal": 0, "cleared": 0}
        matched_skus: set[str] = set()

        for status, skus in buckets.items():
            if not skus:
                continue
            stmt = text(
                """
                UPDATE products
                   SET manual_status = :status,
                       manual_status_synced_at = :synced_at
                 WHERE sku = ANY(:skus)
                RETURNING sku
                """
            ).bindparams(bindparam("skus", expanding=False))
            result = await session.execute(
                stmt,
                {"status": status, "synced_at": now, "skus": skus},
            )
            updated = [row[0] for row in result.fetchall()]
            stats[status] = len(updated)
            matched_skus.update(updated)

        clear_stmt = text(
            """
            UPDATE products
               SET manual_status = 'normal',
                   manual_status_synced_at = :synced_at
             WHERE NOT (sku = ANY(:matched))
               AND manual_status IS DISTINCT FROM 'normal'
            RETURNING sku
            """
        )
        cleared = await session.execute(
            clear_stmt,
            {"synced_at": now, "matched": list(matched_skus)},
        )
        stats["cleared"] = len(cleared.fetchall())
        stats["unmatched_in_db"] = len(statuses) - len(matched_skus)

        await session.commit()
        logger.info("manual_status_apply_ok", **stats)
        return stats

    async def run(self, session: AsyncSession) -> dict[str, Any]:
        """One-shot orchestration: fetch + apply, returning combined stats."""
        statuses = await self.fetch()
        stats = await self.apply(session, statuses)
        return {"fetched": len(statuses), **stats}
