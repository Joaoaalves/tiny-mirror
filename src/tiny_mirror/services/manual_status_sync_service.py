"""Manual SKU status sync.

Pulls the operator's manual SKU classification from the unified Controle
4.0 GAS Web App (action=manual_status) and writes it onto
``products.manual_status``. The GAS endpoint reads cell background colors
on the GERAL tab (columns B/C) and returns ``queima`` / ``analise`` /
``normal`` per SKU.

Runs as a daily scheduler job. Downstream queima / reposição / FL crons
then filter ``WHERE manual_status IS NULL OR manual_status = 'normal'``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final

import structlog
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.services.gas_client import GASClient, GASClientError

logger = structlog.get_logger(__name__)

VALID_STATUSES: Final[frozenset[str]] = frozenset({"queima", "analise", "normal"})


class ManualStatusSyncError(Exception):
    """Raised when the GAS endpoint returns an unrecoverable error."""


class ManualStatusSyncService:
    """Fetches manual SKU classifications and upserts onto ``products``.

    Stateless aside from the injected ``GASClient``. Safe to instantiate
    per call.
    """

    def __init__(self, gas: GASClient) -> None:
        self._gas = gas

    async def fetch(self) -> dict[str, str]:
        """Call the GAS endpoint and return a ``{sku: status}`` mapping.

        Raises ``ManualStatusSyncError`` if the endpoint is unreachable
        or returns an error payload.
        """
        try:
            body = await self._gas.manual_status()
        except GASClientError as exc:
            raise ManualStatusSyncError(str(exc)) from exc

        skus_obj = body.get("skus")
        if not isinstance(skus_obj, dict):
            raise ManualStatusSyncError("GAS payload missing 'skus' object")

        out: dict[str, str] = {}
        for sku, row in skus_obj.items():
            if not isinstance(row, dict):
                continue
            status = row.get("status")
            if status in VALID_STATUSES:
                out[str(sku).strip()] = str(status)
        logger.info(
            "manual_status_fetch_ok",
            total_skus=len(out),
            counts=body.get("counts"),
            generated_at=body.get("generatedAt"),
        )
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
