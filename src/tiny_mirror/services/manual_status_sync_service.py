"""Manual SKU status sync.

Pulls the operator's manual classification of SKUs from a Google Apps
Script Web App (see ``gas/manual_status/``) and writes it into
``products.manual_status``. The GAS endpoint reads cell background
colors on the GERAL tab of the Controle 4.0 spreadsheet (columns B and
C) and returns ``queima`` / ``analise`` / ``normal`` per SKU.

Run as a daily scheduler job. The downstream queima / reposição / FL
crons can then ``WHERE manual_status IS NULL OR manual_status =
'normal'`` to skip SKUs the operator already marked.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Final

import httpx
import structlog
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

VALID_STATUSES: Final[frozenset[str]] = frozenset({"queima", "analise", "normal"})


class ManualStatusSyncError(Exception):
    """Raised when the GAS endpoint returns an unrecoverable error."""


class ManualStatusSyncService:
    """Fetches manual SKU classifications and upserts onto ``products``.

    Stateless aside from the injected ``httpx.AsyncClient``. Safe to
    instantiate per call; the scheduler job creates a fresh one each
    run because the cadence is daily.
    """

    def __init__(
        self,
        http: httpx.AsyncClient,
        gas_url: str,
        gas_token: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._http = http
        self._url = gas_url
        self._token = gas_token
        self._timeout = timeout_seconds

    async def fetch(self) -> dict[str, str]:
        """Call the GAS endpoint and return a ``{sku: status}`` mapping.

        Raises ``ManualStatusSyncError`` if the endpoint is unreachable,
        returns non-JSON, or signals an error in the payload.
        """
        if not self._url or not self._token:
            raise ManualStatusSyncError("GAS URL/token not configured")
        try:
            resp = await self._http.get(
                self._url,
                params={"token": self._token},
                timeout=self._timeout,
                follow_redirects=True,
            )
        except httpx.RequestError as exc:
            raise ManualStatusSyncError(f"GAS request failed: {exc}") from exc
        if resp.status_code >= 400:
            raise ManualStatusSyncError(f"GAS returned HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            raise ManualStatusSyncError(f"GAS returned non-JSON: {exc}") from exc
        if not isinstance(body, dict) or "error" in body:
            raise ManualStatusSyncError(
                f"GAS payload error: {body.get('error') if isinstance(body, dict) else body!r}"
            )
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

        Returns a stats dict with per-status counts and the unmatched
        SKU count (rows in the GAS response that do not exist in the
        products table).
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

        # Pre-bucket by status to issue one UPDATE per bucket.
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

        # Clear any product that was previously marked but no longer present
        # in the GAS payload — operator un-coloured the cell. We keep the
        # cleared-by-default semantics ("missing == normal") but explicit so
        # the audit trail (manual_status_synced_at) reflects the sync.
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
