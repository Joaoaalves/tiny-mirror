"""ML claims sync — reclamações/mediações/devoluções into ml_claims.

``/post-purchase/v1/claims/search`` needs at least one filter, so the sync
walks ``status=opened`` and ``status=closed`` separately, newest
``last_updated`` first (100 per page), and stops once a page holds only
claims already stored with the same ``last_updated``. The first run loads
the whole history; later runs read a page or two per status.

Unknown ``reason_id``s are resolved once through
``/post-purchase/v1/claims/reasons/{id}`` into ``ml_claim_reasons``.
Read-only on ML (GET only).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLClaimORM, MLClaimReasonORM

logger = structlog.get_logger(__name__)

_API = "https://api.mercadolibre.com/post-purchase/v1/claims"
_PAGE = 100
_MAX_PAGES = 200
_STATUSES = ("opened", "closed")


class MLClaimsSyncService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient) -> None:
        self._tok = token_service
        self._http = http_client

    async def _get(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        access = await self._tok.get_valid_access_token()
        r = await self._http.get(url, headers={"Authorization": f"Bearer {access}"}, params=params)
        if r.status_code == 401:
            access = await self._tok.handle_unauthorized()
            r = await self._http.get(
                url, headers={"Authorization": f"Bearer {access}"}, params=params
            )
        return r

    async def sync(self) -> dict[str, Any]:
        async with AsyncSessionLocal() as session:
            known = {
                int(cid): upd
                for cid, upd in (
                    await session.execute(select(MLClaimORM.claim_id, MLClaimORM.last_updated))
                ).all()
            }
            known_reasons = {
                str(r) for (r,) in await session.execute(select(MLClaimReasonORM.reason_id))
            }

        rows: dict[int, dict[str, Any]] = {}
        for status in _STATUSES:
            for page in range(_MAX_PAGES):
                r = await self._get(
                    f"{_API}/search",
                    {
                        "status": status,
                        "sort": "last_updated:desc",
                        "limit": _PAGE,
                        "offset": page * _PAGE,
                    },
                )
                r.raise_for_status()
                data = r.json()
                batch = [row for row in map(parse_claim, data.get("data") or []) if row]
                changed = [
                    row
                    for row in batch
                    if row["claim_id"] not in known or known[row["claim_id"]] != row["last_updated"]
                ]
                rows.update({row["claim_id"]: row for row in changed})
                total = int((data.get("paging") or {}).get("total") or 0)
                if not batch or not changed or (page + 1) * _PAGE >= total:
                    break

        reasons = await self._resolve_reasons(
            {row["reason_id"] for row in rows.values() if row["reason_id"]} - known_reasons
        )
        async with AsyncSessionLocal() as session:
            claim_rows = list(rows.values())
            for i in range(0, len(claim_rows), 500):
                chunk = claim_rows[i : i + 500]
                stmt = pg_insert(MLClaimORM).values(chunk)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["claim_id"],
                    set_={c: stmt.excluded[c] for c in chunk[0] if c != "claim_id"},
                )
                await session.execute(stmt)
            if reasons:
                stmt = pg_insert(MLClaimReasonORM).values(reasons)
                stmt = stmt.on_conflict_do_nothing(index_elements=["reason_id"])
                await session.execute(stmt)
            await session.commit()

        stats = {
            "claims_upserted": len(rows),
            "opened": sum(1 for r in rows.values() if r["status"] == "opened"),
            "reasons_new": len(reasons),
        }
        logger.info("ml_claims.sync_done", **stats)
        return stats

    async def _resolve_reasons(self, reason_ids: set[str]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for reason_id in sorted(reason_ids):
            try:
                r = await self._get(f"{_API}/reasons/{reason_id}")
            except httpx.HTTPError as exc:
                logger.warning("ml_claims.reason_failed", reason_id=reason_id, error=str(exc))
                continue
            if r.status_code != 200:
                logger.warning("ml_claims.reason_status", reason_id=reason_id, status=r.status_code)
                continue
            out.append(parse_reason(reason_id, r.json()))
        return out


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def parse_claim(raw: dict[str, Any]) -> dict[str, Any] | None:
    claim_id = _int(raw.get("id"))
    if claim_id is None:
        return None
    resolution = raw.get("resolution")
    return {
        "claim_id": claim_id,
        "type": raw.get("type"),
        "stage": raw.get("stage"),
        "status": raw.get("status"),
        "reason_id": raw.get("reason_id"),
        "resource": raw.get("resource"),
        "resource_id": _int(raw.get("resource_id")),
        "parent_id": _int(raw.get("parent_id")),
        "fulfilled": raw.get("fulfilled") if isinstance(raw.get("fulfilled"), bool) else None,
        "quantity_type": raw.get("quantity_type"),
        "date_created": _ts(raw.get("date_created")),
        "last_updated": _ts(raw.get("last_updated")),
        "resolution": resolution if isinstance(resolution, dict) and resolution else None,
        "raw": raw,
        "synced_at": datetime.now(UTC),
    }


def parse_reason(reason_id: str, raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "reason_id": reason_id[:30],
        "name": (str(raw["name"])[:120] if raw.get("name") else None),
        "detail": raw.get("detail") or None,
        "flow": (str(raw["flow"])[:60] if raw.get("flow") else None),
        "fetched_at": datetime.now(UTC),
    }


def _int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    s = value.replace("Z", "+00:00")
    if len(s) >= 5 and s[-5] in "+-" and s[-4:].isdigit():
        s = f"{s[:-2]}:{s[-2:]}"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
