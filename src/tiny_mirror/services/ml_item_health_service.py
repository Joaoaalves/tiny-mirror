"""ML listing health — moderation/tags, quality and purchase experience.

Three independent parts land in ``ml_item_health`` (latest state per MLB),
each with its own ``*_fetched_at`` so a failed call keeps the previous value
of that part instead of blanking the row:

- item: ``/items?ids=...&attributes=id,status,sub_status,tags`` (20 per call)
  for every non-closed listing — moderation shows up as status
  ``under_review`` + sub_status (``forbidden``, ``waiting_for_patch``...)
- quality: ``/item/{id}/performance`` — score, level and the per-dimension
  variables still pending (e.g. free shipping, pictures)
- experience: ``/reputation/items/{id}/purchase_experience/integrators``
  (ML answers 302 to the user-product URL; redirects are followed)

Quality and experience are read for active/under_review listings only.
Read-only on ML (GET only).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLItemHealthORM, MLListingORM

logger = structlog.get_logger(__name__)

_API = "https://api.mercadolibre.com"
_MULTIGET = 20
_CONCURRENCY = 8
_DETAIL_STATUSES = {"active", "under_review"}


class MLItemHealthService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient) -> None:
        self._tok = token_service
        self._http = http_client

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response | None:
        try:
            access = await self._tok.get_valid_access_token()
            r = await self._http.get(
                f"{_API}{path}",
                headers={"Authorization": f"Bearer {access}"},
                params=params,
                follow_redirects=True,
            )
            if r.status_code == 401:
                access = await self._tok.handle_unauthorized()
                r = await self._http.get(
                    f"{_API}{path}",
                    headers={"Authorization": f"Bearer {access}"},
                    params=params,
                    follow_redirects=True,
                )
        except httpx.HTTPError as exc:
            logger.warning("ml_item_health.request_failed", path=path, error=str(exc))
            return None
        if r.status_code != 200:
            logger.info("ml_item_health.status", path=path, status=r.status_code)
            return None
        return r

    async def _listings(self) -> dict[str, str | None]:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(MLListingORM.mlb_id, MLListingORM.status).where(
                    (MLListingORM.status.is_(None)) | (MLListingORM.status != "closed")
                )
            )
            return {str(mlb): status for mlb, status in result.all()}

    async def sync(self) -> dict[str, Any]:
        listings = await self._listings()
        mlbs = sorted(listings)
        sem = asyncio.Semaphore(_CONCURRENCY)

        async def limited(coro: Any) -> Any:
            async with sem:
                return await coro

        # Part 1 — moderation state and tags, 20 listings per call.
        item_rows: list[dict[str, Any]] = []
        chunks = [mlbs[i : i + _MULTIGET] for i in range(0, len(mlbs), _MULTIGET)]
        for resp in await asyncio.gather(*(limited(self._multiget(c)) for c in chunks)):
            item_rows.extend(resp)
        # Moderation can change a listing's status since the last listings sync.
        live_status = {r["mlb_id"]: r["item_status"] for r in item_rows}
        detail = [
            m for m in mlbs if (live_status.get(m) or listings.get(m) or "") in _DETAIL_STATUSES
        ]

        # Parts 2 and 3 — per listing.
        quality = await asyncio.gather(*(limited(self._quality(m)) for m in detail))
        experience = await asyncio.gather(*(limited(self._experience(m)) for m in detail))
        quality_rows = [r for r in quality if r]
        experience_rows = [r for r in experience if r]

        async with AsyncSessionLocal() as session:
            for rows in (item_rows, quality_rows, experience_rows):
                for i in range(0, len(rows), 500):
                    chunk = rows[i : i + 500]
                    stmt = pg_insert(MLItemHealthORM).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["mlb_id"],
                        set_={c: stmt.excluded[c] for c in chunk[0] if c != "mlb_id"},
                    )
                    await session.execute(stmt)
            await session.commit()

        stats = {
            "listings": len(mlbs),
            "items": len(item_rows),
            "detail_listings": len(detail),
            "quality": len(quality_rows),
            "experience": len(experience_rows),
            "under_review": sum(1 for r in item_rows if r["item_status"] == "under_review"),
        }
        logger.info("ml_item_health.sync_done", **stats)
        return stats

    async def _multiget(self, mlbs: list[str]) -> list[dict[str, Any]]:
        r = await self._get(
            "/items", {"ids": ",".join(mlbs), "attributes": "id,status,sub_status,tags"}
        )
        if r is None:
            return []
        return [row for row in (parse_multiget_entry(e) for e in r.json() or []) if row]

    async def _quality(self, mlb: str) -> dict[str, Any] | None:
        r = await self._get(f"/item/{mlb}/performance")
        return parse_performance(mlb, r.json()) if r is not None else None

    async def _experience(self, mlb: str) -> dict[str, Any] | None:
        r = await self._get(
            f"/reputation/items/{mlb}/purchase_experience/integrators", {"locale": "pt_BR"}
        )
        return parse_experience(mlb, r.json()) if r is not None else None


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def parse_multiget_entry(entry: dict[str, Any]) -> dict[str, Any] | None:
    if entry.get("code") != 200:
        return None
    body = entry.get("body") or {}
    mlb = body.get("id")
    if not mlb:
        return None
    return {
        "mlb_id": str(mlb)[:20],
        "item_status": body.get("status"),
        "item_sub_status": body.get("sub_status") or [],
        "item_tags": body.get("tags") or [],
        "item_fetched_at": datetime.now(UTC),
    }


def parse_performance(mlb: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or payload.get("score") is None:
        return None
    buckets = payload.get("buckets") or []
    pending = [
        {
            "bucket": bucket.get("title"),
            "key": var.get("key"),
            "title": var.get("title"),
            "score": var.get("score"),
        }
        for bucket in buckets
        for var in bucket.get("variables") or []
        if var.get("status") not in (None, "COMPLETED")
    ]
    return {
        "mlb_id": mlb[:20],
        "quality_score": _dec(payload.get("score")),
        "quality_level": payload.get("level"),
        "quality_level_wording": payload.get("level_wording"),
        "quality_calculated_at": _ts(payload.get("calculated_at")),
        "quality_pending": pending,
        "quality_buckets": buckets,
        "quality_fetched_at": datetime.now(UTC),
    }


def parse_experience(mlb: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    reputation = payload.get("reputation") if isinstance(payload, dict) else None
    if not isinstance(reputation, dict):
        return None
    return {
        "mlb_id": mlb[:20],
        "experience_color": reputation.get("color"),
        "experience_text": reputation.get("text") or None,
        "experience_value": _dec(reputation.get("value")),
        "experience_status": (payload.get("status") or {}).get("id"),
        "experience_raw": payload,
        "experience_fetched_at": datetime.now(UTC),
    }


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
