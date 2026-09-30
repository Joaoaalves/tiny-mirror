"""ML visits sync — daily visits per listing into ml_item_visits_daily.

``GET /items/{id}/visits/time_window?last=N&unit=day`` returns one bucket per
day for the last N days (today included, partial). One call per listing
covers the whole window, so the daily run re-reads the last few days (fixing
the partial ones) and the initial history reads 150 days, ML's maximum.
Read-only on ML (GET only).
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, date, datetime
from typing import Any

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLItemVisitsDailyORM, MLListingORM

logger = structlog.get_logger(__name__)

_API = "https://api.mercadolibre.com"
MAX_DAYS = 150  # ML rejects wider windows ("should be smaller or equal to 150 days")
# The visits endpoint rate-limits harder than the rest of the ML API: at 8
# concurrent calls (next to other ML jobs) 236 of 623 listings got 429 on
# 2026-09-30. Fewer workers + retry with backoff on 429.
_CONCURRENCY = 4
_MAX_ATTEMPTS = 5
_MAX_BACKOFF_SECONDS = 30.0


class MLVisitsSyncService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient) -> None:
        self._tok = token_service
        self._http = http_client

    async def _listing_ids(self) -> list[str]:
        """Every listing we mirror except closed ones (paused ones still get visits)."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(MLListingORM.mlb_id).where(
                    (MLListingORM.status.is_(None)) | (MLListingORM.status != "closed")
                )
            )
            return sorted({str(mlb) for (mlb,) in result.all()})

    async def _fetch(self, mlb_id: str, days: int) -> list[dict[str, Any]] | None:
        url = f"{_API}/items/{mlb_id}/visits/time_window"
        params: dict[str, str | int] = {"last": days, "unit": "day"}
        for attempt in range(_MAX_ATTEMPTS):
            try:
                access = await self._tok.get_valid_access_token()
                r = await self._http.get(
                    url, headers={"Authorization": f"Bearer {access}"}, params=params
                )
                if r.status_code == 401:
                    access = await self._tok.handle_unauthorized()
                    r = await self._http.get(
                        url, headers={"Authorization": f"Bearer {access}"}, params=params
                    )
            except httpx.HTTPError as exc:
                logger.warning("ml_visits.fetch_failed", mlb_id=mlb_id, error=str(exc))
                return None
            if r.status_code == 429 and attempt + 1 < _MAX_ATTEMPTS:
                await asyncio.sleep(_backoff(r, attempt))
                continue
            break
        if r.status_code != 200:
            logger.warning(
                "ml_visits.fetch_status", mlb_id=mlb_id, status=r.status_code, attempts=attempt + 1
            )
            return None
        return parse_time_window(mlb_id, r.json())

    async def sync(self, days: int = 3) -> dict[str, Any]:
        days = max(1, min(days, MAX_DAYS))
        mlbs = await self._listing_ids()
        sem = asyncio.Semaphore(_CONCURRENCY)

        async def one(mlb: str) -> list[dict[str, Any]] | None:
            async with sem:
                return await self._fetch(mlb, days)

        results = await asyncio.gather(*(one(m) for m in mlbs))
        rows = [row for res in results if res for row in res]
        failed = sum(1 for res in results if res is None)
        if rows:
            async with AsyncSessionLocal() as session:
                for i in range(0, len(rows), 1000):
                    chunk = rows[i : i + 1000]
                    stmt = pg_insert(MLItemVisitsDailyORM).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["mlb_id", "visit_date"],
                        set_={
                            "visits": stmt.excluded.visits,
                            "fetched_at": stmt.excluded.fetched_at,
                        },
                    )
                    await session.execute(stmt)
                await session.commit()
        stats = {
            "days": days,
            "listings": len(mlbs),
            "listings_failed": failed,
            "rows": len(rows),
            "visits": sum(r["visits"] for r in rows),
        }
        logger.info("ml_visits.sync_done", **stats)
        return stats


def _backoff(response: httpx.Response, attempt: int) -> float:
    """Seconds to wait after a 429: ``Retry-After`` when ML sends it, else
    exponential (1, 2, 4, 8s) with jitter; capped."""
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), _MAX_BACKOFF_SECONDS)
        except ValueError:
            pass
    return min(2.0**attempt + random.uniform(0, 0.5), _MAX_BACKOFF_SECONDS)


def parse_time_window(mlb_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Rows for ``ml_item_visits_daily`` from a time_window response."""
    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for bucket in payload.get("results") or []:
        raw_date = bucket.get("date")
        if not isinstance(raw_date, str) or len(raw_date) < 10:
            continue
        try:
            day = date.fromisoformat(raw_date[:10])
            visits = int(bucket.get("total") or 0)
        except (TypeError, ValueError):
            continue
        rows.append({"mlb_id": mlb_id[:20], "visit_date": day, "visits": visits, "fetched_at": now})
    return rows
