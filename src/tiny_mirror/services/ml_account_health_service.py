"""ML account health — reputation snapshot + infractions mirror.

- ``/users/{id}`` → ``seller_reputation`` (level, medal, transactions,
  ratings, metrics), stored once per BRT day in ``ml_seller_reputation_daily``.
- ``/moderations/infractions/{user_id}`` (newest first, max 20 per page) →
  ``ml_infractions``. Pages are read until one holds only known ids, so the
  first run loads the whole history and later runs stop after a page or two.

Read-only on ML (GET only).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLInfractionORM, MLSellerReputationDailyORM

logger = structlog.get_logger(__name__)

_API = "https://api.mercadolibre.com"
_INFRACTIONS_PAGE = 20  # ML: "limit max value is 20"
_MAX_INFRACTION_PAGES = 200
_BRT = ZoneInfo("America/Sao_Paulo")


class MLAccountHealthService:
    def __init__(self, token_service: Any, http_client: httpx.AsyncClient, ml_user_id: str) -> None:
        self._tok = token_service
        self._http = http_client
        self._uid = ml_user_id

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        access = await self._tok.get_valid_access_token()
        r = await self._http.get(
            f"{_API}{path}", headers={"Authorization": f"Bearer {access}"}, params=params
        )
        if r.status_code == 401:
            access = await self._tok.handle_unauthorized()
            r = await self._http.get(
                f"{_API}{path}", headers={"Authorization": f"Bearer {access}"}, params=params
            )
        return r

    async def sync(self) -> dict[str, Any]:
        reputation = await self.sync_reputation()
        infractions = await self.sync_infractions()
        stats = {"reputation": reputation, **infractions}
        logger.info("ml_account_health.sync_done", **stats)
        return stats

    async def sync_reputation(self) -> bool:
        r = await self._get(f"/users/{self._uid}")
        r.raise_for_status()
        row = parse_reputation(r.json(), datetime.now(_BRT).date())
        if row is None:
            logger.warning("ml_account_health.no_seller_reputation")
            return False
        async with AsyncSessionLocal() as session:
            stmt = pg_insert(MLSellerReputationDailyORM).values(row)
            stmt = stmt.on_conflict_do_update(
                index_elements=["snapshot_date"],
                set_={c: stmt.excluded[c] for c in row if c != "snapshot_date"},
            )
            await session.execute(stmt)
            await session.commit()
        return True

    async def sync_infractions(self) -> dict[str, int]:
        async with AsyncSessionLocal() as session:
            known = {
                str(i) for (i,) in (await session.execute(select(MLInfractionORM.infraction_id)))
            }
        rows: list[dict[str, Any]] = []
        total = 0
        for page in range(_MAX_INFRACTION_PAGES):
            r = await self._get(
                f"/moderations/infractions/{self._uid}",
                {"limit": _INFRACTIONS_PAGE, "offset": page * _INFRACTIONS_PAGE},
            )
            r.raise_for_status()
            data = r.json()
            batch = [row for row in map(parse_infraction, data.get("infractions") or []) if row]
            total = int((data.get("paging") or {}).get("total") or 0)
            new = [row for row in batch if row["infraction_id"] not in known]
            rows.extend(new)
            if not batch or not new or (page + 1) * _INFRACTIONS_PAGE >= total:
                break
        if rows:
            async with AsyncSessionLocal() as session:
                for i in range(0, len(rows), 500):
                    stmt = pg_insert(MLInfractionORM).values(rows[i : i + 500])
                    stmt = stmt.on_conflict_do_nothing(index_elements=["infraction_id"])
                    await session.execute(stmt)
                await session.commit()
        return {"infractions_new": len(rows), "infractions_total_ml": total}


# ---------------------------------------------------------------------------
# Parsing (pure)
# ---------------------------------------------------------------------------
def parse_reputation(user: dict[str, Any], snapshot_date: date) -> dict[str, Any] | None:
    rep = user.get("seller_reputation")
    if not isinstance(rep, dict) or not rep:
        return None
    tx = rep.get("transactions") or {}
    ratings = tx.get("ratings") or {}
    return {
        "snapshot_date": snapshot_date,
        "level_id": rep.get("level_id"),
        "power_seller_status": rep.get("power_seller_status"),
        "transactions_total": _int(tx.get("total")),
        "transactions_completed": _int(tx.get("completed")),
        "transactions_canceled": _int(tx.get("canceled")),
        "rating_positive": _dec(ratings.get("positive")),
        "rating_neutral": _dec(ratings.get("neutral")),
        "rating_negative": _dec(ratings.get("negative")),
        "metrics": rep.get("metrics") if isinstance(rep.get("metrics"), dict) else None,
        "raw": rep,
        "fetched_at": datetime.now(UTC),
    }


def parse_infraction(raw: dict[str, Any]) -> dict[str, Any] | None:
    infraction_id = raw.get("id")
    if infraction_id in (None, ""):
        return None
    return {
        "infraction_id": str(infraction_id)[:40],
        "date_created": _ts(raw.get("date_created")),
        "element_id": raw.get("element_id") or None,
        "element_type": (raw.get("element_type") or None),
        "filter_subgroup": (raw.get("filter_subgroup") or None),
        "reason": raw.get("reason") or None,
        "remedy": raw.get("remedy") or None,
        "related_item_id": (
            str(raw["related_item_id"])[:40] if raw.get("related_item_id") else None
        ),
        "raw": raw,
        "synced_at": datetime.now(UTC),
    }


def _int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _ts(value: Any) -> datetime | None:
    """ML mixes ``-04:00`` and ``-0400`` offsets; accept both."""
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
