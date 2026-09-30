"""Shared persistence for mp_orders / mp_listings (Amazon, Shopee)."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MPListingORM


async def upsert(model: Any, rows: list[dict[str, Any]], keys: list[str]) -> None:
    if not rows:
        return
    async with AsyncSessionLocal() as session:
        for i in range(0, len(rows), 500):
            chunk = rows[i : i + 500]
            stmt = pg_insert(model).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=keys,
                set_={c: stmt.excluded[c] for c in chunk[0] if c not in keys},
            )
            await session.execute(stmt)
        await session.commit()


async def prune_listings(channel: str, seen: set[tuple[str, str]]) -> int:
    """Drop the channel's listings that a complete pass no longer returned."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            delete(MPListingORM).where(
                MPListingORM.channel == channel,
                tuple_(MPListingORM.listing_id, MPListingORM.variation_id).not_in(list(seen)),
            )
        )
        await session.commit()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]
