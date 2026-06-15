"""End-to-end coverage for `expire_disappeared_started`.

When a STARTED campaign ends, ML stops returning it in the eligible-promos
payload — but the `ignored/started` decision row recorded for it lingers and
keeps showing as "Inscritas" forever (the bug where May campaigns still
appeared in June). The cleanup expires exactly those rows: started + ignored,
for the MLB, whose promo_key is no longer in ML's current set. It must NOT
touch the still-active ones, the operator-owned (pending/approved) rows, or
non-started rows.

Driven with synthetic rows under a sentinel MLB id so it's deterministic and
self-cleaning.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.orm.models import MLPromoDecisionORM
from tiny_mirror.infrastructure.repositories.ml_promo_repository import (
    MLPromoDecisionRepository,
)

pytestmark = pytest.mark.e2e

SENTINEL_MLB = "MLBTEST-CLEAN"
OTHER_MLB = "MLBTEST-OTHER"


@pytest_asyncio.fixture
async def clean_sentinel(live_db: None) -> AsyncIterator[None]:
    await _drop()
    yield
    await _drop()


async def _drop() -> None:
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(MLPromoDecisionORM).where(
                MLPromoDecisionORM.mlb_id.in_([SENTINEL_MLB, OTHER_MLB])
            )
        )
        await session.commit()


async def _add(session, *, mlb: str, key: str, constraint: str | None, status: str) -> None:
    session.add(
        MLPromoDecisionORM(
            mlb_id=mlb,
            sku="SKU-CLEAN",
            promo_key=key,
            promo_type="SMART",
            decision_kind="already_active",
            constraint_used=constraint,
            reason="test",
            status=status,
        )
    )


async def _status(session, *, mlb: str, key: str) -> tuple[str, str | None]:
    row = (
        await session.execute(
            select(MLPromoDecisionORM).where(
                MLPromoDecisionORM.mlb_id == mlb,
                MLPromoDecisionORM.promo_key == key,
            )
        )
    ).scalar_one()
    return row.status, row.expired_reason


@pytest.mark.asyncio
async def test_expires_only_disappeared_started(clean_sentinel: None) -> None:
    async with AsyncSessionLocal() as session:
        await _add(session, mlb=SENTINEL_MLB, key="P-KEEP", constraint="started", status="ignored")
        await _add(session, mlb=SENTINEL_MLB, key="P-GONE", constraint="started", status="ignored")
        # Operator-owned + non-started controls that must never be touched.
        await _add(session, mlb=SENTINEL_MLB, key="P-PEND", constraint="started", status="pending")
        await _add(session, mlb=SENTINEL_MLB, key="P-CAP", constraint=None, status="ignored")
        # Different MLB, also "gone" — must not be touched by this MLB's sweep.
        await _add(session, mlb=OTHER_MLB, key="P-GONE", constraint="started", status="ignored")
        await session.commit()

        repo = MLPromoDecisionRepository(session)
        n = await repo.expire_disappeared_started(mlb_id=SENTINEL_MLB, seen_promo_keys={"P-KEEP"})
        await session.commit()

        assert n == 1
        assert await _status(session, mlb=SENTINEL_MLB, key="P-KEEP") == ("ignored", None)
        assert await _status(session, mlb=SENTINEL_MLB, key="P-GONE") == (
            "expired",
            "campaign_ended",
        )
        assert (await _status(session, mlb=SENTINEL_MLB, key="P-PEND"))[0] == "pending"
        assert (await _status(session, mlb=SENTINEL_MLB, key="P-CAP"))[0] == "ignored"
        assert (await _status(session, mlb=OTHER_MLB, key="P-GONE"))[0] == "ignored"


@pytest.mark.asyncio
async def test_insert_if_absent_refreshes_dates_on_conflict(clean_sentinel: None) -> None:
    """First insert returns the row (fresh); a re-insert with the same
    (mlb, promo_key) returns None (existing) BUT backfills the campaign dates,
    and a later null-date re-insert must not wipe them (COALESCE)."""
    from datetime import UTC, datetime
    from decimal import Decimal

    finish = datetime(2026, 7, 10, 3, 0, tzinfo=UTC)

    def _kwargs(**over: object) -> dict[str, object]:
        base: dict[str, object] = {
            "mlb_id": SENTINEL_MLB,
            "sku": "SKU-CLEAN",
            "promo_key": "P-DATES",
            "promo_id": "P-DATES",
            "promo_type": "DEAL",
            "promo_name": "x",
            "decision_kind": "already_active",
            "target_price": Decimal("10"),
            "target_total_pct": None,
            "target_seller_pct": None,
            "meli_percentage": None,
            "constraint_used": "started",
            "list_price": Decimal("20"),
            "cap_pct": Decimal("30"),
            "floor_price": None,
            "reason": "t",
            "status": "ignored",
        }
        base.update(over)
        return base

    async with AsyncSessionLocal() as session:
        repo = MLPromoDecisionRepository(session)

        # 1) fresh insert with no dates → returns the new row.
        first = await repo.insert_if_absent(**_kwargs())  # type: ignore[arg-type]
        assert first is not None

        # 2) conflict re-insert carrying a finish date → existing (None), but
        #    the date is backfilled.
        again = await repo.insert_if_absent(**_kwargs(promo_finish_date=finish))  # type: ignore[arg-type]
        assert again is None
        await session.commit()
        row = (
            await session.execute(
                select(MLPromoDecisionORM).where(
                    MLPromoDecisionORM.mlb_id == SENTINEL_MLB,
                    MLPromoDecisionORM.promo_key == "P-DATES",
                )
            )
        ).scalar_one()
        assert row.promo_finish_date == finish

        # 3) re-insert with null finish must NOT wipe the stored value.
        await repo.insert_if_absent(**_kwargs(promo_finish_date=None))  # type: ignore[arg-type]
        await session.commit()
        await session.refresh(row)
        assert row.promo_finish_date == finish


@pytest.mark.asyncio
async def test_empty_seen_set_expires_all_started_for_mlb(clean_sentinel: None) -> None:
    async with AsyncSessionLocal() as session:
        await _add(session, mlb=SENTINEL_MLB, key="P-A", constraint="started", status="ignored")
        await _add(session, mlb=SENTINEL_MLB, key="P-B", constraint="started", status="ignored")
        await session.commit()

        repo = MLPromoDecisionRepository(session)
        n = await repo.expire_disappeared_started(mlb_id=SENTINEL_MLB, seen_promo_keys=set())
        await session.commit()

        assert n == 2
        assert (await _status(session, mlb=SENTINEL_MLB, key="P-A"))[0] == "expired"
        assert (await _status(session, mlb=SENTINEL_MLB, key="P-B"))[0] == "expired"
