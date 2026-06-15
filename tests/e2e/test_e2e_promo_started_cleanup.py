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
from sqlalchemy import delete

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
    from sqlalchemy import select

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
