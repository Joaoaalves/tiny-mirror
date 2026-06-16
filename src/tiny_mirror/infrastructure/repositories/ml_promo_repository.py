"""Repositories for ML promotion automation tables.

Three logical surfaces:
- MLPromoCapRepository — user-set caps per SKU (CRUD)
- MLCostsSnapshotRepository — cached cost data from Google Apps Script
- MLPromoActionRepository — audit log writer/reader
- MLPromoAlertRepository — anomalies inbox
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, literal_column, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.infrastructure.orm.models import (
    MLCostsSnapshotORM,
    MLPromoActionORM,
    MLPromoAlertORM,
    MLPromoCapORM,
    MLPromoDecisionORM,
    MLPromoResubscribeJobORM,
)


# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------
class MLPromoCapRepository:
    """Per-MLB cap CRUD (re-keyed from sku to mlb_id on 2026-05-21).

    SKU lookups still exist (drawer needs the full set of MLBs for a
    SKU), but the canonical lookup key is mlb_id.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, mlb_id: str) -> MLPromoCapORM | None:
        result = await self._session.execute(
            select(MLPromoCapORM).where(MLPromoCapORM.mlb_id == mlb_id)
        )
        return result.scalar_one_or_none()

    async def get_by_sku(self, sku: str) -> list[MLPromoCapORM]:
        """All caps belonging to a SKU. Used by the drawer to render the
        SKU-level summary alongside its per-MLB editable rows."""
        result = await self._session.execute(
            select(MLPromoCapORM).where(MLPromoCapORM.sku == sku).order_by(MLPromoCapORM.mlb_id)
        )
        return list(result.scalars().all())

    async def list_all(
        self,
        *,
        only_auto: bool | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> tuple[list[MLPromoCapORM], int]:
        q = select(MLPromoCapORM)
        count_q = select(func.count(MLPromoCapORM.mlb_id))
        if only_auto is not None:
            q = q.where(MLPromoCapORM.auto_apply == only_auto)
            count_q = count_q.where(MLPromoCapORM.auto_apply == only_auto)
        # Order by sku first so consumers that group by SKU read in groups.
        q = q.order_by(MLPromoCapORM.sku, MLPromoCapORM.mlb_id).limit(limit).offset(offset)
        rows = list((await self._session.execute(q)).scalars().all())
        total = int((await self._session.execute(count_q)).scalar_one())
        return rows, total

    async def upsert(
        self,
        mlb_id: str,
        *,
        sku: str,
        max_seller_share_pct: Decimal,
        margin_floor_price: Decimal | None = None,
        auto_apply: bool | None = None,
        has_active_promo: bool | None = None,
        active_promo_price: Decimal | None = None,
        freight_band_opt: bool | None = None,
        skip_when_winning: bool | None = None,
        excluded_promo_types: list[str] | None = None,
        notes: str | None = None,
        updated_by: str | None = None,
    ) -> MLPromoCapORM:
        """Insert or update a cap row. None-valued kwargs preserve current value on update.

        `sku` is required so we always know the grouping key, even for new rows
        introduced after a listing first becomes active.
        """
        update_set: dict[str, Any] = {
            "max_seller_share_pct": max_seller_share_pct,
            "sku": sku,
            "updated_at": func.now(),
        }
        insert_values: dict[str, Any] = {
            "mlb_id": mlb_id,
            "sku": sku,
            "max_seller_share_pct": max_seller_share_pct,
        }
        for k, v in (
            ("margin_floor_price", margin_floor_price),
            ("auto_apply", auto_apply),
            ("has_active_promo", has_active_promo),
            ("active_promo_price", active_promo_price),
            ("freight_band_opt", freight_band_opt),
            ("skip_when_winning", skip_when_winning),
            ("excluded_promo_types", excluded_promo_types),
            ("notes", notes),
            ("updated_by", updated_by),
        ):
            if v is not None:
                update_set[k] = v
                insert_values[k] = v

        stmt = (
            pg_insert(MLPromoCapORM)
            .values(**insert_values)
            .on_conflict_do_update(index_elements=["mlb_id"], set_=update_set)
            .returning(MLPromoCapORM)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.scalar_one()

    async def delete(self, mlb_id: str) -> bool:
        existing = await self.get(mlb_id)
        if existing is None:
            return False
        await self._session.delete(existing)
        await self._session.flush()
        return True


# ---------------------------------------------------------------------------
# Costs snapshot
# ---------------------------------------------------------------------------
class MLCostsSnapshotRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, mlb_id: str) -> MLCostsSnapshotORM | None:
        result = await self._session.execute(
            select(MLCostsSnapshotORM).where(MLCostsSnapshotORM.mlb_id == mlb_id)
        )
        return result.scalar_one_or_none()

    async def get_by_sku(self, sku: str) -> list[MLCostsSnapshotORM]:
        result = await self._session.execute(
            select(MLCostsSnapshotORM)
            .where(MLCostsSnapshotORM.sku == sku)
            .order_by(MLCostsSnapshotORM.fetched_at.desc())
        )
        return list(result.scalars().all())

    async def upsert(
        self,
        mlb_id: str,
        *,
        sku: str,
        active_on_sheet: bool,
        base_cost: Decimal | None,
        commission_pct: Decimal | None,
        commission_label: str | None,
        list_price: Decimal | None,
        sheet_promo_price: Decimal | None,
        sheet_discount_pct: Decimal | None,
        sheet_margin_pct: Decimal | None,
        sheet_margin_value: Decimal | None,
        freight_bands: Any | None,
        fetch_error: str | None = None,
    ) -> MLCostsSnapshotORM:
        values: dict[str, Any] = {
            "mlb_id": mlb_id,
            "sku": sku,
            "active_on_sheet": active_on_sheet,
            "base_cost": base_cost,
            "commission_pct": commission_pct,
            "commission_label": commission_label,
            "list_price": list_price,
            "sheet_promo_price": sheet_promo_price,
            "sheet_discount_pct": sheet_discount_pct,
            "sheet_margin_pct": sheet_margin_pct,
            "sheet_margin_value": sheet_margin_value,
            "freight_bands": freight_bands,
            "fetch_error": fetch_error,
            "fetched_at": func.now(),
        }
        stmt = (
            pg_insert(MLCostsSnapshotORM)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["mlb_id"],
                set_={k: v for k, v in values.items() if k != "mlb_id"},
            )
            .returning(MLCostsSnapshotORM)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return result.scalar_one()


# ---------------------------------------------------------------------------
# Actions audit log
# ---------------------------------------------------------------------------
class MLPromoActionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def log(
        self,
        *,
        sku: str,
        mlb_id: str,
        action: str,
        promo_type: str | None = None,
        promo_id: str | None = None,
        price_before: Decimal | None = None,
        price_after: Decimal | None = None,
        total_pct: Decimal | None = None,
        seller_pct: Decimal | None = None,
        meli_pct: Decimal | None = None,
        reason: str | None = None,
        ml_response: Any | None = None,
        dry_run: bool = False,
        decided_by: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> MLPromoActionORM:
        row = MLPromoActionORM(
            sku=sku,
            mlb_id=mlb_id,
            action=action,
            promo_type=promo_type,
            promo_id=promo_id,
            price_before=price_before,
            price_after=price_after,
            total_pct=total_pct,
            seller_pct=seller_pct,
            meli_pct=meli_pct,
            reason=reason,
            ml_response=ml_response,
            dry_run=dry_run,
            decided_by=decided_by,
            context=context,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def list_all(
        self,
        *,
        sku: str | None = None,
        action: str | None = None,
        since: datetime | None = None,
        include_dry_run: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[MLPromoActionORM], int]:
        q = select(MLPromoActionORM)
        count_q = select(func.count(MLPromoActionORM.id))
        if sku is not None:
            q = q.where(MLPromoActionORM.sku == sku)
            count_q = count_q.where(MLPromoActionORM.sku == sku)
        if action is not None:
            q = q.where(MLPromoActionORM.action == action)
            count_q = count_q.where(MLPromoActionORM.action == action)
        if since is not None:
            q = q.where(MLPromoActionORM.at >= since)
            count_q = count_q.where(MLPromoActionORM.at >= since)
        if not include_dry_run:
            q = q.where(MLPromoActionORM.dry_run.is_(False))
            count_q = count_q.where(MLPromoActionORM.dry_run.is_(False))
        q = q.order_by(MLPromoActionORM.at.desc()).limit(limit).offset(offset)
        rows = list((await self._session.execute(q)).scalars().all())
        total = int((await self._session.execute(count_q)).scalar_one())
        return rows, total


# ---------------------------------------------------------------------------
# Alerts inbox
# ---------------------------------------------------------------------------
class MLPromoAlertRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        sku: str,
        mlb_id: str,
        kind: str,
        message: str,
        data: Any | None = None,
    ) -> MLPromoAlertORM:
        row = MLPromoAlertORM(
            sku=sku,
            mlb_id=mlb_id,
            kind=kind,
            message=message,
            data=data,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def list_all(
        self,
        *,
        acknowledged: bool | None = False,
        kind: str | None = None,
        sku: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[MLPromoAlertORM], int]:
        q = select(MLPromoAlertORM)
        count_q = select(func.count(MLPromoAlertORM.id))
        if acknowledged is not None:
            q = q.where(MLPromoAlertORM.acknowledged.is_(acknowledged))
            count_q = count_q.where(MLPromoAlertORM.acknowledged.is_(acknowledged))
        if kind is not None:
            q = q.where(MLPromoAlertORM.kind == kind)
            count_q = count_q.where(MLPromoAlertORM.kind == kind)
        if sku is not None:
            q = q.where(MLPromoAlertORM.sku == sku)
            count_q = count_q.where(MLPromoAlertORM.sku == sku)
        q = q.order_by(MLPromoAlertORM.at.desc()).limit(limit).offset(offset)
        rows = list((await self._session.execute(q)).scalars().all())
        total = int((await self._session.execute(count_q)).scalar_one())
        return rows, total

    async def acknowledge(self, alert_id: int, by: str | None = None) -> bool:
        row = await self._session.get(MLPromoAlertORM, alert_id)
        if row is None or row.acknowledged:
            return False
        row.acknowledged = True
        row.acknowledged_by = by
        row.acknowledged_at = datetime.now(UTC)
        await self._session.flush()
        return True


# ---------------------------------------------------------------------------
# Decisions (operator approval queue)
# ---------------------------------------------------------------------------
class MLPromoDecisionRepository:
    """CRUD for ``ml_promo_decisions``.

    Generation is idempotent: ``insert_if_absent`` uses the unique
    ``(mlb_id, promo_key)`` constraint so re-running the cron does not
    duplicate rows. A decision that was previously approved or rejected
    by the operator stays in that terminal state — the cron does not
    re-prompt for it.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(
        self,
        *,
        mlb_id: str,
        sku: str,
        promo_key: str,
        promo_id: str | None,
        promo_type: str,
        promo_name: str | None,
        decision_kind: str,
        target_price: Decimal | None,
        target_total_pct: Decimal | None,
        target_seller_pct: Decimal | None,
        meli_percentage: Decimal | None,
        constraint_used: str | None,
        list_price: Decimal | None,
        cap_pct: Decimal | None,
        floor_price: Decimal | None,
        reason: str,
        status: str = "pending",
        promo_finish_date: Any | None = None,
        promo_start_date: Any | None = None,
        min_price: Decimal | None = None,
        max_price: Decimal | None = None,
        stock_min: int | None = None,
        stock_max: int | None = None,
    ) -> MLPromoDecisionORM | None:
        """Insert a decision; return the new row, or None if a row with the
        same ``(mlb_id, promo_key)`` already exists.

        ``status`` defaults to ``pending`` (operator must approve/reject).
        Denied / already-active engine outputs should pass ``ignored`` —
        they end up in the table for visibility but don't pollute the
        operator's pending queue.
        """
        ins = pg_insert(MLPromoDecisionORM).values(
            mlb_id=mlb_id,
            sku=sku,
            promo_key=promo_key,
            promo_id=promo_id,
            promo_type=promo_type,
            promo_name=promo_name,
            decision_kind=decision_kind,
            target_price=target_price,
            target_total_pct=target_total_pct,
            target_seller_pct=target_seller_pct,
            meli_percentage=meli_percentage,
            constraint_used=constraint_used,
            list_price=list_price,
            cap_pct=cap_pct,
            floor_price=floor_price,
            reason=reason,
            status=status,
            promo_finish_date=promo_finish_date,
            promo_start_date=promo_start_date,
            min_price=min_price,
            max_price=max_price,
            stock_min=stock_min,
            stock_max=stock_max,
        )
        # On conflict we DON'T touch the operator/engine state (status,
        # target, etc.) — the row is idempotent. We only REFRESH the campaign
        # validity dates so existing 'started' rows (first seen before these
        # columns existed) get backfilled and any extended campaign updates.
        # COALESCE(new, current) keeps the stored value when ML omits it.
        stmt: Any = (
            ins.on_conflict_do_update(
                constraint="uq_ml_promo_decisions_mlb_promo",
                set_={
                    "promo_finish_date": func.coalesce(
                        ins.excluded.promo_finish_date,
                        MLPromoDecisionORM.promo_finish_date,
                    ),
                    "promo_start_date": func.coalesce(
                        ins.excluded.promo_start_date,
                        MLPromoDecisionORM.promo_start_date,
                    ),
                },
            )
            # xmax = 0 on a fresh INSERT, non-zero when the row already
            # existed (UPDATE path) — lets the caller keep its insert-vs-skip
            # stats unchanged: a refreshed existing row still counts as "skip".
            .returning(MLPromoDecisionORM, literal_column("xmax = 0").label("was_insert"))
        )
        row = (await self._session.execute(stmt)).first()
        await self._session.flush()
        if row is None:
            return None
        orm_obj, was_insert = row[0], row[1]
        return orm_obj if was_insert else None

    async def list_(
        self,
        *,
        status: str | None = None,
        sku: str | None = None,
        constraint_used: str | None = None,
        exclude_promo_types: list[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> tuple[list[MLPromoDecisionORM], int]:
        q = select(MLPromoDecisionORM)
        count_q = select(func.count(MLPromoDecisionORM.id))
        if status is not None:
            q = q.where(MLPromoDecisionORM.status == status)
            count_q = count_q.where(MLPromoDecisionORM.status == status)
        if sku is not None:
            q = q.where(MLPromoDecisionORM.sku == sku)
            count_q = count_q.where(MLPromoDecisionORM.sku == sku)
        if constraint_used is not None:
            q = q.where(MLPromoDecisionORM.constraint_used == constraint_used)
            count_q = count_q.where(MLPromoDecisionORM.constraint_used == constraint_used)
        if exclude_promo_types:
            q = q.where(MLPromoDecisionORM.promo_type.notin_(exclude_promo_types))
            count_q = count_q.where(MLPromoDecisionORM.promo_type.notin_(exclude_promo_types))
        q = q.order_by(MLPromoDecisionORM.created_at.desc()).limit(limit).offset(offset)
        rows = list((await self._session.execute(q)).scalars().all())
        total = int((await self._session.execute(count_q)).scalar_one())
        return rows, total

    async def get(self, decision_id: int) -> MLPromoDecisionORM | None:
        return await self._session.get(MLPromoDecisionORM, decision_id)

    async def decide(
        self,
        decision_id: int,
        *,
        status: str,
        by: str | None = None,
        notes: str | None = None,
        target_price: Decimal | None = None,
        target_total_pct: Decimal | None = None,
        target_seller_pct: Decimal | None = None,
        stock_chosen: int | None = None,
        decision_context: dict[str, Any] | None = None,
    ) -> MLPromoDecisionORM | None:
        """Move a pending decision to a terminal state.

        Valid terminal states: ``approved``, ``rejected``, ``ignored``.
        All three dedupe equally — the cron will not re-prompt for a
        row that has been touched by the operator. ``ignored`` is the
        "skip this without committing yes/no" lane.

        When the operator overrides ``target_price`` before approving,
        the caller passes the recomputed pct values too. Repo trusts
        the validated values — server-side validation happens in the
        router layer (where ``list_price``/``floor_price``/``cap_pct``
        live as decision columns).
        """
        if status not in ("approved", "rejected", "ignored"):
            raise ValueError(f"invalid decision status: {status}")
        row = await self.get(decision_id)
        if row is None or row.status != "pending":
            return None
        row.status = status
        row.decided_at = datetime.now(UTC)
        row.decided_by = by
        if notes is not None:
            row.notes = notes
        if target_price is not None:
            row.target_price = target_price
        if target_total_pct is not None:
            row.target_total_pct = target_total_pct
        if target_seller_pct is not None:
            row.target_seller_pct = target_seller_pct
        if stock_chosen is not None:
            row.stock_chosen = stock_chosen
        if decision_context is not None:
            row.decision_context = decision_context
        await self._session.flush()
        return row

    async def record_apply_result(
        self,
        decision_id: int,
        *,
        status: str,
        status_code: int | None,
        response: str | None,
    ) -> MLPromoDecisionORM | None:
        """Persist the outcome of an ML POST attempt for a decision.

        Called after the apply call returns (success or failure). Does
        NOT touch the operator status column — that's already
        ``approved`` by the time we get here. A row that has never
        been attempted keeps ``ml_apply_status=NULL``.
        """
        row = await self.get(decision_id)
        if row is None:
            return None
        row.ml_apply_status = status
        row.ml_apply_status_code = status_code
        row.ml_apply_response = response[:2000] if response else None
        row.ml_applied_at = datetime.now(UTC)
        await self._session.flush()
        return row

    async def expire(
        self,
        decision_id: int,
        *,
        reason: str,
    ) -> MLPromoDecisionORM | None:
        """Flip a pending decision to ``status='expired'`` with a reason.

        Only acts on rows currently in ``pending``; rows already in any
        terminal state are left alone (returns ``None``). ``decided_at``
        / ``decided_by`` stay untouched — ``expired_at`` is the audit
        column for this transition, so we can tell apart 'operator
        ignored' from 'system auto-expired'.
        """
        row = await self.get(decision_id)
        if row is None or row.status != "pending":
            return None
        row.status = "expired"
        row.expired_at = datetime.now(UTC)
        row.expired_reason = reason
        await self._session.flush()
        return row

    async def expire_disappeared_started(
        self,
        *,
        mlb_id: str,
        seen_promo_keys: set[str],
        reason: str = "campaign_ended",
    ) -> int:
        """Expire 'started' decisions for an MLB whose campaign ML no longer
        returns (campaign ended).

        ML's live ``started`` set (``seen_promo_keys``) is the source of truth
        for what's currently active. Any row still recorded as an active
        campaign for this MLB but absent from that set is marked expired — this
        is what stops finished campaigns (e.g. May SMART/SELLER) from lingering
        forever in the "Inscritas" list. An empty ``seen_promo_keys`` means ML
        returned no started promo for this MLB, so all of its started rows
        expire.

        Only touches rows in ``status='ignored'`` with
        ``constraint_used='started'`` (the visibility-only active rows). Never
        touches pending/approved/operator-decided rows. Returns the count.
        """
        conds = [
            MLPromoDecisionORM.mlb_id == mlb_id,
            MLPromoDecisionORM.constraint_used == "started",
            MLPromoDecisionORM.status == "ignored",
        ]
        if seen_promo_keys:
            conds.append(MLPromoDecisionORM.promo_key.notin_(seen_promo_keys))
        stmt = (
            update(MLPromoDecisionORM)
            .where(*conds)
            .values(
                status="expired",
                expired_at=datetime.now(UTC),
                expired_reason=reason,
            )
            .execution_options(synchronize_session=False)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    async def update_started_price(
        self,
        *,
        mlb_id: str,
        new_price: Decimal,
        promo_id: str | None = None,
        promo_type: str | None = None,
    ) -> int:
        """After a live price change on an active (started) promo, update the
        cached 'started' decision row(s) so the UI reflects the new price BEFORE
        the daily re-sync (ML writes don't touch our mirror). Matches by
        mlb_id + (promo_id or promo_type) among active started rows and
        recomputes the discount % vs list_price. Returns rows updated.
        """
        conds = [
            MLPromoDecisionORM.mlb_id == mlb_id,
            MLPromoDecisionORM.constraint_used == "started",
            MLPromoDecisionORM.status.notin_(["expired", "rejected"]),
        ]
        if promo_id:
            conds.append(
                or_(
                    MLPromoDecisionORM.promo_id == promo_id,
                    MLPromoDecisionORM.promo_key == promo_id,
                )
            )
        elif promo_type:
            conds.append(MLPromoDecisionORM.promo_type == promo_type)
        rows = list(
            (await self._session.execute(select(MLPromoDecisionORM).where(*conds))).scalars()
        )
        for r in rows:
            r.target_price = new_price
            if r.list_price and r.list_price > 0:
                r.target_total_pct = (
                    (r.list_price - new_price) / r.list_price * Decimal(100)
                ).quantize(Decimal("0.01"))
        await self._session.flush()
        return len(rows)

    async def expire_started(
        self,
        *,
        mlb_id: str,
        promo_id: str | None = None,
        promo_type: str | None = None,
        reason: str = "exited",
    ) -> int:
        """After a live exit, drop the cached 'started' row(s) from the active
        list immediately (status=expired) so they leave 'Inscritas' before the
        daily re-sync. Returns rows expired."""
        conds = [
            MLPromoDecisionORM.mlb_id == mlb_id,
            MLPromoDecisionORM.constraint_used == "started",
            MLPromoDecisionORM.status.notin_(["expired", "rejected"]),
        ]
        if promo_id:
            conds.append(
                or_(
                    MLPromoDecisionORM.promo_id == promo_id,
                    MLPromoDecisionORM.promo_key == promo_id,
                )
            )
        elif promo_type:
            conds.append(MLPromoDecisionORM.promo_type == promo_type)
        stmt = (
            update(MLPromoDecisionORM)
            .where(*conds)
            .values(status="expired", expired_at=datetime.now(UTC), expired_reason=reason)
            .execution_options(synchronize_session=False)
        )
        result = await self._session.execute(stmt)
        await self._session.flush()
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    async def restore_started_price(
        self,
        *,
        mlb_id: str,
        new_price: Decimal,
        promo_id: str | None = None,
        promo_type: str | None = None,
    ) -> int:
        """Re-activate a 'started' decision row that was expired while a
        re-subscribe was pending, now that the queue re-enrolled at ``new_price``.
        Un-expires the matching started row(s) and sets the new price so the
        listing reappears in 'Inscritas' before the daily re-sync. Returns rows
        restored (0 if none — the daily sync will recreate it)."""
        conds = [
            MLPromoDecisionORM.mlb_id == mlb_id,
            MLPromoDecisionORM.constraint_used == "started",
            MLPromoDecisionORM.status != "rejected",
        ]
        if promo_id:
            conds.append(
                or_(
                    MLPromoDecisionORM.promo_id == promo_id,
                    MLPromoDecisionORM.promo_key == promo_id,
                )
            )
        elif promo_type:
            conds.append(MLPromoDecisionORM.promo_type == promo_type)
        rows = list(
            (
                await self._session.execute(
                    select(MLPromoDecisionORM)
                    .where(*conds)
                    .order_by(MLPromoDecisionORM.created_at.desc())
                )
            ).scalars()
        )
        for r in rows:
            r.status = "ignored"
            r.expired_at = None
            r.expired_reason = None
            r.target_price = new_price
            if r.list_price and r.list_price > 0:
                r.target_total_pct = (
                    (r.list_price - new_price) / r.list_price * Decimal(100)
                ).quantize(Decimal("0.01"))
        await self._session.flush()
        return len(rows)

    async def revert_to_pending(
        self,
        decision_id: int,
    ) -> MLPromoDecisionORM | None:
        """Undo a terminal decision back to pending.

        Keeps ``decided_at`` / ``decided_by`` populated as audit of the
        last action — the row simply re-enters the queue. The cron's
        dedupe still respects (mlb_id, promo_key) so a fresh `generate`
        won't create a duplicate row; the operator just gets another
        chance to decide.
        """
        row = await self.get(decision_id)
        if row is None or row.status == "pending":
            return None
        row.status = "pending"
        # Clear the auto-expire stamp so a re-expired row can be detected
        # again next sweep; ``decided_at`` / ``decided_by`` are kept as
        # the audit trail of the previous operator action.
        row.expired_at = None
        row.expired_reason = None
        await self._session.flush()
        return row


# ---------------------------------------------------------------------------
# Re-subscribe queue (raise = exit + re-enroll, with ML re-suggest delay)
# ---------------------------------------------------------------------------
class MLPromoResubscribeRepository:
    """CRUD for ``ml_promo_resubscribe_jobs`` — the queue that retries a
    promotion re-enrollment until the ML re-suggests the offer as candidate."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue(
        self,
        *,
        mlb_id: str,
        sku: str,
        promo_type: str,
        target_price: Decimal,
        deadline: datetime,
        promo_id: str | None = None,
        max_attempts: int = 288,
        op_id: str | None = None,
        decided_by: str | None = None,
        last_error: str | None = None,
        last_status_code: int | None = None,
    ) -> MLPromoResubscribeJobORM:
        """Create (or reset) the pending re-subscribe job for this
        (mlb_id, promo_type). A partial unique index allows at most one
        pending row per pair, so a new raise on the same listing updates the
        existing pending job in place instead of stacking duplicates."""
        existing = (
            await self._session.execute(
                select(MLPromoResubscribeJobORM).where(
                    MLPromoResubscribeJobORM.mlb_id == mlb_id,
                    MLPromoResubscribeJobORM.promo_type == promo_type,
                    MLPromoResubscribeJobORM.status == "pending",
                )
            )
        ).scalar_one_or_none()
        now = datetime.now(UTC)
        if existing is not None:
            existing.sku = sku
            existing.promo_id = promo_id
            existing.target_price = target_price
            existing.deadline = deadline
            existing.max_attempts = max_attempts
            existing.attempts = 0
            existing.next_attempt_at = now
            existing.op_id = op_id
            existing.decided_by = decided_by
            existing.last_error = last_error
            existing.last_status_code = last_status_code
            existing.resolved_at = None
            await self._session.flush()
            return existing
        row = MLPromoResubscribeJobORM(
            mlb_id=mlb_id,
            sku=sku,
            promo_type=promo_type,
            promo_id=promo_id,
            target_price=target_price,
            deadline=deadline,
            max_attempts=max_attempts,
            next_attempt_at=now,
            op_id=op_id,
            decided_by=decided_by,
            last_error=last_error,
            last_status_code=last_status_code,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def record_completed(
        self,
        *,
        mlb_id: str,
        sku: str,
        promo_type: str,
        target_price: Decimal,
        promo_id: str | None = None,
        op_id: str | None = None,
        decided_by: str | None = None,
    ) -> MLPromoResubscribeJobORM:
        """Record an IMMEDIATE re-subscription (exit + re-enroll that succeeded
        on the first try, no ML lag) as a done job, so the operator still sees
        the re-inscrição confirmed in the dock for a short while."""
        now = datetime.now(UTC)
        row = MLPromoResubscribeJobORM(
            mlb_id=mlb_id,
            sku=sku,
            promo_type=promo_type,
            promo_id=promo_id,
            target_price=target_price,
            status="done",
            next_attempt_at=now,
            deadline=now,
            resolved_at=now,
            op_id=op_id,
            decided_by=decided_by,
        )
        self._session.add(row)
        await self._session.flush()
        return row

    async def due(
        self, *, now: datetime | None = None, limit: int = 100
    ) -> list[MLPromoResubscribeJobORM]:
        """Pending jobs whose ``next_attempt_at`` has passed, oldest first."""
        ref = now or datetime.now(UTC)
        q = (
            select(MLPromoResubscribeJobORM)
            .where(
                MLPromoResubscribeJobORM.status == "pending",
                MLPromoResubscribeJobORM.next_attempt_at <= ref,
            )
            .order_by(MLPromoResubscribeJobORM.next_attempt_at.asc())
            .limit(limit)
        )
        return list((await self._session.execute(q)).scalars().all())

    async def list_(
        self,
        *,
        status: str | None = None,
        mlb_id: str | None = None,
        limit: int = 200,
    ) -> list[MLPromoResubscribeJobORM]:
        q = select(MLPromoResubscribeJobORM)
        if status is not None:
            q = q.where(MLPromoResubscribeJobORM.status == status)
        if mlb_id is not None:
            q = q.where(MLPromoResubscribeJobORM.mlb_id == mlb_id)
        q = q.order_by(MLPromoResubscribeJobORM.created_at.desc()).limit(limit)
        return list((await self._session.execute(q)).scalars().all())

    async def get(self, job_id: int) -> MLPromoResubscribeJobORM | None:
        return await self._session.get(MLPromoResubscribeJobORM, job_id)

    async def mark_done(self, job: MLPromoResubscribeJobORM) -> None:
        job.status = "done"
        job.resolved_at = datetime.now(UTC)
        job.last_error = None
        await self._session.flush()

    async def mark_failed(
        self, job: MLPromoResubscribeJobORM, *, error: str, status_code: int | None = None
    ) -> None:
        job.status = "failed"
        job.resolved_at = datetime.now(UTC)
        job.last_error = error
        if status_code is not None:
            job.last_status_code = status_code
        await self._session.flush()

    async def cancel(self, job: MLPromoResubscribeJobORM) -> None:
        job.status = "cancelled"
        job.resolved_at = datetime.now(UTC)
        await self._session.flush()

    async def bump_attempt(
        self,
        job: MLPromoResubscribeJobORM,
        *,
        next_attempt_at: datetime,
        error: str | None = None,
        status_code: int | None = None,
    ) -> None:
        """Record one failed/idle poll: increment ``attempts`` and push out
        ``next_attempt_at``. If the attempt budget is exhausted, flip to failed."""
        job.attempts += 1
        job.next_attempt_at = next_attempt_at
        job.last_error = error
        job.last_status_code = status_code
        if job.attempts >= job.max_attempts:
            job.status = "failed"
            job.resolved_at = datetime.now(UTC)
            if error is None:
                job.last_error = "max_attempts atingido"
        await self._session.flush()
