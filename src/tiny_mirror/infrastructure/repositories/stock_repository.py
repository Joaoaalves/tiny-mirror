"""PostgreSQL implementation of :class:`StockRepository`."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import StockRepository
from tiny_mirror.infrastructure.orm.models import (
    ProductORM,
    StockDepositORM,
    StockORM,
)


class PostgreSQLStockRepository(StockRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, stock_data: dict[str, Any]) -> None:
        stmt = pg_insert(StockORM).values(**stock_data)
        update_payload = {
            col: stmt.excluded[col] for col in stock_data if col not in {"product_tiny_id"}
        }
        update_payload["updated_at"] = func.now()  # type: ignore[assignment]
        update_payload["synced_at"] = stock_data.get("synced_at", func.now())

        stmt = stmt.on_conflict_do_update(
            index_elements=["product_tiny_id"],
            set_=update_payload,
        )
        await self._session.execute(stmt)
        await self._session.commit()

    async def upsert_deposits(self, product_tiny_id: int, deposits: list[dict[str, Any]]) -> None:
        # DELETE + bulk INSERT inside a single transaction. The atomic
        # replace keeps stock_deposits consistent with the latest API
        # response — a deposit removed in Tiny disappears here too.
        await self._session.execute(
            delete(StockDepositORM).where(StockDepositORM.product_tiny_id == product_tiny_id)
        )

        if not deposits:
            await self._session.commit()
            return

        rows = [
            {
                "product_tiny_id": product_tiny_id,
                "deposit_tiny_id": d["deposit_tiny_id"],
                "deposit_name": d["deposit_name"],
                "ignore": d.get("ignore", False),
                "balance": d.get("balance", 0),
                "reserved": d.get("reserved", 0),
                "available": d.get("available", 0),
                # Set by _overlay_ml_full_deposit only on the Full ML row;
                # every other row defaults to 0 (Tiny payloads don't carry
                # this field).
                "in_transfer": d.get("in_transfer", 0),
                "company": d.get("company"),
            }
            for d in deposits
        ]
        await self._session.execute(pg_insert(StockDepositORM).values(rows))
        await self._session.commit()

    async def upsert_ml_full_deposit(
        self,
        product_tiny_id: int,
        available_qty: int,
        *,
        deposit_name: str,
        sentinel_deposit_id: int,
        in_transfer_qty: int = 0,
    ) -> None:
        """Insert-or-update only the 'Full Mercado Livre' deposit row.

        Surgical alternative to :meth:`upsert_deposits` for the high-frequency
        ML-only refresh: leaves galpão / A Caminho / Avaria untouched so we
        never wipe Tiny-sourced values between full syncs.

        Matches the existing FL row by ``deposit_name`` (case-sensitive — Tiny
        always returns 'Full Mercado Livre'). If absent, appends a synthetic
        row with ``deposit_tiny_id = sentinel_deposit_id`` so the unique
        constraint per (product, deposit_tiny_id) is honored.

        ``in_transfer_qty`` mirrors ML's ``not_available_detail[status=transfer]``:
        units physically at FL but being relocated between ML's own
        warehouses. ``balance`` stores the *physical* total (available +
        in_transfer) so external joins on ``balance`` see "everything at FL".
        """
        balance_total = available_qty + in_transfer_qty
        existing = await self._session.execute(
            select(StockDepositORM.id, StockDepositORM.deposit_tiny_id).where(
                StockDepositORM.product_tiny_id == product_tiny_id,
                StockDepositORM.deposit_name == deposit_name,
            )
        )
        row = existing.first()
        deposit_tiny_id = row.deposit_tiny_id if row is not None else sentinel_deposit_id
        stmt = (
            pg_insert(StockDepositORM)
            .values(
                product_tiny_id=product_tiny_id,
                deposit_tiny_id=deposit_tiny_id,
                deposit_name=deposit_name,
                ignore=False,
                balance=balance_total,
                reserved=0,
                available=available_qty,
                in_transfer=in_transfer_qty,
                company="Mercado Livre",
            )
            .on_conflict_do_update(
                index_elements=["product_tiny_id", "deposit_tiny_id"],
                set_={
                    "balance": balance_total,
                    "available": available_qty,
                    "in_transfer": in_transfer_qty,
                    "reserved": 0,
                    "ignore": False,
                    "updated_at": func.now(),
                },
            )
        )
        await self._session.execute(stmt)
        await self._session.commit()

    async def get_product_tiny_ids_to_sync(self) -> list[int]:
        result = await self._session.execute(
            select(ProductORM.tiny_id).where(ProductORM.situation == "A")
        )
        return [int(tid) for (tid,) in result.all()]

    async def get_by_product_tiny_id(self, product_tiny_id: int) -> dict[str, Any] | None:
        stock_result = await self._session.execute(
            select(StockORM).where(StockORM.product_tiny_id == product_tiny_id)
        )
        stock = stock_result.scalar_one_or_none()
        if stock is None:
            return None

        stock_dict = _row_to_dict(stock)
        deposits_result = await self._session.execute(
            select(StockDepositORM)
            .where(StockDepositORM.product_tiny_id == product_tiny_id)
            .order_by(StockDepositORM.id)
        )
        stock_dict["deposits"] = [_row_to_dict(d) for d in deposits_result.scalars().all()]
        return stock_dict


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {col.name: getattr(row, col.name) for col in row.__table__.columns}
