"""PostgreSQL implementation of :class:`OrderRepository`."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from sqlalchemy import delete, exists as sa_exists, func, literal_column, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import OrderRepository
from tiny_mirror.infrastructure.orm.models import OrderItemORM, OrderORM


class PostgreSQLOrderRepository(OrderRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, order_data: dict[str, Any]) -> str:
        stmt = pg_insert(OrderORM).values(**order_data)
        update_payload = {
            col: stmt.excluded[col]
            for col in order_data
            if col not in {"tiny_id", "created_at"}
        }
        update_payload["updated_at"] = func.now()
        update_payload["synced_at"] = order_data.get("synced_at", func.now())

        stmt = stmt.on_conflict_do_update(
            index_elements=["tiny_id"],
            set_=update_payload,
        ).returning(literal_column("(xmax = 0)").label("inserted"))

        result = await self._session.execute(stmt)
        inserted = result.scalar_one()
        await self._session.commit()
        return "created" if inserted else "updated"

    async def upsert_items(
        self, order_tiny_id: int, items: list[dict[str, Any]]
    ) -> None:
        await self._session.execute(
            delete(OrderItemORM).where(OrderItemORM.order_tiny_id == order_tiny_id)
        )

        if not items:
            await self._session.commit()
            return

        # The FK on product_tiny_id is nullable on purpose: an order may
        # reference a product that hasn't been synced yet. Resolve every id
        # against the products table and replace unknowns with NULL so the
        # INSERT cannot raise ForeignKeyViolationError. product_sku stays as
        # the stable identifier.
        from tiny_mirror.infrastructure.orm.models import ProductORM

        candidate_ids = {
            item.get("product_tiny_id")
            for item in items
            if item.get("product_tiny_id") is not None
        }
        existing_ids: set[int] = set()
        if candidate_ids:
            existing = await self._session.execute(
                select(ProductORM.tiny_id).where(
                    ProductORM.tiny_id.in_(candidate_ids)
                )
            )
            existing_ids = {int(tid) for (tid,) in existing.all()}

        rows = []
        for item in items:
            raw_pid = item.get("product_tiny_id")
            resolved = (
                int(raw_pid) if raw_pid is not None and int(raw_pid) in existing_ids
                else None
            )
            rows.append(
                {
                    "order_tiny_id": order_tiny_id,
                    "product_tiny_id": resolved,
                    "product_sku": item["product_sku"],
                    "product_description": item.get("product_description"),
                    "product_type": item.get("product_type"),
                    "quantity": item["quantity"],
                    "unit_value": item["unit_value"],
                    "additional_info": item.get("additional_info"),
                }
            )
        await self._session.execute(pg_insert(OrderItemORM).values(rows))
        await self._session.commit()

    async def get_by_tiny_id(self, tiny_id: int) -> dict[str, Any] | None:
        order_result = await self._session.execute(
            select(OrderORM).where(OrderORM.tiny_id == tiny_id)
        )
        order = order_result.scalar_one_or_none()
        if order is None:
            return None
        order_dict = _row_to_dict(order)

        items_result = await self._session.execute(
            select(OrderItemORM)
            .where(OrderItemORM.order_tiny_id == tiny_id)
            .order_by(OrderItemORM.id)
        )
        order_dict["items"] = [_row_to_dict(item) for item in items_result.scalars().all()]
        return order_dict

    async def get_recent_product_tiny_ids(self, hours: int) -> list[int]:
        # DISTINCT product_tiny_id from order_items joined to orders updated
        # within the lookback. Used to fan out incremental stock refresh.
        cutoff = func.now() - text(f"INTERVAL '{int(hours)} hours'")
        stmt = (
            select(OrderItemORM.product_tiny_id)
            .where(OrderItemORM.product_tiny_id.is_not(None))
            .where(
                sa_exists().where(
                    (OrderORM.tiny_id == OrderItemORM.order_tiny_id)
                    & (OrderORM.updated_at >= cutoff)
                )
            )
            .distinct()
        )
        result = await self._session.execute(stmt)
        return [int(row[0]) for row in result.all() if row[0] is not None]

    async def exists(self, tiny_id: int) -> bool:
        result = await self._session.execute(
            select(OrderORM.tiny_id).where(OrderORM.tiny_id == tiny_id).limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def count(self) -> int:
        result = await self._session.execute(select(func.count(OrderORM.tiny_id)))
        return int(result.scalar_one())

    async def get_orders_in_period(
        self, date_from: date, date_to: date
    ) -> list[dict[str, Any]]:
        # Inclusive of the boundary dates: [date_from, date_to + 1 day).
        upper_bound = date_to + timedelta(days=1)
        result = await self._session.execute(
            select(OrderORM)
            .where(OrderORM.order_date >= date_from)
            .where(OrderORM.order_date < upper_bound)
            .order_by(OrderORM.order_date, OrderORM.tiny_id)
        )
        orders = result.scalars().all()
        if not orders:
            return []

        order_ids = [o.tiny_id for o in orders]
        items_result = await self._session.execute(
            select(OrderItemORM)
            .where(OrderItemORM.order_tiny_id.in_(order_ids))
            .order_by(OrderItemORM.order_tiny_id, OrderItemORM.id)
        )
        items_by_order: dict[int, list[dict[str, Any]]] = {oid: [] for oid in order_ids}
        for item in items_result.scalars().all():
            items_by_order[int(item.order_tiny_id)].append(_row_to_dict(item))

        out: list[dict[str, Any]] = []
        for order in orders:
            d = _row_to_dict(order)
            d["items"] = items_by_order.get(int(order.tiny_id), [])
            out.append(d)
        return out


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {col.name: getattr(row, col.name) for col in row.__table__.columns}
