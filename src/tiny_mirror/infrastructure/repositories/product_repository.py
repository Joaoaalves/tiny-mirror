"""PostgreSQL implementation of :class:`ProductRepository`."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tiny_mirror.domain.interfaces import ProductRepository
from tiny_mirror.infrastructure.orm.models import (
    ProductKitComponentORM,
    ProductORM,
)


class PostgreSQLProductRepository(ProductRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ------------------------------------------------------------------
    async def upsert(self, product_data: dict[str, Any]) -> str:
        """Upsert by ``tiny_id`` and use the system column ``xmax`` to detect
        whether the row was inserted (``xmax = 0``) or updated (``xmax != 0``).

        ``parent_product_tiny_id`` is a self-referential FK and Tiny does
        not guarantee that parents arrive before children during a fan-out
        sync. Resolve the parent against the table on the way in and write
        ``NULL`` if it has not landed yet — the next scheduled sync pass
        will re-link it once the parent exists. The same pattern is used
        for ``order_items.product_tiny_id``.
        """
        product_data = await self._resolve_parent_or_null(product_data)

        stmt = pg_insert(ProductORM).values(**product_data)
        update_payload = {
            col: stmt.excluded[col] for col in product_data if col not in {"tiny_id", "created_at"}
        }
        update_payload["updated_at"] = func.now()  # type: ignore[assignment]
        update_payload["synced_at"] = product_data.get("synced_at", func.now())

        stmt = stmt.on_conflict_do_update(  # type: ignore[assignment]
            index_elements=["tiny_id"],
            set_=update_payload,
        ).returning(literal_column("(xmax = 0)").label("inserted"))

        result = await self._session.execute(stmt)
        inserted = result.scalar_one()
        await self._session.commit()
        return "created" if inserted else "updated"

    async def _resolve_parent_or_null(self, product_data: dict[str, Any]) -> dict[str, Any]:
        parent_id = product_data.get("parent_product_tiny_id")
        if parent_id is None:
            return product_data
        # A product is its own root — never violate the FK with self-reference.
        if parent_id == product_data.get("tiny_id"):
            return {**product_data, "parent_product_tiny_id": None}
        exists = await self._session.scalar(
            select(ProductORM.tiny_id).where(ProductORM.tiny_id == parent_id)
        )
        if exists is None:
            return {**product_data, "parent_product_tiny_id": None}
        return product_data

    async def get_by_tiny_id(self, tiny_id: int) -> dict[str, Any] | None:
        result = await self._session.execute(
            select(ProductORM).where(ProductORM.tiny_id == tiny_id)
        )
        row = result.scalar_one_or_none()
        return _row_to_dict(row) if row is not None else None

    async def get_by_sku(self, sku: str) -> dict[str, Any] | None:
        result = await self._session.execute(select(ProductORM).where(ProductORM.sku == sku))
        row = result.scalar_one_or_none()
        return _row_to_dict(row) if row is not None else None

    async def list_active(self) -> list[int]:
        result = await self._session.execute(
            select(ProductORM.tiny_id).where(ProductORM.situation == "A")
        )
        return [int(tid) for (tid,) in result.all()]

    async def list_active_skus(self) -> list[str]:
        result = await self._session.execute(
            select(ProductORM.sku).where(ProductORM.situation == "A")
        )
        return [str(sku) for (sku,) in result.all()]

    async def count(self) -> int:
        result = await self._session.execute(select(func.count(ProductORM.tiny_id)))
        return int(result.scalar_one())

    # ------------------------------------------------------------------
    async def upsert_kit_components(
        self, kit_tiny_id: int, components: list[dict[str, Any]]
    ) -> None:
        # Atomic replace inside a single transaction: delete every existing
        # component for this kit, then insert the new set. Any rows the
        # caller no longer cares about disappear.
        await self._session.execute(
            delete(ProductKitComponentORM).where(
                ProductKitComponentORM.kit_product_tiny_id == kit_tiny_id
            )
        )

        if not components:
            await self._session.commit()
            return

        # Resolve component_product_tiny_id from sku when the component
        # exists in the products table; otherwise leave it NULL — the FK
        # is nullable on purpose, since a component may be synced later.
        skus = [c["component_sku"] for c in components if c.get("component_sku")]
        existing_skus: dict[str, int] = {}
        if skus:
            sku_lookup = await self._session.execute(
                select(ProductORM.sku, ProductORM.tiny_id).where(ProductORM.sku.in_(skus))
            )
            existing_skus = {sku: int(tid) for sku, tid in sku_lookup.all()}

        rows = []
        for c in components:
            sku = c.get("component_sku")
            resolved_id = existing_skus.get(sku) if sku else None
            rows.append(
                {
                    "kit_product_tiny_id": kit_tiny_id,
                    "component_product_tiny_id": resolved_id,
                    "component_sku": sku,
                    "component_description": c.get("component_description"),
                    "component_type": c.get("component_type"),
                    "quantity": c["quantity"],
                }
            )
        await self._session.execute(pg_insert(ProductKitComponentORM).values(rows))
        await self._session.commit()

    async def get_kit_components(self, kit_tiny_id: int) -> list[dict[str, Any]]:
        result = await self._session.execute(
            select(ProductKitComponentORM)
            .where(ProductKitComponentORM.kit_product_tiny_id == kit_tiny_id)
            .order_by(ProductKitComponentORM.id)
        )
        return [_row_to_dict(row) for row in result.scalars().all()]

    async def get_kit_components_for_ids(
        self, kit_tiny_ids: list[int]
    ) -> dict[int, list[dict[str, Any]]]:
        if not kit_tiny_ids:
            return {}
        result = await self._session.execute(
            select(ProductKitComponentORM)
            .where(ProductKitComponentORM.kit_product_tiny_id.in_(kit_tiny_ids))
            .order_by(
                ProductKitComponentORM.kit_product_tiny_id,
                ProductKitComponentORM.id,
            )
        )
        bucket: dict[int, list[dict[str, Any]]] = {kid: [] for kid in kit_tiny_ids}
        for row in result.scalars().all():
            bucket.setdefault(int(row.kit_product_tiny_id), []).append(_row_to_dict(row))
        return bucket


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {col.name: getattr(row, col.name) for col in row.__table__.columns}
