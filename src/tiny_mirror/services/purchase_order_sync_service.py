"""Syncs Ordens de Compra from Tiny v3 and derives supplier lead times.

Paginates through all OCs from /ordem-compra, upserts them into
purchase_orders, and then recomputes supplier_lead_times from the current
snapshot of all OCs.

Lead time calculation:
  - If an OC has date_prevista > data: planned lead time = date_prevista - data
  - If we detect situacao transitioning to '4' (Concluída), completed_at
    is set to NOW() and actual lead time = completed_at::date - data.
  - supplier_lead_times stores the MEDIAN days across all qualifying OCs
    per supplier (minimum 1 sample; any data beats the 15-day fallback).

Situacao codes (Tiny v3 OC):
  '0' = Aberta/Pendente
  '1' = Aprovada
  '2' = Em Andamento
  '4' = Concluída  (completed — sets completed_at when first seen)
  '5' = Cancelada  (excluded from lead time calc)
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from typing import Any

import structlog
from sqlalchemy import text

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.exceptions import TinyAPIException
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository

logger = structlog.get_logger(__name__)

_COMPLETED_SITUACAO = {"4"}
_CANCELLED_SITUACAO = {"5"}
_PAGE_SIZE = 100


class PurchaseOrderSyncService:
    def __init__(self, tiny_client: TinyAPIClient) -> None:
        self._tiny = tiny_client

    async def run_sync(self, sync_log_id: int) -> None:
        logger.info("Starting purchase order sync", sync_log_id=sync_log_id)

        all_ocs: list[dict[str, Any]] = []
        offset = 0

        while True:
            try:
                resp = await self._tiny.list_purchase_orders(offset=offset, limit=_PAGE_SIZE)
            except TinyAPIException as exc:
                logger.error("Tiny v3 OC fetch failed", offset=offset, error=str(exc))
                async with AsyncSessionLocal() as session:
                    await SyncLogRepository(session).update_sync_log_failed(
                        sync_log_id,
                        error_message=str(exc),
                        items_processed=len(all_ocs),
                        items_failed=1,
                    )
                return

            items = resp.get("itens") or []
            all_ocs.extend(items)

            pag = resp.get("paginacao") or {}
            total = int(pag.get("total", 0))
            offset += len(items)

            if not items or offset >= total:
                break

            await asyncio.sleep(0.5)

        logger.info("Purchase orders fetched", count=len(all_ocs))

        processed = 0
        failed = 0

        async with AsyncSessionLocal() as session:
            for oc in all_ocs:
                try:
                    await self._upsert_oc(session, oc)
                    processed += 1
                except Exception as exc:
                    logger.warning(
                        "Failed to upsert purchase_order",
                        oc_id=oc.get("id"),
                        error=str(exc),
                    )
                    failed += 1
            await session.commit()

        await self._recompute_lead_times()

        async with AsyncSessionLocal() as session:
            repo = SyncLogRepository(session)
            if failed > 0 and processed == 0:
                await repo.update_sync_log_failed(
                    sync_log_id,
                    error_message=f"All {failed} OCs failed",
                    items_processed=0,
                    items_failed=failed,
                )
            else:
                await repo.update_sync_log_complete(
                    sync_log_id, items_processed=processed, items_failed=failed
                )

        logger.info(
            "Purchase order sync complete",
            processed=processed,
            failed=failed,
        )

    @staticmethod
    def _parse_date(v: str | None) -> date | None:
        if not v:
            return None
        try:
            return datetime.strptime(v, "%Y-%m-%d").date()
        except ValueError:
            return None

    async def _upsert_oc(self, session: Any, oc: dict[str, Any]) -> None:
        oc_id = int(oc["id"])
        contato = oc.get("contato") or {}
        situacao = str(oc.get("situacao") or "")

        date_prevista_raw = self._parse_date(oc.get("dataPrevista"))
        data_raw = self._parse_date(oc.get("data"))

        existing = await session.execute(
            text("SELECT situacao, completed_at FROM purchase_orders WHERE id = :id"),
            {"id": oc_id},
        )
        row = existing.fetchone()
        existing_situacao = row[0] if row else None
        existing_completed_at = row[1] if row else None

        completed_at = existing_completed_at
        if situacao in _COMPLETED_SITUACAO and existing_situacao not in _COMPLETED_SITUACAO:
            completed_at = datetime.now(UTC)

        await session.execute(
            text("""
                INSERT INTO purchase_orders
                    (id, numero, data, situacao, date_prevista,
                     total_produtos, total_pedido,
                     supplier_id, supplier_name, supplier_cnpj,
                     observacoes, observacoes_internas,
                     completed_at, synced_at)
                VALUES
                    (:id, :numero, :data, :situacao, :date_prevista,
                     :total_produtos, :total_pedido,
                     :supplier_id, :supplier_name, :supplier_cnpj,
                     :observacoes, :observacoes_internas,
                     :completed_at, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    situacao              = EXCLUDED.situacao,
                    date_prevista         = EXCLUDED.date_prevista,
                    total_produtos        = EXCLUDED.total_produtos,
                    total_pedido          = EXCLUDED.total_pedido,
                    supplier_name         = EXCLUDED.supplier_name,
                    supplier_cnpj         = EXCLUDED.supplier_cnpj,
                    observacoes           = EXCLUDED.observacoes,
                    observacoes_internas  = EXCLUDED.observacoes_internas,
                    completed_at          = COALESCE(purchase_orders.completed_at, EXCLUDED.completed_at),
                    synced_at             = EXCLUDED.synced_at
            """),
            {
                "id": oc_id,
                "numero": oc.get("numero"),
                "data": data_raw,
                "situacao": situacao,
                "date_prevista": date_prevista_raw,
                "total_produtos": oc.get("totalProdutos"),
                "total_pedido": oc.get("totalPedidoCompra"),
                "supplier_id": contato.get("id"),
                "supplier_name": contato.get("nome"),
                "supplier_cnpj": contato.get("cpfCnpj"),
                "observacoes": oc.get("observacoes"),
                "observacoes_internas": oc.get("observacoesInternas"),
                "completed_at": completed_at,
            },
        )

    async def _recompute_lead_times(self) -> None:
        """Recompute supplier_lead_times from current purchase_orders snapshot.

        Uses whichever date is available first:
          1. completed_at::date (actual receipt, most accurate)
          2. date_prevista (planned delivery date, proxy)

        Supplier names with at least 1 qualifying OC get a lead time row.
        Cancelled OCs (situacao='5') are excluded.
        """
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("""
                    INSERT INTO supplier_lead_times (supplier_name, lead_time_days, sample_count, last_computed)
                    SELECT
                        supplier_name,
                        GREATEST(1, PERCENTILE_CONT(0.5) WITHIN GROUP (
                            ORDER BY (
                                COALESCE(completed_at::date, date_prevista) - data
                            )::float
                        ))::numeric(5,1) AS lead_time_days,
                        COUNT(*) AS sample_count,
                        NOW() AS last_computed
                    FROM purchase_orders
                    WHERE supplier_name IS NOT NULL
                      AND data IS NOT NULL
                      AND (
                          (completed_at IS NOT NULL AND completed_at::date >= data)
                          OR (date_prevista IS NOT NULL AND date_prevista >= data)
                      )
                      AND situacao != '5'
                    GROUP BY supplier_name
                    HAVING COUNT(*) >= 1
                    ON CONFLICT (supplier_name) DO UPDATE SET
                        lead_time_days = EXCLUDED.lead_time_days,
                        sample_count   = EXCLUDED.sample_count,
                        last_computed  = EXCLUDED.last_computed
                """)
            )
            await session.commit()

        logger.info("supplier_lead_times recomputed")
