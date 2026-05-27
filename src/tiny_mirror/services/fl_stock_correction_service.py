"""Hourly cron: corrige o saldo do depósito 'Full Mercado Livre' no Tiny.

Para cada produto base (não kit/combo) que tem listing FL ativo:

  1. Lê saldo físico atual no Tiny (campo 'saldo', não 'disponivel' — ver
     docs/02_fl_stock_correction_plan.md "saldo vs disponível").
  2. Compara com o ML real (= ``stock_deposits.available`` filtrado pelo
     depósito Full ML, alimentado pelo cron ml_fl_stock de 15 min).
  3. Se diff: captura snapshot de investigação (estado completo do produto +
     orders/transfers recentes) e aplica balanço ``tipo='B'`` no Tiny.
  4. Persiste tudo em ``fl_stock_corrections_log`` independente de sucesso.

Filtro hard de SKUs: apenas SKUs base com listing FL ativo.

  Kits (NU-X), combos (COM-*) e KIT-* são EXCLUÍDOS porque o Tiny rejeita
  update manual em kit (HTTP 400 "Não é possível atualizar o estoque de um
  produto kit"). Tiny calcula o saldo deles automaticamente via componentes
  — ao corrigir os base, os kits seguem.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import text

from tiny_mirror.database import AsyncSessionLocal
from tiny_mirror.infrastructure.external.tiny_client import TinyAPIClient
from tiny_mirror.infrastructure.repositories.fl_stock_correction_log_repository import (
    FLStockCorrectionLogRepository,
)
from tiny_mirror.infrastructure.repositories.sync_log_repository import SyncLogRepository

logger = structlog.get_logger(__name__)

FULL_ML_DEPOSITO_ID = 912048995
FULL_ML_DEPOSITO_NAME = "Full Mercado Livre"
CORRECTION_MESSAGE = "[AUTO - Correção automática estoque Fulfillment]"


class FLStockCorrectionService:
    def __init__(self, tiny_client: TinyAPIClient) -> None:
        self._tiny = tiny_client

    # ------------------------------------------------------------------
    async def run_correction(self, sync_log_id: int) -> None:
        """Executes one correction pass over all eligible base SKUs.

        For every mismatch detected:
          - Records a row in fl_stock_corrections_log with full forensics.
          - Applies the balance (tipo=B) on the Tiny side.

        Failures during a single SKU do not abort the pass — they're logged
        and the loop continues.
        """
        logger.info("FL stock correction job started", sync_log_id=sync_log_id)

        candidates = await self._load_candidates()
        logger.info("FL correction candidates loaded", count=len(candidates))

        processed = 0
        failed = 0
        corrected = 0
        for tiny_id, sku, ml_qty in candidates:
            try:
                result = await self._handle_one(tiny_id, sku, ml_qty)
                processed += 1
                if result == "corrected":
                    corrected += 1
            except Exception as exc:
                failed += 1
                logger.warning(
                    "FL correction failed for SKU, continuing",
                    sku=sku,
                    tiny_id=tiny_id,
                    error=str(exc),
                )

        async with AsyncSessionLocal() as session:
            sync_logs = SyncLogRepository(session)
            for _ in range(processed):
                await sync_logs.increment_processed(sync_log_id)
            for _ in range(failed):
                await sync_logs.increment_failed(sync_log_id)
            await sync_logs.try_finalize(sync_log_id)

        logger.info(
            "FL stock correction job completed",
            sync_log_id=sync_log_id,
            processed=processed,
            corrected=corrected,
            failed=failed,
            total=len(candidates),
        )

    # ------------------------------------------------------------------
    async def _load_candidates(self) -> list[tuple[int, str, int]]:
        """Returns [(tiny_id, sku, ml_qty)] for every base SKU with FL listing.

        Filters:
          - p.situation = 'A' (active product in Tiny)
          - has stock_deposits row for 'Full Mercado Livre' (= ML truth populated)
          - has at least one MLB with logistic_type='fulfillment' AND status='active'
          - is NOT a kit/combo/KIT-* (Tiny rejects updates on kits)
          - is NOT a component-only kit_product (no rows in product_kit_components
            where this product is the kit)
          - excludes test SKUs (SKU-TEST*)
        """
        sql = text(
            """
            SELECT sd.product_tiny_id, p.sku, sd.available::int AS ml_qty
            FROM stock_deposits sd
            JOIN products p ON p.tiny_id = sd.product_tiny_id
            WHERE sd.deposit_name = :deposit_name
              AND p.situation = 'A'
              AND p.sku IS NOT NULL
              AND p.sku <> ''
              AND p.sku NOT LIKE 'SKU-TEST%'
              AND p.sku !~ '^[0-9]+U-'
              AND p.sku NOT LIKE 'COM-%'
              AND p.sku NOT LIKE 'KIT-%'
              AND NOT EXISTS (
                  SELECT 1 FROM product_kit_components kc
                  WHERE kc.kit_product_tiny_id = p.tiny_id
              )
              AND EXISTS (
                  SELECT 1 FROM ml_listings ml
                  WHERE ml.sku = p.sku
                    AND ml.logistic_type = 'fulfillment'
                    AND ml.status = 'active'
              );
            """
        )
        async with AsyncSessionLocal() as session:
            result = await session.execute(sql, {"deposit_name": FULL_ML_DEPOSITO_NAME})
            rows = result.all()
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]

    # ------------------------------------------------------------------
    async def _handle_one(self, tiny_id: int, sku: str, ml_qty: int) -> str:
        """Process one SKU: detect mismatch, capture investigation, apply correction.

        Returns "aligned" if no correction was needed, "corrected" otherwise.
        """
        # 1. Fetch current Tiny estoque (full snapshot for investigation)
        tiny_estoque = await self._tiny.get_stock(tiny_id)
        tiny_saldo = _extract_full_saldo(tiny_estoque)

        if tiny_saldo == ml_qty:
            return "aligned"

        delta = ml_qty - tiny_saldo
        investigation = await self._build_investigation(tiny_id, sku, tiny_estoque)

        # 2. Apply balance
        observacoes = (
            f"{CORRECTION_MESSAGE} Origem: ML API via tiny-sync. "
            f"Snapshot: {datetime.now(UTC).isoformat(timespec='seconds')}. "
            f"ML qty={ml_qty}, Tiny anterior={tiny_saldo}, delta={delta:+d}. "
            f"Operação: balanço (saldo final = {ml_qty})."
        )
        applied = False
        id_lancamento: int | None = None
        saldo_after: int | None = None
        http_status: int | None = None
        error_msg: str | None = None
        try:
            resp = await self._tiny.record_stock_movement(
                product_id=tiny_id,
                deposit_id=FULL_ML_DEPOSITO_ID,
                tipo="B",
                quantity=ml_qty,
                price_unit=0,
                data=datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
                observacoes=observacoes,
            )
            applied = True
            raw_id = resp.get("idLancamento") if resp else None
            id_lancamento = int(raw_id) if raw_id is not None else None
            http_status = 200
            # Re-fetch to capture post-correction saldo
            try:
                tiny_estoque_after = await self._tiny.get_stock(tiny_id)
                saldo_after = _extract_full_saldo(tiny_estoque_after)
            except Exception:
                saldo_after = None
        except Exception as exc:
            error_msg = str(exc)
            # Try to extract status code from exception if available
            http_status = getattr(exc, "status_code", None)

        # 3. Persist audit row
        async with AsyncSessionLocal() as session:
            repo = FLStockCorrectionLogRepository(session)
            await repo.record(
                product_tiny_id=tiny_id,
                sku=sku,
                tiny_saldo_before=tiny_saldo,
                ml_qty=ml_qty,
                delta=delta,
                correction_applied=applied,
                tiny_id_lancamento=id_lancamento,
                tiny_saldo_after=saldo_after,
                http_status=http_status,
                error_message=error_msg,
                investigation_payload=investigation,
            )

        if applied:
            logger.info(
                "FL stock corrected",
                sku=sku,
                tiny_id=tiny_id,
                before=tiny_saldo,
                after=saldo_after,
                delta=delta,
                id_lancamento=id_lancamento,
            )
        else:
            logger.warning(
                "FL stock correction failed",
                sku=sku,
                tiny_id=tiny_id,
                delta=delta,
                error=error_msg,
            )
        return "corrected"

    # ------------------------------------------------------------------
    async def _build_investigation(
        self, tiny_id: int, sku: str, tiny_estoque: dict[str, Any]
    ) -> dict[str, Any]:
        """Gather forensic context for the mismatch.

        Captures, for the last 7 days:
          - tiny_estoque: full /estoque/{id} response (all deposits)
          - recent_orders: count + sample by status
          - recent_fulfillment_transfers
          - recent_stock_history snapshots (if available)

        Designed to give the operator enough context to investigate the drift
        cause later without re-running queries.
        """
        cutoff = datetime.now(UTC) - timedelta(days=7)

        async with AsyncSessionLocal() as session:
            orders_result = await session.execute(
                text(
                    """
                    SELECT o.tiny_id, o.ecommerce_order_number, o.order_date::text,
                           o.ecommerce_name, oi.quantity::int, o.situation
                    FROM orders o
                    JOIN order_items oi ON oi.order_tiny_id = o.tiny_id
                    WHERE oi.product_sku = :sku
                      AND o.order_date >= :cutoff
                    ORDER BY o.order_date DESC
                    LIMIT 50;
                    """
                ),
                {"sku": sku, "cutoff": cutoff.date()},
            )
            orders = [
                {
                    "tiny_id": int(r[0]),
                    "ecommerce_order_number": r[1],
                    "order_date": r[2],
                    "ecommerce_name": r[3],
                    "quantity": int(r[4]),
                    "situation": int(r[5]) if r[5] is not None else None,
                }
                for r in orders_result.all()
            ]

            transfers_result = await session.execute(
                text(
                    """
                    SELECT id, quantity, transferred_at::text, received_at::text,
                           status, source
                    FROM fulfillment_transfers
                    WHERE product_tiny_id = :tiny_id
                      AND transferred_at >= :cutoff
                    ORDER BY transferred_at DESC
                    LIMIT 30;
                    """
                ),
                {"tiny_id": tiny_id, "cutoff": cutoff},
            )
            transfers = [
                {
                    "id": int(r[0]),
                    "quantity": int(r[1]),
                    "transferred_at": r[2],
                    "received_at": r[3],
                    "status": r[4],
                    "source": r[5],
                }
                for r in transfers_result.all()
            ]

            # stock_history might be empty for some SKUs — handled gracefully
            history_result = await session.execute(
                text(
                    """
                    SELECT snapshot_date::text, deposit_name, balance::int, available::int
                    FROM stock_history
                    WHERE product_tiny_id = :tiny_id
                      AND snapshot_date >= :cutoff_date
                    ORDER BY snapshot_date DESC, deposit_name
                    LIMIT 50;
                    """
                ),
                {"tiny_id": tiny_id, "cutoff_date": cutoff.date()},
            )
            history = [
                {
                    "snapshot_date": r[0],
                    "deposit_name": r[1],
                    "balance": int(r[2]),
                    "available": int(r[3]),
                }
                for r in history_result.all()
            ]

        return {
            "tiny_estoque_before": tiny_estoque,
            "recent_orders_7d": orders,
            "recent_fulfillment_transfers_7d": transfers,
            "recent_stock_history_7d": history,
        }


# ---------------------------------------------------------------------------
def _extract_full_saldo(tiny_estoque: dict[str, Any]) -> int:
    """Extract the SALDO (físico) of the Full Mercado Livre deposit.

    Returns 0 if the deposit row is missing — pra correção significa que
    o produto está no Tiny mas sem o depósito FL configurado, e a próxima
    movimentação Tipo=E vai criá-lo.
    """
    for d in tiny_estoque.get("depositos", []) or []:
        if d.get("nome") == FULL_ML_DEPOSITO_NAME:
            return int(d.get("saldo") or 0)
    return 0
