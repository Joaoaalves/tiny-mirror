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

        # Single-pass cron: no fan-out, so flip 'running' → 'completed'
        # synchronously. try_finalize requires metadata.total_enqueued, which
        # this job never sets — calling it would leak the sync_log until the
        # 90-min stale watchdog flipped it to 'failed'.
        async with AsyncSessionLocal() as session:
            await SyncLogRepository(session).update_sync_log_complete(
                sync_log_id, items_processed=processed, items_failed=failed
            )

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
        """Returns [(tiny_id, sku, ml_qty)] for every base SKU with FL listing
        eligible for the cron's drift check.

        Filters:
          - p.situation = 'A' (active product in Tiny)
          - has stock_deposits row for 'Full Mercado Livre' (= ML truth populated)
          - has at least one MLB with logistic_type='fulfillment' AND status='active'
          - is NOT a kit/combo/KIT-* (Tiny rejects updates on kits)
          - is NOT a component-only kit_product (no rows in product_kit_components
            where this product is the kit)
          - excludes test SKUs (SKU-TEST*)
          - **quiet**: no Mercado Livre order activity in the last 6h.
            Avoids racing Tiny's auto-NF + FL auto-baixa pipeline that runs
            minutes after the ML shipment. SKUs that aren't quiet this run
            get picked up by the next one.

        The lag-vs-drift discrimination (ML's Inventory API trailing Tiny's
        NF by hours) is NOT done here — high-velocity SKUs would never be
        eligible if we widened this window. It lives downstream in
        ``_handle_one`` via the ping-pong + cooldown guards, which weigh
        each detected drift against the recent correction history.
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
              )
              AND NOT EXISTS (
                  -- "quiet" check: ML order activity in the last 6h means
                  -- Tiny's auto-baixa pipeline is still in motion — a
                  -- correction during that window risks racing the
                  -- invoicing and double-counting. SKUs that aren't quiet
                  -- this run get picked up by the next one.
                  --
                  -- A LONGER window (24h) was tried in 2026-06-01 to catch
                  -- the ML-Inventory-lag pattern, but it permanently
                  -- excluded high-velocity SKUs from EVER being corrected
                  -- — they always have order activity in 24h. The right
                  -- defence against lag isn't candidate-side exclusion
                  -- but downstream guards (_is_ping_pong + _is_in_cooldown
                  -- in _handle_one), which evaluate each detected drift
                  -- against the recent correction history per SKU.
                  SELECT 1
                  FROM order_items oi
                  JOIN orders o ON o.tiny_id = oi.order_tiny_id
                  WHERE oi.product_sku = p.sku
                    AND o.ecommerce_name LIKE 'Mercado Livre%'
                    AND o.synced_at >= NOW() - INTERVAL '6 hours'
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

        Returns "aligned" if no correction was needed, "skipped" when the
        ping-pong guard rejected the run, "corrected" when the balance
        was applied (success or failure — see fl_stock_corrections_log).
        """
        # 1. Fetch current Tiny estoque (full snapshot for investigation)
        tiny_estoque = await self._tiny.get_stock(tiny_id)
        tiny_saldo = _extract_full_saldo(tiny_estoque)

        if tiny_saldo == ml_qty:
            return "aligned"

        delta = ml_qty - tiny_saldo
        investigation = await self._build_investigation(tiny_id, sku, tiny_estoque)

        # 1b. Ping-pong guard: if we just corrected this SKU in the
        # opposite direction within 48h and applying this delta would
        # cancel that out, refuse to flip back. Per the 2026-06-01
        # audit, 22% of corrections in the last 30 days were the second
        # leg of a ping-pong — almost always caused by ML's Inventory
        # API lagging behind a Tiny NF. The right behaviour is to wait;
        # the next cron run picks it up once both sides settle.
        recent = await self._recent_correction(sku)
        skip_reason: str | None = None
        skip_guard: str | None = None
        if _is_ping_pong(delta=delta, recent_correction=recent):
            assert recent is not None  # narrowed by _is_ping_pong
            age_hours = (datetime.now(UTC) - recent["created_at"]).total_seconds() / 3600.0
            skip_reason = (
                f"skipped: ping-pong contra correção anterior delta={recent['delta']:+d} "
                f"de {age_hours:.1f}h atrás (soma {recent['delta'] + delta:+d}); "
                f"provável lag ML Inventory vs Tiny NF"
            )
            skip_guard = "ping-pong"
        elif _is_in_cooldown(delta=delta, recent_correction=recent):
            assert recent is not None  # narrowed by _is_in_cooldown
            age_hours = (datetime.now(UTC) - recent["created_at"]).total_seconds() / 3600.0
            skip_reason = (
                f"skipped: cooldown — correção anterior delta={recent['delta']:+d} "
                f"de {age_hours:.1f}h atrás e |delta atual|={abs(delta)} ≤ "
                f"{COOLDOWN_MAX_MAGNITUDE}; provável lag oscilatório de alta-velocidade"
            )
            skip_guard = "cooldown"

        if skip_reason is not None:
            assert recent is not None
            logger.info(
                "FL correction skipped",
                guard=skip_guard,
                sku=sku,
                tiny_id=tiny_id,
                delta=delta,
                prev_delta=recent["delta"],
            )
            async with AsyncSessionLocal() as session:
                repo = FLStockCorrectionLogRepository(session)
                await repo.record(
                    product_tiny_id=tiny_id,
                    sku=sku,
                    tiny_saldo_before=tiny_saldo,
                    ml_qty=ml_qty,
                    delta=delta,
                    correction_applied=False,
                    tiny_id_lancamento=None,
                    tiny_saldo_after=None,
                    http_status=None,
                    error_message=skip_reason,
                    investigation_payload=investigation,
                )
            return "skipped"

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
    async def _recent_correction(self, sku: str) -> dict[str, Any] | None:
        """Return the most recent successful correction for this SKU in
        the last 48h, or None. Used by the ping-pong guard.

        Filters ``correction_applied = true`` so a previous skip doesn't
        block all future runs. Returns ``delta`` (signed) and
        ``created_at`` (timezone-aware) — enough for the caller to
        compute age and check for sign-flip.
        """
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text(
                    """
                    SELECT delta, created_at
                    FROM fl_stock_corrections_log
                    WHERE sku = :sku
                      AND correction_applied = true
                      AND created_at >= NOW() - INTERVAL '48 hours'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"sku": sku},
            )
            row = result.first()
        if row is None:
            return None
        return {"delta": int(row[0]), "created_at": row[1]}

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

        Each query is independently fault-tolerant — failures here are logged
        but never abort the correction itself (the audit row still gets written).
        """
        cutoff = datetime.now(UTC) - timedelta(days=7)
        out: dict[str, Any] = {"tiny_estoque_before": tiny_estoque}

        async with AsyncSessionLocal() as session:
            try:
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
                out["recent_orders_7d"] = [
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
            except Exception as exc:
                logger.warning("Investigation orders query failed", sku=sku, error=str(exc))
                out["recent_orders_7d_error"] = str(exc)

            try:
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
                out["recent_fulfillment_transfers_7d"] = [
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
            except Exception as exc:
                logger.warning("Investigation transfers query failed", sku=sku, error=str(exc))
                out["recent_fulfillment_transfers_7d_error"] = str(exc)

            try:
                history_result = await session.execute(
                    text(
                        """
                        SELECT snapshot_date::text, deposit_name, balance::int
                        FROM stock_history
                        WHERE product_tiny_id = :tiny_id
                          AND snapshot_date >= :cutoff_date
                        ORDER BY snapshot_date DESC, deposit_name
                        LIMIT 50;
                        """
                    ),
                    {"tiny_id": tiny_id, "cutoff_date": cutoff.date()},
                )
                out["recent_stock_history_7d"] = [
                    {
                        "snapshot_date": r[0],
                        "deposit_name": r[1],
                        "balance": int(r[2]),
                    }
                    for r in history_result.all()
                ]
            except Exception as exc:
                logger.warning("Investigation stock_history query failed", sku=sku, error=str(exc))
                out["recent_stock_history_7d_error"] = str(exc)

        return out


# ---------------------------------------------------------------------------
# Cooldown threshold (hours) and max |delta| considered "small" for the
# cooldown rule. Small drifts within the cooldown window are treated as
# lag oscillation; larger drifts pass through so real drift is still
# corrected (the DEL-VIS-ETIQ-BRNC -102/+79 cases from the audit).
COOLDOWN_HOURS = 12.0
COOLDOWN_MAX_MAGNITUDE = 5


def _is_in_cooldown(
    *,
    delta: int,
    recent_correction: dict[str, Any] | None,
    now: datetime | None = None,
    hours: float = COOLDOWN_HOURS,
    max_magnitude: int = COOLDOWN_MAX_MAGNITUDE,
) -> bool:
    """True when the SKU just had a correction and the current drift is
    small enough to look like lag noise rather than fresh real drift.

    Pair this with :func:`_is_ping_pong` (called first): ping-pong
    catches exact cancellation, cooldown catches the "drift kept
    creeping the same direction" oscillation pattern that ping-pong
    misses (e.g., -1 then -3 then -1 inside an hour for a high-velocity
    SKU where ML's Inventory is consistently 1-3 units behind Tiny's
    NF).

    Returns False when there's no recent correction (no cooldown to
    enforce) or when ``|delta| > max_magnitude`` (drift big enough to
    be probable real drift; let it through even within the window).

    The 12h / 5-unit defaults come from the 2026-06-01 audit: the
    DEL-VIS-ETIQ-BRNC oscillation cluster on 27/05 had |delta| in
    {1, 3, 1} within ~3h, while the real drifts on the same SKU were
    -102 and +79 — well above the threshold.
    """
    if recent_correction is None:
        return False
    if abs(delta) > max_magnitude:
        return False
    now = now or datetime.now(UTC)
    age_hours: float = (now - recent_correction["created_at"]).total_seconds() / 3600.0
    return age_hours <= hours


def _is_ping_pong(*, delta: int, recent_correction: dict[str, Any] | None) -> bool:
    """True when applying ``delta`` would cancel a recent correction
    for the same SKU.

    The ping-pong pattern observed in the 2026-06-01 audit: ML's
    Inventory API lags a Tiny NF by hours, so our cron applies a
    +N correction; later ML catches up and the cron applies a
    matching -N, leaving Tiny temporarily wrong. To stop the
    ping-pong we refuse the second leg.

    Three conditions, all must hold:

    - A previous correction exists for the SKU in the last 48h.
    - It pointed in the opposite direction (sign flip).
    - The sum of the two deltas is within ±1 of zero — i.e. this
      correction would essentially cancel the previous one.

    The ±1 tolerance handles a sale or two that landed between the
    pair (real drift) without losing the ping-pong signal. If real
    drift accumulates beyond 1 unit, the deltas won't cancel and the
    correction proceeds normally.
    """
    if recent_correction is None:
        return False
    prev_delta = int(recent_correction.get("delta", 0))
    if prev_delta == 0 or delta == 0:
        return False
    # Sign-flip check: previous and current must be opposite signs.
    if (prev_delta > 0) == (delta > 0):
        return False
    return abs(prev_delta + delta) <= 1


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
