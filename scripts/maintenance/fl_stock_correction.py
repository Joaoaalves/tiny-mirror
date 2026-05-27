"""Correção do estoque do depósito 'Full Mercado Livre' no Tiny.

Lê a fonte da verdade do nosso DB (stock_deposits, alimentada pelo cron
ML-only de 15 min) e gera operações no Tiny pra cada produto cujo saldo
no depósito FL difere.

DOIS MODOS DE OPERAÇÃO:

  --mode delta (default, SEGURO)
    Para cada SKU com diff:
      - se db > tiny: POST tipo="E" com quantidade=(db - tiny)
      - se db < tiny: POST tipo="S" com quantidade=(tiny - db)
    Reusa o `record_stock_movement` que já está em produção no
    fulfillment_transfer_service.py (testado).

  --mode balance (BALANÇO, NOVO)
    POST tipo="B" com quantidade=db (saldo absoluto desejado).
    Operação mais limpa no histórico mas SEM PRECEDENTE no código —
    payload exato a confirmar contra doc Tiny v3 oficial.

Observação fixa em ambos os modos:
    "[AUTO - Correção automática estoque Fulfillment]"

Tiny NÃO permite delete de operações de estoque. Por isso:
  - default = dry-run (não POSTa)
  - --apply exige confirmação no terminal ("APLICAR" em maiúsculas)
  - --max default = 10 (batch pequeno pra detectar erros cedo)
  - --force-large pra incluir |delta| > 50 (precisa ser explícito)

Uso:
    # Default: dry-run com modo delta
    python scripts/maintenance/fl_stock_correction.py

    # Aplica até 10 correções com modo delta (E/S)
    python scripts/maintenance/fl_stock_correction.py --apply

    # Aplica modo balanço (tipo=B)
    python scripts/maintenance/fl_stock_correction.py --apply --mode balance

    # Aplica todos os 183 SKUs
    python scripts/maintenance/fl_stock_correction.py --apply --max 250 --force-large

    # Exporta dry-run CSV
    python scripts/maintenance/fl_stock_correction.py --csv /tmp/fl_corrections.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime

CORRECTION_MESSAGE = "[AUTO - Correção automática estoque Fulfillment]"
FULL_ML_DEPOSITO_ID = 912048995
LARGE_DELTA_THRESHOLD = 50  # exige --force-large pra processar SKUs com diff maior


# ---------- IO helpers ----------


def env_var(path: str, key: str) -> str | None:
    try:
        with open(path) as f:
            for line in f:
                if line.startswith(key + "="):
                    return line[len(key) + 1 :].strip().strip('"').strip("'")
    except FileNotFoundError:
        return None
    return None


def http_request(method: str, url: str, headers: dict, body: dict | None = None, timeout: int = 30):
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers = {**headers, "Content-Type": "application/json"}
    req = urllib.request.Request(url, headers=headers, data=data, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            try:
                return r.status, json.load(r)
            except json.JSONDecodeError:
                return r.status, None
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        return None, str(e)


def psql(sql: str) -> str:
    return subprocess.check_output(
        ["sudo", "-u", "postgres", "psql", "-d", "tiny_mirror_db", "-A", "-F\t", "-c", sql]
    ).decode()


# ---------- Domain ----------


@dataclass
class Correction:
    product_tiny_id: int
    sku: str
    description: str
    db_qty: int  # nossa truth (= ML real)
    tiny_qty: int  # o que o Tiny pensa que tem
    delta: int  # db - tiny (positivo = Tiny precisa SUBIR)


def load_db_truth(base_only: bool = False) -> list[tuple[int, str, str, int]]:
    """Retorna [(product_tiny_id, sku, description, available_full)] do stock_deposits.

    Exclui SKUs de teste sempre. Se base_only=True, também exclui:
      - kits (padrão XU- onde X é um ou mais dígitos: 2U-, 5U-, 10U-, 100U-)
      - combos (COM-*)
      - kits explícitos (KIT-*)
      - produtos com componentes em product_kit_components
    """
    base_filter = ""
    if base_only:
        base_filter = """
          AND p.sku !~ '^[0-9]+U-'
          AND p.sku NOT LIKE 'COM-%'
          AND p.sku NOT LIKE 'KIT-%'
          AND NOT EXISTS (
            SELECT 1 FROM product_kit_components kc
            WHERE kc.kit_product_tiny_id = p.tiny_id
          )
        """
    rows = []
    for ln in (
        psql(f"""
        SELECT sd.product_tiny_id, p.sku, p.description, sd.available::int
        FROM stock_deposits sd
        JOIN products p ON p.tiny_id = sd.product_tiny_id
        WHERE sd.deposit_name = 'Full Mercado Livre'
          AND p.situation = 'A'
          AND p.sku IS NOT NULL
          AND p.sku <> ''
          AND p.sku NOT LIKE 'SKU-TEST%'
          {base_filter};
    """)
        .strip()
        .split("\n")
    ):
        if not ln or ln.startswith("(") or ln.startswith("product_tiny_id"):
            continue
        p = ln.split("\t")
        if len(p) >= 4:
            try:
                rows.append((int(p[0]), p[1], p[2], int(p[3])))
            except ValueError:
                continue
    return rows


def get_tiny_full_qty(tiny_token: str, product_tiny_id: int) -> int | None:
    """Retorna o SALDO FÍSICO (não o disponível) do depósito Full Mercado Livre.

    Importante: Tiny distingue
      saldo     = quantidade física no depósito
      reservado = unidades vendidas mas ainda não enviadas (pending)
      disponivel = saldo - reservado

    Pra correção de balanço, queremos alinhar o saldo físico com o ML real
    (que já desconta reservas do lado do ML). Comparar `disponivel` levaria
    a falsos diffs sempre que houver venda em processamento no Tiny.
    """
    code, body = http_request(
        "GET",
        f"https://api.tiny.com.br/public-api/v3/estoque/{product_tiny_id}",
        {"Authorization": f"Bearer {tiny_token}", "Accept": "application/json"},
    )
    if code != 200 or not isinstance(body, dict):
        return None
    for d in body.get("depositos", []):
        if d.get("nome") == "Full Mercado Livre":
            return int(d.get("saldo") or 0)
    return None


def apply_delta(tiny_token: str, c: Correction) -> tuple[bool, str]:
    """Modo DELTA: usa tipo=E (entrada) ou S (saída) com a diferença.

    Reusa o pattern de fulfillment_transfer_service.py mas com
    ``precoUnitario=0`` — convenção de balanço/correção. Tiny não atualiza
    o precoCustoMedio nessa operação, mantendo a auditoria neutra.
    """
    if c.delta > 0:
        tipo = "E"  # entrada (precisa SUBIR)
        qty = c.delta
    else:
        tipo = "S"  # saída (precisa DESCER)
        qty = abs(c.delta)
    observacoes = (
        f"{CORRECTION_MESSAGE} Origem: ML API via tiny-sync. "
        f"Snapshot: {datetime.now(UTC).isoformat(timespec='seconds')}. "
        f"ML qty={c.db_qty}, Tiny anterior={c.tiny_qty}, delta={c.delta:+d}."
    )
    payload = {
        "deposito": {"id": FULL_ML_DEPOSITO_ID},
        "tipo": tipo,
        "quantidade": qty,
        "precoUnitario": 0,  # correção de balanço — neutro pro custo médio
        "data": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        "observacoes": observacoes,
    }
    code, body = http_request(
        "POST",
        f"https://api.tiny.com.br/public-api/v3/estoque/{c.product_tiny_id}",
        {"Authorization": f"Bearer {tiny_token}"},
        body=payload,
    )
    if code in (200, 201):
        return True, f"ok HTTP {code} (tipo={tipo}, qty={qty})"
    return False, f"HTTP {code}: {body}"


def apply_balance(tiny_token: str, c: Correction) -> tuple[bool, str]:
    """Modo BALANÇO: usa tipo=B com quantidade absoluta = saldo desejado.

    Payload exato a confirmar contra doc Tiny v3 oficial. Sem precedente no
    código atual — USAR COM CUIDADO. Recomendado testar em 1 SKU primeiro.
    """
    observacoes = (
        f"{CORRECTION_MESSAGE} Origem: ML API via tiny-sync. "
        f"Snapshot: {datetime.now(UTC).isoformat(timespec='seconds')}. "
        f"ML qty={c.db_qty}, Tiny anterior={c.tiny_qty}, delta={c.delta:+d}. "
        f"Operação: balanço (saldo final = {c.db_qty})."
    )
    payload = {
        "deposito": {"id": FULL_ML_DEPOSITO_ID},
        "tipo": "B",
        "quantidade": c.db_qty,
        "precoUnitario": 0,  # balanço — neutro pro custo médio
        "data": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        "observacoes": observacoes,
    }
    code, body = http_request(
        "POST",
        f"https://api.tiny.com.br/public-api/v3/estoque/{c.product_tiny_id}",
        {"Authorization": f"Bearer {tiny_token}"},
        body=payload,
    )
    if code in (200, 201):
        return True, f"ok HTTP {code} (tipo=B, qty={c.db_qty})"
    return False, f"HTTP {code}: {body}"


# ---------- Main ----------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="POSTar mudanças (default: dry-run)")
    ap.add_argument(
        "--mode",
        choices=["delta", "balance"],
        default="balance",
        help="balance = B (default, validado 2026-05-27 com idLanc 972127755); "
        "delta = E/S (fallback se balance falhar em algum SKU)",
    )
    ap.add_argument("--max", type=int, default=10, help="Máximo de SKUs a processar")
    ap.add_argument(
        "--force-large",
        action="store_true",
        help=f"Processar SKUs com |delta| > {LARGE_DELTA_THRESHOLD}",
    )
    ap.add_argument("--only-sku", help="Aplicar APENAS neste SKU (pra teste em 1 produto)")
    ap.add_argument(
        "--base-only",
        action="store_true",
        help="Exclui kits (XU-), combos (COM-), KIT-* e produtos com kit_components",
    )
    ap.add_argument("--csv", help="Exportar todos os deltas pra CSV")
    args = ap.parse_args()

    tiny_token = env_var("/root/.openclaw/.env", "TINY_V3_ACCESS_TOKEN")
    if not tiny_token:
        print("ERROR: TINY_V3_ACCESS_TOKEN não encontrado em /root/.openclaw/.env", file=sys.stderr)
        return 1

    print(f"[1/3] Loading our DB FL stock truth (base_only={args.base_only})...", file=sys.stderr)
    db_rows = load_db_truth(base_only=args.base_only)
    if args.only_sku:
        db_rows = [r for r in db_rows if r[1] == args.only_sku]
        if not db_rows:
            print(f"ERROR: SKU {args.only_sku} não encontrado", file=sys.stderr)
            return 1
    print(f"  {len(db_rows)} products with FL deposit row", file=sys.stderr)

    print("[2/3] Comparing vs Tiny v3 (1 GET per product, sleep 0.5s)...", file=sys.stderr)
    corrections: list[Correction] = []
    for i, (tid, sku, desc, db_qty) in enumerate(db_rows):
        tiny_qty = get_tiny_full_qty(tiny_token, tid)
        if tiny_qty is None:
            print(f"  ⚠ {sku} (tid {tid}): no FL deposit row in Tiny", file=sys.stderr)
            continue
        delta = db_qty - tiny_qty
        if delta != 0:
            corrections.append(Correction(tid, sku, desc, db_qty, tiny_qty, delta))
        time.sleep(0.5)
        if (i + 1) % 25 == 0:
            print(f"  scanned {i+1}/{len(db_rows)}", file=sys.stderr)

    corrections.sort(key=lambda c: -abs(c.delta))

    print(f"\n[3/3] {'APPLY MODE' if args.apply else 'DRY-RUN'} | mode={args.mode} — corrections:")
    print(f"  Total products needing balance: {len(corrections)}")
    print(f"\n  {'SKU':<30} {'tiny→db':<10} {'delta':<8} {'description':<60}")
    print(f"  {'-'*30} {'-'*10} {'-'*8} {'-'*60}")
    for c in corrections:
        flag = " ⚠LARGE" if abs(c.delta) > LARGE_DELTA_THRESHOLD else ""
        print(
            f"  {c.sku:<30} {c.tiny_qty:>3}→{c.db_qty:<5} {c.delta:+<+8} {c.description[:60]}{flag}"
        )

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["product_tiny_id", "sku", "description", "tiny_qty", "db_qty", "delta"])
            for c in corrections:
                w.writerow([c.product_tiny_id, c.sku, c.description, c.tiny_qty, c.db_qty, c.delta])
        print(f"\nCSV: {args.csv}")

    if not args.apply:
        print("\n(dry-run — nada foi enviado ao Tiny)")
        print("Pra aplicar com modo delta (proven): --apply --max <N>")
        print("Pra aplicar com modo balanço (novo): --apply --mode balance --max <N>")
        print("Pra testar em 1 SKU específico: --apply --only-sku <SKU>")
        return 0

    # Apply mode
    to_apply = [c for c in corrections if abs(c.delta) <= LARGE_DELTA_THRESHOLD or args.force_large]
    to_apply = to_apply[: args.max]
    if not to_apply:
        print("\nNada elegível pra aplicar (use --force-large pra incluir deltas grandes)")
        return 0

    print(f"\n⚠ Vai aplicar em {len(to_apply)} SKUs no Tiny — modo={args.mode}")
    print("⚠ Tiny não permite delete. As operações ficarão no histórico permanente.")
    print(f"Mensagem: {CORRECTION_MESSAGE}")
    confirm = input("\nDigite 'APLICAR' (em maiúsculas) pra prosseguir: ")
    if confirm != "APLICAR":
        print("Cancelado.")
        return 1

    apply_fn = apply_balance if args.mode == "balance" else apply_delta
    success = 0
    failed = 0
    for c in to_apply:
        ok, msg = apply_fn(tiny_token, c)
        if ok:
            print(f"  ✓ {c.sku}: {c.tiny_qty} → {c.db_qty} ({msg})")
            success += 1
        else:
            print(f"  ✗ {c.sku}: FAILED ({msg})")
            failed += 1
        time.sleep(1.5)

    print(f"\nResultado: {success} sucesso, {failed} falha")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
