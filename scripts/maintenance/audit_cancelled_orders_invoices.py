"""Audit Hipótese 1: pedidos cancelados que ficaram com NF ativa.

Quando um pedido FL é cancelado no ML após o Tiny ter salvado a NF (gerando
baixa no estoque), se a NF não for cancelada o estoque fica permanentemente
descontado. Esse script lista todos os candidatos pra revisão.

Read-only. Lista pra revisão manual no painel Tiny.

Uso:
    python scripts/maintenance/audit_cancelled_orders_invoices.py [--days 30]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def env_var(path: str, key: str) -> str | None:
    try:
        with open(path) as f:
            for line in f:
                if line.startswith(key + "="):
                    return line[len(key) + 1 :].strip().strip('"').strip("'")
    except FileNotFoundError:
        return None
    return None


def http_get(url: str, headers: dict, timeout: int = 20):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception:
        return None, None


def psql(sql: str) -> str:
    return subprocess.check_output(
        ["sudo", "-u", "postgres", "psql", "-d", "tiny_mirror_db", "-A", "-F\t", "-c", sql]
    ).decode()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    args = ap.parse_args()

    tiny_token = env_var("/root/.openclaw/.env", "TINY_V3_ACCESS_TOKEN")
    if not tiny_token:
        print("ERROR: TINY_V3_ACCESS_TOKEN not found", file=sys.stderr)
        return 1

    print(
        f"[1/2] Loading FL orders cancelled in last {args.days}d with invoice_id...",
        file=sys.stderr,
    )
    # Tiny situação values:
    #   1=Em aberto, 2=Aprovado, 3=Preparando envio, 4=Faturado, 5=Pronto p/ envio,
    #   6=Enviado, 7=Atendido (entregue), 8=Não entregue, 9=Cancelado
    rows = []
    for ln in (
        psql(f"""
        SELECT o.tiny_id, o.order_number, o.invoice_id, o.order_date, o.situation,
               o.customer->>'nome' AS cust,
               o.total_order_value::numeric::text,
               oi.product_sku, oi.quantity::int
        FROM orders o
        JOIN order_items oi ON oi.order_tiny_id = o.tiny_id
        WHERE o.ecommerce_name = 'Mercado Livre FULL'
          AND o.invoice_id IS NOT NULL
          AND o.situation = 9   -- Cancelado
          AND o.order_date >= CURRENT_DATE - INTERVAL '{args.days} days'
        ORDER BY o.order_date DESC;
    """)
        .strip()
        .split("\n")
    ):
        if not ln or ln.startswith("(") or ln.startswith("tiny_id"):
            continue
        p = ln.split("\t")
        if len(p) >= 9:
            rows.append(
                {
                    "tiny_id": p[0],
                    "tiny_num": p[1],
                    "invoice_id": p[2],
                    "order_date": p[3],
                    "situation": p[4],
                    "cust": p[5],
                    "total": p[6],
                    "sku": p[7],
                    "qty": int(p[8]),
                }
            )

    print(f"  Found {len(rows)} cancelled-with-invoice candidates", file=sys.stderr)
    if not rows:
        print("\nNenhum candidato — Hipótese 1 não está ativa ou tudo OK.")
        return 0

    print("\n[2/2] Checking invoice status in Tiny v3 (parallel 5)...", file=sys.stderr)

    def check_nf(invoice_id):
        code, body = http_get(
            f"https://api.tiny.com.br/public-api/v3/notas-fiscais/{invoice_id}",
            {"Authorization": f"Bearer {tiny_token}", "Accept": "application/json"},
        )
        if code != 200 or not isinstance(body, dict):
            return invoice_id, None, f"HTTP {code}"
        return invoice_id, body.get("situacao"), body.get("dataCancelamento")

    nf_status = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(check_nf, r["invoice_id"]): r["invoice_id"] for r in rows}
        for fut in as_completed(futures):
            inv_id, situ, cancel_dt = fut.result()
            nf_status[inv_id] = (situ, cancel_dt)

    # Tiny NF situação:
    #   1=Pendente, 2=Emitida, 3=Cancelada, 4=Denegada, 5=Inutilizada
    leaked = []  # pedido cancelado + NF NÃO cancelada (= estoque vazou)
    correct = []  # pedido cancelado + NF cancelada (= OK)
    unknown = []  # NF não encontrada / erro

    for r in rows:
        st = nf_status.get(r["invoice_id"], (None, None))
        situ = st[0]
        if situ is None:
            unknown.append(r)
        elif situ == 3:  # Cancelada
            correct.append(r)
        else:
            leaked.append({**r, "nf_situacao": situ})

    print("\n📊 Resultado:")
    print(f"  Pedidos cancelados com NF ATIVA (estoque vazou): {len(leaked)}")
    print(f"  Pedidos cancelados com NF cancelada (OK):        {len(correct)}")
    print(f"  NF não encontrada / erro:                        {len(unknown)}")

    if leaked:
        total_vazado = sum(r["qty"] for r in leaked)
        print(f"\n=== ESTOQUE VAZADO: {len(leaked)} casos, {total_vazado} unidades ===")
        print(
            f"{'tiny_num':<10} {'invoice':<10} {'date':<12} {'sku':<28} {'qty':<5} {'NF_situ':<8} {'cliente'}"
        )
        for r in leaked[:50]:
            print(
                f"{r['tiny_num']:<10} {r['invoice_id']:<10} {r['order_date']:<12} {r['sku']:<28} {r['qty']:<5} {r['nf_situacao']:<8} {r['cust'][:30]}"
            )
        if len(leaked) > 50:
            print(f"... mais {len(leaked) - 50} casos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
