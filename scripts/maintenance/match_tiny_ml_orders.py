"""Cruza orders Tiny ↔ ML pra mapear o ML order_id real que o Tiny perdeu.

Contexto: a partir de ~15/05/2026 o Tiny passou a importar pedidos do canal
Mercado Livre FULL sem preservar o ML order_id (numeroPedidoCanalVenda vazio,
numeroPedidoEcommerce no formato fake "2000013*"). Ver docs/01_*.md.

Este script faz best-effort match via chave composta (date BRT + total_amount).
Quando há ambiguidade no mesmo bucket, usa CPF/CNPJ parcial + nome.

Saída: CSV com (tiny_id, tiny_numero, ml_real_order_id, match_confidence).

Uso:
    python scripts/maintenance/match_tiny_ml_orders.py \\
        --days 30 \\
        --output /tmp/matched_orders.csv \\
        [--include-pre-15may]   # incluir período pré-bug pra validação

Read-only. Nunca modifica nada.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime, timedelta

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


def http_get(url: str, headers: dict, timeout: int = 30, retries: int = 2):
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(2**attempt)
                continue
            return e.code, None
        except Exception:
            if attempt < retries:
                time.sleep(1)
                continue
            return None, None
    return None, None


def psql(sql: str) -> str:
    return subprocess.check_output(
        ["sudo", "-u", "postgres", "psql", "-d", "tiny_mirror_db", "-A", "-F\t", "-c", sql]
    ).decode()


# ---------- Normalization ----------


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return "".join(c.upper() for c in s if c.isalnum())


def cpf_tail(cpf: str) -> str:
    """Last 4 digits of CPF/CNPJ, stripped."""
    if not cpf:
        return ""
    digits = re.sub(r"\D", "", cpf)
    return digits[-4:] if len(digits) >= 4 else digits


# ---------- Pulls ----------


def fetch_ml_orders(seller: str, token: str, days: int) -> list[dict]:
    now_utc = datetime.now(UTC)
    today_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    date_from = (today_midnight - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.000-00:00")
    date_to = today_midnight.strftime("%Y-%m-%dT%H:%M:%S.000-00:00")

    out = []
    offset = 0
    while True:
        params = urllib.parse.urlencode(
            {
                "seller": seller,
                "order.date_created.from": date_from,
                "order.date_created.to": date_to,
                "limit": 50,
                "offset": offset,
                "sort": "date_desc",
            }
        )
        code, body = http_get(
            f"https://api.mercadolibre.com/orders/search?{params}",
            {"Authorization": f"Bearer {token}"},
        )
        if code != 200 or not body:
            break
        results = body.get("results", [])
        if not results:
            break
        out.extend(results)
        if len(results) < 50:
            break
        offset += 50
        if offset > 20000:
            break
    return out


def fetch_db_fl_orders(days: int) -> list[dict]:
    sql = f"""
        SELECT
          o.tiny_id, o.order_number, o.ecommerce_order_number,
          o.order_date::text,
          o.total_order_value::numeric::text,
          o.customer->>'nome' AS customer_name,
          o.customer->>'cpfCnpj' AS cpf,
          o.carrier->>'codigoRastreamento' AS tracking,
          o.situation
        FROM orders o
        WHERE o.ecommerce_name = 'Mercado Livre FULL'
          AND o.order_date >= CURRENT_DATE - INTERVAL '{days} days'
          AND o.order_date < CURRENT_DATE;
    """
    rows = []
    for ln in psql(sql).strip().split("\n"):
        if not ln or ln.startswith("(") or ln.startswith("tiny_id"):
            continue
        parts = ln.split("\t")
        if len(parts) >= 9:
            rows.append(
                {
                    "tiny_id": parts[0],
                    "tiny_num": parts[1],
                    "ec_order_num": parts[2],
                    "order_date": parts[3],
                    "total": parts[4],
                    "customer_name": parts[5],
                    "cpf": parts[6],
                    "tracking": parts[7],
                    "situation": parts[8],
                }
            )
    return rows


# ---------- Matching ----------


def index_ml(orders: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """Index ML orders by (date_brt, total_amount_rounded)."""
    idx = defaultdict(list)
    for o in orders:
        date_brt = o["date_created"][:10]  # ML retorna em -04 (BRT) já
        total = round(float(o.get("total_amount") or 0), 2)
        idx[(date_brt, f"{total:.2f}")].append(
            {
                "id": o["id"],
                "status": o.get("status"),
                "buyer_id": (o.get("buyer") or {}).get("id"),
                "buyer_nick": (o.get("buyer") or {}).get("nickname") or "",
                "first_name": (o.get("buyer") or {}).get("first_name") or "",
                "last_name": (o.get("buyer") or {}).get("last_name") or "",
            }
        )
    return idx


def best_match(db: dict, candidates: list[dict]) -> tuple[dict | None, str]:
    """Returns (chosen_ml_order, confidence_label)."""
    if not candidates:
        return None, "no_candidates"
    if len(candidates) == 1:
        return candidates[0], "single_match"

    # Multiple candidates same (date, total). Try to disambiguate via name parts.
    db_name_norm = norm_name(db["customer_name"])
    best = None
    best_score = 0
    for c in candidates:
        ml_first = norm_name(c.get("first_name", ""))
        ml_last = norm_name(c.get("last_name", ""))
        ml_nick = norm_name(c.get("buyer_nick", ""))
        score = 0
        if ml_first and ml_first in db_name_norm:
            score += 2
        if ml_last and ml_last in db_name_norm:
            score += 2
        if ml_nick and ml_nick in db_name_norm:
            score += 1
        if score > best_score:
            best, best_score = c, score
    if best and best_score >= 2:
        return best, f"name_match_{best_score}"
    return candidates[0], "ambiguous_picked_first"


# ---------- Main ----------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--output", default="/tmp/matched_orders.csv")
    args = ap.parse_args()

    ml_token = (
        subprocess.check_output(
            [
                "sudo",
                "-u",
                "postgres",
                "psql",
                "-d",
                "tiny_mirror_db",
                "-At",
                "-c",
                "SELECT access_token FROM ml_oauth_tokens ORDER BY updated_at DESC LIMIT 1",
            ]
        )
        .decode()
        .strip()
    )
    seller = env_var("/opt/tiny-mirror/current/.env", "ML_USER_ID")
    if not seller or not ml_token:
        print("ERROR: ML_USER_ID / ml token not found", file=sys.stderr)
        return 1

    print(f"[1/3] Fetching ML orders {args.days}d...", file=sys.stderr)
    ml_orders = fetch_ml_orders(seller, ml_token, args.days)
    print(f"  Got {len(ml_orders)}", file=sys.stderr)

    print(f"[2/3] Fetching DB FL orders {args.days}d...", file=sys.stderr)
    db_orders = fetch_db_fl_orders(args.days)
    print(f"  Got {len(db_orders)}", file=sys.stderr)

    print("[3/3] Matching...", file=sys.stderr)
    ml_idx = index_ml(ml_orders)

    stats = defaultdict(int)
    matched = []
    for db in db_orders:
        # Skip if Tiny already has a real ML id (2000016*) — usefull pra validar antes/depois
        ec_id = db["ec_order_num"] or ""
        had_real = ec_id.startswith("2000016")
        total = round(float(db["total"] or 0), 2)
        key = (db["order_date"], f"{total:.2f}")
        candidates = ml_idx.get(key, [])
        ml_order, confidence = best_match(db, candidates)
        stats[confidence] += 1
        matched.append(
            {
                "tiny_id": db["tiny_id"],
                "tiny_num": db["tiny_num"],
                "ec_order_num_in_db": ec_id,
                "had_real_ml_id": had_real,
                "order_date": db["order_date"],
                "total": db["total"],
                "customer_name": db["customer_name"],
                "ml_order_id": ml_order["id"] if ml_order else "",
                "ml_buyer_nick": ml_order.get("buyer_nick", "") if ml_order else "",
                "ml_status": ml_order.get("status", "") if ml_order else "",
                "match_confidence": confidence,
            }
        )

    with open(args.output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(matched[0].keys()) if matched else [])
        w.writeheader()
        for row in matched:
            w.writerow(row)

    print(f"\nWrote {len(matched)} rows to {args.output}", file=sys.stderr)
    print("\nMatch confidence breakdown:", file=sys.stderr)
    for label, count in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"  {label}: {count}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
