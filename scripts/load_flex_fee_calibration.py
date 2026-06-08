"""Populate ml_flex_fee_calibration from a settled-orders calibration CSV.

Reads the raw per-order-item CSV produced by the calibration pull (columns:
mlb,sku,qty,unit_price,sale_fee,logistic_type,seller_freight,...), keeps only
NON-fulfillment (Flex) listings, and computes per MLB:

  real_comm_pct          = median(sale_fee / unit_price * 100)
  freight_per_unit_lt79  = mean(seller_freight / qty) over sub-R$79 sales
  freight_per_unit_ge79  = mean(seller_freight / qty) over >=R$79 sales

Missing bands fall back to the global Flex mean for that band. Writes the whole
table in one transaction (full recompute). Run on the VPS where psql + the CSV
are available:

    python3 scripts/load_flex_fee_calibration.py /tmp/fee_calib_raw.csv
"""

from __future__ import annotations

import csv
import statistics as st
import subprocess
import sys


def f(x: str) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def main(csv_path: str) -> None:
    rows = list(csv.DictReader(open(csv_path)))
    by: dict[str, dict] = {}
    glob_lt: list[float] = []
    glob_ge: list[float] = []
    for r in rows:
        lt = r.get("logistic_type")
        if lt in ("fulfillment", "", None):
            continue  # fulfillment is already correct — never calibrate it
        p, sf, q = f(r["unit_price"]), f(r["sale_fee"]), (f(r["qty"]) or 1)
        if p is None or sf is None or p <= 0:
            continue
        d = by.setdefault(
            r["mlb"],
            {"sku": r.get("sku"), "rates": [], "fr_lt": [], "fr_ge": [], "pb_lt": [], "pb_ge": []},
        )
        d["rates"].append(sf / p * 100)
        fr = f(r["seller_freight"])
        if fr is not None:
            per_unit = fr / q
            pb = (f(r.get("ml_freight_save")) or 0.0) / q  # ML subsidy per unit
            if p >= 79:
                d["fr_ge"].append(per_unit)
                d["pb_ge"].append(pb)
                glob_ge.append(per_unit)
            else:
                d["fr_lt"].append(per_unit)
                d["pb_lt"].append(pb)
                glob_lt.append(per_unit)

    fb_lt = round(st.mean(glob_lt), 2) if glob_lt else 0.0
    fb_ge = round(st.mean(glob_ge), 2) if glob_ge else 0.0
    print(f"flex MLBs: {len(by)} | global fallback freight lt79={fb_lt} ge79={fb_ge}")

    def sql_num(v: float | None) -> str:
        return "NULL" if v is None else f"{v:.2f}"

    values = []
    for mlb, d in by.items():
        comm = round(st.median(d["rates"]), 2) if d["rates"] else None
        fr_lt = round(st.mean(d["fr_lt"]), 2) if d["fr_lt"] else fb_lt
        fr_ge = round(st.mean(d["fr_ge"]), 2) if d["fr_ge"] else fb_ge
        pb_lt = round(st.mean(d["pb_lt"]), 2) if d["pb_lt"] else 0.0
        pb_ge = round(st.mean(d["pb_ge"]), 2) if d["pb_ge"] else 0.0
        sku = (d["sku"] or "").replace("'", "''")
        values.append(
            f"('{mlb}', '{sku}', {len(d['rates'])}, {sql_num(comm)}, "
            f"{sql_num(fr_lt)}, {sql_num(fr_ge)}, {sql_num(pb_lt)}, {sql_num(pb_ge)}, "
            f"{len(d['fr_lt'])}, {len(d['fr_ge'])}, now())"
        )

    sql = (
        "BEGIN;\nDELETE FROM ml_flex_fee_calibration;\n"
        "INSERT INTO ml_flex_fee_calibration "
        "(mlb_id, sku, n_sales, real_comm_pct, freight_per_unit_lt79, "
        "freight_per_unit_ge79, payback_per_unit_lt79, payback_per_unit_ge79, "
        "n_freight_lt79, n_freight_ge79, updated_at) VALUES\n" + ",\n".join(values) + ";\nCOMMIT;\n"
    )
    proc = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-d", "tiny_mirror_db", "-v", "ON_ERROR_STOP=1"],
        input=sql,
        capture_output=True,
        text=True,
    )
    print(proc.stdout.strip())
    if proc.returncode != 0:
        print("ERROR:", proc.stderr.strip())
        sys.exit(1)
    print(f"loaded {len(values)} rows into ml_flex_fee_calibration")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/fee_calib_raw.csv")
