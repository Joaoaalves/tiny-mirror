"""Audit phantom products — produtos criados pelo Tiny absorvendo vendas órfãs do ML.

Mecânica do bug:
  Quando um pedido ML chega no Tiny mas o anúncio não tem SELLER_SKU configurado
  (ou o SKU não existe nos produtos cadastrados), o Tiny faz fallback pra busca
  por título. Se não encontra, **cria um produto novo** e associa a venda nele.
  Esse produto absorve a baixa de estoque (drena 1 unidade do "fantasma" no FL)
  enquanto o produto real continua intocado — gerando drift permanente.

Critério usado (alta confiança):
  - Tem >=1 pedido no canal Mercado Livre (alguma venda ocorreu)
  - NÃO tem MLB próprio em ml_listings (nenhum anúncio com esse seller_sku)
  - NÃO é componente de kit com MLB (não vende via expansão de kit)
  - NÃO é kit que contém produto com MLB (não tem componente-MLB conhecido)

Resultado: produto que TEM vendas no ML mas a integração não consegue
mapear (= fantasma criado).

Pra cada candidato, lista também as orders específicas que o tocam — pra
o operador investigar de onde a venda veio (e qual anúncio ML originou).

Read-only.

Uso:
    python scripts/maintenance/audit_phantom_products.py [--csv out.csv] [--with-orders]
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys


def psql(sql: str) -> str:
    return subprocess.check_output(
        ["sudo", "-u", "postgres", "psql", "-d", "tiny_mirror_db", "-A", "-F\t", "-c", sql]
    ).decode()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="Exportar lista de fantasmas pra CSV")
    ap.add_argument(
        "--with-orders",
        action="store_true",
        help="Pra cada fantasma, listar as orders que o tocam",
    )
    args = ap.parse_args()

    sql = """
        WITH order_counts AS (
            SELECT
                oi.product_sku,
                COUNT(DISTINCT oi.order_tiny_id) FILTER (
                    WHERE o.ecommerce_name LIKE 'Mercado Livre%'
                ) AS pedidos_ml,
                SUM(oi.quantity) FILTER (
                    WHERE o.ecommerce_name LIKE 'Mercado Livre%'
                )::int AS units_ml,
                COUNT(DISTINCT oi.order_tiny_id) AS pedidos_total,
                SUM(oi.quantity)::int AS units_total,
                MIN(o.order_date)::text AS primeira_venda,
                MAX(o.order_date)::text AS ultima_venda
            FROM order_items oi
            JOIN orders o ON o.tiny_id = oi.order_tiny_id
            GROUP BY oi.product_sku
        )
        SELECT
            p.tiny_id, p.sku, p.description, p.situation, p.type,
            p.brand_name,
            COALESCE(p.created_at_tiny, p.created_at)::date::text AS criado,
            COALESCE(jsonb_array_length(p.attachments), 0) AS num_fotos,
            COALESCE(oc.pedidos_ml, 0) AS pedidos_ml,
            COALESCE(oc.units_ml, 0) AS units_ml,
            COALESCE(oc.pedidos_total, 0) AS pedidos_total,
            COALESCE(oc.units_total, 0) AS units_total,
            oc.primeira_venda, oc.ultima_venda
        FROM products p
        JOIN order_counts oc ON oc.product_sku = p.sku
        WHERE p.situation = 'A'
          AND oc.pedidos_ml >= 1
          -- 1. Sem MLB próprio
          AND NOT EXISTS (SELECT 1 FROM ml_listings ml WHERE ml.sku = p.sku)
          -- 2. Não é componente de kit com MLB
          AND NOT EXISTS (
            SELECT 1 FROM product_kit_components kc
            JOIN products kp ON kp.tiny_id = kc.kit_product_tiny_id
            JOIN ml_listings ml ON ml.sku = kp.sku
            WHERE kc.component_sku = p.sku
          )
          -- 3. Não é kit que contém produto com MLB
          AND NOT EXISTS (
            SELECT 1 FROM product_kit_components kc
            JOIN ml_listings ml ON ml.sku = kc.component_sku
            WHERE kc.kit_product_tiny_id = p.tiny_id
          )
          AND p.sku NOT LIKE 'SKU-TEST%'
        ORDER BY oc.units_ml DESC NULLS LAST, p.sku
        LIMIT 200;
    """

    rows = []
    for ln in psql(sql).strip().split("\n"):
        if not ln or ln.startswith("(") or ln.startswith("tiny_id"):
            continue
        parts = ln.split("\t")
        if len(parts) >= 14:
            rows.append(parts)

    if not rows:
        print("✓ Nenhum fantasma detectado (critério: vende ML mas sem MLB próprio ou via kit).")
        return 0

    headers = [
        "tiny_id",
        "sku",
        "type",
        "situation",
        "description",
        "brand_name",
        "criado",
        "num_fotos",
        "pedidos_ml",
        "units_ml",
        "pedidos_total",
        "units_total",
        "primeira_venda",
        "ultima_venda",
    ]

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(headers)
            w.writerows(rows)

    total_units_ml = sum(int(r[9]) for r in rows)
    total_pedidos_ml = sum(int(r[8]) for r in rows)

    print(f"\n📊 {len(rows)} CANDIDATOS A FANTASMA (vendendo ML sem MLB conhecido)\n")
    print(f"  Total de pedidos ML absorvidos: {total_pedidos_ml}")
    print(f"  Total de unidades ML drenadas:  {total_units_ml}")
    print(f"  (= estoque do produto REAL ficou {total_units_ml}u acima do ML — drift)")

    print("\n🔥 Top 20 por volume:")
    print(f"  {'tiny_id':<11} {'sku':<28} {'pedidos':<8} {'units':<6} {'criado':<12} descrição")
    print(f"  {'-'*11} {'-'*28} {'-'*8} {'-'*6} {'-'*12} {'-'*40}")
    for r in rows[:20]:
        tiny_id, sku, _t, _s, desc, _b, criado, _f, pedidos, units, _pt, _ut, _pv, _uv = r
        desc_short = desc[:45] + "..." if len(desc) > 48 else desc
        print(f"  {tiny_id:<11} {sku[:27]:<28} {pedidos:<8} {units:<6} {criado:<12} {desc_short}")

    if len(rows) > 20:
        print(f"  ... mais {len(rows)-20} (ver CSV)")

    if args.with_orders:
        print("\n\n📋 ORDERS ESPECÍFICAS por fantasma (top 5 candidatos):\n")
        for r in rows[:5]:
            tiny_id, sku, _t, _s, desc, *_ = r
            print(f"\n  ━━ {sku} (id {tiny_id}) ━━")
            print(f"  {desc[:80]}")
            orders_out = (
                psql(f"""
                SELECT o.tiny_id, o.ecommerce_order_number, o.order_date::text,
                       o.ecommerce_name, oi.quantity::int, o.situation
                FROM order_items oi
                JOIN orders o ON o.tiny_id = oi.order_tiny_id
                WHERE oi.product_sku = '{sku}'
                  AND o.ecommerce_name LIKE 'Mercado Livre%'
                ORDER BY o.order_date DESC
                LIMIT 10;
            """)
                .strip()
                .split("\n")
            )
            print(
                f"    {'tiny_id':<11} {'ec_order':<20} {'data':<12} {'canal':<22} {'qty':<4} situ"
            )
            for ol in orders_out:
                if not ol or ol.startswith("(") or ol.startswith("tiny_id"):
                    continue
                parts = ol.split("\t")
                if len(parts) >= 6:
                    print(
                        f"    {parts[0]:<11} {parts[1][:19]:<20} {parts[2]:<12} {parts[3][:22]:<22} {parts[4]:<4} {parts[5]}"
                    )

    if args.csv:
        print(f"\nCSV: {args.csv}")

    print("\n⚠ Pra cada fantasma confirmado:")
    print("  1. Abrir produto no Tiny e verificar o histórico de pedidos")
    print("  2. Identificar pelo TÍTULO/descrição qual anúncio ML originou")
    print("  3. No painel ML, atualizar SELLER_SKU do anúncio pra apontar pro produto correto")
    print("  4. Inativar o fantasma no Tiny (não excluir — NFs históricas)")
    print("  5. Proximo cron de FL stock correction (1x/hora) alinha o estoque do produto real")

    return 0


if __name__ == "__main__":
    sys.exit(main())
