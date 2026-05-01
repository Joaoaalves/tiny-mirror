"""Translation layer between the Tiny ERP product schema (PT) and ours (EN).

The mapper is intentionally tolerant: any field that the Tiny API may omit
becomes ``None`` instead of an empty string, ``0`` or being silently
filled. The product-sync pipeline depends on this — a NULL slot in the DB
means "Tiny did not send this", which is different from "Tiny sent zero".
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class ProductMapper:
    @staticmethod
    def from_tiny_api(raw: dict[str, Any]) -> dict[str, Any]:
        categoria = raw.get("categoria") or {}
        marca = raw.get("marca") or {}
        produto_pai = raw.get("produtoPai") or {}
        precos = raw.get("precos") or {}
        estoque = raw.get("estoque") or {}
        dimensoes = raw.get("dimensoes")

        dimensions: dict[str, Any] | None
        if dimensoes:
            dimensions = {
                "packaging_type": dimensoes.get("embalagem"),
                "width": dimensoes.get("largura"),
                "height": dimensoes.get("altura"),
                "length": dimensoes.get("comprimento"),
                "diameter": dimensoes.get("diametro"),
                "net_weight": dimensoes.get("pesoLiquido"),
                "gross_weight": dimensoes.get("pesoBruto"),
                "volume_count": dimensoes.get("quantidadeVolumes"),
            }
        else:
            dimensions = None

        prices = (
            {
                "price": precos.get("preco"),
                "promotional_price": precos.get("precoPromocional"),
                "cost_price": precos.get("precoCusto"),
                "average_cost_price": precos.get("precoCustoMedio"),
            }
            if precos
            else {}
        )

        return {
            "tiny_id": int(raw["id"]),
            "sku": raw["sku"],
            "description": raw["descricao"],
            "complementary_description": raw.get("descricaoComplementar"),
            "type": raw["tipo"],
            "situation": raw["situacao"],
            "parent_product_tiny_id": _to_int_or_none(produto_pai.get("id")),
            "unit": raw.get("unidade"),
            "unit_per_box": raw.get("unidadePorCaixa"),
            "ncm": raw.get("ncm"),
            "gtin": raw.get("gtin"),
            "origin": raw.get("origem"),
            "warranty": raw.get("garantia"),
            "observations": raw.get("observacoes"),
            "category_id": _to_int_or_none(categoria.get("id")),
            "category_name": categoria.get("nome"),
            "category_full_path": categoria.get("caminhoCompleto"),
            "brand_id": _to_int_or_none(marca.get("id")),
            "brand_name": marca.get("nome"),
            "dimensions": dimensions,
            "prices": prices,
            "stock_control": estoque.get("controlar"),
            "stock_on_order": estoque.get("sobEncomenda"),
            "stock_preparation_days": estoque.get("diasPreparacao"),
            "stock_location": estoque.get("localizacao"),
            "stock_min": estoque.get("minimo"),
            "stock_max": estoque.get("maximo"),
            "stock_quantity": estoque.get("quantidade"),
            "suppliers": raw.get("fornecedores", []),
            "seo": raw.get("seo"),
            "taxation": raw.get("tributacao"),
            "attachments": raw.get("anexos", []),
            "variation_type": raw.get("tipoVariacao"),
            "created_at_tiny": _parse_iso_utc(raw.get("dataCriacao")),
            "updated_at_tiny": _parse_iso_utc(raw.get("dataAlteracao")),
            "synced_at": datetime.now(UTC),
        }

    @staticmethod
    def extract_kit_components(raw: dict[str, Any]) -> list[dict[str, Any]]:
        if raw.get("tipo") != "K":
            return []
        kit = raw.get("kit") or []
        components: list[dict[str, Any]] = []
        for item in kit:
            produto = item.get("produto") if isinstance(item, dict) else None
            quantidade = item.get("quantidade") if isinstance(item, dict) else None
            if not produto or quantidade is None:
                logger.warning(
                    "Skipping malformed kit component",
                    parent_tiny_id=raw.get("id"),
                    item=item,
                )
                continue
            components.append(
                {
                    "component_product_tiny_id": _to_int_or_none(produto.get("id")),
                    "component_sku": produto.get("sku"),
                    "component_description": produto.get("descricao"),
                    "component_type": produto.get("tipo"),
                    "quantity": float(quantidade),
                }
            )
        return components


def _to_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_utc(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # Normalize trailing Z to +00:00 for fromisoformat (Python <3.11 quirk).
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
