"""Repository / flow integration coverage lives in ``tests/e2e/``.

Stage 13 originally divided tests into unit / integration (testcontainers)
/ e2e. We collapsed integration into e2e because the e2e suite already
runs against the same docker-compose Postgres + Redis + RabbitMQ stack
that testcontainers would spin up. See
``memory/project_test_strategy.md`` for the rationale.

Equivalent coverage:
- repository upsert / replace semantics — exercised by every persistence
  test in ``tests/e2e/test_e2e_products.py``, ``test_e2e_orders.py``,
  ``test_e2e_stock.py``, ``test_e2e_sale_buckets.py``.
- full sync flow with kit expansion — ``test_e2e_sale_buckets.py`` runs
  it end to end with synthetic data.
"""

from __future__ import annotations
