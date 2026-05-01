# tiny-mirror

Local mirror of the Tiny ERP catalog and orders, exposed as a small REST API
for internal systems and an in-house LLM. The service syncs products, orders
and stock from the Tiny REST API into Postgres, recomputes per-day x SKU x
channel sale buckets (with kit-component expansion), and serves the data via
HTTP.

## Stack

- Python 3.12, FastAPI, async SQLAlchemy 2.0 + asyncpg
- PostgreSQL 16, Redis 7, RabbitMQ 3.12 (via aio-pika)
- APScheduler for cron jobs, structlog for JSON logging
- Pytest with mock-based unit suite + live e2e suite

## Local development

```bash
poetry install
docker compose up -d                       # postgres + redis + rabbitmq
cp .env.example .env                       # then fill TINY_* values
poetry run alembic upgrade head
poetry run uvicorn src.tiny_mirror.main:app --reload
```

## Tests

```bash
poetry run pytest -m unit                  # ~1s, fully mocked
E2E_TINY_ACCESS_TOKEN=1 \
E2E_TINY_TEST_PRODUCT_ID=... \
E2E_TINY_TEST_KIT_ID=... \
E2E_TINY_TEST_ORDER_ID=... \
poetry run pytest -m e2e                   # ~25s, hits live Tiny + Postgres
```

## Deployment

CI/CD via `.github/workflows/deploy.yml` (lint -> unit tests -> SSH-keyed
rsync to the VPS -> `alembic upgrade head` -> `systemctl restart tiny-mirror`).
VPS layout, NGINX, and read-only Postgres user setup are documented in the
project's deployment runbook.
