# CLAUDE.md — tiny-mirror

Espelho do Tiny ERP + integrações Mercado Livre (FastAPI + Postgres + Redis + RabbitMQ).
**Este código roda em PRODUÇÃO** na VPS (serviço systemd `tiny-mirror`, uvicorn em
`127.0.0.1:8001`, exposto em `https://dash.offshop.work/tiny-mirror/` via nginx). Trate
qualquer mudança como mudança de produção. ⚠️ `erp.offshop.work` NÃO é este serviço: é o
ERP .NET (`erp-api@8080`), sem consumidores — não usar.

## Regras INVIOLÁVEIS (segurança operacional)

1. **NUNCA escrever no Tiny ERP.** Nem "teste controlado". Validação é só leitura +
   espelho. Quem dispara ação no Tiny é o operador pela UI. Existe kill-switch
   `TINY_STOCK_WRITES_ENABLED` — não o ligue.
2. **NUNCA executar escrita no Mercado Livre** (promoções, migrações, enrolls).
   "Adicionar/mudar a migração" = construir o recurso, NÃO rodá-lo. `dry_run` ok;
   `dry_run=false` é só o operador pela UI.
3. **NUNCA editar `/opt/tiny-mirror/current/` na VPS diretamente.** É o deploy de
   produção. Todo trabalho acontece num clone do repo.
4. **Segredos vivem só em `/opt/tiny-mirror/.env` na VPS.** Nunca commitar tokens,
   cookies ou `.env` — `.env.example` é a referência do que existe. Exceção: credenciais
   de marketplaces (Amazon SP-API, Shopee) são do **OpenClaw**; um cron root
   (`deploy/export_marketplace_credentials.py`, instalado em `/usr/local/bin`) exporta uma
   cópia só-leitura para `/opt/tiny-mirror/marketplace-credentials.json`.
5. **Outros marketplaces (Amazon, Shopee): só leitura (GET).** O tiny-mirror **NUNCA
   renova o token da Shopee** (o refresh token gira a cada uso; quem renova é o OpenClaw).

## Fluxo de trabalho

- **1 branch por mudança. NUNCA commitar direto na `main`.**
- Commits: **Conventional Commits em inglês** (`feat(...):`, `fix(...):`). Sem
  `Co-Authored-By: Claude`.
- Terminou: push da branch + PR (`gh pr create`). **Merge e deploy são conduzidos
  pelo João Alves** — não faça merge na main nem deploy por conta própria.
- **Merge na `main` = deploy.** O CI (`.github/workflows/deploy.yml`) faz `rsync --delete`
  da `main` inteira para `/opt/tiny-mirror/current/` + `alembic upgrade head` + restart.
  Hotfix aplicado só em produção é REVERTIDO no próximo merge — antes de mergear, simular
  (`rsync -n -c --delete` de um `git archive` da branch contra a VPS) e conferir que só
  mudam os arquivos do PR. Deploy manual (arquivo→arquivo de TODOS os arquivos tocados)
  só com `[skip ci]` no merge. Deploy parcial = drift = incidente.

## Toolchain e gates (pre-commit espelha o CI)

```bash
poetry install            # Python 3.12
poetry run pre-commit install
```

Todo commit passa por: `ruff` + `ruff-format` + `mypy src/tiny_mirror
--ignore-missing-imports` + suíte unit com gate de cobertura:

```bash
poetry run pytest tests/unit -m unit --cov=src/tiny_mirror --cov-fail-under=35 -q
```

- **Toda mudança traz seus próprios unit tests** (marcados `pytest.mark.unit`, HTTP
  externo sempre mockado) **e seus e2e** — cada etapa escreve os dela, não deixe
  "pro final".
- E2E (`tests/e2e`) roda contra infra docker viva e é gateado por
  `E2E_TINY_ACCESS_TOKEN` (sem a env var, skip silencioso — CI seguro).
- `tests/integration` é deliberadamente vazio.

## Arquitetura (mapa rápido)

- `src/tiny_mirror/main.py` — lifespan monta clients/serviços em `app.state`; routers
  em `create_app()` (tudo protegido por `X-API-Key` via `verify_api_key`, exceto
  `/health` e `/webhooks`).
- `src/tiny_mirror/services/` — lógica de negócio. Padrão ML: serviços recebem
  `MercadoLivreTokenService` + `httpx.AsyncClient`; toda chamada ML usa
  `get_valid_access_token()` e, em 401, `handle_unauthorized()` + 1 retry.
- `src/tiny_mirror/api/routers/` — rotas finas; serviço resolvido de `app.state`
  via dependency (`_service_dep`), 503 se a integração estiver desligada.
- `src/tiny_mirror/scheduler/jobs.py` — crons APScheduler; `src/tiny_mirror/queue/`
  — consumers RabbitMQ. Consumer novo = sincronizar bootstrap + topology + publisher;
  `sync_type` novo = migração para o CHECK constraint.
- `alembic/versions/` — migrações de schema.
- Peculiaridades Tiny: `dataAtualizacao` só aceita `YYYY-MM-DD` e significa "atualizado
  DESDE a data"; excluir NF antiga no Tiny zera o `idNotaFiscal` do pedido e o "atualiza"
  (tempestade de 30/09 → corte por idade no sync). **Webhook de situação de pedido do
  Tiny NÃO é usado** (não chega/não é confiável): mudança de situação é detectada pelo
  sync incremental comparando a `situacao` da listagem com o espelho. Pedido do Tiny não
  tem hora (v2 e v3) — hora exata vem do ML (`v_order_datetime`). Client já retenta
  status transientes sob um budget único de retries.

## Contexto de equipe

- Frontend (leon-mission-control) consome esta API; o agente Dante (ads) usa as rotas
  `/ads/*`; o OpenClaw lê o Postgres como `tiny_readonly`.
- Dúvida sobre intenção de produto ou algo que exija mexer em produção → parar e
  perguntar ao João Alves antes.
