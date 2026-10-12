#!/usr/bin/env bash
# Deploy tiny-mirror sem o GitHub Actions: o mesmo job "Deploy to VPS" do
# .github/workflows/deploy.yml, rodado de uma máquina com SSH root na VPS.
#
# Uso:
#   scripts/deploy.sh --dry-run   # só mostra o que o rsync mudaria na VPS
#   scripts/deploy.sh             # deploy de origin/main
#
# Sempre envia um export limpo de origin/main (git archive), nunca a árvore local.

set -euo pipefail

HOST="${DEPLOY_HOST:-root@212.85.1.135}"
DIR=/opt/tiny-mirror/current
REF="${DEPLOY_REF:-origin/main}"

DRY=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    *) echo "Argumento desconhecido: $arg" >&2; exit 1 ;;
  esac
done

cd "$(git rev-parse --show-toplevel)"
git fetch -q origin
SRC="$(mktemp -d)"
trap 'rm -rf "$SRC"' EXIT
git archive "$REF" | tar -x -C "$SRC"
echo "→ $REF = $(git rev-parse --short "$REF")"

# Mesmos excludes do workflow: .env e .venv da VPS nunca são tocados.
EXCLUDES=(
  --exclude '.git' --exclude '__pycache__' --exclude '*.pyc' --exclude '*.pyo'
  --exclude '.pytest_cache' --exclude '.mypy_cache' --exclude '.ruff_cache'
  --exclude 'tests/' --exclude '.env' --exclude '.env.local' --exclude '*.egg-info'
  --exclude '.venv' --exclude 'htmlcov/' --exclude 'coverage.xml' --exclude '.coverage'
  --exclude '.github/' --exclude '.claude/' --exclude '.mcp.json' --exclude 'audit/'
  --exclude 'webhook_listener.py' --exclude 'webhook_log*.jsonl'
)

# O rsync remoto roda como tiny-deploy, dono dos arquivos como o CI deixava;
# rodar como root quebra o próximo deploy (exit 23 por permissão).
RSYNC=(rsync -rlptzc --no-owner --no-group --delete --itemize-changes
  --rsync-path="sudo -u tiny-deploy rsync" "${EXCLUDES[@]}")

if [[ "$DRY" == "1" ]]; then
  "${RSYNC[@]}" --dry-run "$SRC/" "$HOST:$DIR/"
  exit 0
fi

echo "→ Rsync para $HOST:$DIR ..."
"${RSYNC[@]}" "$SRC/" "$HOST:$DIR/"

echo "→ Dependências, migrações e restart ..."
ssh "$HOST" bash -se <<EOF
set -euo pipefail
sudo -u tiny-deploy -H bash -lc 'cd $DIR && poetry install --only main --no-interaction && ln -sfn /opt/tiny-mirror/.env $DIR/.env && poetry run alembic upgrade head'
systemctl restart tiny-mirror
for i in \$(seq 1 20); do
  if curl -fsS http://127.0.0.1:8001/health; then echo; exit 0; fi
  sleep 2
done
echo "Serviço não respondeu ao /health" >&2
journalctl -u tiny-mirror --no-pager -n 50
exit 1
EOF
echo "✓ Deploy concluído"
