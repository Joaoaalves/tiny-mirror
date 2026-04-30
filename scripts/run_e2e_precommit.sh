#!/usr/bin/env bash
# Run the end-to-end test suite when real Tiny credentials are available.
#
# This hook is intentionally a no-op when E2E_TINY_ACCESS_TOKEN is not set
# so that contributors without API access can still commit.

set -euo pipefail

if [[ -z "${E2E_TINY_ACCESS_TOKEN:-}" ]]; then
  echo "Skipping E2E tests: E2E_TINY_ACCESS_TOKEN not set"
  exit 0
fi

exec poetry run pytest tests/e2e -m e2e --timeout=30 -q
