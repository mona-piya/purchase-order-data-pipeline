#!/usr/bin/env bash
# Local gate before push: same checks as CI (ruff + pytest).
set -euo pipefail
cd "$(dirname "$0")/.."

echo '==> ruff check .'
ruff check .

echo '==> pytest -q'
pytest -q

echo 'OK — safe to push'
