#!/usr/bin/env bash
# Local gate before push: same checks as CI (ruff + pytest + dbt parse + sqlfluff).
set -euo pipefail
cd "$(dirname "$0")/.."

echo '==> ruff check .'
ruff check .

echo '==> pytest -q'
pytest -q

echo '==> dbt parse'
(cd airflow/dags/dbt && dbt parse --profiles-dir .)

echo '==> sqlfluff lint dbt models'
(cd airflow/dags/dbt && sqlfluff lint models --dialect snowflake)

echo 'OK — safe to push'
