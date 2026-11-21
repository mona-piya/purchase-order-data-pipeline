#!/usr/bin/env bash
# Create Snowflake + Airflow secrets in namespace purchase-orders from local .env / rsa_key.p8.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
NS="${K8S_NAMESPACE:-purchase-orders}"

if [[ ! -f .env ]]; then
  echo "missing .env — copy from .env.example" >&2
  exit 1
fi
if [[ ! -f rsa_key.p8 ]]; then
  echo "missing rsa_key.p8" >&2
  exit 1
fi

kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

# shellcheck disable=SC1091
set -a
source .env
set +a

: "${SNOWFLAKE_ACCOUNT:?}"
: "${SNOWFLAKE_USER:?}"
: "${AIRFLOW__CORE__FERNET_KEY:?}"

kubectl -n "$NS" create secret generic snowflake-creds \
  --from-literal=SNOWFLAKE_ACCOUNT="$SNOWFLAKE_ACCOUNT" \
  --from-literal=SNOWFLAKE_USER="$SNOWFLAKE_USER" \
  --from-literal=SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-}" \
  --from-literal=SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-ACCOUNTADMIN}" \
  --from-literal=SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-PO_WH}" \
  --from-literal=SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-PURCHASE_ORDER_DB}" \
  --from-literal=SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-RAW}" \
  --from-literal=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="${SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:-}" \
  --from-literal=AIRFLOW__CORE__FERNET_KEY="$AIRFLOW__CORE__FERNET_KEY" \
  --from-literal=AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}" \
  --from-literal=AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NS" create secret generic snowflake-key \
  --from-file=rsa_key.p8=./rsa_key.p8 \
  --dry-run=client -o yaml | kubectl apply -f -

echo "secrets applied in namespace $NS"
