# Purchase Order Data Pipeline

Personal technical exercise: move synthetic purchase orders from a file-backed **source API** through **Kafka** to a **target API** (SQLite) and later to **Snowflake** (RAW → CORE → dbt), orchestrated by one Airflow DAG.

**Flow:** Source API → (Airflow extract / dedupe / publish) → Kafka → Target subscriber → Target API, and in parallel → Snowflake subscriber → RAW → merge → CORE → dbt MART.

## What works now

- Source API (`:8000`): `GET /purchase-orders`, `changed_since`, `/health`
- Kafka (KRaft): topics `purchase-orders` (3 partitions) and `purchase-orders-failed`
- Kafka UI (`:8080`) for topics and consumer groups
- Target API (`:8001`): SQLite upsert with `LastUpdated` 409 guard; `GET` list returns `{count, items}`
- Target subscriber (group `target-api`): consume → PUT → commit on 2xx/409
- `scripts/publish_initial.py`: publish the initial file before Airflow exists
- CI: ruff, pytest, gitleaks

## Setup (macOS)

```bash
cd purchase-order-data-pipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env   # set KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
ruff check .
pytest -q
```

## Run the stack

```bash
docker compose up -d --build
```

| Service | URL |
| --- | --- |
| Source API | http://127.0.0.1:8000/health · [docs](http://127.0.0.1:8000/docs) |
| Target API | http://127.0.0.1:8001/health · [docs](http://127.0.0.1:8001/docs) |
| Kafka UI | http://127.0.0.1:8080 |
| Kafka (host) | `127.0.0.1:9092` (use IPv4; `localhost` may try IPv6 first) |
| Kafka (in Compose) | `kafka:29092` |

### Smoke checks

```bash
curl -s 'http://127.0.0.1:8000/purchase-orders?load_type=initial' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["load_type"], d["record_count"])'

python scripts/publish_initial.py
curl -s 'http://127.0.0.1:8001/purchase-orders' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print("total count:", d["count"])'
```

Expect about **100** orders in the target API. Publishing again should not raise the count when `LastUpdated` is unchanged (409 path).

`changed_since` on the source API is strict greater-than on UTC `LastUpdated`. The source API returns the file as-is (including duplicate versions); dedupe belongs in Airflow later.

The JSON under `data/` is synthetic. Do not commit real credentials (`.env` is gitignored).

## Still to build

- [ ] Snowflake subscriber → RAW (run on host or Compose; needs `.env` Snowflake creds)
- [ ] CORE merge SQL and dbt MART
- [ ] Airflow DAG `purchase_order_pipeline` and dbt
- [ ] Optional: OpenTelemetry / Kubernetes
