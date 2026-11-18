# Purchase Order Data Pipeline

## Session 1: Source API

Personal technical exercise. This first milestone provides file-backed FastAPI endpoints,
Pydantic validation, timezone-aware deduplication, and CI. Kafka, Target API, Snowflake,
Airflow and dbt will be implemented in subsequent sessions.

### Run locally (macOS)

```bash
cd purchase-order-data-pipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
ruff check .
pytest -q
uvicorn services.source_api.main:app --reload --host 0.0.0.0 --port 8000
```

Browse http://localhost:8000/docs or test:

```bash
curl 'http://localhost:8000/health'
curl 'http://localhost:8000/purchase-orders?load_type=initial'
curl 'http://localhost:8000/purchase-orders?load_type=incremental'
curl 'http://localhost:8000/purchase-orders?load_type=incremental&changed_since=2026-11-15T01:15:00Z'
```

`changed_since` uses **strictly greater than** comparison on UTC-normalized `LastUpdated`.
It filters the selected source file; it does not automatically combine initial and incremental.
The source API intentionally preserves all original source records, including duplicate versions.
Deduplication belongs to the later Airflow `validate_dedupe` task, not this API.

The two provided JSON files are synthetic exercise data. Do not commit real credentials.

### Planned architecture

Source API -> Airflow extraction/dedupe -> Kafka -> Target API and Snowflake RAW -> CORE -> dbt.

### TODO

- [ ] Kafka KRaft, target API and subscriber
- [ ] Snowflake RAW landing and CORE merge
- [ ] Airflow DAG and dbt
- [ ] Docker Compose and optional observability
