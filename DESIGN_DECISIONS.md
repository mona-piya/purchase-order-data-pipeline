# Design decisions

How purchase orders move from the source API to the target API and Snowflake, and why Kafka and Airflow sit where they do.

For each decision: **context**, **options**, **choice**, **why**, **trade-offs**.

Local notes with diagrams live under `docs/` (gitignored). This file is the **committed** record for GitHub review.

| ID | Decision |
| --- | --- |
| [ADR-001](#adr-001-where-kafka-sits-between-extraction-and-both-destinations) | Kafka between extract and both destinations |
| [ADR-002](#adr-002-one-dag-owns-a-batch) | One DAG owns a batch |
| [ADR-003](#adr-003-message-design-one-complete-document-per-order) | One complete document per order |
| [ADR-004](#adr-004-idempotency-and-ordering) | Idempotency by parsed `LastUpdated` |
| [ADR-005](#adr-005-snowflake-landing-raw-then-merge) | RAW VARIANT then SQL merge |
| [ADR-006](#adr-006-dbt-dimensional-model) | Three facts at three grains |
| [ADR-007](#adr-007-local-runtime-compose-first) | Compose first, Kubernetes second |
| [ADR-008](#adr-008-secrets) | Gitignored `.env` + example |
| [ADR-009](#adr-009-watermark-two-slots) | Two watermark slots; advance only after verify |

---

## ADR-001 Where Kafka sits: between extraction and both destinations

**Context.** Two destinations need the same order changes. They have different speeds and failure modes: an HTTP API that can be down or slow, and a warehouse that prefers batches.

**Options.**

1. Airflow calls the source API and writes directly to the target API and Snowflake.
2. Source API publishes to Kafka itself on every change; consumers write to each destination.
3. Airflow extracts and publishes to Kafka; one consumer group per destination.

**Choice.** Option 3.

**Why.** Kafka decouples the destinations. If the target API is down, Snowflake still loads, and the target subscriber resumes from its committed offset when the API is back. Adding a third consumer later needs no change upstream. Option 2 is the most "real-time", but the source here is a file-backed API with no change feed; making it a producer would invent behaviour the brief does not describe. Option 1 couples both destinations into one task, so a retry for one replays the other.

**Trade-offs.** More moving parts than option 1. Delivery is at-least-once, so both subscribers must be idempotent ([ADR-004](#adr-004-idempotency-and-ordering)). Latency is bounded by the extract schedule, not by Kafka.

---

## ADR-002 One DAG owns a batch

**Context.** Airflow is a batch orchestrator. Kafka consumers are long-running processes. The merge must not start until the Snowflake subscriber has inserted this batch into `RAW`.

**Options.**

1. Airflow tasks consume from Kafka on a schedule (poll for N seconds, then exit).
2. Two DAGs. One publishes. A second DAG, triggered afterwards and again on a schedule, runs the merge and dbt.
3. One DAG, `purchase_order_pipeline`. Consumers stay up. After publish, a sensor waits until `RAW` has every order for this `batch_id`, then the same run merges, runs dbt, and verifies. A second sensor (`wait_for_target_api`) waits until each published order exists on the target API and joins at verify.

**Choice.** Option 3.

**Why.** One DagRun is one batch, which is what the demo trigger should mean. The wait task makes a single DAG safe: merge starts only after the subscriber has landed the batch. Option 2 leaves a gap where Kafka has the orders and `CORE` is still old. Option 1 fights task timeouts and offset commits. Clearing a failed merge, dbt, or wait task does not re-run publish. The target-API wait is parallel with the merge/dbt path, so a down target API does not block Snowflake; it stops the DAG at verify.

**Trade-offs.** The wait needs a `batch_id` on every message and on every `RAW` row, plus a timeout. A stuck subscriber fails the DagRun even though publish succeeded. Consumers still need their own health checks outside Airflow.

---

## ADR-003 Message design: one complete document per order

**Context.** The source exports a nested document per order, and an incremental record is the full current document.

**Options.**

1. One message per order document.
2. Flatten first; one message per row of each of the four tables (four topics).
3. One message per file.

**Choice.** Option 1, key `OrderNumber`, value is the document plus an envelope: `batch_id`, `event_id`, `source_load_type`, `extracted_at`, `last_updated_utc`, `schema_version`. `batch_id` is what the DAG wait task counts.

**Why.** The target API wants a whole order. Snowflake wants the whole order too, because a child row that disappears from a newer document must disappear from CORE, and only the full document says that. Keying by `OrderNumber` keeps every version of one order on the same partition in publish order. Option 2 loses the "document is complete" signal and needs cross-topic coordination. Option 3 is a file transfer, not pub/sub.

**Trade-offs.** Message size grows with lines (largest order here has 5 lines; fine). Flattening happens once, in Snowflake SQL, rather than in a stream processor.

---

## ADR-004 Idempotency and ordering

**Context.** The incremental file sends `PO1040078` twice, and the later array element is the **older** version (`11:00+10:00` → 01:00 UTC, before `01:30Z`). It also re-sends `PO1040000` unchanged. Kafka delivers at least once.

**Options.**

1. Trust Kafka order and overwrite.
2. Dedupe in the producer only.
3. Dedupe in the producer and guard in every subscriber on `LastUpdated`.

**Choice.** Option 3.

**Why.** The producer collapses each batch to the newest version per order using `datetime.fromisoformat` with offsets, so the known bad case never reaches Kafka. Subscribers still compare `LastUpdated` because a redelivery or a replayed older batch must not win: the target API returns **409** for an older document, and the Snowflake merge applies a document only when its `LastUpdated` is greater than or equal to the stored one. RAW keeps every received version for audit.

**Trade-offs.** Equal timestamps with different content would be accepted last-writer-wins. Not seen in this data; logged if it happens.

---

## ADR-005 Snowflake landing: RAW then merge

**Context.** The four ER tables are the contract. The documents are nested and the data has quality issues.

**Options.**

1. Flatten in Python in the subscriber and insert into the four tables directly.
2. Snowflake Kafka Connector into a RAW table, then SQL.
3. Custom Python subscriber into a RAW table, then SQL merge run by Airflow.

**Choice.** Option 3, with option 2 noted as the production path.

**Why.** Landing raw keeps every version and makes the merge replayable from Snowflake alone. The merge needs transactions across four tables and child-row replacement, which is clearer in SQL than in a streaming subscriber. A custom Python subscriber is small with `snowflake-connector-python` and runs on any laptop; Kafka Connect adds a JVM service and key-pair setup that costs disproportionate time for this exercise.

**Trade-offs.** The Python subscriber is simpler but less robust than the connector (no exactly-once). Covered by idempotent merge and offset columns in RAW.

---

## ADR-006 dbt dimensional model

**Context.** After the merge, CORE holds three grains at once: order line, coding split, and goods receipt. A line can have several coding rows and several receipts. Joining both children onto the line multiplies measures.

**Options.**

1. Report straight off the four CORE tables (wide joins in BI).
2. One flat MART fact that joins line + coding + receipts.
3. Three facts at three grains, plus shared dimensions; thin staging views for rename/cast/keys only.

**Choice.** Option 3. dbt under `airflow/dags/dbt/` (`STAGING` views → `MART` tables). Facts: `fct_purchase_order_lines`, `fct_po_cost_allocations`, `fct_goods_receipts`. Dimensions: supplier, category, account, cost centre, project, date.

**Why.** Each measure must live at the grain where it is true once (ordered $ vs allocated $ vs received $). Staging does not invent business rules; tests cover uniqueness, relationships, splits = 100, and category = cost-centre active net.

**Trade-offs.** More models than a single wide table. Consumers must pick the right fact for the question.

---

## ADR-007 Local runtime: Compose first

**Context.** The brief requires a runnable solution on personal / free-tier resources. Kubernetes is an optional bonus.

**Options.**

1. Cloud-only (managed Kafka, Airflow, warehouse) as the primary path.
2. Docker Compose as the default local stack; optional Kubernetes with the same app images.
3. Kubernetes-only local demo (minikube / kind) as the primary path.

**Choice.** Option 2.

**Why.** One `docker-compose.yml` is the path a reviewer can start with one command: Kafka, source API, target API, both subscribers, Airflow (LocalExecutor + Postgres), Jaeger / OTel. Snowflake stays a personal trial outside the compose network. Kubernetes via minikube + Kustomize (`kubernetes/`) reuses the same images for the bonus; it is not the default demo path because host mounts and image loads are flakier on a laptop.

**Trade-offs.** Compose is not production. Minikube needs explicit waits before port-forwards, shared logs PVC for the Airflow UI, and care with hostPath (e.g. source-api uses image-baked data rather than a flaky mount). Prefer Compose for the walk-through unless showing the K8s bonus.

---

## ADR-008 Secrets

**Context.** Snowflake trial credentials and the Airflow Fernet key must stay out of git; reviewers still need a runnable template.

**Options.**

1. Commit a filled `.env` (or secrets in Compose YAML).
2. Cloud secret manager only (no local file).
3. Gitignored `.env` + committed `.env.example`; CI `gitleaks`; prefer Snowflake key-pair over password.

**Choice.** Option 3. Compose mounts `./rsa_key.p8` into Airflow and the snowflake-subscriber. Kubernetes uses Secrets from `./scripts/kubernetes_create_secrets.sh` (same values).

**Why.** Option 1 fails the brief and gitleaks. Option 2 is overkill for a laptop demo. Example + gitignore is what a stranger can copy in one step.

**Trade-offs.** Presenter must keep `.env` / `rsa_key.p8` on the machine; a missing key breaks the Snowflake subscriber and Airflow merge/dbt.

---

## ADR-009 Watermark: two slots

**Context.** Incremental extract needs a `changed_since` cursor. Initial and incremental must not overwrite each other's progress. A failed DagRun must not move the cursor.

**Options.**

1. One row per pipeline (single `changed_since`); every successful run overwrites it.
2. Append-only watermark history table; extract reads `MAX(changed_since)`.
3. Two control rows keyed by `(pipeline_name, load_type)` — slots `initial` and `incremental`. `verify` MERGEs only this DagRun's slot. Incremental extract reads the incremental slot, falling back to initial if empty. Empty publish batch does not advance.

**Choice.** Option 3. Table `CORE.PIPELINE_WATERMARK`; helpers in `airflow/dags/utils/watermark.py`. Triggers stay manual for now (schedules later).

**Why.** Option 1 made an initial re-run clobber the incremental cursor. Option 2 is audit-heavy for a demo cursor. Two slots keep bootstrap and steady-state independent; advancing only in `verify` means a failed wait/merge/dbt leaves the next extract unchanged.

**Trade-offs.** Presenter must know which `load_type` they triggered. `load_dts` is when the slot was written — not the API filter (that is `changed_since`).

---

## Related reading

| Doc | What's there |
| --- | --- |
| [README.md](README.md) | Runbook, DAG picture, issue → design table |
| `docs/architecture.md` (local) | Mermaid diagrams, failure table, CI notes |
| `docs/data-analysis.md` (local) | Data findings that drove CORE / MART |
