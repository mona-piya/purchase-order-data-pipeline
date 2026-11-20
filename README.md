# 🛒 Purchase Order Data Pipeline

Personal technical exercise: synthetic purchase orders from a file-backed **source API** through **Kafka** to a **target API** (SQLite) and **Snowflake** (RAW → CORE → staging → MART), orchestrated by one Airflow DAG.

## 🏗️ Architecture

```mermaid
flowchart LR
  SRC[Source API] --> AF[Airflow<br/>purchase_order_pipeline]
  AF --> K[Kafka<br/>purchase-orders]

  K --> TS[Target subscriber]
  TS --> TA[Target API<br/>SQLite]

  K --> SS[Snowflake subscriber]
  SS --> RAW

  subgraph SF[Snowflake]
    direction LR
    RAW[RAW] --> M[merge]
    M --> CORE[CORE]
    CORE --> STG[staging]
    STG --> MART[MART]
  end
```

| Layer | What it holds |
| --- | --- |
| 📥 **RAW** | One Kafka message = one `VARIANT` PO document |
| 🧱 **CORE** | Four ER tables (merge from RAW) |
| 🔧 **staging** | dbt views — rename, cast, keys |
| 📊 **MART** | 6 dimensions + 3 facts |

Detailed design notes (local only): [`docs/data-analysis.md`](docs/data-analysis.md).

---

## 🧩 Components

| Piece | Port / path | Notes |
| --- | --- | --- |
| Source API | `:8000` | `GET /purchase-orders` (`load_type`, `changed_since`), `/health` |
| Kafka (KRaft) | UI `:8080` | Topics `purchase-orders` (3 partitions), `purchase-orders-failed` |
| Target API | `:8001` | SQLite upsert + `LastUpdated` guard · **200** created/updated · **409** older version rejected |
| Target subscriber | group `target-api` | PUT → commit on **2xx** or **409** |
| Snowflake subscriber | group `snowflake-raw` | Lands in `RAW.PURCHASE_ORDER_PAYLOADS` (host process; key-pair via `.env`) |
| Airflow | `:8081` (`admin`/`admin`) | DAG `purchase_order_pipeline` |
| SQL | `airflow/dags/sql/` | DDL + RAW→CORE merge |
| dbt | `airflow/dags/dbt/` | Staging → MART; unique/not-null/relationships + splits=100 + category=cost-centre |
| Tests / CI | `tests/`, `airflow/dags/tests/` | ruff, pytest, `dbt parse`, sqlfluff, gitleaks |
| Smoke | `scripts/publish_initial.py` | Pre-Airflow publish check |

### 🌀 Airflow DAG

```
start → extract → validate_dedupe → publish
      → (wait_for_snowflake_raw → merge → dbt_build) ∥ wait_for_target_api
      → verify → end
```

| Detail | Value |
| --- | --- |
| Param `load_type` | `initial` \| `incremental` (incremental always hits the incremental file; watermark adds `changed_since`) |
| Watermark | Variable `purchase_order_watermark` — set only by `verify` |
| Logs | Full order payloads omitted (`show_return_value_in_logs=False`) |

---

## 📐 Data modelling

### 🧩 From these issues to the model we built

The extract is consistent enough to model, and inconsistent enough that a straight flatten-and-insert is wrong. The pipeline and MART answer each trap once, at the right layer:

| Finding | What goes wrong if you ignore it | How we solve it |
| --- | --- | --- |
| Duplicate `PO1040078`, later array row is older | Last-in-file / string-sort “wins” and you drop `GR700192` | Airflow `validate_dedupe` + merge: parse `LastUpdated` with offset → UTC; keep **newest** per `OrderNumber` |
| Mix of `Z` and `+10:00` timestamps | Same instant compared as different strings | Store / compare as `TIMESTAMP_TZ` (`TO_TIMESTAMP_TZ` at ingest and in merge) |
| Header net ≠ sum of all lines (cancelled keep amounts) | Reporting “order total” double-counts or invents money | CORE stores amounts **as sent**; MART `active_net_amount` / `cancelled_net_amount` on the **line** fact |
| `Quantity` / `LineNumber` / `RowIndex` as text; receipt qty int vs float | Bad maths or silent cast loss | CORE keeps ER text types; **staging** casts for measures (`ordered_qty`, ints for sort) |
| Unchanged replay of `PO1040000` | PK clash or blind overwrite of a newer CORE row | Merge guard: apply only when incoming `LastUpdated` **≥** stored; equal = no-op |
| Reversal = new receipt with negative qty | Net received overstated if negatives dropped | CORE keeps every receipt + self-FK; fact `fct_goods_receipts` + `is_reversal`; net = **SUM** including negatives |
| Header `OrderStatus` ≠ line/receipt reality | “Received” / “Closed” used as truth for fulfilment | Store source status; derive `derived_line_fulfilment` on `fct_purchase_order_lines` |
| `LineNumber` / `RowIndex` only unique inside parent | Colliding keys across orders | Composite PKs on CORE; surrogate keys in staging (`order_line_key`, `coding_key`); GR number globally unique |
| Fan-out (coding 1–3 × receipts 0–4 on one line) | One wide join multiplies $ and qty | **Three facts** at three grains — see MART below |

### 🗄️ CORE model

Four ER tables after RAW→CORE merge. Full notes: `docs/data-analysis.md` §3.2.

```mermaid
erDiagram
    PURCHASE_ORDER ||--o{ PURCHASE_ORDER_LINE : has
    PURCHASE_ORDER_LINE ||--o{ PURCHASE_ORDER_LINE_CODING_ROW : allocates
    PURCHASE_ORDER_LINE ||--o{ PURCHASE_ORDER_LINE_GOODS_RECEIPT : receives
    PURCHASE_ORDER_LINE_GOODS_RECEIPT ||--o| PURCHASE_ORDER_LINE_GOODS_RECEIPT : reverses

    PURCHASE_ORDER {
        string OrderNumber PK
        string OrderStatus
        string LastUpdated
        string SupplierCode
        float NetSum
    }

    PURCHASE_ORDER_LINE {
        string OrderNumber PK
        string LineNumber PK
        string LineStatus
        string Quantity
        float NetSum
    }

    PURCHASE_ORDER_LINE_CODING_ROW {
        string OrderNumber PK
        string LineNumber PK
        string RowIndex PK
        float SplitPercent
        string AccountCode
        string CostCenterCode
        string ProjectCode
    }

    PURCHASE_ORDER_LINE_GOODS_RECEIPT {
        string GoodsReceiptNumber PK
        string OrderNumber FK
        string LineNumber FK
        string ReversedGoodsReceiptNumber FK
        float ReceivedQuantity
    }
```


### ⭐ MART / dbt model

Staging views over CORE, then six dimensions + three facts in `MART` (dbt under `airflow/dags/dbt/`).

**Why three facts:** a line can have several coding rows *and* several goods receipts. One flat join multiplies rows and double-counts money. Each measure lives at one grain:

| Fact | Grain | Question |
| --- | --- | --- |
| `fct_purchase_order_lines` | order + line | What was ordered / fulfilment? |
| `fct_po_cost_allocations` | order + line + coding row | Which account / cost centre / project? |
| `fct_goods_receipts` | goods receipt number | What arrived or was reversed? |

#### 🌟 Star schema

```mermaid
erDiagram
  dim_supplier ||--o{ fct_purchase_order_lines : supplier_code
  dim_category ||--o{ fct_purchase_order_lines : category_code
  dim_date ||--o{ fct_purchase_order_lines : creation_date

  dim_account ||--o{ fct_po_cost_allocations : account_code
  dim_cost_center ||--o{ fct_po_cost_allocations : cost_center_code
  dim_project ||--o{ fct_po_cost_allocations : project_code
  fct_purchase_order_lines ||--o{ fct_po_cost_allocations : order_line_key

  dim_supplier ||--o{ fct_goods_receipts : supplier_code
  dim_category ||--o{ fct_goods_receipts : category_code
  dim_date ||--o{ fct_goods_receipts : receiving_date

  fct_purchase_order_lines {
    string order_line_key PK
    string order_number
    string line_number
    number active_net_amount
    string derived_line_fulfilment
  }

  fct_po_cost_allocations {
    string coding_key PK
    number allocated_active_net_amount
    number split_percent
  }

  fct_goods_receipts {
    string goods_receipt_number PK
    number received_qty
    boolean is_reversal
  }

  dim_supplier {
    string supplier_code PK
    string supplier_name
  }

  dim_category {
    string category_code PK
    string category_name
  }

  dim_account {
    string account_code PK
    string account_name
  }

  dim_cost_center {
    string cost_center_code PK
    string cost_center_name
  }

  dim_project {
    string project_code PK
    string project_name
  }

  dim_date {
    date date_day PK
    number year_number
    string year_month
  }
```

After both loads: Σ line `active_net_amount` = Σ allocation `allocated_active_net_amount` ≈ **703,170.63 AUD**.

---

## 🛠️ Setup (macOS)

```bash
cd purchase-order-data-pipeline
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# DAG tests need Airflow (same pin as Compose image):
python -m pip install "apache-airflow==2.10.5" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

cp .env.example .env   # Kafka + Snowflake
./scripts/check.sh     # ruff + pytest (same gate as CI)
```

Synthetic JSON lives under `data/`. Do not commit `.env` or `rsa_key.*`.

---

## 🚀 Run the stack

```bash
docker compose up -d --build

# Snowflake subscriber: run on the host
# (Compose profile needs the key mounted inside the container)
python -m services.snowflake_subscriber.main
```

| Service | URL |
| --- | --- |
| Source API | http://127.0.0.1:8000/health · [docs](http://127.0.0.1:8000/docs) |
| Target API | http://127.0.0.1:8001/health · [docs](http://127.0.0.1:8001/docs) |
| Kafka UI | http://127.0.0.1:8080 |
| Airflow UI | http://127.0.0.1:8081 (`admin` / `admin`) |
| Kafka (host) | `127.0.0.1:9092` (prefer IPv4; `localhost` may hit IPv6 first) |
| Kafka (Compose) | `kafka:29092` |

### ▶️ Airflow demo

1. Unpause `purchase_order_pipeline`.
2. Trigger with conf `{"load_type": "initial"}` → ~100 orders; CORE ≈ **100 / 215 / 275 / 181**.
3. Trigger again with `{"load_type": "incremental"}` → ~18 after dedupe; CORE ≈ **104 / 224 / 286 / 189**.
4. Keep the host Snowflake subscriber running so `wait_for_snowflake_raw` can succeed.

### 💨 Smoke (without Airflow)

```bash
curl -s 'http://127.0.0.1:8000/purchase-orders?load_type=initial' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["load_type"], d["record_count"])'

python scripts/publish_initial.py

curl -s 'http://127.0.0.1:8001/purchase-orders' \
  | python3 -c 'import json,sys; print("total count:", json.load(sys.stdin)["count"])'
```

`changed_since` is strict greater-than on UTC `LastUpdated`. The source returns the file as-is (including duplicate versions); Airflow `validate_dedupe` keeps the newest per `OrderNumber`.

### 📦 dbt

```bash
set -a && source .env && set +a
cd airflow/dags/dbt
dbt build --profiles-dir .
```

Rebuild Airflow once so `dbt_build` has the CLI:

```bash
docker compose build airflow-webserver airflow-scheduler && docker compose up -d
```

---

### 💥 Failure demos

```bash
# 1) Catch-up: stop target API, publish one order, start API again
docker compose stop target-api
python scripts/demo_failures.py publish-catchup
# watch: docker logs -f purchase-order-data-pipeline-target-subscriber-1
docker compose start target-api
# subscriber retries then applies (TARGET_PUT_ATTEMPTS=10)

# 2) Broken document → purchase-orders-failed
python scripts/demo_failures.py publish-broken
python scripts/demo_failures.py peek-failed
# or Kafka UI http://127.0.0.1:8080 → topic purchase-orders-failed
```

---

## 📋 Still to build

- [ ] ADRs
- [ ] Optional: OpenTelemetry / Kubernetes
