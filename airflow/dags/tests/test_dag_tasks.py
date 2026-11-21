"""Overview: unit tests for DAG callables in airflow/dags/utils/tasks.py.

Mocks HTTP and Snowflake — no real Target API or warehouse.
Checks:
  - target_batch_ready: True when all OrderNumbers GET 200 (batch_id may differ)
  - target_batch_ready: False when any OrderNumber is missing (404)
  - raw_batch_ready: True when RAW distinct count >= published_count for batch_id
  - raw_batch_ready: False when RAW count is still short

Pair with test_dag_purchase_order_pipeline.py for DAG graph / wiring.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Skip if Airflow is missing (tasks.py imports Airflow / Snowflake helpers).
pytest.importorskip('airflow')

# parents[1] = airflow/dags (for `utils`), parents[3] = repo root (for `services`).
DAGS = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]
for path in (str(DAGS), str(ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from utils import tasks  # noqa: E402


# Minimal stand-in for httpx.Response used by target_batch_ready.
class FakeResponse:
    def __init__(self, status_code: int, body: dict | None = None):
        self.status_code = status_code
        self._body = body or {}

    def json(self):
        return self._body


# Sensor succeeds when every OrderNumber returns GET 200 — even if stored batch_id differs.
# (wait_for_target_api checks presence, not batch_id match.)
def test_target_batch_ready_true_when_all_present_other_batch(monkeypatch):
    published = {
        'batch_id': 'initial-this-run',
        'order_numbers': ['PO1', 'PO2'],
    }
    ti = MagicMock()
    ti.xcom_pull.return_value = published

    responses = {
        'PO1': FakeResponse(200, {'batch_id': 'initial-old'}),
        'PO2': FakeResponse(200, {'batch_id': 'initial-old'}),
    }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            order_number = url.rsplit('/', 1)[-1]
            return responses[order_number]

    monkeypatch.setattr(tasks.httpx, 'Client', FakeClient)
    assert tasks.target_batch_ready(ti=ti) is True


# Sensor keeps poking (False) until every published OrderNumber exists in the target API.
def test_target_batch_ready_false_when_missing(monkeypatch):
    published = {
        'batch_id': 'initial-this-run',
        'order_numbers': ['PO1', 'PO2'],
    }
    ti = MagicMock()
    ti.xcom_pull.return_value = published

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url):
            order_number = url.rsplit('/', 1)[-1]
            if order_number == 'PO1':
                return FakeResponse(200, {'batch_id': 'initial-this-run'})
            return FakeResponse(404)

    monkeypatch.setattr(tasks.httpx, 'Client', FakeClient)
    assert tasks.target_batch_ready(ti=ti) is False


# wait_for_snowflake_raw: True when RAW has at least published_count distinct orders for this batch_id.
def test_raw_batch_ready_compares_count(monkeypatch):
    published = {'batch_id': 'b1', 'published_count': 2}
    ti = MagicMock()
    ti.xcom_pull.return_value = published

    cur = MagicMock()
    cur.fetchone.return_value = (2,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(tasks, 'connect', lambda: conn)

    assert tasks.raw_batch_ready(ti=ti) is True
    cur.execute.assert_called_once()
    # Query is scoped to this DagRun's batch_id.
    assert cur.execute.call_args.args[1] == ('b1',)


# Incomplete RAW batch → sensor returns False (merge must not start yet).
def test_raw_batch_ready_false_when_short(monkeypatch):
    published = {'batch_id': 'b1', 'published_count': 5}
    ti = MagicMock()
    ti.xcom_pull.return_value = published

    cur = MagicMock()
    cur.fetchone.return_value = (2,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(tasks, 'connect', lambda: conn)

    assert tasks.raw_batch_ready(ti=ti) is False
