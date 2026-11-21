"""Overview: structural tests for purchase_order_pipeline.

Loads the DAG with Airflow DagBag (parse only — no DagRun, Kafka, or Snowflake).
Checks:
  - dag_id / manual schedule / no catchup
  - expected task ids (start … end)
  - wiring: linear prefix, parallel waits after publish, merge → dbt → verify join → end

Pair with test_dag_tasks.py for sensor callable behaviour.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

# Skip the whole module if Airflow is not installed (e.g. API-only venv).
pytest.importorskip('airflow')

# parents[1] = airflow/dags, parents[3] = repo root.
DAGS = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]

# Full task set for one DagRun (rename here if you rename tasks in the DAG).
EXPECTED_TASKS = {
    'start',
    'extract',
    'validate_dedupe',
    'publish',
    'wait_for_snowflake_raw',
    'wait_for_target_api',
    'merge_raw_to_core',
    'dbt_build',
    'verify',
    'end',
}


# Load the DAG once per module: parse files only — does not run a DagRun.
@pytest.fixture(scope='module')
def dag():
    os.environ.setdefault('AIRFLOW_HOME', tempfile.mkdtemp(prefix='po-airflow-'))
    os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'false'
    # DAG imports `from utils import tasks` — dags folder must be on path.
    dags_path = str(DAGS)
    if dags_path not in sys.path:
        sys.path.insert(0, dags_path)
    root_path = str(ROOT)
    if root_path not in sys.path:
        sys.path.insert(0, root_path)

    from airflow.models import DagBag

    # Airflow 3 DagBag: no include_examples; LOAD_EXAMPLES=false + .airflowignore skip tests/.
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}, bag.import_errors
    assert 'purchase_order_pipeline' in bag.dags
    return bag.dags['purchase_order_pipeline']


# Manual DAG: no schedule, no catchup of historical dates.
def test_dag_id_and_schedule(dag):
    assert dag.dag_id == 'purchase_order_pipeline'
    # Airflow 3: schedule=None on the DAG (schedule_interval was removed).
    assert dag.schedule is None
    assert dag.catchup is False


# Guard against accidental task add/remove/rename.
def test_task_ids(dag):
    assert set(dag.task_ids) == EXPECTED_TASKS


# start → extract → validate_dedupe → publish
def test_linear_prefix(dag):
    assert dag.get_task('start').downstream_task_ids == {'extract'}
    assert dag.get_task('extract').downstream_task_ids == {'validate_dedupe'}
    assert dag.get_task('validate_dedupe').downstream_task_ids == {'publish'}


# After publish, RAW wait (+ merge) and target wait run in parallel.
def test_parallel_after_publish(dag):
    assert dag.get_task('publish').downstream_task_ids == {
        'wait_for_snowflake_raw',
        'wait_for_target_api',
    }
    assert dag.get_task('wait_for_snowflake_raw').downstream_task_ids == {'merge_raw_to_core'}
    assert dag.get_task('merge_raw_to_core').downstream_task_ids == {'dbt_build'}
    assert dag.get_task('dbt_build').downstream_task_ids == {'verify'}
    assert dag.get_task('wait_for_target_api').downstream_task_ids == {'verify'}


# verify waits for both branches, then end.
def test_verify_joins_then_end(dag):
    assert dag.get_task('verify').upstream_task_ids == {
        'dbt_build',
        'wait_for_target_api',
    }
    assert dag.get_task('verify').downstream_task_ids == {'end'}
    assert dag.get_task('end').upstream_task_ids == {'verify'}
