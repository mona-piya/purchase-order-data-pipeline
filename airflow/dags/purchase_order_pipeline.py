"""purchase_order_pipeline — one DagRun = one batch_id."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from utils import tasks

default_args = {
    'owner': 'mona-piya',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Manual DAG: start → extract → validate/dedupe → publish → wait RAW/target → merge → dbt → verify → end.
with DAG(
    dag_id='purchase_order_pipeline',
    description='Extract → Kafka → wait RAW/target → merge CORE → dbt → verify',
    default_args=default_args,
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    params={'load_type': 'initial'},
    tags=['purchase-order'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # GET orders from the source API.
    extract = PythonOperator(
        task_id='extract',
        python_callable=tasks.extract,
        show_return_value_in_logs=False,  # orders payload stays in XCom only
    )
    # Validate + keep newest LastUpdated per OrderNumber.
    validate_dedupe = PythonOperator(
        task_id='validate_dedupe',
        python_callable=tasks.validate_dedupe,
        show_return_value_in_logs=False,  # orders payload stays in XCom only
    )
    # Publish envelopes to Kafka with this run's batch_id.
    publish = PythonOperator(
        task_id='publish',
        python_callable=tasks.publish,
        show_return_value_in_logs=False,
    )
    # Wait until Snowflake RAW has every order for this batch_id.
    wait_for_snowflake_raw = PythonSensor(
        task_id='wait_for_snowflake_raw',
        python_callable=tasks.raw_batch_ready,
        poke_interval=10,
        timeout=60 * 10,
        mode='poke',
    )
    # Wait until the target API has every published OrderNumber.
    wait_for_target_api = PythonSensor(
        task_id='wait_for_target_api',
        python_callable=tasks.target_batch_ready,
        poke_interval=10,
        timeout=60 * 10,
        mode='poke',
    )
    # Merge this batch from RAW into the four CORE tables.
    merge_raw_to_core = PythonOperator(
        task_id='merge_raw_to_core',
        python_callable=tasks.merge_raw_to_core,
        show_return_value_in_logs=False,
    )
    # Build MART (staging casts, dims, three facts + tests).
    dbt_build = PythonOperator(
        task_id='dbt_build',
        python_callable=tasks.dbt_build,
        show_return_value_in_logs=False,
    )
    # Check counts and advance the watermark Variable.
    verify = PythonOperator(
        task_id='verify',
        python_callable=tasks.verify,
        show_return_value_in_logs=False,
    )

    start >> extract >> validate_dedupe >> publish
    publish >> wait_for_snowflake_raw >> merge_raw_to_core >> dbt_build
    publish >> wait_for_target_api
    [dbt_build, wait_for_target_api] >> verify >> end
