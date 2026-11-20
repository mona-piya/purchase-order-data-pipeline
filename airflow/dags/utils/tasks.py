"""Callable steps used by the purchase_order_pipeline DAG."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import httpx
from airflow.models import Variable

from services.common.dedupe import dedupe_orders
from services.common.models import PurchaseOrder, PurchaseOrderExtract
from services.common.publish import DLQ_TOPIC, make_producer, publish_orders
from services.snowflake_subscriber.db import connect
from utils.merge import run_merge

log = logging.getLogger(__name__)

WATERMARK_VAR = 'purchase_order_watermark'
SOURCE_URL = os.getenv('SOURCE_API_URL', 'http://127.0.0.1:8000').rstrip('/')
TARGET_URL = os.getenv('TARGET_API_URL', 'http://127.0.0.1:8001').rstrip('/')
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')


# Read load_type from DagRun conf or DAG params (initial | incremental).
def _load_type(**context) -> str:
    params = context.get('params') or {}
    dag_run = context.get('dag_run')
    conf = (dag_run.conf or {}) if dag_run else {}
    value = conf.get('load_type') or params.get('load_type') or 'initial'
    if value not in ('initial', 'incremental'):
        raise ValueError(f'load_type must be initial|incremental, got {value!r}')
    return value


# Pull purchase orders from the source API for this run.
# initial → full file; incremental → changed_since watermark (or incremental file).
def extract(**context) -> dict:
    load_type = _load_type(**context)
    if load_type == 'initial':
        url = f'{SOURCE_URL}/purchase-orders?load_type=initial'
    else:
        # Always hit the incremental file; optional watermark filters LastUpdated > cutoff.
        url = f'{SOURCE_URL}/purchase-orders?load_type=incremental'
        watermark = Variable.get(WATERMARK_VAR, default_var=None)
        if watermark:
            url = f'{url}&changed_since={watermark}'

    log.info('extract %s', url)
    response = httpx.get(url, timeout=120.0)
    response.raise_for_status()
    extract_body = PurchaseOrderExtract.model_validate(response.json())
    orders = [o.model_dump(mode='json') for o in extract_body.purchased_orders]
    log.info(
        'extract done load_type=%s record_count=%s',
        extract_body.load_type, len(orders),
    )
    return {
        'load_type': extract_body.load_type,
        'extracted_at': extract_body.extracted_at.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'orders': orders,
        'record_count': len(orders),
    }


# Validate each order with Pydantic; bad docs go to purchase-orders-failed.
# Keep one version per OrderNumber (newest LastUpdated).
def validate_dedupe(**context) -> dict:
    ti = context['ti']
    payload = ti.xcom_pull(task_ids='extract')
    good: list[PurchaseOrder] = []
    bad: list[dict] = []
    for raw in payload['orders']:
        try:
            order = PurchaseOrder.model_validate(raw)
            good.append(order)
        except Exception as exc:  # noqa: BLE001 — send to DLQ path
            bad.append({'order': raw, 'error': str(exc)})

    if bad:
        producer = make_producer(BOOTSTRAP)
        for item in bad:
            producer.produce(
                DLQ_TOPIC,
                key=(item['order'].get('OrderNumber') or 'unknown').encode(),
                value=json.dumps(item).encode(),
                headers=[('error_reason', b'validate_failed')],
            )
        producer.flush(30)
        log.warning('sent %s invalid orders to %s', len(bad), DLQ_TOPIC)

    deduped = dedupe_orders(good)
    log.info(
        'validate_dedupe record_count=%s rejected_count=%s',
        len(deduped), len(bad),
    )
    return {
        'load_type': payload['load_type'],
        'extracted_at': payload['extracted_at'],
        'orders': [o.model_dump(mode='json') for o in deduped],
        'order_numbers': [o.OrderNumber for o in deduped],
        'record_count': len(deduped),
        'rejected_count': len(bad),
    }


# Publish deduped orders to Kafka topic purchase-orders.
# batch_id = "{load_type}-{dag_run_id}" so wait/merge can find this batch.
def publish(**context) -> dict:
    ti = context['ti']
    payload = ti.xcom_pull(task_ids='validate_dedupe')
    dag_run = context['dag_run']
    load_type = payload['load_type']
    batch_id = f'{load_type}-{dag_run.run_id}'
    orders = [PurchaseOrder.model_validate(o) for o in payload['orders']]
    extracted_at = datetime.fromisoformat(payload['extracted_at'].replace('Z', '+00:00'))

    producer = make_producer(BOOTSTRAP)
    count = publish_orders(
        producer,
        orders,
        batch_id=batch_id,
        load_type=load_type,
        extracted_at=extracted_at,
    )
    log.info('published %s orders batch_id=%s', count, batch_id)
    return {
        'batch_id': batch_id,
        'load_type': load_type,
        'published_count': count,
        'order_numbers': payload['order_numbers'],
        'extracted_at': payload['extracted_at'],
    }


# Sensor poke: True when RAW has at least as many distinct order_numbers as we published.
# Used by wait_for_snowflake_raw so merge does not start on an incomplete batch.
def raw_batch_ready(**context) -> bool:
    ti = context['ti']
    published = ti.xcom_pull(task_ids='publish')
    batch_id = published['batch_id']
    expected = published['published_count']
    if expected == 0:
        return True
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT COUNT(DISTINCT order_number) '
            'FROM RAW.PURCHASE_ORDER_PAYLOADS WHERE batch_id = %s',
            (batch_id,),
        )
        (got,) = cur.fetchone()
        cur.close()
    finally:
        conn.close()
    log.info('wait_for_snowflake_raw batch_id=%s got=%s expected=%s', batch_id, got, expected)
    return int(got) >= int(expected)


# Run RAW → CORE merge SQL for this batch_id (dedupe, LastUpdated guard, replace children).
def merge_raw_to_core(**context) -> dict:
    ti = context['ti']
    published = ti.xcom_pull(task_ids='publish')
    batch_id = published['batch_id']
    counts = run_merge(batch_id)
    log.info('merge complete batch_id=%s counts=%s', batch_id, counts)
    return {'batch_id': batch_id, 'counts': [list(r) for r in counts]}


# Sensor poke: True when every published OrderNumber exists in the target API (GET 200).
# Used by wait_for_target_api (parallel with merge). Logs batch_id match counts (does not require match).
def target_batch_ready(**context) -> bool:
    ti = context['ti']
    published = ti.xcom_pull(task_ids='publish')
    batch_id = published['batch_id']
    order_numbers = published['order_numbers']
    if not order_numbers:
        return True
    with httpx.Client(timeout=30.0) as client:
        missing = []
        matched = 0
        other_batch = 0
        for order_number in order_numbers:
            response = client.get(f'{TARGET_URL}/purchase-orders/{order_number}')
            if response.status_code != 200:
                missing.append(order_number)
                continue
            stored_batch = response.json().get('batch_id')
            if stored_batch == batch_id:
                matched += 1
            else:
                other_batch += 1
        log.info(
            'wait_for_target_api batch_id=%s missing=%s / %s matched_batch=%s other_batch=%s',
            batch_id, len(missing), len(order_numbers), matched, other_batch,
        )
        return not missing


# Final checks: target API count + CORE header count; set watermark for the next incremental run.
def verify(**context) -> dict:
    ti = context['ti']
    published = ti.xcom_pull(task_ids='publish')
    merge_result = ti.xcom_pull(task_ids='merge_raw_to_core')
    order_numbers = published['order_numbers']

    with httpx.Client(timeout=30.0) as client:
        listing = client.get(f'{TARGET_URL}/purchase-orders')
        listing.raise_for_status()
        target_count = listing.json()['count']

    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM CORE.PURCHASE_ORDER')
        (core_orders,) = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    # Watermark = max LastUpdated in this batch (only verify advances it).
    deduped = ti.xcom_pull(task_ids='validate_dedupe')
    max_lu = None
    for raw in deduped['orders']:
        lu = raw['LastUpdated']
        if max_lu is None or lu > max_lu:
            max_lu = lu
    if max_lu:
        Variable.set(WATERMARK_VAR, max_lu)

    summary = {
        'batch_id': published['batch_id'],
        'published_count': published['published_count'],
        'target_api_count': target_count,
        'core_purchase_order_count': int(core_orders),
        'merge_counts': merge_result.get('counts') if merge_result else None,
        'watermark': max_lu,
        'sample_orders': order_numbers[:5],
    }
    log.info('verify %s', summary)
    return summary
