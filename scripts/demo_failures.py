#!/usr/bin/env python3
"""Failure-demo helpers for Session 5.

1) catch-up order  — publish one new PO while target-api is down; start it again to see retries succeed.
2) broken          — publish a bad envelope; target-subscriber sends it to purchase-orders-failed.

Usage (repo root, Kafka up):
  python scripts/demo_failures.py publish-catchup
  python scripts/demo_failures.py publish-broken
  python scripts/demo_failures.py peek-failed
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.common.publish import DLQ_TOPIC, TOPIC  # noqa: E402
from services.common.tracing import force_flush, init_tracing, kafka_inject_headers  # noqa: E402

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')


def _producer() -> Producer:
    return Producer({'bootstrap.servers': BOOTSTRAP})


def publish_catchup() -> None:
    """Publish one synthetic order the target has never seen."""
    order_number = f'PO-DEMO-{uuid.uuid4().hex[:8].upper()}'
    now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    envelope = {
        'batch_id': f'demo-catchup-{uuid.uuid4().hex[:8]}',
        'event_id': str(uuid.uuid4()),
        'source_load_type': 'incremental',
        'extracted_at': now,
        'last_updated_utc': now,
        'schema_version': '1',
        'order': {
            'OrderNumber': order_number,
            'OrderType': 'Standard',
            'OrderStatus': 'Sent',
            'CreationTime': now,
            'LastUpdated': now,
            'SupplierCode': 'DEMO',
            'SupplierName': 'Demo Supplier',
            'Creator': 'demo',
            'PurchaseCurrency': 'AUD',
            'NetSum': 1.0,
            'TaxSum': 0.1,
            'GrossSum': 1.1,
            'OrderLines': [],
        },
    }
    tracer = init_tracing('po-publisher')
    p = _producer()
    with tracer.start_as_current_span('po.publish') as span:
        span.set_attribute('order_number', order_number)
        span.set_attribute('batch_id', envelope['batch_id'])
        p.produce(
            TOPIC,
            key=order_number.encode(),
            value=json.dumps(envelope).encode(),
            headers=kafka_inject_headers(),
        )
        p.flush(10)
    force_flush()
    print(f'published catch-up order {order_number} -> topic {TOPIC}')
    print('Jaeger: http://127.0.0.1:16686  (search tag order_number)')
    print('If target-api was down, watch target-subscriber logs retry; then start target-api.')


def publish_broken() -> None:
    """Publish a message that fails envelope validation (no order / batch_id)."""
    key = b'PO-BROKEN'
    bad = {
        'event_id': str(uuid.uuid4()),
        'note': 'intentionally broken — missing order and batch_id',
    }
    p = _producer()
    p.produce(TOPIC, key=key, value=json.dumps(bad).encode())
    p.flush(10)
    print(f'published broken envelope key=PO-BROKEN -> topic {TOPIC}')
    print(f'target-subscriber should DLQ it to {DLQ_TOPIC} (Kafka UI or peek-failed).')


def peek_failed(*, timeout_s: float = 8.0) -> None:
    """Read recent messages from purchase-orders-failed (unique demo consumer group)."""
    group = f'demo-peek-{uuid.uuid4().hex[:8]}'
    c = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    c.subscribe([DLQ_TOPIC])
    print(f'peeking {DLQ_TOPIC} (group={group}, ~{timeout_s}s)…')
    deadline = datetime.now(timezone.utc).timestamp() + timeout_s
    seen = 0
    while datetime.now(timezone.utc).timestamp() < deadline:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('error:', msg.error())
            continue
        seen += 1
        reason = None
        if msg.headers():
            for h_key, h_val in msg.headers():
                if h_key == 'error_reason' and h_val is not None:
                    reason = h_val.decode(errors='replace')
        print('---')
        print('key:', None if msg.key() is None else msg.key().decode(errors='replace'))
        print('error_reason:', reason)
        try:
            print('value:', json.dumps(json.loads(msg.value()), indent=2)[:500])
        except Exception:
            print('value(raw):', msg.value()[:300])
    c.close()
    print(f'done — saw {seen} message(s)')


def main() -> None:
    load_dotenv(ROOT / '.env')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'action',
        choices=('publish-catchup', 'publish-broken', 'peek-failed'),
    )
    args = parser.parse_args()
    if args.action == 'publish-catchup':
        publish_catchup()
    elif args.action == 'publish-broken':
        publish_broken()
    else:
        peek_failed()


if __name__ == '__main__':
    main()
