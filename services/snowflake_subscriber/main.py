"""Snowflake subscriber: Kafka purchase-orders -> RAW.PURCHASE_ORDER_PAYLOADS.

Interview loop:
1. Poll messages into a buffer (group snowflake-raw).
2. Flush when buffer hits N messages or T seconds have passed.
3. INSERT rows (order + Kafka metadata); commit offsets only after a successful insert.
4. Bad envelopes go to purchase-orders-failed, then that offset is committed.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message, Producer
from dotenv import load_dotenv

from services.common.publish import DLQ_TOPIC, TOPIC
from services.snowflake_subscriber.db import connect

# Repo-root .env (same pattern as scripts/publish_initial.py)
load_dotenv(Path(__file__).resolve().parents[2] / '.env')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('snowflake-subscriber')

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'snowflake-raw')
BATCH_SIZE = int(os.getenv('SNOWFLAKE_BATCH_SIZE', '50'))
FLUSH_SECONDS = float(os.getenv('SNOWFLAKE_FLUSH_SECONDS', '2'))

INSERT_SQL = """
INSERT INTO PURCHASE_ORDER_PAYLOADS (
    order_number,
    batch_id,
    load_type,
    extracted_at,
    last_updated,
    payload,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    source_file
)
SELECT
    %s,
    %s,
    %s,
    TO_TIMESTAMP_TZ(%s),
    TO_TIMESTAMP_TZ(%s),
    PARSE_JSON(%s),
    %s,
    %s,
    %s,
    %s
"""


@dataclass
class PendingRow:
    kafka_msg: Message
    values: tuple[Any, ...]


def send_to_failed(producer: Producer, raw_value: bytes, key: bytes | None, reason: str) -> None:
    headers = [('error_reason', reason.encode())]
    producer.produce(DLQ_TOPIC, key=key, value=raw_value, headers=headers)
    producer.flush(10)


def parse_row(msg: Message) -> PendingRow | str:
    """Return a PendingRow, or an error reason string for DLQ."""
    value = msg.value() or b''
    try:
        envelope = json.loads(value)
        if not isinstance(envelope, dict):
            raise ValueError('envelope is not an object')
        order = envelope.get('order')
        if not isinstance(order, dict):
            raise ValueError('envelope missing order object')
        order_number = order.get('OrderNumber')
        batch_id = envelope.get('batch_id')
        load_type = envelope.get('source_load_type')
        extracted_at = envelope.get('extracted_at')
        last_updated = order.get('LastUpdated')
        if not all([order_number, batch_id, load_type, extracted_at, last_updated]):
            raise ValueError('missing required envelope/order fields')
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        return f'bad_envelope: {exc}'

    values = (
        order_number,
        batch_id,
        load_type,
        extracted_at,
        last_updated,
        json.dumps(order),
        msg.topic(),
        msg.partition(),
        msg.offset(),
        envelope.get('source_file'),
    )
    return PendingRow(kafka_msg=msg, values=values)


def insert_rows(conn, rows: list[PendingRow]) -> None:
    if not rows:
        return
    # Per-row execute: connector executemany cannot rewrite INSERT with PARSE_JSON/TO_TIMESTAMP_TZ.
    cur = conn.cursor()
    try:
        for row in rows:
            cur.execute(INSERT_SQL, row.values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def flush(consumer: Consumer, conn, buffer: list[PendingRow]) -> int:
    if not buffer:
        return 0
    count = len(buffer)
    insert_rows(conn, buffer)
    # Commit each message's offset after a successful insert (at-least-once).
    for item in buffer:
        consumer.commit(message=item.kafka_msg, asynchronous=False)
    log.info('flushed %s rows into RAW.PURCHASE_ORDER_PAYLOADS', count)
    buffer.clear()
    return count


def run() -> None:
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    producer = Producer({'bootstrap.servers': BOOTSTRAP})
    consumer.subscribe([TOPIC])
    log.info(
        'subscribed to %s as group %s (batch_size=%s flush_seconds=%s)',
        TOPIC, GROUP_ID, BATCH_SIZE, FLUSH_SECONDS,
    )

    conn = connect()
    buffer: list[PendingRow] = []
    last_flush = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())
                parsed = parse_row(msg)
                if isinstance(parsed, str):
                    send_to_failed(producer, msg.value() or b'', msg.key(), parsed)
                    log.warning('sent bad message to %s: %s', DLQ_TOPIC, parsed)
                    consumer.commit(message=msg, asynchronous=False)
                else:
                    buffer.append(parsed)

            due = (time.monotonic() - last_flush) >= FLUSH_SECONDS
            if buffer and (len(buffer) >= BATCH_SIZE or due):
                flush(consumer, conn, buffer)
                last_flush = time.monotonic()
    except KeyboardInterrupt:
        log.info('stopped')
        if buffer:
            flush(consumer, conn, buffer)
    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    run()
