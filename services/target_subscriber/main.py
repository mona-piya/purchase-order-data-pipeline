"""Target subscriber: Kafka purchase-orders -> Target API PUT.

Interview loop:
1. Poll one message (group target-api).
2. PUT /purchase-orders/{OrderNumber} with {batch_id, order}.
3. 2xx or 409 -> commit the Kafka offset (done).
4. Transient errors -> retry with backoff, then purchase-orders-failed + commit.
"""

from __future__ import annotations

import json
import logging
import os
import time

import httpx
from confluent_kafka import Consumer, KafkaException, Producer
from opentelemetry.propagate import inject

from services.common.publish import DLQ_TOPIC, TOPIC
from services.common.tracing import (
    attach_from_kafka_headers,
    detach_context,
    init_tracing,
    set_span_error,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('target-subscriber')

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'target-api')
TARGET_URL = os.getenv('TARGET_API_URL', 'http://localhost:8001').rstrip('/')
MAX_ATTEMPTS = int(os.getenv('TARGET_PUT_ATTEMPTS', '3'))


# PUT one order envelope to the target API (propagates current trace via headers).
def put_order(client: httpx.Client, envelope: dict) -> httpx.Response:
    order = envelope['order']
    order_number = order['OrderNumber']
    headers: dict[str, str] = {}
    inject(headers)
    return client.put(
        f'{TARGET_URL}/purchase-orders/{order_number}',
        json={'batch_id': envelope['batch_id'], 'order': order},
        headers=headers,
        timeout=30.0,
    )


# Send a failed message to purchase-orders-failed with an error_reason header.
def send_to_failed(producer: Producer, raw_value: bytes, key: bytes | None, reason: str) -> None:
    headers = [('error_reason', reason.encode())]
    producer.produce(DLQ_TOPIC, key=key, value=raw_value, headers=headers)
    producer.flush(10)


# Handle one Kafka message: validate, PUT with retries, or DLQ.
# Returns 'ok', 'conflict', or 'failed'.
def process_message(
    client: httpx.Client,
    producer: Producer,
    tracer,
    *,
    key: bytes | None,
    value: bytes,
    headers,
) -> str:
    token = attach_from_kafka_headers(headers)
    try:
        with tracer.start_as_current_span('po.deliver.target_api') as span:
            try:
                envelope = json.loads(value)
                if 'order' not in envelope or 'batch_id' not in envelope:
                    raise ValueError('envelope missing order or batch_id')
                if 'OrderNumber' not in envelope['order']:
                    raise ValueError('order missing OrderNumber')
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                set_span_error(span, exc)
                send_to_failed(producer, value, key, f'bad_envelope: {exc}')
                log.warning('sent bad message to %s: %s', DLQ_TOPIC, exc)
                return 'failed'

            order_number = envelope['order']['OrderNumber']
            span.set_attribute('order_number', order_number)
            span.set_attribute('batch_id', envelope['batch_id'])
            last_error = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                span.set_attribute('delivery.attempt', attempt)
                try:
                    response = put_order(client, envelope)
                    if response.status_code in (200, 201):
                        log.info('%s applied (%s)', order_number, response.json().get('status'))
                        return 'ok'
                    if response.status_code == 409:
                        log.info('%s already stored with newer LastUpdated (409)', order_number)
                        return 'conflict'
                    last_error = f'HTTP {response.status_code}: {response.text[:200]}'
                except httpx.HTTPError as exc:
                    last_error = str(exc)

                if attempt < MAX_ATTEMPTS:
                    sleep_for = 2 ** (attempt - 1)
                    log.warning(
                        '%s attempt %s/%s failed (%s); retry in %ss',
                        order_number, attempt, MAX_ATTEMPTS, last_error, sleep_for,
                    )
                    time.sleep(sleep_for)

            send_to_failed(producer, value, key, f'delivery_failed: {last_error}')
            log.error(
                '%s sent to %s after %s attempts: %s',
                order_number, DLQ_TOPIC, MAX_ATTEMPTS, last_error,
            )
            span.set_attribute('delivery.failed', True)
            return 'failed'
    finally:
        detach_context(token)


# Long-running loop: poll Kafka, process, commit offset.
def run() -> None:
    tracer = init_tracing('target-subscriber')
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    producer = Producer({'bootstrap.servers': BOOTSTRAP})
    consumer.subscribe([TOPIC])
    log.info('subscribed to %s as group %s -> %s', TOPIC, GROUP_ID, TARGET_URL)

    with httpx.Client() as client:
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                process_message(
                    client,
                    producer,
                    tracer,
                    key=msg.key(),
                    value=msg.value(),
                    headers=msg.headers(),
                )
                consumer.commit(asynchronous=False)
        except KeyboardInterrupt:
            log.info('stopped')
        finally:
            consumer.close()


if __name__ == '__main__':
    run()
