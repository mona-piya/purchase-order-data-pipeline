"""Overview: unit tests for services.target_subscriber.process_message.

Mocks Target API with httpx.MockTransport; FakeProducer captures DLQ.
Checks:
  - 200 from Target API → result 'ok', no DLQ
  - 409 conflict → treated as success ('conflict'), no retry/DLQ
  - bad Kafka envelope → 'failed' and message on purchase-orders-failed
"""

import json

import httpx

from opentelemetry import trace

from services.common.publish import DLQ_TOPIC
from services.target_subscriber.main import process_message

TRACER = trace.get_tracer('test-target-subscriber')


# Records produce() calls (including DLQ).
class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key=None, value=None, headers=None):
        self.messages.append({'topic': topic, 'key': key, 'value': value, 'headers': headers})

    def flush(self, _timeout=None):
        return 0


def _envelope(order: dict, batch_id: str = 'batch-1') -> bytes:
    return json.dumps({
        'batch_id': batch_id,
        'event_id': 'e1',
        'source_load_type': 'initial',
        'extracted_at': '2026-01-01T00:00:00Z',
        'last_updated_utc': order['LastUpdated'],
        'schema_version': '1',
        'order': order,
    }).encode()


def test_process_ok(initial, monkeypatch):
    order = initial.purchased_orders[0].model_dump(mode='json')
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={'status': 'created'})

    transport = httpx.MockTransport(handler)
    producer = FakeProducer()
    with httpx.Client(transport=transport) as client:
        result = process_message(
            client, producer, TRACER,
            key=order['OrderNumber'].encode(), value=_envelope(order), headers=None,
        )
    assert result == 'ok'
    assert len(calls) == 1
    assert producer.messages == []


# 409 means target already has a newer/equal version — commit offset, do not DLQ.
def test_process_409_is_success(initial, monkeypatch):
    order = initial.purchased_orders[0].model_dump(mode='json')

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={'detail': {'reason': 'older_version_rejected'}})

    producer = FakeProducer()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = process_message(
            client, producer, TRACER,
            key=order['OrderNumber'].encode(), value=_envelope(order), headers=None,
        )
    assert result == 'conflict'
    assert producer.messages == []


def test_bad_envelope_goes_to_failed_topic():
    producer = FakeProducer()
    with httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as client:
        result = process_message(
            client, producer, TRACER, key=b'x', value=b'not-json', headers=None,
        )
    assert result == 'failed'
    assert producer.messages[0]['topic'] == DLQ_TOPIC
