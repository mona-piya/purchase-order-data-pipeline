"""Overview: unit tests for services.snowflake_subscriber parse + DLQ helpers.

No live Snowflake — FakeMsg / FakeProducer only.
Checks:
  - parse_row builds PendingRow bind values (order + Kafka metadata)
  - bad envelope returns an error string (caller sends to DLQ)
  - send_to_failed produces to purchase-orders-failed
"""

import json

from services.common.publish import DLQ_TOPIC
from services.snowflake_subscriber.main import PendingRow, parse_row, send_to_failed


# Minimal confluent_kafka.Message stand-in.
class FakeMsg:
    def __init__(self, value, *, topic='purchase-orders', partition=0, offset=7, key=b'PO1'):
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key


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


# Happy path: values align with RAW.PURCHASE_ORDER_PAYLOADS insert columns.
def test_parse_row_ok(initial):
    order = initial.purchased_orders[0].model_dump(mode='json')
    msg = FakeMsg(_envelope(order), key=order['OrderNumber'].encode())
    result = parse_row(msg)
    assert isinstance(result, PendingRow)
    assert result.values[0] == order['OrderNumber']
    assert result.values[1] == 'batch-1'
    assert result.values[2] == 'initial'
    assert result.values[3] == '2026-01-01T00:00:00Z'
    assert result.values[4] == order['LastUpdated']
    assert json.loads(result.values[5])['OrderNumber'] == order['OrderNumber']
    assert result.values[6] == 'purchase-orders'
    assert result.values[7] == 0
    assert result.values[8] == 7
    assert result.values[9] is None


def test_parse_row_bad_envelope():
    result = parse_row(FakeMsg(b'not-json'))
    assert isinstance(result, str)
    assert result.startswith('bad_envelope:')


def test_bad_envelope_dlq_helper():
    producer = FakeProducer()
    send_to_failed(producer, b'{}', b'x', 'bad_envelope: x')
    assert producer.messages[0]['topic'] == DLQ_TOPIC
