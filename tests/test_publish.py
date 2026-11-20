"""Overview: unit tests for services.common.publish (Kafka envelope + produce).

Uses a FakeProducer — no real broker.
Checks:
  - envelope carries batch_id, load_type, schema_version, Zulu last_updated, order
  - publish_orders keys messages by OrderNumber on the purchase-orders topic
"""

import json

from services.common.publish import TOPIC, envelope, publish_orders


# In-memory Kafka producer stub that records produce() calls.
class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, on_delivery=None):
        self.messages.append((topic, key, value))
        if on_delivery is not None:
            on_delivery(None, None)

    def poll(self, _timeout):
        return None

    def flush(self):
        return 0


# Envelope shape used by Airflow publish and scripts/publish_initial.py.
def test_envelope_carries_batch_and_order(initial):
    order = initial.purchased_orders[0]
    body = envelope(order, batch_id='batch-1', load_type='initial', extracted_at=initial.extracted_at)
    assert body['batch_id'] == 'batch-1'
    assert body['source_load_type'] == 'initial'
    assert body['schema_version'] == '1'
    assert body['last_updated_utc'].endswith('Z')
    assert body['order']['OrderNumber'] == order.OrderNumber


# One Kafka message per order; key = OrderNumber for partition affinity.
def test_publish_keys_by_order_number(initial):
    orders = initial.purchased_orders[:2]
    producer = FakeProducer()
    count = publish_orders(
        producer, orders, batch_id='batch-1', load_type='initial', extracted_at=initial.extracted_at,
    )
    assert count == 2
    topics = {topic for topic, _key, _value in producer.messages}
    assert topics == {TOPIC}
    keys = [key.decode() for _topic, key, _value in producer.messages]
    assert keys == [order.OrderNumber for order in orders]
    body = json.loads(producer.messages[0][2])
    assert body['batch_id'] == 'batch-1'
