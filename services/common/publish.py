import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from services.common.models import PurchaseOrder
from services.common.tracing import init_tracing, kafka_inject_headers, set_span_error

TOPIC = 'purchase-orders'
DLQ_TOPIC = 'purchase-orders-failed'


# Build one Kafka message value (ADR-003 envelope + full order document).
def envelope(order: PurchaseOrder, *, batch_id: str, load_type: str, extracted_at: datetime) -> dict:
    return {
        'batch_id': batch_id,
        'event_id': str(uuid.uuid4()),
        'source_load_type': load_type,
        'extracted_at': _utc_z(extracted_at),
        'last_updated_utc': _utc_z(order.LastUpdated),
        'schema_version': '1',
        'order': order.model_dump(mode='json'),
    }


# Create a Kafka producer for the given bootstrap servers.
def make_producer(bootstrap_servers: str) -> Producer:
    return Producer({'bootstrap.servers': bootstrap_servers})


# Publish one message per order (key = OrderNumber). Returns how many were sent.
# Each message gets a po.publish span and a W3C traceparent Kafka header.
def publish_orders(
    producer,
    orders: list[PurchaseOrder],
    *,
    batch_id: str,
    load_type: str,
    extracted_at: datetime,
    topic: str = TOPIC,
) -> int:
    tracer = init_tracing(os_service_name())
    errors: list[str] = []

    # Collect async delivery failures so we can raise after flush.
    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))

    for order in orders:
        with tracer.start_as_current_span('po.publish') as span:
            span.set_attribute('order_number', order.OrderNumber)
            span.set_attribute('batch_id', batch_id)
            span.set_attribute('messaging.system', 'kafka')
            span.set_attribute('messaging.destination', topic)
            try:
                producer.produce(
                    topic,
                    key=order.OrderNumber.encode(),
                    value=json.dumps(envelope(
                        order, batch_id=batch_id, load_type=load_type, extracted_at=extracted_at,
                    )).encode(),
                    headers=kafka_inject_headers(),
                    on_delivery=on_delivery,
                )
                producer.poll(0)
            except Exception as exc:  # noqa: BLE001 — record on span then re-raise
                set_span_error(span, exc)
                raise
    producer.flush()
    if errors:
        raise RuntimeError(errors[0])
    return len(orders)


def os_service_name() -> str:
    return os.getenv('OTEL_SERVICE_NAME', 'po-publisher')


# Format a datetime as UTC ISO-8601 with a trailing Z.
def _utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
