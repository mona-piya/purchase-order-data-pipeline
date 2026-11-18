import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from services.common.models import PurchaseOrder

TOPIC = 'purchase-orders'
DLQ_TOPIC = 'purchase-orders-failed'


def envelope(order: PurchaseOrder, *, batch_id: str, load_type: str, extracted_at: datetime) -> dict:
    """One Kafka value: the order document plus the fields ADR-003 requires."""
    return {
        'batch_id': batch_id,
        'event_id': str(uuid.uuid4()),
        'source_load_type': load_type,
        'extracted_at': _utc_z(extracted_at),
        'last_updated_utc': _utc_z(order.LastUpdated),
        'schema_version': '1',
        'order': order.model_dump(mode='json'),
    }


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer({'bootstrap.servers': bootstrap_servers})


def publish_orders(
    producer,
    orders: list[PurchaseOrder],
    *,
    batch_id: str,
    load_type: str,
    extracted_at: datetime,
    topic: str = TOPIC,
) -> int:
    """Publish one message per order. Key is OrderNumber, so one order stays on one partition."""
    errors: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))

    for order in orders:
        producer.produce(
            topic,
            key=order.OrderNumber.encode(),
            value=json.dumps(envelope(
                order, batch_id=batch_id, load_type=load_type, extracted_at=extracted_at,
            )).encode(),
            on_delivery=on_delivery,
        )
        producer.poll(0)
    producer.flush()
    if errors:
        raise RuntimeError(errors[0])
    return len(orders)


def _utc_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
