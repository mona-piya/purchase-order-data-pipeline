"""Overview: HTTP tests for services.target_api (SQLite upsert + LastUpdated guard).

Uses FastAPI TestClient with a temp SQLite file — no Docker.
Checks:
  - PUT creates; GET / list returns count + items with batch_id
  - older LastUpdated → 409 older_version_rejected (stored row unchanged)
  - same LastUpdated → 200 updated (idempotent overwrite, batch_id can change)
  - path OrderNumber mismatch → 400
"""

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from services.common.models import PurchaseOrder
from services.target_api import main as target_main


# Fresh SQLite DB per test via tmp_path.
def _client(tmp_path):
    target_main._conn = None
    target_main.DB_PATH = tmp_path / 'target.db'
    return TestClient(target_main.app)


def _body(order: PurchaseOrder, batch_id: str = 'batch-1') -> dict:
    return {'batch_id': batch_id, 'order': order.model_dump(mode='json')}


def test_put_creates_and_get_returns(initial, tmp_path):
    client = _client(tmp_path)
    order = initial.purchased_orders[0]
    r = client.put(f'/purchase-orders/{order.OrderNumber}', json=_body(order))
    assert r.status_code == 200
    assert r.json()['status'] == 'created'
    got = client.get(f'/purchase-orders/{order.OrderNumber}')
    assert got.status_code == 200
    assert got.json()['batch_id'] == 'batch-1'
    assert got.json()['order']['OrderNumber'] == order.OrderNumber
    listing = client.get('/purchase-orders').json()
    assert listing['count'] == 1
    assert listing['items'][0]['order_number'] == order.OrderNumber
    assert listing['items'][0]['batch_id'] == 'batch-1'


# Incoming older than stored → reject; keep original status and batch_id.
def test_put_older_last_updated_returns_409(initial, tmp_path):
    client = _client(tmp_path)
    order = initial.purchased_orders[0]
    assert client.put(f'/purchase-orders/{order.OrderNumber}', json=_body(order)).status_code == 200

    older = order.model_copy(update={
        'LastUpdated': datetime(2020, 1, 1, tzinfo=timezone.utc),
        'OrderStatus': 'ShouldNotWin',
    })
    r = client.put(f'/purchase-orders/{order.OrderNumber}', json=_body(older, batch_id='batch-old'))
    assert r.status_code == 409
    detail = r.json()['detail']
    assert detail['reason'] == 'older_version_rejected'
    assert detail['order_number'] == order.OrderNumber
    assert detail['incoming_last_updated'] == '2020-01-01T00:00:00Z'
    assert detail['stored_last_updated'] == order.LastUpdated.isoformat().replace('+00:00', 'Z')
    stored = client.get(f'/purchase-orders/{order.OrderNumber}').json()
    assert stored['order']['OrderStatus'] == order.OrderStatus
    assert stored['batch_id'] == 'batch-1'


# Equal LastUpdated is allowed; batch_id updates to the latest successful write.
def test_put_same_last_updated_is_idempotent(initial, tmp_path):
    client = _client(tmp_path)
    order = initial.purchased_orders[0]
    assert client.put(f'/purchase-orders/{order.OrderNumber}', json=_body(order)).status_code == 200
    r = client.put(f'/purchase-orders/{order.OrderNumber}', json=_body(order, batch_id='batch-2'))
    assert r.status_code == 200
    assert r.json()['status'] == 'updated'
    assert client.get(f'/purchase-orders/{order.OrderNumber}').json()['batch_id'] == 'batch-2'


def test_path_mismatch_returns_400(initial, tmp_path):
    client = _client(tmp_path)
    order = initial.purchased_orders[0]
    r = client.put('/purchase-orders/WRONG', json=_body(order))
    assert r.status_code == 400
