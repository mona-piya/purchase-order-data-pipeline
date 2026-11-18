from fastapi.testclient import TestClient
from services.source_api.main import app

client = TestClient(app)


def test_health():
    assert client.get('/health').json() == {'status': 'ok'}


def test_initial_and_incremental():
    for load_type, count in [('initial', 100), ('incremental', 20)]:
        response = client.get('/purchase-orders', params={'load_type': load_type})
        assert response.status_code == 200
        assert response.json()['record_count'] == count
        assert len(response.json()['purchased_orders']) == count


def test_changed_since_uses_utc_instant():
    response = client.get('/purchase-orders', params={
        'load_type': 'incremental', 'changed_since': '2026-11-15T01:15:00Z',
    })
    assert response.status_code == 200
    matches = [o for o in response.json()['purchased_orders'] if o['OrderNumber'] == 'PO1040078']
    assert len(matches) == 1
    assert matches[0]['LastUpdated'].startswith('2026-11-15T01:30:00')


def test_bad_filter_and_load_type():
    assert client.get('/purchase-orders', params={'changed_since': 'yesterday'}).status_code == 422
    assert client.get('/purchase-orders', params={'load_type': 'wrong'}).status_code == 422
