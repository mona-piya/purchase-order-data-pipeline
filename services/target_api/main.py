import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from services.common.models import PurchaseOrder
from services.target_api import db

DB_PATH = Path(os.getenv('TARGET_DB_PATH', Path(__file__).resolve().parents[2] / 'data' / 'target.db'))
app = FastAPI(title='Purchase Order Target API', version='0.1.0')
_conn = None


def get_conn():
    global _conn
    if _conn is None:
        _conn = db.connect(DB_PATH)
    return _conn


class UpsertBody(BaseModel):
    batch_id: str = Field(min_length=1)
    order: PurchaseOrder


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/purchase-orders')
def list_purchase_orders(batch_id: str | None = None):
    """List stored purchase orders. Optional batch_id filter for Airflow wait_for_target."""
    conn = get_conn()
    items = db.list_orders(conn, batch_id=batch_id)
    body = {'count': len(items), 'items': items}
    if batch_id is not None:
        body['batch_id'] = batch_id
    return body


@app.get('/purchase-orders/{order_number}')
def get_purchase_order(order_number: str):
    row = db.get_order(get_conn(), order_number)
    if row is None:
        raise HTTPException(status_code=404, detail=f'{order_number} not found')
    return row


@app.put('/purchase-orders/{order_number}')
def put_purchase_order(order_number: str, body: UpsertBody):
    if body.order.OrderNumber != order_number:
        raise HTTPException(
            status_code=400,
            detail='path OrderNumber does not match body order.OrderNumber',
        )
    result = db.upsert(
        get_conn(),
        order_number=order_number,
        last_updated=body.order.LastUpdated,
        batch_id=body.batch_id,
        document=body.order.model_dump(mode='json'),
    )
    if result == 'rejected':
        stored = db.get_order(get_conn(), order_number)
        raise HTTPException(
            status_code=409,
            detail={
                'reason': 'older_version_rejected',
                'message': (
                    f'{order_number} was not updated: the stored purchase order is newer '
                    f'than the one you sent'
                ),
                'order_number': order_number,
                'stored_last_updated': stored['last_updated'] if stored else None,
                'incoming_last_updated': body.order.LastUpdated.isoformat().replace('+00:00', 'Z'),
                'stored_batch_id': stored['batch_id'] if stored else None,
                'incoming_batch_id': body.batch_id,
            },
        )
    return {'status': result, 'order_number': order_number, 'batch_id': body.batch_id}
