import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from opentelemetry import trace
from opentelemetry.propagate import extract
from pydantic import BaseModel, Field

from services.common.models import PurchaseOrder
from services.common.tracing import init_tracing
from services.target_api import db

tracer = init_tracing('target-api')

DB_PATH = Path(os.getenv('TARGET_DB_PATH', Path(__file__).resolve().parents[2] / 'data' / 'target.db'))
app = FastAPI(title='Purchase Order Target API', version='0.1.0')
_conn = None


# Lazily open the SQLite connection used by all handlers.
def get_conn():
    global _conn
    if _conn is None:
        _conn = db.connect(DB_PATH)
    return _conn


# Request body for PUT: Kafka batch_id plus the full order document.
class UpsertBody(BaseModel):
    batch_id: str = Field(min_length=1)
    order: PurchaseOrder


@app.middleware('http')
async def otel_http_middleware(request: Request, call_next):
    """Continue trace from inbound W3C headers (injected by the subscriber's httpx client)."""
    ctx = extract(dict(request.headers))
    with tracer.start_as_current_span(
        f'{request.method} {request.url.path}',
        context=ctx,
        kind=trace.SpanKind.SERVER,
    ) as span:
        span.set_attribute('http.method', request.method)
        span.set_attribute('http.route', request.url.path)
        response = await call_next(request)
        span.set_attribute('http.status_code', response.status_code)
        return response


# Liveness check.
@app.get('/health')
def health():
    return {'status': 'ok'}


# List stored orders; optional batch_id filter for Airflow wait_for_target_api.
@app.get('/purchase-orders')
def list_purchase_orders(batch_id: str | None = None):
    conn = get_conn()
    items = db.list_orders(conn, batch_id=batch_id)
    body = {'count': len(items), 'items': items}
    if batch_id is not None:
        body['batch_id'] = batch_id
    return body


# Fetch one order by OrderNumber (404 if missing).
@app.get('/purchase-orders/{order_number}')
def get_purchase_order(order_number: str):
    row = db.get_order(get_conn(), order_number)
    if row is None:
        raise HTTPException(status_code=404, detail=f'{order_number} not found')
    return row


# Upsert one order. 200 created/updated; 409 if stored LastUpdated is newer.
@app.put('/purchase-orders/{order_number}')
def put_purchase_order(order_number: str, body: UpsertBody):
    if body.order.OrderNumber != order_number:
        raise HTTPException(
            status_code=400,
            detail='path OrderNumber does not match body order.OrderNumber',
        )
    span = trace.get_current_span()
    if span is not None:
        span.set_attribute('order_number', order_number)
        span.set_attribute('batch_id', body.batch_id)
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
