import json
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from services.common.models import PurchaseOrderExtract, parse_timestamp

DATA_DIR = Path(os.getenv('PO_DATA_DIR', Path(__file__).resolve().parents[2] / 'data'))
FILES = {
    'initial': 'purchased_order_initial.json',
    'incremental': 'purchased_order_incremental.json',
}
app = FastAPI(title='Purchase Order Source API', version='0.1.0')


@lru_cache(maxsize=2)
def read_extract(load_type: str) -> PurchaseOrderExtract:
    path = DATA_DIR / FILES[load_type]
    try:
        return PurchaseOrderExtract.model_validate(json.loads(path.read_text(encoding='utf-8')))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f'Cannot load {path}: {exc}') from exc


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/purchase-orders', response_model=PurchaseOrderExtract)
def get_purchase_orders(
    load_type: Literal['initial', 'incremental'] = 'initial',
    changed_since: str | None = Query(default=None, description='ISO 8601 timestamp with offset'),
):
    try:
        extract = read_extract(load_type)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    if changed_since is None:
        return extract  # Return the original file, including duplicate versions.
    try:
        cutoff: datetime = parse_timestamp(changed_since)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    filtered = [order for order in extract.purchased_orders if order.LastUpdated > cutoff]
    return extract.model_copy(update={
        'record_count': len(filtered), 'purchased_orders': filtered,
    })
