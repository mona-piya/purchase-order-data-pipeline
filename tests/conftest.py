import json
from pathlib import Path
import pytest
from services.common.models import PurchaseOrderExtract

DATA = Path(__file__).resolve().parents[1] / 'data'


@pytest.fixture(scope='session')
def initial():
    return PurchaseOrderExtract.model_validate(json.loads((DATA / 'purchased_order_initial.json').read_text()))


@pytest.fixture(scope='session')
def incremental():
    return PurchaseOrderExtract.model_validate(json.loads((DATA / 'purchased_order_incremental.json').read_text()))
