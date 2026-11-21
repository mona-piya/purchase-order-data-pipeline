"""Overview: shared fixtures for tests/ (synthetic PO JSON under data/).

Loads:
  - initial  → purchased_order_initial.json (~100 orders)
  - incremental → purchased_order_incremental.json (~20 orders, includes dupes/replays)

Used by API, publish, dedupe, model, and subscriber tests.
"""

import json
import os
from pathlib import Path

import pytest

# Unit tests must not talk to a local OTLP collector.
os.environ.setdefault('OTEL_SDK_DISABLED', 'true')

from services.common.models import PurchaseOrderExtract

DATA = Path(__file__).resolve().parents[1] / 'data'


# Full initial extract (session-scoped — parse once).
@pytest.fixture(scope='session')
def initial():
    return PurchaseOrderExtract.model_validate(json.loads((DATA / 'purchased_order_initial.json').read_text()))


# Incremental extract with later versions / replays for dedupe tests.
@pytest.fixture(scope='session')
def incremental():
    return PurchaseOrderExtract.model_validate(
        json.loads((DATA / 'purchased_order_incremental.json').read_text()),
    )
