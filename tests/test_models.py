"""Overview: unit tests for services.common.models (Pydantic + timestamps).

Checks:
  - initial/incremental counts and field types from synthetic JSON
  - parse_timestamp compares across offsets; rejects naive timestamps
  - PurchaseOrder rejects bad LastUpdated values
"""

from datetime import timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from services.common.models import PurchaseOrder, parse_timestamp


# Smoke: fixture sizes + string line fields + Decimal receipts + UTC LastUpdated.
def test_source_counts_and_types(initial, incremental):
    assert len(initial.purchased_orders) == 100
    assert len(incremental.purchased_orders) == 20
    line = initial.purchased_orders[0].OrderLines[0]
    assert isinstance(line.LineNumber, str)
    assert isinstance(line.Quantity, str)
    receipts = [r for o in initial.purchased_orders for line in o.OrderLines for r in line.GoodsReceipts]
    assert receipts and isinstance(receipts[0].ReceivedQuantity, Decimal)
    assert initial.purchased_orders[0].LastUpdated.tzinfo == timezone.utc


# Offset-aware compare: +10:00 morning is before 01:30Z the same calendar day in UTC terms as coded.
def test_offset_comparison():
    assert parse_timestamp('2026-11-15T11:00:00+10:00') < parse_timestamp('2026-11-15T01:30:00Z')
    with pytest.raises(ValueError):
        parse_timestamp('2026-11-15T01:30:00')


# Invalid LastUpdated must fail validation (not silently coerce).
def test_reject_bad_timestamp(initial):
    data = initial.purchased_orders[0].model_dump(mode='json')
    data['LastUpdated'] = 'not-a-date'
    with pytest.raises(ValidationError):
        PurchaseOrder.model_validate(data)
