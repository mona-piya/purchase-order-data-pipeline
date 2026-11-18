from services.common.dedupe import dedupe_orders


def test_incremental_keeps_newest_not_last_position(incremental):
    orders = incremental.purchased_orders
    assert orders[7].OrderNumber == orders[17].OrderNumber == 'PO1040078'
    assert orders[7].LastUpdated > orders[17].LastUpdated
    deduped = dedupe_orders(orders)
    assert len(deduped) == 19
    selected = next(o for o in deduped if o.OrderNumber == 'PO1040078')
    assert selected.LastUpdated == orders[7].LastUpdated
    receipts = {r.GoodsReceiptNumber for line in selected.OrderLines for r in line.GoodsReceipts}
    assert 'GR700192' in receipts


def test_unchanged_replay(initial, incremental):
    old = next(o for o in initial.purchased_orders if o.OrderNumber == 'PO1040000')
    replay = next(o for o in incremental.purchased_orders if o.OrderNumber == 'PO1040000')
    assert old == replay
    deduped = dedupe_orders([old, replay])
    assert len(deduped) == 1
    assert deduped[0] == old
