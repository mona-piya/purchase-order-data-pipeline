from collections.abc import Iterable
from services.common.models import PurchaseOrder


def dedupe_orders(orders: Iterable[PurchaseOrder]) -> list[PurchaseOrder]:
    """Keep the latest UTC LastUpdated for each order; ties keep first seen."""
    newest: dict[str, PurchaseOrder] = {}
    for order in orders:
        previous = newest.get(order.OrderNumber)
        if previous is None or order.LastUpdated > previous.LastUpdated:
            newest[order.OrderNumber] = order
    return list(newest.values())
