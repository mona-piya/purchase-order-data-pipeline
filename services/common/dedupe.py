from collections.abc import Iterable
from services.common.models import PurchaseOrder


# Keep the newest LastUpdated per OrderNumber (ties keep the first seen).
def dedupe_orders(orders: Iterable[PurchaseOrder]) -> list[PurchaseOrder]:
    newest: dict[str, PurchaseOrder] = {}
    for order in orders:
        previous = newest.get(order.OrderNumber)
        if previous is None or order.LastUpdated > previous.LastUpdated:
            newest[order.OrderNumber] = order
    return list(newest.values())
