"""Versioned v1 event projection. The dashboard stays on its legacy queues."""
from uuid import UUID, NAMESPACE_URL, uuid5

ROUTES = {
    "InventoryReserved": "inventory.stock-reserved.v1",
    "LowStockDetected": "replenishment.risk-detected.v1",
}

def _correlation_uuid(value):
    try:
        return str(UUID(value))
    except ValueError:
        return str(uuid5(NAMESPACE_URL, f"urn:smart-inventory:correlation:{value}"))

def versioned_event(event):
    route = ROUTES.get(event["eventType"])
    if route is None:
        return None
    p = event["payload"]
    if event["eventType"] == "InventoryReserved":
        payload = {"reservationId": p["reservation_id"], "sku": p["sku"], "qty": p["quantity"],
                   "location": p["location"],
                   "lots": [{"lot": a["lot"], "qty": a["quantity"]} for a in p["allocations"]]}
    else:
        payload = {"suggestionId": p["suggestion_id"], "sku": p["sku"], "location": p["location"],
                   "available": p["available"], "reorderPoint": p["reorder_point"],
                   "supplier": p["supplier"], "suggestedQty": p["suggested_quantity"]}
    return route, {**event, "eventType": route.removesuffix(".v1"), "schemaVersion": "1.0.0",
                   "correlationId": _correlation_uuid(event["correlationId"]), "payload": payload}
