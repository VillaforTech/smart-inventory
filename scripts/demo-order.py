"""Insert one explicit synthetic OrderStatusChanged event; no network calls."""
import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from inventory.store import Store
from inventory.worker import now, validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.environ.get("INVENTORY_DB"), help="local SQLite file")
    parser.add_argument("--order-id", required=True, help="fictional order ID beginning DEMO-")
    parser.add_argument("--location", required=True)
    parser.add_argument("--status", required=True, choices=("CREATED", "PREPARING", "READY", "SHIPPED", "CANCELLED"))
    args = parser.parse_args()
    if not args.db:
        parser.error("set --db or INVENTORY_DB")
    if not args.order_id.startswith("DEMO-") or len(args.order_id) == 5:
        parser.error("--order-id must be fictional and start with DEMO-")
    event = validate({
        "eventId": str(uuid4()),
        "eventType": "OrderStatusChanged",
        "schemaVersion": 1,
        "occurredAt": now(),
        "correlationId": str(uuid4()),
        "producer": "order-demo-stub",
        "payload": {"order_id": args.order_id, "location": args.location, "status": args.status},
    })
    Store(args.db)
    with sqlite3.connect(args.db, timeout=10) as db:
        db.execute(
            "INSERT INTO outbox (event_id,event_type,occurred_at,envelope) VALUES (?,?,?,?)",
            (event["eventId"], event["eventType"], event["occurredAt"], json.dumps(event)),
        )
    print(json.dumps(event, ensure_ascii=False))


if __name__ == "__main__":
    main()
