"""RabbitMQ outbox publisher and durable dashboard projection."""
import argparse
import json
import logging
import os
import sqlite3
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone

EXCHANGE = "inventory.events"
RETRY_EXCHANGE = "inventory.retry"
DEAD_EXCHANGE = "inventory.dead"
QUEUE = "inventory.dashboard"
DEAD_QUEUE = "inventory.dashboard.dead"
RETRY_QUEUES = (("inventory.dashboard.retry.1", 1000), ("inventory.dashboard.retry.2", 5000))
ROUTES = {
    "InventoryReserved": "inventory.reserved",
    "LowStockDetected": "inventory.low-stock",
    "ReplenishmentApproved": "inventory.replenishment-approved",
    "OrderStatusChanged": "order.status-changed",
}
KEYS = {"eventId", "eventType", "schemaVersion", "occurredAt", "correlationId", "producer", "payload"}
MAX_BODY = 65_536
HEARTBEAT_MAX_AGE = 20
LOG = logging.getLogger("inventory.worker")

DDL = """
CREATE TABLE IF NOT EXISTS dashboard_inbox (
  event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL,
  received_at TEXT NOT NULL, occurred_at TEXT NOT NULL,
  correlation_id TEXT NOT NULL, envelope TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dashboard_orders (
  order_id TEXT PRIMARY KEY, location TEXT NOT NULL,
  status TEXT NOT NULL, occurred_at TEXT NOT NULL, event_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dashboard_worker_state (
  key TEXT PRIMARY KEY, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dashboard_worker_counters (
  name TEXT PRIMARY KEY, count INTEGER NOT NULL
);
"""


def now():
    return datetime.now(timezone.utc).isoformat()


def seconds_since(timestamp):
    if not timestamp:
        return None
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def validate(envelope):
    if not isinstance(envelope, dict) or set(envelope) != KEYS:
        raise ValueError("invalid event envelope")
    for key in ("eventId", "eventType", "occurredAt", "correlationId", "producer"):
        if not isinstance(envelope[key], str) or not envelope[key].strip() or len(envelope[key]) > 250:
            raise ValueError(f"invalid {key}")
    if envelope["eventType"] not in ROUTES or type(envelope["schemaVersion"]) is not int or envelope["schemaVersion"] != 1:
        raise ValueError("unsupported event version or type")
    if not isinstance(envelope["payload"], dict):
        raise ValueError("invalid payload")
    seconds_since(envelope["occurredAt"])
    if envelope["eventType"] == "OrderStatusChanged":
        payload = envelope["payload"]
        if set(payload) != {"order_id", "location", "status"} or any(
            not isinstance(value, str) or not value.strip() or len(value) > 100
            for value in payload.values()
        ):
            raise ValueError("invalid order status payload")
    return envelope


def decode(body):
    if len(body) > MAX_BODY:
        raise ValueError("event exceeds limit")
    return validate(json.loads(body))


def encode(envelope):
    body = json.dumps(validate(envelope), ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    if len(body) > MAX_BODY:
        raise ValueError("event exceeds limit")
    return body


@contextmanager
def database(path):
    with closing(sqlite3.connect(path, timeout=10)) as db:
        db.row_factory = sqlite3.Row
        db.executescript(DDL)
        with db:
            yield db


def put(db, key, value):
    db.execute(
        "INSERT INTO dashboard_worker_state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


def inc(db, counter):
    db.execute(
        "INSERT INTO dashboard_worker_counters VALUES (?,1) ON CONFLICT(name) DO UPDATE SET count=count+1",
        (counter,),
    )


def error(path, code, counter=None):
    with database(path) as db:
        put(db, "last_error", code)
        put(db, "last_error_at", now())
        if counter:
            inc(db, counter)


def heartbeat(path, connected=True):
    with database(path) as db:
        put(db, "last_seen", now())
        put(db, "connected", int(connected))


def broker_health(path):
    with database(path) as db:
        s = dict(db.execute("SELECT key,value FROM dashboard_worker_state").fetchall())
    age = seconds_since(s.get("last_seen"))
    return {
        "healthy": s.get("connected") == "1" and age is not None and age < HEARTBEAT_MAX_AGE,
        "last_seen": s.get("last_seen"),
        "last_error": s.get("last_error"),
    }


def project_event(path, envelope):
    """Atomic inbox and materialization. Duplicate deliveries have no activity side effects."""
    encoded = encode(envelope).decode()
    with database(path) as db:
        cursor = db.execute(
            "INSERT INTO dashboard_inbox VALUES (?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING",
            (envelope["eventId"], envelope["eventType"], now(), envelope["occurredAt"], envelope["correlationId"], encoded),
        )
        new = cursor.rowcount == 1
        if not new:
            existing = db.execute("SELECT envelope FROM dashboard_inbox WHERE event_id=?", (envelope["eventId"],)).fetchone()
            if json.loads(existing[0]) != envelope:
                raise ValueError("event ID reused for different content")
        inc(db, "processed" if new else "duplicates")
        if new:
            if envelope["eventType"] == "OrderStatusChanged":
                payload = envelope["payload"]
                stamp = datetime.fromisoformat(envelope["occurredAt"].replace("Z", "+00:00"))
                ordered = stamp.astimezone(timezone.utc).isoformat(timespec="microseconds")
                db.execute(
                    """INSERT INTO dashboard_orders VALUES (?,?,?,?,?)
                    ON CONFLICT(order_id) DO UPDATE SET location=excluded.location, status=excluded.status,
                      occurred_at=excluded.occurred_at, event_id=excluded.event_id
                    WHERE (excluded.occurred_at,excluded.event_id) >
                          (dashboard_orders.occurred_at,dashboard_orders.event_id)""",
                    (payload["order_id"], payload["location"], payload["status"], ordered, envelope["eventId"]),
                )
            put(db, "last_sync", now())
            put(db, "last_event_lag_seconds", seconds_since(envelope["occurredAt"]))
        return new


def projection(path):
    """Read-only view of inventory from the committed inbox plus worker/backlog status."""
    with database(path) as db:
        db.execute("BEGIN")
        s = dict(db.execute("SELECT key,value FROM dashboard_worker_state").fetchall())
        metrics = dict(db.execute("SELECT name,count FROM dashboard_worker_counters").fetchall())
        events_by_type = dict(db.execute("SELECT event_type,COUNT(*) FROM dashboard_inbox GROUP BY event_type").fetchall())
        orders = [dict(row) for row in db.execute(
            "SELECT order_id, location, status, occurred_at, event_id FROM dashboard_orders ORDER BY order_id"
        )]
        queued_rows = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbox'"
        ).fetchone()
        unprojected = dict(db.execute(
            """SELECT COUNT(*) AS backlog, MIN(o.occurred_at) AS oldest FROM outbox o
               LEFT JOIN dashboard_inbox i ON i.event_id = o.event_id
               WHERE i.event_id IS NULL"""
        ).fetchone()) if queued_rows else {}
        pending = dict(db.execute(
            """SELECT COUNT(*) AS pending_count, MIN(occurred_at) AS oldest_pending_at
               FROM outbox WHERE published_at IS NULL"""
        ).fetchone()) if queued_rows else {}
        recent_events = [
            json.loads(row[0])
            for row in db.execute("SELECT envelope FROM dashboard_inbox ORDER BY rowid DESC LIMIT 30")
        ]
    oldest = pending.get("oldest_pending_at")
    oldest_unprojected = unprojected.get("oldest")
    last_delay = float(s["last_event_lag_seconds"]) if "last_event_lag_seconds" in s else None
    return {
        "last_sync": s.get("last_sync"),
        "last_error": s.get("last_error"),
        "last_error_at": s.get("last_error_at"),
        "lag_seconds": seconds_since(oldest_unprojected) if oldest_unprojected else last_delay,
        "events_processed": sum(events_by_type.values()),
        "events_by_type": events_by_type,
        "recent_events": recent_events,
        "orders": orders,
        "pending_outbox": pending.get("pending_count", 0),
        "oldest_pending_at": oldest,
        "unprojected_outbox": unprojected.get("backlog", 0),
        "oldest_unprojected_at": oldest_unprojected,
        "metrics": metrics,
    }


def declare(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    for exchange in (RETRY_EXCHANGE, DEAD_EXCHANGE):
        channel.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DEAD_QUEUE, durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DEAD_EXCHANGE, routing_key="dashboard")
    channel.queue_declare(
        queue=QUEUE, durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": DEAD_EXCHANGE,
            "x-dead-letter-routing-key": "dashboard",
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
            "x-delivery-limit": 20,
        },
    )
    for route in ROUTES.values():
        channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=route)
    for key, delay in RETRY_QUEUES:
        channel.queue_declare(
            queue=key, durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-message-ttl": delay,
                # Route retry only back to this consumer, not to other subscribers.
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": QUEUE,
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
            },
        )
        channel.queue_bind(queue=key, exchange=RETRY_EXCHANGE, routing_key=key)
    channel.confirm_delivery()


def confirmed(channel, exchange, routing_key, body, properties):
    # BlockingChannel raises on nack/unroutable, older API variants may return False.
    result = channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=body, properties=properties, mandatory=True
    )
    if result is False:
        raise RuntimeError("publish not confirmed")


def properties_for(envelope, properties_factory, attempt=0):
    return properties_factory(
        delivery_mode=2, content_type="application/json",
        message_id=envelope["eventId"], type=envelope["eventType"],
        correlation_id=envelope["correlationId"], headers={"dashboard-retry": attempt},
    )


def publish_pending(channel, store, path, properties_factory, limit=16):
    for event in store.pending_events(limit=limit):
        body = encode(event)
        confirmed(channel, EXCHANGE, ROUTES[event["eventType"]], body, properties_for(event, properties_factory))
        if store.mark_event_published(event["eventId"]):
            with database(path) as db:
                inc(db, "published")
            LOG.info("published message eventId=%r eventType=%s correlationId=%r",
                     event["eventId"], event["eventType"], event["correlationId"])


def process_delivery(channel, method, props, body, path, properties_factory, projector=project_event):
    tag = method.delivery_tag
    try:
        event = decode(body)
        if props.message_id and props.message_id != event["eventId"]:
            raise ValueError("message ID mismatch")
        attempt = (props.headers or {}).get("dashboard-retry", 0)
        if type(attempt) is not int or not 0 <= attempt <= len(RETRY_QUEUES):
            raise ValueError("invalid retry header")
    except (ValueError, TypeError, OverflowError):
        error(path, "invalid_envelope", "dead_lettered")
        channel.basic_nack(delivery_tag=tag, requeue=False)
        return
    try:
        applied = projector(path, event)
    except ValueError:
        error(path, "event_id_content_conflict", "dead_lettered")
        channel.basic_nack(delivery_tag=tag, requeue=False)
        return
    except sqlite3.Error:
        # A failed DB cannot record its own metrics; RabbitMQ deliveries remain authoritative.
        try:
            error(path, "projection_db_error", "projection_errors")
        except sqlite3.Error:
            LOG.warning("cannot persist projection error")
        if attempt == len(RETRY_QUEUES):
            channel.basic_nack(delivery_tag=tag, requeue=False)
            try:
                error(path, "projection_retry_exhausted", "dead_lettered")
            except sqlite3.Error:
                LOG.warning("cannot persist terminal metric")
            return
        try:
            confirmed(
                channel, RETRY_EXCHANGE, RETRY_QUEUES[attempt][0], body,
                properties_for(event, properties_factory, attempt + 1),
            )
        except Exception:
            channel.basic_nack(delivery_tag=tag, requeue=True)
            raise
        channel.basic_ack(delivery_tag=tag)
        try:
            with database(path) as db:
                inc(db, "retried")
        except sqlite3.Error:
            LOG.warning("cannot persist retry metric")
        return
    channel.basic_ack(delivery_tag=tag)
    LOG.info("consumed message eventId=%r eventType=%s correlationId=%r applied=%s",
             event["eventId"], event["eventType"], event["correlationId"], bool(applied))


def redrive(channel, event_id, accept, properties_factory):
    """Recover only the exact valid head, after the operator has corrected its cause."""
    if not accept or not event_id:
        raise ValueError("require --event-id and --confirm-cause-fixed")
    method, props, body = channel.basic_get(queue=DEAD_QUEUE, auto_ack=False)
    if method is None:
        raise ValueError("DLQ empty")
    try:
        event = decode(body)
        if event["eventId"] != event_id or (props.message_id and props.message_id != event_id):
            raise ValueError("DLQ head does not match event ID")
        confirmed(channel, "", QUEUE, body, properties_for(event, properties_factory))
    except Exception:
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        raise
    channel.basic_ack(delivery_tag=method.delivery_tag)


def run(path, url, recovery=None):
    import pika
    from .store import Store

    params = pika.URLParameters(url)
    params.heartbeat = 15
    params.socket_timeout = 5
    params.stack_timeout = 10
    params.blocked_connection_timeout = 10
    params.connection_attempts = 1
    if recovery:
        with pika.BlockingConnection(params) as connection:
            channel = connection.channel()
            declare(channel)
            redrive(channel, *recovery, pika.BasicProperties)
        return
    store = Store(path)
    backoff = 1
    while True:
        try:
            with pika.BlockingConnection(params) as connection:
                channel = connection.channel()
                declare(channel)
                heartbeat(path)
                last_heartbeat = time.monotonic()
                while True:
                    publish_pending(channel, store, path, pika.BasicProperties)
                    for _ in range(32):
                        method, props, body = channel.basic_get(queue=QUEUE, auto_ack=False)
                        if method is None:
                            break
                        process_delivery(channel, method, props, body, path, pika.BasicProperties)
                    if time.monotonic() - last_heartbeat >= 4:
                        heartbeat(path)
                        last_heartbeat = time.monotonic()
                    backoff = 1
                    connection.process_data_events(time_limit=0.3)
        except (pika.exceptions.AMQPError, OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
            # No payload, URL, credentials or exception details in logs.
            LOG.warning("broker worker will reconnect (%s)", type(exc).__name__)
            try:
                heartbeat(path, False)
                error(path, type(exc).__name__, "worker_errors")
            except sqlite3.Error:
                LOG.warning("cannot persist worker status")
            time.sleep(backoff)
            backoff = min(backoff * 2, 10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "redrive"), default="run", nargs="?")
    parser.add_argument("--event-id")
    parser.add_argument("--confirm-cause-fixed", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    db = os.getenv("INVENTORY_DB", "/data/inventory.sqlite3")
    rabbit = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    if args.action == "redrive" and (not args.event_id or not args.confirm_cause_fixed):
        parser.error("redrive requires --event-id and --confirm-cause-fixed")
    try:
        run(db, rabbit, (args.event_id, args.confirm_cause_fixed) if args.action == "redrive" else None)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
