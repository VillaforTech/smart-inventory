import os
import sqlite3
import time
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from inventory import worker


def event(n="e-01"):
    return {
        "eventId": n,
        "eventType": "InventoryReserved",
        "schemaVersion": 1,
        "occurredAt": worker.now(),
        "correlationId": "c-01",
        "producer": "smart-inventory",
        "payload": {"sku": "SKU-DEMO", "quantity": 2},
    }


class Channel:
    def __init__(self, head=None, fail=False, ack_fail=False, calls=None):
        self.calls = calls if calls is not None else []
        self.head = head
        self.fail = fail
        self.ack_fail = ack_fail

    def basic_publish(self, **args):
        self.calls.append(("publish", args))
        if self.fail:
            raise OSError("broker offline")

    def basic_ack(self, **args):
        self.calls.append(("ack", args))
        if self.ack_fail:
            raise OSError("connection lost before ACK")

    def basic_nack(self, **args):
        self.calls.append(("nack", args))

    def basic_get(self, **args):
        self.calls.append(("get", args))
        return self.head or (None, None, None)


def props(**fields):
    return SimpleNamespace(**fields)


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "inventory.sqlite3")

    def deliver(self, channel, item, retry=0, projector=worker.project_event):
        worker.process_delivery(
            channel, SimpleNamespace(delivery_tag=7),
            worker.properties_for(item, props, retry), worker.encode(item), self.path, props, projector,
        )

    def test_ack_loss_redelivery_has_one_durable_effect(self):
        item = event()
        failed = Channel(ack_fail=True)
        with self.assertRaises(OSError):
            self.deliver(failed, item)
        recovered = Channel()
        self.deliver(recovered, item)
        self.assertEqual([x[0] for x in recovered.calls], ["ack"])
        view = worker.projection(self.path)
        self.assertEqual(view["events_processed"], 1)
        self.assertEqual(view["recent_events"], [item])
        self.assertEqual(view["metrics"]["duplicates"], 1)
        self.assertIsNotNone(view["last_sync"])

    def test_conflicting_body_same_id_is_not_an_event(self):
        item = event()
        c = Channel()
        self.deliver(c, item)
        self.deliver(c, item | {"payload": {"quantity": 999}})
        self.assertEqual(c.calls[-1], ("nack", {"delivery_tag": 7, "requeue": False}))
        self.assertEqual(worker.projection(self.path)["recent_events"], [item])

    def test_retry_published_before_ack_and_exhausts_to_dlx(self):
        item = event()
        c = Channel()

        def db_error(*args):
            raise sqlite3.OperationalError("busy")

        for retry in (0, 1, 2):
            self.deliver(c, item, retry, db_error)
        self.assertEqual([x[0] for x in c.calls], ["publish", "ack", "publish", "ack", "nack"])
        self.assertEqual(c.calls[-1], ("nack", {"delivery_tag": 7, "requeue": False}))
        for i, idx in enumerate((0, 2)):
            args = c.calls[idx][1]
            self.assertTrue(args["mandatory"])
            self.assertEqual(args["routing_key"], worker.RETRY_QUEUES[i][0])
            self.assertEqual(args["properties"].message_id, item["eventId"])
            self.assertEqual(args["properties"].headers["dashboard-retry"], i + 1)
        view = worker.projection(self.path)
        self.assertEqual(view["metrics"]["retried"], 2)
        self.assertEqual(view["metrics"]["dead_lettered"], 1)
        self.assertEqual(view["events_processed"], 0)

    def test_retry_publish_failure_preserves_original(self):
        def db_error(*args):
            raise sqlite3.OperationalError("busy")

        c = Channel(fail=True)
        with self.assertRaises(OSError):
            self.deliver(c, event(), projector=db_error)
        self.assertEqual([x[0] for x in c.calls], ["publish", "nack"])
        self.assertTrue(c.calls[-1][1]["requeue"])

    def test_publish_failure_does_not_mark_outbox(self):
        item, calls = event(), []

        class Store:
            def pending_events(self, limit=100):
                return [item]

            def mark_event_published(self, event_id):
                calls.append(("mark", event_id))
                return True

        with self.assertRaises(OSError):
            worker.publish_pending(Channel(fail=True, calls=calls), Store(), self.path, props)
        self.assertEqual([x[0] for x in calls], ["publish"])
        worker.publish_pending(Channel(calls=calls), Store(), self.path, props)
        self.assertEqual([x[0] for x in calls], ["publish", "publish", "mark"])
        self.assertEqual(calls[-1][1], item["eventId"])
        self.assertEqual(calls[-2][1]["routing_key"], "inventory.reserved")
        self.assertEqual(calls[-2][1]["properties"].delivery_mode, 2)

    def test_real_store_outbox_drains_into_dashboard_without_double_effect(self):
        from inventory.store import Store

        store = Store(self.path)
        store.seed_demo()
        store.reserve("MILK-1L", "UIO-01", 6, "demo-reserve-1", "reserve-01")
        pending = store.pending_events()
        self.assertEqual([x["eventType"] for x in pending], ["InventoryReserved", "LowStockDetected"])
        self.assertEqual(worker.projection(self.path)["pending_outbox"], 2)
        self.assertIsNotNone(worker.projection(self.path)["oldest_pending_at"])

        publisher = Channel()
        worker.publish_pending(publisher, store, self.path, props)
        self.assertEqual(worker.projection(self.path)["pending_outbox"], 0)
        self.assertEqual(worker.projection(self.path)["unprojected_outbox"], 2)
        consumer = Channel()
        for message in [*publisher.calls, publisher.calls[0]]:
            fields = message[1]
            worker.process_delivery(
                consumer, SimpleNamespace(delivery_tag=7), fields["properties"], fields["body"],
                self.path, props,
            )
        dashboard = worker.projection(self.path)
        self.assertEqual(dashboard["events_by_type"], {"InventoryReserved": 1, "LowStockDetected": 1})
        self.assertEqual(dashboard["metrics"]["published"], 2)
        self.assertEqual(dashboard["unprojected_outbox"], 0)
        self.assertEqual(dashboard["metrics"]["duplicates"], 1)
        self.assertTrue(all(it["correlationId"] == "reserve-01" for it in dashboard["recent_events"]))

    def test_order_projection_keeps_latest_instant_and_stable_tie(self):
        changes = [
            ("z-winner", "2026-11-20T10:00:00.123456-05:00", "READY"),
            ("zz-older", "2026-11-20T14:59:59.999999Z", "CREATED"),
            ("a-same", "2026-11-20T15:00:00.123456Z", "PREPARING"),
        ]
        channel = Channel()
        for event_id, stamp, status in changes:
            self.deliver(channel, event(event_id) | {
                "eventType": "OrderStatusChanged", "occurredAt": stamp,
                "producer": "order-demo-stub",
                "payload": {"order_id": "DEMO-01", "location": "UIO-01", "status": status},
            })
        result = worker.projection(self.path)
        self.assertEqual(result["events_by_type"]["OrderStatusChanged"], 3)
        self.assertEqual(result["orders"], [{
            "order_id": "DEMO-01", "location": "UIO-01", "status": "READY",
            "occurred_at": "2026-11-20T15:00:00.123456+00:00", "event_id": "z-winner",
        }])

    def test_guarded_redrive_same_id_and_nack_on_mismatch(self):
        item = event()
        c = Channel((SimpleNamespace(delivery_tag=9), worker.properties_for(item, props, 2), worker.encode(item)))
        with self.assertRaises(ValueError):
            worker.redrive(c, item["eventId"], False, props)
        self.assertEqual(c.calls, [])
        with self.assertRaises(ValueError):
            worker.redrive(c, "other-id", True, props)
        self.assertEqual([x[0] for x in c.calls], ["get", "nack"])
        self.assertTrue(c.calls[1][1]["requeue"])
        worker.redrive(c, item["eventId"], True, props)
        self.assertEqual([x[0] for x in c.calls[-3:]], ["get", "publish", "ack"])
        self.assertEqual(c.calls[-2][1]["properties"].headers["dashboard-retry"], 0)
        self.assertEqual(c.calls[-2][1]["properties"].message_id, item["eventId"])

    def test_bad_envelope_is_terminal(self):
        c = Channel()
        worker.process_delivery(c, SimpleNamespace(delivery_tag=7), props(message_id=None, headers={}), b"{}", self.path, props)
        self.assertEqual(c.calls, [("nack", {"delivery_tag": 7, "requeue": False})])
        self.assertEqual(worker.projection(self.path)["last_error"], "invalid_envelope")

    def test_broker_readiness_requires_fresh_heartbeat(self):
        self.assertFalse(worker.broker_health(self.path)["healthy"])
        worker.heartbeat(self.path)
        self.assertTrue(worker.broker_health(self.path)["healthy"])
        with worker.database(self.path) as con:
            worker.put(con, "last_seen", "2020-01-01T00:00:00+00:00")
        self.assertFalse(worker.broker_health(self.path)["healthy"])
        worker.heartbeat(self.path, False)
        self.assertFalse(worker.broker_health(self.path)["healthy"])


@unittest.skipUnless(os.getenv("INVENTORY_BROKER_TEST") == "1", "requires disposable Compose broker")
class RealBrokerTests(unittest.TestCase):
    def test_confirm_retry_dlx_replay_and_idempotence(self):
        import pika

        # This test runs only on the disposable CI stack, with its worker stopped.
        with tempfile.TemporaryDirectory() as tmp, pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_URL"])
        ) as conn:
            path = Path(tmp) / "projection.sqlite3"
            channel = conn.channel()
            worker.declare(channel)
            item = event("broker-ci-e-01")
            body = worker.encode(item)

            def take(queue):
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    delivery = channel.basic_get(queue=queue, auto_ack=False)
                    if delivery[0] is not None:
                        return delivery
                    conn.process_data_events(time_limit=0.1)
                self.fail(f"timeout waiting for {queue}")

            def transient(*args):
                raise sqlite3.OperationalError("controlled test failure")

            worker.confirmed(channel, worker.EXCHANGE, worker.ROUTES[item["eventType"]], body,
                             worker.properties_for(item, pika.BasicProperties))
            for retry in (0, 1, 2):
                method, props_, packet = take(worker.QUEUE)
                self.assertEqual(props_.message_id, item["eventId"])
                self.assertEqual(props_.headers["dashboard-retry"], retry)
                worker.process_delivery(channel, method, props_, packet, path, pika.BasicProperties, transient)
            dead, dead_props, dead_packet = take(worker.DEAD_QUEUE)
            self.assertEqual(worker.decode(dead_packet), item)
            self.assertEqual(dead_props.message_id, item["eventId"])
            channel.basic_nack(delivery_tag=dead.delivery_tag, requeue=True)
            worker.redrive(channel, item["eventId"], True, pika.BasicProperties)
            worker.process_delivery(channel, *take(worker.QUEUE), path, pika.BasicProperties)
            worker.confirmed(channel, worker.EXCHANGE, worker.ROUTES[item["eventType"]], body,
                             worker.properties_for(item, pika.BasicProperties))
            worker.process_delivery(channel, *take(worker.QUEUE), path, pika.BasicProperties)
            result = worker.projection(path)
            self.assertEqual(result["events_processed"], 1)
            self.assertEqual(result["metrics"]["retried"], 2)
            self.assertEqual(result["metrics"]["dead_lettered"], 1)
            self.assertEqual(result["metrics"]["duplicates"], 1)


if __name__ == "__main__":
    unittest.main()
