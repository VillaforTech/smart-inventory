import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from inventory.store import Store, StoreError


class StoreTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name) / "inventory.db"
        self.store = Store(self.path)
        self.store.seed_demo()

    def reserve(self, quantity, key="key", correlation="correlation"):
        return self.store.reserve("MILK-1L", "UIO-01", quantity, key, correlation)

    def lot_status(self):
        return {row["lot"]: row["reserved"] for row in
                self.store.list_inventory(sku="MILK-1L", location="UIO-01")["items"]}

    def event_types(self):
        return [event["eventType"] for event in self.store.pending_events()]

    def test_filtered_inventory_order_and_pagination(self):
        first = self.store.list_inventory("MILK-1L", "UIO-01", page=1, page_size=2)
        last = self.store.list_inventory("MILK-1L", "UIO-01", page=2, page_size=2)
        self.assertEqual(first["total"], 3)
        self.assertEqual([r["lot"] for r in first["items"]], ["EXPIRED", "LOT-A"])
        self.assertEqual([r["lot"] for r in last["items"]], ["LOT-B"])
        lot = self.store.list_inventory(lot="LOT-A")["items"][0]
        self.assertEqual((lot["physical"], lot["reserved"], lot["available"]), (4, 0, 4))
        self.assertEqual(self.store.list_inventory(sku="absent")["total"], 0)

    def test_fefo_splits_lots_and_excludes_expired(self):
        result = self.reserve(6)
        self.assertEqual(result["allocations"], [{"lot": "LOT-A", "quantity": 4},
                                                 {"lot": "LOT-B", "quantity": 2}])
        self.assertEqual(self.lot_status(), {"EXPIRED": 0, "LOT-A": 4, "LOT-B": 2})
        self.assertEqual(self.event_types(), ["InventoryReserved", "LowStockDetected"])

    def test_fefo_same_expiry_is_deterministic(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE inventory_lots SET expires_on=(SELECT expires_on FROM inventory_lots "
                       "WHERE lot='LOT-A') WHERE lot='LOT-B'")
        self.assertEqual(self.reserve(1)["allocations"], [{"lot": "LOT-A", "quantity": 1}])

    def test_no_stock_never_partially_writes_and_failed_key_can_retry(self):
        with self.assertRaises(StoreError) as err:
            self.reserve(13)  # EXPIRED has 30 but unexpired availability is 12.
        self.assertEqual((err.exception.status, err.exception.code), (409, "insufficient_stock"))
        self.assertEqual(self.lot_status(), {"EXPIRED": 0, "LOT-A": 0, "LOT-B": 0})
        self.assertEqual(self.event_types(), [])
        self.assertEqual(self.reserve(12)["quantity"], 12)
        self.assertEqual(sum(self.lot_status().values()), 12)

    def test_persisted_replay_no_duplicate_outbox_and_key_conflict(self):
        original = self.reserve(6)
        reopened = Store(self.path)
        replay = reopened.reserve("MILK-1L", "UIO-01", 6, "key", "new-correlation")
        self.assertEqual(replay, {**original, "replayed": True})
        self.assertEqual(len(reopened.pending_events()), 2)
        self.assertTrue(all(e["correlationId"] == "correlation" for e in reopened.pending_events()))
        with self.assertRaises(StoreError) as err:
            reopened.reserve("MILK-1L", "UIO-02", 6, "key", "new-correlation")
        self.assertEqual((err.exception.status, err.exception.code), (409, "idempotency_conflict"))

    def test_concurrent_distinct_keys_cannot_oversell(self):
        barrier = Barrier(2)

        def run(key):
            barrier.wait(timeout=5)
            try:
                return self.reserve(8, key=key)
            except StoreError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, ("a", "b")))
        self.assertEqual(sum(isinstance(result, dict) for result in results), 1)
        self.assertIn("insufficient_stock", results)
        self.assertEqual(sum(self.lot_status().values()), 8)
        self.assertEqual(self.event_types(), ["InventoryReserved", "LowStockDetected"])

    def test_concurrent_same_key_allows_only_one_mutation(self):
        barrier = Barrier(2)

        def run(correlation):
            barrier.wait(timeout=5)
            return self.reserve(8, correlation=correlation)

        with ThreadPoolExecutor(max_workers=2) as pool:
            a, b = pool.map(run, ("a", "b"))
        self.assertEqual(a["reservation_id"], b["reservation_id"])
        self.assertNotEqual(a["replayed"], b["replayed"])
        self.assertEqual(sum(self.lot_status().values()), 8)
        self.assertEqual(self.event_types(), ["InventoryReserved", "LowStockDetected"])

    def test_low_stock_dedup_filters_and_one_way_approval(self):
        self.reserve(6, key="a")
        self.reserve(1, key="b")
        demo = self.store.list_suggestions(state="pending", sku="MILK-1L", supplier="DEMO-DAIRY")
        self.assertEqual(demo["total"], 1)
        self.assertEqual(self.store.list_suggestions(state="PENDING", supplier="DEMO-GRAINS")["total"], 0)
        self.assertEqual(self.event_types().count("LowStockDetected"), 1)
        item = demo["items"][0]
        self.assertEqual(item["suggested_quantity"], 14)
        sid = item["id"]
        self.assertEqual(self.store.approve(sid, "approval"), {"id": sid, "state": "APPROVED"})
        self.assertEqual(self.store.list_suggestions(state="PENDING")["total"], 0)
        self.assertEqual(self.store.list_suggestions(state="APPROVED")["items"][0]["id"], sid)
        self.assertEqual(self.store.pending_events()[-1]["correlationId"], "approval")
        with self.assertRaises(StoreError) as err:
            self.store.approve(sid, "twice")
        self.assertEqual((err.exception.status, err.exception.code), (409, "invalid_suggestion_state"))
        self.assertEqual(self.event_types().count("ReplenishmentApproved"), 1)
        with self.assertRaises(StoreError) as err:
            self.store.approve("no-such-id", "missing")
        self.assertEqual(err.exception.status, 404)

    def test_outbox_commit_envelope_publish_and_pending_status(self):
        self.assertEqual(self.store.outbox_status(), {"pending_count": 0, "oldest_pending_at": None})
        result = self.reserve(3)
        first, = self.store.pending_events(limit=1)
        self.assertEqual(set(first), {"eventId", "eventType", "schemaVersion", "occurredAt",
                                      "correlationId", "producer", "payload"})
        self.assertEqual(first["schemaVersion"], 1)
        self.assertEqual(first["payload"]["reservation_id"], result["reservation_id"])
        self.assertEqual(self.store.outbox_status(),
                         {"pending_count": 1, "oldest_pending_at": first["occurredAt"]})
        self.assertTrue(self.store.mark_event_published(first["eventId"]))
        self.assertFalse(self.store.mark_event_published(first["eventId"]))
        self.assertEqual(self.store.pending_events(), [])

    def test_outbox_failure_rolls_back_reservation_and_alert_atomically(self):
        emit = self.store._emit

        def fail_low_stock(db, event_type, *args):
            if event_type == "LowStockDetected":
                raise RuntimeError("simulated event write failure")
            return emit(db, event_type, *args)

        with patch.object(self.store, "_emit", side_effect=fail_low_stock):
            with self.assertRaisesRegex(RuntimeError, "simulated event write failure"):
                self.reserve(6)
        self.assertEqual(sum(self.lot_status().values()), 0)
        self.assertEqual(self.store.list_suggestions()["total"], 0)
        self.assertEqual(self.store.pending_events(), [])
        self.assertFalse(self.reserve(6)["replayed"])

    def test_reseed_is_idempotent_and_does_not_restore_reserved_stock(self):
        self.reserve(2)
        self.store.seed_demo()
        self.assertEqual(self.store.list_inventory()["total"], 5)
        self.assertEqual(sum(self.lot_status().values()), 2)

    def test_invalid_inputs(self):
        for quantity in (0, -1, 1.5, True):
            with self.subTest(quantity=quantity), self.assertRaises(StoreError):
                self.reserve(quantity)
        with self.assertRaises(StoreError):
            self.reserve(1, key=" ")
        with self.assertRaises(StoreError):
            self.store.list_inventory(page=0)
        with self.assertRaises(StoreError):
            self.store.list_inventory(page_size=101)


if __name__ == "__main__":
    unittest.main()
