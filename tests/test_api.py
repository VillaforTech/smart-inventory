import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from inventory.api import create_app
from inventory.security import SCOPES
from inventory.worker import heartbeat, project_event


class ApiTests(unittest.TestCase):
    tokens = {
        "synthetic-global": {"subject": "global", "scopes": sorted(SCOPES), "locations": ["*"]},
        "synthetic-one": {"subject": "one", "scopes": sorted(SCOPES), "locations": ["UIO-01"]},
        "synthetic-two": {"subject": "two", "scopes": sorted(SCOPES), "locations": ["UIO-02"]},
        "synthetic-view": {"subject": "observer", "scopes": ["inventory:read"], "locations": ["UIO-01"]},
    }

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.db = Path(folder.name) / "test.db"
        self.api = create_app(self.db, tokens_json=json.dumps(self.tokens), seed_demo=True, rate_limit=60)
        self.client = self.enterContext(TestClient(self.api))

    def headers(self, who="global", **kwargs):
        return {"Authorization": "Bearer synthetic-" + who, **kwargs}

    def request_reservation(self, quantity=6, location="UIO-01", who="global", key="first", **kwargs):
        return self.client.post("/api/v1/reservations", headers=self.headers(who, **{"Idempotency-Key": key, **kwargs}),
                                json={"sku": "MILK-1L", "location": location, "quantity": quantity})

    def test_list_pagination_filters_and_horizontal_scope(self):
        path = "/api/v1/inventory"
        response = self.client.get(path, headers=self.headers("one"), params={"sku": "MILK-1L", "page_size": 2})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total"], 3)
        self.assertEqual([item["location"] for item in response.json()["items"]], ["UIO-01", "UIO-01"])
        self.assertEqual(self.client.get(path, headers=self.headers("one"), params={"location": "UIO-02"}).status_code, 403)
        self.assertEqual(self.client.get(path, headers=self.headers("one"), params={"lot": "LOT-C"}).json()["total"], 0)
        self.assertEqual(self.client.get(path, headers=self.headers(), params={"location": "UIO-02"}).json()["total"], 1)
        self.assertEqual(self.client.get(path, headers=self.headers(), params={"page_size": 101}).status_code, 422)

    def test_authentication_scope_and_input_validation(self):
        path = "/api/v1/inventory"
        response = self.client.get(path)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["WWW-Authenticate"], "Bearer")
        self.assertEqual(self.request_reservation(who="view").status_code, 403)
        self.assertEqual(self.request_reservation(who="two", location="UIO-01").status_code, 403)
        self.assertEqual(self.client.post("/api/v1/reservations", headers=self.headers()).status_code, 422)
        for quantity in [True, 0, "5", 1.5]:
            with self.subTest(quantity=quantity):
                self.assertEqual(self.request_reservation(quantity=quantity).status_code, 422)
        body = self.client.post("/api/v1/reservations", headers=self.headers(**{"Idempotency-Key": "new"}),
                                json={"sku": "MILK-1L", "location": "UIO-01", "quantity": 1,
                                      "unrecognized_argument": "not-reflected"})
        self.assertEqual(body.status_code, 422)
        self.assertNotIn("not-reflected", body.text)

    def test_reservation_replay_conflict_and_correlation(self):
        response = self.request_reservation(**{"X-Correlation-Id": "demo-transaction"})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.headers["X-Correlation-Id"], "demo-transaction")
        self.assertEqual(response.json()["allocations"], [{"lot": "LOT-A", "quantity": 4}, {"lot": "LOT-B", "quantity": 2}])
        self.assertEqual(self.request_reservation().json(), {**response.json(), "replayed": True})
        conflict = self.request_reservation(quantity=5)
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["detail"]["code"], "idempotency_conflict")
        self.assertEqual(self.api.state.store.pending_events()[0]["correlationId"], "demo-transaction")
        another = self.request_reservation(quantity=1, who="one")
        self.assertEqual(another.status_code, 201)
        self.assertFalse(another.json()["replayed"])
        self.assertEqual(self.request_reservation(quantity=100, key="lost").json()["detail"]["code"], "insufficient_stock")

    def test_replenishment_permissions_filters_and_state_transition(self):
        self.assertEqual(self.request_reservation().status_code, 201)
        path = "/api/v1/replenishment/suggestions"
        params = {"state": "PENDING", "sku": "MILK-1L", "supplier": "DEMO-DAIRY"}
        suggestion, = self.client.get(path, headers=self.headers("one"), params=params).json()["items"]
        self.assertEqual(self.client.get(path, headers=self.headers("two")).json()["total"], 0)
        approve = f"{path}/{suggestion['id']}/approve"
        self.assertEqual(self.client.post(approve, headers=self.headers("view")).status_code, 403)
        self.assertEqual(self.client.post(approve, headers=self.headers("two")).status_code, 404)
        self.assertEqual(self.client.post(approve, headers=self.headers("one")).json()["state"], "APPROVED")
        self.assertEqual(self.client.post(approve, headers=self.headers("one")).status_code, 409)
        self.assertEqual(self.client.get(path, headers=self.headers("one"), params={"state": "APPROVED"}).json()["total"], 1)

    def test_rate_limits_application_and_reset(self):
        db = self.db.parent / "rate.db"
        clock = [100.25]
        api = create_app(db, tokens_json=json.dumps(self.tokens), seed_demo=True, rate_limit=2,
                         rate_window_seconds=10, clock=lambda: clock[0])
        path = "/api/v1/inventory"
        with TestClient(api) as client:
            first = client.get(path, headers=self.headers("one"))
            self.assertEqual(first.headers["RateLimit-Limit"], "2")
            self.assertEqual(first.headers["RateLimit-Remaining"], "1")
            second = client.get(path, headers=self.headers("one"))
            self.assertEqual(second.headers["RateLimit-Remaining"], "0")
            third = client.get(path, headers=self.headers("one"))
            self.assertEqual(third.status_code, 429)
            self.assertEqual(third.headers["Retry-After"], "10")
            self.assertEqual(third.headers["RateLimit-Reset"], "10")
            self.assertEqual(client.get(path, headers=self.headers("two")).status_code, 200)
            clock[0] = 110.0
            self.assertEqual(client.get(path, headers=self.headers("one")).status_code, 200)

    def test_dashboard_health_and_metrics_show_observed_worker_state(self):
        response = self.client.get("/health")
        self.assertEqual(response.json()["status"], "degraded")
        self.assertEqual(self.client.get("/live").status_code, 200)
        self.assertEqual(self.client.get("/ready").status_code, 200)
        self.request_reservation()
        path = "/api/v1/dashboard"
        late = self.client.get(path, headers=self.headers())
        self.assertTrue(late.json()["stale"])
        self.assertEqual(late.json()["pending_outbox"], 2)
        self.assertIsNone(late.json()["last_sync"])
        for event in self.api.state.store.pending_events():
            project_event(self.db, event)
            self.api.state.store.mark_event_published(event["eventId"])
        heartbeat(self.db)
        self.assertEqual(self.client.get("/health").json()["status"], "ok")
        done = self.client.get(path, headers=self.headers("one")).json()
        self.assertFalse(done["stale"])
        self.assertIsNone(done["pending_outbox"])
        self.assertIsNone(done["unprojected_outbox"])
        self.assertIsNotNone(done["last_sync"])
        self.assertEqual(len(done["reservations"]), 1)
        self.assertEqual(self.client.get(path, headers=self.headers("two")).json()["reservations"], [])
        self.assertIsNone(done["events_processed"])
        self.assertEqual(self.client.get("/metrics").status_code, 401)
        self.assertEqual(self.client.get("/metrics", headers=self.headers("one")).status_code, 403)
        self.assertIn("inventory_http_requests_total", self.client.get("/metrics", headers=self.headers()).text)

    def test_dashboard_orders_are_location_scoped(self):
        def projection(_path):
            return {"recent_events": [], "events_processed": 2, "events_by_type": {"OrderStatusChanged": 2},
                    "last_sync": None, "last_error": None, "last_error_at": None, "lag_seconds": None,
                    "pending_outbox": 0, "orders": [
                        {"order_id": "a", "location": "UIO-01", "status": "CREATED", "occurred_at": "2026-01-01"},
                        {"order_id": "b", "location": "UIO-02", "status": "CREATED", "occurred_at": "2026-01-01"}]}
        with patch("inventory.api.worker.projection", side_effect=projection):
            path = "/api/v1/dashboard"
            self.assertEqual([r["order_id"] for r in self.client.get(path, headers=self.headers("one")).json()["orders"]], ["a"])
            response = self.client.get(path, params={"location": "UIO-02"}, headers=self.headers())
            self.assertEqual([r["order_id"] for r in response.json()["orders"]], ["b"])
            self.assertEqual(self.client.get(path, params={"location": "UIO-02"}, headers=self.headers("one")).status_code, 403)

    def test_dashboard_html_is_local_and_token_lives_in_memory(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("script-src 'sha256-", response.headers["Content-Security-Policy"])
        self.assertIn("connect-src 'self'", response.headers["Content-Security-Policy"])
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertNotIn("localStorage", response.text)
        self.assertNotIn("synthetic-global", response.text)

    def test_access_log_uses_route_template_and_correlation_not_secret(self):
        with self.assertLogs("uvicorn.error", level="INFO") as logs:
            self.client.get("/api/v1/inventory", headers=self.headers(**{"X-Correlation-Id": "log-demo"}),
                            params={"sku": "do-not-log-query"})
        payload = json.loads(logs.records[0].message)
        self.assertEqual(payload["route"], "/api/v1/inventory")
        self.assertEqual(payload["correlationId"], "log-demo")
        self.assertNotIn("synthetic-global", logs.records[0].message)
        self.assertNotIn("do-not-log-query", logs.records[0].message)


if __name__ == "__main__":
    unittest.main()
