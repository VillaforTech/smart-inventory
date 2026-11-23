"""SQLite inventory domain. Every mutation uses one BEGIN IMMEDIATE transaction."""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


class StoreError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


_SCHEMA = """
CREATE TABLE IF NOT EXISTS inventory_lots (
    sku TEXT NOT NULL,
    location TEXT NOT NULL,
    lot TEXT NOT NULL,
    expires_on TEXT NOT NULL,
    physical INTEGER NOT NULL CHECK (physical >= 0),
    reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved BETWEEN 0 AND physical),
    PRIMARY KEY (sku, location, lot)
);
CREATE INDEX IF NOT EXISTS inventory_fefo ON inventory_lots (sku, location, expires_on, lot);
CREATE TABLE IF NOT EXISTS policies (
    sku TEXT NOT NULL,
    location TEXT NOT NULL,
    supplier TEXT NOT NULL,
    reorder_point INTEGER NOT NULL CHECK (reorder_point >= 0),
    target_stock INTEGER NOT NULL CHECK (target_stock > reorder_point),
    PRIMARY KEY (sku, location)
);
CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    location TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservation_allocations (
    reservation_id TEXT NOT NULL REFERENCES reservations (id),
    sku TEXT NOT NULL,
    location TEXT NOT NULL,
    lot TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (reservation_id, lot),
    FOREIGN KEY (sku, location, lot) REFERENCES inventory_lots (sku, location, lot)
);
CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    request TEXT NOT NULL,
    response TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS replenishment_suggestions (
    id TEXT PRIMARY KEY,
    sku TEXT NOT NULL,
    location TEXT NOT NULL,
    supplier TEXT NOT NULL,
    suggested_quantity INTEGER NOT NULL CHECK (suggested_quantity > 0),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'APPROVED')),
    created_at TEXT NOT NULL,
    approved_at TEXT
);
CREATE INDEX IF NOT EXISTS suggestions_filter ON replenishment_suggestions (state, sku, supplier);
CREATE TABLE IF NOT EXISTS low_stock_alerts (
    sku TEXT NOT NULL,
    location TEXT NOT NULL,
    suggestion_id TEXT NOT NULL REFERENCES replenishment_suggestions (id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (sku, location)
);
CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    envelope TEXT NOT NULL,
    published_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (seq) WHERE published_at IS NULL;
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _pagination(page: int, page_size: int) -> None:
    if (type(page) is not int or type(page_size) is not int
            or not 1 <= page <= 2_147_483_647 or not 1 <= page_size <= 100):
        raise StoreError(400, "invalid_pagination", "page must be positive and page_size must be 1–100")


def _filters(**values: str | None) -> tuple[str, list[str]]:
    clauses = [(column, value) for column, value in values.items() if value is not None]
    return (" WHERE " + " AND ".join(f"{c} = ?" for c, _ in clauses) if clauses else "",
            [v for _, v in clauses])


def _restrict_locations(where, args, permitted_locations):
    if permitted_locations is None:
        return where, args
    allowed = tuple(sorted(permitted_locations))
    restriction = "location IN (" + ",".join("?" for _ in allowed) + ")" if allowed else "0"
    return where + (" AND " if where else " WHERE ") + restriction, [*args, *allowed]


def _required(value: str, code: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise StoreError(400, code, f"{label} must be a non-empty string of at most 128 characters")
    return value.strip()


class Store:
    """File-backed store; independent connections allow real concurrent requests."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("Store requires a file-backed SQLite database")
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        return db

    @contextmanager
    def _write(self):
        # Acquire the write lock before reading inventory, checking idempotency,
        # or allocating lots. All effects and their outbox entries commit together.
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @contextmanager
    def _read(self):
        # Count and item queries use the same snapshot.
        with closing(self._connect()) as db:
            db.execute("BEGIN")
            yield db

    @staticmethod
    def _emit(db, event_type, correlation_id, payload, now):
        event = {
            "eventId": str(uuid4()),
            "eventType": event_type,
            "schemaVersion": 1,
            "occurredAt": now,
            "correlationId": correlation_id,
            "producer": "smart-inventory",
            "payload": payload,
        }
        db.execute(
            "INSERT INTO outbox (event_id,event_type,occurred_at,envelope) VALUES (?,?,?,?)",
            (event["eventId"], event_type, now, json.dumps(event, sort_keys=True)),
        )

    def list_inventory(self, sku=None, location=None, lot=None, page=1, page_size=20, *, permitted_locations=None):
        _pagination(page, page_size)
        where, args = _restrict_locations(*_filters(sku=sku, location=location, lot=lot), permitted_locations)
        with self._read() as db:
            total = db.execute("SELECT count(*) FROM inventory_lots" + where, args).fetchone()[0]
            rows = db.execute(
                "SELECT *, physical - reserved AS available FROM inventory_lots" + where
                + " ORDER BY sku, location, expires_on, lot LIMIT ? OFFSET ?",
                (*args, page_size, (page - 1) * page_size),
            ).fetchall()
        return {"items": [dict(row) for row in rows], "page": page, "page_size": page_size, "total": total}

    def reserve(self, sku, location, quantity, key, correlation_id):
        sku = _required(sku, "invalid_sku", "sku")
        location = _required(location, "invalid_location", "location")
        # Validate but preserve the exact key. Whitespace is not silently collapsed.
        _required(key, "invalid_idempotency_key", "Idempotency-Key")
        if type(quantity) is not int or not 0 < quantity <= 9_223_372_036_854_775_807:
            raise StoreError(400, "invalid_quantity", "quantity must be a positive integer")
        request = json.dumps([sku, location, quantity], separators=(",", ":"))
        with self._write() as db:
            previous = db.execute("SELECT request,response FROM idempotency WHERE key=?", (key,)).fetchone()
            if previous:
                if previous["request"] != request:
                    raise StoreError(409, "idempotency_conflict", "Idempotency-Key was already used for a different request")
                return {**json.loads(previous["response"]), "replayed": True}

            # A lot with a date strictly before the current UTC date has expired;
            # a lot is valid through its stated expiry date.
            now = _now()
            timestamp, today = now.isoformat(), now.date().isoformat()
            rows = db.execute(
                "SELECT lot, physical - reserved AS available FROM inventory_lots "
                "WHERE sku=? AND location=? AND expires_on>=? AND physical>reserved "
                "ORDER BY expires_on, lot", (sku, location, today)
            ).fetchall()
            remaining_available = sum(row["available"] for row in rows)
            if remaining_available < quantity:
                raise StoreError(409, "insufficient_stock", "Insufficient unexpired stock")

            reservation_id = str(uuid4())
            db.execute("INSERT INTO reservations VALUES (?,?,?,?,?)",
                       (reservation_id, sku, location, quantity, timestamp))
            allocations, remaining = [], quantity
            for row in rows:
                amount = min(row["available"], remaining)
                changed = db.execute(
                    "UPDATE inventory_lots SET reserved=reserved+? WHERE sku=? AND location=? "
                    "AND lot=? AND physical-reserved>=?",
                    (amount, sku, location, row["lot"], amount),
                ).rowcount
                if changed != 1:
                    raise StoreError(409, "insufficient_stock", "Insufficient unexpired stock")
                db.execute("INSERT INTO reservation_allocations VALUES (?,?,?,?,?)",
                           (reservation_id, sku, location, row["lot"], amount))
                allocations.append({"lot": row["lot"], "quantity": amount})
                remaining -= amount
                if not remaining:
                    break

            response = {"reservation_id": reservation_id, "sku": sku, "location": location,
                        "quantity": quantity, "allocations": allocations, "replayed": False}
            db.execute("INSERT INTO idempotency VALUES (?,?,?)", (key, request, json.dumps(response)))
            self._emit(db, "InventoryReserved", correlation_id,
                       {k: v for k, v in response.items() if k != "replayed"}, timestamp)
            self._check_low_stock(db, sku, location, remaining_available - quantity, correlation_id, timestamp)
            return response

    def _check_low_stock(self, db, sku, location, available, correlation_id, now):
        policy = db.execute("SELECT * FROM policies WHERE sku=? AND location=?", (sku, location)).fetchone()
        if not policy:
            return
        if available > policy["reorder_point"]:
            db.execute("DELETE FROM low_stock_alerts WHERE sku=? AND location=?", (sku, location))
            return
        if db.execute("SELECT 1 FROM low_stock_alerts WHERE sku=? AND location=?", (sku, location)).fetchone():
            return
        suggestion_id = str(uuid4())
        suggested = policy["target_stock"] - available
        db.execute("INSERT INTO replenishment_suggestions VALUES (?,?,?,?,?,'PENDING',?,NULL)",
                   (suggestion_id, sku, location, policy["supplier"], suggested, now))
        db.execute("INSERT INTO low_stock_alerts VALUES (?,?,?,?)", (sku, location, suggestion_id, now))
        self._emit(db, "LowStockDetected", correlation_id,
                   {"sku": sku, "location": location, "available": available,
                    "reorder_point": policy["reorder_point"], "supplier": policy["supplier"],
                    "suggested_quantity": suggested, "suggestion_id": suggestion_id}, now)

    def list_suggestions(self, state=None, sku=None, supplier=None, page=1, page_size=20, *, permitted_locations=None):
        _pagination(page, page_size)
        if state is not None:
            state = _required(state, "invalid_suggestion_state", "state").upper()
            if state not in {"PENDING", "APPROVED"}:
                raise StoreError(400, "invalid_suggestion_state", "state must be PENDING or APPROVED")
        where, args = _restrict_locations(*_filters(state=state, sku=sku, supplier=supplier), permitted_locations)
        with self._read() as db:
            total = db.execute("SELECT count(*) FROM replenishment_suggestions" + where, args).fetchone()[0]
            rows = db.execute(
                "SELECT * FROM replenishment_suggestions" + where
                + " ORDER BY created_at DESC, id LIMIT ? OFFSET ?",
                (*args, page_size, (page - 1) * page_size),
            ).fetchall()
        return {"items": [dict(row) for row in rows], "page": page, "page_size": page_size, "total": total}

    def approve(self, suggestion_id, correlation_id, *, permitted_locations=None):
        with self._write() as db:
            row = db.execute("SELECT * FROM replenishment_suggestions WHERE id=?", (suggestion_id,)).fetchone()
            if row is None or (permitted_locations is not None and row["location"] not in permitted_locations):
                raise StoreError(404, "suggestion_not_found", "Replenishment suggestion was not found")
            if row["state"] != "PENDING":
                raise StoreError(409, "invalid_suggestion_state", "Only PENDING suggestions can be approved")
            now = _now().isoformat()
            db.execute("UPDATE replenishment_suggestions SET state='APPROVED', approved_at=? WHERE id=?",
                       (now, suggestion_id))
            self._emit(db, "ReplenishmentApproved", correlation_id,
                       {**dict(row), "state": "APPROVED", "approved_at": now}, now)
            return {"id": suggestion_id, "state": "APPROVED"}

    def pending_events(self, limit=100):
        if type(limit) is not int or limit < 1:
            raise ValueError("outbox limit must be a positive integer")
        with closing(self._connect()) as db:
            rows = db.execute("SELECT envelope FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT ?",
                              (limit,)).fetchall()
        return [json.loads(row["envelope"]) for row in rows]

    def mark_event_published(self, event_id):
        with self._write() as db:
            return db.execute("UPDATE outbox SET published_at=? WHERE event_id=? AND published_at IS NULL",
                              (_now().isoformat(), event_id)).rowcount == 1

    def outbox_status(self):
        with closing(self._connect()) as db:
            row = db.execute("SELECT count(*) AS pending_count, MIN(occurred_at) AS oldest_pending_at "
                             "FROM outbox WHERE published_at IS NULL").fetchone()
        return dict(row)

    def seed_demo(self):
        today = _now().date()
        date_in = lambda days: (today + timedelta(days=days)).isoformat()
        with self._write() as db:
            db.executemany(
                "INSERT OR IGNORE INTO inventory_lots (sku,location,lot,expires_on,physical) VALUES (?,?,?,?,?)",
                [("MILK-1L", "UIO-01", "EXPIRED", date_in(-1), 30),
                 ("MILK-1L", "UIO-01", "LOT-A", date_in(1), 4),
                 ("MILK-1L", "UIO-01", "LOT-B", date_in(7), 8),
                 ("MILK-1L", "UIO-02", "LOT-C", date_in(10), 16),
                 ("RICE-1KG", "UIO-01", "LOT-R", date_in(90), 25)],
            )
            db.executemany(
                "INSERT OR IGNORE INTO policies VALUES (?,?,?,?,?)",
                [("MILK-1L", "UIO-01", "DEMO-DAIRY", 6, 20),
                 ("MILK-1L", "UIO-02", "DEMO-DAIRY", 4, 18),
                 ("RICE-1KG", "UIO-01", "DEMO-GRAINS", 5, 30)],
            )
