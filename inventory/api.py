"""HTTP API for the synthetic Smart Inventory demo."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict
from contextlib import asynccontextmanager, closing
from pathlib import Path
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from . import worker
from .security import Authenticator, FixedWindowLimiter
from .store import Store, StoreError

LOG = logging.getLogger("uvicorn.error")
CORRELATION = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
Key = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
Filter = Annotated[str | None, Query(min_length=1, max_length=128)]
Page = Annotated[int, Query(ge=1, le=2_147_483_647)]
PageSize = Annotated[int, Query(ge=1, le=100)]
AUTH = HTTPBearer(auto_error=False, scheme_name="BearerAuth")
STATUS_RESPONSES = {
    401: {"description": "Missing or invalid bearer token"},
    403: {"description": "Operation or location not allowed"},
    429: {"description": "Rate limit exceeded; retry after Retry-After seconds"},
}
HEALTH_METRICS = ("inventory_outbox_pending", "inventory_projection_processed",
                  "inventory_projection_lag_seconds", "inventory_worker_healthy")


class ReservationBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sku: Key
    location: Key
    quantity: Annotated[int, Field(strict=True, gt=0, le=9_223_372_036_854_775_807)]


class Allocation(BaseModel):
    lot: str
    quantity: int


class ReservationResponse(BaseModel):
    reservation_id: str
    sku: str
    location: str
    quantity: int
    allocations: list[Allocation]
    replayed: bool


class InventoryLot(BaseModel):
    sku: str
    location: str
    lot: str
    expires_on: str
    physical: int
    reserved: int
    available: int


class InventoryPage(BaseModel):
    items: list[InventoryLot]
    page: int
    page_size: int
    total: int


class Suggestion(BaseModel):
    id: str
    sku: str
    location: str
    supplier: str
    suggested_quantity: int
    state: Literal["PENDING", "APPROVED"]
    created_at: str
    approved_at: str | None


class SuggestionPage(BaseModel):
    items: list[Suggestion]
    page: int
    page_size: int
    total: int


class ApprovalResponse(BaseModel):
    id: str
    state: Literal["APPROVED"]


class ProjectedOrder(BaseModel):
    order_id: str
    location: str
    status: str
    occurred_at: str
    event_id: str | None = None


class ProjectedReservation(BaseModel):
    reservation_id: str
    sku: str
    location: str
    quantity: int
    allocations: list[Allocation]


class EventEnvelope(BaseModel):
    eventId: str
    eventType: str
    schemaVersion: int
    occurredAt: str
    correlationId: str
    producer: str
    payload: dict


class DashboardResponse(BaseModel):
    inventory: InventoryPage
    inventory_source: Literal["sqlite_operation"]
    activity_source: Literal["rabbitmq_projection"]
    broker_healthy: bool
    worker_last_seen: str | None
    last_sync: str | None
    last_error: str | None
    last_error_at: str | None
    lag_seconds: float | None
    lag_source: Literal["oldest_unprojected_outbox", "oldest_pending_outbox", "last_processed_event"] | None
    pending_outbox: int | None
    unprojected_outbox: int | None
    stale: bool
    recent_events: list[EventEnvelope]
    reservations: list[ProjectedReservation]
    orders: list[ProjectedOrder]
    events_processed: int | None
    events_by_type: dict[str, int]


def _redacted_projection(data, principal):
    reservation_events = [event for event in data["recent_events"]
                          if principal.permits(event["payload"].get("location"))]
    for_event_types = {"InventoryReserved", "LowStockDetected", "ReplenishmentApproved", "OrderStatusChanged"}
    return {
        "recent_events": reservation_events,
        "orders": [row for row in data.get("orders", []) if principal.permits(row.get("location"))],
        "reservations": [event["payload"] for event in reservation_events
                         if event["eventType"] == "InventoryReserved"],
        # For restricted users avoid cross-location activity totals. Pipeline-level health
        # is available via dashboard scope; counts split by event are only for global access.
        "events_processed": data["events_processed"] if principal.permitted_locations is None else None,
        "events_by_type": {
            k: v for k, v in data["events_by_type"].items() if k in for_event_types
        } if principal.permitted_locations is None else {},
    }


class HttpMetrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.totals = defaultdict(lambda: [0, 0.0])

    def record(self, method, route, status, duration):
        if method not in {"GET", "POST", "HEAD", "OPTIONS"}:
            method = "OTHER"
        with self.lock:
            aggregate = self.totals[(method, route, status)]
            aggregate[0] += 1
            aggregate[1] += duration

    def prometheus(self):
        with self.lock:
            totals = [(key, tuple(value)) for key, value in self.totals.items()]
        lines = ["# TYPE inventory_http_requests_total counter",
                 "# TYPE inventory_http_request_duration_seconds summary"]
        for (method, route, status), (count, seconds) in sorted(totals):
            labels = f'method="{method}",route="{route}",status="{status}"'
            lines.extend([f"inventory_http_requests_total{{{labels}}} {count}",
                          f"inventory_http_request_duration_seconds_count{{{labels}}} {count}",
                          f"inventory_http_request_duration_seconds_sum{{{labels}}} {seconds:.9f}"])
        return lines


def _csp(dashboard):
    groups = []
    for tag in ("script", "style"):
        blocks = re.findall(rf"<{tag}>(.*?)</{tag}>", dashboard, re.S)
        hashes = ["'sha256-" + base64.b64encode(hashlib.sha256(block.encode()).digest()).decode() + "'"
                  for block in blocks]
        groups.append(f"{tag}-src " + (" ".join(hashes) if hashes else "'none'"))
    return "; ".join(["default-src 'none'", *groups, "connect-src 'self'", "base-uri 'none'",
                      "form-action 'none'", "frame-ancestors 'none'"])


def create_app(db_path=None, *, tokens_json=None, seed_demo=None, rate_limit=None, rate_window_seconds=None, clock=None):
    db_path = db_path if db_path is not None else os.getenv("INVENTORY_DB", "./inventory.db")
    configured_tokens = tokens_json if tokens_json is not None else os.getenv("INVENTORY_TOKENS_JSON")
    seed_demo = seed_demo if seed_demo is not None else os.getenv("INVENTORY_SEED_DEMO", "0").lower() in {"1", "yes", "true"}
    rate_limit = rate_limit if rate_limit is not None else int(os.getenv("INVENTORY_RATE_LIMIT", "60"))
    rate_window_seconds = (rate_window_seconds if rate_window_seconds is not None
                           else int(os.getenv("INVENTORY_RATE_WINDOW_SECONDS", "60")))
    auth = Authenticator(configured_tokens)
    limiter = FixedWindowLimiter(rate_limit, rate_window_seconds, clock=clock or time.monotonic)
    metrics = HttpMetrics()

    @asynccontextmanager
    async def lifespan(app):
        app.state.store = Store(db_path)
        if seed_demo:
            app.state.store.seed_demo()
        yield

    api = FastAPI(title="Smart Inventory", version="1.0.0", lifespan=lifespan,
                  description="Inventory, FEFO reservations, replenishment and asynchronous dashboard.")

    @api.middleware("http")
    async def request_context(request, call_next):
        start = time.monotonic()
        inbound = request.headers.get("X-Correlation-Id", "")
        request.state.correlation_id = inbound if CORRELATION.fullmatch(inbound) else str(uuid4())
        response = await call_next(request)
        response.headers["X-Correlation-Id"] = request.state.correlation_id
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        for key, value in getattr(request.state, "rate_headers", {}).items():
            response.headers[key] = value
        template = getattr(request.scope.get("route"), "path", "unmatched")
        elapsed = time.monotonic() - start
        metrics.record(request.method, template, response.status_code, elapsed)
        LOG.info(json.dumps({"event": "http_request", "method": request.method, "route": template,
                             "status": response.status_code, "duration_ms": round(elapsed * 1000, 3),
                             "correlationId": request.state.correlation_id}, separators=(",", ":")))
        return response

    @api.exception_handler(StoreError)
    async def store_error(request, exc):
        return JSONResponse({"detail": {"code": exc.code, "message": exc.message}}, status_code=exc.status)

    @api.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Do not reflect raw request input (e.g. credentials accidentally supplied in JSON).
        fields = [".".join(str(piece) for piece in error["loc"]) for error in exc.errors()]
        return JSONResponse({"detail": {"code": "validation_error", "message": "Invalid request fields",
                                        "fields": fields}}, status_code=422)

    @api.exception_handler(sqlite3.Error)
    async def sqlite_failure(request, exc):
        LOG.warning(json.dumps({"event": "sqlite_unavailable", "correlationId": request.state.correlation_id,
                                "error_type": type(exc).__name__}))
        return JSONResponse({"detail": {"code": "storage_unavailable", "message": "Inventory storage unavailable"}},
                            status_code=503)

    def authorize(scope, operation=None):
        def check(request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(AUTH)]):
            principal = auth.authenticate(credentials.credentials if credentials else None, scope)
            allowed, headers = limiter.consume(principal.subject, operation or scope)
            request.state.rate_headers = headers
            if not allowed:
                raise HTTPException(429, {"code": "rate_limited", "message": "Too many requests"},
                                    headers=headers)
            return principal
        return check

    def store(request: Request):
        return request.app.state.store

    @api.get("/live", tags=["health"])
    def live():
        return {"status": "ok"}

    @api.get("/ready", tags=["health"])
    @api.get("/health", tags=["health"])
    def health():
        # RabbitMQ failure does not make the write API unavailable: outbox accepts durable work.
        try:
            with closing(sqlite3.connect(db_path, timeout=0.5)) as connection:
                connection.execute("SELECT 1 FROM inventory_lots LIMIT 1").fetchall()
        except sqlite3.Error:
            return JSONResponse({"status": "unavailable", "database": "down"}, status_code=503)
        try:
            connected = worker.broker_health(db_path)["healthy"]
        except sqlite3.Error:
            connected = False
        return {"status": "ok" if connected else "degraded", "database": "up",
                "worker": "connected" if connected else "stale_or_unavailable"}

    @api.get("/metrics", response_class=PlainTextResponse, tags=["health"], responses=STATUS_RESPONSES)
    def metric_values(principal=Depends(authorize("dashboard:read", "metrics"))):
        if principal.permitted_locations is not None:
            raise HTTPException(403, {"code": "scope_forbidden", "message": "Global metrics require all locations"})
        lines = metrics.prometheus()
        try:
            data = worker.projection(db_path)
            healthy = worker.broker_health(db_path)["healthy"]
            values = (data["pending_outbox"], data["events_processed"], data["lag_seconds"], int(healthy))
        except sqlite3.Error:
            values = (None, None, None, 0)
        for name, value in zip(HEALTH_METRICS, values):
            lines.append(f"# TYPE {name} gauge")
            if value is not None:
                lines.append(f"{name} {value}")
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    @api.get("/api/v1/inventory", response_model=InventoryPage, responses=STATUS_RESPONSES, tags=["inventory"])
    def list_inventory(principal=Depends(authorize("inventory:read")), db=Depends(store), sku: Filter = None,
                       location: Filter = None, lot: Filter = None, page: Page = 1, page_size: PageSize = 20):
        principal.check_location(location)
        return db.list_inventory(sku, location, lot, page, page_size,
                                 permitted_locations=principal.permitted_locations)

    @api.post("/api/v1/reservations", status_code=201, response_model=ReservationResponse, tags=["inventory"],
              responses={**STATUS_RESPONSES, 409: {"description": "Insufficient stock or idempotency conflict"}})
    def reserve(body: ReservationBody, request: Request,
                key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128)],
                principal=Depends(authorize("inventory:reserve")), db=Depends(store)):
        principal.check_location(body.location)
        # Scope retry keys by principal so one account cannot read another's reservation result.
        # The internal stable SHA-256 token fits the store's length bound even with long subjects.
        scoped_key = hashlib.sha256(json.dumps([principal.subject, key], separators=(",", ":")).encode()).hexdigest()
        if not key.strip():
            raise HTTPException(400, {"code": "invalid_idempotency_key", "message": "Idempotency-Key cannot be blank"})
        return db.reserve(body.sku, body.location, body.quantity, scoped_key, request.state.correlation_id)

    @api.get("/api/v1/replenishment/suggestions", response_model=SuggestionPage, tags=["replenishment"],
             responses=STATUS_RESPONSES)
    def suggestions(principal=Depends(authorize("replenishment:read")), db=Depends(store),
                    state: Filter = None, sku: Filter = None, supplier: Filter = None,
                    page: Page = 1, page_size: PageSize = 20):
        return db.list_suggestions(state, sku, supplier, page, page_size,
                                   permitted_locations=principal.permitted_locations)

    @api.post("/api/v1/replenishment/suggestions/{suggestion_id}/approve", response_model=ApprovalResponse,
              tags=["replenishment"], responses={**STATUS_RESPONSES,
                404: {"description": "Suggestion not found in allowed locations"},
                409: {"description": "Suggestion is not in pending state"}})
    def approve(suggestion_id: str, request: Request, principal=Depends(authorize("replenishment:approve")),
                db=Depends(store)):
        return db.approve(suggestion_id, request.state.correlation_id,
                          permitted_locations=principal.permitted_locations)

    @api.get("/api/v1/dashboard", response_model=DashboardResponse, tags=["dashboard"], responses=STATUS_RESPONSES)
    def dashboard_data(principal=Depends(authorize("dashboard:read")), db=Depends(store),
                       location: Filter = None, page: Page = 1, page_size: PageSize = 20):
        principal.check_location(location)
        data = worker.projection(db_path)
        status = worker.broker_health(db_path)
        outbox = data["pending_outbox"]
        unprojected = data.get("unprojected_outbox", 0)
        unprojected_age = worker.seconds_since(data.get("oldest_unprojected_at"))
        events = _redacted_projection(data, principal)
        if location is not None:
            events["recent_events"] = [e for e in events["recent_events"] if e["payload"].get("location") == location]
            events["reservations"] = [e for e in events["reservations"] if e.get("location") == location]
            events["orders"] = [order for order in events["orders"] if order.get("location") == location]
            events["events_processed"] = None
            events["events_by_type"] = {}
        return {
            "inventory": db.list_inventory(location=location, page=page, page_size=page_size,
                                            permitted_locations=principal.permitted_locations),
            "inventory_source": "sqlite_operation",
            "activity_source": "rabbitmq_projection",
            "broker_healthy": status["healthy"], "worker_last_seen": status["last_seen"],
            "last_sync": data["last_sync"], "last_error": data["last_error"], "last_error_at": data["last_error_at"],
            "lag_seconds": unprojected_age if unprojected_age is not None else data["lag_seconds"],
            "lag_source": ("oldest_unprojected_outbox" if unprojected_age is not None else
                "oldest_pending_outbox" if outbox else "last_processed_event" if data["last_sync"] else None),
            "pending_outbox": outbox if principal.permitted_locations is None else None,
            "unprojected_outbox": unprojected if principal.permitted_locations is None else None,
            "stale": not status["healthy"] or bool(outbox) or bool(unprojected),
            **events,
        }

    @api.get("/", response_class=HTMLResponse, include_in_schema=False)
    @api.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
    def dashboard_page():
        page = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")
        return HTMLResponse(page, headers={"Content-Security-Policy": _csp(page), "Referrer-Policy": "no-referrer"})

    return api


app = create_app()
