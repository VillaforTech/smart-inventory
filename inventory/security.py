"""Demo bearer principal configuration and per-subject/per-operation fixed windows."""
from __future__ import annotations

import hashlib
import json
import math
import re
import threading
import time
from dataclasses import dataclass

from fastapi import HTTPException

SCOPES = frozenset({
    "inventory:read", "inventory:reserve", "replenishment:read",
    "replenishment:approve", "dashboard:read",
})
TOKEN_FORMAT = re.compile(r"[A-Za-z0-9._~+/=-]{1,512}\Z")


@dataclass(frozen=True)
class Principal:
    subject: str
    scopes: frozenset[str]
    locations: frozenset[str]

    @property
    def permitted_locations(self):
        return None if "*" in self.locations else self.locations

    def check_location(self, location):
        if location is not None and self.permitted_locations is not None and location not in self.locations:
            raise HTTPException(403, {"code": "location_forbidden", "message": "Location is outside the assigned scope"})

    def permits(self, location):
        return self.permitted_locations is None or location in self.locations


class Authenticator:
    def __init__(self, raw_config):
        records = json.loads(raw_config or "{}")
        if not isinstance(records, dict):
            raise ValueError("INVENTORY_TOKENS_JSON must be an object")
        self._principals = {}
        for token, raw in records.items():
            if not TOKEN_FORMAT.fullmatch(token) or not isinstance(raw, dict):
                raise ValueError("Invalid bearer principal configuration")
            if set(raw) != {"subject", "scopes", "locations"}:
                raise ValueError("Bearer principal needs subject, scopes and locations")
            subject, scopes, locations = raw["subject"], raw["scopes"], raw["locations"]
            if (not isinstance(subject, str) or not subject.strip()
                    or len(subject) > 100 or not isinstance(scopes, list)
                    or any(not isinstance(scope, str) or scope not in SCOPES for scope in scopes)
                    or not isinstance(locations, list) or not locations
                    or any(not isinstance(loc, str) or not loc.strip() or len(loc) > 128 for loc in locations)
                    or ("*" in locations and locations != ["*"])):
                raise ValueError("Invalid bearer principal configuration")
            self._principals[self._digest(token)] = Principal(subject, frozenset(scopes), frozenset(locations))

    @staticmethod
    def _digest(token):
        return hashlib.sha256(token.encode()).digest()

    def authenticate(self, supplied_token, scope):
        if not supplied_token or not TOKEN_FORMAT.fullmatch(supplied_token):
            principal = None
        else:
            principal = self._principals.get(self._digest(supplied_token))
        if principal is None:
            raise HTTPException(401, {"code": "unauthenticated", "message": "A valid bearer token is required"},
                                headers={"WWW-Authenticate": "Bearer"})
        if scope not in principal.scopes:
            raise HTTPException(403, {"code": "scope_forbidden", "message": "Required permission is missing"})
        return principal


class FixedWindowLimiter:
    """One process, one counter per authenticated subject and operation."""

    def __init__(self, limit=60, window_seconds=60, clock=time.monotonic):
        if not 1 <= limit <= 10_000 or not 1 <= window_seconds <= 3_600:
            raise ValueError("Invalid rate limit settings")
        self.limit, self.window_seconds, self.clock = limit, window_seconds, clock
        self._period, self._counts = None, {}
        self._lock = threading.Lock()

    def consume(self, subject, operation):
        with self._lock:
            now = self.clock()
            period = int(now // self.window_seconds)
            if self._period != period:
                self._period, self._counts = period, {}
            key = (subject, operation)
            count = self._counts.get(key, 0)
            allowed = count < self.limit
            if allowed:
                count += 1
                self._counts[key] = count
            seconds = max(1, math.ceil((period + 1) * self.window_seconds - now))
            headers = {
                "RateLimit-Limit": str(self.limit),
                "RateLimit-Remaining": str(max(self.limit - count, 0)),
                "RateLimit-Reset": str(seconds),
            }
            if not allowed:
                headers["Retry-After"] = str(seconds)
            return allowed, headers
