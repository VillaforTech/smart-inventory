"""Reproducible localhost-only in-process ASGI baseline; no external requests."""
from __future__ import annotations

import json
import math
import os
import platform
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
from inventory.store import Store

COUNT = 240
TARGETS = {"query": 75, "command": 125}

def measure(name, invoke, expected_status):
    elapsed = []
    codes = Counter()
    start = time.perf_counter()
    for i in range(COUNT):
        before = time.perf_counter_ns()
        response = invoke(i)
        elapsed.append((time.perf_counter_ns() - before) / 1_000_000)
        codes[response.status_code] += 1
    wall = time.perf_counter() - start
    elapsed.sort()
    def pct(p):
        return round(elapsed[math.ceil(len(elapsed)*p)-1], 3)
    return {
        "requests": COUNT, "p50_ms": pct(.50), "p95_ms": pct(.95),
        "target_p95_ms": TARGETS[name], "elapsed_s": round(wall, 3),
        "requests_per_second": round(COUNT / wall, 2),
        "unexpected_statuses": sum(n for status,n in codes.items() if status not in expected_status),
        "statuses": dict(codes),
    }

def main():
    token = "synthetic-benchmark-token"
    with tempfile.TemporaryDirectory(prefix="inventory-perf-") as directory:
        db = Path(directory) / "bench.sqlite3"
        os.environ.update({
            "INVENTORY_DB": str(db),
            "INVENTORY_SEED_DEMO": "false",
            "INVENTORY_TOKENS_JSON": json.dumps({
                token: {"subject":"bench","scopes":["inventory:read","inventory:reserve"],"locations":["*"]},
            }),
            "INVENTORY_RATE_LIMIT": "10000",
        })
        Store(db)
        lots=[(f"SKU-{part:04d}", "TEST-BENCH",f"LOT-{j:02d}","2099-01-01",5000,0)
              for part in range(240) for j in range(10)]
        with sqlite3.connect(db) as con:
            con.executemany("INSERT INTO inventory_lots(sku,location,lot,expires_on,physical,reserved) VALUES(?,?,?,?,?,?)", lots)
        from inventory.api import app
        auth={"Authorization":f"Bearer {token}"}
        body={"sku":"SKU-0000","location":"TEST-BENCH","quantity":1}
        with TestClient(app) as test:
            query=lambda i:test.get("/api/v1/inventory",params={"location":"TEST-BENCH","page_size":20,"page":(i%10)+1},headers=auth)
            command=lambda i:test.post("/api/v1/reservations",json=body,headers={**auth,"Idempotency-Key":f"bench-{i}"})
            assert query(0).status_code == 200
            assert command(-1).status_code in (200,201)
            results={"query":measure("query",query,{200}),"command":measure("command",command,{200,201})}
        output={
            "mode":"local in-process ASGI, one request at a time; excludes network, real broker and staging",
            "python":platform.python_version(),"platform":platform.system(),"machine":platform.machine(),
            "sqlite":sqlite3.sqlite_version,"logical_cpus_visible":os.cpu_count(),
            "dataset":{"lots":len(lots),"distinct_skus":240,"locations":1},
            "authentication":"configured opaque local test token",
            "statement":"Commands create genuine reservations and outbox rows in a temporary database. RabbitMQ and log shipping are not measured.",
            "results":results,
        }
        print(json.dumps(output,ensure_ascii=False,indent=2))
        return int(any(r["unexpected_statuses"] for r in results.values()))

if __name__=="__main__":
    sys.exit(main())
