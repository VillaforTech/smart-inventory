# Smart Inventory

**Inventory reservations and replenishment for perishable stock.**

Smart Inventory allocates the earliest-expiring eligible lots (FEFO), prevents duplicate reservations on client retries, and suggests replenishment when available stock drops to its configured threshold. A separate worker sends durable events to RabbitMQ and builds an operations dashboard without holding up the request path.

**Release 0.1.0 · Release candidate.** The documented stack is configured for local evaluation with synthetic data. It is not validated for production traffic.

## Highlights

- Atomic allocation across lots, with expiry filtering, warehouse-scoped authorization and API idempotency.
- Replenishment suggestions with a separate approval permission and guarded state transition.
- Transactional outbox, publisher confirmations, duplicate-safe projection, bounded retries and dead-letter recovery.
- Browser dashboard, versioned OpenAPI contract and a repeatable verification pipeline.

## Project lineage

This component builds on the inventory domain explored in [LogistPulse](https://github.com/VillaforTech/logistpulse). The prior LogistPulse team is [@VillaforTech](https://github.com/VillaforTech), [@nikotov](https://github.com/nikotov), [@DanielSalazar0710](https://github.com/DanielSalazar0710), [@Dmt-155lbs](https://github.com/Dmt-155lbs), and [@oandretty010](https://github.com/oandretty010). This credit does not attribute individual changes or suggest approval of this release.

[@Ryan2040](https://github.com/Ryan2040) is also listed as a team member for the API catalog and contracts. This credit does not attribute code changes.

## Quick start

Requirements: Python 3.12 and Docker Compose v2.

```sh
python scripts/demo-env.py
docker compose config --quiet
docker compose up --build --wait
```

1. Open http://localhost:8000 for the dashboard or http://localhost:8000/docs for the interactive API reference.
2. In `.env`, find the token configured for the `demo-admin` subject in `INVENTORY_TOKENS_JSON`. Use the `demo-viewer` subject to verify read-only permissions and `demo-bodega-uio` to verify warehouse isolation. Treat these generated tokens as local secrets.
3. Look up `MILK-1L` at `UIO-01`. Reserve six units with a unique `Idempotency-Key`, then repeat the same request. Allocation consumes eligible lots by expiration date, skips the expired lot, and does not reserve twice. The remaining eligible stock reaches the threshold and creates a suggestion that an authorized operator can approve.

To demonstrate order status without a live integration, generate synthetic order events. The worker publishes them to the broker, and the consumer updates the same dashboard row:

```sh
docker compose exec -T api python scripts/demo-order.py --order-id DEMO-001 --location UIO-01 --status PREPARING
docker compose exec -T api python scripts/demo-order.py --order-id DEMO-001 --location UIO-01 --status READY
```

Read [OpenAPI](docs/openapi.json) without Docker. The following local checks require no broker except the opt-in broker integration test:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python scripts/benchmark.py
```

## API contract

| Method and path | Scope | Result |
| --- | --- | --- |
| GET `/api/v1/inventory` | `inventory:read` | On-hand, reserved and available stock; SKU, warehouse and lot filters; pagination. |
| POST `/api/v1/reservations` | `inventory:reserve` | Atomic, idempotent FEFO reservation with warehouse-scoped access. |
| GET `/api/v1/replenishment/suggestions` | `replenishment:read` | Suggestions filtered by status, SKU and supplier. |
| POST `/api/v1/replenishment/suggestions/{id}/approve` | `replenishment:approve` | Approves a pending suggestion exactly once at the business-state level. |
| GET `/api/v1/dashboard` | `dashboard:read` | Projection, last sync, processing lag and recent errors. |

## Documentation and verification

- [Architecture](docs/ARQUITECTURA.md), [acceptance and release plan](docs/GESTION.md), [operations runbook](docs/OPERACION.md) and [demo walkthrough](docs/DEMO.md).
- [Local test and benchmark evidence](evidence/README.md). Results characterize a local process and do not establish a production SLA.
- [GitHub Actions](https://github.com/VillaforTech/smart-inventory/actions) provides per-revision check status; workflow configuration is not a green run.
- The sample order publisher is synthetic. No integration with live customer orders or fleet systems is included.

## Production readiness gates

Before serving live traffic, connect an identity provider with credential rotation; enforce TLS, distributed limits and telemetry at the ingress; validate a resilient data store and broker topology, backup/restore, and disaster recovery; and record passing integration/security checks and workload-specific load and recovery tests in staging. Do not expose the local Compose stack or its demo credentials on the public internet.
