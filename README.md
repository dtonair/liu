# liu

A minimal but production-capable **durable workflow engine** in Go. It owns
multi-step process state, survives crashes and long waits, and resumes exactly
where it left off — without you hand-rolling retries, timers, and audit trails
across services.

`liu` is a durable orchestration *kernel*: a small JSON DSL → an instance state
machine → a Postgres-backed task queue (`FOR UPDATE SKIP LOCKED`) → HTTP
long-poll workers → durable timers → a signal inbox → append-only history, with
idempotency, retry/backoff, and a transactional outbox. It is deliberately *not*
a Temporal clone (no code-as-workflow replay) and not a BPMN suite.

## Concepts

- **Definition** — a versioned workflow: `{name, version, initial, steps[]}`.
- **Step types** — `activity` (enqueue work for a worker), `wait_signal` (block
  for an external event, with optional timeout), `sleep_until` (durable delay),
  `end` (terminal success).
- **Instance** — one running occurrence; status `RUNNABLE → WAITING →
  SUCCEEDED/FAILED`.
- **Task** — a unit of work a worker leases, runs, and reports.
- **Signal** — an external event delivered to an instance's inbox.
- **Worker** — an out-of-engine process that polls for tasks and runs business
  logic. At-least-once delivery means handlers must be **idempotent**.

## Architecture

```
client ─▶ API ─▶ registry / instances / history / tasks / timers / signals (Postgres)
                 ▲                                   │
   leader-only loops: scheduler · timers · sweeper · outbox · sampler
workers ◀── poll / complete / fail / heartbeat ──▶ API
```

See `docs/runbook.md` for operations and `~/code/brain/spec/20260628/` for the
spec and implementation plan.

## Quickstart

Requires Go 1.24 and Docker.

```bash
make up                       # start Postgres
make migrate                  # apply schema (embedded migrations)
make run-engine               # terminal 1: API + loops on :8080 (auth disabled)
make run-worker               # terminal 2: demo order_approval worker
```

Then drive a workflow (auth-disabled mode uses the `X-Tenant-ID` header):

```bash
# Register the sample definition
curl -s localhost:8080/v1/definitions -H 'X-Tenant-ID: demo' \
  --data-binary @workflows/order_approval.json

# Start an instance
ID=$(curl -s localhost:8080/v1/workflows/order_approval/instances \
  -H 'X-Tenant-ID: demo' -d '{"idempotency_key":"order-1"}' | jq -r .instance_id)

# The worker completes reserve_inventory; the instance parks on approval.
curl -s localhost:8080/v1/instances/$ID -H 'X-Tenant-ID: demo' | jq .status   # WAITING

# Approve; the worker completes capture_payment.
curl -s -XPOST localhost:8080/v1/instances/$ID/signals/manager_approval -H 'X-Tenant-ID: demo'
curl -s localhost:8080/v1/instances/$ID -H 'X-Tenant-ID: demo' | jq .status   # SUCCEEDED

# Audit trail
curl -s localhost:8080/v1/instances/$ID/history -H 'X-Tenant-ID: demo' | jq '.events[].type'
```

## API

All routes are authenticated and tenant-scoped. With `LIU_AUTH_DISABLED=true`
the tenant comes from the `X-Tenant-ID` header; otherwise from the `tenant_id`
claim of an HS256 bearer JWT.

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/definitions` | Register a definition (409 on checksum conflict, 400 if invalid) |
| POST | `/v1/workflows/{name}/instances` | Start an instance (idempotent on `idempotency_key`) |
| GET | `/v1/instances` | List instances (filter `?status=&workflow=`) |
| GET | `/v1/instances/{id}` | Get instance state |
| GET | `/v1/instances/{id}/history` | Append-only event history |
| POST | `/v1/instances/{id}/signals/{name}` | Deliver a signal |
| POST | `/v1/tasks/poll` | Long-poll for a task (204 on timeout) |
| POST | `/v1/tasks/{id}/complete` | Report success |
| POST | `/v1/tasks/{id}/fail` | Report failure (`retryable`, `error_class`) |
| POST | `/v1/tasks/{id}/heartbeat` | Extend a lease |
| GET | `/healthz` `/readyz` `/metrics` | Health and Prometheus metrics |

## Configuration (engine)

| Env | Default | Meaning |
|---|---|---|
| `LIU_DATABASE_URL` | `postgres://liu:liu@localhost:5432/liu?sslmode=disable` | Postgres DSN |
| `LIU_HTTP_ADDR` | `:8080` | Listen address |
| `LIU_AUTH_DISABLED` | `false` | Header-based tenant (local dev) |
| `LIU_JWT_SECRET` | — | HS256 secret when auth is enabled |
| `LIU_MIGRATE_ON_BOOT` | `false` | Apply migrations at startup |
| `LIU_TLS_CERT` / `LIU_TLS_KEY` | — | Enable TLS |
| `LIU_LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

## Development

```bash
make test       # unit + in-memory tests (no DB needed)
make test-pg    # full suite incl. Postgres contract (needs `make up`)
make chaos      # worker-crash recovery test (Postgres)
make lint
```

Postgres-backed tests skip automatically when `LIU_TEST_DATABASE_URL` is unset.
