# AGENTS.md — liu workflow engine

Cached repo memory for future sessions. Keep this current when patterns change.

## What this is

A minimal durable workflow engine in Go. Durable orchestration kernel: JSON DSL
→ instance state machine → Postgres task queue (`FOR UPDATE SKIP LOCKED`) → HTTP
long-poll workers → durable timers → signal inbox → append-only history, with
idempotency, retry/backoff, transactional outbox, Prometheus metrics, and
advisory-lock leader election. Not a Temporal clone; no BPMN.

Spec + plan: `~/code/brain/spec/20260628/0(1)_{spec,impl}_simple_workflow_engine.md`.

## Layout

```
cmd/engine    control plane: API + leader-only loops (scheduler/timers/sweeper/outbox/sampler)
cmd/worker    out-of-engine task worker (demo order_approval handlers)
internal/model      Definition IR, DSL validation, checksum; instance/task/timer/signal/event/outbox types
internal/store      Store interface + Tx; in-memory impl; Postgres impl (pgx); embedded migrations; leader election
internal/engine     transitions (engine.go), retry calc, scheduler, timer loop, lease sweeper, outbox publisher, metrics sampler, clock
internal/api        chi router, handlers (definitions/instances/tasks/signals), health, /metrics
internal/security   JWT/header auth + tenant context
internal/telemetry  slog logger, Prometheus metrics, OTel trace API
internal/worker     worker HTTP client + Runner (poll/dispatch/heartbeat)
workflows/          sample definition (order_approval.json)
```

## Core invariants (do not break)

1. **Advance-under-lock**: every instance state transition runs inside
   `Store.Tx`, after `GetInstanceForUpdate` (FOR UPDATE / mutex) and guarded by
   a `row_version` CAS. Repeated `Advance` is a no-op once status ≠ RUNNABLE.
2. **Commit-before-side-effect**: DB state + outbox commit in one Tx before
   anything is externally observable. No 2PC — use the outbox.
3. **Record-then-apply**: signals and timer fires are recorded as events first,
   then applied by the scheduler/engine. Never mutate instance state directly
   from ingress.
4. **At-least-once + idempotency**: task delivery is at-least-once; workers must
   dedupe side effects on `task.IdempotencyKey`. The key is `instance|step|
   rowversion` — unique per step-entry, stable across retries of that task.
5. **Definitions resolved before Tx**: never call `e.definition()` inside a
   `Store.Tx` callback — the in-memory store serializes a whole Tx under one
   mutex and would deadlock. Resolve via `defForInstance` first.

## State machine

`RUNNABLE` → (Advance enters current step) → `WAITING` (activity task / signal /
sleep) or `SUCCEEDED` (end). Task completion / signal / timer set the next step
and flip back to `RUNNABLE`. Terminal failure → `FAILED`.

## How to extend

- **Add a step type**: add to `model.StepType` + validation in
  `model.Definition.Validate`; handle entry in `engine.enterStep`; handle its
  completion/transition path; cover it in `engine_test.go`. Keep the IR the
  execution kernel (authoring formats map onto it).
- **Add a Store backend**: implement `store.Store` + `store.Tx`, then make it
  pass `store.RunStoreContract` (the single source of truth). Memory + Postgres
  already share it.
- **Add a metric**: register it in `telemetry.Metrics`, increment at the commit
  boundary in the engine (see `OnTaskComplete`/`OnTaskFail` patterns).

## Testing

- `make test` — in-memory only, no DB.
- `make test-pg` / set `LIU_TEST_DATABASE_URL` — runs the Postgres contract +
  chaos test. They skip silently without the env var.
- The Postgres-backed `RunStoreContract` includes an 8-worker/50-task
  no-double-dispatch lease test; the chaos test proves crash recovery.

## Toolchain gotcha

Pinned to **Go 1.24** (`go 1.24.0`, `toolchain go1.24.5`) so the installed
golangci-lint (built with 1.24) works. pgx is pinned to 5.7.6 and OTel to 1.34
because their latest releases require Go 1.25. Use `GOTOOLCHAIN=local` and avoid
`go get`-ing deps that bump the go directive. Lint is clean — keep it that way
(revive's `unused-parameter` is disabled for interface conformance).

## Known v1 limits

Single active scheduler (advisory-lock leader, no sharding); no in-flight
version migration; tracing is OTel-API-only (no exporter wired); sub-workflows /
fan-out / saga-compensation steps are out of scope (IR is designed to admit
them).
