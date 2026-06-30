# liu operations runbook

## Signals to watch (Prometheus, `/metrics`)

| Metric | Type | Meaning / action |
|---|---|---|
| `liu_transitions_total{event}` | counter | State transitions by event type. Sudden drop ⇒ scheduler stalled (check leader). |
| `liu_tasks_completed_total` | counter | Successful task completions. |
| `liu_tasks_failed_total` | counter | Terminal task failures. Spike ⇒ a worker/activity is broken. |
| `liu_task_retries_total` | counter | Retries scheduled. Rising fast ⇒ flaky downstream; check worker logs. |
| `liu_timers_fired_total` | counter | Durable timers fired. Flatline with pending timers ⇒ timer loop/leader down. |
| `liu_schedule_runs_started_total` | counter | Workflow instances started by cron schedules. Flatline with due schedules ⇒ schedule loop/leader down. |
| `liu_schedule_run_failures_total` | counter | Due schedule attempts that failed before schedule advancement. Nonzero ⇒ inspect schedule loop logs. |
| `liu_leases_reclaimed_total` | counter | Expired leases reclaimed. Sustained nonzero ⇒ workers crashing or too slow. |
| `liu_advance_seconds` | histogram | Advance transaction latency. p95 climbing ⇒ DB pressure. |
| `liu_instances{status}` | gauge (sampled 5s) | Instance counts by status. Growing `RUNNABLE` ⇒ scheduler not keeping up; growing `FAILED` ⇒ investigate. |

## Health endpoints
- `GET /healthz` — process up.
- `GET /readyz` — DB reachable. Returns 503 if the pool can't be pinged.

## Leadership
- Only the replica holding Postgres advisory lock `0x11A20001` runs the
  scheduler/schedule/timer/sweeper/outbox loops. Others serve the API only.
- Failover: if the leader dies, its Postgres session ends and the advisory lock
  releases automatically; restart/another replica acquires it on next boot.
- To find the leader: it logs `this replica is the scheduler leader` at startup.

## Common tasks

**Find stuck instances** (waiting a long time):
```sql
SELECT id, workflow_name, current_step, status, updated_at
FROM workflow_instances
WHERE status = 'WAITING' AND updated_at < now() - interval '1 hour'
ORDER BY updated_at;
```

**Inspect an instance's history** (audit / debug):
```sql
SELECT seq, type, step_id, created_at FROM workflow_history
WHERE instance_id = '<id>' ORDER BY seq;
```

**Queue backlog by activity**:
```sql
SELECT activity_type, count(*) FROM tasks
WHERE status = 'QUEUED' AND visible_at <= now()
GROUP BY activity_type ORDER BY 2 DESC;
```

**Due workflow schedules**:
```sql
SELECT id, tenant_id, workflow_name, cron, timezone, next_run_at
FROM workflow_schedules
WHERE enabled AND next_run_at <= now()
ORDER BY next_run_at;
```

## Drills
- **Kill-worker test**: stop all workers mid-flow; confirm leased tasks expire,
  the sweeper requeues them, and instances complete once workers return. This is
  automated as `make chaos` (Postgres required).
- **Restore drill**: restore a DB backup into a scratch instance and run
  `make migrate` + smoke the API.

## Known v1 limits
- Single active scheduler/schedule loop (advisory-lock leader); no sharding.
- No schedule backfill/catch-up.
- No automatic in-flight migration across definition versions.
- Tracing is wired via the OpenTelemetry API only (no-op until an SDK/exporter
  is installed in `cmd/engine`); metrics are Prometheus.
