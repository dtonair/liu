-- liu workflow engine: initial schema (spec data model, FR1-FR14).
-- Idempotent: safe to run repeatedly (IF NOT EXISTS throughout).

CREATE TABLE IF NOT EXISTS workflow_definitions (
    name            text        NOT NULL,
    version         integer     NOT NULL,
    definition_json jsonb       NOT NULL,
    checksum        text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, version)
);

CREATE TABLE IF NOT EXISTS workflow_instances (
    id              text           PRIMARY KEY,
    workflow_name   text           NOT NULL,
    version         integer        NOT NULL,
    current_step    text           NOT NULL,
    status          text           NOT NULL,
    tenant_id       text           NOT NULL,
    input_json      jsonb,
    idempotency_key text           NOT NULL DEFAULT '',
    error           text           NOT NULL DEFAULT '',
    row_version     integer        NOT NULL DEFAULT 0,
    created_at      timestamptz    NOT NULL DEFAULT now(),
    updated_at      timestamptz    NOT NULL DEFAULT now()
);

-- Idempotent start: one instance per (tenant, idempotency_key) when set.
CREATE UNIQUE INDEX IF NOT EXISTS uq_instances_idem
    ON workflow_instances (tenant_id, idempotency_key)
    WHERE idempotency_key <> '';

CREATE INDEX IF NOT EXISTS ix_instances_status   ON workflow_instances (status);
CREATE INDEX IF NOT EXISTS ix_instances_tenant   ON workflow_instances (tenant_id);
CREATE INDEX IF NOT EXISTS ix_instances_workflow ON workflow_instances (workflow_name);

CREATE TABLE IF NOT EXISTS workflow_history (
    id           bigserial   PRIMARY KEY,
    instance_id  text        NOT NULL,
    seq          bigint      NOT NULL,
    type         text        NOT NULL,
    step_id      text        NOT NULL DEFAULT '',
    payload_json jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (instance_id, seq)
);

CREATE TABLE IF NOT EXISTS tasks (
    id               text        PRIMARY KEY,
    instance_id      text        NOT NULL,
    step_id          text        NOT NULL,
    tenant_id        text        NOT NULL,
    activity_type    text        NOT NULL,
    status           text        NOT NULL,
    payload_json     jsonb,
    idempotency_key  text        NOT NULL DEFAULT '',
    attempt          integer     NOT NULL DEFAULT 1,
    max_attempts     integer     NOT NULL DEFAULT 1,
    priority         integer     NOT NULL DEFAULT 0,
    visible_at       timestamptz NOT NULL DEFAULT now(),
    leased_by        text        NOT NULL DEFAULT '',
    lease_token      text        NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- Dedupe re-enqueues of the same logical task (keeps advance idempotent).
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_idem
    ON tasks (idempotency_key)
    WHERE idempotency_key <> '';

-- Dequeue access path: poll by activity type for visible queued work.
CREATE INDEX IF NOT EXISTS ix_tasks_dequeue
    ON tasks (activity_type, status, visible_at);

CREATE INDEX IF NOT EXISTS ix_tasks_lease
    ON tasks (status, lease_expires_at)
    WHERE status = 'LEASED';

CREATE TABLE IF NOT EXISTS timers (
    id          text        PRIMARY KEY,
    instance_id text        NOT NULL,
    step_id     text        NOT NULL,
    tenant_id   text        NOT NULL,
    kind        text        NOT NULL,
    fire_at     timestamptz NOT NULL,
    fired       boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_timers_due ON timers (fire_at) WHERE NOT fired;

CREATE TABLE IF NOT EXISTS signals (
    id           text        PRIMARY KEY,
    instance_id  text        NOT NULL,
    tenant_id    text        NOT NULL,
    name         text        NOT NULL,
    payload_json jsonb,
    consumed     boolean     NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_signals_pending
    ON signals (instance_id, name, created_at)
    WHERE NOT consumed;

CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial   PRIMARY KEY,
    instance_id  text        NOT NULL,
    tenant_id    text        NOT NULL,
    event_type   text        NOT NULL,
    payload_json jsonb,
    sent_at      timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
