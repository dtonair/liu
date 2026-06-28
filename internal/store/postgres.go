package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dtonair/liu/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore is the production Postgres-backed Store. State transitions use real
// DB transactions; the task queue leases with FOR UPDATE SKIP LOCKED.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore connects to Postgres using the given URL and returns a store. The
// caller owns the lifecycle; Close releases the pool.
func NewPgStore(ctx context.Context, url string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &PgStore{pool: pool}, nil
}

// Pool exposes the underlying pool (used by Migrate and leader election).
func (s *PgStore) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the embedded schema migrations.
func (s *PgStore) Migrate(ctx context.Context) error { return Migrate(ctx, s.pool) }

// Close releases the connection pool.
func (s *PgStore) Close() error { s.pool.Close(); return nil }

func raw(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

// --- Definitions ---

func (s *PgStore) PutDefinition(ctx context.Context, def *model.Definition, checksum string) error {
	body, err := json.Marshal(def)
	if err != nil {
		return err
	}
	// Insert if absent; on conflict verify the checksum matches.
	var existing string
	err = s.pool.QueryRow(ctx, `
		INSERT INTO workflow_definitions (name, version, definition_json, checksum)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (name, version) DO UPDATE SET checksum = workflow_definitions.checksum
		RETURNING checksum`,
		def.Name, def.Version, body, checksum).Scan(&existing)
	if err != nil {
		return err
	}
	if existing != checksum {
		return ErrChecksumConflict
	}
	return nil
}

func (s *PgStore) GetDefinition(ctx context.Context, name string, version int) (*model.Definition, error) {
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT definition_json FROM workflow_definitions WHERE name=$1 AND version=$2`, name, version).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var d model.Definition
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *PgStore) GetLatestDefinition(ctx context.Context, name string) (*model.Definition, error) {
	var body []byte
	err := s.pool.QueryRow(ctx, `SELECT definition_json FROM workflow_definitions WHERE name=$1 ORDER BY version DESC LIMIT 1`, name).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var d model.Definition
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// --- Instances ---

func (s *PgStore) CreateInstance(ctx context.Context, inst *model.Instance) (*model.Instance, bool, error) {
	// Attempt insert; ON CONFLICT on the partial idempotency index does nothing.
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO workflow_instances
			(id, workflow_name, version, current_step, status, tenant_id, input_json, context_json, idempotency_key, error, row_version, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'',0,$10,$11)
		ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key <> '' DO NOTHING`,
		inst.ID, inst.WorkflowName, inst.Version, inst.CurrentStep, inst.Status, inst.TenantID,
		nullableJSON(inst.Input), nullableJSON(inst.Context), inst.IdempotencyKey, inst.CreatedAt, inst.UpdatedAt)
	if err != nil {
		return nil, false, err
	}
	if tag.RowsAffected() == 1 {
		got, err := s.GetInstance(ctx, inst.ID)
		return got, true, err
	}
	// Conflict: return the existing instance for this idempotency key.
	existing, err := s.getInstanceByIdem(ctx, inst.TenantID, inst.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

func (s *PgStore) getInstanceByIdem(ctx context.Context, tenant, key string) (*model.Instance, error) {
	row := s.pool.QueryRow(ctx, instanceCols+` WHERE tenant_id=$1 AND idempotency_key=$2`, tenant, key)
	return scanInstance(row)
}

func (s *PgStore) GetInstance(ctx context.Context, id string) (*model.Instance, error) {
	row := s.pool.QueryRow(ctx, instanceCols+` WHERE id=$1`, id)
	inst, err := scanInstance(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return inst, err
}

func (s *PgStore) ListInstances(ctx context.Context, f InstanceFilter) ([]*model.Instance, error) {
	q := instanceCols + ` WHERE ($1='' OR tenant_id=$1) AND ($2='' OR workflow_name=$2) AND ($3='' OR status=$3) ORDER BY created_at`
	args := []any{f.TenantID, f.WorkflowName, string(f.Status)}
	if f.Limit > 0 {
		q += ` LIMIT $4`
		args = append(args, f.Limit)
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInstances(rows)
}

func (s *PgStore) RunnableInstances(ctx context.Context, limit int) ([]*model.Instance, error) {
	rows, err := s.pool.Query(ctx, instanceCols+` WHERE status=$1 ORDER BY updated_at LIMIT $2`, model.StatusRunnable, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInstances(rows)
}

// --- Tasks ---

func (s *PgStore) LeaseTasks(ctx context.Context, req LeaseRequest) ([]*model.Task, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	leaseToken := uuidNew()
	expires := req.Now.Add(req.LeaseFor)
	rows, err := s.pool.Query(ctx, `
		WITH next_tasks AS (
			SELECT id FROM tasks
			WHERE status='QUEUED' AND activity_type=$1
			  AND ($2='' OR tenant_id=$2)
			  AND visible_at <= $3
			ORDER BY priority DESC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $4
		)
		UPDATE tasks t
		SET status='LEASED', leased_by=$5, lease_token=$6, lease_expires_at=$7
		FROM next_tasks
		WHERE t.id = next_tasks.id
		RETURNING `+taskColListT,
		req.ActivityType, req.TenantID, req.Now, limit, req.WorkerID, leaseToken, expires)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func (s *PgStore) GetTask(ctx context.Context, id string) (*model.Task, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+taskColList+` FROM tasks WHERE id=$1`, id)
	t, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

func (s *PgStore) HeartbeatTask(ctx context.Context, taskID, workerID, leaseToken string, until time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET lease_expires_at=$1
		WHERE id=$2 AND status='LEASED' AND lease_token=$3 AND leased_by=$4`,
		until, taskID, leaseToken, workerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseInvalid
	}
	return nil
}

func (s *PgStore) ExpireLeases(ctx context.Context, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks SET status='QUEUED', leased_by='', lease_token='', lease_expires_at=NULL
		WHERE status='LEASED' AND lease_expires_at <= $1`, now)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// --- Timers ---

func (s *PgStore) DueTimers(ctx context.Context, now time.Time, limit int) ([]*model.Timer, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, instance_id, step_id, tenant_id, kind, fire_at, fired, created_at
		FROM timers WHERE NOT fired AND fire_at <= $1 ORDER BY fire_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Timer
	for rows.Next() {
		var t model.Timer
		if err := rows.Scan(&t.ID, &t.InstanceID, &t.StepID, &t.TenantID, &t.Kind, &t.FireAt, &t.Fired, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}

// --- Signals ---

func (s *PgStore) AppendSignal(ctx context.Context, sig *model.Signal) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO signals (id, instance_id, tenant_id, name, payload_json, consumed, created_at)
		VALUES ($1,$2,$3,$4,$5,false,$6)`,
		sig.ID, sig.InstanceID, sig.TenantID, sig.Name, nullableJSON(sig.Payload), sig.CreatedAt)
	return err
}

// --- History ---

func (s *PgStore) History(ctx context.Context, instanceID string) ([]*model.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, instance_id, seq, type, step_id, payload_json, created_at
		FROM workflow_history WHERE instance_id=$1 ORDER BY seq`, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Event
	for rows.Next() {
		var e model.Event
		var payload []byte
		if err := rows.Scan(&e.ID, &e.InstanceID, &e.Seq, &e.Type, &e.StepID, &payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Payload = raw(payload)
		out = append(out, &e)
	}
	return out, rows.Err()
}

// --- Outbox ---

func (s *PgStore) UnsentOutbox(ctx context.Context, limit int) ([]*model.OutboxRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, instance_id, tenant_id, event_type, payload_json, sent_at, created_at
		FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.OutboxRecord
	for rows.Next() {
		var r model.OutboxRecord
		var payload []byte
		if err := rows.Scan(&r.ID, &r.InstanceID, &r.TenantID, &r.EventType, &payload, &r.SentAt, &r.CreatedAt); err != nil {
			return nil, err
		}
		r.Payload = raw(payload)
		out = append(out, &r)
	}
	return out, rows.Err()
}

func (s *PgStore) MarkOutboxSent(ctx context.Context, id int64, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox SET sent_at=$1 WHERE id=$2`, at, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// --- Transactions ---

func (s *PgStore) Tx(ctx context.Context, fn func(tx Tx) error) error {
	pgtx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(&pgTx{tx: pgtx}); err != nil {
		_ = pgtx.Rollback(ctx)
		return err
	}
	return pgtx.Commit(ctx)
}

type pgTx struct{ tx pgx.Tx }

func (t *pgTx) GetInstanceForUpdate(ctx context.Context, id string) (*model.Instance, error) {
	row := t.tx.QueryRow(ctx, instanceCols+` WHERE id=$1 FOR UPDATE`, id)
	inst, err := scanInstance(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return inst, err
}

func (t *pgTx) UpdateInstance(ctx context.Context, inst *model.Instance) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE workflow_instances
		SET current_step=$1, status=$2, error=$3, input_json=$4, context_json=$5, updated_at=$6, row_version=row_version+1
		WHERE id=$7 AND row_version=$8`,
		inst.CurrentStep, inst.Status, inst.Error, nullableJSON(inst.Input), nullableJSON(inst.Context), inst.UpdatedAt, inst.ID, inst.RowVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	return nil
}

func (t *pgTx) AppendEvent(ctx context.Context, e *model.Event) (*model.Event, error) {
	row := t.tx.QueryRow(ctx, `
		INSERT INTO workflow_history (instance_id, seq, type, step_id, payload_json, created_at)
		VALUES ($1, (SELECT COALESCE(MAX(seq),0)+1 FROM workflow_history WHERE instance_id=$1), $2, $3, $4, $5)
		RETURNING id, seq`,
		e.InstanceID, e.Type, e.StepID, nullableJSON(e.Payload), e.CreatedAt)
	cp := *e
	if err := row.Scan(&cp.ID, &cp.Seq); err != nil {
		return nil, err
	}
	return &cp, nil
}

func (t *pgTx) EnqueueTask(ctx context.Context, task *model.Task) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO tasks
			(id, instance_id, step_id, tenant_id, activity_type, status, payload_json, idempotency_key, attempt, max_attempts, priority, visible_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (idempotency_key) WHERE idempotency_key <> '' DO NOTHING`,
		task.ID, task.InstanceID, task.StepID, task.TenantID, task.ActivityType, task.Status,
		nullableJSON(task.Payload), task.IdempotencyKey, task.Attempt, task.MaxAttempts, task.Priority, task.VisibleAt, task.CreatedAt)
	return err
}

func (t *pgTx) CompleteTask(ctx context.Context, taskID, leaseToken string, _ json.RawMessage) error {
	return t.settleTask(ctx, taskID, leaseToken, model.TaskDone)
}

func (t *pgTx) FailTask(ctx context.Context, taskID, leaseToken string) error {
	return t.settleTask(ctx, taskID, leaseToken, model.TaskFailed)
}

func (t *pgTx) settleTask(ctx context.Context, taskID, leaseToken string, target model.TaskStatus) error {
	var status string
	var token string
	err := t.tx.QueryRow(ctx, `SELECT status, lease_token FROM tasks WHERE id=$1 FOR UPDATE`, taskID).Scan(&status, &token)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if model.TaskStatus(status) == model.TaskDone || model.TaskStatus(status) == model.TaskFailed {
		return nil // duplicate/terminal: no-op
	}
	if token != leaseToken {
		return ErrLeaseInvalid
	}
	_, err = t.tx.Exec(ctx, `UPDATE tasks SET status=$1 WHERE id=$2`, target, taskID)
	return err
}

func (t *pgTx) RequeueTask(ctx context.Context, taskID, leaseToken string, attempt int, visibleAt time.Time) error {
	var status string
	var token string
	err := t.tx.QueryRow(ctx, `SELECT status, lease_token FROM tasks WHERE id=$1 FOR UPDATE`, taskID).Scan(&status, &token)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if model.TaskStatus(status) == model.TaskDone {
		return nil
	}
	if token != leaseToken {
		return ErrLeaseInvalid
	}
	_, err = t.tx.Exec(ctx, `
		UPDATE tasks SET status='QUEUED', attempt=$1, visible_at=$2, leased_by='', lease_token='', lease_expires_at=NULL
		WHERE id=$3`, attempt, visibleAt, taskID)
	return err
}

func (t *pgTx) CreateTimer(ctx context.Context, timer *model.Timer) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO timers (id, instance_id, step_id, tenant_id, kind, fire_at, fired, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,false,$7)`,
		timer.ID, timer.InstanceID, timer.StepID, timer.TenantID, timer.Kind, timer.FireAt, timer.CreatedAt)
	return err
}

func (t *pgTx) MarkTimerFired(ctx context.Context, timerID string) error {
	tag, err := t.tx.Exec(ctx, `UPDATE timers SET fired=true WHERE id=$1`, timerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (t *pgTx) ConsumeSignal(ctx context.Context, instanceID, name string) (*model.Signal, bool, error) {
	var sig model.Signal
	var payload []byte
	err := t.tx.QueryRow(ctx, `
		SELECT id, instance_id, tenant_id, name, payload_json, created_at
		FROM signals
		WHERE instance_id=$1 AND name=$2 AND NOT consumed
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, instanceID, name).Scan(&sig.ID, &sig.InstanceID, &sig.TenantID, &sig.Name, &payload, &sig.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if _, err := t.tx.Exec(ctx, `UPDATE signals SET consumed=true WHERE id=$1`, sig.ID); err != nil {
		return nil, false, err
	}
	sig.Consumed = true
	sig.Payload = raw(payload)
	return &sig, true, nil
}

func (t *pgTx) EnqueueOutbox(ctx context.Context, r *model.OutboxRecord) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO outbox (instance_id, tenant_id, event_type, payload_json, created_at)
		VALUES ($1,$2,$3,$4,$5)`,
		r.InstanceID, r.TenantID, r.EventType, nullableJSON(r.Payload), r.CreatedAt)
	return err
}

// --- scan helpers ---

const instanceCols = `SELECT id, workflow_name, version, current_step, status, tenant_id, input_json, context_json, idempotency_key, error, row_version, created_at, updated_at FROM workflow_instances`

const taskColList = `id, instance_id, step_id, tenant_id, activity_type, status, payload_json, idempotency_key, attempt, max_attempts, priority, visible_at, leased_by, lease_token, lease_expires_at, created_at`

// taskColListT is taskColList qualified with the `t` alias, for RETURNING in
// the lease CTE where an unqualified `id` would be ambiguous with next_tasks.
const taskColListT = `t.id, t.instance_id, t.step_id, t.tenant_id, t.activity_type, t.status, t.payload_json, t.idempotency_key, t.attempt, t.max_attempts, t.priority, t.visible_at, t.leased_by, t.lease_token, t.lease_expires_at, t.created_at`

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanInstance(r rowScanner) (*model.Instance, error) {
	var inst model.Instance
	var input []byte
	var contextJSON []byte
	if err := r.Scan(&inst.ID, &inst.WorkflowName, &inst.Version, &inst.CurrentStep, &inst.Status, &inst.TenantID,
		&input, &contextJSON, &inst.IdempotencyKey, &inst.Error, &inst.RowVersion, &inst.CreatedAt, &inst.UpdatedAt); err != nil {
		return nil, err
	}
	inst.Input = raw(input)
	inst.Context = raw(contextJSON)
	return &inst, nil
}

func scanInstances(rows pgx.Rows) ([]*model.Instance, error) {
	var out []*model.Instance
	for rows.Next() {
		inst, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func scanTask(r rowScanner) (*model.Task, error) {
	var t model.Task
	var payload []byte
	if err := r.Scan(&t.ID, &t.InstanceID, &t.StepID, &t.TenantID, &t.ActivityType, &t.Status,
		&payload, &t.IdempotencyKey, &t.Attempt, &t.MaxAttempts, &t.Priority, &t.VisibleAt,
		&t.LeasedBy, &t.LeaseToken, &t.LeaseExpiresAt, &t.CreatedAt); err != nil {
		return nil, err
	}
	t.Payload = raw(payload)
	return &t, nil
}

func scanTasks(rows pgx.Rows) ([]*model.Task, error) {
	var out []*model.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func nullableJSON(m json.RawMessage) any {
	if len(m) == 0 {
		return nil
	}
	return []byte(m)
}

var (
	_ Store = (*PgStore)(nil)
	_ Tx    = (*pgTx)(nil)
)
