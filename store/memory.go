package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/dtonair/liu/model"
)

// MemStore is an in-memory Store implementation used for local development and
// as the fast backend for the shared contract suite. It is safe for concurrent
// use: a single mutex guards all state and is held for the duration of each Tx.
type MemStore struct {
	mu sync.Mutex

	defs        map[string]*model.Definition // key: name|version
	defChecksum map[string]string            // key: name|version
	latest      map[string]int               // name -> max version

	instances map[string]*model.Instance
	idemIndex map[string]string // tenant|key -> instance id

	schedules map[string]*model.Schedule

	events  map[string][]*model.Event
	seq     map[string]int64
	eventID int64

	tasks    map[string]*model.Task
	taskIdem map[string]string // idempotency key -> task id

	timers  map[string]*model.Timer
	signals map[string]*model.Signal

	outbox    []*model.OutboxRecord
	outboxSeq int64
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		defs:        map[string]*model.Definition{},
		defChecksum: map[string]string{},
		latest:      map[string]int{},
		instances:   map[string]*model.Instance{},
		idemIndex:   map[string]string{},
		schedules:   map[string]*model.Schedule{},
		events:      map[string][]*model.Event{},
		seq:         map[string]int64{},
		tasks:       map[string]*model.Task{},
		taskIdem:    map[string]string{},
		timers:      map[string]*model.Timer{},
		signals:     map[string]*model.Signal{},
	}
}

func defKey(name string, version int) string { return fmt.Sprintf("%s|%d", name, version) }

// --- Definitions ---

func (m *MemStore) PutDefinition(_ context.Context, def *model.Definition, checksum string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := defKey(def.Name, def.Version)
	if existing, ok := m.defChecksum[k]; ok {
		if existing != checksum {
			return ErrChecksumConflict
		}
		return nil
	}
	cp := *def
	m.defs[k] = &cp
	m.defChecksum[k] = checksum
	if def.Version > m.latest[def.Name] {
		m.latest[def.Name] = def.Version
	}
	return nil
}

func (m *MemStore) GetDefinition(_ context.Context, name string, version int) (*model.Definition, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.defs[defKey(name, version)]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *d
	return &cp, nil
}

func (m *MemStore) GetLatestDefinition(ctx context.Context, name string) (*model.Definition, error) {
	m.mu.Lock()
	v, ok := m.latest[name]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return m.GetDefinition(ctx, name, v)
}

// --- Instances ---

func (m *MemStore) CreateInstance(_ context.Context, inst *model.Instance) (*model.Instance, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inst.IdempotencyKey != "" {
		ik := inst.TenantID + "|" + inst.IdempotencyKey
		if id, ok := m.idemIndex[ik]; ok {
			cp := *m.instances[id]
			return &cp, false, nil
		}
		m.idemIndex[ik] = inst.ID
	}
	cp := *inst
	m.instances[inst.ID] = &cp
	ret := cp
	return &ret, true, nil
}

func (m *MemStore) GetInstance(_ context.Context, id string) (*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.instances[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *i
	return &cp, nil
}

func (m *MemStore) ListInstances(_ context.Context, f InstanceFilter) ([]*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.Instance
	for _, i := range m.instances {
		if f.TenantID != "" && i.TenantID != f.TenantID {
			continue
		}
		if f.WorkflowName != "" && i.WorkflowName != f.WorkflowName {
			continue
		}
		if f.Status != "" && i.Status != f.Status {
			continue
		}
		cp := *i
		out = append(out, &cp)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.Before(out[b].CreatedAt) })
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (m *MemStore) RunnableInstances(_ context.Context, limit int) ([]*model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.Instance
	for _, i := range m.instances {
		if i.Status == model.StatusRunnable {
			cp := *i
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].UpdatedAt.Before(out[b].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// --- Schedules ---

func (m *MemStore) CreateSchedule(_ context.Context, sched *model.Schedule) (*model.Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.schedules[sched.ID]; ok {
		return nil, ErrVersionConflict
	}
	cp := *sched
	m.schedules[sched.ID] = &cp
	ret := cp
	return &ret, nil
}

func (m *MemStore) GetSchedule(_ context.Context, id string) (*model.Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.schedules[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *MemStore) ListSchedules(_ context.Context, tenantID string) ([]*model.Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.Schedule
	for _, s := range m.schedules {
		if tenantID != "" && s.TenantID != tenantID {
			continue
		}
		cp := *s
		out = append(out, &cp)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.Before(out[b].CreatedAt) })
	return out, nil
}

func (m *MemStore) UpdateScheduleEnabled(_ context.Context, id, tenantID string, enabled bool, nextRunAt time.Time, updatedAt time.Time) (*model.Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.schedules[id]
	if !ok || s.TenantID != tenantID {
		return nil, ErrNotFound
	}
	s.Enabled = enabled
	if !nextRunAt.IsZero() {
		s.NextRunAt = nextRunAt
	}
	s.UpdatedAt = updatedAt
	cp := *s
	return &cp, nil
}

func (m *MemStore) DeleteSchedule(_ context.Context, id, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.schedules[id]
	if !ok || s.TenantID != tenantID {
		return ErrNotFound
	}
	delete(m.schedules, id)
	return nil
}

func (m *MemStore) DueSchedules(_ context.Context, now time.Time, limit int) ([]*model.Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.Schedule
	for _, s := range m.schedules {
		if s.Enabled && !s.NextRunAt.After(now) {
			if s.ClaimedUntil != nil && s.ClaimedUntil.After(now) {
				continue
			}
			claim := now.Add(time.Minute)
			s.ClaimedUntil = &claim
			cp := *s
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].NextRunAt.Before(out[b].NextRunAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) MarkScheduleRun(_ context.Context, run ScheduleRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.schedules[run.ScheduleID]
	if !ok {
		return ErrNotFound
	}
	last := run.RunAt
	s.LastRunAt = &last
	s.NextRunAt = run.NextRunAt
	s.ClaimedUntil = nil
	s.UpdatedAt = run.UpdatedAt
	return nil
}

// --- Tasks ---

func (m *MemStore) LeaseTasks(_ context.Context, req LeaseRequest) ([]*model.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var cands []*model.Task
	for _, t := range m.tasks {
		if t.Status != model.TaskQueued {
			continue
		}
		if t.ActivityType != req.ActivityType {
			continue
		}
		if req.TenantID != "" && t.TenantID != req.TenantID {
			continue
		}
		if t.VisibleAt.After(req.Now) {
			continue
		}
		cands = append(cands, t)
	}
	sort.Slice(cands, func(a, b int) bool {
		if cands[a].Priority != cands[b].Priority {
			return cands[a].Priority > cands[b].Priority
		}
		return cands[a].CreatedAt.Before(cands[b].CreatedAt)
	})
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	var out []*model.Task
	for _, t := range cands {
		if len(out) >= limit {
			break
		}
		exp := req.Now.Add(req.LeaseFor)
		t.Status = model.TaskLeased
		t.LeasedBy = req.WorkerID
		t.LeaseToken = uuidNew()
		t.LeaseExpiresAt = &exp
		cp := *t
		out = append(out, &cp)
	}
	return out, nil
}

func (m *MemStore) GetTask(_ context.Context, id string) (*model.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (m *MemStore) HeartbeatTask(_ context.Context, taskID, workerID, leaseToken string, until time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[taskID]
	if !ok {
		return ErrNotFound
	}
	if t.Status != model.TaskLeased || t.LeaseToken != leaseToken || t.LeasedBy != workerID {
		return ErrLeaseInvalid
	}
	t.LeaseExpiresAt = &until
	return nil
}

func (m *MemStore) ExpireLeases(_ context.Context, now time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, t := range m.tasks {
		if t.Status == model.TaskLeased && t.LeaseExpiresAt != nil && !t.LeaseExpiresAt.After(now) {
			t.Status = model.TaskQueued
			t.LeasedBy = ""
			t.LeaseToken = ""
			t.LeaseExpiresAt = nil
			n++
		}
	}
	return n, nil
}

// --- Timers ---

func (m *MemStore) DueTimers(_ context.Context, now time.Time, limit int) ([]*model.Timer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.Timer
	for _, t := range m.timers {
		if !t.Fired && !t.FireAt.After(now) {
			cp := *t
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].FireAt.Before(out[b].FireAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// --- Signals ---

func (m *MemStore) AppendSignal(_ context.Context, sig *model.Signal) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *sig
	m.signals[sig.ID] = &cp
	return nil
}

// --- History ---

func (m *MemStore) History(_ context.Context, instanceID string) ([]*model.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.events[instanceID]
	out := make([]*model.Event, len(src))
	for i, e := range src {
		cp := *e
		out[i] = &cp
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Seq < out[b].Seq })
	return out, nil
}

// --- Outbox ---

func (m *MemStore) UnsentOutbox(_ context.Context, limit int) ([]*model.OutboxRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*model.OutboxRecord
	for _, r := range m.outbox {
		if r.SentAt == nil {
			cp := *r
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) MarkOutboxSent(_ context.Context, id int64, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.outbox {
		if r.ID == id {
			t := at
			r.SentAt = &t
			return nil
		}
	}
	return ErrNotFound
}

func (m *MemStore) Close() error { return nil }

// --- Transactions ---

type memSnapshot struct {
	instances map[string]*model.Instance
	schedules map[string]*model.Schedule
	events    map[string][]*model.Event
	seq       map[string]int64
	eventID   int64
	tasks     map[string]*model.Task
	taskIdem  map[string]string
	timers    map[string]*model.Timer
	signals   map[string]*model.Signal
	outbox    []*model.OutboxRecord
	outboxSeq int64
}

func (m *MemStore) snapshot() memSnapshot {
	s := memSnapshot{
		instances: make(map[string]*model.Instance, len(m.instances)),
		schedules: make(map[string]*model.Schedule, len(m.schedules)),
		events:    make(map[string][]*model.Event, len(m.events)),
		seq:       make(map[string]int64, len(m.seq)),
		eventID:   m.eventID,
		tasks:     make(map[string]*model.Task, len(m.tasks)),
		taskIdem:  make(map[string]string, len(m.taskIdem)),
		timers:    make(map[string]*model.Timer, len(m.timers)),
		signals:   make(map[string]*model.Signal, len(m.signals)),
		outbox:    append([]*model.OutboxRecord(nil), m.outbox...),
		outboxSeq: m.outboxSeq,
	}
	// Snapshot stores copies so in-place mutation inside the failed Tx cannot
	// leak into the restored state.
	for k, v := range m.instances {
		cp := *v
		s.instances[k] = &cp
	}
	for k, v := range m.schedules {
		cp := *v
		s.schedules[k] = &cp
	}
	for k, v := range m.events {
		s.events[k] = append([]*model.Event(nil), v...)
	}
	for k, v := range m.seq {
		s.seq[k] = v
	}
	for k, v := range m.tasks {
		cp := *v
		s.tasks[k] = &cp
	}
	for k, v := range m.taskIdem {
		s.taskIdem[k] = v
	}
	for k, v := range m.timers {
		cp := *v
		s.timers[k] = &cp
	}
	for k, v := range m.signals {
		cp := *v
		s.signals[k] = &cp
	}
	return s
}

func (m *MemStore) restore(s memSnapshot) {
	m.instances = s.instances
	m.schedules = s.schedules
	m.events = s.events
	m.seq = s.seq
	m.eventID = s.eventID
	m.tasks = s.tasks
	m.taskIdem = s.taskIdem
	m.timers = s.timers
	m.signals = s.signals
	m.outbox = s.outbox
	m.outboxSeq = s.outboxSeq
}

func (m *MemStore) Tx(ctx context.Context, fn func(tx Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := m.snapshot()
	if err := fn(&memTx{m: m}); err != nil {
		m.restore(snap)
		return err
	}
	return nil
}

// memTx mutates MemStore state directly; the enclosing Tx holds m.mu and
// snapshots state for rollback on error.
type memTx struct{ m *MemStore }

func (t *memTx) GetInstanceForUpdate(_ context.Context, id string) (*model.Instance, error) {
	i, ok := t.m.instances[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *i
	return &cp, nil
}

func (t *memTx) UpdateInstance(_ context.Context, inst *model.Instance) error {
	cur, ok := t.m.instances[inst.ID]
	if !ok {
		return ErrNotFound
	}
	if cur.RowVersion != inst.RowVersion {
		return ErrVersionConflict
	}
	cp := *inst
	cp.RowVersion = inst.RowVersion + 1
	t.m.instances[inst.ID] = &cp
	return nil
}

func (t *memTx) AppendEvent(_ context.Context, e *model.Event) (*model.Event, error) {
	t.m.seq[e.InstanceID]++
	t.m.eventID++
	cp := *e
	cp.Seq = t.m.seq[e.InstanceID]
	cp.ID = t.m.eventID
	t.m.events[e.InstanceID] = append(t.m.events[e.InstanceID], &cp)
	ret := cp
	return &ret, nil
}

func (t *memTx) EnqueueTask(_ context.Context, task *model.Task) error {
	if task.IdempotencyKey != "" {
		if _, dup := t.m.taskIdem[task.IdempotencyKey]; dup {
			return nil // dedupe: advance stays idempotent
		}
		t.m.taskIdem[task.IdempotencyKey] = task.ID
	}
	cp := *task
	t.m.tasks[task.ID] = &cp
	return nil
}

func (t *memTx) CompleteTask(_ context.Context, taskID, leaseToken string, _ json.RawMessage) error {
	task, ok := t.m.tasks[taskID]
	if !ok {
		return ErrNotFound
	}
	if task.Status == model.TaskDone {
		return nil // duplicate delivery: no-op
	}
	if task.LeaseToken != leaseToken {
		return ErrLeaseInvalid
	}
	task.Status = model.TaskDone
	return nil
}

func (t *memTx) FailTask(_ context.Context, taskID, leaseToken string) error {
	task, ok := t.m.tasks[taskID]
	if !ok {
		return ErrNotFound
	}
	if task.Status == model.TaskDone || task.Status == model.TaskFailed {
		return nil
	}
	if task.LeaseToken != leaseToken {
		return ErrLeaseInvalid
	}
	task.Status = model.TaskFailed
	return nil
}

func (t *memTx) RequeueTask(_ context.Context, taskID, leaseToken string, attempt int, visibleAt time.Time) error {
	task, ok := t.m.tasks[taskID]
	if !ok {
		return ErrNotFound
	}
	if task.Status == model.TaskDone {
		return nil
	}
	if task.LeaseToken != leaseToken {
		return ErrLeaseInvalid
	}
	task.Status = model.TaskQueued
	task.Attempt = attempt
	task.VisibleAt = visibleAt
	task.LeasedBy = ""
	task.LeaseToken = ""
	task.LeaseExpiresAt = nil
	return nil
}

func (t *memTx) CreateTimer(_ context.Context, timer *model.Timer) error {
	cp := *timer
	t.m.timers[timer.ID] = &cp
	return nil
}

func (t *memTx) MarkTimerFired(_ context.Context, timerID string) error {
	timer, ok := t.m.timers[timerID]
	if !ok {
		return ErrNotFound
	}
	timer.Fired = true
	return nil
}

func (t *memTx) ConsumeSignal(_ context.Context, instanceID, name string) (*model.Signal, bool, error) {
	var oldest *model.Signal
	for _, s := range t.m.signals {
		if s.InstanceID != instanceID || s.Name != name || s.Consumed {
			continue
		}
		if oldest == nil || s.CreatedAt.Before(oldest.CreatedAt) {
			oldest = s
		}
	}
	if oldest == nil {
		return nil, false, nil
	}
	oldest.Consumed = true
	cp := *oldest
	return &cp, true, nil
}

func (t *memTx) EnqueueOutbox(_ context.Context, r *model.OutboxRecord) error {
	t.m.outboxSeq++
	cp := *r
	cp.ID = t.m.outboxSeq
	t.m.outbox = append(t.m.outbox, &cp)
	return nil
}

// compile-time interface checks.
var (
	_ Store = (*MemStore)(nil)
	_ Tx    = (*memTx)(nil)
)
