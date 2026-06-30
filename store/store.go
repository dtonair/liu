// Package store defines the persistence contract for the workflow engine and
// provides an in-memory implementation. A Postgres implementation lives in the
// same package behind the `postgres` build tag. Both implementations are
// verified by the single RunStoreContract suite so they are interchangeable.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dtonair/liu/model"
)

// Sentinel errors returned across all Store implementations.
var (
	// ErrNotFound is returned when a requested entity does not exist.
	ErrNotFound = errors.New("store: not found")
	// ErrVersionConflict is returned when an optimistic-concurrency update
	// loses the race (the row_version no longer matches).
	ErrVersionConflict = errors.New("store: row version conflict")
	// ErrChecksumConflict is returned when re-registering a definition under an
	// existing (name, version) with different content.
	ErrChecksumConflict = errors.New("store: definition checksum conflict")
	// ErrLeaseInvalid is returned when a task mutation presents a lease token
	// that does not match the current lease holder.
	ErrLeaseInvalid = errors.New("store: invalid or expired lease")
)

// InstanceFilter narrows ListInstances results. Zero-value fields are ignored.
type InstanceFilter struct {
	TenantID     string
	WorkflowName string
	Status       model.InstanceStatus
	Limit        int
}

// LeaseRequest parameterises a task poll (spec FR5).
type LeaseRequest struct {
	TenantID     string
	ActivityType string
	WorkerID     string
	Now          time.Time
	LeaseFor     time.Duration
	Limit        int
}

// ScheduleRun parameterises advancing a schedule after a due occurrence has
// successfully started a workflow instance.
type ScheduleRun struct {
	ScheduleID string
	RunAt      time.Time
	NextRunAt  time.Time
	UpdatedAt  time.Time
}

// Store is the durable persistence boundary. Read/utility methods live here;
// all multi-write state transitions go through Tx so they commit atomically and
// uphold the engine's advance-under-lock and commit-before-side-effect
// invariants.
type Store interface {
	// --- Definitions (immutable once written) ---

	// PutDefinition stores a definition+checksum. Re-putting the same
	// (name, version) with a different checksum returns ErrChecksumConflict;
	// re-putting identical content is a no-op success.
	PutDefinition(ctx context.Context, def *model.Definition, checksum string) error
	GetDefinition(ctx context.Context, name string, version int) (*model.Definition, error)
	GetLatestDefinition(ctx context.Context, name string) (*model.Definition, error)

	// --- Instances ---

	// CreateInstance persists a new instance. If an instance with the same
	// (tenant_id, idempotency_key) already exists, it is returned with
	// created=false and no new row is written (spec FR3).
	CreateInstance(ctx context.Context, inst *model.Instance) (result *model.Instance, created bool, err error)
	GetInstance(ctx context.Context, id string) (*model.Instance, error)
	ListInstances(ctx context.Context, f InstanceFilter) ([]*model.Instance, error)
	// RunnableInstances returns up to limit instances in RUNNABLE state for the
	// scheduler to advance.
	RunnableInstances(ctx context.Context, limit int) ([]*model.Instance, error)

	// --- Schedules ---

	CreateSchedule(ctx context.Context, sched *model.Schedule) (*model.Schedule, error)
	GetSchedule(ctx context.Context, id string) (*model.Schedule, error)
	ListSchedules(ctx context.Context, tenantID string) ([]*model.Schedule, error)
	UpdateScheduleEnabled(ctx context.Context, id, tenantID string, enabled bool, nextRunAt time.Time, updatedAt time.Time) (*model.Schedule, error)
	DeleteSchedule(ctx context.Context, id, tenantID string) error
	DueSchedules(ctx context.Context, now time.Time, limit int) ([]*model.Schedule, error)
	MarkScheduleRun(ctx context.Context, run ScheduleRun) error

	// --- Tasks (queue) ---

	// LeaseTasks atomically leases up to req.Limit QUEUED, visible tasks of the
	// requested activity type, marking them LEASED with an expiry. Concurrent
	// callers never receive the same task (Postgres: FOR UPDATE SKIP LOCKED).
	LeaseTasks(ctx context.Context, req LeaseRequest) ([]*model.Task, error)
	GetTask(ctx context.Context, id string) (*model.Task, error)
	// HeartbeatTask extends a lease for a long-running task (spec FR10).
	HeartbeatTask(ctx context.Context, taskID, workerID, leaseToken string, until time.Time) error
	// ExpireLeases returns timed-out LEASED tasks to QUEUED and reports how many
	// were reclaimed (spec FR10).
	ExpireLeases(ctx context.Context, now time.Time) (int, error)

	// --- Timers ---

	// DueTimers returns unfired timers whose fire_at <= now (spec FR8).
	DueTimers(ctx context.Context, now time.Time, limit int) ([]*model.Timer, error)

	// --- Signals ---

	// AppendSignal records an inbound signal in the instance inbox (spec FR9).
	AppendSignal(ctx context.Context, sig *model.Signal) error

	// --- History ---

	History(ctx context.Context, instanceID string) ([]*model.Event, error)

	// --- Outbox (transactional outbox publisher side) ---

	UnsentOutbox(ctx context.Context, limit int) ([]*model.OutboxRecord, error)
	MarkOutboxSent(ctx context.Context, id int64, at time.Time) error

	// --- Transactional unit ---

	// Tx runs fn inside a single transaction. On Postgres this is a DB
	// transaction; in memory it is a serialized critical section. Returning a
	// non-nil error rolls back all writes made through the Tx.
	Tx(ctx context.Context, fn func(tx Tx) error) error

	// Close releases underlying resources.
	Close() error
}

// Tx is the set of mutating operations available inside Store.Tx. Every method
// participates in the enclosing transaction.
type Tx interface {
	// GetInstanceForUpdate loads an instance and locks it for the remainder of
	// the transaction so concurrent advances serialize.
	GetInstanceForUpdate(ctx context.Context, id string) (*model.Instance, error)
	// UpdateInstance persists an instance using optimistic concurrency: it
	// requires the stored row_version to equal inst.RowVersion, then bumps it.
	// Returns ErrVersionConflict on mismatch.
	UpdateInstance(ctx context.Context, inst *model.Instance) error

	// AppendEvent appends to the instance's append-only history, assigning the
	// next per-instance seq and returning the stored event.
	AppendEvent(ctx context.Context, e *model.Event) (*model.Event, error)

	// EnqueueTask inserts a task. Inserting a task whose idempotency key already
	// exists is a no-op (dedupe), keeping advance idempotent.
	EnqueueTask(ctx context.Context, t *model.Task) error
	// CompleteTask marks a leased task DONE. A token mismatch returns
	// ErrLeaseInvalid; an already-DONE task is a no-op (duplicate delivery).
	CompleteTask(ctx context.Context, taskID, leaseToken string, output json.RawMessage) error
	// FailTask marks a leased task FAILED (terminal for this attempt).
	FailTask(ctx context.Context, taskID, leaseToken string) error
	// RequeueTask re-queues a task for another attempt at visibleAt with an
	// incremented attempt counter (retry path).
	RequeueTask(ctx context.Context, taskID, leaseToken string, attempt int, visibleAt time.Time) error

	// CreateTimer inserts a durable timer.
	CreateTimer(ctx context.Context, t *model.Timer) error
	// MarkTimerFired flags a timer fired so it is not processed twice.
	MarkTimerFired(ctx context.Context, timerID string) error

	// ConsumeSignal claims the oldest unconsumed signal of the given name for
	// the instance, returning found=false if none is pending.
	ConsumeSignal(ctx context.Context, instanceID, name string) (sig *model.Signal, found bool, err error)

	// EnqueueOutbox writes an event to be relayed after commit (spec FR12).
	EnqueueOutbox(ctx context.Context, r *model.OutboxRecord) error
}

// NewID returns a fresh unique identifier for entities that need one.
func NewID() string { return uuidNew() }
