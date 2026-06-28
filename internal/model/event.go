package model

import (
	"encoding/json"
	"time"
)

// Event types recorded in the append-only history (spec FR13).
const (
	EventWorkflowStarted    = "WORKFLOW_STARTED"
	EventTaskScheduled      = "TASK_SCHEDULED"
	EventTaskCompleted      = "TASK_COMPLETED"
	EventTaskFailed         = "TASK_FAILED"
	EventTaskRetryScheduled = "TASK_RETRY_SCHEDULED"
	EventSignalReceived     = "SIGNAL_RECEIVED"
	EventTimerCreated       = "TIMER_CREATED"
	EventTimerFired         = "TIMER_FIRED"
	EventStepEntered        = "STEP_ENTERED"
	EventWorkflowSucceeded  = "WORKFLOW_SUCCEEDED"
	EventWorkflowFailed     = "WORKFLOW_FAILED"
)

// Event is one entry in an instance's append-only history. Seq is a per-instance
// monotonic sequence number that makes history deterministically ordered.
type Event struct {
	ID         int64           `json:"id"`
	InstanceID string          `json:"instance_id"`
	Seq        int64           `json:"seq"`
	Type       string          `json:"type"`
	StepID     string          `json:"step_id,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

// TaskStatus is the lifecycle of a queued unit of work.
type TaskStatus string

const (
	TaskQueued TaskStatus = "QUEUED"
	TaskLeased TaskStatus = "LEASED"
	TaskDone   TaskStatus = "DONE"
	TaskFailed TaskStatus = "FAILED"
)

// Task is a unit of work for a worker to execute (spec FR5).
type Task struct {
	ID             string          `json:"id"`
	InstanceID     string          `json:"instance_id"`
	StepID         string          `json:"step_id"`
	TenantID       string          `json:"tenant_id"`
	ActivityType   string          `json:"activity_type"`
	Status         TaskStatus      `json:"status"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	IdempotencyKey string          `json:"idempotency_key"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	Priority       int             `json:"priority"`
	VisibleAt      time.Time       `json:"visible_at"`
	LeasedBy       string          `json:"leased_by,omitempty"`
	LeaseToken     string          `json:"lease_token,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// TimerKind distinguishes a wait_signal timeout from a sleep_until delay.
type TimerKind string

const (
	TimerSignalTimeout TimerKind = "signal_timeout"
	TimerSleep         TimerKind = "sleep"
)

// Timer is a durable scheduled wakeup (spec FR8).
type Timer struct {
	ID         string    `json:"id"`
	InstanceID string    `json:"instance_id"`
	StepID     string    `json:"step_id"`
	TenantID   string    `json:"tenant_id"`
	Kind       TimerKind `json:"kind"`
	FireAt     time.Time `json:"fire_at"`
	Fired      bool      `json:"fired"`
	CreatedAt  time.Time `json:"created_at"`
}

// Signal is an inbox entry for an external event (spec FR9).
type Signal struct {
	ID         string          `json:"id"`
	InstanceID string          `json:"instance_id"`
	TenantID   string          `json:"tenant_id"`
	Name       string          `json:"name"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Consumed   bool            `json:"consumed"`
	CreatedAt  time.Time       `json:"created_at"`
}

// OutboxRecord is an event to be relayed to external systems after commit
// (spec FR12, transactional outbox).
type OutboxRecord struct {
	ID         int64           `json:"id"`
	InstanceID string          `json:"instance_id"`
	TenantID   string          `json:"tenant_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	SentAt     *time.Time      `json:"sent_at,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}
