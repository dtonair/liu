package model

import (
	"encoding/json"
	"time"
)

// InstanceStatus is the lifecycle state of a workflow instance.
type InstanceStatus string

const (
	// StatusRunnable means the scheduler should advance the instance.
	StatusRunnable InstanceStatus = "RUNNABLE"
	// StatusWaiting means the instance is parked on a signal or timer.
	StatusWaiting InstanceStatus = "WAITING"
	// StatusSucceeded is a terminal success state.
	StatusSucceeded InstanceStatus = "SUCCEEDED"
	// StatusFailed is a terminal failure state.
	StatusFailed InstanceStatus = "FAILED"
)

// Terminal reports whether the status is a terminal state.
func (s InstanceStatus) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed
}

// Instance is a running occurrence of a workflow definition.
type Instance struct {
	ID             string          `json:"id"`
	WorkflowName   string          `json:"workflow_name"`
	Version        int             `json:"version"`
	CurrentStep    string          `json:"current_step"`
	Status         InstanceStatus  `json:"status"`
	TenantID       string          `json:"tenant_id"`
	Input          json.RawMessage `json:"input,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Error          string          `json:"error,omitempty"`
	// RowVersion implements optimistic concurrency control; every persisted
	// mutation must bump it and verify the prior value.
	RowVersion int       `json:"row_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
