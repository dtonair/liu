package model

import (
	"encoding/json"
	"time"
)

// Schedule is a tenant-owned cron trigger that starts workflow instances.
type Schedule struct {
	ID           string          `json:"id"`
	TenantID     string          `json:"tenant_id"`
	WorkflowName string          `json:"workflow_name"`
	Version      int             `json:"version,omitempty"`
	Cron         string          `json:"cron"`
	Timezone     string          `json:"timezone"`
	Input        json.RawMessage `json:"input,omitempty"`
	Enabled      bool            `json:"enabled"`
	LastRunAt    *time.Time      `json:"last_run_at,omitempty"`
	NextRunAt    time.Time       `json:"next_run_at"`
	ClaimedUntil *time.Time      `json:"-"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}
