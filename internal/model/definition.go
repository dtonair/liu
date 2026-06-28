// Package model defines the workflow definition IR (the internal
// representation parsed from the JSON DSL) and the core persisted data types:
// instances, tasks, timers, signals, history events, and outbox records.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// StepType enumerates the four kernel step types (spec FR2).
type StepType string

// The four supported step types.
const (
	StepActivity   StepType = "activity"
	StepWaitSignal StepType = "wait_signal"
	StepSleepUntil StepType = "sleep_until"
	StepEnd        StepType = "end"
)

// RetryPolicy controls how a failed activity task is retried (spec FR7).
type RetryPolicy struct {
	MaxAttempts        int      `json:"max_attempts,omitempty"`
	InitialInterval    Duration `json:"initial_interval,omitempty"`
	BackoffCoefficient float64  `json:"backoff_coefficient,omitempty"`
	MaxInterval        Duration `json:"max_interval,omitempty"`
	Jitter             float64  `json:"jitter,omitempty"`
	NonRetryableErrors []string `json:"non_retryable_errors,omitempty"`
}

// DefaultRetryPolicy returns the policy applied to activity steps that omit one.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:        3,
		InitialInterval:    Duration(1_000_000_000), // 1s
		BackoffCoefficient: 2.0,
		MaxInterval:        Duration(60_000_000_000), // 60s
		Jitter:             0.2,
	}
}

// Step is a single node in a workflow definition.
type Step struct {
	ID   string   `json:"id"`
	Type StepType `json:"type"`

	// Activity steps.
	Activity string       `json:"activity,omitempty"`
	Retry    *RetryPolicy `json:"retry,omitempty"`

	// wait_signal steps.
	Signal       string   `json:"signal,omitempty"`
	TimeoutNext  string   `json:"timeout_next,omitempty"`
	TimeoutAfter Duration `json:"timeout_after,omitempty"`

	// sleep_until steps.
	SleepFor Duration `json:"sleep_for,omitempty"`

	// Common successor (activity, wait_signal, sleep_until).
	Next string `json:"next,omitempty"`
}

// Definition is a versioned workflow definition (spec FR1).
type Definition struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Initial string `json:"initial"`
	Steps   []Step `json:"steps"`
}

// StepByID returns the step with the given id, or false if absent.
func (d *Definition) StepByID(id string) (Step, bool) {
	for _, s := range d.Steps {
		if s.ID == id {
			return s, true
		}
	}
	return Step{}, false
}

// EffectiveRetry returns the step's retry policy, or the default if unset.
func (s Step) EffectiveRetry() RetryPolicy {
	if s.Retry != nil {
		return *s.Retry
	}
	return DefaultRetryPolicy()
}

// ParseDefinition unmarshals and validates a definition from JSON bytes.
func ParseDefinition(b []byte) (*Definition, error) {
	var d Definition
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("parse definition: %w", err)
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Validate enforces structural integrity (spec FR1):
//   - name set, version >= 1
//   - initial set and references an existing step
//   - unique step ids
//   - next / timeout_next reference existing steps
//   - at least one end step is reachable from initial
//   - per-type required fields are present
func (d *Definition) Validate() error {
	if d.Name == "" {
		return fmt.Errorf("definition: name is required")
	}
	if d.Version < 1 {
		return fmt.Errorf("definition %q: version must be >= 1", d.Name)
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("definition %q: at least one step is required", d.Name)
	}

	ids := make(map[string]Step, len(d.Steps))
	for _, s := range d.Steps {
		if s.ID == "" {
			return fmt.Errorf("definition %q: a step is missing an id", d.Name)
		}
		if _, dup := ids[s.ID]; dup {
			return fmt.Errorf("definition %q: duplicate step id %q", d.Name, s.ID)
		}
		ids[s.ID] = s
	}

	if d.Initial == "" {
		return fmt.Errorf("definition %q: initial is required", d.Name)
	}
	if _, ok := ids[d.Initial]; !ok {
		return fmt.Errorf("definition %q: initial step %q does not exist", d.Name, d.Initial)
	}

	ref := func(from, target, field string) error {
		if target == "" {
			return nil
		}
		if _, ok := ids[target]; !ok {
			return fmt.Errorf("definition %q: step %q %s references unknown step %q", d.Name, from, field, target)
		}
		return nil
	}

	for _, s := range d.Steps {
		switch s.Type {
		case StepActivity:
			if s.Activity == "" {
				return fmt.Errorf("definition %q: activity step %q missing activity name", d.Name, s.ID)
			}
			if s.Next == "" {
				return fmt.Errorf("definition %q: activity step %q missing next", d.Name, s.ID)
			}
			if r := s.Retry; r != nil && r.MaxAttempts < 1 {
				return fmt.Errorf("definition %q: activity step %q retry max_attempts must be >= 1", d.Name, s.ID)
			}
		case StepWaitSignal:
			if s.Signal == "" {
				return fmt.Errorf("definition %q: wait_signal step %q missing signal name", d.Name, s.ID)
			}
			if s.Next == "" {
				return fmt.Errorf("definition %q: wait_signal step %q missing next", d.Name, s.ID)
			}
			if s.TimeoutAfter > 0 && s.TimeoutNext == "" {
				return fmt.Errorf("definition %q: wait_signal step %q has timeout_after but no timeout_next", d.Name, s.ID)
			}
		case StepSleepUntil:
			if s.SleepFor <= 0 {
				return fmt.Errorf("definition %q: sleep_until step %q missing positive sleep_for", d.Name, s.ID)
			}
			if s.Next == "" {
				return fmt.Errorf("definition %q: sleep_until step %q missing next", d.Name, s.ID)
			}
		case StepEnd:
			// terminal: no successors required
		default:
			return fmt.Errorf("definition %q: step %q has unknown type %q", d.Name, s.ID, s.Type)
		}

		if err := ref(s.ID, s.Next, "next"); err != nil {
			return err
		}
		if err := ref(s.ID, s.TimeoutNext, "timeout_next"); err != nil {
			return err
		}
	}

	if !d.reachesEnd(ids) {
		return fmt.Errorf("definition %q: no end step is reachable from initial %q", d.Name, d.Initial)
	}
	return nil
}

// reachesEnd does a BFS from initial following next/timeout_next and reports
// whether any reachable step is an end step.
func (d *Definition) reachesEnd(ids map[string]Step) bool {
	seen := map[string]bool{}
	queue := []string{d.Initial}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if seen[cur] {
			continue
		}
		seen[cur] = true
		s := ids[cur]
		if s.Type == StepEnd {
			return true
		}
		for _, nxt := range []string{s.Next, s.TimeoutNext} {
			if nxt != "" && !seen[nxt] {
				queue = append(queue, nxt)
			}
		}
	}
	return false
}

// Checksum returns a stable SHA-256 over the canonical JSON of the definition,
// used to detect conflicting re-registrations of the same (name, version).
func (d *Definition) Checksum() (string, error) {
	canon, err := canonicalJSON(d)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalJSON marshals v with map keys sorted so the byte output is stable.
func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, err
	}
	return marshalSorted(generic)
}

func marshalSorted(v any) ([]byte, error) {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf := []byte{'{'}
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			kb, _ := json.Marshal(k)
			buf = append(buf, kb...)
			buf = append(buf, ':')
			vb, err := marshalSorted(val[k])
			if err != nil {
				return nil, err
			}
			buf = append(buf, vb...)
		}
		return append(buf, '}'), nil
	case []any:
		buf := []byte{'['}
		for i, e := range val {
			if i > 0 {
				buf = append(buf, ',')
			}
			eb, err := marshalSorted(e)
			if err != nil {
				return nil, err
			}
			buf = append(buf, eb...)
		}
		return append(buf, ']'), nil
	default:
		return json.Marshal(val)
	}
}
