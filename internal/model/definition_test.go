package model

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseOrderApproval(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "workflows", "order_approval.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	def, err := ParseDefinition(b)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if def.Name != "order_approval" || def.Version != 1 || def.Initial != "reserve_inventory" {
		t.Fatalf("unexpected header: %+v", def)
	}
	if len(def.Steps) != 5 {
		t.Fatalf("want 5 steps, got %d", len(def.Steps))
	}
	approval, ok := def.StepByID("manager_approval")
	if !ok {
		t.Fatal("missing manager_approval step")
	}
	if approval.TimeoutAfter.Std() != 24*time.Hour {
		t.Fatalf("timeout_after = %v, want 24h", approval.TimeoutAfter.Std())
	}
	if approval.Type != StepWaitSignal {
		t.Fatalf("type = %v, want wait_signal", approval.Type)
	}
}

func TestChecksumStable(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "workflows", "order_approval.json"))
	if err != nil {
		t.Fatal(err)
	}
	d1, _ := ParseDefinition(b)
	d2, _ := ParseDefinition(b)
	c1, err := d1.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := d2.Checksum()
	if c1 != c2 {
		t.Fatalf("checksum not stable: %s != %s", c1, c2)
	}

	// Changing a field changes the checksum.
	d2.Steps[0].Activity = "different"
	c3, _ := d2.Checksum()
	if c3 == c1 {
		t.Fatal("checksum unchanged after mutation")
	}
}

func TestValidate(t *testing.T) {
	base := func() *Definition {
		return &Definition{
			Name:    "wf",
			Version: 1,
			Initial: "a",
			Steps: []Step{
				{ID: "a", Type: StepActivity, Activity: "do", Next: "end"},
				{ID: "end", Type: StepEnd},
			},
		}
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("base should be valid: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Definition)
	}{
		{"no name", func(d *Definition) { d.Name = "" }},
		{"bad version", func(d *Definition) { d.Version = 0 }},
		{"no steps", func(d *Definition) { d.Steps = nil }},
		{"no initial", func(d *Definition) { d.Initial = "" }},
		{"unknown initial", func(d *Definition) { d.Initial = "ghost" }},
		{"duplicate id", func(d *Definition) {
			d.Steps = append(d.Steps, Step{ID: "a", Type: StepEnd})
		}},
		{"dangling next", func(d *Definition) { d.Steps[0].Next = "ghost" }},
		{"unknown type", func(d *Definition) { d.Steps[0].Type = "frobnicate" }},
		{"activity missing activity", func(d *Definition) { d.Steps[0].Activity = "" }},
		{"activity missing next", func(d *Definition) { d.Steps[0].Next = "" }},
		{"no reachable end", func(d *Definition) {
			d.Steps = []Step{{ID: "a", Type: StepActivity, Activity: "do", Next: "a"}}
		}},
		{"wait_signal missing signal", func(d *Definition) {
			d.Steps[0] = Step{ID: "a", Type: StepWaitSignal, Next: "end"}
		}},
		{"timeout without timeout_next", func(d *Definition) {
			d.Steps[0] = Step{ID: "a", Type: StepWaitSignal, Signal: "s", Next: "end", TimeoutAfter: Duration(time.Hour)}
		}},
		{"sleep without duration", func(d *Definition) {
			d.Steps[0] = Step{ID: "a", Type: StepSleepUntil, Next: "end"}
		}},
		{"dangling timeout_next", func(d *Definition) {
			d.Steps[0] = Step{ID: "a", Type: StepWaitSignal, Signal: "s", Next: "end", TimeoutNext: "ghost", TimeoutAfter: Duration(time.Hour)}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := base()
			tc.mutate(d)
			if err := d.Validate(); err == nil {
				t.Fatalf("expected validation error for %q", tc.name)
			}
		})
	}
}

func TestEffectiveRetryDefault(t *testing.T) {
	s := Step{ID: "a", Type: StepActivity}
	if got := s.EffectiveRetry(); got.MaxAttempts != DefaultRetryPolicy().MaxAttempts {
		t.Fatalf("default retry not applied: %+v", got)
	}
	custom := RetryPolicy{MaxAttempts: 7}
	s.Retry = &custom
	if got := s.EffectiveRetry(); got.MaxAttempts != 7 {
		t.Fatalf("custom retry ignored: %+v", got)
	}
}
