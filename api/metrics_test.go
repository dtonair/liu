package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dtonair/liu/engine"
	"github.com/dtonair/liu/model"
	"github.com/dtonair/liu/security"
	"github.com/dtonair/liu/store"
	"github.com/dtonair/liu/telemetry"
	"net/http/httptest"
)

// TestMetricsExposed runs the flow on an engine wired with metrics and asserts
// the /metrics endpoint reflects the work (counters incremented, instruments
// registered).
func TestMetricsExposed(t *testing.T) {
	st := store.NewMemStore()
	metrics := telemetry.NewMetrics()
	eng := engine.New(st, engine.WithMetrics(metrics))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = engine.NewScheduler(eng, 10*time.Millisecond, 100).Run(ctx) }()
	go func() { _ = engine.NewMetricsSampler(eng, 20*time.Millisecond).Run(ctx) }()

	srv := NewServer(eng, st, Options{Auth: &security.Authenticator{Disabled: true}, PollInterval: 5 * time.Millisecond})
	hs := httptest.NewServer(srv.Router())
	t.Cleanup(hs.Close)

	registerOrderApproval(t, hs.URL)
	_, body := do(t, http.MethodPost, hs.URL+"/v1/workflows/order_approval/instances", "demo", map[string]any{"idempotency_key": "m1"})
	var started struct {
		InstanceID string `json:"instance_id"`
	}
	_ = json.Unmarshal(body, &started)

	pollComplete(t, hs.URL, "reserve_inventory")
	waitStatus(t, hs.URL, started.InstanceID, model.StatusWaiting)
	do(t, http.MethodPost, hs.URL+"/v1/instances/"+started.InstanceID+"/signals/manager_approval", "demo", nil)
	pollComplete(t, hs.URL, "capture_payment")
	waitStatus(t, hs.URL, started.InstanceID, model.StatusSucceeded)

	// Scrape /metrics.
	resp, scrape := do(t, http.MethodGet, hs.URL+"/metrics", "demo", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics: %d", resp.StatusCode)
	}
	text := string(scrape)
	for _, want := range []string{
		"liu_tasks_completed_total",
		"liu_advance_seconds",
		"liu_transitions_total",
		"liu_instances",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
	// At least the two activity completions must be counted.
	if !strings.Contains(text, "liu_tasks_completed_total 2") {
		t.Fatalf("expected liu_tasks_completed_total 2, metrics:\n%s", text)
	}
}
