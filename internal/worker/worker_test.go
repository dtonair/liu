package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dtonair/liu/internal/api"
	"github.com/dtonair/liu/internal/engine"
	"github.com/dtonair/liu/internal/model"
	"github.com/dtonair/liu/internal/security"
	"github.com/dtonair/liu/internal/store"
)

// testEngine spins up an in-memory engine behind httptest with the scheduler
// running, returning the base URL and the engine for signalling.
func testEngine(t *testing.T) (string, *engine.Engine) {
	t.Helper()
	st := store.NewMemStore()
	eng := engine.New(st)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = engine.NewScheduler(eng, 10*time.Millisecond, 100).Run(ctx) }()

	b, _ := os.ReadFile(filepath.Join("..", "..", "workflows", "order_approval.json"))
	def, err := model.ParseDefinition(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterDefinition(context.Background(), def); err != nil {
		t.Fatal(err)
	}
	srv := api.NewServer(eng, st, api.Options{Auth: &security.Authenticator{Disabled: true}, PollInterval: 5 * time.Millisecond})
	hs := httptest.NewServer(srv.Router())
	t.Cleanup(hs.Close)
	return hs.URL, eng
}

func TestWorkerEndToEnd(t *testing.T) {
	baseURL, eng := testEngine(t)

	client := NewClient(baseURL, "worker-1")
	client.TenantID = "demo"
	runner := NewRunner(client, RunnerOptions{Concurrency: 4, LeaseSeconds: 30})
	RegisterOrderApprovalHandlers(runner)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()

	// Start an instance directly on the engine.
	inst, err := eng.StartInstance(context.Background(), engine.StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "e2e-1"})
	if err != nil {
		t.Fatal(err)
	}

	// The worker should complete reserve_inventory, parking the instance on the
	// approval signal.
	waitFor(t, func() bool {
		return instStatus(t, eng, inst.ID) == model.StatusWaiting && onStep(t, eng, inst.ID, "manager_approval")
	})

	// Approve, and the worker should drive capture_payment to completion.
	if err := eng.SignalInstance(context.Background(), inst.ID, "demo", "manager_approval", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return instStatus(t, eng, inst.ID) == model.StatusSucceeded })
}

func TestWorkerNonRetryableFailure(t *testing.T) {
	baseURL, eng := testEngine(t)
	client := NewClient(baseURL, "worker-1")
	client.TenantID = "demo"
	runner := NewRunner(client, RunnerOptions{Concurrency: 2, LeaseSeconds: 30})
	runner.Register("reserve_inventory", func(_ context.Context, _ *model.Task) (json.RawMessage, error) {
		return nil, NonRetryable("bad_order", errors.New("invalid order"))
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()

	inst, _ := eng.StartInstance(context.Background(), engine.StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "e2e-2"})
	waitFor(t, func() bool { return instStatus(t, eng, inst.ID) == model.StatusFailed })
}

func TestWorkerRetryThenSucceed(t *testing.T) {
	baseURL, eng := testEngine(t)
	client := NewClient(baseURL, "worker-1")
	client.TenantID = "demo"
	runner := NewRunner(client, RunnerOptions{Concurrency: 2, LeaseSeconds: 30})

	var calls int32
	runner.Register("reserve_inventory", func(_ context.Context, _ *model.Task) (json.RawMessage, error) {
		// Fail (retryable) the first attempt, succeed on the retry.
		if atomic.AddInt32(&calls, 1) == 1 {
			return nil, errors.New("transient")
		}
		return json.RawMessage(`{"reserved":true}`), nil
	})
	// Also handle the rest of the flow so it can complete.
	for name, h := range OrderApprovalHandlers() {
		if name != "reserve_inventory" {
			runner.Register(name, h)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()

	inst, _ := eng.StartInstance(context.Background(), engine.StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "e2e-3"})
	// The default retry policy backs off ~1s; wait for the retry to land us on
	// the approval signal.
	waitForLong(t, func() bool { return onStep(t, eng, inst.ID, "manager_approval") }, 5*time.Second)
	if atomic.LoadInt32(&calls) < 2 {
		t.Fatalf("expected at least 2 handler calls (retry), got %d", calls)
	}
}

// --- helpers ---

func instStatus(t *testing.T, eng *engine.Engine, id string) model.InstanceStatus {
	t.Helper()
	inst, err := eng.Instance(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return inst.Status
}

func onStep(t *testing.T, eng *engine.Engine, id, step string) bool {
	t.Helper()
	inst, err := eng.Instance(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return inst.CurrentStep == step
}

func waitFor(t *testing.T, cond func() bool) { waitForLong(t, cond, 3*time.Second) }

func waitForLong(t *testing.T, cond func() bool, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
