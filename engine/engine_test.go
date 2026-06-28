package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dtonair/liu/model"
	"github.com/dtonair/liu/store"
)

func loadOrderApproval(t *testing.T) *model.Definition {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "workflows", "order_approval.json"))
	if err != nil {
		t.Fatal(err)
	}
	def, err := model.ParseDefinition(b)
	if err != nil {
		t.Fatal(err)
	}
	return def
}

// harness wires a memory store, engine with a fake clock, and a scheduler.
type harness struct {
	ctx   context.Context
	st    store.Store
	eng   *Engine
	sched *Scheduler
	clk   *FakeClock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	st := store.NewMemStore()
	clk := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	eng := New(st, WithClock(clk))
	def := loadOrderApproval(t)
	if err := eng.RegisterDefinition(context.Background(), def); err != nil {
		t.Fatalf("register: %v", err)
	}
	return &harness{ctx: context.Background(), st: st, eng: eng, sched: NewScheduler(eng, 0, 0), clk: clk}
}

// leaseAndComplete simulates a worker completing the next task of activityType.
func (h *harness) leaseAndComplete(t *testing.T, activityType string, output json.RawMessage) *model.Task {
	t.Helper()
	leased, err := h.st.LeaseTasks(h.ctx, store.LeaseRequest{TenantID: "demo", ActivityType: activityType, WorkerID: "w1", Now: h.clk.Now(), LeaseFor: time.Minute, Limit: 1})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease %s: err=%v n=%d", activityType, err, len(leased))
	}
	if err := h.eng.OnTaskComplete(h.ctx, leased[0].ID, "w1", leased[0].LeaseToken, output); err != nil {
		t.Fatalf("complete %s: %v", activityType, err)
	}
	return leased[0]
}

func (h *harness) status(t *testing.T, id string) model.InstanceStatus {
	t.Helper()
	inst, err := h.st.GetInstance(h.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return inst.Status
}

func (h *harness) eventTypes(t *testing.T, id string) []string {
	t.Helper()
	hist, _ := h.st.History(h.ctx, id)
	var out []string
	for _, e := range hist {
		out = append(out, e.Type)
	}
	return out
}

func TestHappyPath(t *testing.T) {
	h := newHarness(t)
	inst, err := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "order-1"})
	if err != nil {
		t.Fatal(err)
	}

	// reserve_inventory activity is enqueued, then completed.
	h.sched.Drain(h.ctx, 10)
	if got := h.status(t, inst.ID); got != model.StatusWaiting {
		t.Fatalf("after reserve scheduled, status=%s want WAITING", got)
	}
	h.leaseAndComplete(t, "reserve_inventory", nil)

	// Now parked on manager_approval (wait_signal).
	h.sched.Drain(h.ctx, 10)
	if got := h.status(t, inst.ID); got != model.StatusWaiting {
		t.Fatalf("after reserve complete, status=%s want WAITING (signal)", got)
	}

	// Approval signal advances to capture_payment.
	if err := h.eng.SignalInstance(h.ctx, inst.ID, "demo", "manager_approval", nil); err != nil {
		t.Fatal(err)
	}
	h.sched.Drain(h.ctx, 10)
	h.leaseAndComplete(t, "capture_payment", nil)
	h.sched.Drain(h.ctx, 10)

	if got := h.status(t, inst.ID); got != model.StatusSucceeded {
		t.Fatalf("final status=%s want SUCCEEDED", got)
	}

	// History is ordered and includes the key milestones.
	want := []string{
		model.EventWorkflowStarted,
		model.EventTaskScheduled,
		model.EventTaskCompleted,
		model.EventTimerCreated, // 24h approval timeout armed
		model.EventSignalReceived,
		model.EventTaskScheduled,
		model.EventTaskCompleted,
		model.EventWorkflowSucceeded,
	}
	got := h.eventTypes(t, inst.ID)
	if len(got) != len(want) {
		t.Fatalf("history = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history[%d] = %s, want %s (full %v)", i, got[i], want[i], got)
		}
	}
}

func TestTimeoutBranch(t *testing.T) {
	h := newHarness(t)
	inst, err := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "order-2"})
	if err != nil {
		t.Fatal(err)
	}
	h.sched.Drain(h.ctx, 10)
	h.leaseAndComplete(t, "reserve_inventory", nil)
	h.sched.Drain(h.ctx, 10) // parks on manager_approval, creates 24h timer

	// No approval; advance the clock past the 24h timeout and fire the timer.
	h.clk.Advance(25 * time.Hour)
	due, _ := h.st.DueTimers(h.ctx, h.clk.Now(), 10)
	if len(due) != 1 {
		t.Fatalf("want 1 due timer, got %d", len(due))
	}
	if err := h.eng.OnTimerFired(h.ctx, due[0]); err != nil {
		t.Fatal(err)
	}

	// Routes to cancel_order -> release_inventory activity.
	h.sched.Drain(h.ctx, 10)
	h.leaseAndComplete(t, "release_inventory", nil)
	h.sched.Drain(h.ctx, 10)

	if got := h.status(t, inst.ID); got != model.StatusSucceeded {
		t.Fatalf("final status=%s want SUCCEEDED (cancel path)", got)
	}
	types := h.eventTypes(t, inst.ID)
	if !contains(types, model.EventTimerFired) {
		t.Fatalf("expected TIMER_FIRED in history: %v", types)
	}
}

func TestSignalBeatsTimer(t *testing.T) {
	h := newHarness(t)
	inst, _ := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "order-3"})
	h.sched.Drain(h.ctx, 10)
	h.leaseAndComplete(t, "reserve_inventory", nil)
	h.sched.Drain(h.ctx, 10)

	// Signal arrives, advancing the instance off manager_approval.
	if err := h.eng.SignalInstance(h.ctx, inst.ID, "demo", "manager_approval", nil); err != nil {
		t.Fatal(err)
	}
	h.sched.Drain(h.ctx, 10)

	// Now the (still-pending) timer fires late: it must be a no-op, not reroute.
	h.clk.Advance(25 * time.Hour)
	due, _ := h.st.DueTimers(h.ctx, h.clk.Now(), 10)
	for _, ti := range due {
		if err := h.eng.OnTimerFired(h.ctx, ti); err != nil {
			t.Fatal(err)
		}
	}
	h.leaseAndComplete(t, "capture_payment", nil) // proves we took the approval path
	h.sched.Drain(h.ctx, 10)
	if got := h.status(t, inst.ID); got != model.StatusSucceeded {
		t.Fatalf("status=%s want SUCCEEDED via approval path", got)
	}
}

func TestEarlySignal(t *testing.T) {
	h := newHarness(t)
	inst, _ := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "order-4"})
	h.sched.Drain(h.ctx, 10)
	h.leaseAndComplete(t, "reserve_inventory", nil)

	// Signal arrives BEFORE the instance parks on manager_approval.
	if err := h.eng.SignalInstance(h.ctx, inst.ID, "demo", "manager_approval", nil); err != nil {
		t.Fatal(err)
	}
	// Now drain: entering wait_signal should consume the pending signal and
	// advance immediately.
	h.sched.Drain(h.ctx, 10)
	h.leaseAndComplete(t, "capture_payment", nil)
	h.sched.Drain(h.ctx, 10)
	if got := h.status(t, inst.ID); got != model.StatusSucceeded {
		t.Fatalf("status=%s want SUCCEEDED via early-signal path", got)
	}
}

func TestStartIdempotent(t *testing.T) {
	h := newHarness(t)
	a, _ := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "dup"})
	b, _ := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "dup"})
	if a.ID != b.ID {
		t.Fatalf("idempotent start returned different ids: %s vs %s", a.ID, b.ID)
	}
}

func TestActivityPayloadUsesAccumulatedContext(t *testing.T) {
	h := newHarness(t)
	input := json.RawMessage(`{"order_id":"order-ctx"}`)
	inst, err := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "order-ctx", Input: input})
	if err != nil {
		t.Fatal(err)
	}

	h.sched.Drain(h.ctx, 10)
	reserve := h.leaseAndComplete(t, "reserve_inventory", json.RawMessage(`{"reservation_id":"r1"}`))
	assertJSONEqual(t, reserve.Payload, `{"input":{"order_id":"order-ctx"},"steps":{}}`)

	h.sched.Drain(h.ctx, 10)
	if err := h.eng.SignalInstance(h.ctx, inst.ID, "demo", "manager_approval", nil); err != nil {
		t.Fatal(err)
	}
	h.sched.Drain(h.ctx, 10)
	capture, err := h.st.LeaseTasks(h.ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "capture_payment", WorkerID: "w1", Now: h.clk.Now(), LeaseFor: time.Minute, Limit: 1})
	if err != nil || len(capture) != 1 {
		t.Fatalf("lease capture_payment: err=%v n=%d", err, len(capture))
	}
	assertJSONEqual(t, capture[0].Payload, `{"input":{"order_id":"order-ctx"},"steps":{"reserve_inventory":{"reservation_id":"r1"}}}`)

	got, err := h.st.GetInstance(h.ctx, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, got.Context, `{"input":{"order_id":"order-ctx"},"steps":{"reserve_inventory":{"reservation_id":"r1"}}}`)
}

func assertJSONEqual(t *testing.T, got json.RawMessage, want string) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("got invalid json %q: %v", got, err)
	}
	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("want invalid json %q: %v", want, err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("json = %s, want %s", got, want)
	}
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
