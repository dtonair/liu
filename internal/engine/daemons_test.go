package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dtonair/liu/internal/model"
	"github.com/dtonair/liu/internal/store"
)

// retryDef is a one-activity workflow with a deterministic (jitter-free) retry
// policy so backoff timing is exactly assertable.
func retryDef() *model.Definition {
	return &model.Definition{
		Name:    "retry_wf",
		Version: 1,
		Initial: "act",
		Steps: []model.Step{
			{
				ID: "act", Type: model.StepActivity, Activity: "do", Next: "done",
				Retry: &model.RetryPolicy{
					MaxAttempts:        3,
					InitialInterval:    model.Duration(time.Second),
					BackoffCoefficient: 2.0,
					MaxInterval:        model.Duration(time.Minute),
					Jitter:             0, // deterministic
				},
			},
			{ID: "done", Type: model.StepEnd},
		},
	}
}

func newRetryHarness(t *testing.T) *harness {
	t.Helper()
	st := store.NewMemStore()
	clk := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	eng := New(st, WithClock(clk))
	if err := eng.RegisterDefinition(context.Background(), retryDef()); err != nil {
		t.Fatal(err)
	}
	return &harness{ctx: context.Background(), st: st, eng: eng, sched: NewScheduler(eng, 0, 0), clk: clk}
}

func (h *harness) leaseOne(t *testing.T) *model.Task {
	t.Helper()
	leased, err := h.st.LeaseTasks(h.ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "do", WorkerID: "w1", Now: h.clk.Now(), LeaseFor: time.Minute, Limit: 1})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: err=%v n=%d", err, len(leased))
	}
	return leased[0]
}

func TestRetryBackoffSchedule(t *testing.T) {
	h := newRetryHarness(t)
	inst, _ := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "retry_wf", TenantID: "demo", IdempotencyKey: "r1"})
	h.sched.Drain(h.ctx, 5)

	// Attempt 1 fails (retryable) -> requeued at now+1s (initial interval).
	task := h.leaseOne(t)
	start := h.clk.Now()
	if err := h.eng.OnTaskFail(h.ctx, task.ID, "w1", task.LeaseToken, "boom", "", true); err != nil {
		t.Fatal(err)
	}
	got, _ := h.st.GetTask(h.ctx, task.ID)
	if got.Status != model.TaskQueued || got.Attempt != 2 {
		t.Fatalf("after fail#1: status=%s attempt=%d", got.Status, got.Attempt)
	}
	if want := start.Add(time.Second); !got.VisibleAt.Equal(want) {
		t.Fatalf("backoff#1 visible_at=%v want %v", got.VisibleAt, want)
	}
	// Not visible yet -> cannot lease before backoff elapses.
	early, _ := h.st.LeaseTasks(h.ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "do", WorkerID: "w1", Now: h.clk.Now(), LeaseFor: time.Minute, Limit: 1})
	if len(early) != 0 {
		t.Fatalf("task leased before backoff elapsed")
	}

	// Advance past backoff, attempt 2 fails -> requeued at now+2s (coeff^1).
	h.clk.Advance(time.Second)
	task2 := h.leaseOne(t)
	start2 := h.clk.Now()
	if err := h.eng.OnTaskFail(h.ctx, task2.ID, "w1", task2.LeaseToken, "boom", "", true); err != nil {
		t.Fatal(err)
	}
	got2, _ := h.st.GetTask(h.ctx, task2.ID)
	if got2.Attempt != 3 {
		t.Fatalf("after fail#2 attempt=%d want 3", got2.Attempt)
	}
	if want := start2.Add(2 * time.Second); !got2.VisibleAt.Equal(want) {
		t.Fatalf("backoff#2 visible_at=%v want %v", got2.VisibleAt, want)
	}

	// Attempt 3 fails -> budget exhausted -> instance FAILED.
	h.clk.Advance(2 * time.Second)
	task3 := h.leaseOne(t)
	if err := h.eng.OnTaskFail(h.ctx, task3.ID, "w1", task3.LeaseToken, "fatal", "", true); err != nil {
		t.Fatal(err)
	}
	if got := h.status(t, inst.ID); got != model.StatusFailed {
		t.Fatalf("status=%s want FAILED after exhausting retries", got)
	}
}

func TestNonRetryableFailsImmediately(t *testing.T) {
	h := newRetryHarness(t)
	inst, _ := h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "retry_wf", TenantID: "demo", IdempotencyKey: "r2"})
	h.sched.Drain(h.ctx, 5)
	task := h.leaseOne(t)
	// retryable=false -> no retry regardless of remaining budget.
	if err := h.eng.OnTaskFail(h.ctx, task.ID, "w1", task.LeaseToken, "bad input", "", false); err != nil {
		t.Fatal(err)
	}
	if got := h.status(t, inst.ID); got != model.StatusFailed {
		t.Fatalf("status=%s want FAILED on non-retryable error", got)
	}
}

func TestTimerLoopFires(t *testing.T) {
	st := store.NewMemStore()
	clk := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	eng := New(st, WithClock(clk))
	if err := eng.RegisterDefinition(context.Background(), loadOrderApproval(t)); err != nil {
		t.Fatal(err)
	}
	h := &harness{ctx: context.Background(), st: st, eng: eng, sched: NewScheduler(eng, 0, 0), clk: clk}
	inst, _ := eng.StartInstance(h.ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "tl"})
	h.sched.Drain(h.ctx, 5)
	h.leaseAndComplete(t, "reserve_inventory")
	h.sched.Drain(h.ctx, 5) // parks on manager_approval w/ 24h timer

	loop := NewTimerLoop(eng, 0, 0)
	// Before timeout: nothing fires.
	if n := loop.RunOnce(h.ctx); n != 0 {
		t.Fatalf("fired %d timers before due", n)
	}
	// Simulate outage: clock jumps well past the deadline; catch-up fires it.
	clk.Advance(48 * time.Hour)
	if n := loop.RunOnce(h.ctx); n != 1 {
		t.Fatalf("fired %d timers, want 1", n)
	}
	h.sched.Drain(h.ctx, 5)
	if got := h.status(t, inst.ID); got != model.StatusWaiting {
		t.Fatalf("status=%s want WAITING (cancel activity scheduled)", got)
	}
	// Confirm we routed down the timeout (cancel) branch.
	leased, _ := st.LeaseTasks(h.ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "release_inventory", WorkerID: "w1", Now: clk.Now(), LeaseFor: time.Minute, Limit: 1})
	if len(leased) != 1 {
		t.Fatalf("expected release_inventory task on timeout branch, got %d", len(leased))
	}
}

func TestLeaseSweeperReclaims(t *testing.T) {
	h := newRetryHarness(t)
	_, _ = h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "retry_wf", TenantID: "demo", IdempotencyKey: "sw"})
	h.sched.Drain(h.ctx, 5)
	task := h.leaseOne(t) // leased for 1 minute
	sweeper := NewLeaseSweeper(h.eng, 0)

	if n := sweeper.RunOnce(h.ctx); n != 0 {
		t.Fatalf("reclaimed %d before expiry", n)
	}
	h.clk.Advance(2 * time.Minute)
	if n := sweeper.RunOnce(h.ctx); n != 1 {
		t.Fatalf("reclaimed %d, want 1", n)
	}
	got, _ := h.st.GetTask(h.ctx, task.ID)
	if got.Status != model.TaskQueued {
		t.Fatalf("status=%s want QUEUED after sweep", got.Status)
	}
}

// failOnceSink fails the first publish to prove retry, then succeeds.
type failOnceSink struct {
	mu        sync.Mutex
	failed    bool
	published []int64
}

func (s *failOnceSink) Publish(_ context.Context, r *model.OutboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.failed {
		s.failed = true
		return errors.New("transient")
	}
	s.published = append(s.published, r.ID)
	return nil
}

func TestOutboxPublisherRetriesThenMarksSent(t *testing.T) {
	h := newRetryHarness(t)
	_, _ = h.eng.StartInstance(h.ctx, StartRequest{WorkflowName: "retry_wf", TenantID: "demo", IdempotencyKey: "ob"})
	// StartInstance enqueued a WORKFLOW_STARTED outbox record.
	sink := &failOnceSink{}
	pub := NewOutboxPublisher(h.eng, sink, 0, 10)

	// First tick: sink fails -> record stays unsent.
	if n := pub.RunOnce(h.ctx); n != 0 {
		t.Fatalf("published %d on failing tick, want 0", n)
	}
	if unsent, _ := h.st.UnsentOutbox(h.ctx, 10); len(unsent) != 1 {
		t.Fatalf("want 1 still-unsent, got %d", len(unsent))
	}
	// Second tick: succeeds, marked sent, not redelivered.
	if n := pub.RunOnce(h.ctx); n != 1 {
		t.Fatalf("published %d, want 1", n)
	}
	if n := pub.RunOnce(h.ctx); n != 0 {
		t.Fatalf("redelivered already-sent record: %d", n)
	}
	if len(sink.published) != 1 {
		t.Fatalf("sink saw %d publishes, want 1", len(sink.published))
	}
}
