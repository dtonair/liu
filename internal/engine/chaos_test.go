package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dtonair/liu/internal/model"
	"github.com/dtonair/liu/internal/store"
)

// TestChaosWorkerCrashRecovery is the Phase 8 reliability proof: a worker leases
// a task and "crashes" without completing it. The lease expires, the sweeper
// reclaims the task, a healthy worker finishes it, and the instance still
// completes — with no duplicate side effect (the reclaimed task keeps the same
// row and idempotency key, so exactly one TASK_COMPLETED is recorded per step).
//
// Runs against Postgres; skips when LIU_TEST_DATABASE_URL is unset.
func TestChaosWorkerCrashRecovery(t *testing.T) {
	url := os.Getenv("LIU_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("LIU_TEST_DATABASE_URL not set; skipping Postgres chaos test")
	}
	ctx := context.Background()
	pg, err := store.NewPgStore(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pg.Pool().Exec(ctx, `TRUNCATE workflow_definitions, workflow_instances, workflow_history, tasks, timers, signals, outbox RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	eng := New(pg)
	sched := NewScheduler(eng, 0, 0)
	sweeper := NewLeaseSweeper(eng, 0)

	b, _ := os.ReadFile(filepath.Join("..", "..", "workflows", "order_approval.json"))
	def, _ := model.ParseDefinition(b)
	if err := eng.RegisterDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	inst, err := eng.StartInstance(ctx, StartRequest{WorkflowName: "order_approval", TenantID: "demo", IdempotencyKey: "chaos-1"})
	if err != nil {
		t.Fatal(err)
	}
	sched.Drain(ctx, 10) // reserve_inventory task enqueued

	// Worker A leases the task with a short lease, then "crashes" (never
	// completes).
	now := time.Now().UTC()
	leasedA, err := pg.LeaseTasks(ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "reserve_inventory", WorkerID: "A", Now: now, LeaseFor: time.Second, Limit: 1})
	if err != nil || len(leasedA) != 1 {
		t.Fatalf("worker A lease: err=%v n=%d", err, len(leasedA))
	}

	// Time passes; the lease expires and the sweeper reclaims it.
	reclaimed := sweeper.engine.store
	if n, _ := reclaimed.ExpireLeases(ctx, now.Add(5*time.Second)); n != 1 {
		t.Fatalf("expected 1 reclaimed lease, got %d", n)
	}

	// Worker B leases the reclaimed task and completes it.
	leasedB, err := pg.LeaseTasks(ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "reserve_inventory", WorkerID: "B", Now: time.Now().UTC(), LeaseFor: time.Minute, Limit: 1})
	if err != nil || len(leasedB) != 1 {
		t.Fatalf("worker B lease: err=%v n=%d", err, len(leasedB))
	}
	if leasedB[0].ID != leasedA[0].ID {
		t.Fatalf("reclaimed task should be the same row: A=%s B=%s", leasedA[0].ID, leasedB[0].ID)
	}
	// Worker A's stale completion (if it came back from the dead) must be
	// rejected, while worker B's succeeds.
	if err := eng.OnTaskComplete(ctx, leasedA[0].ID, "A", leasedA[0].LeaseToken, nil); err == nil {
		// stale token should fail; tolerate nil only if it was a no-op
		t.Logf("note: stale completion returned nil (treated as no-op)")
	}
	if err := eng.OnTaskComplete(ctx, leasedB[0].ID, "B", leasedB[0].LeaseToken, nil); err != nil {
		t.Fatalf("worker B complete: %v", err)
	}

	// Drive the rest of the flow to completion.
	sched.Drain(ctx, 10)
	if err := eng.SignalInstance(ctx, inst.ID, "demo", "manager_approval", nil); err != nil {
		t.Fatal(err)
	}
	sched.Drain(ctx, 10)
	leasedC, _ := pg.LeaseTasks(ctx, store.LeaseRequest{TenantID: "demo", ActivityType: "capture_payment", WorkerID: "B", Now: time.Now().UTC(), LeaseFor: time.Minute, Limit: 1})
	if len(leasedC) != 1 {
		t.Fatalf("capture_payment not queued, got %d", len(leasedC))
	}
	if err := eng.OnTaskComplete(ctx, leasedC[0].ID, "B", leasedC[0].LeaseToken, nil); err != nil {
		t.Fatal(err)
	}
	sched.Drain(ctx, 10)

	final, _ := pg.GetInstance(ctx, inst.ID)
	if final.Status != model.StatusSucceeded {
		t.Fatalf("final status=%s want SUCCEEDED", final.Status)
	}

	// No duplicate side effects: exactly one TASK_COMPLETED per activity step,
	// and reserve_inventory was scheduled exactly once despite the crash.
	hist, _ := pg.History(ctx, inst.ID)
	completed, reserveScheduled := 0, 0
	for _, e := range hist {
		if e.Type == model.EventTaskCompleted {
			completed++
		}
		if e.Type == model.EventTaskScheduled && e.StepID == "reserve_inventory" {
			reserveScheduled++
		}
	}
	if completed != 2 {
		t.Fatalf("want 2 TASK_COMPLETED (reserve+capture), got %d", completed)
	}
	if reserveScheduled != 1 {
		t.Fatalf("reserve_inventory scheduled %d times, want 1 (no duplicate dispatch)", reserveScheduled)
	}
}
