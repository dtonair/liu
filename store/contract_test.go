package store

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dtonair/liu/model"
)

// RunStoreContract exercises the full Store contract. Both the in-memory and
// Postgres implementations must pass it unchanged, which is what lets the engine
// treat them as interchangeable. newStore must return a fresh, empty store.
func RunStoreContract(t *testing.T, newStore func() Store) {
	t.Helper()

	t.Run("definitions", func(t *testing.T) { testDefinitions(t, newStore()) })
	t.Run("instances", func(t *testing.T) { testInstances(t, newStore()) })
	t.Run("instance_cas", func(t *testing.T) { testInstanceCAS(t, newStore()) })
	t.Run("history_seq", func(t *testing.T) { testHistorySeq(t, newStore()) })
	t.Run("task_lease", func(t *testing.T) { testTaskLease(t, newStore()) })
	t.Run("task_lease_concurrent", func(t *testing.T) { testTaskLeaseConcurrent(t, newStore()) })
	t.Run("task_complete_fail", func(t *testing.T) { testTaskCompleteFail(t, newStore()) })
	t.Run("lease_expiry", func(t *testing.T) { testLeaseExpiry(t, newStore()) })
	t.Run("timers", func(t *testing.T) { testTimers(t, newStore()) })
	t.Run("signals", func(t *testing.T) { testSignals(t, newStore()) })
	t.Run("outbox", func(t *testing.T) { testOutbox(t, newStore()) })
	t.Run("tx_rollback", func(t *testing.T) { testTxRollback(t, newStore()) })
}

func sampleDef(version int) *model.Definition {
	return &model.Definition{
		Name:    "wf",
		Version: version,
		Initial: "a",
		Steps: []model.Step{
			{ID: "a", Type: model.StepActivity, Activity: "do", Next: "end"},
			{ID: "end", Type: model.StepEnd},
		},
	}
}

func newInstance(tenant, idem string) *model.Instance {
	now := time.Now().UTC()
	return &model.Instance{
		ID:             NewID(),
		WorkflowName:   "wf",
		Version:        1,
		CurrentStep:    "a",
		Status:         model.StatusRunnable,
		TenantID:       tenant,
		Input:          json.RawMessage(`{"order_id":"o1"}`),
		Context:        json.RawMessage(`{"input":{"order_id":"o1"},"steps":{}}`),
		IdempotencyKey: idem,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func testDefinitions(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	d := sampleDef(1)
	sum, _ := d.Checksum()
	if err := s.PutDefinition(ctx, d, sum); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Idempotent re-put with identical content.
	if err := s.PutDefinition(ctx, d, sum); err != nil {
		t.Fatalf("re-put identical: %v", err)
	}
	// Conflicting checksum rejected.
	if err := s.PutDefinition(ctx, d, "different-sum"); !errors.Is(err, ErrChecksumConflict) {
		t.Fatalf("want ErrChecksumConflict, got %v", err)
	}
	got, err := s.GetDefinition(ctx, "wf", 1)
	if err != nil || got.Initial != "a" {
		t.Fatalf("get: %v %+v", err, got)
	}
	if _, err := s.GetDefinition(ctx, "wf", 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	// Latest tracks the max version.
	d2 := sampleDef(2)
	sum2, _ := d2.Checksum()
	if err := s.PutDefinition(ctx, d2, sum2); err != nil {
		t.Fatal(err)
	}
	latest, err := s.GetLatestDefinition(ctx, "wf")
	if err != nil || latest.Version != 2 {
		t.Fatalf("latest: %v v=%d", err, latest.Version)
	}
}

func testInstances(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst := newInstance("t1", "order-42")
	got, created, err := s.CreateInstance(ctx, inst)
	if err != nil || !created {
		t.Fatalf("create: %v created=%v", err, created)
	}
	// Idempotent start: same key returns the existing instance, not a new one.
	dup := newInstance("t1", "order-42")
	got2, created2, err := s.CreateInstance(ctx, dup)
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatal("expected idempotent replay, got created=true")
	}
	if got2.ID != got.ID {
		t.Fatalf("idempotency returned different instance: %s vs %s", got2.ID, got.ID)
	}
	if string(got2.Context) != string(got.Context) {
		t.Fatalf("idempotency changed context: %s vs %s", got2.Context, got.Context)
	}
	// Same key, different tenant is a distinct instance.
	other := newInstance("t2", "order-42")
	_, created3, err := s.CreateInstance(ctx, other)
	if err != nil || !created3 {
		t.Fatalf("cross-tenant create: %v created=%v", err, created3)
	}

	list, err := s.ListInstances(ctx, InstanceFilter{TenantID: "t1"})
	if err != nil || len(list) != 1 {
		t.Fatalf("list t1: %v n=%d", err, len(list))
	}
	runnable, err := s.RunnableInstances(ctx, 10)
	if err != nil || len(runnable) != 2 {
		t.Fatalf("runnable: %v n=%d", err, len(runnable))
	}
}

func testInstanceCAS(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst := newInstance("t1", "")
	created, _, _ := s.CreateInstance(ctx, inst)

	// First update at row_version 0 succeeds and bumps to 1.
	err := s.Tx(ctx, func(tx Tx) error {
		cur, err := tx.GetInstanceForUpdate(ctx, created.ID)
		if err != nil {
			return err
		}
		cur.Status = model.StatusWaiting
		return tx.UpdateInstance(ctx, cur)
	})
	if err != nil {
		t.Fatalf("first update: %v", err)
	}

	// A stale update at the old row_version must conflict.
	stale := *created // row_version 0
	stale.Status = model.StatusFailed
	err = s.Tx(ctx, func(tx Tx) error { return tx.UpdateInstance(ctx, &stale) })
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}

	after, _ := s.GetInstance(ctx, created.ID)
	if after.Status != model.StatusWaiting || after.RowVersion != 1 {
		t.Fatalf("state after CAS: status=%s rv=%d", after.Status, after.RowVersion)
	}
}

func testHistorySeq(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst := newInstance("t1", "")
	created, _, _ := s.CreateInstance(ctx, inst)

	for i := 0; i < 3; i++ {
		err := s.Tx(ctx, func(tx Tx) error {
			_, err := tx.AppendEvent(ctx, &model.Event{InstanceID: created.ID, Type: model.EventStepEntered, StepID: "a"})
			return err
		})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	hist, err := s.History(ctx, created.ID)
	if err != nil || len(hist) != 3 {
		t.Fatalf("history: %v n=%d", err, len(hist))
	}
	for i, e := range hist {
		if e.Seq != int64(i+1) {
			t.Fatalf("seq[%d] = %d, want %d", i, e.Seq, i+1)
		}
	}
}

func enqueueTask(t *testing.T, s Store, instID, idem string, visibleAt time.Time) string {
	t.Helper()
	id := NewID()
	err := s.Tx(context.Background(), func(tx Tx) error {
		return tx.EnqueueTask(context.Background(), &model.Task{
			ID:             id,
			InstanceID:     instID,
			StepID:         "a",
			TenantID:       "t1",
			ActivityType:   "do",
			Status:         model.TaskQueued,
			IdempotencyKey: idem,
			Attempt:        1,
			MaxAttempts:    3,
			VisibleAt:      visibleAt,
			CreatedAt:      time.Now().UTC(),
		})
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return id
}

func testTaskLease(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	now := time.Now().UTC()

	// A task visible in the future is not leased.
	enqueueTask(t, s, inst.ID, "k-future", now.Add(time.Hour))
	leased, err := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w1", Now: now, LeaseFor: 30 * time.Second, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 0 {
		t.Fatalf("future task should not lease, got %d", len(leased))
	}

	// Dedupe: enqueuing the same idempotency key twice yields one task.
	enqueueTask(t, s, inst.ID, "k1", now.Add(-time.Second))
	enqueueTask(t, s, inst.ID, "k1", now.Add(-time.Second))
	leased, err = s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w1", Now: now, LeaseFor: 30 * time.Second, Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 {
		t.Fatalf("want 1 leased task (deduped), got %d", len(leased))
	}
	if leased[0].LeaseToken == "" || leased[0].LeasedBy != "w1" {
		t.Fatalf("lease not stamped: %+v", leased[0])
	}
	// Already leased: a second poll returns nothing.
	again, _ := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w2", Now: now, LeaseFor: 30 * time.Second, Limit: 5})
	if len(again) != 0 {
		t.Fatalf("leased task re-leased: %d", len(again))
	}
}

func testTaskLeaseConcurrent(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	now := time.Now().UTC()
	const n = 50
	for i := 0; i < n; i++ {
		enqueueTask(t, s, inst.ID, NewID(), now.Add(-time.Second))
	}

	var mu sync.Mutex
	seen := map[string]bool{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				leased, err := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w", Now: time.Now().UTC(), LeaseFor: time.Minute, Limit: 3})
				if err != nil || len(leased) == 0 {
					return
				}
				mu.Lock()
				for _, tk := range leased {
					if seen[tk.ID] {
						t.Errorf("task %s leased twice", tk.ID)
					}
					seen[tk.ID] = true
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("leased %d distinct tasks, want %d", len(seen), n)
	}
}

func testTaskCompleteFail(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	now := time.Now().UTC()
	taskID := enqueueTask(t, s, inst.ID, "k1", now.Add(-time.Second))
	leased, _ := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w1", Now: now, LeaseFor: time.Minute, Limit: 1})
	token := leased[0].LeaseToken

	// Wrong token rejected.
	err := s.Tx(ctx, func(tx Tx) error { return tx.CompleteTask(ctx, taskID, "wrong", nil) })
	if !errors.Is(err, ErrLeaseInvalid) {
		t.Fatalf("want ErrLeaseInvalid, got %v", err)
	}
	// Correct token completes.
	if err := s.Tx(ctx, func(tx Tx) error { return tx.CompleteTask(ctx, taskID, token, nil) }); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// Duplicate completion is a no-op.
	if err := s.Tx(ctx, func(tx Tx) error { return tx.CompleteTask(ctx, taskID, token, nil) }); err != nil {
		t.Fatalf("dup complete: %v", err)
	}
	got, _ := s.GetTask(ctx, taskID)
	if got.Status != model.TaskDone {
		t.Fatalf("status = %s, want DONE", got.Status)
	}

	// Requeue path on a fresh task.
	taskID2 := enqueueTask(t, s, inst.ID, "k2", now.Add(-time.Second))
	leased2, _ := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w1", Now: now, LeaseFor: time.Minute, Limit: 1})
	tok2 := leased2[0].LeaseToken
	vis := now.Add(5 * time.Second)
	if err := s.Tx(ctx, func(tx Tx) error { return tx.RequeueTask(ctx, taskID2, tok2, 2, vis) }); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	rq, _ := s.GetTask(ctx, taskID2)
	if rq.Status != model.TaskQueued || rq.Attempt != 2 {
		t.Fatalf("requeued task: status=%s attempt=%d", rq.Status, rq.Attempt)
	}
}

func testLeaseExpiry(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	now := time.Now().UTC()
	taskID := enqueueTask(t, s, inst.ID, "k1", now.Add(-time.Second))
	leased, _ := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w1", Now: now, LeaseFor: time.Second, Limit: 1})
	token := leased[0].LeaseToken

	// Heartbeat extends the lease well into the future.
	if err := s.HeartbeatTask(ctx, taskID, "w1", token, now.Add(time.Hour)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	n, _ := s.ExpireLeases(ctx, now.Add(2*time.Second))
	if n != 0 {
		t.Fatalf("heartbeated lease should not expire, reclaimed %d", n)
	}
	// Past the extended expiry it is reclaimed.
	n, _ = s.ExpireLeases(ctx, now.Add(2*time.Hour))
	if n != 1 {
		t.Fatalf("want 1 reclaimed, got %d", n)
	}
	got, _ := s.GetTask(ctx, taskID)
	if got.Status != model.TaskQueued {
		t.Fatalf("expired task status = %s, want QUEUED", got.Status)
	}
}

func testTimers(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	now := time.Now().UTC()
	id := NewID()
	err := s.Tx(ctx, func(tx Tx) error {
		return tx.CreateTimer(ctx, &model.Timer{ID: id, InstanceID: inst.ID, StepID: "a", TenantID: "t1", Kind: model.TimerSleep, FireAt: now.Add(time.Hour), CreatedAt: now})
	})
	if err != nil {
		t.Fatal(err)
	}
	// Not yet due.
	due, _ := s.DueTimers(ctx, now, 10)
	if len(due) != 0 {
		t.Fatalf("timer should not be due, got %d", len(due))
	}
	// Due after fire_at (catch-up semantics).
	due, _ = s.DueTimers(ctx, now.Add(2*time.Hour), 10)
	if len(due) != 1 {
		t.Fatalf("want 1 due timer, got %d", len(due))
	}
	if err := s.Tx(ctx, func(tx Tx) error { return tx.MarkTimerFired(ctx, id) }); err != nil {
		t.Fatal(err)
	}
	due, _ = s.DueTimers(ctx, now.Add(2*time.Hour), 10)
	if len(due) != 0 {
		t.Fatalf("fired timer still due: %d", len(due))
	}
}

func testSignals(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))

	// No pending signal.
	err := s.Tx(ctx, func(tx Tx) error {
		_, found, err := tx.ConsumeSignal(ctx, inst.ID, "approve")
		if err != nil {
			return err
		}
		if found {
			t.Error("found signal that was never sent")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	mk := func(name string) {
		if e := s.AppendSignal(ctx, &model.Signal{ID: NewID(), InstanceID: inst.ID, TenantID: "t1", Name: name, CreatedAt: time.Now().UTC()}); e != nil {
			t.Fatal(e)
		}
		time.Sleep(time.Millisecond)
	}
	mk("approve")
	mk("approve")

	consumed := 0
	for i := 0; i < 3; i++ {
		_ = s.Tx(ctx, func(tx Tx) error {
			_, found, err := tx.ConsumeSignal(ctx, inst.ID, "approve")
			if err != nil {
				return err
			}
			if found {
				consumed++
			}
			return nil
		})
	}
	if consumed != 2 {
		t.Fatalf("consumed %d signals, want 2", consumed)
	}
}

func testOutbox(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	err := s.Tx(ctx, func(tx Tx) error {
		return tx.EnqueueOutbox(ctx, &model.OutboxRecord{InstanceID: inst.ID, TenantID: "t1", EventType: "started", CreatedAt: time.Now().UTC()})
	})
	if err != nil {
		t.Fatal(err)
	}
	unsent, _ := s.UnsentOutbox(ctx, 10)
	if len(unsent) != 1 {
		t.Fatalf("want 1 unsent, got %d", len(unsent))
	}
	if err := s.MarkOutboxSent(ctx, unsent[0].ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	unsent, _ = s.UnsentOutbox(ctx, 10)
	if len(unsent) != 0 {
		t.Fatalf("want 0 unsent after mark, got %d", len(unsent))
	}
}

func testTxRollback(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Close()
	inst, _, _ := s.CreateInstance(ctx, newInstance("t1", ""))
	sentinel := errors.New("boom")

	err := s.Tx(ctx, func(tx Tx) error {
		if _, e := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventStepEntered}); e != nil {
			return e
		}
		if e := tx.EnqueueTask(ctx, &model.Task{ID: NewID(), InstanceID: inst.ID, StepID: "a", TenantID: "t1", ActivityType: "do", Status: model.TaskQueued, IdempotencyKey: "roll", VisibleAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}); e != nil {
			return e
		}
		return sentinel // force rollback
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("want sentinel error, got %v", err)
	}
	// Neither the event nor the task should have survived.
	hist, _ := s.History(ctx, inst.ID)
	if len(hist) != 0 {
		t.Fatalf("rollback left %d events", len(hist))
	}
	leased, _ := s.LeaseTasks(ctx, LeaseRequest{TenantID: "t1", ActivityType: "do", WorkerID: "w1", Now: time.Now().UTC(), LeaseFor: time.Minute, Limit: 5})
	if len(leased) != 0 {
		t.Fatalf("rollback left %d tasks", len(leased))
	}
}
