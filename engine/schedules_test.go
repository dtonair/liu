package engine

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dtonair/liu/model"
	"github.com/dtonair/liu/store"
	"github.com/dtonair/liu/telemetry"
)

func newScheduleHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t)
	h.clk = NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	h.eng.clock = h.clk
	return h
}

func createTestSchedule(t *testing.T, h *harness, id string, nextRunAt time.Time, enabled bool) *model.Schedule {
	t.Helper()
	now := h.clk.Now()
	sched, err := h.st.CreateSchedule(h.ctx, &model.Schedule{
		ID:           id,
		TenantID:     "demo",
		WorkflowName: "order_approval",
		Version:      1,
		Cron:         "*/5 * * * *",
		Timezone:     "UTC",
		Input:        []byte(`{"source":"schedule"}`),
		Enabled:      enabled,
		NextRunAt:    nextRunAt,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	return sched
}

func instancesForTenant(t *testing.T, h *harness) []*model.Instance {
	t.Helper()
	insts, err := h.st.ListInstances(h.ctx, store.InstanceFilter{TenantID: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	return insts
}

func TestScheduleLoopStartsDueWorkflow(t *testing.T) {
	h := newScheduleHarness(t)
	runAt := h.clk.Now()
	createTestSchedule(t, h, "sched-1", runAt, true)

	loop := NewScheduleLoop(h.eng, 0, 0)
	if n := loop.RunOnce(h.ctx); n != 1 {
		t.Fatalf("started %d schedules, want 1", n)
	}

	insts := instancesForTenant(t, h)
	if len(insts) != 1 {
		t.Fatalf("instances=%d want 1", len(insts))
	}
	if insts[0].IdempotencyKey != scheduleIdempotencyKey("sched-1", runAt) {
		t.Fatalf("idempotency key=%q", insts[0].IdempotencyKey)
	}
	if string(insts[0].Input) != `{"source":"schedule"}` {
		t.Fatalf("input=%s", insts[0].Input)
	}
	sched, err := h.st.GetSchedule(h.ctx, "sched-1")
	if err != nil {
		t.Fatal(err)
	}
	if sched.LastRunAt == nil || !sched.LastRunAt.Equal(runAt) {
		t.Fatalf("last_run_at=%v want %v", sched.LastRunAt, runAt)
	}
	wantNext := runAt.Add(5 * time.Minute)
	if !sched.NextRunAt.Equal(wantNext) {
		t.Fatalf("next_run_at=%v want %v", sched.NextRunAt, wantNext)
	}
}

func TestScheduleLoopSkipsMissedRuns(t *testing.T) {
	h := newScheduleHarness(t)
	runAt := h.clk.Now().Add(-3 * time.Hour)
	createTestSchedule(t, h, "sched-missed", runAt, true)

	loop := NewScheduleLoop(h.eng, 0, 0)
	if n := loop.RunOnce(h.ctx); n != 1 {
		t.Fatalf("started %d schedules, want 1", n)
	}
	if insts := instancesForTenant(t, h); len(insts) != 1 {
		t.Fatalf("instances=%d want 1", len(insts))
	}
	sched, err := h.st.GetSchedule(h.ctx, "sched-missed")
	if err != nil {
		t.Fatal(err)
	}
	if sched.LastRunAt == nil || !sched.LastRunAt.Equal(runAt) {
		t.Fatalf("last_run_at=%v want original due run %v", sched.LastRunAt, runAt)
	}
	if !sched.NextRunAt.After(h.clk.Now()) {
		t.Fatalf("next_run_at=%v should be after now=%v", sched.NextRunAt, h.clk.Now())
	}
}

func TestScheduleLoopIgnoresDisabledSchedules(t *testing.T) {
	h := newScheduleHarness(t)
	createTestSchedule(t, h, "sched-disabled", h.clk.Now(), false)

	loop := NewScheduleLoop(h.eng, 0, 0)
	if n := loop.RunOnce(h.ctx); n != 0 {
		t.Fatalf("started %d schedules, want 0", n)
	}
	if insts := instancesForTenant(t, h); len(insts) != 0 {
		t.Fatalf("instances=%d want 0", len(insts))
	}
}

type failMarkStore struct {
	store.Store
	failed bool
}

func (s *failMarkStore) MarkScheduleRun(ctx context.Context, run store.ScheduleRun) error {
	if !s.failed {
		s.failed = true
		return errors.New("mark failed")
	}
	return s.Store.MarkScheduleRun(ctx, run)
}

func TestScheduleLoopRetriesAfterMarkRunFailureWithoutDuplicateInstance(t *testing.T) {
	base := store.NewMemStore()
	clk := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	st := &failMarkStore{Store: base}
	eng := New(st, WithClock(clk))
	if err := eng.RegisterDefinition(context.Background(), loadOrderApproval(t)); err != nil {
		t.Fatal(err)
	}
	h := &harness{ctx: context.Background(), st: st, eng: eng, sched: NewScheduler(eng, 0, 0), clk: clk}
	runAt := clk.Now()
	createTestSchedule(t, h, "sched-retry", runAt, true)

	loop := NewScheduleLoop(eng, 0, 0)
	if n := loop.RunOnce(h.ctx); n != 0 {
		t.Fatalf("first run advanced %d schedules, want 0 after mark failure", n)
	}
	if insts := instancesForTenant(t, h); len(insts) != 1 {
		t.Fatalf("instances after failed mark=%d want 1", len(insts))
	}

	clk.Advance(2 * time.Minute)
	if n := loop.RunOnce(h.ctx); n != 1 {
		t.Fatalf("second run advanced %d schedules, want 1", n)
	}
	insts := instancesForTenant(t, h)
	if len(insts) != 1 {
		t.Fatalf("idempotent replay created %d instances, want 1", len(insts))
	}
	sched, err := h.st.GetSchedule(h.ctx, "sched-retry")
	if err != nil {
		t.Fatal(err)
	}
	if sched.LastRunAt == nil || !sched.LastRunAt.Equal(runAt) {
		t.Fatalf("last_run_at=%v want %v", sched.LastRunAt, runAt)
	}
}

func TestScheduleLoopMetrics(t *testing.T) {
	st := store.NewMemStore()
	clk := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	metrics := telemetry.NewMetrics()
	eng := New(st, WithClock(clk), WithMetrics(metrics))
	if err := eng.RegisterDefinition(context.Background(), loadOrderApproval(t)); err != nil {
		t.Fatal(err)
	}
	h := &harness{ctx: context.Background(), st: st, eng: eng, sched: NewScheduler(eng, 0, 0), clk: clk}
	createTestSchedule(t, h, "sched-metrics", clk.Now(), true)

	if n := NewScheduleLoop(eng, 0, 0).RunOnce(h.ctx); n != 1 {
		t.Fatalf("started %d schedules, want 1", n)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Result().Body)
	text := string(body)
	if !strings.Contains(text, "liu_schedule_runs_started_total 1") {
		t.Fatalf("missing schedule success metric:\n%s", text)
	}
	if !strings.Contains(text, "liu_schedule_run_failures_total 0") {
		t.Fatalf("missing schedule failure metric:\n%s", text)
	}
}
