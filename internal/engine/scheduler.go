package engine

import (
	"context"
	"time"
)

// Scheduler repeatedly advances RUNNABLE instances. It is intentionally simple:
// a single active scheduler is assumed (spec assumption), with leader election
// layered on in Phase 8.
type Scheduler struct {
	engine   *Engine
	interval time.Duration
	batch    int
}

// NewScheduler returns a Scheduler that ticks every interval, advancing up to
// batch instances per tick.
func NewScheduler(e *Engine, interval time.Duration, batch int) *Scheduler {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	if batch <= 0 {
		batch = 100
	}
	return &Scheduler{engine: e, interval: interval, batch: batch}
}

// Run blocks, advancing instances until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// tick advances one batch of RUNNABLE instances. It is exported-for-test via
// RunOnce.
func (s *Scheduler) tick(ctx context.Context) {
	insts, err := s.engine.store.RunnableInstances(ctx, s.batch)
	if err != nil {
		s.engine.log.Error("scheduler: list runnable", "error", err)
		return
	}
	for _, inst := range insts {
		if err := s.engine.Advance(ctx, inst.ID); err != nil {
			s.engine.log.Error("scheduler: advance", "instance", inst.ID, "error", err)
		}
	}
}

// RunOnce performs a single scheduling tick. Tests use it to drive the engine
// deterministically without a background goroutine.
func (s *Scheduler) RunOnce(ctx context.Context) { s.tick(ctx) }

// Drain advances RUNNABLE instances repeatedly until none remain or maxRounds
// is reached. Tests use it to run a workflow to a stable (waiting/terminal)
// state.
func (s *Scheduler) Drain(ctx context.Context, maxRounds int) {
	for i := 0; i < maxRounds; i++ {
		insts, err := s.engine.store.RunnableInstances(ctx, s.batch)
		if err != nil || len(insts) == 0 {
			return
		}
		for _, inst := range insts {
			_ = s.engine.Advance(ctx, inst.ID)
		}
	}
}
