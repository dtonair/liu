package engine

import (
	"context"
	"time"
)

// LeaseSweeper returns timed-out task leases to the queue so a crashed worker's
// in-flight task is retried within its budget (spec FR10). Combined with
// idempotent activities, this gives at-least-once execution.
type LeaseSweeper struct {
	engine   *Engine
	interval time.Duration
}

// NewLeaseSweeper returns a LeaseSweeper ticking every interval.
func NewLeaseSweeper(e *Engine, interval time.Duration) *LeaseSweeper {
	if interval <= 0 {
		interval = time.Second
	}
	return &LeaseSweeper{engine: e, interval: interval}
}

// Run blocks, reclaiming expired leases until ctx is cancelled.
func (s *LeaseSweeper) Run(ctx context.Context) error {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s.RunOnce(ctx)
		}
	}
}

// RunOnce reclaims all currently-expired leases and returns the count.
func (s *LeaseSweeper) RunOnce(ctx context.Context) int {
	n, err := s.engine.store.ExpireLeases(ctx, s.engine.clock.Now())
	if err != nil {
		s.engine.log.Error("lease sweeper", "error", err)
		return 0
	}
	if n > 0 {
		s.engine.log.Info("lease sweeper reclaimed tasks", "count", n)
	}
	return n
}
