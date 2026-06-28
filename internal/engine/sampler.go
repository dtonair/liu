package engine

import (
	"context"
	"time"

	"github.com/dtonair/liu/internal/model"
	"github.com/dtonair/liu/internal/store"
)

// MetricsSampler periodically samples instance counts by status into the
// gauges. Counters are incremented inline by the engine; gauges need polling.
type MetricsSampler struct {
	engine   *Engine
	interval time.Duration
}

// NewMetricsSampler returns a sampler ticking every interval.
func NewMetricsSampler(e *Engine, interval time.Duration) *MetricsSampler {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &MetricsSampler{engine: e, interval: interval}
}

// Run blocks, sampling gauges until ctx is cancelled.
func (s *MetricsSampler) Run(ctx context.Context) error {
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

// RunOnce samples once. Best-effort; sampling errors are logged, not fatal.
func (s *MetricsSampler) RunOnce(ctx context.Context) {
	if s.engine.metrics == nil {
		return
	}
	counts := map[model.InstanceStatus]int{
		model.StatusRunnable: 0, model.StatusWaiting: 0,
		model.StatusSucceeded: 0, model.StatusFailed: 0,
	}
	// Empty filter lists across all tenants (bounded by Limit).
	insts, err := s.engine.store.ListInstances(ctx, store.InstanceFilter{Limit: 10000})
	if err != nil {
		s.engine.log.Error("metrics sampler", "error", err)
		return
	}
	for _, i := range insts {
		counts[i.Status]++
	}
	for status, n := range counts {
		s.engine.metrics.SetInstanceCount(string(status), n)
	}
}
