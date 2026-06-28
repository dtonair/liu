package engine

import (
	"context"
	"time"
)

// TimerLoop fires due durable timers, advancing the instances that were parked
// on them. After an engine outage it naturally catches up: DueTimers returns
// every timer whose fire_at has passed (spec FR8).
type TimerLoop struct {
	engine   *Engine
	interval time.Duration
	batch    int
}

// NewTimerLoop returns a TimerLoop ticking every interval.
func NewTimerLoop(e *Engine, interval time.Duration, batch int) *TimerLoop {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	if batch <= 0 {
		batch = 100
	}
	return &TimerLoop{engine: e, interval: interval, batch: batch}
}

// Run blocks, firing due timers until ctx is cancelled.
func (l *TimerLoop) Run(ctx context.Context) error {
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			l.RunOnce(ctx)
		}
	}
}

// RunOnce fires all currently-due timers (up to batch) and returns the count.
func (l *TimerLoop) RunOnce(ctx context.Context) int {
	now := l.engine.clock.Now()
	due, err := l.engine.store.DueTimers(ctx, now, l.batch)
	if err != nil {
		l.engine.log.Error("timer loop: due timers", "error", err)
		return 0
	}
	fired := 0
	for _, timer := range due {
		if err := l.engine.OnTimerFired(ctx, timer); err != nil {
			l.engine.log.Error("timer loop: fire", "timer", timer.ID, "error", err)
			continue
		}
		fired++
	}
	return fired
}
