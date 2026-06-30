package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/dtonair/liu/model"
	"github.com/dtonair/liu/schedule"
	"github.com/dtonair/liu/store"
)

// ScheduleLoop starts workflow instances for due cron schedules. It is intended
// to run only on the elected leader, like the scheduler and timer loops.
type ScheduleLoop struct {
	engine   *Engine
	interval time.Duration
	batch    int
}

// NewScheduleLoop returns a ScheduleLoop ticking every interval.
func NewScheduleLoop(e *Engine, interval time.Duration, batch int) *ScheduleLoop {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if batch <= 0 {
		batch = 100
	}
	return &ScheduleLoop{engine: e, interval: interval, batch: batch}
}

// Run blocks, starting due schedules until ctx is cancelled.
func (l *ScheduleLoop) Run(ctx context.Context) error {
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

// RunOnce starts all currently-due schedules up to the loop batch and returns
// the number of schedules successfully advanced.
func (l *ScheduleLoop) RunOnce(ctx context.Context) int {
	now := l.engine.clock.Now()
	due, err := l.engine.store.DueSchedules(ctx, now, l.batch)
	if err != nil {
		l.engine.log.Error("schedule loop: due schedules", "error", err)
		return 0
	}
	started := 0
	for _, sched := range due {
		if err := l.runSchedule(ctx, sched, now); err != nil {
			l.engine.metrics.ScheduleRunFailed()
			l.engine.log.Error("schedule loop: run", "schedule", sched.ID, "tenant", sched.TenantID, "error", err)
			continue
		}
		l.engine.metrics.ScheduleRunStarted()
		started++
	}
	return started
}

func (l *ScheduleLoop) runSchedule(ctx context.Context, sched *model.Schedule, now time.Time) error {
	runAt := sched.NextRunAt.UTC()
	_, err := l.engine.StartInstance(ctx, StartRequest{
		WorkflowName:   sched.WorkflowName,
		Version:        sched.Version,
		TenantID:       sched.TenantID,
		Input:          sched.Input,
		IdempotencyKey: scheduleIdempotencyKey(sched.ID, runAt),
	})
	if err != nil {
		return err
	}
	nextRunAt, err := schedule.NextAfterInLocation(sched.Cron, sched.Timezone, now)
	if err != nil {
		return err
	}
	return l.engine.store.MarkScheduleRun(ctx, store.ScheduleRun{
		ScheduleID: sched.ID,
		RunAt:      runAt,
		NextRunAt:  nextRunAt,
		UpdatedAt:  now,
	})
}

func scheduleIdempotencyKey(scheduleID string, runAt time.Time) string {
	return fmt.Sprintf("schedule:%s:%s", scheduleID, runAt.UTC().Format(time.RFC3339))
}
