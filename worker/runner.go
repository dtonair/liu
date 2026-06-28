package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dtonair/liu/model"
)

// Handler executes the business logic for one activity. The returned output is
// recorded with the task completion. Handlers MUST be idempotent: at-least-once
// delivery means a handler may run more than once for the same logical task.
// Use task.IdempotencyKey to dedupe external side effects.
type Handler func(ctx context.Context, task *model.Task) (json.RawMessage, error)

// NonRetryableError wraps an error a handler considers terminal (e.g. invalid
// input). The runner reports it as non-retryable so the instance fails fast.
type NonRetryableError struct {
	Class string
	Err   error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable marks err as non-retryable with an optional class label.
func NonRetryable(class string, err error) error { return &NonRetryableError{Class: class, Err: err} }

// Runner polls the engine for tasks and dispatches them to registered handlers.
type Runner struct {
	client      *Client
	log         *slog.Logger
	handlers    map[string]Handler
	concurrency int
	leaseSecs   int

	sem chan struct{}
	wg  sync.WaitGroup
}

// RunnerOptions configures a Runner.
type RunnerOptions struct {
	Concurrency  int // max concurrent in-flight tasks (default 8)
	LeaseSeconds int // task lease duration (default 30)
	Logger       *slog.Logger
}

// NewRunner returns a Runner over the given client.
func NewRunner(c *Client, opts RunnerOptions) *Runner {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 8
	}
	if opts.LeaseSeconds <= 0 {
		opts.LeaseSeconds = 30
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Runner{
		client:      c,
		log:         opts.Logger,
		handlers:    map[string]Handler{},
		concurrency: opts.Concurrency,
		leaseSecs:   opts.LeaseSeconds,
		sem:         make(chan struct{}, opts.Concurrency),
	}
}

// Register binds a handler to an activity type.
func (r *Runner) Register(activity string, h Handler) { r.handlers[activity] = h }

// Run starts one poll loop per registered activity type and blocks until ctx is
// cancelled, then drains in-flight tasks.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.handlers) == 0 {
		return fmt.Errorf("worker: no handlers registered")
	}
	var loops sync.WaitGroup
	for activity := range r.handlers {
		loops.Add(1)
		go func(activity string) {
			defer loops.Done()
			r.pollLoop(ctx, activity)
		}(activity)
	}
	loops.Wait() // poll loops exit on ctx cancellation
	r.wg.Wait()  // drain in-flight task handlers
	r.log.Info("worker drained")
	return nil
}

func (r *Runner) pollLoop(ctx context.Context, activity string) {
	for {
		if ctx.Err() != nil {
			return
		}
		task, ok, err := r.client.Poll(ctx, activity, 5, r.leaseSecs)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.log.Error("worker poll", "activity", activity, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if !ok {
			continue // long-poll timed out; loop again
		}
		// Acquire a concurrency slot before dispatching.
		select {
		case r.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		r.wg.Add(1)
		go func(task *model.Task) {
			defer r.wg.Done()
			defer func() { <-r.sem }()
			r.dispatch(ctx, task)
		}(task)
	}
}

func (r *Runner) dispatch(ctx context.Context, task *model.Task) {
	handler := r.handlers[task.ActivityType]
	log := r.log.With("task", task.ID, "instance", task.InstanceID, "activity", task.ActivityType, "attempt", task.Attempt)

	// Heartbeat the lease for the duration of the handler so long tasks are not
	// reclaimed by the sweeper.
	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go r.heartbeat(hbCtx, task, log)

	output, err := r.safeInvoke(ctx, handler, task)
	stopHB()

	if err != nil {
		class, retryable := classify(err)
		log.Warn("task failed", "error", err, "retryable", retryable)
		if ferr := r.client.Fail(ctx, task.ID, task.LeaseToken, err.Error(), class, retryable); ferr != nil {
			log.Error("report failure", "error", ferr)
		}
		return
	}
	if cerr := r.client.Complete(ctx, task.ID, task.LeaseToken, output); cerr != nil {
		log.Error("report completion", "error", cerr)
		return
	}
	log.Info("task completed")
}

// safeInvoke runs the handler, converting panics into errors so one bad task
// cannot crash the worker.
func (r *Runner) safeInvoke(ctx context.Context, h Handler, task *model.Task) (out json.RawMessage, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("handler panic: %v", rec)
		}
	}()
	return h(ctx, task)
}

func (r *Runner) heartbeat(ctx context.Context, task *model.Task, log *slog.Logger) {
	// Renew at one third of the lease so we comfortably beat expiry.
	interval := time.Duration(r.leaseSecs) * time.Second / 3
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.client.Heartbeat(ctx, task.ID, task.LeaseToken, r.leaseSecs); err != nil {
				if ctx.Err() == nil {
					log.Warn("heartbeat", "error", err)
				}
				return
			}
		}
	}
}

func classify(err error) (class string, retryable bool) {
	var nre *NonRetryableError
	if errors.As(err, &nre) {
		return nre.Class, false
	}
	return "", true
}
