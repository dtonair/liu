// Package engine implements the durable workflow kernel: the state-machine
// transitions over a Store (start, advance, task completion/failure, signals,
// timer fires) and the background loops that drive them. All multi-write
// transitions run inside Store.Tx so they uphold the advance-under-lock and
// commit-before-side-effect invariants.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dtonair/liu/internal/model"
	"github.com/dtonair/liu/internal/store"
)

// Engine drives workflow instances over a Store.
type Engine struct {
	store store.Store
	clock Clock
	log   *slog.Logger

	mu       sync.RWMutex
	defCache map[string]*model.Definition // name|version -> def
}

// Option configures an Engine.
type Option func(*Engine)

// WithClock overrides the engine clock (used by tests).
func WithClock(c Clock) Option { return func(e *Engine) { e.clock = c } }

// WithLogger sets the engine logger.
func WithLogger(l *slog.Logger) Option { return func(e *Engine) { e.log = l } }

// New constructs an Engine over the given Store.
func New(s store.Store, opts ...Option) *Engine {
	e := &Engine{
		store:    s,
		clock:    SystemClock{},
		log:      slog.Default(),
		defCache: map[string]*model.Definition{},
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func cacheKey(name string, version int) string { return fmt.Sprintf("%s|%d", name, version) }

// definition resolves a definition by name+version, caching the immutable
// result.
func (e *Engine) definition(ctx context.Context, name string, version int) (*model.Definition, error) {
	k := cacheKey(name, version)
	e.mu.RLock()
	d, ok := e.defCache[k]
	e.mu.RUnlock()
	if ok {
		return d, nil
	}
	d, err := e.store.GetDefinition(ctx, name, version)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.defCache[k] = d
	e.mu.Unlock()
	return d, nil
}

// RegisterDefinition validates and persists a workflow definition (spec FR1).
func (e *Engine) RegisterDefinition(ctx context.Context, def *model.Definition) error {
	if err := def.Validate(); err != nil {
		return err
	}
	sum, err := def.Checksum()
	if err != nil {
		return err
	}
	return e.store.PutDefinition(ctx, def, sum)
}

// StartRequest parameterises StartInstance.
type StartRequest struct {
	WorkflowName   string
	Version        int // 0 means latest
	TenantID       string
	Input          json.RawMessage
	IdempotencyKey string
}

// StartInstance creates (or idempotently returns) a workflow instance pinned to
// a definition version, and records the WORKFLOW_STARTED event (spec FR3).
func (e *Engine) StartInstance(ctx context.Context, req StartRequest) (*model.Instance, error) {
	var def *model.Definition
	var err error
	if req.Version == 0 {
		def, err = e.store.GetLatestDefinition(ctx, req.WorkflowName)
	} else {
		def, err = e.definition(ctx, req.WorkflowName, req.Version)
	}
	if err != nil {
		return nil, err
	}

	now := e.clock.Now()
	inst := &model.Instance{
		ID:             store.NewID(),
		WorkflowName:   def.Name,
		Version:        def.Version,
		CurrentStep:    def.Initial,
		Status:         model.StatusRunnable,
		TenantID:       req.TenantID,
		Input:          req.Input,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	created, isNew, err := e.store.CreateInstance(ctx, inst)
	if err != nil {
		return nil, err
	}
	if !isNew {
		return created, nil // idempotent replay
	}

	// Record the start event + outbox notification atomically.
	err = e.store.Tx(ctx, func(tx store.Tx) error {
		if _, err := tx.AppendEvent(ctx, &model.Event{
			InstanceID: created.ID,
			Type:       model.EventWorkflowStarted,
			StepID:     created.CurrentStep,
			Payload:    req.Input,
			CreatedAt:  now,
		}); err != nil {
			return err
		}
		return tx.EnqueueOutbox(ctx, &model.OutboxRecord{
			InstanceID: created.ID,
			TenantID:   created.TenantID,
			EventType:  model.EventWorkflowStarted,
			CreatedAt:  now,
		})
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// Advance moves a RUNNABLE instance through entering its current step. It is a
// no-op for non-RUNNABLE instances, so repeated invocation is safe (spec FR4).
//
// The definition is resolved before the transaction opens: definitions are
// immutable, and resolving inside the Tx would re-enter the store lock (the
// in-memory Store serializes a whole Tx under one mutex).
func (e *Engine) Advance(ctx context.Context, instanceID string) error {
	def, err := e.defForInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	return e.store.Tx(ctx, func(tx store.Tx) error {
		inst, err := tx.GetInstanceForUpdate(ctx, instanceID)
		if err != nil {
			return err
		}
		if inst.Status != model.StatusRunnable {
			return nil // already handled by a concurrent advance
		}
		step, ok := def.StepByID(inst.CurrentStep)
		if !ok {
			return fmt.Errorf("instance %s: current step %q not in definition", inst.ID, inst.CurrentStep)
		}
		return e.enterStep(ctx, tx, inst, step)
	})
}

// defForInstance loads an instance (committed read) and resolves its pinned
// definition. Used to resolve definitions before opening a transaction.
func (e *Engine) defForInstance(ctx context.Context, instanceID string) (*model.Definition, error) {
	inst, err := e.store.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	return e.definition(ctx, inst.WorkflowName, inst.Version)
}

// enterStep applies the entry transition for a step. Caller holds the instance
// lock and has verified the instance is RUNNABLE.
func (e *Engine) enterStep(ctx context.Context, tx store.Tx, inst *model.Instance, step model.Step) error {
	now := e.clock.Now()
	switch step.Type {
	case model.StepActivity:
		task := &model.Task{
			ID:             store.NewID(),
			InstanceID:     inst.ID,
			StepID:         step.ID,
			TenantID:       inst.TenantID,
			ActivityType:   step.Activity,
			Status:         model.TaskQueued,
			Payload:        inst.Input,
			IdempotencyKey: taskIdempotencyKey(inst, step),
			Attempt:        1,
			MaxAttempts:    step.EffectiveRetry().MaxAttempts,
			VisibleAt:      now,
			CreatedAt:      now,
		}
		if err := tx.EnqueueTask(ctx, task); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTaskScheduled, StepID: step.ID, CreatedAt: now}); err != nil {
			return err
		}
		inst.Status = model.StatusWaiting

	case model.StepWaitSignal:
		// A signal may have arrived before we parked (early signal): consume it
		// and advance immediately instead of waiting (spec FR9 edge case).
		if sig, found, err := tx.ConsumeSignal(ctx, inst.ID, step.Signal); err != nil {
			return err
		} else if found {
			return e.applySignal(ctx, tx, inst, step, sig, now)
		}
		if step.TimeoutAfter > 0 {
			timer := &model.Timer{
				ID:         store.NewID(),
				InstanceID: inst.ID,
				StepID:     step.ID,
				TenantID:   inst.TenantID,
				Kind:       model.TimerSignalTimeout,
				FireAt:     now.Add(step.TimeoutAfter.Std()),
				CreatedAt:  now,
			}
			if err := tx.CreateTimer(ctx, timer); err != nil {
				return err
			}
			if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTimerCreated, StepID: step.ID, CreatedAt: now}); err != nil {
				return err
			}
		}
		inst.Status = model.StatusWaiting

	case model.StepSleepUntil:
		timer := &model.Timer{
			ID:         store.NewID(),
			InstanceID: inst.ID,
			StepID:     step.ID,
			TenantID:   inst.TenantID,
			Kind:       model.TimerSleep,
			FireAt:     now.Add(step.SleepFor.Std()),
			CreatedAt:  now,
		}
		if err := tx.CreateTimer(ctx, timer); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTimerCreated, StepID: step.ID, CreatedAt: now}); err != nil {
			return err
		}
		inst.Status = model.StatusWaiting

	case model.StepEnd:
		inst.Status = model.StatusSucceeded
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventWorkflowSucceeded, StepID: step.ID, CreatedAt: now}); err != nil {
			return err
		}
		if err := tx.EnqueueOutbox(ctx, &model.OutboxRecord{InstanceID: inst.ID, TenantID: inst.TenantID, EventType: model.EventWorkflowSucceeded, CreatedAt: now}); err != nil {
			return err
		}

	default:
		return fmt.Errorf("instance %s: unsupported step type %q", inst.ID, step.Type)
	}

	inst.UpdatedAt = now
	return tx.UpdateInstance(ctx, inst)
}

// OnTaskComplete records a worker's successful task result and advances the
// instance to the step's successor (spec FR6).
func (e *Engine) OnTaskComplete(ctx context.Context, taskID, workerID, leaseToken string, output json.RawMessage) error {
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	def, err := e.defForInstance(ctx, task.InstanceID)
	if err != nil {
		return err
	}
	return e.store.Tx(ctx, func(tx store.Tx) error {
		if err := tx.CompleteTask(ctx, taskID, leaseToken, output); err != nil {
			return err
		}
		inst, err := tx.GetInstanceForUpdate(ctx, task.InstanceID)
		if err != nil {
			return err
		}
		// Only advance if the instance is still parked on this exact step;
		// otherwise this is a duplicate/stale completion (record-then-apply).
		if inst.Status != model.StatusWaiting || inst.CurrentStep != task.StepID {
			return nil
		}
		step, _ := def.StepByID(task.StepID)
		now := e.clock.Now()
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTaskCompleted, StepID: task.StepID, Payload: output, CreatedAt: now}); err != nil {
			return err
		}
		inst.CurrentStep = step.Next
		inst.Status = model.StatusRunnable
		inst.UpdatedAt = now
		return tx.UpdateInstance(ctx, inst)
	})
}

// OnTaskFail records a worker's task failure, applying the step's retry policy:
// a retryable failure with budget remaining re-queues the task with backoff;
// otherwise the instance fails (spec FR7).
func (e *Engine) OnTaskFail(ctx context.Context, taskID, workerID, leaseToken, errMsg, errClass string, retryable bool) error {
	task, err := e.store.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	def, err := e.defForInstance(ctx, task.InstanceID)
	if err != nil {
		return err
	}
	return e.store.Tx(ctx, func(tx store.Tx) error {
		inst, err := tx.GetInstanceForUpdate(ctx, task.InstanceID)
		if err != nil {
			return err
		}
		if inst.Status != model.StatusWaiting || inst.CurrentStep != task.StepID {
			// Stale/duplicate: just settle the task without touching state.
			return tx.FailTask(ctx, taskID, leaseToken)
		}
		step, _ := def.StepByID(task.StepID)
		now := e.clock.Now()
		dec := decideRetry(step.EffectiveRetry(), task.Attempt, retryable, errClass, now)

		if dec.retry {
			if err := tx.RequeueTask(ctx, taskID, leaseToken, task.Attempt+1, dec.nextVisit); err != nil {
				return err
			}
			payload, _ := json.Marshal(map[string]any{"error": errMsg, "attempt": task.Attempt, "next_visible_at": dec.nextVisit})
			_, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTaskRetryScheduled, StepID: task.StepID, Payload: payload, CreatedAt: now})
			return err
		}

		// Terminal failure.
		if err := tx.FailTask(ctx, taskID, leaseToken); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{"error": errMsg, "attempt": task.Attempt})
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTaskFailed, StepID: task.StepID, Payload: payload, CreatedAt: now}); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventWorkflowFailed, StepID: task.StepID, CreatedAt: now}); err != nil {
			return err
		}
		inst.Status = model.StatusFailed
		inst.Error = errMsg
		inst.UpdatedAt = now
		if err := tx.EnqueueOutbox(ctx, &model.OutboxRecord{InstanceID: inst.ID, TenantID: inst.TenantID, EventType: model.EventWorkflowFailed, CreatedAt: now}); err != nil {
			return err
		}
		return tx.UpdateInstance(ctx, inst)
	})
}

// SignalInstance records an external signal in the inbox (spec FR9). The
// transition is applied later by ApplySignals / the scheduler, keeping ingress
// decoupled from state mutation.
func (e *Engine) SignalInstance(ctx context.Context, instanceID, tenantID, name string, payload json.RawMessage) error {
	now := e.clock.Now()
	if err := e.store.AppendSignal(ctx, &model.Signal{
		ID:         store.NewID(),
		InstanceID: instanceID,
		TenantID:   tenantID,
		Name:       name,
		Payload:    payload,
		CreatedAt:  now,
	}); err != nil {
		return err
	}
	// Best-effort immediate application; the scheduler also retries this.
	return e.ApplyPendingSignal(ctx, instanceID, name)
}

// ApplyPendingSignal consumes a pending signal for an instance parked on a
// matching wait_signal step and advances it.
func (e *Engine) ApplyPendingSignal(ctx context.Context, instanceID, name string) error {
	def, err := e.defForInstance(ctx, instanceID)
	if err != nil {
		return err
	}
	return e.store.Tx(ctx, func(tx store.Tx) error {
		inst, err := tx.GetInstanceForUpdate(ctx, instanceID)
		if err != nil {
			return err
		}
		if inst.Status != model.StatusWaiting {
			return nil
		}
		step, ok := def.StepByID(inst.CurrentStep)
		if !ok || step.Type != model.StepWaitSignal || step.Signal != name {
			return nil // not waiting on this signal
		}
		sig, found, err := tx.ConsumeSignal(ctx, instanceID, name)
		if err != nil || !found {
			return err
		}
		return e.applySignal(ctx, tx, inst, step, sig, e.clock.Now())
	})
}

// applySignal records SIGNAL_RECEIVED and transitions a waiting instance to the
// wait_signal step's successor. Caller holds the instance lock.
func (e *Engine) applySignal(ctx context.Context, tx store.Tx, inst *model.Instance, step model.Step, sig *model.Signal, now time.Time) error {
	if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventSignalReceived, StepID: step.ID, Payload: sig.Payload, CreatedAt: now}); err != nil {
		return err
	}
	inst.CurrentStep = step.Next
	inst.Status = model.StatusRunnable
	inst.UpdatedAt = now
	return tx.UpdateInstance(ctx, inst)
}

// OnTimerFired applies a fired timer: a signal-timeout routes the instance to
// the step's timeout_next; a sleep routes to next. Terminal/advanced instances
// are a no-op (the signal-vs-timer race resolves to whichever the scheduler
// applies first) (spec FR8).
func (e *Engine) OnTimerFired(ctx context.Context, timer *model.Timer) error {
	def, err := e.defForInstance(ctx, timer.InstanceID)
	if err != nil {
		return err
	}
	return e.store.Tx(ctx, func(tx store.Tx) error {
		inst, err := tx.GetInstanceForUpdate(ctx, timer.InstanceID)
		if err != nil {
			return err
		}
		// Mark fired regardless so we never reprocess it.
		if err := tx.MarkTimerFired(ctx, timer.ID); err != nil {
			return err
		}
		if inst.Status != model.StatusWaiting || inst.CurrentStep != timer.StepID {
			return nil // already advanced (e.g. signal won the race)
		}
		step, _ := def.StepByID(timer.StepID)
		now := e.clock.Now()
		if _, err := tx.AppendEvent(ctx, &model.Event{InstanceID: inst.ID, Type: model.EventTimerFired, StepID: timer.StepID, CreatedAt: now}); err != nil {
			return err
		}
		switch timer.Kind {
		case model.TimerSignalTimeout:
			inst.CurrentStep = step.TimeoutNext
		case model.TimerSleep:
			inst.CurrentStep = step.Next
		default:
			return fmt.Errorf("unknown timer kind %q", timer.Kind)
		}
		inst.Status = model.StatusRunnable
		inst.UpdatedAt = now
		return tx.UpdateInstance(ctx, inst)
	})
}

func taskIdempotencyKey(inst *model.Instance, step model.Step) string {
	// RowVersion at entry makes the key unique per step-entry (so revisiting a
	// step yields a new task) yet stable across retries of that task (retry
	// does not change the key), satisfying the worker idempotency contract.
	return fmt.Sprintf("%s|%s|%d", inst.ID, step.ID, inst.RowVersion)
}
