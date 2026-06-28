package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/dtonair/liu/model"
)

// OutboxSink relays a committed outbox record to an external destination
// (message broker, webhook, log). Implementations must be safe to call with
// at-least-once delivery; downstream consumers dedupe on the record identity.
type OutboxSink interface {
	Publish(ctx context.Context, r *model.OutboxRecord) error
}

// LogSink is the default OutboxSink: it logs each event. Real deployments swap
// in a broker/webhook sink.
type LogSink struct{ Log *slog.Logger }

// Publish writes the event to the log.
func (s LogSink) Publish(_ context.Context, r *model.OutboxRecord) error {
	log := s.Log
	if log == nil {
		log = slog.Default()
	}
	log.Info("outbox event", "instance", r.InstanceID, "tenant", r.TenantID, "type", r.EventType)
	return nil
}

// OutboxPublisher drains unsent outbox records to a sink and marks them sent,
// implementing the publisher side of the transactional outbox (spec FR12).
type OutboxPublisher struct {
	engine   *Engine
	sink     OutboxSink
	interval time.Duration
	batch    int
}

// NewOutboxPublisher returns an OutboxPublisher ticking every interval.
func NewOutboxPublisher(e *Engine, sink OutboxSink, interval time.Duration, batch int) *OutboxPublisher {
	if sink == nil {
		sink = LogSink{Log: e.log}
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if batch <= 0 {
		batch = 100
	}
	return &OutboxPublisher{engine: e, sink: sink, interval: interval, batch: batch}
}

// Run blocks, draining the outbox until ctx is cancelled.
func (p *OutboxPublisher) Run(ctx context.Context) error {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			p.RunOnce(ctx)
		}
	}
}

// RunOnce relays all currently-unsent records and returns the count published.
// A sink failure leaves the record unsent for the next tick (at-least-once).
func (p *OutboxPublisher) RunOnce(ctx context.Context) int {
	records, err := p.engine.store.UnsentOutbox(ctx, p.batch)
	if err != nil {
		p.engine.log.Error("outbox: list unsent", "error", err)
		return 0
	}
	sent := 0
	for _, r := range records {
		if err := p.sink.Publish(ctx, r); err != nil {
			p.engine.log.Error("outbox: publish", "id", r.ID, "error", err)
			continue // retry next tick
		}
		if err := p.engine.store.MarkOutboxSent(ctx, r.ID, p.engine.clock.Now()); err != nil {
			p.engine.log.Error("outbox: mark sent", "id", r.ID, "error", err)
			continue
		}
		sent++
	}
	return sent
}
