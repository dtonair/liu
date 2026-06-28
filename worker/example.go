package worker

import (
	"context"
	"encoding/json"

	"github.com/dtonair/liu/model"
)

// OrderApprovalHandlers returns the demo order_approval activity handlers keyed
// by activity type. They are intentionally trivial (and idempotent) so the
// example flow runs end-to-end without external dependencies.
func OrderApprovalHandlers() map[string]Handler {
	return map[string]Handler{
		"reserve_inventory": func(_ context.Context, t *model.Task) (json.RawMessage, error) {
			return json.Marshal(map[string]any{"reserved": true, "idempotency_key": t.IdempotencyKey})
		},
		"capture_payment": func(_ context.Context, t *model.Task) (json.RawMessage, error) {
			return json.Marshal(map[string]any{"captured": true, "idempotency_key": t.IdempotencyKey})
		},
		"release_inventory": func(_ context.Context, t *model.Task) (json.RawMessage, error) {
			return json.Marshal(map[string]any{"released": true, "idempotency_key": t.IdempotencyKey})
		},
	}
}

// RegisterOrderApprovalHandlers registers all demo handlers on a runner.
func RegisterOrderApprovalHandlers(r *Runner) {
	for name, h := range OrderApprovalHandlers() {
		r.Register(name, h)
	}
}
