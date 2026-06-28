package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

type signalRequest struct {
	Payload json.RawMessage `json:"payload,omitempty"`
}

// handleSignal records an external signal for an instance (spec FR9). The
// transition is applied by the engine's record-then-apply path.
func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	name := chi.URLParam(r, "name")

	// Tenant guard: only signal instances you own.
	inst, err := s.store.GetInstance(r.Context(), id)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	if inst.TenantID != tenant {
		writeError(w, http.StatusNotFound, "instance not found")
		return
	}

	var req signalRequest
	if r.ContentLength != 0 {
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if err := s.engine.SignalInstance(r.Context(), id, tenant, name, req.Payload); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}
