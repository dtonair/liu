package api

import (
	"encoding/json"
	"net/http"

	"github.com/dtonair/liu/internal/engine"
	"github.com/dtonair/liu/internal/model"
	"github.com/dtonair/liu/internal/store"
	"github.com/go-chi/chi/v5"
)

type startInstanceRequest struct {
	Input          json.RawMessage `json:"input,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Version        int             `json:"version,omitempty"`
}

// handleStartInstance starts (or idempotently returns) a workflow instance
// (spec FR3). The tenant comes from the authenticated context, never the body.
func (s *Server) handleStartInstance(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	name := chi.URLParam(r, "name")
	var req startInstanceRequest
	if r.ContentLength != 0 {
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	inst, err := s.engine.StartInstance(r.Context(), engine.StartRequest{
		WorkflowName:   name,
		Version:        req.Version,
		TenantID:       tenant,
		Input:          req.Input,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"instance_id": inst.ID, "status": inst.Status})
}

// handleGetInstance returns the current state of an instance (tenant-scoped).
func (s *Server) handleGetInstance(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	inst, err := s.store.GetInstance(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	if inst.TenantID != tenant {
		// Do not leak existence across tenants.
		writeError(w, http.StatusNotFound, "instance not found")
		return
	}
	writeJSON(w, http.StatusOK, inst)
}

// handleGetHistory returns the append-only history of an instance (spec FR13).
func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	inst, err := s.store.GetInstance(r.Context(), id)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	if inst.TenantID != tenant {
		writeError(w, http.StatusNotFound, "instance not found")
		return
	}
	events, err := s.store.History(r.Context(), id)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"instance_id": id, "events": events})
}

// handleListInstances lists instances for the caller's tenant (spec FR13).
func (s *Server) handleListInstances(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	f := store.InstanceFilter{
		TenantID:     tenant,
		WorkflowName: r.URL.Query().Get("workflow"),
		Status:       model.InstanceStatus(r.URL.Query().Get("status")),
		Limit:        100,
	}
	insts, err := s.store.ListInstances(r.Context(), f)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"instances": insts})
}
