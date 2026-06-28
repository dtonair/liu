package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dtonair/liu/store"
	"github.com/go-chi/chi/v5"
)

type pollRequest struct {
	ActivityType string `json:"activity_type"`
	WorkerID     string `json:"worker_id"`
	WaitSeconds  int    `json:"wait_seconds"`
	LeaseSeconds int    `json:"lease_seconds"`
}

// handlePollTask long-polls for the next task of an activity type (spec FR5).
// It returns 204 if none becomes available within wait_seconds.
func (s *Server) handlePollTask(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	var req pollRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.ActivityType == "" || req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "activity_type and worker_id are required")
		return
	}
	leaseFor := time.Duration(req.LeaseSeconds) * time.Second
	if leaseFor <= 0 {
		leaseFor = 30 * time.Second
	}
	deadline := time.Now().Add(time.Duration(req.WaitSeconds) * time.Second)

	for {
		leased, err := s.store.LeaseTasks(r.Context(), store.LeaseRequest{
			TenantID:     tenant,
			ActivityType: req.ActivityType,
			WorkerID:     req.WorkerID,
			Now:          time.Now().UTC(),
			LeaseFor:     leaseFor,
			Limit:        1,
		})
		if err != nil {
			writeError(w, storeErrorStatus(err), err.Error())
			return
		}
		if len(leased) > 0 {
			writeJSON(w, http.StatusOK, leased[0])
			return
		}
		if !time.Now().Before(deadline) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case <-r.Context().Done():
			w.WriteHeader(http.StatusNoContent)
			return
		case <-time.After(s.pollInterval):
		}
	}
}

type completeRequest struct {
	WorkerID   string          `json:"worker_id"`
	LeaseToken string          `json:"lease_token"`
	Output     json.RawMessage `json:"output,omitempty"`
}

// handleCompleteTask records a successful task result (spec FR6).
func (s *Server) handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	if !s.taskTenantOK(w, r, id, tenant) {
		return
	}
	var req completeRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.engine.OnTaskComplete(r.Context(), id, req.WorkerID, req.LeaseToken, req.Output); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type failRequest struct {
	WorkerID   string `json:"worker_id"`
	LeaseToken string `json:"lease_token"`
	Error      string `json:"error"`
	ErrorClass string `json:"error_class,omitempty"`
	Retryable  bool   `json:"retryable"`
}

// handleFailTask records a task failure, applying the retry policy (spec FR7).
func (s *Server) handleFailTask(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	if !s.taskTenantOK(w, r, id, tenant) {
		return
	}
	var req failRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.engine.OnTaskFail(r.Context(), id, req.WorkerID, req.LeaseToken, req.Error, req.ErrorClass, req.Retryable); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type heartbeatRequest struct {
	WorkerID     string `json:"worker_id"`
	LeaseToken   string `json:"lease_token"`
	LeaseSeconds int    `json:"lease_seconds"`
}

// handleHeartbeatTask extends a task lease for a long-running activity (FR10).
func (s *Server) handleHeartbeatTask(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	id := chi.URLParam(r, "id")
	if !s.taskTenantOK(w, r, id, tenant) {
		return
	}
	var req heartbeatRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	extend := time.Duration(req.LeaseSeconds) * time.Second
	if extend <= 0 {
		extend = 30 * time.Second
	}
	until := time.Now().UTC().Add(extend)
	if err := s.store.HeartbeatTask(r.Context(), id, req.WorkerID, req.LeaseToken, until); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_expires_at": until})
}

// taskTenantOK verifies the task belongs to the caller's tenant, blocking
// cross-tenant task manipulation (spec FR14). Returns false (and writes a
// response) if the check fails.
func (s *Server) taskTenantOK(w http.ResponseWriter, r *http.Request, taskID, tenant string) bool {
	task, err := s.store.GetTask(r.Context(), taskID)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return false
	}
	if task.TenantID != tenant {
		writeError(w, http.StatusNotFound, "task not found")
		return false
	}
	return true
}
