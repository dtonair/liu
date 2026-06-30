package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/dtonair/liu/model"
	schedulecron "github.com/dtonair/liu/schedule"
	"github.com/dtonair/liu/store"
	"github.com/go-chi/chi/v5"
)

type createScheduleRequest struct {
	WorkflowName string          `json:"workflow_name"`
	Version      int             `json:"version,omitempty"`
	Cron         string          `json:"cron"`
	Timezone     string          `json:"timezone,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	Enabled      *bool           `json:"enabled,omitempty"`
}

func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	var req createScheduleRequest
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.WorkflowName == "" {
		writeError(w, http.StatusBadRequest, "workflow_name is required")
		return
	}
	if req.Version == 0 {
		if _, err := s.store.GetLatestDefinition(r.Context(), req.WorkflowName); err != nil {
			writeError(w, storeErrorStatus(err), err.Error())
			return
		}
	} else if _, err := s.store.GetDefinition(r.Context(), req.WorkflowName, req.Version); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	timezone := req.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	now := time.Now().UTC()
	nextRunAt, err := schedulecron.NextAfterInLocation(req.Cron, timezone, now)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	sched, err := s.store.CreateSchedule(r.Context(), &model.Schedule{
		ID:           store.NewID(),
		TenantID:     tenant,
		WorkflowName: req.WorkflowName,
		Version:      req.Version,
		Cron:         req.Cron,
		Timezone:     timezone,
		Input:        req.Input,
		Enabled:      enabled,
		NextRunAt:    nextRunAt,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, sched)
}

func (s *Server) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	schedules, err := s.store.ListSchedules(r.Context(), tenant)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": schedules})
}

func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	sched, found := s.scheduleForTenant(w, r, tenant)
	if !found {
		return
	}
	writeJSON(w, http.StatusOK, sched)
}

func (s *Server) handlePauseSchedule(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	now := time.Now().UTC()
	sched, err := s.store.UpdateScheduleEnabled(r.Context(), chi.URLParam(r, "id"), tenant, false, time.Time{}, now)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sched)
}

func (s *Server) handleResumeSchedule(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	sched, found := s.scheduleForTenant(w, r, tenant)
	if !found {
		return
	}
	now := time.Now().UTC()
	nextRunAt, err := schedulecron.NextAfterInLocation(sched.Cron, sched.Timezone, now)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.store.UpdateScheduleEnabled(r.Context(), sched.ID, tenant, true, nextRunAt, now)
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) handleDeleteSchedule(w http.ResponseWriter, r *http.Request) {
	tenant, ok := s.tenant(w, r)
	if !ok {
		return
	}
	if err := s.store.DeleteSchedule(r.Context(), chi.URLParam(r, "id"), tenant); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) scheduleForTenant(w http.ResponseWriter, r *http.Request, tenant string) (*model.Schedule, bool) {
	sched, err := s.store.GetSchedule(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return nil, false
	}
	if sched.TenantID != tenant {
		writeError(w, http.StatusNotFound, "schedule not found")
		return nil, false
	}
	return sched, true
}
