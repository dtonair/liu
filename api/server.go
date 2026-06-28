// Package api exposes the workflow engine control plane over HTTP/REST: the
// definition registry, instance lifecycle, the worker task protocol
// (poll/complete/fail/heartbeat), and signal ingress. All routes are
// authenticated and tenant-scoped (spec FR1, FR3, FR5, FR6, FR9, FR13, FR14).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dtonair/liu/engine"
	"github.com/dtonair/liu/security"
	"github.com/dtonair/liu/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Server holds the dependencies for the HTTP API.
type Server struct {
	engine *engine.Engine
	store  store.Store
	auth   *security.Authenticator

	// pollInterval is how often a long-poll re-checks the queue.
	pollInterval time.Duration
	// readyFn reports readiness (DB reachable, scheduler leader). Optional.
	readyFn func(context.Context) error
}

// Options configures the Server.
type Options struct {
	Auth         *security.Authenticator
	PollInterval time.Duration
	ReadyFn      func(context.Context) error
}

// NewServer constructs an API server.
func NewServer(e *engine.Engine, s store.Store, opts Options) *Server {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 200 * time.Millisecond
	}
	return &Server{engine: e, store: s, auth: opts.Auth, pollInterval: opts.PollInterval, readyFn: opts.ReadyFn}
}

// Router builds the HTTP handler.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	// Health and metrics are unauthenticated (scraped by infra).
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	if m := s.engine.Metrics(); m != nil {
		r.Handle("/metrics", m.Handler())
	}

	r.Group(func(r chi.Router) {
		r.Use(s.auth.Middleware)

		r.Post("/v1/definitions", s.handleCreateDefinition)

		r.Post("/v1/workflows/{name}/instances", s.handleStartInstance)
		r.Get("/v1/instances", s.handleListInstances)
		r.Get("/v1/instances/{id}", s.handleGetInstance)
		r.Get("/v1/instances/{id}/history", s.handleGetHistory)
		r.Post("/v1/instances/{id}/signals/{name}", s.handleSignal)

		r.Post("/v1/tasks/poll", s.handlePollTask)
		r.Post("/v1/tasks/{id}/complete", s.handleCompleteTask)
		r.Post("/v1/tasks/{id}/fail", s.handleFailTask)
		r.Post("/v1/tasks/{id}/heartbeat", s.handleHeartbeatTask)
	})

	return r
}

// --- helpers ---

func (s *Server) tenant(w http.ResponseWriter, r *http.Request) (string, bool) {
	t, ok := security.TenantFrom(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no tenant in context")
		return "", false
	}
	return t, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decode(r *http.Request, v any) error {
	if r.Body == nil {
		return errors.New("empty body")
	}
	return json.NewDecoder(r.Body).Decode(v)
}

// storeErrorStatus maps store sentinel errors to HTTP status codes.
func storeErrorStatus(err error) int {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, store.ErrChecksumConflict):
		return http.StatusConflict
	case errors.Is(err, store.ErrLeaseInvalid):
		return http.StatusConflict
	case errors.Is(err, store.ErrVersionConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.readyFn != nil {
		if err := s.readyFn(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
