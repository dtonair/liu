package api

import (
	"net/http"

	"github.com/dtonair/liu/model"
)

// handleCreateDefinition registers a workflow definition (spec FR1). Structural
// validation failures return 400; a conflicting re-registration returns 409.
func (s *Server) handleCreateDefinition(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.tenant(w, r); !ok {
		return
	}
	var def model.Definition
	if err := decode(r, &def); err != nil {
		writeError(w, http.StatusBadRequest, "invalid definition JSON: "+err.Error())
		return
	}
	// Validate up front so malformed definitions are a clear 400, separate from
	// store-level errors.
	if err := def.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.engine.RegisterDefinition(r.Context(), &def); err != nil {
		writeError(w, storeErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"name": def.Name, "version": def.Version})
}
