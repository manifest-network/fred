package httpserver

import (
	"encoding/json"
	"net/http"

	"github.com/manifest-network/fred/internal/backend"
)

func (s *Server) writeJSON(w http.ResponseWriter, status int, data any) {
	encoded, err := json.Marshal(data)
	if err != nil {
		s.logger.Error("failed to encode response", "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal encoding error"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, writeErr := w.Write(encoded); writeErr != nil {
		s.logger.Debug("failed to write response body", "error", writeErr)
	}
}

func (s *Server) errorResponse(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, ErrorResponse{Error: message})
}

// errorResponseWithCode writes an error response carrying a machine-readable
// discriminator code so the client can disambiguate an overloaded status code
// (e.g. Restore's 409, which is shared by ErrInvalidState and ErrAlreadyProvisioned).
func (s *Server) errorResponseWithCode(w http.ResponseWriter, status int, message, code string) {
	s.writeJSON(w, status, ErrorResponse{Error: message, Code: code})
}

// validationErrorResponse writes a 400 response with a validation_code field
// so the HTTPClient can reconstruct the correct sentinel error.
func (s *Server) validationErrorResponse(w http.ResponseWriter, err error) {
	s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
		Error:          err.Error(),
		ValidationCode: backend.ClassifyValidationError(err),
	})
}

// JSONError writes a JSON error response with the correct Content-Type.
// Used by standalone middleware that doesn't have access to Server methods.
func JSONError(w http.ResponseWriter, status int, message string) {
	encoded, _ := json.Marshal(ErrorResponse{Error: message})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}
