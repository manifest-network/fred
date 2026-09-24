package main

import (
	"net/http"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func (s *Server) lifecyclePendingResponse(w http.ResponseWriter, err error) bool {
	if !shared.IsLifecyclePending(err) && !leasesm.IsLifecyclePending(err) {
		return false
	}
	s.logger.Debug("lifecycle execution remains pending", "error", err)
	s.errorResponseWithCode(w, http.StatusServiceUnavailable,
		"admitted lifecycle work remains pending", backend.CodeLifecyclePending)
	return true
}
