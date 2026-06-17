package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
)

// BackendService defines the methods that HTTP handlers call on the backend.
// Both docker.Backend and k3s.Backend satisfy this interface structurally.
type BackendService interface {
	Provision(ctx context.Context, req backend.ProvisionRequest) error
	Deprovision(ctx context.Context, leaseUUID string) error
	GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error)
	GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error)
	GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error)
	ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error)
	LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error)
	Restart(ctx context.Context, req backend.RestartRequest) error
	Update(ctx context.Context, req backend.UpdateRequest) error
	Restore(ctx context.Context, req backend.RestoreRequest) error
	ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error
	GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error)
	Health(ctx context.Context) error
	Stats() shared.ResourceStats
}

// Server handles HTTP requests for a backend.
type Server struct {
	backend        BackendService
	callbackSecret string
	logger         *slog.Logger
}

// NewServer creates a new HTTP server bound to the given backend.
func NewServer(b BackendService, callbackSecret string, logger *slog.Logger) *Server {
	return &Server{
		backend:        b,
		callbackSecret: callbackSecret,
		logger:         logger,
	}
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	authMw := HmacAuthMiddleware(s.callbackSecret, s.logger)
	mux.Handle("POST /provision", authMw(http.HandlerFunc(s.handleProvision)))
	mux.Handle("POST /deprovision", authMw(http.HandlerFunc(s.handleDeprovision)))
	mux.Handle("GET /info/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetInfo)))
	mux.Handle("GET /logs/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetLogs)))
	mux.Handle("GET /provisions/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetProvision)))
	mux.Handle("GET /provisions", authMw(http.HandlerFunc(s.handleListProvisions)))
	mux.Handle("POST /restart", authMw(http.HandlerFunc(s.handleRestart)))
	mux.Handle("POST /update", authMw(http.HandlerFunc(s.handleUpdate)))
	mux.Handle("POST /restore", authMw(http.HandlerFunc(s.handleRestore)))
	mux.Handle("POST /reconcile_custom_domain", authMw(http.HandlerFunc(s.handleReconcileCustomDomain)))
	mux.Handle("GET /releases/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetReleases)))

	// Operational endpoints — no auth required (monitoring, health checks).
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.Handle("GET /metrics", promhttp.Handler())

	return mux
}

func (s *Server) handleProvision(w http.ResponseWriter, r *http.Request) {
	var req backend.ProvisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}
	if req.CallbackURL == "" {
		s.errorResponse(w, http.StatusBadRequest, "callback_url is required")
		return
	}
	if err := ValidateCallbackURL(req.CallbackURL); err != nil {
		s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid callback_url: %s", err))
		return
	}
	if len(req.Items) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "items is required")
		return
	}

	err := s.backend.Provision(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrAlreadyProvisioned) {
			s.errorResponse(w, http.StatusConflict, "lease already provisioned")
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			s.validationErrorResponse(w, err)
			return
		}
		if errors.Is(err, backend.ErrInsufficientResources) {
			s.errorResponse(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusAccepted, backend.ProvisionResponse{
		ProvisionID: req.LeaseUUID,
	})
}

func (s *Server) handleDeprovision(w http.ResponseWriter, r *http.Request) {
	var req DeprovisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	if err := s.backend.Deprovision(r.Context(), req.LeaseUUID); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, StatusResponse{Status: "ok"})
}

func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	info, err := s.backend.GetInfo(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	tail := 100 // default
	if v := r.URL.Query().Get("tail"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			s.errorResponse(w, http.StatusBadRequest, "tail must be a positive integer")
			return
		}
		if n > MaxTailLines {
			s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("tail must not exceed %d", MaxTailLines))
			return
		}
		tail = n
	}

	logs, err := s.backend.GetLogs(r.Context(), leaseUUID, tail)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, logs)
}

func (s *Server) handleGetProvision(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	info, err := s.backend.GetProvision(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleListProvisions(w http.ResponseWriter, r *http.Request) {
	// If lease_uuid query params are present, switch to the filtered subset path.
	// Same wire shape as the unfiltered path: ListProvisionsResponse{Provisions: [...]}.
	// Successful responses are 200 with a (possibly empty) Provisions slice — never
	// 404 — so the caller can distinguish "no matches" from "endpoint missing".
	// Validation failures still return 400 and backend errors still return 500.
	if uuids, ok := r.URL.Query()["lease_uuid"]; ok {
		if len(uuids) == 0 || len(uuids) > backend.MaxLookupUUIDs {
			s.errorResponse(w, http.StatusBadRequest,
				fmt.Sprintf("lease_uuid count must be between 1 and %d", backend.MaxLookupUUIDs))
			return
		}
		for _, u := range uuids {
			if !config.IsValidUUID(u) {
				s.errorResponse(w, http.StatusBadRequest, "invalid lease_uuid")
				return
			}
		}

		provisions, err := s.backend.LookupProvisions(r.Context(), uuids)
		if err != nil {
			s.errorResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		// Ensure the slice serializes as `[]` not `null` even if the backend
		// returned a nil slice (defensive — current impls return non-nil).
		if provisions == nil {
			provisions = []backend.ProvisionInfo{}
		}
		s.writeJSON(w, http.StatusOK, backend.ListProvisionsResponse{Provisions: provisions})
		return
	}

	provisions, err := s.backend.ListProvisions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, backend.ListProvisionsResponse{
		Provisions: provisions,
	})
}

func (s *Server) handleRestart(w http.ResponseWriter, r *http.Request) {
	var req backend.RestartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}
	if req.CallbackURL == "" {
		s.errorResponse(w, http.StatusBadRequest, "callback_url is required")
		return
	}
	if err := ValidateCallbackURL(req.CallbackURL); err != nil {
		s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid callback_url: %s", err))
		return
	}

	err := s.backend.Restart(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			s.errorResponse(w, http.StatusConflict, "invalid state for restart")
			return
		}
		s.logger.Error("restart failed", "lease_uuid", req.LeaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusAccepted, StatusResponse{Status: "restarting"})
}

func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	var req backend.UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}
	if req.CallbackURL == "" {
		s.errorResponse(w, http.StatusBadRequest, "callback_url is required")
		return
	}
	if err := ValidateCallbackURL(req.CallbackURL); err != nil {
		s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid callback_url: %s", err))
		return
	}
	if len(req.Payload) == 0 {
		s.errorResponse(w, http.StatusBadRequest, "payload is required")
		return
	}

	err := s.backend.Update(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			s.errorResponse(w, http.StatusConflict, "invalid state for update")
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			s.validationErrorResponse(w, err)
			return
		}
		s.logger.Error("update failed", "lease_uuid", req.LeaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusAccepted, StatusResponse{Status: "updating"})
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	var req backend.RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}
	if req.FromLeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "from_lease_uuid is required")
		return
	}
	if req.CallbackURL == "" {
		s.errorResponse(w, http.StatusBadRequest, "callback_url is required")
		return
	}
	if err := ValidateCallbackURL(req.CallbackURL); err != nil {
		s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("invalid callback_url: %s", err))
		return
	}

	err := s.backend.Restore(r.Context(), req)
	if err != nil {
		if errors.Is(err, backend.ErrNotRetained) {
			s.errorResponse(w, http.StatusUnprocessableEntity, "no retained data for lease")
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			// 409 with no code → client maps to ErrInvalidState.
			s.errorResponse(w, http.StatusConflict, "invalid state for restore")
			return
		}
		if errors.Is(err, backend.ErrAlreadyProvisioned) {
			// 409 with code="already_provisioned" → client reconstructs
			// ErrAlreadyProvisioned (Restore overloads 409 for two sentinels).
			s.errorResponseWithCode(w, http.StatusConflict, "lease already provisioned", "already_provisioned")
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			s.validationErrorResponse(w, err)
			return
		}
		if errors.Is(err, backend.ErrInsufficientResources) {
			// 503 Service Unavailable, matching handleProvision: the backend is at
			// capacity, not a permanent client error.
			s.errorResponse(w, http.StatusServiceUnavailable, "insufficient resources for restore")
			return
		}
		s.logger.Error("restore failed", "lease_uuid", req.LeaseUUID, "from_lease_uuid", req.FromLeaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusAccepted, StatusResponse{Status: "restoring"})
}

func (s *Server) handleReconcileCustomDomain(w http.ResponseWriter, r *http.Request) {
	var req backend.ReconcileCustomDomainRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.LeaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	if err := s.backend.ReconcileCustomDomain(r.Context(), req.LeaseUUID, req.Items); err != nil {
		// Surface ErrNotProvisioned and ErrInvalidState as 404/409 so the
		// HTTPClient can map them back to typed errors. Both signal benign
		// races (lease just deprovisioned, or status flipped between our
		// pre-check and Restart's own check) — the providerd-side circuit
		// breaker exempts them from the trip count. Collapsing them into
		// 500 here would cause the CB to count routine reconcile-tick
		// races as backend failures.
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			s.errorResponse(w, http.StatusConflict, "invalid state for reconcile")
			return
		}
		s.logger.Error("reconcile_custom_domain failed", "lease_uuid", req.LeaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetReleases(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")
	if leaseUUID == "" {
		s.errorResponse(w, http.StatusBadRequest, "lease_uuid is required")
		return
	}

	releases, err := s.backend.GetReleases(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			s.errorResponse(w, http.StatusNotFound, "not provisioned")
			return
		}
		s.logger.Error("get releases failed", "lease_uuid", leaseUUID, "error", err)
		s.errorResponse(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.writeJSON(w, http.StatusOK, releases)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.backend.Health(r.Context()); err != nil {
		s.errorResponse(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, StatusResponse{Status: "healthy"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.backend.Stats()

	s.writeJSON(w, http.StatusOK, StatsResponse{
		TotalCPUCores:     stats.TotalCPU,
		TotalMemoryMB:     stats.TotalMemoryMB,
		TotalDiskMB:       stats.TotalDiskMB,
		AllocatedCPUCores: stats.AllocatedCPU,
		AllocatedMemoryMB: stats.AllocatedMemoryMB,
		AllocatedDiskMB:   stats.AllocatedDiskMB,
		AvailableCPUCores: stats.AvailableCPU(),
		AvailableMemoryMB: stats.AvailableMemoryMB(),
		AvailableDiskMB:   stats.AvailableDiskMB(),
		ActiveContainers:  stats.AllocationCount,
	})
}
