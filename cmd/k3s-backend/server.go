// Package main is the k3s-backend HTTP entry point. server.go (this file)
// declares the Server type, the backendService interface every handler
// calls into, the route table, the HMAC inbound middleware, the SSRF
// guard on outbound callback URLs, and the response helpers. main.go
// wires a *k3s.Backend instance into NewServer and runs the HTTP loop
// with graceful shutdown.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/k3s"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// backendService defines the methods that handlers call on the backend.
// *k3s.Backend satisfies this interface structurally. The compile-time
// guard in internal/backend/k3s/provision_stub.go re-states the contract
// inline so any signature drift surfaces at the package boundary rather
// than only at the NewServer call site.
type backendService interface {
	Provision(ctx context.Context, req backend.ProvisionRequest) error
	Deprovision(ctx context.Context, leaseUUID string) error
	GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error)
	GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error)
	GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error)
	ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error)
	LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error)
	Restart(ctx context.Context, req backend.RestartRequest) error
	Update(ctx context.Context, req backend.UpdateRequest) error
	ListRetentions(ctx context.Context) ([]backend.RetainedLease, error)
	ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error
	GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error)
	Health(ctx context.Context) error
	Stats() shared.ResourceStats
}

// Server handles HTTP requests for the K3s backend.
type Server struct {
	backend            backendService
	callbackSecret     string
	logger             *slog.Logger
	maxRequestBodySize int64
}

// NewServer creates a new HTTP server for the K3s backend. maxRequestBodySize
// caps inbound request bodies; a non-positive value falls back to
// k3s.DefaultMaxRequestBodySize. (ENG-448 / F42)
func NewServer(b backendService, callbackSecret string, logger *slog.Logger, maxRequestBodySize int64) *Server {
	if maxRequestBodySize <= 0 {
		maxRequestBodySize = k3s.DefaultMaxRequestBodySize
	}
	return &Server{
		backend:            b,
		callbackSecret:     callbackSecret,
		logger:             logger,
		maxRequestBodySize: maxRequestBodySize,
	}
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	authMw := hmacAuthMiddleware(s.callbackSecret, s.logger, s.maxRequestBodySize)
	mux.Handle("POST /provision", authMw(http.HandlerFunc(s.handleProvision)))
	mux.Handle("POST /deprovision", authMw(http.HandlerFunc(s.handleDeprovision)))
	mux.Handle("GET /info/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetInfo)))
	mux.Handle("GET /logs/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetLogs)))
	mux.Handle("GET /provisions/{lease_uuid}", authMw(http.HandlerFunc(s.handleGetProvision)))
	mux.Handle("GET /provisions", authMw(http.HandlerFunc(s.handleListProvisions)))
	mux.Handle("POST /restart", authMw(http.HandlerFunc(s.handleRestart)))
	mux.Handle("POST /update", authMw(http.HandlerFunc(s.handleUpdate)))
	mux.Handle("GET /retentions", authMw(http.HandlerFunc(s.handleListRetentions)))
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
	if err := validateCallbackURL(req.CallbackURL); err != nil {
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
		if n > maxTailLines {
			s.errorResponse(w, http.StatusBadRequest, fmt.Sprintf("tail must not exceed %d", maxTailLines))
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

// DeprovisionRequest is the request body for /deprovision.
type DeprovisionRequest struct {
	LeaseUUID string `json:"lease_uuid"`
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
	if err := validateCallbackURL(req.CallbackURL); err != nil {
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

func (s *Server) handleListRetentions(w http.ResponseWriter, r *http.Request) {
	retentions, err := s.backend.ListRetentions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Serialize as `[]` not `null` even if the backend returned a nil slice.
	if retentions == nil {
		retentions = []backend.RetainedLease{}
	}
	s.writeJSON(w, http.StatusOK, backend.ListRetentionsResponse{Retentions: retentions})
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
		// Forward-compat error mapping for ENG-134+. In the ENG-133 scaffold,
		// ReconcileCustomDomain unconditionally returns nil (no-op) because
		// ingress is rejected at config-Validate time, so the branches below
		// are unreachable today. When ENG-134+ ships real custom-domain
		// reconciliation, transient sentinels (lease just deprovisioned,
		// status flipped mid-reconcile) must surface as 404/409 — not 500 —
		// so providerd's circuit breaker exempts them from the trip count.
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
	if err := validateCallbackURL(req.CallbackURL); err != nil {
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

// StatsResponse is the response body for /stats.
type StatsResponse struct {
	TotalCPUCores     float64 `json:"total_cpu_cores"`
	TotalMemoryMB     int64   `json:"total_memory_mb"`
	TotalDiskMB       int64   `json:"total_disk_mb"`
	AllocatedCPUCores float64 `json:"allocated_cpu_cores"`
	AllocatedMemoryMB int64   `json:"allocated_memory_mb"`
	AllocatedDiskMB   int64   `json:"allocated_disk_mb"`
	AvailableCPUCores float64 `json:"available_cpu_cores"`
	AvailableMemoryMB int64   `json:"available_memory_mb"`
	AvailableDiskMB   int64   `json:"available_disk_mb"`
	ActiveContainers  int     `json:"active_containers"`
}

// StatusResponse is a simple status response.
type StatusResponse struct {
	Status string `json:"status"`
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

	limit, cont, perr := backend.ParseProvisionsPageParams(r.URL.Query())
	if perr != nil {
		s.errorResponse(w, http.StatusBadRequest, perr.Error())
		return
	}

	provisions, err := s.backend.ListProvisions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	page, next := backend.PaginateProvisions(provisions, cont, limit)
	// Serialize as [] not null even if empty. PaginateProvisions returns non-nil
	// today; this mirrors the lease_uuid branch above and stays defensive.
	if page == nil {
		page = []backend.ProvisionInfo{}
	}

	s.writeJSON(w, http.StatusOK, backend.ListProvisionsResponse{
		Provisions: page,
		Continue:   next,
	})
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

// ErrorResponse is the response body for errors.
type ErrorResponse struct {
	Error          string                 `json:"error"`
	ValidationCode backend.ValidationCode `json:"validation_code,omitempty"`
}

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

// validationErrorResponse writes a 400 response with a validation_code field
// so the HTTPClient can reconstruct the correct sentinel error.
func (s *Server) validationErrorResponse(w http.ResponseWriter, err error) {
	s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
		Error:          err.Error(),
		ValidationCode: backend.ClassifyValidationError(err),
	})
}

// jsonError writes a JSON error response with the correct Content-Type.
// Used by standalone middleware that doesn't have access to Server methods.
func jsonError(w http.ResponseWriter, status int, message string) {
	encoded, _ := json.Marshal(ErrorResponse{Error: message})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

const maxTailLines = 10000 // Upper bound for log tail requests

// validateCallbackURL validates that a callback URL is safe to use.
// It rejects non-HTTP(S) schemes and dangerous IP addresses to prevent SSRF.
//
// Note: This validation is defense-in-depth. The callback URL comes from Fred
// (providerd), not from untrusted tenants. Fred should validate its own
// callback_base_url configuration. We allow localhost and private IPs here
// since backends commonly run on private networks alongside Fred.
func validateCallbackURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("malformed URL")
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}

	if parsed.Host == "" {
		return fmt.Errorf("host is required")
	}

	// Extract hostname (without port) and normalize for SSRF parsing.
	//
	// Trailing-dot trim mirrors internal/config.validateExternalURL — without
	// it, "http://169.254.169.254./..." would make net.ParseIP return nil and
	// skip the link-local block, despite resolving to the same metadata IP.
	//
	// Zone-suffix split handles RFC 6874 zone-scoped IPv6 literals like
	// "http://[fe80::1%25eth0]/...". url.Hostname() returns "fe80::1%eth0"
	// (URL-decodes %25 to %); net.ParseIP rejects zone-suffixed strings;
	// Go's net.Dialer DOES dial them on Linux/BSD. Stripping the zone
	// suffix before ParseIP makes the IP-class check see "fe80::1" and
	// correctly trip IsLinkLocalUnicast.
	hostname := parsed.Hostname()
	hostname = strings.TrimSuffix(hostname, ".")
	if i := strings.IndexByte(hostname, '%'); i >= 0 {
		hostname = hostname[:i]
	}

	// Check if hostname is an IP address
	ip := net.ParseIP(hostname)
	if ip != nil {
		// Block link-local addresses (169.254.0.0/16, fe80::/10) which
		// include cloud metadata endpoints like 169.254.169.254.
		if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return fmt.Errorf("link-local addresses are not allowed")
		}
		// Block unspecified addresses (0.0.0.0, ::). These resolve to
		// "any interface" on the target host and shouldn't be used as
		// callback destinations. Matches internal/config.validateExternalURL.
		if ip.IsUnspecified() {
			return fmt.Errorf("unspecified addresses are not allowed")
		}
	}

	return nil
}

// hmacAuthMiddleware returns middleware that verifies HMAC-SHA256 signatures on requests.
func hmacAuthMiddleware(secret string, logger *slog.Logger, maxRequestBodySize int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Limit request body size
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)

			sig := r.Header.Get(hmacauth.SignatureHeader)
			if sig == "" {
				logger.Warn("missing signature header", "remote", r.RemoteAddr, "path", r.URL.Path)
				jsonError(w, http.StatusUnauthorized, "missing signature")
				return
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				logger.Warn("failed to read request body", "error", err)
				jsonError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}

			if err := hmacauth.VerifyRequest(secret, r, body, sig, 5*time.Minute); err != nil {
				logger.Warn("signature verification failed",
					"error", err,
					"remote", r.RemoteAddr,
					"path", r.URL.Path,
				)
				jsonError(w, http.StatusUnauthorized, "invalid signature")
				return
			}

			// Replace the body so handlers can read it
			r.Body = io.NopCloser(bytes.NewReader(body))
			next.ServeHTTP(w, r)
		})
	}
}
