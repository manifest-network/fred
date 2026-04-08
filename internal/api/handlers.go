package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner"
)

// ChainClient defines the chain operations needed by handlers.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	Ping(ctx context.Context) error
}

// TokenTrackerInterface defines the interface for token replay protection.
// This interface allows for testing with mock implementations.
type TokenTrackerInterface interface {
	TryUse(signature string) error
	Healthy() error
	Close() error
}

// PlacementLookup provides lease→backend mapping for read-path routing.
type PlacementLookup interface {
	Get(leaseUUID string) string
	Healthy() error
}

// Handlers contains HTTP request handlers.
type Handlers struct {
	client            ChainClient
	backendRouter     *backend.Router
	tokenTracker      TokenTrackerInterface
	statusChecker     StatusChecker
	placementLookup   PlacementLookup
	eventBroker       *EventBroker
	wsUpgrader        websocket.Upgrader
	wsMaxMessageSize  int64         // max bytes the server will read from a client message on /events
	wsMaxConnLifetime time.Duration // max lifetime of an /events subscription before forced reconnect
	providerUUID      string
	bech32Prefix      string
	callbackBaseURL   string
}

// HandlersConfig configures a Handlers instance.
type HandlersConfig struct {
	Client          ChainClient
	BackendRouter   *backend.Router
	TokenTracker    TokenTrackerInterface // optional but recommended for replay attack protection
	StatusChecker   StatusChecker         // optional but required for the /status endpoint
	PlacementLookup PlacementLookup       // optional — used for routing reads to the correct backend
	EventBroker     *EventBroker          // optional — if nil, the events endpoint will return 501
	ProviderUUID    string
	Bech32Prefix    string
	CallbackBaseURL string // used for restart/update callbacks to the backend
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(cfg HandlersConfig) *Handlers {
	return &Handlers{
		client:          cfg.Client,
		backendRouter:   cfg.BackendRouter,
		tokenTracker:    cfg.TokenTracker,
		statusChecker:   cfg.StatusChecker,
		placementLookup: cfg.PlacementLookup,
		eventBroker:     cfg.EventBroker,
		wsUpgrader: websocket.Upgrader{
			// Allow all origins: this API is not browser-facing. Clients are
			// CLI tools and services that authenticate with cryptographically
			// signed ADR-036 tokens (no cookies/sessions). Origin checks would
			// break non-browser clients that don't send Origin headers.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		wsMaxMessageSize:  wsDefaultMaxMessageSize,
		wsMaxConnLifetime: wsDefaultMaxConnLifetime,
		providerUUID:      cfg.ProviderUUID,
		bech32Prefix:      cfg.Bech32Prefix,
		callbackBaseURL:   cfg.CallbackBaseURL,
	}
}

// AuthenticatedRequest contains the result of a successful authentication.
type AuthenticatedRequest struct {
	Token *AuthToken
	Lease *billingtypes.Lease
}

// AuthenticateLeaseRequest performs common authentication and authorization for lease endpoints.
// It extracts and validates the bearer token, optionally checks for replay attacks,
// queries the lease from chain, and verifies tenant and provider ownership.
//
// Parameters:
//   - r: the HTTP request
//   - leaseUUID: the lease UUID from the URL path
//   - checkReplay: whether to check for token replay (set false for idempotent/read-heavy endpoints like status)
//   - requireActive: if true, only ACTIVE leases are accepted; if false, any state is allowed
//
// Returns AuthenticatedRequest on success, or an error with the appropriate HTTP status code.
func (h *Handlers) AuthenticateLeaseRequest(r *http.Request, leaseUUID string, checkReplay bool, requireActive bool) (*AuthenticatedRequest, int, error) {
	// Validate lease UUID format
	if !config.IsValidUUID(leaseUUID) {
		return nil, http.StatusBadRequest, errors.New(errMsgInvalidLeaseUUID)
	}

	// Use pre-validated token from middleware context if available (avoids redundant
	// ECDSA verification). Falls back to self-validate when rate limiting is disabled.
	token := AuthTokenFromContext(r.Context())
	if token == nil {
		var err error
		token, err = h.extractToken(r)
		if err != nil {
			return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
		}
		if err := token.Validate(h.bech32Prefix); err != nil {
			return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
		}
	}

	// Check for token replay attack (if tracker is configured and checkReplay is true)
	if checkReplay && h.tokenTracker != nil {
		if err := h.tokenTracker.TryUse(token.Signature); err != nil {
			if errors.Is(err, ErrTokenAlreadyUsed) {
				slog.Warn("token replay detected",
					"lease_uuid", leaseUUID,
					"tenant", token.Tenant,
				)
				return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
			}
			// Database error - fail closed to prevent potential replay attacks.
			// Token lifetime is short (30s), so clients can retry with a fresh token.
			slog.Error("token tracker unavailable", "error", err)
			return nil, http.StatusServiceUnavailable, errors.New(errMsgServiceUnavailable)
		}
	}

	// Verify the token's lease UUID matches the request
	if token.LeaseUUID != leaseUUID {
		slog.Warn("lease UUID mismatch",
			"token_lease_uuid", token.LeaseUUID,
			"request_lease_uuid", leaseUUID,
		)
		return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
	}

	lease, status, err := verifyLeaseAccess(r.Context(), h.client, h.providerUUID, leaseUUID, token.Tenant, requireActive)
	if err != nil {
		return nil, status, err
	}

	return &AuthenticatedRequest{
		Token: token,
		Lease: lease,
	}, http.StatusOK, nil
}

// authenticateAndResolve performs the common handler preamble: extract lease UUID
// from the path, authenticate, verify the backend router is configured, and resolve
// the correct backend for the lease. On failure it writes the appropriate HTTP error
// and returns ok=false; the caller should return immediately.
func (h *Handlers) authenticateAndResolve(w http.ResponseWriter, r *http.Request, checkReplay, requireActive bool) (auth *AuthenticatedRequest, leaseUUID string, b backend.Backend, ok bool) {
	leaseUUID = r.PathValue("lease_uuid")

	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, checkReplay, requireActive)
	if err != nil {
		writeError(w, err.Error(), status)
		return nil, leaseUUID, nil, false
	}

	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return nil, leaseUUID, nil, false
	}

	sku := provisioner.ExtractRoutingSKU(auth.Lease)
	b = h.resolveBackend(leaseUUID, sku)
	if b == nil {
		slog.Error("no backend available", "sku", sku, "lease_uuid", leaseUUID)
		writeError(w, "service unavailable", http.StatusServiceUnavailable)
		return nil, leaseUUID, nil, false
	}

	return auth, leaseUUID, b, true
}

// resolveBackend determines the correct backend for a lease.
// Checks placement first (handles round-robin routing), falls back to SKU routing.
func (h *Handlers) resolveBackend(leaseUUID, sku string) backend.Backend {
	if h.placementLookup != nil {
		if name := h.placementLookup.Get(leaseUUID); name != "" {
			if b := h.backendRouter.GetBackendByName(name); b != nil {
				return b
			}
			slog.Debug("stale placement record, falling back to SKU routing",
				"lease_uuid", leaseUUID,
				"placement_backend", name,
			)
		}
	}
	return h.backendRouter.Route(sku)
}

// ConnectionResponse represents the response for connection details.
type ConnectionResponse struct {
	LeaseUUID    string            `json:"lease_uuid"`
	Tenant       string            `json:"tenant"`
	ProviderUUID string            `json:"provider_uuid"`
	Connection   ConnectionDetails `json:"connection"`
}

// ConnectionDetails contains the connection information for a lease.
// For multi-instance leases, the Instances array contains per-instance details.
// For stack (multi-service) leases, the Services map contains per-service details.
type ConnectionDetails struct {
	Host      string                              `json:"host"`
	FQDN      string                              `json:"fqdn,omitempty"`
	Ports     map[string]PortMapping              `json:"ports,omitempty"`
	Instances []InstanceInfo                      `json:"instances,omitempty"`
	Services  map[string]ServiceConnectionDetails `json:"services,omitempty"`
	Protocol  string                              `json:"protocol,omitempty"`
	Metadata  map[string]string                   `json:"metadata,omitempty"`
}

// ServiceConnectionDetails contains connection details for a single service in a stack.
type ServiceConnectionDetails struct {
	FQDN      string         `json:"fqdn,omitempty"`
	Instances []InstanceInfo `json:"instances"`
}

// InstanceInfo contains connection details for a single instance in a multi-instance lease.
type InstanceInfo struct {
	InstanceIndex int                    `json:"instance_index"`
	ContainerID   string                 `json:"container_id,omitempty"`
	Image         string                 `json:"image,omitempty"`
	Status        string                 `json:"status,omitempty"`
	FQDN          string                 `json:"fqdn,omitempty"`
	Ports         map[string]PortMapping `json:"ports,omitempty"`
}

// PortMapping represents a port binding from container to host.
type PortMapping struct {
	HostIP   string `json:"host_ip"`
	HostPort int    `json:"host_port"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// CallbackResponse represents the response for backend callbacks.
// Used to provide debugging information to authenticated backends.
type CallbackResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Common error messages for API responses.
// These constants ensure consistency across handlers and simplify testing.
const (
	errMsgUnauthorized         = "unauthorized"
	errMsgForbidden            = "forbidden"
	errMsgInternalServerError  = "internal server error"
	errMsgServiceNotConfigured = "service not configured"
	errMsgServiceUnavailable   = "service temporarily unavailable"
	errMsgInvalidLeaseUUID     = "invalid lease UUID format"
	errMsgLeaseNotFound        = "lease not found"
)

// GetLeaseConnection handles GET /v1/leases/{lease_uuid}/connection
func (h *Handlers) GetLeaseConnection(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, true, true)
	if !ok {
		return
	}

	info, err := backendClient.GetInfo(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			slog.Warn("lease not yet provisioned", "lease_uuid", leaseUUID)
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		slog.Error("failed to get info from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	// Build response with lease info from backend
	response := ConnectionResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Lease.Tenant,
		ProviderUUID: h.providerUUID,
		Connection:   extractConnectionDetails(*info),
	}

	slog.Info("lease info served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// LeaseStatusResponse represents the response for lease status.
// Includes tenant and provider_uuid for consistency with ConnectionResponse.
type LeaseStatusResponse struct {
	LeaseUUID           string `json:"lease_uuid"`
	Tenant              string `json:"tenant"`
	ProviderUUID        string `json:"provider_uuid"`
	State               string `json:"state"`
	RequiresPayload     bool   `json:"requires_payload"`
	MetaHashHex         string `json:"meta_hash_hex,omitempty"` // For debugging - shows the expected payload hash
	PayloadReceived     bool   `json:"payload_received"`
	ProvisioningStarted bool   `json:"provisioning_started"`
	ProvisionStatus     string `json:"provision_status,omitempty"`
	FailCount           int    `json:"fail_count,omitempty"`
	LastError           string `json:"last_error,omitempty"`
}

// GetLeaseStatus handles GET /v1/leases/{lease_uuid}/status
func (h *Handlers) GetLeaseStatus(w http.ResponseWriter, r *http.Request) {
	leaseUUID := r.PathValue("lease_uuid")

	// Authenticate and authorize the request (any lease state, no replay check for read-heavy endpoint)
	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, false, false)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	// Build status response
	hasMetaHash := len(auth.Lease.MetaHash) > 0
	response := LeaseStatusResponse{
		LeaseUUID:       leaseUUID,
		Tenant:          auth.Token.Tenant,
		ProviderUUID:    h.providerUUID,
		State:           auth.Lease.State.String(),
		RequiresPayload: hasMetaHash,
	}
	if hasMetaHash {
		response.MetaHashHex = hex.EncodeToString(auth.Lease.MetaHash)
	}

	// Check provisioning status if checker is available
	if h.statusChecker != nil {
		hasPayload, err := h.statusChecker.HasPayload(leaseUUID)
		if err != nil {
			slog.Warn("failed to check payload status", "lease_uuid", leaseUUID, "error", err)
		}
		response.PayloadReceived = hasPayload
		response.ProvisioningStarted = h.statusChecker.IsInFlight(leaseUUID)
	}

	// For active leases, include provision status from the backend
	if h.backendRouter != nil && auth.Lease.State == billingtypes.LEASE_STATE_ACTIVE {
		sku := provisioner.ExtractRoutingSKU(auth.Lease)
		if backendClient := h.resolveBackend(leaseUUID, sku); backendClient != nil {
			info, err := backendClient.GetProvision(r.Context(), leaseUUID)
			if err == nil {
				response.ProvisionStatus = string(info.Status)
				response.FailCount = info.FailCount
				response.LastError = info.LastError
			}
			// Errors are intentionally ignored — provision status is best-effort.
			// ErrNotProvisioned during initial setup is expected and safe to skip.
		}
	}

	slog.Info("lease status served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"state", response.State,
	)

	writeJSON(w, response, http.StatusOK)
}

// LeaseProvisionResponse represents the response for provision diagnostics.
type LeaseProvisionResponse struct {
	LeaseUUID    string `json:"lease_uuid"`
	Tenant       string `json:"tenant"`
	ProviderUUID string `json:"provider_uuid"`
	Status       string `json:"status"`
	FailCount    int    `json:"fail_count"`
	LastError    string `json:"last_error,omitempty"`
}

// LeaseLogsResponse represents the response for container logs.
type LeaseLogsResponse struct {
	LeaseUUID    string            `json:"lease_uuid"`
	Tenant       string            `json:"tenant"`
	ProviderUUID string            `json:"provider_uuid"`
	Logs         map[string]string `json:"logs"`
}

// GetLeaseProvision handles GET /v1/leases/{lease_uuid}/provision
func (h *Handlers) GetLeaseProvision(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, false, false)
	if !ok {
		return
	}

	info, err := backendClient.GetProvision(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "provision not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to get provision from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	response := LeaseProvisionResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Token.Tenant,
		ProviderUUID: h.providerUUID,
		Status:       string(info.Status),
		FailCount:    info.FailCount,
		LastError:    info.LastError,
	}

	slog.Info("lease provision info served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"status", info.Status,
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// GetLeaseLogs handles GET /v1/leases/{lease_uuid}/logs
func (h *Handlers) GetLeaseLogs(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, false, false)
	if !ok {
		return
	}

	// Parse tail parameter
	tail := 100 // default
	if v := r.URL.Query().Get("tail"); v != "" {
		n, parseErr := strconv.Atoi(v)
		if parseErr != nil || n < 1 {
			writeError(w, "tail must be a positive integer", http.StatusBadRequest)
			return
		}
		if n > 10000 {
			writeError(w, "tail must not exceed 10000", http.StatusBadRequest)
			return
		}
		tail = n
	}

	logs, err := backendClient.GetLogs(r.Context(), leaseUUID, tail)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "logs not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to get logs from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	response := LeaseLogsResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Token.Tenant,
		ProviderUUID: h.providerUUID,
		Logs:         logs,
	}

	slog.Info("lease logs served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// LeaseReleasesResponse represents the response for release history.
type LeaseReleasesResponse struct {
	LeaseUUID    string                `json:"lease_uuid"`
	Tenant       string                `json:"tenant"`
	ProviderUUID string                `json:"provider_uuid"`
	Releases     []backend.ReleaseInfo `json:"releases"`
}

// RestartLease handles POST /v1/leases/{lease_uuid}/restart
func (h *Handlers) RestartLease(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, true, true)
	if !ok {
		return
	}

	err := backendClient.Restart(r.Context(), backend.RestartRequest{
		LeaseUUID:   leaseUUID,
		CallbackURL: provisioner.BuildCallbackURL(h.callbackBaseURL),
	})
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			writeError(w, "invalid state for restart", http.StatusConflict)
			return
		}
		slog.Error("failed to restart lease", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	// Publish "restarting" event after the backend accepts the request.
	// This is safe: the backend transitions to Restarting synchronously and
	// spawns the async work in a goroutine, so the completion callback cannot
	// arrive before Restart() returns.
	if h.eventBroker != nil {
		h.eventBroker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusRestarting,
			Timestamp: time.Now(),
		})
	}

	slog.Info("lease restart initiated",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
	)

	writeJSON(w, map[string]string{"status": "restarting"}, http.StatusAccepted)
}

// UpdateLease handles POST /v1/leases/{lease_uuid}/update
func (h *Handlers) UpdateLease(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, true, true)
	if !ok {
		return
	}

	// Read the request body (new manifest payload)
	var updateReq struct {
		Payload []byte `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&updateReq); err != nil {
		writeError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if len(updateReq.Payload) == 0 {
		writeError(w, "payload is required", http.StatusBadRequest)
		return
	}

	err := backendClient.Update(r.Context(), backend.UpdateRequest{
		LeaseUUID:   leaseUUID,
		CallbackURL: provisioner.BuildCallbackURL(h.callbackBaseURL),
		Payload:     updateReq.Payload,
	})
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		if errors.Is(err, backend.ErrInvalidState) {
			writeError(w, "invalid state for update", http.StatusConflict)
			return
		}
		if errors.Is(err, backend.ErrValidation) {
			writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		slog.Error("failed to update lease", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	// Publish "updating" event after the backend accepts the request.
	// This is safe: the backend transitions to Updating synchronously and
	// spawns the async work in a goroutine, so the completion callback cannot
	// arrive before Update() returns.
	if h.eventBroker != nil {
		h.eventBroker.Publish(backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    backend.ProvisionStatusUpdating,
			Timestamp: time.Now(),
		})
	}

	slog.Info("lease update initiated",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"backend", backendClient.Name(),
		"payload_size", len(updateReq.Payload),
	)

	writeJSON(w, map[string]string{"status": "updating"}, http.StatusAccepted)
}

// GetLeaseReleases handles GET /v1/leases/{lease_uuid}/releases
func (h *Handlers) GetLeaseReleases(w http.ResponseWriter, r *http.Request) {
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, false, false)
	if !ok {
		return
	}

	releases, err := backendClient.GetReleases(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		slog.Error("failed to get releases from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, errMsgInternalServerError, http.StatusInternalServerError)
		return
	}

	response := LeaseReleasesResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       auth.Token.Tenant,
		ProviderUUID: h.providerUUID,
		Releases:     releases,
	}

	slog.Info("lease releases served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"release_count", len(releases),
		"backend", backendClient.Name(),
	)

	writeJSON(w, response, http.StatusOK)
}

// HealthStats contains operational statistics for the health response.
type HealthStats struct {
	InFlightProvisions int `json:"in_flight_provisions"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status       string                  `json:"status"`
	ProviderUUID string                  `json:"provider_uuid"`
	Checks       map[string]*CheckResult `json:"checks"`
	Stats        *HealthStats            `json:"stats,omitempty"`
}

// CheckResult represents the result of a single health check.
type CheckResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// HealthCheck handles GET /health
func (h *Handlers) HealthCheck(w http.ResponseWriter, r *http.Request) {
	checks := make(map[string]*CheckResult)
	overallHealthy := true

	// Check chain connectivity
	if h.client != nil {
		if err := h.client.Ping(r.Context()); err != nil {
			slog.Warn("health check: chain unhealthy", "error", err)
			checks["chain"] = &CheckResult{
				Status:  "unhealthy",
				Message: "chain connectivity failed",
			}
			overallHealthy = false
		} else {
			checks["chain"] = &CheckResult{
				Status: "healthy",
			}
		}
	}

	// Check all backends
	if h.backendRouter != nil {
		backendResults, backendsHealthy := h.backendRouter.HealthCheck(r.Context())
		for _, result := range backendResults {
			checkKey := "backend:" + result.Name
			if result.Healthy {
				checks[checkKey] = &CheckResult{
					Status: "healthy",
				}
			} else {
				slog.Warn("health check: backend unhealthy", "backend", result.Name, "error", result.Error)
				checks[checkKey] = &CheckResult{
					Status:  "unhealthy",
					Message: "backend health check failed",
				}
			}
		}
		if !backendsHealthy {
			overallHealthy = false
		}
	}

	// Check token tracker (bbolt database)
	if h.tokenTracker != nil {
		if err := h.tokenTracker.Healthy(); err != nil {
			slog.Warn("health check: token tracker unhealthy", "error", err)
			checks["token_tracker"] = &CheckResult{
				Status:  "unhealthy",
				Message: "token tracker unavailable",
			}
			overallHealthy = false
		} else {
			checks["token_tracker"] = &CheckResult{
				Status: "healthy",
			}
		}
	}

	// Check placement store (bbolt database)
	if h.placementLookup != nil {
		if err := h.placementLookup.Healthy(); err != nil {
			slog.Warn("health check: placement store unhealthy", "error", err)
			checks["placement_store"] = &CheckResult{
				Status:  "unhealthy",
				Message: "placement store unavailable",
			}
			overallHealthy = false
		} else {
			checks["placement_store"] = &CheckResult{
				Status: "healthy",
			}
		}
	}

	status := "healthy"
	httpStatus := http.StatusOK
	if !overallHealthy {
		status = "unhealthy"
		httpStatus = http.StatusServiceUnavailable
	}

	response := HealthResponse{
		Status:       status,
		ProviderUUID: h.providerUUID,
		Checks:       checks,
	}

	if h.statusChecker != nil {
		response.Stats = &HealthStats{
			InFlightProvisions: h.statusChecker.InFlightCount(),
		}
	}

	writeJSON(w, response, httpStatus)
}

// WorkloadLookupResponse is the response from the GET /workloads endpoint.
// Workloads is a map keyed by lease_uuid so callers can join by UUID without
// building a client-side index. Unknown leases are absent from the map.
// Warnings is non-nil and may be empty (initialized as []string{} for stable
// JSON serialization as `[]` rather than `null`).
type WorkloadLookupResponse struct {
	Workloads map[string]WorkloadEntry `json:"workloads"`
	Warnings  []string                 `json:"warnings"`
}

// WorkloadEntry describes a single lease's workload for observability.
// LeaseUUID is intentionally absent — the map key in WorkloadLookupResponse
// carries it.
type WorkloadEntry struct {
	Status      backend.ProvisionStatus `json:"status"`
	CreatedAt   time.Time               `json:"created_at"`
	BackendName string                  `json:"backend_name"`
	Items       []WorkloadItem          `json:"items"`
}

// WorkloadItem describes a single SKU+image within a workload.
type WorkloadItem struct {
	ServiceName string `json:"service_name,omitempty"`
	SKU         string `json:"sku"`
	Image       string `json:"image,omitempty"`
	Count       int    `json:"count"`
}

// GetWorkloads handles GET /workloads?lease_uuid=<u1>&lease_uuid=<u2>...
//
// Returns workload metadata for the requested lease UUIDs from any backend
// that knows about them. Caller is the manifest-admin SPA (cross-origin via
// CORS); the visible page of leases on the admin's leases pages drives the
// UUID list. The 1..MaxLookupUUIDs cap matches the admin's PAGE_SIZE (25)
// with headroom.
//
// Per-backend errors are surfaced via the Warnings slice rather than failing
// the whole request, so a single broken backend doesn't blank the admin's
// image column. This is a deliberate divergence from reconciler.go's
// fetchAllProvisions, which aborts on any backend error to avoid mistaking
// a transient failure for "lease no longer exists" — that concern doesn't
// apply here since /workloads is read-only metadata for display.
func (h *Handlers) GetWorkloads(w http.ResponseWriter, r *http.Request) {
	uuids := r.URL.Query()["lease_uuid"]
	if len(uuids) == 0 {
		writeError(w, "lease_uuid query parameter required", http.StatusBadRequest)
		return
	}
	if len(uuids) > backend.MaxLookupUUIDs {
		writeError(w, fmt.Sprintf("too many lease_uuids (max %d)", backend.MaxLookupUUIDs), http.StatusBadRequest)
		return
	}
	for _, u := range uuids {
		if !config.IsValidUUID(u) {
			writeError(w, "invalid lease_uuid", http.StatusBadRequest)
			return
		}
	}

	// Initialize with non-nil zero values so JSON serialization is `{}` and `[]`
	// rather than `null` even when no backend returns anything.
	merged := make(map[string]WorkloadEntry)
	warnings := []string{}
	var mu sync.Mutex

	if h.backendRouter != nil {
		backends := h.backendRouter.Backends()
		g, gctx := errgroup.WithContext(r.Context())
		for _, b := range backends {
			g.Go(func() error {
				provisions, err := b.LookupProvisions(gctx, uuids)
				if err != nil {
					// If the parent context was canceled (client went away), gctx.Err()
					// is non-nil and the error is the propagation of that cancel.
					// Skip logging — the post-Wait check below discards the response.
					// We check gctx.Err() rather than errors.Is(err, context.Canceled)
					// because the backend HTTP client has its own Timeout, which also
					// produces a context.DeadlineExceeded that we DO want to surface
					// as a backend failure.
					if gctx.Err() != nil {
						return nil
					}
					slog.Error("workloads: backend LookupProvisions failed",
						"backend", b.Name(), "error", err)
					mu.Lock()
					warnings = append(warnings, fmt.Sprintf("backend %q unavailable", b.Name()))
					mu.Unlock()
					// Return nil so a single backend failure doesn't cancel sibling
					// fetches via gctx (same convention as fetchAllProvisions in
					// internal/provisioner/reconciler.go).
					return nil
				}

				mu.Lock()
				defer mu.Unlock()
				for _, p := range provisions {
					if existing, dup := merged[p.LeaseUUID]; dup {
						// Chain enforces single-provider-per-lease, so a duplicate
						// across backends is an invariant violation. Keep the first
						// entry and log; we don't surface this in the response.
						slog.Warn("workloads: lease reported by multiple backends",
							"lease_uuid", p.LeaseUUID,
							"first_backend", existing.BackendName,
							"duplicate_backend", b.Name(),
						)
						continue
					}
					merged[p.LeaseUUID] = provisionToWorkloadEntry(p, b.Name())
				}
				return nil
			})
		}
		// Goroutines always return nil (errors are collected into `warnings`),
		// so g.Wait() is expected to return nil. Defensively log if a future
		// change ever returns an error from a goroutine.
		if err := g.Wait(); err != nil {
			slog.Error("workloads: errgroup returned unexpected error", "error", err)
		}
	}

	// If the client canceled mid-fan-out, the partial response would silently
	// mask the cancellation. Bail without writing a body — the Go HTTP server
	// handles the dead connection. (Reconciler.fetchAllProvisions doesn't do
	// this check because it's not user-facing; we add it here so admin doesn't
	// see partial data on a flaky connection.)
	if err := r.Context().Err(); err != nil {
		return
	}

	writeJSON(w, WorkloadLookupResponse{
		Workloads: merged,
		Warnings:  warnings,
	}, http.StatusOK)
}

// provisionToWorkloadEntry converts a ProvisionInfo into a WorkloadEntry.
// Stack leases produce one WorkloadItem per service; non-stack leases produce
// a single item with the top-level SKU/Image/Quantity.
func provisionToWorkloadEntry(p backend.ProvisionInfo, backendName string) WorkloadEntry {
	entry := WorkloadEntry{
		Status:      p.Status,
		CreatedAt:   p.CreatedAt,
		BackendName: backendName,
	}
	if len(p.Items) > 0 {
		entry.Items = make([]WorkloadItem, len(p.Items))
		for i, item := range p.Items {
			entry.Items[i] = WorkloadItem{
				ServiceName: item.ServiceName,
				SKU:         item.SKU,
				Image:       p.ServiceImages[item.ServiceName],
				Count:       item.Quantity,
			}
		}
	} else {
		entry.Items = []WorkloadItem{{
			SKU:   p.SKU,
			Image: p.Image,
			Count: p.Quantity,
		}}
	}
	return entry
}

// verifyLeaseAccess queries a lease from chain and verifies tenant and provider ownership.
func verifyLeaseAccess(ctx context.Context, client ChainClient, providerUUID, leaseUUID, tenant string, requireActive bool) (*billingtypes.Lease, int, error) {
	var lease *billingtypes.Lease
	var err error
	if requireActive {
		lease, err = client.GetActiveLease(ctx, leaseUUID)
	} else {
		lease, err = client.GetLease(ctx, leaseUUID)
	}
	if err != nil {
		slog.Error("failed to query lease", "error", err, "lease_uuid", leaseUUID)
		return nil, http.StatusInternalServerError, errors.New(errMsgInternalServerError)
	}
	if lease == nil {
		if requireActive {
			return nil, http.StatusNotFound, errors.New(errMsgLeaseNotFound + " or not active")
		}
		return nil, http.StatusNotFound, errors.New(errMsgLeaseNotFound)
	}
	if lease.Tenant != tenant {
		slog.Warn("tenant mismatch", "token_tenant", tenant, "lease_tenant", lease.Tenant)
		return nil, http.StatusForbidden, errors.New(errMsgForbidden)
	}
	if lease.ProviderUuid != providerUUID {
		slog.Warn("provider UUID mismatch", "lease_provider_uuid", lease.ProviderUuid, "our_provider_uuid", providerUUID)
		return nil, http.StatusForbidden, errors.New(errMsgForbidden)
	}
	return lease, http.StatusOK, nil
}

// extractToken extracts and parses the bearer token from the Authorization header.
func (h *Handlers) extractToken(r *http.Request) (*AuthToken, error) {
	tokenStr, err := extractBearerToken(r)
	if err != nil {
		return nil, err
	}
	return ParseAuthToken(tokenStr)
}

// writeJSON writes a JSON response.
// Pre-encodes to buffer to catch encoding errors before writing headers.
func writeJSON(w http.ResponseWriter, data any, status int) {
	// Encode first to catch errors before writing headers
	encoded, err := json.Marshal(data)
	if err != nil {
		slog.Error("failed to encode response", "error", err)
		http.Error(w, `{"error":"internal encoding error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
	_, _ = w.Write([]byte("\n")) // Match json.Encoder behavior (adds newline)
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, message string, status int) {
	response := ErrorResponse{
		Error: message,
		Code:  status,
	}
	writeJSON(w, response, status)
}

// convertPortBinding converts a backend PortBinding (string host_port) to an API PortMapping (int host_port).
func convertPortBinding(b backend.PortBinding) PortMapping {
	var port int
	if b.HostPort != "" {
		port, _ = strconv.Atoi(b.HostPort)
	}
	return PortMapping{HostIP: b.HostIP, HostPort: port}
}

// convertInstance converts a backend LeaseInstance to an API InstanceInfo.
func convertInstance(inst backend.LeaseInstance) InstanceInfo {
	ii := InstanceInfo{
		InstanceIndex: inst.InstanceIndex,
		ContainerID:   inst.ContainerID,
		Image:         inst.Image,
		Status:        inst.Status,
		FQDN:          inst.FQDN,
	}
	if len(inst.Ports) > 0 {
		ii.Ports = make(map[string]PortMapping, len(inst.Ports))
		for k, v := range inst.Ports {
			ii.Ports[k] = convertPortBinding(v)
		}
	}
	return ii
}

// extractConnectionDetails converts a backend LeaseInfo into API ConnectionDetails.
func extractConnectionDetails(info backend.LeaseInfo) ConnectionDetails {
	details := ConnectionDetails{
		Host:     info.Host,
		FQDN:     info.FQDN,
		Protocol: info.Protocol,
		Metadata: info.Metadata,
	}
	if details.Metadata == nil {
		details.Metadata = make(map[string]string)
	}

	// Convert top-level ports.
	if len(info.Ports) > 0 {
		details.Ports = make(map[string]PortMapping, len(info.Ports))
		for k, v := range info.Ports {
			details.Ports[k] = convertPortBinding(v)
		}
	}

	// Convert flat instances.
	for _, inst := range info.Instances {
		details.Instances = append(details.Instances, convertInstance(inst))
	}

	// Propagate FQDN from the first instance to the top level.
	if details.FQDN == "" && len(details.Instances) > 0 {
		details.FQDN = details.Instances[0].FQDN
	}

	// Convert services (stack leases).
	if len(info.Services) > 0 {
		details.Services = make(map[string]ServiceConnectionDetails, len(info.Services))
		for name, svc := range info.Services {
			svcDetails := ServiceConnectionDetails{FQDN: svc.FQDN}
			for _, inst := range svc.Instances {
				svcDetails.Instances = append(svcDetails.Instances, convertInstance(inst))
			}
			if svcDetails.FQDN == "" && len(svcDetails.Instances) > 0 {
				svcDetails.FQDN = svcDetails.Instances[0].FQDN
			}
			details.Services[name] = svcDetails
		}
	}

	return details
}

// Sentinel errors for authentication (unexported - internal to package)
var (
	errMissingAuth       = errors.New("missing authorization header")
	errInvalidAuthFormat = errors.New("invalid authorization format, expected 'Bearer <token>'")
)

// WebSocket subscription tunables for StreamLeaseEvents. Production values
// are conservative for an event-only stream where the client never sends
// application data. Tests override the per-Handlers fields (either through
// newTestHandlers or by direct assignment after construction); these consts
// are the production defaults wired in by NewHandlers.
const (
	// wsDefaultMaxMessageSize bounds the size of any message the server will
	// read from a client (gorilla's SetReadLimit caps the assembled message
	// size, summed across fragments). The lease events stream is push-only —
	// clients only ever send tiny control frames (close, ping/pong) — so a
	// small limit is sufficient and prevents memory exhaustion via oversized
	// messages. Matches the gorilla/websocket chat example.
	wsDefaultMaxMessageSize int64 = 512

	// wsDefaultMaxConnLifetime caps the total lifetime of a single
	// subscription. On expiry the server sends a clean close frame and the
	// client must reconnect (and re-authenticate). This prevents a single
	// tenant from holding a slot in the per-lease subscription pool
	// indefinitely.
	wsDefaultMaxConnLifetime = time.Hour
)

// StreamLeaseEvents serves a WebSocket stream of lease status events.
// GET /v1/leases/{lease_uuid}/events
func (h *Handlers) StreamLeaseEvents(w http.ResponseWriter, r *http.Request) {
	if h.eventBroker == nil {
		writeError(w, "events not enabled", http.StatusNotImplemented)
		return
	}

	leaseUUID := r.PathValue("lease_uuid")

	// Token promotion and stripping is handled by wsTokenPromoter middleware.

	// Authenticate BEFORE upgrading so auth failures return normal HTTP errors.
	_, statusCode, err := h.AuthenticateLeaseRequest(r, leaseUUID, false, false)
	if err != nil {
		writeError(w, err.Error(), statusCode)
		return
	}

	conn, err := h.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "lease_uuid", leaseUUID, "error", err)
		return // Upgrade writes its own HTTP error response.
	}
	defer func() { _ = conn.Close() }()

	// Bound the size of messages the server will read. Per the application
	// contract this endpoint is server-push only, so any client message
	// larger than the limit indicates a buggy or malicious client and must
	// not be allowed to allocate memory.
	//
	// Defensive fallback: gorilla treats SetReadLimit(0) as "no limit", so
	// a zero-valued field (e.g., from a Handlers literal that bypassed
	// NewHandlers) would silently disable the protection. Treat anything
	// non-positive as misconfiguration: log loudly and fall back to the
	// production default rather than serve traffic without the cap.
	maxMessageSize := h.wsMaxMessageSize
	if maxMessageSize <= 0 {
		slog.Error("websocket read limit misconfigured, falling back to default",
			"configured", maxMessageSize,
			"default", wsDefaultMaxMessageSize,
		)
		maxMessageSize = wsDefaultMaxMessageSize
	}
	conn.SetReadLimit(maxMessageSize)

	ch, subErr := h.eventBroker.Subscribe(leaseUUID)
	if subErr != nil {
		slog.Warn("subscription rejected", "lease_uuid", leaseUUID, "error", subErr)
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "too many connections"))
		return
	}
	if ch == nil {
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseGoingAway, "broker closed"))
		return
	}
	defer h.eventBroker.Unsubscribe(leaseUUID, ch)

	const (
		pingInterval = 30 * time.Second
		writeWait    = 10 * time.Second
		pongWait     = 40 * time.Second
	)

	// Read pump. gorilla handles ping/pong control frames silently and
	// surfaces a peer-initiated close as a *websocket.CloseError from
	// ReadMessage — neither path appears as a successful data-message read.
	// On this push-only endpoint a successful ReadMessage therefore means
	// the client violated the application contract by sending application
	// data; close with 1008 ClosePolicyViolation to prevent CPU/slot
	// exhaustion via small-message spam. An error means one of three things:
	// oversized message (ErrReadLimit — logged; gorilla also auto-sends a
	// 1009 frame back), a benign disconnect (peer close, read deadline
	// expiry, TCP drop — silently ignored), or a protocol violation / other
	// abuse signal (logged).
	closeCh := make(chan struct{})
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	go func() {
		defer close(closeCh)
		_, _, err := conn.ReadMessage()
		if err != nil {
			switch {
			case errors.Is(err, websocket.ErrReadLimit):
				slog.Warn("websocket oversized message from client",
					"lease_uuid", leaseUUID,
					"remote_addr", r.RemoteAddr,
					"limit", maxMessageSize,
				)
			case isBenignReadError(err):
				// Normal disconnect — silent.
			default:
				// Protocol violation or unexpected error; log as abuse
				// signal so weird client behavior is visible in operator
				// logs rather than vanishing into the benign bucket.
				slog.Warn("websocket read error from client",
					"lease_uuid", leaseUUID,
					"remote_addr", r.RemoteAddr,
					"error", err,
				)
			}
			return
		}
		// Successful read = unexpected client data message. Per gorilla's
		// docs WriteControl is concurrent-safe with the main goroutine's
		// writes, so it's OK to send the close frame from here.
		slog.Warn("websocket unexpected client message",
			"lease_uuid", leaseUUID,
			"remote_addr", r.RemoteAddr,
		)
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "client messages not allowed"),
			time.Now().Add(writeWait))
	}()

	// Write pump: send events + ping frames.
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	// Cap the total connection lifetime so a single tenant cannot hold a
	// per-lease subscription slot indefinitely. On expiry we send a clean
	// close frame; the client is expected to reconnect with a fresh token.
	// CloseTryAgainLater (1013) tells the client this is a transient,
	// server-initiated rotation — distinct from CloseGoingAway used for
	// broker shutdown below — so reconnect logic can be unconditional.
	//
	// Defensive fallback (same shape as the read-limit guard above):
	// time.NewTimer(0) — or any non-positive duration — fires immediately,
	// which would close every /events connection right after subscribe.
	// Treat <= 0 as misconfiguration and fall back to the production
	// default rather than introduce a "disable the cap" knob.
	maxLifetime := h.wsMaxConnLifetime
	if maxLifetime <= 0 {
		slog.Error("websocket lifetime misconfigured, falling back to default",
			"configured", maxLifetime,
			"default", wsDefaultMaxConnLifetime,
		)
		maxLifetime = wsDefaultMaxConnLifetime
	}
	lifetimeTimer := time.NewTimer(maxLifetime)
	defer lifetimeTimer.Stop()

	for {
		select {
		case <-closeCh:
			return
		case <-lifetimeTimer.C:
			slog.Info("websocket max lifetime reached", "lease_uuid", leaseUUID)
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "max connection lifetime reached"),
				time.Now().Add(writeWait))
			return
		case event, ok := <-ch:
			if !ok {
				// Broker closed — send clean close frame.
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseGoingAway, ""),
					time.Now().Add(writeWait))
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteJSON(event); err != nil {
				slog.Debug("websocket write failed", "lease_uuid", leaseUUID, "error", err)
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Debug("websocket ping failed", "lease_uuid", leaseUUID, "error", err)
				return
			}
		}
	}
}

// isBenignReadError returns true for error categories that represent a
// normal client disconnect on an /events WebSocket: a peer-initiated close
// (any code), any net.Error (timeout, "use of closed network connection",
// etc.), or a plain EOF. Everything else — including gorilla's plain
// `errors.New("websocket: ...")` protocol-violation errors (bad opcode,
// bad mask, invalid utf8 in close frame, continuation after FIN, etc.) —
// is treated as a potential abuse signal by the caller and logged.
func isBenignReadError(err error) bool {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// extractBearerToken extracts the raw token string from the Authorization header.
// Expects "Bearer <token>" format. Returns errMissingAuth if no header is present.
func extractBearerToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", errMissingAuth
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return "", errInvalidAuthFormat
	}
	return parts[1], nil
}
