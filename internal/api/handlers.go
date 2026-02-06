package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

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
	client          ChainClient
	backendRouter   *backend.Router
	tokenTracker    TokenTrackerInterface
	statusChecker   StatusChecker
	placementLookup PlacementLookup
	providerUUID    string
	bech32Prefix    string
}

// NewHandlers creates a new Handlers instance.
// tokenTracker is optional but recommended for replay attack protection.
// statusChecker is optional but required for the /status endpoint.
// placementLookup is optional — used for routing reads to the correct backend.
func NewHandlers(client ChainClient, backendRouter *backend.Router, tokenTracker TokenTrackerInterface, statusChecker StatusChecker, placementLookup PlacementLookup, providerUUID, bech32Prefix string) *Handlers {
	return &Handlers{
		client:          client,
		backendRouter:   backendRouter,
		tokenTracker:    tokenTracker,
		statusChecker:   statusChecker,
		placementLookup: placementLookup,
		providerUUID:    providerUUID,
		bech32Prefix:    bech32Prefix,
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

	// Extract and validate bearer token
	token, err := h.extractToken(r)
	if err != nil {
		return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
	}

	// Validate the token
	if err := token.Validate(h.bech32Prefix); err != nil {
		return nil, http.StatusUnauthorized, errors.New(errMsgUnauthorized)
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
type ConnectionDetails struct {
	Host      string                 `json:"host"`
	Ports     map[string]PortMapping `json:"ports,omitempty"`
	Instances []InstanceInfo         `json:"instances,omitempty"`
	Protocol  string                 `json:"protocol,omitempty"`
	Metadata  map[string]string      `json:"metadata,omitempty"`
}

// InstanceInfo contains connection details for a single instance in a multi-instance lease.
type InstanceInfo struct {
	InstanceIndex int                    `json:"instance_index"`
	ContainerID   string                 `json:"container_id,omitempty"`
	Image         string                 `json:"image,omitempty"`
	Status        string                 `json:"status,omitempty"`
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
	leaseUUID := r.PathValue("lease_uuid")

	// Authenticate and authorize the request (requires active lease, checks replay)
	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, true, true)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	// Check if backend router is configured
	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, "service not configured", http.StatusServiceUnavailable)
		return
	}

	// Extract SKU for routing (use first item's SKU - all items share same provider)
	sku := provisioner.ExtractRoutingSKU(auth.Lease)

	// Resolve backend: placement first, then SKU routing
	backendClient := h.resolveBackend(leaseUUID, sku)
	if backendClient == nil {
		slog.Error("no backend available", "sku", sku, "lease_uuid", leaseUUID)
		writeError(w, "service unavailable", http.StatusServiceUnavailable)
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
	leaseUUID := r.PathValue("lease_uuid")

	// Authenticate and authorize (any lease state, no replay check for read endpoint)
	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, false, false)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return
	}

	sku := provisioner.ExtractRoutingSKU(auth.Lease)
	backendClient := h.resolveBackend(leaseUUID, sku)
	if backendClient == nil {
		slog.Error("no backend available", "sku", sku, "lease_uuid", leaseUUID)
		writeError(w, "service unavailable", http.StatusServiceUnavailable)
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
	leaseUUID := r.PathValue("lease_uuid")

	// Authenticate and authorize (any lease state, no replay check for read endpoint)
	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, false, false)
	if err != nil {
		writeError(w, err.Error(), status)
		return
	}

	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
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

	sku := provisioner.ExtractRoutingSKU(auth.Lease)
	backendClient := h.resolveBackend(leaseUUID, sku)
	if backendClient == nil {
		slog.Error("no backend available", "sku", sku, "lease_uuid", leaseUUID)
		writeError(w, "service unavailable", http.StatusServiceUnavailable)
		return
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

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status       string                  `json:"status"`
	ProviderUUID string                  `json:"provider_uuid"`
	Checks       map[string]*CheckResult `json:"checks"`
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

	writeJSON(w, response, httpStatus)
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

// extractPortMapping extracts a PortMapping from a binding map (JSON unmarshaled format).
// Handles both string and float64 representations of host_port.
func extractPortMapping(binding map[string]any) PortMapping {
	var hostPort int
	if hp, ok := binding["host_port"].(string); ok {
		if parsed, err := strconv.Atoi(hp); err == nil {
			hostPort = parsed
		}
	} else if hp, ok := binding["host_port"].(float64); ok {
		hostPort = int(hp)
	}
	hostIP, _ := binding["host_ip"].(string)
	return PortMapping{
		HostIP:   hostIP,
		HostPort: hostPort,
	}
}

// extractConnectionDetails extracts ConnectionDetails from a backend LeaseInfo map.
// Known fields (host, port, ports, protocol, metadata) are mapped to struct fields.
// Unknown top-level string fields are placed in Metadata.
func extractConnectionDetails(info backend.LeaseInfo) ConnectionDetails {
	details := ConnectionDetails{
		Metadata: make(map[string]string),
	}

	// Known fields that map to struct fields
	knownFields := map[string]bool{
		"host":      true,
		"ports":     true,
		"instances": true,
		"protocol":  true,
		"metadata":  true,
	}

	// Extract known fields
	if host, ok := info["host"].(string); ok {
		details.Host = host
	}
	if protocol, ok := info["protocol"].(string); ok {
		details.Protocol = protocol
	}

	// Extract ports map (from docker backend format)
	if ports, ok := info["ports"].(map[string]map[string]string); ok {
		details.Ports = make(map[string]PortMapping)
		for containerPort, binding := range ports {
			var hostPort int
			if hp, ok := binding["host_port"]; ok {
				if parsed, err := strconv.Atoi(hp); err == nil {
					hostPort = parsed
				}
			}
			details.Ports[containerPort] = PortMapping{
				HostIP:   binding["host_ip"],
				HostPort: hostPort,
			}
		}
	} else if ports, ok := info["ports"].(map[string]any); ok {
		// Handle JSON unmarshaled format (map[string]any)
		details.Ports = make(map[string]PortMapping)
		for containerPort, bindingAny := range ports {
			if binding, ok := bindingAny.(map[string]any); ok {
				details.Ports[containerPort] = extractPortMapping(binding)
			}
		}
	}

	// Extract instances array (for multi-container leases)
	if instances, ok := info["instances"].([]any); ok {
		for _, instAny := range instances {
			if inst, ok := instAny.(map[string]any); ok {
				instance := InstanceInfo{}

				// Extract instance index
				if idx, ok := inst["instance_index"].(float64); ok {
					instance.InstanceIndex = int(idx)
				}

				// Extract container ID
				if cid, ok := inst["container_id"].(string); ok {
					instance.ContainerID = cid
				}

				// Extract image
				if img, ok := inst["image"].(string); ok {
					instance.Image = img
				}

				// Extract status
				if status, ok := inst["status"].(string); ok {
					instance.Status = status
				}

				// Extract per-instance ports
				if ports, ok := inst["ports"].(map[string]any); ok {
					instance.Ports = make(map[string]PortMapping)
					for containerPort, bindingAny := range ports {
						if binding, ok := bindingAny.(map[string]any); ok {
							instance.Ports[containerPort] = extractPortMapping(binding)
						}
					}
				}

				details.Instances = append(details.Instances, instance)
			}
		}
	}

	// Extract explicit metadata field first
	if metadata, ok := info["metadata"].(map[string]string); ok {
		for k, v := range metadata {
			details.Metadata[k] = v
		}
	} else if metadata, ok := info["metadata"].(map[string]any); ok {
		for k, v := range metadata {
			if s, ok := v.(string); ok {
				details.Metadata[k] = s
			}
		}
	}

	// Add unknown top-level string fields to Metadata
	for k, v := range info {
		if knownFields[k] {
			continue
		}
		if s, ok := v.(string); ok {
			details.Metadata[k] = s
		}
	}

	return details
}

// Sentinel errors for authentication (unexported - internal to package)
var (
	errMissingAuth       = errors.New("missing authorization header")
	errInvalidAuthFormat = errors.New("invalid authorization format, expected 'Bearer <token>'")
)

// extractBearerToken extracts the raw token string from a Bearer authorization header.
// Returns the token string or an error if the header is missing or malformed.
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
