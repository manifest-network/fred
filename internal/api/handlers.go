package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
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

// ProvisioningStatusChecker provides status information about provisioning.
type ProvisioningStatusChecker interface {
	HasPayload(leaseUUID string) bool
	IsInFlight(leaseUUID string) bool
}

// Handlers contains HTTP request handlers.
type Handlers struct {
	client        ChainClient
	backendRouter *backend.Router
	tokenTracker  *TokenTracker
	statusChecker ProvisioningStatusChecker
	providerUUID  string
	bech32Prefix  string
}

// NewHandlers creates a new Handlers instance.
// tokenTracker is optional but recommended for replay attack protection.
// statusChecker is optional but required for the /status endpoint.
func NewHandlers(client ChainClient, backendRouter *backend.Router, tokenTracker *TokenTracker, statusChecker ProvisioningStatusChecker, providerUUID, bech32Prefix string) *Handlers {
	return &Handlers{
		client:        client,
		backendRouter: backendRouter,
		tokenTracker:  tokenTracker,
		statusChecker: statusChecker,
		providerUUID:  providerUUID,
		bech32Prefix:  bech32Prefix,
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
			// Database error - log but don't block the request
			slog.Error("token tracker error", "error", err)
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

	// Query the lease from chain
	var lease *billingtypes.Lease
	if requireActive {
		lease, err = h.client.GetActiveLease(r.Context(), leaseUUID)
	} else {
		lease, err = h.client.GetLease(r.Context(), leaseUUID)
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

	// Verify the tenant matches
	if lease.Tenant != token.Tenant {
		slog.Warn("tenant mismatch",
			"token_tenant", token.Tenant,
			"lease_tenant", lease.Tenant,
		)
		return nil, http.StatusForbidden, errors.New(errMsgForbidden)
	}

	// Verify the provider UUID matches
	if lease.ProviderUuid != h.providerUUID {
		slog.Warn("provider UUID mismatch",
			"lease_provider_uuid", lease.ProviderUuid,
			"our_provider_uuid", h.providerUUID,
		)
		return nil, http.StatusForbidden, errors.New(errMsgForbidden)
	}

	return &AuthenticatedRequest{
		Token: token,
		Lease: lease,
	}, http.StatusOK, nil
}

// ConnectionResponse represents the response for connection details.
type ConnectionResponse struct {
	LeaseUUID    string            `json:"lease_uuid"`
	Tenant       string            `json:"tenant"`
	ProviderUUID string            `json:"provider_uuid"`
	Connection   ConnectionDetails `json:"connection"`
}

// ConnectionDetails contains the connection information for a lease.
type ConnectionDetails struct {
	Host     string            `json:"host"`
	Port     int               `json:"port"`
	Protocol string            `json:"protocol"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

// Common error messages for API responses.
// These constants ensure consistency across handlers and simplify testing.
const (
	errMsgUnauthorized         = "unauthorized"
	errMsgForbidden            = "forbidden"
	errMsgInternalServerError  = "internal server error"
	errMsgServiceNotConfigured = "service not configured"
	errMsgInvalidLeaseUUID     = "invalid lease UUID format"
	errMsgLeaseNotFound        = "lease not found"
)

// GetLeaseConnection handles GET /v1/leases/{lease_uuid}/connection
func (h *Handlers) GetLeaseConnection(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	leaseUUID := vars["lease_uuid"]

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
	sku := provisioner.ExtractPrimarySKU(auth.Lease)

	// Route to appropriate backend based on SKU (Route already falls back to default)
	backendClient := h.backendRouter.Route(sku)
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
type LeaseStatusResponse struct {
	LeaseUUID           string `json:"lease_uuid"`
	State               string `json:"state"`
	RequiresPayload     bool   `json:"requires_payload"`
	MetaHashHex         string `json:"meta_hash_hex,omitempty"` // For debugging - shows the expected payload hash
	PayloadReceived     bool   `json:"payload_received"`
	ProvisioningStarted bool   `json:"provisioning_started"`
}

// GetLeaseStatus handles GET /v1/leases/{lease_uuid}/status
func (h *Handlers) GetLeaseStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	leaseUUID := vars["lease_uuid"]

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
		State:           auth.Lease.State.String(),
		RequiresPayload: hasMetaHash,
	}
	if hasMetaHash {
		response.MetaHashHex = hex.EncodeToString(auth.Lease.MetaHash)
	}

	// Check provisioning status if checker is available
	if h.statusChecker != nil {
		response.PayloadReceived = h.statusChecker.HasPayload(leaseUUID)
		response.ProvisioningStarted = h.statusChecker.IsInFlight(leaseUUID)
	}

	slog.Info("lease status served",
		"lease_uuid", leaseUUID,
		"tenant", auth.Token.Tenant,
		"state", response.State,
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
			checks["chain"] = &CheckResult{
				Status:  "unhealthy",
				Message: err.Error(),
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
				checks[checkKey] = &CheckResult{
					Status:  "unhealthy",
					Message: result.Error,
				}
			}
		}
		if !backendsHealthy {
			overallHealthy = false
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

// extractToken extracts and parses the bearer token from the Authorization header.
func (h *Handlers) extractToken(r *http.Request) (*AuthToken, error) {
	tokenStr, err := extractBearerToken(r)
	if err != nil {
		return nil, err
	}
	return ParseAuthToken(tokenStr)
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// writeError writes an error response.
func writeError(w http.ResponseWriter, message string, status int) {
	response := ErrorResponse{
		Error: message,
		Code:  status,
	}
	writeJSON(w, response, status)
}

// decodeJSONBytes decodes JSON from a byte slice.
func decodeJSONBytes(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// extractConnectionDetails extracts ConnectionDetails from a backend LeaseInfo map.
// Known fields (host, port, protocol, metadata) are mapped to struct fields.
// Unknown top-level string fields are placed in Metadata.
func extractConnectionDetails(info backend.LeaseInfo) ConnectionDetails {
	details := ConnectionDetails{
		Metadata: make(map[string]string),
	}

	// Known fields that map to struct fields
	knownFields := map[string]bool{
		"host":     true,
		"port":     true,
		"protocol": true,
		"metadata": true,
	}

	// Extract known fields
	if host, ok := info["host"].(string); ok {
		details.Host = host
	}
	if port, ok := info["port"].(float64); ok {
		details.Port = int(port)
	} else if port, ok := info["port"].(int); ok {
		details.Port = port
	}
	if protocol, ok := info["protocol"].(string); ok {
		details.Protocol = protocol
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
