package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
)

// ChainClient defines the chain operations needed by handlers.
type ChainClient interface {
	GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	Ping(ctx context.Context) error
}

// Handlers contains HTTP request handlers.
type Handlers struct {
	client        ChainClient
	backendRouter *backend.Router
	providerUUID  string
	bech32Prefix  string
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(client *chain.Client, backendRouter *backend.Router, providerUUID, bech32Prefix string) *Handlers {
	return &Handlers{
		client:        client,
		backendRouter: backendRouter,
		providerUUID:  providerUUID,
		bech32Prefix:  bech32Prefix,
	}
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

// GetLeaseConnection handles GET /v1/leases/{lease_uuid}/connection
func (h *Handlers) GetLeaseConnection(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	leaseUUID := vars["lease_uuid"]

	// Validate lease UUID format
	if !config.IsValidUUID(leaseUUID) {
		slog.Warn("invalid lease UUID format", "lease_uuid", leaseUUID)
		writeError(w, "invalid lease UUID format", http.StatusBadRequest)
		return
	}

	// Extract and validate bearer token
	token, err := h.extractToken(r)
	if err != nil {
		slog.Warn("invalid authorization", "error", err)
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Validate the token
	if err := token.Validate(h.bech32Prefix); err != nil {
		slog.Warn("token validation failed", "error", err)
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Verify the token's lease UUID matches the request
	if token.LeaseUUID != leaseUUID {
		slog.Warn("lease UUID mismatch",
			"token_lease_uuid", token.LeaseUUID,
			"request_lease_uuid", leaseUUID,
		)
		writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Query the lease from chain to verify it's active and tenant matches
	lease, err := h.client.GetActiveLease(r.Context(), leaseUUID)
	if err != nil {
		slog.Error("failed to query lease", "error", err, "lease_uuid", leaseUUID)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if lease == nil {
		slog.Warn("lease not found or not active", "lease_uuid", leaseUUID)
		writeError(w, "lease not found or not active", http.StatusNotFound)
		return
	}

	// Verify the tenant matches
	if lease.Tenant != token.Tenant {
		slog.Warn("tenant mismatch",
			"token_tenant", token.Tenant,
			"lease_tenant", lease.Tenant,
		)
		writeError(w, "forbidden", http.StatusForbidden)
		return
	}

	// Verify the provider UUID matches
	if lease.ProviderUuid != h.providerUUID {
		slog.Warn("provider UUID mismatch",
			"lease_provider_uuid", lease.ProviderUuid,
			"our_provider_uuid", h.providerUUID,
		)
		writeError(w, "forbidden", http.StatusForbidden)
		return
	}

	// Check if backend router is configured
	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, "service not configured", http.StatusServiceUnavailable)
		return
	}

	// Get lease info from backend
	// Route by SKU (Phase 2: use lease.Sku when available)
	backendClient := h.backendRouter.Default()
	info, err := backendClient.GetInfo(r.Context(), leaseUUID)
	if err != nil {
		if errors.Is(err, backend.ErrNotProvisioned) {
			slog.Warn("lease not yet provisioned", "lease_uuid", leaseUUID)
			writeError(w, "lease not yet provisioned", http.StatusNotFound)
			return
		}
		slog.Error("failed to get info from backend", "error", err, "lease_uuid", leaseUUID)
		writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Build response with lease info from backend
	response := ConnectionResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       lease.Tenant,
		ProviderUUID: h.providerUUID,
		Connection:   extractConnectionDetails(*info),
	}

	slog.Info("lease info served",
		"lease_uuid", leaseUUID,
		"tenant", token.Tenant,
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
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, errMissingAuth
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return nil, errInvalidAuthFormat
	}

	return ParseAuthToken(parts[1])
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

// decodeJSON decodes a JSON request body.
func decodeJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

// extractConnectionDetails extracts ConnectionDetails from a backend LeaseInfo map.
// Unknown fields are placed in Metadata.
func extractConnectionDetails(info backend.LeaseInfo) ConnectionDetails {
	details := ConnectionDetails{
		Metadata: make(map[string]string),
	}

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
	if metadata, ok := info["metadata"].(map[string]string); ok {
		details.Metadata = metadata
	} else if metadata, ok := info["metadata"].(map[string]any); ok {
		for k, v := range metadata {
			if s, ok := v.(string); ok {
				details.Metadata[k] = s
			}
		}
	}

	return details
}

// Sentinel errors for authentication (unexported - internal to package)
var (
	errMissingAuth       = errors.New("missing authorization header")
	errInvalidAuthFormat = errors.New("invalid authorization format, expected 'Bearer <token>'")
)
