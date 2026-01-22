package api

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/config"
)

// Connection property lists for deterministic generation
var (
	hostnames = []string{
		"compute-alpha.example.com",
		"compute-beta.example.com",
		"compute-gamma.example.com",
		"compute-delta.example.com",
		"node-east.example.com",
		"node-west.example.com",
		"node-central.example.com",
		"worker-1.example.com",
		"worker-2.example.com",
		"worker-3.example.com",
	}

	ports = []int{8443, 9443, 10443, 11443, 12443, 8080, 9090, 3000, 5000, 6000}

	protocols = []string{"https", "grpc", "wss"}

	regions = []string{
		"us-east-1",
		"us-west-2",
		"eu-west-1",
		"eu-central-1",
		"ap-southeast-1",
		"ap-northeast-1",
	}

	tiers = []string{"standard", "premium", "dedicated"}
)

// Handlers contains HTTP request handlers.
type Handlers struct {
	client       *chain.Client
	providerUUID string
	bech32Prefix string
}

// NewHandlers creates a new Handlers instance.
func NewHandlers(client *chain.Client, providerUUID, bech32Prefix string) *Handlers {
	return &Handlers{
		client:       client,
		providerUUID: providerUUID,
		bech32Prefix: bech32Prefix,
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
		h.writeError(w, "invalid lease UUID format", http.StatusBadRequest)
		return
	}

	// Extract and validate bearer token
	token, err := h.extractToken(r)
	if err != nil {
		slog.Warn("invalid authorization", "error", err)
		h.writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Validate the token
	if err := token.Validate(h.bech32Prefix); err != nil {
		slog.Warn("token validation failed", "error", err)
		h.writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Verify the token's lease UUID matches the request
	if token.LeaseUUID != leaseUUID {
		slog.Warn("lease UUID mismatch",
			"token_lease_uuid", token.LeaseUUID,
			"request_lease_uuid", leaseUUID,
		)
		h.writeError(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// Query the lease from chain to verify it's active and tenant matches
	lease, err := h.client.GetActiveLease(r.Context(), leaseUUID)
	if err != nil {
		slog.Error("failed to query lease", "error", err, "lease_uuid", leaseUUID)
		h.writeError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if lease == nil {
		slog.Warn("lease not found or not active", "lease_uuid", leaseUUID)
		h.writeError(w, "lease not found or not active", http.StatusNotFound)
		return
	}

	// Verify the tenant matches
	if lease.Tenant != token.Tenant {
		slog.Warn("tenant mismatch",
			"token_tenant", token.Tenant,
			"lease_tenant", lease.Tenant,
		)
		h.writeError(w, "forbidden", http.StatusForbidden)
		return
	}

	// Verify the provider UUID matches
	if lease.ProviderUuid != h.providerUUID {
		slog.Warn("provider UUID mismatch",
			"lease_provider_uuid", lease.ProviderUuid,
			"our_provider_uuid", h.providerUUID,
		)
		h.writeError(w, "forbidden", http.StatusForbidden)
		return
	}

	// Generate deterministic connection details based on lease UUID
	connection := generateConnectionDetails(leaseUUID)

	response := ConnectionResponse{
		LeaseUUID:    leaseUUID,
		Tenant:       lease.Tenant,
		ProviderUUID: h.providerUUID,
		Connection:   connection,
	}

	slog.Info("connection details served",
		"lease_uuid", leaseUUID,
		"tenant", token.Tenant,
	)

	h.writeJSON(w, response, http.StatusOK)
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

	h.writeJSON(w, response, httpStatus)
}

// extractToken extracts and parses the bearer token from the Authorization header.
func (h *Handlers) extractToken(r *http.Request) (*AuthToken, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, ErrMissingAuth
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return nil, ErrInvalidAuthFormat
	}

	return ParseAuthToken(parts[1])
}

// writeJSON writes a JSON response.
func (h *Handlers) writeJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

// writeError writes an error response.
func (h *Handlers) writeError(w http.ResponseWriter, message string, status int) {
	response := ErrorResponse{
		Error: message,
		Code:  status,
	}
	h.writeJSON(w, response, status)
}

// Error types
type AuthError struct {
	message string
}

func (e *AuthError) Error() string {
	return e.message
}

var (
	ErrMissingAuth       = &AuthError{"missing authorization header"}
	ErrInvalidAuthFormat = &AuthError{"invalid authorization format, expected 'Bearer <token>'"}
)

// generateConnectionDetails creates deterministic connection details from a lease UUID.
// The same UUID always produces the same connection details.
func generateConnectionDetails(leaseUUID string) ConnectionDetails {
	// Hash the UUID to get deterministic random bytes
	hash := sha256.Sum256([]byte(leaseUUID))

	// Use different parts of the hash to select from each list
	hostIdx := int(binary.BigEndian.Uint32(hash[0:4])) % len(hostnames)
	portIdx := int(binary.BigEndian.Uint32(hash[4:8])) % len(ports)
	protoIdx := int(binary.BigEndian.Uint32(hash[8:12])) % len(protocols)
	regionIdx := int(binary.BigEndian.Uint32(hash[12:16])) % len(regions)
	tierIdx := int(binary.BigEndian.Uint32(hash[16:20])) % len(tiers)

	// Generate a deterministic instance ID from the hash
	instanceID := binary.BigEndian.Uint32(hash[20:24]) % 10000

	return ConnectionDetails{
		Host:     hostnames[hostIdx],
		Port:     ports[portIdx],
		Protocol: protocols[protoIdx],
		Metadata: map[string]string{
			"region":      regions[regionIdx],
			"tier":        tiers[tierIdx],
			"instance_id": fmt.Sprintf("i-%04d", instanceID),
			"status":      "connected",
		},
	}
}
