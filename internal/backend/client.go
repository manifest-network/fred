package backend

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/sony/gobreaker"

	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
)

// Backend defines the interface for interacting with a provisioning backend.
// Any backend (Kubernetes, GPU, VM, etc.) must implement these 6 operations.
type Backend interface {
	// Provision starts async provisioning of a resource.
	// The backend will call the callback URL when provisioning completes.
	Provision(ctx context.Context, req ProvisionRequest) error

	// GetInfo returns lease information including connection details.
	// Returns ErrNotProvisioned if the lease is not yet provisioned.
	GetInfo(ctx context.Context, leaseUUID string) (*LeaseInfo, error)

	// Deprovision releases resources for a lease. Must be idempotent.
	Deprovision(ctx context.Context, leaseUUID string) error

	// ListProvisions returns all currently provisioned resources.
	// Used for reconciliation to detect orphans.
	ListProvisions(ctx context.Context) ([]ProvisionInfo, error)

	// Health checks if the backend is reachable and healthy.
	// Returns nil if healthy, error otherwise.
	Health(ctx context.Context) error

	// Name returns the backend's configured name.
	Name() string
}

// LeaseItem represents a single SKU with its quantity in a lease.
type LeaseItem struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// ProvisionRequest contains the data needed to provision a resource.
type ProvisionRequest struct {
	LeaseUUID    string      `json:"lease_uuid"`
	Tenant       string      `json:"tenant"`
	ProviderUUID string      `json:"provider_uuid"`
	Items        []LeaseItem `json:"items"`
	CallbackURL  string      `json:"callback_url"`
	Payload      []byte      `json:"payload,omitempty"`
	PayloadHash  string      `json:"payload_hash,omitempty"`
}

// RoutingSKU returns the SKU of the first item for backend routing decisions.
//
// Why this exists: A lease may contain multiple items with different SKUs
// (e.g., [{sku: "docker-micro", qty: 2}, {sku: "docker-large", qty: 1}]).
// However, all items in a single lease are guaranteed to belong to the same
// provider - this is enforced by the chain. Therefore, any SKU from the lease
// can be used to determine which backend should handle the request.
//
// This method returns the first SKU purely for routing. It should NOT be used
// to determine resource allocation - use Items directly for that.
func (r ProvisionRequest) RoutingSKU() string {
	if len(r.Items) == 0 {
		return ""
	}
	return r.Items[0].SKU
}

// TotalQuantity returns the sum of quantities across all items.
func (r ProvisionRequest) TotalQuantity() int {
	total := 0
	for _, item := range r.Items {
		total += item.Quantity
	}
	return total
}

// ProvisionResponse is returned by the backend after accepting a provision request.
type ProvisionResponse struct {
	ProvisionID string `json:"provision_id"`
}

// LeaseInfo contains backend-specific information about a provisioned lease.
// The structure is flexible to allow different backends to return different data.
// For single-instance leases, fields are at the top level.
// For multi-instance leases, use the "instances" key with an array of instance info.
// Common fields include: host, ports, protocol, status, container_id, image.
type LeaseInfo map[string]any

// InstanceInfo represents information about a single provisioned instance.
// Used when a lease has multiple containers/pods/allocations.
type InstanceInfo struct {
	ID       string            `json:"id"`                 // Instance identifier (e.g., container ID)
	Host     string            `json:"host"`               // Host address
	Ports    map[string]any    `json:"ports,omitempty"`    // Port mappings
	Status   string            `json:"status"`             // Instance status
	Metadata map[string]string `json:"metadata,omitempty"` // Additional metadata
}

// ProvisionInfo describes a single provisioned resource.
type ProvisionInfo struct {
	LeaseUUID    string          `json:"lease_uuid"`
	ProviderUUID string          `json:"provider_uuid"`
	Status       ProvisionStatus `json:"status"` // "provisioning", "ready", "failed"
	CreatedAt    time.Time       `json:"created_at"`
	FailCount    int             `json:"fail_count"`
	BackendName  string          `json:"-"` // Set by reconciler, not from backend
}

// ListProvisionsResponse is the response from the /provisions endpoint.
type ListProvisionsResponse struct {
	Provisions []ProvisionInfo `json:"provisions"`
}

// CallbackPayload is sent by backends to fred's callback endpoint.
type CallbackPayload struct {
	LeaseUUID string         `json:"lease_uuid"`
	Status    CallbackStatus `json:"status"` // "success" or "failed"
	Error     string         `json:"error,omitempty"`
}

// ErrNotProvisioned is returned when a lease is not yet provisioned.
var ErrNotProvisioned = errors.New("lease not provisioned")

// ErrAlreadyProvisioned is returned when attempting to provision an already provisioned lease.
var ErrAlreadyProvisioned = errors.New("lease already provisioned")

// ProvisionStatus represents the status of a provisioned resource.
type ProvisionStatus string

// Provision status constants.
const (
	ProvisionStatusProvisioning ProvisionStatus = "provisioning"
	ProvisionStatusReady        ProvisionStatus = "ready"
	ProvisionStatusFailed       ProvisionStatus = "failed"
	ProvisionStatusUnknown      ProvisionStatus = "unknown"
)

// CallbackStatus represents the status sent in a callback payload.
type CallbackStatus string

// Callback status constants.
const (
	CallbackStatusSuccess CallbackStatus = "success"
	CallbackStatusFailed  CallbackStatus = "failed"
)

// ErrValidation is returned when a provision request fails pre-flight validation
// (e.g., unknown SKU, invalid manifest, disallowed image registry).
var ErrValidation = errors.New("validation error")

// ErrInsufficientResources is returned when there are not enough resources
// to fulfill a provision request.
var ErrInsufficientResources = errors.New("insufficient resources")

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// isCircuitBreakerError checks if the error is a circuit breaker error
// (either open state or too many requests in half-open state).
func isCircuitBreakerError(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}

// HTTPClient implements Backend using HTTP calls to a backend service.
type HTTPClient struct {
	name       string
	baseURL    string
	secret     string
	httpClient *http.Client
	cb         *gobreaker.CircuitBreaker
}

// HTTPClientConfig configures an HTTP backend client.
type HTTPClientConfig struct {
	Name                string
	BaseURL             string
	Timeout             time.Duration
	MaxIdleConns        int // Max idle connections across all hosts (default: 100)
	MaxIdleConnsPerHost int // Max idle connections per host (default: 10)
	Secret              string

	// Circuit breaker settings
	CBMaxRequests   uint32        // Max requests in half-open state (default: 1)
	CBInterval      time.Duration // Interval to clear counts in closed state (default: 0, never clear)
	CBTimeout       time.Duration // Time to wait before transitioning from open to half-open (default: 60s)
	CBFailureThresh uint32        // Number of failures to trip the breaker (default: 5)
}

// NewHTTPClient creates a new HTTP backend client.
func NewHTTPClient(cfg HTTPClientConfig) *HTTPClient {
	// Apply defaults using cmp.Or (returns first non-zero value)
	timeout := cmp.Or(cfg.Timeout, 30*time.Second)
	maxIdleConns := cmp.Or(cfg.MaxIdleConns, 100)
	maxIdleConnsPerHost := cmp.Or(cfg.MaxIdleConnsPerHost, 10) // Higher than default (2)

	// Circuit breaker defaults
	cbMaxRequests := cmp.Or(cfg.CBMaxRequests, uint32(1))
	cbTimeout := cmp.Or(cfg.CBTimeout, 60*time.Second)
	cbFailureThresh := cmp.Or(cfg.CBFailureThresh, uint32(5))

	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
	}

	// Create circuit breaker
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cbMaxRequests,
		Interval:    cfg.CBInterval, // 0 = don't clear counts
		Timeout:     cbTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cbFailureThresh
		},
		IsSuccessful: func(err error) bool {
			// ErrNotProvisioned (404 from GetInfo) is a valid response, not a backend failure.
			// It shouldn't count toward the circuit breaker failure threshold.
			return err == nil || errors.Is(err, ErrNotProvisioned)
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("circuit breaker state change",
				"backend", name,
				"from", from.String(),
				"to", to.String(),
			)
			metrics.BackendCircuitBreakerState.WithLabelValues(name).Set(float64(to))
		},
	})

	return &HTTPClient{
		name:    cfg.Name,
		baseURL: cfg.BaseURL,
		secret:  cfg.Secret,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		cb: cb,
	}
}

// Name returns the backend's configured name.
func (c *HTTPClient) Name() string {
	return c.name
}

// recordMetrics records request duration and count for a backend operation.
func (c *HTTPClient) recordMetrics(operation string, start time.Time, err error) {
	duration := time.Since(start).Seconds()
	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.BackendRequestDuration.WithLabelValues(c.name, operation, status).Observe(duration)
	metrics.BackendRequestsTotal.WithLabelValues(c.name, operation, status).Inc()
}

// readErrorBody reads up to 4 KiB from an HTTP response body for inclusion
// in error messages. If reading fails, the read error is reported instead.
func readErrorBody(resp *http.Response) string {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return fmt.Sprintf("<body read error: %v>", err)
	}
	return string(body)
}

// signRequest adds an HMAC-SHA256 signature header to the request.
// If no secret is configured, this is a no-op (backwards compatible).
func (c *HTTPClient) signRequest(req *http.Request, body []byte) {
	if c.secret == "" {
		return
	}
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(c.secret, body))
}

// Provision sends a provision request to the backend.
func (c *HTTPClient) Provision(ctx context.Context, req ProvisionRequest) (err error) {
	start := time.Now()
	defer func() { c.recordMetrics("provision", start, err) }()

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal provision request: %w", err)
	}

	_, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/provision", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.signRequest(httpReq, body)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("provision request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusAccepted {
			return nil, fmt.Errorf("provision failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}

		return nil, nil
	})

	if isCircuitBreakerError(cbErr) {
		return ErrCircuitOpen
	}
	return cbErr
}

// GetInfo retrieves lease information including connection details.
func (c *HTTPClient) GetInfo(ctx context.Context, leaseUUID string) (_ *LeaseInfo, err error) {
	start := time.Now()
	defer func() { c.recordMetrics("get_info", start, err) }()

	url := fmt.Sprintf("%s/info/%s", c.baseURL, leaseUUID)

	result, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("get info request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotProvisioned
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("get info failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}

		var info LeaseInfo
		if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
			return nil, fmt.Errorf("decode info response: %w", err)
		}

		return &info, nil
	})

	if isCircuitBreakerError(cbErr) {
		return nil, ErrCircuitOpen
	}
	if cbErr != nil {
		return nil, cbErr
	}
	return result.(*LeaseInfo), nil
}

// Deprovision releases resources for a lease.
func (c *HTTPClient) Deprovision(ctx context.Context, leaseUUID string) (err error) {
	start := time.Now()
	defer func() { c.recordMetrics("deprovision", start, err) }()

	body, err := json.Marshal(map[string]string{"lease_uuid": leaseUUID})
	if err != nil {
		return fmt.Errorf("marshal deprovision request: %w", err)
	}

	_, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/deprovision", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.signRequest(httpReq, body)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("deprovision request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("deprovision failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}

		return nil, nil
	})

	if isCircuitBreakerError(cbErr) {
		return ErrCircuitOpen
	}
	return cbErr
}

// ListProvisions returns all provisioned resources from this backend.
func (c *HTTPClient) ListProvisions(ctx context.Context) (_ []ProvisionInfo, err error) {
	start := time.Now()
	defer func() { c.recordMetrics("list_provisions", start, err) }()

	result, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/provisions", nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("list provisions request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("list provisions failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}

		var result struct {
			Provisions []ProvisionInfo `json:"provisions"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode provisions response: %w", err)
		}

		return result.Provisions, nil
	})

	if isCircuitBreakerError(cbErr) {
		return nil, ErrCircuitOpen
	}
	if cbErr != nil {
		return nil, cbErr
	}
	return result.([]ProvisionInfo), nil
}

// Health checks if the backend is reachable and healthy.
// It sends a GET request to /health on the backend.
func (c *HTTPClient) Health(ctx context.Context) error {
	// Don't go through circuit breaker for health checks - we want to know actual status
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return fmt.Errorf("create health request: %w", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backend unhealthy: status %d", resp.StatusCode)
	}

	return nil
}
