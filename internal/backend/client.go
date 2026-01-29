package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/sony/gobreaker"

	"github.com/manifest-network/fred/internal/metrics"
)

// Backend defines the interface for interacting with a provisioning backend.
// Any backend (Kubernetes, GPU, VM, etc.) must implement these 5 operations.
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

// ProvisionRequest contains the data needed to provision a resource.
type ProvisionRequest struct {
	LeaseUUID    string `json:"lease_uuid"`
	Tenant       string `json:"tenant"`
	ProviderUUID string `json:"provider_uuid"`
	SKU          string `json:"sku"`
	CallbackURL  string `json:"callback_url"`
	Payload      []byte `json:"payload,omitempty"`
	PayloadHash  string `json:"payload_hash,omitempty"`
}

// ProvisionResponse is returned by the backend after accepting a provision request.
type ProvisionResponse struct {
	ProvisionID string `json:"provision_id"`
}

// LeaseInfo contains backend-specific information about a provisioned lease.
// The structure is flexible to allow different backends to return different data.
// Common fields include: host, port, protocol, credentials, metadata.
type LeaseInfo map[string]any

// ProvisionInfo describes a single provisioned resource.
type ProvisionInfo struct {
	LeaseUUID    string    `json:"lease_uuid"`
	ProviderUUID string    `json:"provider_uuid"`
	Status       string    `json:"status"` // "provisioning", "ready", "failed"
	CreatedAt    time.Time `json:"created_at"`
	BackendName  string    `json:"-"` // Set by reconciler, not from backend
}

// CallbackPayload is sent by backends to fred's callback endpoint.
type CallbackPayload struct {
	LeaseUUID string `json:"lease_uuid"`
	Status    string `json:"status"` // "success" or "failed"
	Error     string `json:"error,omitempty"`
}

// ErrNotProvisioned is returned when a lease is not yet provisioned.
var ErrNotProvisioned = errors.New("lease not provisioned")

// Provision status constants.
const (
	ProvisionStatusProvisioning = "provisioning"
	ProvisionStatusReady        = "ready"
	ProvisionStatusFailed       = "failed"
)

// Callback status constants.
const (
	CallbackStatusSuccess = "success"
	CallbackStatusFailed  = "failed"
)

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

	// Circuit breaker settings
	CBMaxRequests   uint32        // Max requests in half-open state (default: 1)
	CBInterval      time.Duration // Interval to clear counts in closed state (default: 0, never clear)
	CBTimeout       time.Duration // Time to wait before transitioning from open to half-open (default: 60s)
	CBFailureThresh uint32        // Number of failures to trip the breaker (default: 5)
}

// NewHTTPClient creates a new HTTP backend client.
func NewHTTPClient(cfg HTTPClientConfig) *HTTPClient {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	maxIdleConns := cfg.MaxIdleConns
	if maxIdleConns == 0 {
		maxIdleConns = 100
	}

	maxIdleConnsPerHost := cfg.MaxIdleConnsPerHost
	if maxIdleConnsPerHost == 0 {
		maxIdleConnsPerHost = 10 // Higher than default (2) for better concurrency
	}

	// Circuit breaker defaults
	cbMaxRequests := cfg.CBMaxRequests
	if cbMaxRequests == 0 {
		cbMaxRequests = 1
	}
	cbTimeout := cfg.CBTimeout
	if cbTimeout == 0 {
		cbTimeout = 60 * time.Second
	}
	cbFailureThresh := cfg.CBFailureThresh
	if cbFailureThresh == 0 {
		cbFailureThresh = 5
	}

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

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("provision request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusAccepted {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("provision failed with status %d: %s", resp.StatusCode, string(bodyBytes))
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
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("get info failed with status %d: %s", resp.StatusCode, string(bodyBytes))
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

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("deprovision request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("deprovision failed with status %d: %s", resp.StatusCode, string(bodyBytes))
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
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("list provisions failed with status %d: %s", resp.StatusCode, string(bodyBytes))
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
