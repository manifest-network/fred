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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker"

	"github.com/manifest-network/fred/internal/hmacauth"
)

// Backend defines the interface for interacting with a provisioning backend.
// Any backend (Kubernetes, GPU, VM, etc.) must implement these operations.
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

	// RefreshState synchronizes in-memory provision state with the
	// underlying infrastructure. Backends should query the real
	// container/VM state and update their internal tracking.
	// Called by the reconciler before ListProvisions to avoid stale reads.
	RefreshState(ctx context.Context) error

	// GetProvision returns status information for a single provision.
	// Returns ErrNotProvisioned if the lease is not found.
	GetProvision(ctx context.Context, leaseUUID string) (*ProvisionInfo, error)

	// GetLogs returns container logs for a provisioned lease.
	// The tail parameter limits the number of log lines per container.
	// Returns ErrNotProvisioned if the lease is not found.
	GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error)

	// Restart restarts containers for a lease without changing the manifest.
	// Returns ErrNotProvisioned if lease not found, ErrInvalidState if not restartable.
	Restart(ctx context.Context, req RestartRequest) error

	// Update deploys a new manifest for a lease, replacing containers.
	// Returns ErrNotProvisioned if lease not found, ErrInvalidState if not updatable.
	Update(ctx context.Context, req UpdateRequest) error

	// GetReleases returns the release history for a lease.
	// Returns ErrNotProvisioned if lease not found.
	GetReleases(ctx context.Context, leaseUUID string) ([]ReleaseInfo, error)

	// Name returns the backend's configured name.
	Name() string
}

// LeaseItem represents a single SKU with its quantity in a lease.
type LeaseItem struct {
	SKU         string `json:"sku"`
	Quantity    int    `json:"quantity"`
	ServiceName string `json:"service_name,omitempty"`
}

// IsStack returns true when the lease items represent a stack (multi-service deployment).
// A stack lease has ServiceName set on every item. Legacy leases have no ServiceName
// on any item. The modes are all-or-nothing (enforced on-chain).
// Returns an error if items contain a mix of set and unset ServiceName values,
// which indicates a chain enforcement bug or corrupted request.
func IsStack(items []LeaseItem) (bool, error) {
	if len(items) == 0 {
		return false, nil
	}
	isStack := items[0].ServiceName != ""
	for _, item := range items[1:] {
		if (item.ServiceName != "") != isStack {
			return false, fmt.Errorf("mixed ServiceName in lease items: items[0].ServiceName=%q but found %q", items[0].ServiceName, item.ServiceName)
		}
	}
	return isStack, nil
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
type LeaseInfo struct {
	Host      string                  `json:"host,omitempty"`
	FQDN      string                  `json:"fqdn,omitempty"`
	Protocol  string                  `json:"protocol,omitempty"`
	Ports     map[string]PortBinding  `json:"ports,omitempty"`
	Instances []LeaseInstance         `json:"instances,omitempty"`
	Services  map[string]LeaseService `json:"services,omitempty"`
	Metadata  map[string]string       `json:"metadata,omitempty"`
}

// PortBinding represents a port mapping from container to host as reported by a backend.
type PortBinding struct {
	HostIP   string `json:"host_ip"`
	HostPort string `json:"host_port"`
}

// LeaseInstance represents a single provisioned instance (container/pod).
type LeaseInstance struct {
	InstanceIndex int                    `json:"instance_index"`
	ContainerID   string                 `json:"container_id,omitempty"`
	Image         string                 `json:"image,omitempty"`
	Status        string                 `json:"status,omitempty"`
	FQDN          string                 `json:"fqdn,omitempty"`
	Ports         map[string]PortBinding `json:"ports,omitempty"`
}

// LeaseService groups instances belonging to a single service in a stack lease.
type LeaseService struct {
	FQDN      string          `json:"fqdn,omitempty"`
	Instances []LeaseInstance `json:"instances,omitempty"`
}

// ProvisionInfo describes a single provisioned resource.
type ProvisionInfo struct {
	LeaseUUID    string          `json:"lease_uuid"`
	ProviderUUID string          `json:"provider_uuid"`
	Status       ProvisionStatus `json:"status"` // see ProvisionStatus* constants
	CreatedAt    time.Time       `json:"created_at"`
	FailCount    int             `json:"fail_count"`
	LastError    string          `json:"last_error,omitempty"`
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

// RestartRequest contains the data needed to restart a lease's containers.
type RestartRequest struct {
	LeaseUUID   string `json:"lease_uuid"`
	CallbackURL string `json:"callback_url"`
}

// UpdateRequest contains the data needed to update a lease to a new manifest.
type UpdateRequest struct {
	LeaseUUID   string `json:"lease_uuid"`
	CallbackURL string `json:"callback_url"`
	Payload     []byte `json:"payload"`
	PayloadHash string `json:"payload_hash,omitempty"`
}

// ReleaseInfo describes a single release in the history.
type ReleaseInfo struct {
	Version   int       `json:"version"`
	Image     string    `json:"image"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error,omitempty"`
	Manifest  []byte    `json:"manifest"`
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
	ProvisionStatusRestarting   ProvisionStatus = "restarting"
	ProvisionStatusUpdating     ProvisionStatus = "updating"
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

// Validation sub-category sentinels. These wrap ErrValidation so errors.Is(err, ErrValidation)
// still works, while allowing callers to classify the failure without string matching.
var (
	ErrUnknownSKU      = fmt.Errorf("%w: unknown SKU", ErrValidation)
	ErrInvalidManifest = fmt.Errorf("%w: invalid manifest", ErrValidation)
	ErrImageNotAllowed = fmt.Errorf("%w: image not allowed", ErrValidation)
)

// ValidationCode identifies the sub-category of a validation error in HTTP
// responses. Backends include this in 400 JSON bodies so the client can
// reconstruct the correct sentinel error across the HTTP boundary.
type ValidationCode string

// Validation code constants used in HTTP error responses.
const (
	ValidationCodeUnknownSKU      ValidationCode = "unknown_sku"
	ValidationCodeInvalidManifest ValidationCode = "invalid_manifest"
	ValidationCodeImageNotAllowed ValidationCode = "image_not_allowed"
)

// validationCodeErrors is the single source of truth mapping validation codes
// to their sentinel errors. Used by both ClassifyValidationError (server-side,
// error → code) and parseValidationError (client-side, code → error).
var validationCodeErrors = map[ValidationCode]error{
	ValidationCodeUnknownSKU:      ErrUnknownSKU,
	ValidationCodeInvalidManifest: ErrInvalidManifest,
	ValidationCodeImageNotAllowed: ErrImageNotAllowed,
}

// ClassifyValidationError returns the ValidationCode for a validation error.
// Returns "" if the error does not match any known sub-category.
func ClassifyValidationError(err error) ValidationCode {
	for code, sentinel := range validationCodeErrors {
		if errors.Is(err, sentinel) {
			return code
		}
	}
	return ""
}

// ErrInsufficientResources is returned when there are not enough resources
// to fulfill a provision request.
var ErrInsufficientResources = errors.New("insufficient resources")

// ErrInvalidState is returned when an operation is not valid for the current lease state.
var ErrInvalidState = errors.New("invalid state for operation")

// ErrResponseTooLarge is returned when a backend response body exceeds the configured size limit.
var ErrResponseTooLarge = errors.New("response body too large")

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

	// Response body size limits
	maxInfoBytes       int64
	maxProvisionBytes  int64
	maxProvisionsBytes int64
	maxLogsBytes       int64
	maxReleasesBytes   int64

	// Optional Prometheus metrics (nil = skip recording)
	requestDuration *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec
}

// Default response body size limits (defense-in-depth against buggy/misrouted backends).
const (
	DefaultMaxInfoBytes       int64 = 1 << 20  // 1 MiB — single lease info
	DefaultMaxProvisionBytes  int64 = 1 << 20  // 1 MiB — single provision record
	DefaultMaxProvisionsBytes int64 = 8 << 20  // 8 MiB — list of all provisions
	DefaultMaxLogsBytes       int64 = 16 << 20 // 16 MiB — container logs can be large
	DefaultMaxReleasesBytes   int64 = 8 << 20  // 8 MiB — release history with manifests
)

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

	// Response body size limits (0 = use default). Defense-in-depth caps to prevent
	// a buggy or misrouted backend from pressuring memory with unbounded responses.
	MaxInfoBytes       int64 // GetInfo response limit (default: 1 MiB)
	MaxProvisionBytes  int64 // GetProvision response limit (default: 1 MiB)
	MaxProvisionsBytes int64 // ListProvisions response limit (default: 8 MiB)
	MaxLogsBytes       int64 // GetLogs response limit (default: 16 MiB)
	MaxReleasesBytes   int64 // GetReleases response limit (default: 8 MiB)

	// Optional Prometheus metrics. When nil, metric recording is skipped.
	// This prevents binaries that don't use these metrics (e.g., docker-backend)
	// from registering phantom fred-level metrics via transitive imports.
	RequestDuration     *prometheus.HistogramVec // labels: backend, operation, status
	RequestsTotal       *prometheus.CounterVec   // labels: backend, operation, status
	CircuitBreakerState *prometheus.GaugeVec     // labels: backend
}

// positiveOr returns v if v > 0, otherwise returns fallback.
// Used to normalize response size limits so that zero and negative
// config values are treated as "use default".
func positiveOr(v, fallback int64) int64 {
	if v > 0 {
		return v
	}
	return fallback
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
			// These errors represent client-side or capacity conditions, not backend
			// health failures. They should NOT count toward the circuit breaker
			// failure threshold:
			//   - ErrNotProvisioned: 404 from GetInfo/GetProvision/GetLogs (valid "not found")
			//   - ErrValidation: 400 from Provision/Update (permanent client error)
			//   - ErrInsufficientResources: 503 from Provision (backend at capacity, not unhealthy)
			//   - ErrAlreadyProvisioned: 409 from Provision (idempotent duplicate)
			//   - ErrInvalidState: 409 from Restart/Update (wrong lease state for operation)
			return err == nil ||
				errors.Is(err, ErrNotProvisioned) ||
				errors.Is(err, ErrValidation) ||
				errors.Is(err, ErrInsufficientResources) ||
				errors.Is(err, ErrAlreadyProvisioned) ||
				errors.Is(err, ErrInvalidState)
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("circuit breaker state change",
				"backend", name,
				"from", from.String(),
				"to", to.String(),
			)
			if cfg.CircuitBreakerState != nil {
				cfg.CircuitBreakerState.WithLabelValues(name).Set(float64(to))
			}
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
		cb:                 cb,
		maxInfoBytes:       positiveOr(cfg.MaxInfoBytes, DefaultMaxInfoBytes),
		maxProvisionBytes:  positiveOr(cfg.MaxProvisionBytes, DefaultMaxProvisionBytes),
		maxProvisionsBytes: positiveOr(cfg.MaxProvisionsBytes, DefaultMaxProvisionsBytes),
		maxLogsBytes:       positiveOr(cfg.MaxLogsBytes, DefaultMaxLogsBytes),
		maxReleasesBytes:   positiveOr(cfg.MaxReleasesBytes, DefaultMaxReleasesBytes),
		requestDuration:    cfg.RequestDuration,
		requestsTotal:      cfg.RequestsTotal,
	}
}

// Name returns the backend's configured name.
func (c *HTTPClient) Name() string {
	return c.name
}

// recordMetrics records request duration and count for a backend operation.
// No-op when metrics are not configured.
func (c *HTTPClient) recordMetrics(operation string, start time.Time, err error) {
	if c.requestDuration == nil && c.requestsTotal == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	if c.requestDuration != nil {
		c.requestDuration.WithLabelValues(c.name, operation, status).Observe(time.Since(start).Seconds())
	}
	if c.requestsTotal != nil {
		c.requestsTotal.WithLabelValues(c.name, operation, status).Inc()
	}
}

// readErrorBodyBytes reads up to 4 KiB from an HTTP response body for
// inclusion in error messages. Remaining bytes are drained to allow
// connection reuse. If reading fails, a placeholder message is returned.
func readErrorBodyBytes(resp *http.Response) []byte {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	// Drain any remaining bytes so the underlying connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return []byte(fmt.Sprintf("<body read error: %v>", err))
	}
	return body
}

// readErrorBody reads the response body as a string (convenience wrapper).
func readErrorBody(resp *http.Response) string {
	return string(readErrorBodyBytes(resp))
}

// decodeJSONLimited reads at most limit bytes from r, then JSON-unmarshals into dst.
// Returns ErrResponseTooLarge if the body exceeds limit. Remaining bytes are drained
// to allow connection reuse.
func decodeJSONLimited(r io.ReadCloser, limit int64, dst any) error {
	lr := io.LimitReader(r, limit+1)
	body, err := io.ReadAll(lr)
	// Drain any remaining bytes so the underlying connection can be reused.
	_, _ = io.Copy(io.Discard, r)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if int64(len(body)) > limit {
		return fmt.Errorf("%w: %d bytes exceeds %d byte limit", ErrResponseTooLarge, len(body), limit)
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// parseValidationError parses a 400 response body and returns an error
// wrapping the appropriate validation sentinel. If the body contains a
// validation_code field, the corresponding sub-category sentinel is used
// (ErrUnknownSKU, ErrInvalidManifest, ErrImageNotAllowed). Otherwise
// falls back to the generic ErrValidation.
func parseValidationError(body []byte) error {
	var resp struct {
		Error          string         `json:"error"`
		ValidationCode ValidationCode `json:"validation_code"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("%w: %s", ErrValidation, string(body))
	}

	msg := resp.Error
	if msg == "" {
		msg = string(body)
	}

	if sentinel, ok := validationCodeErrors[resp.ValidationCode]; ok {
		return fmt.Errorf("%w: %s", sentinel, msg)
	}
	return fmt.Errorf("%w: %s", ErrValidation, msg)
}

// signRequest adds an HMAC-SHA256 signature header to the request.
// If no secret is configured, this is a no-op (backwards compatible).
func (c *HTTPClient) signRequest(req *http.Request, body []byte) {
	if c.secret == "" {
		return
	}
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(c.secret, body))
}

// doGet executes a GET request through the circuit breaker, decoding the
// JSON response as T. It handles 404→ErrNotProvisioned and enforces a
// response size limit.
func doGet[T any](c *HTTPClient, ctx context.Context, metric, url string, maxBytes int64) (_ *T, err error) {
	start := time.Now()
	defer func() { c.recordMetrics(metric, start, err) }()

	result, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		c.signRequest(httpReq, nil)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("%s request failed: %w", metric, err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotProvisioned
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s failed with status %d: %s", metric, resp.StatusCode, readErrorBody(resp))
		}

		var v T
		if err := decodeJSONLimited(resp.Body, maxBytes, &v); err != nil {
			return nil, fmt.Errorf("decode %s response: %w", metric, err)
		}
		return &v, nil
	})

	if isCircuitBreakerError(cbErr) {
		return nil, ErrCircuitOpen
	}
	if cbErr != nil {
		return nil, cbErr
	}
	v, ok := result.(*T)
	if !ok {
		return nil, fmt.Errorf("%s: unexpected result type %T", metric, result)
	}
	return v, nil
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
			// Map well-known status codes to sentinel errors so the circuit
			// breaker and callers can distinguish them from backend failures.
			switch resp.StatusCode {
			case http.StatusBadRequest:
				// 400: validation error — permanent, won't succeed on retry.
				return nil, parseValidationError(readErrorBodyBytes(resp))
			case http.StatusConflict:
				// 409: lease already provisioned — idempotent duplicate.
				return nil, fmt.Errorf("%w: %s", ErrAlreadyProvisioned, readErrorBody(resp))
			case http.StatusServiceUnavailable:
				// 503: backend at capacity — not a health failure.
				return nil, fmt.Errorf("%w: %s", ErrInsufficientResources, readErrorBody(resp))
			default:
				return nil, fmt.Errorf("provision failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
			}
		}

		return nil, nil
	})

	if isCircuitBreakerError(cbErr) {
		return ErrCircuitOpen
	}
	return cbErr
}

// GetInfo retrieves lease information including connection details.
func (c *HTTPClient) GetInfo(ctx context.Context, leaseUUID string) (*LeaseInfo, error) {
	return doGet[LeaseInfo](c, ctx, "get_info", fmt.Sprintf("%s/info/%s", c.baseURL, leaseUUID), c.maxInfoBytes)
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
		c.signRequest(httpReq, nil)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("list provisions request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("list provisions failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}

		var provResult struct {
			Provisions []ProvisionInfo `json:"provisions"`
		}
		if err := decodeJSONLimited(resp.Body, c.maxProvisionsBytes, &provResult); err != nil {
			return nil, fmt.Errorf("decode provisions response: %w", err)
		}

		return provResult.Provisions, nil
	})

	if isCircuitBreakerError(cbErr) {
		return nil, ErrCircuitOpen
	}
	if cbErr != nil {
		return nil, cbErr
	}
	provisions, ok := result.([]ProvisionInfo)
	if !ok {
		return nil, fmt.Errorf("list provisions: unexpected result type %T", result)
	}
	return provisions, nil
}

// GetProvision retrieves status information for a single provision.
func (c *HTTPClient) GetProvision(ctx context.Context, leaseUUID string) (*ProvisionInfo, error) {
	return doGet[ProvisionInfo](c, ctx, "get_provision", fmt.Sprintf("%s/provisions/%s", c.baseURL, leaseUUID), c.maxProvisionBytes)
}

// GetLogs retrieves container logs for a provisioned lease.
func (c *HTTPClient) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	result, err := doGet[map[string]string](c, ctx, "get_logs", fmt.Sprintf("%s/logs/%s?tail=%d", c.baseURL, leaseUUID, tail), c.maxLogsBytes)
	if err != nil {
		return nil, err
	}
	return *result, nil
}

// Restart sends a restart request to the backend.
func (c *HTTPClient) Restart(ctx context.Context, req RestartRequest) (err error) {
	start := time.Now()
	defer func() { c.recordMetrics("restart", start, err) }()

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal restart request: %w", err)
	}

	_, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/restart", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.signRequest(httpReq, body)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("restart request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case http.StatusAccepted:
			return nil, nil
		case http.StatusNotFound:
			return nil, ErrNotProvisioned
		case http.StatusConflict:
			return nil, ErrInvalidState
		default:
			return nil, fmt.Errorf("restart failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}
	})

	if isCircuitBreakerError(cbErr) {
		return ErrCircuitOpen
	}
	return cbErr
}

// Update sends an update request to the backend.
func (c *HTTPClient) Update(ctx context.Context, req UpdateRequest) (err error) {
	start := time.Now()
	defer func() { c.recordMetrics("update", start, err) }()

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal update request: %w", err)
	}

	_, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/update", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.signRequest(httpReq, body)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("update request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case http.StatusAccepted:
			return nil, nil
		case http.StatusBadRequest:
			return nil, parseValidationError(readErrorBodyBytes(resp))
		case http.StatusNotFound:
			return nil, ErrNotProvisioned
		case http.StatusConflict:
			return nil, ErrInvalidState
		default:
			return nil, fmt.Errorf("update failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}
	})

	if isCircuitBreakerError(cbErr) {
		return ErrCircuitOpen
	}
	return cbErr
}

// GetReleases retrieves release history for a lease.
func (c *HTTPClient) GetReleases(ctx context.Context, leaseUUID string) ([]ReleaseInfo, error) {
	result, err := doGet[[]ReleaseInfo](c, ctx, "get_releases", fmt.Sprintf("%s/releases/%s", c.baseURL, leaseUUID), c.maxReleasesBytes)
	if err != nil {
		return nil, err
	}
	return *result, nil
}

// RefreshState is a no-op for remote backends (they refresh server-side).
func (c *HTTPClient) RefreshState(ctx context.Context) error {
	return nil
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
