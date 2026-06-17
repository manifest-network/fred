package backend

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
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
	// Used by the reconciler for orphan detection.
	ListProvisions(ctx context.Context) ([]ProvisionInfo, error)

	// LookupProvisions returns provision info for the requested lease UUIDs.
	// Missing leases are simply absent from the returned slice (not an error).
	// Used by the /workloads endpoint to fetch metadata for a known set of leases
	// without scanning the full provisions corpus. Caller is responsible for
	// enforcing MaxLookupUUIDs; implementations may also enforce it defensively.
	//
	// Named LookupProvisions (not GetProvisions) to avoid visual ambiguity with
	// the existing singular GetProvision.
	LookupProvisions(ctx context.Context, leaseUUIDs []string) ([]ProvisionInfo, error)

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

	// Restore re-provisions a new lease from the retained volumes of a prior
	// soft-deleted lease (ENG-325). Returns ErrNotRetained if no retained data
	// exists for FromLeaseUUID, ErrInvalidState if the target lease is not in
	// a valid state for restore, ErrValidation for bad inputs.
	Restore(ctx context.Context, req RestoreRequest) error

	// ReconcileCustomDomain reapplies the per-LeaseItem custom_domain values
	// from chain onto the running provision. When any item's CustomDomain
	// differs from the in-memory provision state, the backend snapshots the
	// current values, updates them, and triggers a Restart() to re-emit
	// Traefik labels. On Restart failure the in-memory state is rolled back
	// so the next reconciler tick can retry.
	//
	// No-op when:
	//   - the lease is not provisioned by this backend (silent),
	//   - the provision is not currently active,
	//   - all items already match the incoming values.
	//
	// Idempotent: repeated calls with the same items list produce no
	// container churn beyond the first reconciling call.
	ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []LeaseItem) error

	// GetReleases returns the release history for a lease.
	// Returns ErrNotProvisioned if lease not found.
	GetReleases(ctx context.Context, leaseUUID string) ([]ReleaseInfo, error)

	// GetLoadStats returns the backend's current resource-load snapshot,
	// used for least-loaded provision routing. Implementations that do not
	// track load may return a snapshot whose CPUAllocatedRatio is not ok.
	GetLoadStats(ctx context.Context) (*LoadStats, error)

	// Name returns the backend's configured name.
	Name() string
}

// LeaseItem represents a single SKU with its quantity in a lease.
type LeaseItem struct {
	SKU         string `json:"sku"`
	Quantity    int    `json:"quantity"`
	ServiceName string `json:"service_name,omitempty"`
	// CustomDomain is the optional FQDN the tenant has assigned to this item.
	// When non-empty (and the service has a routable HTTP port), the backend
	// emits a secondary Traefik router on the item's container(s) routing
	// Host(<CustomDomain>) to the same loadbalancer service. Validated on-chain
	// in MsgSetItemCustomDomain; Fred re-runs cheap defense-in-depth checks
	// before applying labels.
	CustomDomain string `json:"custom_domain,omitempty"`
}

// defaultServiceName is the synthetic service name applied when fred
// normalizes a legacy single-item-without-service_name request into the
// stack-shaped contract. Must stay in lock-step with
// manifest.DefaultServiceName; duplicated here only because the manifest
// package imports backend (cycle prevents the inverse import). If this
// ever drifts, the wire-level auto-wrap and the item auto-tag would
// disagree and lease items would no longer match the wrapped manifest's
// service map — caught by TestNormalizeProvisionRequest_* +
// TestParsePayload_WrapsFlat in tandem.
const defaultServiceName = "app"

// NormalizeProvisionRequest applies boundary normalization to a provision
// request: any single-item request that lacks ServiceName is auto-tagged
// with defaultServiceName, transitioning legacy wire-format leases into
// the stack-shaped contract every downstream component now expects. Named
// items are passed through. Mixed-presence and multi-unnamed combinations
// are rejected — both were structurally invalid under the legacy
// contract too, so this only formalizes the rejection.
//
// The normalization is in-place; callers should invoke this at the very
// entry of Provision/Update, before any docker work or pool allocation,
// so the rest of the path operates on uniformly stack-shaped state.
func NormalizeProvisionRequest(req *ProvisionRequest) error {
	if req == nil || len(req.Items) == 0 {
		return fmt.Errorf("%w: provision request has no items", ErrValidation)
	}
	named, unnamed := 0, 0
	for _, item := range req.Items {
		if item.ServiceName == "" {
			unnamed++
		} else {
			named++
		}
	}
	if named > 0 && unnamed > 0 {
		return fmt.Errorf("%w: mixed service_name presence across items (got %d named, %d unnamed)",
			ErrInvalidManifest, named, unnamed)
	}
	if unnamed > 1 {
		return fmt.Errorf("%w: legacy single-service form requires exactly 1 item; got %d",
			ErrInvalidManifest, unnamed)
	}
	if unnamed == 1 {
		req.Items[0].ServiceName = defaultServiceName
	}
	return nil
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
	BackendName  string          `json:"-"` // Set by the backend or reconciler; excluded from JSON serialization

	// Workload metadata — populated by ListProvisions/GetProvision to describe what is running.
	Image         string            `json:"image,omitempty"`          // Docker image (non-stack leases)
	SKU           string            `json:"sku,omitempty"`            // SKU identifier (non-stack leases)
	Quantity      int               `json:"quantity"`                 // Total expected container count
	Items         []LeaseItem       `json:"items,omitempty"`          // Per-service items (stack leases)
	ServiceImages map[string]string `json:"service_images,omitempty"` // service name → image (stack leases)
}

// ListProvisionsResponse is the response from the /provisions endpoint.
type ListProvisionsResponse struct {
	Provisions []ProvisionInfo `json:"provisions"`
}

// CallbackPayload is sent by backends to fred's callback endpoint.
type CallbackPayload struct {
	LeaseUUID string         `json:"lease_uuid"`
	Status    CallbackStatus `json:"status"` // "success", "failed", or "deprovisioned"
	Error     string         `json:"error,omitempty"`
	Backend   string         `json:"backend,omitempty"` // Backend name; empty from pre-upgrade senders.
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

// RestoreRequest contains the data needed to restore a soft-deleted lease's
// retained volumes into a NEW lease (ENG-325). FromLeaseUUID identifies the
// original (retained) lease whose data is adopted; LeaseUUID is the new lease
// the data is restored into. Items must match the retained set's shape
// (service-name → summed-quantity).
type RestoreRequest struct {
	LeaseUUID     string      `json:"lease_uuid"`      // NEW lease
	FromLeaseUUID string      `json:"from_lease_uuid"` // original (retained) lease
	Tenant        string      `json:"tenant"`
	ProviderUUID  string      `json:"provider_uuid"` // when non-empty, cross-checked against the retained record
	Items         []LeaseItem `json:"items"`         // must match the retained set
	CallbackURL   string      `json:"callback_url"`
}

// ErrNotRetained is returned when no retained data exists for the original
// lease (absent, expired, or owned by a different tenant). Distinct from
// ErrNotProvisioned (which concerns live provisions).
var ErrNotRetained = errors.New("no retained data for lease")

// ReleaseInfo describes a single release in the history.
type ReleaseInfo struct {
	Version   int       `json:"version"`
	Image     string    `json:"image"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Error     string    `json:"error,omitempty"`
	Manifest  []byte    `json:"manifest"`
}

// LoadStats is the backend load snapshot fred consumes for least-loaded
// provision routing. It is decoded from the backend's GET /stats response.
// Only the CPU-related fields are needed for routing — CPU is the binding
// resource on current configurations, so memory/disk are intentionally ignored.
// Unknown JSON fields in the response are discarded by the decoder.
type LoadStats struct {
	TotalCPUCores     float64 `json:"total_cpu_cores"`
	AllocatedCPUCores float64 `json:"allocated_cpu_cores"`
	ActiveContainers  int     `json:"active_containers"`
}

// CPUAllocatedRatio returns the fraction of CPU allocated (allocated/total) and
// ok=true when that ratio is meaningful. ok is false when total capacity is
// unknown or non-positive (e.g., a backend that does not report CPU capacity, or
// a nil snapshot), signaling the caller to treat this backend as having no
// usable load signal.
func (s *LoadStats) CPUAllocatedRatio() (ratio float64, ok bool) {
	if s == nil || s.TotalCPUCores <= 0 {
		return 0, false
	}
	return s.AllocatedCPUCores / s.TotalCPUCores, true
}

// ErrNotProvisioned is returned when a lease is not yet provisioned.
var ErrNotProvisioned = errors.New("lease not provisioned")

// ErrAlreadyProvisioned is returned when attempting to provision an already provisioned lease.
var ErrAlreadyProvisioned = errors.New("lease already provisioned")

// ProvisionStatus represents the status of a provisioned resource.
type ProvisionStatus string

// Provision status constants.
//
// ProvisionStatusFailing marks the window between a container's death being
// detected and the terminal Failed callback being emitted. A Deprovision
// request arriving in this window transitions the lease straight to
// Deprovisioning without ever reaching Failed — preventing a stale Failed
// callback under concurrent ownership transfer.
const (
	ProvisionStatusProvisioning   ProvisionStatus = "provisioning"
	ProvisionStatusReady          ProvisionStatus = "ready"
	ProvisionStatusFailing        ProvisionStatus = "failing"
	ProvisionStatusFailed         ProvisionStatus = "failed"
	ProvisionStatusUnknown        ProvisionStatus = "unknown"
	ProvisionStatusRestarting     ProvisionStatus = "restarting"
	ProvisionStatusUpdating       ProvisionStatus = "updating"
	ProvisionStatusDeprovisioning ProvisionStatus = "deprovisioning"
	// ProvisionStatusRetained is published to the tenant at lease close/expire
	// time to signal that managed volumes may have been kept under a
	// fred-retained- namespace if the backend has retain_on_close enabled.
	// This is a best-effort hint; the backend may not have retention configured.
	ProvisionStatusRetained ProvisionStatus = "retained"
)

// CallbackStatus represents the status sent in a callback payload.
type CallbackStatus string

// Callback status constants.
const (
	CallbackStatusSuccess       CallbackStatus = "success"
	CallbackStatusFailed        CallbackStatus = "failed"
	CallbackStatusDeprovisioned CallbackStatus = "deprovisioned"
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
	maxInfoBytes             int64
	maxProvisionBytes        int64
	maxProvisionsBytes       int64
	maxLookupProvisionsBytes int64
	maxLogsBytes             int64
	maxReleasesBytes         int64
	maxStatsBytes            int64

	// Optional Prometheus metrics (nil = skip recording)
	requestDuration *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec
}

// MaxLookupUUIDs caps the number of lease UUIDs accepted by a single
// LookupProvisions call (and the matching server-side filter on
// GET /provisions?lease_uuid=...). Shared across the HTTP client, the
// docker-backend handler, and the Fred-level GetWorkloads handler so a
// future change to the cap stays consistent end-to-end.
const MaxLookupUUIDs = 100

// Default response body size limits (defense-in-depth against buggy/misrouted backends).
const (
	DefaultMaxInfoBytes             int64 = 1 << 20  // 1 MiB — single lease info
	DefaultMaxProvisionBytes        int64 = 1 << 20  // 1 MiB — single provision record
	DefaultMaxProvisionsBytes       int64 = 8 << 20  // 8 MiB — list of all provisions
	DefaultMaxLookupProvisionsBytes int64 = 8 << 20  // 8 MiB — filtered provisions lookup; matches MaxProvisionsBytes because each ProvisionInfo carries an unbounded LastError and stack leases carry unbounded ServiceImages
	DefaultMaxLogsBytes             int64 = 16 << 20 // 16 MiB — container logs can be large
	DefaultMaxReleasesBytes         int64 = 8 << 20  // 8 MiB — release history with manifests
	DefaultMaxStatsBytes            int64 = 1 << 20  // 1 MiB — load stats snapshot (small JSON)
)

// HTTPClientConfig configures an HTTP backend client.
type HTTPClientConfig struct {
	Name                string
	BaseURL             string
	Timeout             time.Duration
	MaxIdleConns        int // Max idle connections across all hosts (default: 100)
	MaxIdleConnsPerHost int // Max idle connections per host (default: 10)
	Secret              string

	// TLSClientConfig, when non-nil, is applied to the backend HTTP transport
	// (private-CA trust and/or a client certificate for mTLS). Built by the
	// caller from per-backend config so this package performs no file I/O.
	TLSClientConfig *tls.Config

	// Circuit breaker settings
	CBMaxRequests   uint32        // Max requests in half-open state (default: 1)
	CBInterval      time.Duration // Interval to clear counts in closed state (default: 0, never clear)
	CBTimeout       time.Duration // Time to wait before transitioning from open to half-open (default: 60s)
	CBFailureThresh uint32        // Number of failures to trip the breaker (default: 5)

	// Response body size limits (0 = use default). Defense-in-depth caps to prevent
	// a buggy or misrouted backend from pressuring memory with unbounded responses.
	MaxInfoBytes             int64 // GetInfo response limit (default: 1 MiB)
	MaxProvisionBytes        int64 // GetProvision response limit (default: 1 MiB)
	MaxProvisionsBytes       int64 // ListProvisions response limit (default: 8 MiB)
	MaxLookupProvisionsBytes int64 // LookupProvisions response limit (default: 8 MiB)
	MaxLogsBytes             int64 // GetLogs response limit (default: 16 MiB)
	MaxReleasesBytes         int64 // GetReleases response limit (default: 8 MiB)
	MaxStatsBytes            int64 // GetLoadStats response limit (default: 1 MiB)

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
	if cfg.TLSClientConfig != nil {
		transport.TLSClientConfig = cfg.TLSClientConfig
		// The hop stays HTTP/1.1 over TLS (it is plaintext HTTP/1.1 today).
		// A custom TLSClientConfig disables Go's automatic HTTP/2; we
		// deliberately do NOT set ForceAttemptHTTP2 — these are low-volume
		// JSON request/response calls and h2 with a custom TLS config carries
		// a known footgun (golang/go#20645).
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
			//   - ErrNotRetained: 422 from Restore (no retained data — benign client condition)
			return err == nil ||
				errors.Is(err, ErrNotProvisioned) ||
				errors.Is(err, ErrValidation) ||
				errors.Is(err, ErrInsufficientResources) ||
				errors.Is(err, ErrAlreadyProvisioned) ||
				errors.Is(err, ErrInvalidState) ||
				errors.Is(err, ErrNotRetained)
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
		cb:                       cb,
		maxInfoBytes:             positiveOr(cfg.MaxInfoBytes, DefaultMaxInfoBytes),
		maxProvisionBytes:        positiveOr(cfg.MaxProvisionBytes, DefaultMaxProvisionBytes),
		maxProvisionsBytes:       positiveOr(cfg.MaxProvisionsBytes, DefaultMaxProvisionsBytes),
		maxLookupProvisionsBytes: positiveOr(cfg.MaxLookupProvisionsBytes, DefaultMaxLookupProvisionsBytes),
		maxLogsBytes:             positiveOr(cfg.MaxLogsBytes, DefaultMaxLogsBytes),
		maxReleasesBytes:         positiveOr(cfg.MaxReleasesBytes, DefaultMaxReleasesBytes),
		maxStatsBytes:            positiveOr(cfg.MaxStatsBytes, DefaultMaxStatsBytes),
		requestDuration:          cfg.RequestDuration,
		requestsTotal:            cfg.RequestsTotal,
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

// parseErrorCode extracts the omitempty "code" discriminator from an error
// response body (best-effort; returns "" when absent or unparseable). Used to
// disambiguate overloaded status codes — e.g. Restore's 409, shared by
// ErrInvalidState (no code) and ErrAlreadyProvisioned (code="already_provisioned").
func parseErrorCode(body []byte) (code, msg string) {
	var resp struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", string(body)
	}
	if resp.Error == "" {
		resp.Error = string(body)
	}
	return resp.Code, resp.Error
}

// signRequest adds an HMAC-SHA256 signature header to the request.
// If no secret is configured, this is a no-op (backwards compatible).
func (c *HTTPClient) signRequest(req *http.Request, body []byte) {
	if c.secret == "" {
		return
	}
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.SignRequest(c.secret, req, body))
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

// LookupProvisions returns provision info for the requested lease UUIDs.
// Missing leases are absent from the returned slice (not an error).
//
// Wire path: GET {baseURL}/provisions?lease_uuid=...&lease_uuid=...
// The docker-backend handler shares the same route as the unfiltered ListProvisions
// path; the filter param toggles the subset behavior. Same wire type either way.
func (c *HTTPClient) LookupProvisions(ctx context.Context, uuids []string) ([]ProvisionInfo, error) {
	if len(uuids) == 0 || len(uuids) > MaxLookupUUIDs {
		return nil, fmt.Errorf("lookup provisions: invalid uuid count: %d", len(uuids))
	}

	// Copy and sort to produce a stable URL for the same UUID set, regardless of
	// caller order (HTTP cache friendliness; HMAC is over body so URL order is
	// signature-irrelevant). Don't mutate the caller's slice.
	sortedUUIDs := append([]string(nil), uuids...)
	sort.Strings(sortedUUIDs)

	q := url.Values{"lease_uuid": sortedUUIDs}
	target := c.baseURL + "/provisions?" + q.Encode()

	resp, err := doGet[ListProvisionsResponse](c, ctx, "lookup_provisions", target, c.maxLookupProvisionsBytes)
	if errors.Is(err, ErrNotProvisioned) {
		// doGet maps 404 to ErrNotProvisioned. The filtered /provisions handler
		// always returns 200 with a possibly-empty Provisions list, so this branch
		// is defensive — but if any backend ever does return 404, treat it as
		// "no matches" so the top-level GetWorkloads handler can still merge
		// other backends' results.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return resp.Provisions, nil
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

// Restore sends a restore request to the backend.
func (c *HTTPClient) Restore(ctx context.Context, req RestoreRequest) (err error) {
	start := time.Now()
	defer func() { c.recordMetrics("restore", start, err) }()

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal restore request: %w", err)
	}

	_, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/restore", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.signRequest(httpReq, body)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("restore request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case http.StatusAccepted:
			return nil, nil
		case http.StatusUnprocessableEntity:
			return nil, ErrNotRetained
		case http.StatusConflict:
			// Restore overloads 409 for two sentinels: the backend tags the
			// already-provisioned case with code="already_provisioned" so we can
			// reconstruct ErrAlreadyProvisioned (otherwise a duplicate restore would
			// surface the wrong ErrInvalidState message). A bare 409 (no code) is
			// ErrInvalidState (wrong lease state for restore).
			code, msg := parseErrorCode(readErrorBodyBytes(resp))
			if code == "already_provisioned" {
				return nil, fmt.Errorf("%w: %s", ErrAlreadyProvisioned, msg)
			}
			return nil, ErrInvalidState
		case http.StatusServiceUnavailable:
			// 503: backend at capacity — not a health failure (matches Provision).
			return nil, fmt.Errorf("%w: %s", ErrInsufficientResources, readErrorBody(resp))
		case http.StatusBadRequest:
			// Reconstruct the validation sub-category sentinel from the
			// validation_code body field (matching Provision/Update). Restore's
			// prelude returns ErrUnknownSKU/ErrInvalidManifest/ErrImageNotAllowed
			// via GetSKUProfile/ValidateImage; the returned error still wraps
			// ErrValidation so the breaker allowlist and 400 mapping hold.
			return nil, parseValidationError(readErrorBodyBytes(resp))
		default:
			return nil, fmt.Errorf("restore failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
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

// ReconcileCustomDomainRequest is the wire format for POST /reconcile_custom_domain.
type ReconcileCustomDomainRequest struct {
	LeaseUUID string      `json:"lease_uuid"`
	Items     []LeaseItem `json:"items"`
}

// ReconcileCustomDomain forwards a reconciliation request to the remote
// backend. The reconciler calls this on every active lease each tick; the
// remote side decides whether any action is needed (idempotent on no-change).
func (c *HTTPClient) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []LeaseItem) (err error) {
	start := time.Now()
	defer func() { c.recordMetrics("reconcile_custom_domain", start, err) }()

	body, err := json.Marshal(ReconcileCustomDomainRequest{LeaseUUID: leaseUUID, Items: items})
	if err != nil {
		return fmt.Errorf("marshal reconcile_custom_domain request: %w", err)
	}

	_, cbErr := c.cb.Execute(func() (any, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/reconcile_custom_domain", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		c.signRequest(httpReq, body)

		resp, err := c.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("reconcile_custom_domain request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		switch resp.StatusCode {
		case http.StatusAccepted, http.StatusNoContent:
			return nil, nil
		case http.StatusNotFound:
			return nil, ErrNotProvisioned
		case http.StatusConflict:
			return nil, ErrInvalidState
		default:
			return nil, fmt.Errorf("reconcile_custom_domain failed with status %d: %s", resp.StatusCode, readErrorBody(resp))
		}
	})

	if isCircuitBreakerError(cbErr) {
		return ErrCircuitOpen
	}
	return cbErr
}

// GetLoadStats retrieves the backend's current resource-load snapshot from
// GET /stats. Used by the router for least-loaded provision placement.
func (c *HTTPClient) GetLoadStats(ctx context.Context) (*LoadStats, error) {
	return doGet[LoadStats](c, ctx, "get_load_stats", c.baseURL+"/stats", c.maxStatsBytes)
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
