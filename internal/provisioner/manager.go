package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
)

// Sentinel errors for provisioner operations.
var (
	// ErrMalformedMessage indicates the message payload could not be parsed.
	// This is a terminal error - the message should not be retried.
	ErrMalformedMessage = errors.New("malformed message payload")

	// ErrNoBackendAvailable indicates no backend is configured to handle the request.
	ErrNoBackendAvailable = errors.New("no backend available")

	// ErrProvisioningFailed indicates the backend failed to provision the resource.
	ErrProvisioningFailed = errors.New("provisioning failed")

	// ErrDeprovisionFailed indicates the backend failed to deprovision the resource.
	ErrDeprovisionFailed = errors.New("deprovision failed")

	// ErrAcknowledgeFailed indicates the lease acknowledgment on chain failed.
	ErrAcknowledgeFailed = errors.New("lease acknowledgment failed")
)

// isTerminalAcknowledgeError returns true if the error indicates the lease
// cannot be acknowledged and retrying won't help. This includes cases where
// the lease is already acknowledged (not in PENDING state).
func isTerminalAcknowledgeError(err error) bool {
	if err == nil {
		return false
	}
	// Check against specific billing module errors using errors.Is().
	// ErrLeaseNotPending: lease is already ACTIVE or in another terminal state.
	// ErrLeaseNotFound: lease doesn't exist (may have been deleted).
	return errors.Is(err, billingtypes.ErrLeaseNotPending) ||
		errors.Is(err, billingtypes.ErrLeaseNotFound)
}

// Watermill topic names for internal event routing.
const (
	TopicLeaseCreated    = "events.lease.created"
	TopicLeaseClosed     = "events.lease.closed"
	TopicLeaseExpired    = "events.lease.expired"
	TopicBackendCallback = "events.backend.callback"
	TopicPayloadReceived = "events.payload.received"
)

// CallbackPath is the path suffix for backend provision callbacks.
const CallbackPath = "/callbacks/provision"

// BuildCallbackURL constructs the full callback URL from a base URL.
func BuildCallbackURL(baseURL string) string {
	return baseURL + CallbackPath
}

// ChainClient defines the chain operations needed by the provisioner.
type ChainClient interface {
	GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}

// Manager handles the provisioning lifecycle using Watermill for event routing.
type Manager struct {
	providerUUID    string
	callbackBaseURL string
	router          *backend.Router
	chainClient     ChainClient
	publisher       message.Publisher
	wmRouter        *message.Router
	payloadStore    *PayloadStore

	// Track in-flight provisions (ephemeral - recovered via reconciliation)
	inFlight   map[string]inFlightProvision
	inFlightMu sync.RWMutex

	// Callback timeout handling
	callbackTimeout      time.Duration
	timeoutCheckInterval time.Duration
}

type inFlightProvision struct {
	LeaseUUID string
	Tenant    string
	SKU       string
	Backend   string
	StartTime time.Time // For duration metrics
}

// TrackInFlight registers a lease as being provisioned.
// This allows the manager to handle callbacks for this lease.
func (m *Manager) TrackInFlight(leaseUUID, tenant, sku, backendName string) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	m.inFlight[leaseUUID] = inFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		SKU:       sku,
		Backend:   backendName,
		StartTime: time.Now(),
	}
	metrics.InFlightProvisions.Inc()
}

// TryTrackInFlight atomically checks if a lease is already in-flight and tracks it if not.
// Returns true if the lease was successfully tracked (was not already in-flight),
// false if the lease was already being provisioned.
// This prevents TOCTOU races between checking and tracking.
func (m *Manager) TryTrackInFlight(leaseUUID, tenant, sku, backendName string) bool {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	if _, exists := m.inFlight[leaseUUID]; exists {
		return false
	}
	m.inFlight[leaseUUID] = inFlightProvision{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		SKU:       sku,
		Backend:   backendName,
		StartTime: time.Now(),
	}
	metrics.InFlightProvisions.Inc()
	return true
}

// UntrackInFlight removes a lease from the in-flight tracking.
func (m *Manager) UntrackInFlight(leaseUUID string) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	if _, exists := m.inFlight[leaseUUID]; exists {
		delete(m.inFlight, leaseUUID)
		metrics.InFlightProvisions.Dec()
	}
}

// IsInFlight checks if a lease is currently being provisioned.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	_, exists := m.inFlight[leaseUUID]
	return exists
}

// PopInFlight atomically removes and returns an in-flight provision.
// Returns the provision info and true if found, or zero value and false if not found.
func (m *Manager) PopInFlight(leaseUUID string) (inFlightProvision, bool) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	provision, exists := m.inFlight[leaseUUID]
	if exists {
		delete(m.inFlight, leaseUUID)
		metrics.InFlightProvisions.Dec()
	}
	return provision, exists
}

// GetInFlight returns the in-flight provision info without removing it.
// Returns the provision info and true if found, or zero value and false if not found.
func (m *Manager) GetInFlight(leaseUUID string) (inFlightProvision, bool) {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	provision, exists := m.inFlight[leaseUUID]
	return provision, exists
}

// ManagerConfig configures the provision manager.
type ManagerConfig struct {
	ProviderUUID         string
	CallbackBaseURL      string        // Base URL for backend callbacks (e.g., "http://fred.example.com:8080")
	PayloadStore         *PayloadStore // Optional external payload store (if nil, manager won't handle payloads)
	CallbackTimeout      time.Duration // Timeout for backend callbacks (default: 10 minutes, 0 = disabled)
	TimeoutCheckInterval time.Duration // How often to check for timeouts (default: 1 minute)
}

// NewManager creates a new provision manager with Watermill routing.
func NewManager(cfg ManagerConfig, router *backend.Router, chainClient ChainClient) (*Manager, error) {
	// Validate required dependencies
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	if chainClient == nil {
		return nil, fmt.Errorf("chainClient is required")
	}

	// Validate required config fields
	if cfg.ProviderUUID == "" {
		return nil, fmt.Errorf("ProviderUUID is required")
	}
	if cfg.CallbackBaseURL == "" {
		return nil, fmt.Errorf("CallbackBaseURL is required")
	}

	// Create Watermill logger adapter
	wmLogger := watermill.NewSlogLogger(slog.Default())

	// Create in-memory pub/sub (messages don't survive crash - that's OK, we reconcile)
	pubSub := gochannel.NewGoChannel(gochannel.Config{
		OutputChannelBuffer: 100,
	}, wmLogger)

	// Create Watermill router with middleware
	wmRouter, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		return nil, fmt.Errorf("create watermill router: %w", err)
	}

	// Add middleware for retries and panic recovery
	wmRouter.AddMiddleware(
		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: time.Second,
			Logger:          wmLogger,
		}.Middleware,
		middleware.Recoverer,
	)

	// Apply defaults for timeout settings
	callbackTimeout := cfg.CallbackTimeout
	if callbackTimeout == 0 {
		callbackTimeout = 10 * time.Minute
	}
	timeoutCheckInterval := cfg.TimeoutCheckInterval
	if timeoutCheckInterval == 0 {
		timeoutCheckInterval = time.Minute
	}

	m := &Manager{
		providerUUID:         cfg.ProviderUUID,
		callbackBaseURL:      cfg.CallbackBaseURL,
		router:               router,
		chainClient:          chainClient,
		publisher:            pubSub,
		wmRouter:             wmRouter,
		payloadStore:         cfg.PayloadStore,
		inFlight:             make(map[string]inFlightProvision),
		callbackTimeout:      callbackTimeout,
		timeoutCheckInterval: timeoutCheckInterval,
	}

	// Register handlers
	wmRouter.AddNoPublisherHandler(
		"handle_lease_created",
		TopicLeaseCreated,
		pubSub,
		m.handleLeaseCreated,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_lease_closed",
		TopicLeaseClosed,
		pubSub,
		m.handleLeaseClosed,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_lease_expired",
		TopicLeaseExpired,
		pubSub,
		m.handleLeaseExpired,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_backend_callback",
		TopicBackendCallback,
		pubSub,
		m.handleBackendCallback,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_payload_received",
		TopicPayloadReceived,
		pubSub,
		m.handlePayloadReceived,
	)

	return m, nil
}

// Start begins the Watermill router and callback timeout checker.
func (m *Manager) Start(ctx context.Context) error {
	slog.Info("starting provision manager",
		"callback_timeout", m.callbackTimeout,
		"timeout_check_interval", m.timeoutCheckInterval,
	)

	// Start callback timeout checker in background
	go m.runTimeoutChecker(ctx)

	return m.wmRouter.Run(ctx)
}

// runTimeoutChecker periodically checks for timed-out provisions and rejects them.
func (m *Manager) runTimeoutChecker(ctx context.Context) {
	ticker := time.NewTicker(m.timeoutCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkCallbackTimeouts(ctx)
		}
	}
}

// checkCallbackTimeouts checks for provisions that have exceeded the callback timeout
// and rejects them so the tenant's credit is released.
func (m *Manager) checkCallbackTimeouts(ctx context.Context) {
	now := time.Now()
	var timedOut []inFlightProvision

	// Find timed-out provisions under read lock
	m.inFlightMu.RLock()
	for _, p := range m.inFlight {
		if now.Sub(p.StartTime) > m.callbackTimeout {
			timedOut = append(timedOut, p)
		}
	}
	m.inFlightMu.RUnlock()

	if len(timedOut) == 0 {
		return
	}

	slog.Warn("found timed-out provisions",
		"count", len(timedOut),
		"timeout", m.callbackTimeout,
	)

	// Process each timed-out provision
	for _, p := range timedOut {
		// Check context before each operation
		if ctx.Err() != nil {
			return
		}

		// Remove from in-flight first
		m.UntrackInFlight(p.LeaseUUID)
		metrics.CallbackTimeoutsTotal.Inc()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, p.Backend).Inc()

		// Record duration (from start until timeout)
		duration := now.Sub(p.StartTime).Seconds()
		metrics.ProvisioningDuration.WithLabelValues(p.Backend).Observe(duration)

		// Reject the lease on chain so tenant's credit is released
		rejected, txHashes, err := m.chainClient.RejectLeases(ctx, []string{p.LeaseUUID}, "callback timeout")
		if err != nil {
			slog.Error("failed to reject timed-out lease",
				"lease_uuid", p.LeaseUUID,
				"error", err,
			)
			// Continue with next - reconciler will pick this up
			continue
		}

		slog.Warn("rejected timed-out provision",
			"lease_uuid", p.LeaseUUID,
			"tenant", p.Tenant,
			"backend", p.Backend,
			"age", now.Sub(p.StartTime),
			"rejected", rejected,
			"tx_hashes", txHashes,
		)
	}
}

// Close shuts down the provision manager.
func (m *Manager) Close() error {
	// Log in-flight provisions to help operators understand state during shutdown
	count := m.InFlightCount()
	if count > 0 {
		slog.Warn("shutting down with in-flight provisions",
			"count", count,
			"note", "these will be recovered by reconciliation on restart",
		)
	}

	// Close Watermill router
	if err := m.wmRouter.Close(); err != nil {
		return err
	}

	// Close payload store if configured
	if m.payloadStore != nil {
		if err := m.payloadStore.Close(); err != nil {
			slog.Error("failed to close payload store", "error", err)
			return err
		}
	}

	return nil
}

// InFlightCount returns the number of provisions currently in flight.
func (m *Manager) InFlightCount() int {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	return len(m.inFlight)
}

// GetInFlightLeases returns a snapshot of all in-flight lease UUIDs.
// Used for logging during shutdown.
func (m *Manager) GetInFlightLeases() []string {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()

	leases := make([]string, 0, len(m.inFlight))
	for uuid := range m.inFlight {
		leases = append(leases, uuid)
	}
	return leases
}

// WaitForDrain waits for all in-flight provisions to complete, up to the given timeout.
// Returns the number of provisions that were still in-flight when the timeout expired.
// If all provisions complete before the timeout, returns 0.
func (m *Manager) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	if m.InFlightCount() == 0 {
		return 0
	}

	slog.Info("waiting for in-flight provisions to drain",
		"count", m.InFlightCount(),
		"timeout", timeout,
	)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			remaining := m.InFlightCount()
			if remaining > 0 {
				slog.Warn("drain interrupted by context cancellation",
					"remaining", remaining,
					"leases", m.GetInFlightLeases(),
				)
			}
			return remaining

		case <-ticker.C:
			count := m.InFlightCount()
			if count == 0 {
				slog.Info("all in-flight provisions drained successfully")
				return 0
			}

			if time.Now().After(deadline) {
				slog.Warn("drain timeout expired with provisions still in-flight",
					"remaining", count,
					"leases", m.GetInFlightLeases(),
				)
				return count
			}

			slog.Debug("waiting for provisions to drain",
				"remaining", count,
				"time_left", time.Until(deadline).Round(time.Second),
			)
		}
	}
}

// PublishLeaseEvent publishes a chain event to the appropriate Watermill topic.
// This is called by the chain event subscriber.
func (m *Manager) PublishLeaseEvent(event chain.LeaseEvent) error {
	var topic string
	switch event.Type {
	case chain.LeaseCreated:
		topic = TopicLeaseCreated
	case chain.LeaseClosed:
		topic = TopicLeaseClosed
	case chain.LeaseExpired:
		topic = TopicLeaseExpired
	default:
		// Other event types are not handled by provisioner
		return nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	return m.publisher.Publish(topic, msg)
}

// PublishCallback publishes a backend callback to Watermill.
// This is called by the API server when it receives a callback.
func (m *Manager) PublishCallback(callback backend.CallbackPayload) error {
	payload, err := json.Marshal(callback)
	if err != nil {
		return fmt.Errorf("marshal callback: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	return m.publisher.Publish(TopicBackendCallback, msg)
}

// recordWatermillMetrics records the outcome of a Watermill message handler.
func recordWatermillMetrics(topic string, err error) {
	outcome := metrics.OutcomeSuccess
	if err != nil {
		outcome = metrics.OutcomeError
	}
	metrics.WatermillMessagesTotal.WithLabelValues(topic, outcome).Inc()
}

// handleLeaseCreated processes new lease events.
func (m *Manager) handleLeaseCreated(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseCreated, err) }()

	var event chain.LeaseEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		metrics.MalformedMessagesTotal.WithLabelValues(TopicLeaseCreated).Inc()
		slog.Error("failed to unmarshal lease created event",
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return nil // Don't retry malformed messages
	}

	slog.Info("processing lease created",
		"lease_uuid", event.LeaseUUID,
		"tenant", event.Tenant,
	)

	// Fetch lease details from chain to get SKU for routing
	lease, err := m.chainClient.GetLease(msg.Context(), event.LeaseUUID)
	if err != nil {
		slog.Error("failed to fetch lease details",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("failed to fetch lease %s: %w", event.LeaseUUID, err)
	}
	if lease == nil {
		slog.Warn("lease not found, skipping",
			"lease_uuid", event.LeaseUUID,
		)
		return nil
	}

	// Check if lease requires a payload (has MetaHash)
	// If so, skip immediate provisioning - wait for payload upload
	if len(lease.MetaHash) > 0 {
		slog.Info("lease has meta_hash, awaiting payload upload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
		)
		return nil // Don't provision yet - wait for payload
	}

	// Extract primary SKU from lease items for routing
	sku := ExtractPrimarySKU(lease)

	// Route to appropriate backend based on SKU (Route already falls back to default)
	backendClient := m.router.Route(sku)
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, event.LeaseUUID)
	}

	// Atomically track in-flight BEFORE calling Provision to prevent:
	// 1. Race with reconciler (TOCTOU between IsInFlight check and TrackInFlight)
	// 2. Race with fast backend response (callback arriving before tracking)
	if !m.TryTrackInFlight(event.LeaseUUID, event.Tenant, sku, backendClient.Name()) {
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", event.LeaseUUID,
		)
		return nil
	}

	// Start provisioning (async - backend will call back)
	err = backendClient.Provision(msg.Context(), backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		SKU:          sku,
		CallbackURL:  BuildCallbackURL(m.callbackBaseURL),
	})
	if err != nil {
		// Clean up in-flight tracking on failure
		m.UntrackInFlight(event.LeaseUUID)

		slog.Error("failed to start provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
			"backend", backendClient.Name(),
			"error", err,
		)
		return fmt.Errorf("%w: %w", ErrProvisioningFailed, err)
	}

	slog.Info("provisioning started",
		"lease_uuid", event.LeaseUUID,
		"sku", sku,
		"backend", backendClient.Name(),
	)

	return nil
}

// ExtractPrimarySKU returns the SKU UUID from the first lease item.
// For multi-SKU leases, we route based on the first SKU (all SKUs in a lease
// must belong to the same provider, so routing by the first is sufficient).
func ExtractPrimarySKU(lease *billingtypes.Lease) string {
	if lease == nil || len(lease.Items) == 0 {
		return ""
	}
	return lease.Items[0].SkuUuid
}

// handleLeaseClosed processes lease closure events.
func (m *Manager) handleLeaseClosed(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseClosed, err) }()

	var event chain.LeaseEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		metrics.MalformedMessagesTotal.WithLabelValues(TopicLeaseClosed).Inc()
		slog.Error("failed to unmarshal lease closed event",
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return nil // Don't retry malformed messages
	}

	slog.Info("processing lease closed", "lease_uuid", event.LeaseUUID)

	// Remove from in-flight if present (atomic check-and-delete)
	provision, wasInFlight := m.PopInFlight(event.LeaseUUID)

	// Determine the backend for deprovisioning.
	// Priority:
	// 1. From in-flight tracking (most reliable - we know exactly where it's provisioned)
	// 2. Route by SKU (consistent with provisioning path)
	// 3. Fallback: deprovision from all backends (ensures cleanup even if routing differs)
	var backendClient backend.Backend

	if wasInFlight && provision.Backend != "" {
		// Case 1: Was in-flight - use the tracked backend
		backendClient = m.router.GetBackendByName(provision.Backend)
		if backendClient == nil {
			slog.Warn("backend not found by name, will route by SKU",
				"lease_uuid", event.LeaseUUID,
				"backend_name", provision.Backend,
			)
		}
	}

	if backendClient == nil {
		// Case 2: Try to route by SKU (fetch from chain if we have SKU, or query lease)
		var sku string
		if wasInFlight && provision.SKU != "" {
			sku = provision.SKU
		} else {
			// Fetch lease details from chain to get SKU for routing
			// This ensures consistency with the provisioning path
			lease, err := m.chainClient.GetLease(msg.Context(), event.LeaseUUID)
			if err != nil {
				slog.Warn("failed to fetch lease details for deprovision routing",
					"lease_uuid", event.LeaseUUID,
					"error", err,
				)
				// Fall through to deprovision from all backends
			} else if lease != nil {
				sku = ExtractPrimarySKU(lease)
			}
		}

		if sku != "" {
			backendClient = m.router.Route(sku)
			slog.Debug("routing deprovision by SKU",
				"lease_uuid", event.LeaseUUID,
				"sku", sku,
				"backend", backendClient.Name(),
			)
		}
	}

	if backendClient != nil {
		// Deprovision from the determined backend
		if err := backendClient.Deprovision(msg.Context(), event.LeaseUUID); err != nil {
			slog.Error("failed to deprovision",
				"lease_uuid", event.LeaseUUID,
				"backend", backendClient.Name(),
				"error", err,
			)
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, event.LeaseUUID, err)
		}

		slog.Info("deprovisioned successfully",
			"lease_uuid", event.LeaseUUID,
			"backend", backendClient.Name(),
		)
	} else {
		// Case 3: Fallback - deprovision from all backends
		// This ensures cleanup even if we can't determine the correct backend
		slog.Warn("could not determine backend for deprovision, trying all backends",
			"lease_uuid", event.LeaseUUID,
		)

		var lastErr error
		deprovisioned := false
		for _, b := range m.router.Backends() {
			if err := b.Deprovision(msg.Context(), event.LeaseUUID); err != nil {
				slog.Debug("deprovision from backend returned error",
					"lease_uuid", event.LeaseUUID,
					"backend", b.Name(),
					"error", err,
				)
				lastErr = err
			} else {
				slog.Info("deprovisioned successfully",
					"lease_uuid", event.LeaseUUID,
					"backend", b.Name(),
				)
				deprovisioned = true
			}
		}

		if !deprovisioned && lastErr != nil {
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, event.LeaseUUID, lastErr)
		}
	}

	return nil
}

// handleLeaseExpired processes lease expiration events.
func (m *Manager) handleLeaseExpired(msg *message.Message) error {
	// Same handling as closed - deprovision the resource
	return m.handleLeaseClosed(msg)
}

// handleBackendCallback processes callbacks from backends.
func (m *Manager) handleBackendCallback(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()

	var callback backend.CallbackPayload
	if err := json.Unmarshal(msg.Payload, &callback); err != nil {
		metrics.MalformedMessagesTotal.WithLabelValues(TopicBackendCallback).Inc()
		slog.Error("failed to unmarshal backend callback",
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return nil // Don't retry malformed messages
	}

	// Check if this lease is in-flight (idempotency check)
	// Ignore callbacks for unknown/already-processed leases to prevent:
	// - Duplicate on-chain transactions from replay attacks
	// - Processing misrouted callbacks from other providers
	provision, exists := m.GetInFlight(callback.LeaseUUID)
	if !exists {
		slog.Warn("ignoring callback for unknown or already-processed lease",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
		return nil // Don't retry - this is not an error
	}

	slog.Info("processing backend callback",
		"lease_uuid", callback.LeaseUUID,
		"status", callback.Status,
		"backend", provision.Backend,
	)

	// Record provisioning duration if we have the start time
	recordDuration := func() {
		if !provision.StartTime.IsZero() {
			duration := time.Since(provision.StartTime).Seconds()
			metrics.ProvisioningDuration.WithLabelValues(provision.Backend).Observe(duration)
		}
	}

	switch callback.Status {
	case backend.CallbackStatusSuccess:
		// Acknowledge the lease on chain
		acknowledged, txHashes, err := m.chainClient.AcknowledgeLeases(msg.Context(), []string{callback.LeaseUUID})
		if err != nil {
			// Check if this is a terminal error (e.g., lease already acknowledged)
			if isTerminalAcknowledgeError(err) {
				// Lease is already in a non-PENDING state (likely already ACTIVE).
				// This can happen if we received a duplicate callback or the reconciler
				// already acknowledged it. Treat as success - the lease is active.
				m.UntrackInFlight(callback.LeaseUUID)
				recordDuration()
				metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend).Inc()
				slog.Info("lease already acknowledged, skipping",
					"lease_uuid", callback.LeaseUUID,
				)
				return nil
			}

			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
			// Keep in-flight tracking for retry - Watermill will retry this message
			return fmt.Errorf("%w: lease %s: %w", ErrAcknowledgeFailed, callback.LeaseUUID, err)
		}

		// Only remove from in-flight after successful acknowledgment
		m.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend).Inc()

		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.LeaseUUID,
			"acknowledged", acknowledged,
			"tx_hashes", txHashes,
		)

	case backend.CallbackStatusFailed:
		// Remove from in-flight - this is a terminal state, no retry needed
		m.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeFailed, provision.Backend).Inc()

		// Reject the lease on chain so tenant's credit is released immediately
		reason := callback.Error
		if reason == "" {
			reason = "provisioning failed"
		}
		rejected, txHashes, err := m.chainClient.RejectLeases(msg.Context(), []string{callback.LeaseUUID}, reason)
		if err != nil {
			// Log but don't retry - the lease will eventually expire if rejection fails
			slog.Error("failed to reject lease after provisioning failure",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
		} else {
			slog.Info("lease rejected after provisioning failure",
				"lease_uuid", callback.LeaseUUID,
				"rejected", rejected,
				"tx_hashes", txHashes,
				"reason", reason,
			)
		}

	default:
		// Unknown status is treated as terminal to prevent leases from being stuck
		// in the in-flight map indefinitely. The reconciler will pick up the lease
		// and handle it based on its actual chain/backend state.
		m.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, provision.Backend).Inc()

		slog.Warn("unknown callback status, treating as terminal",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
	}

	return nil
}

// PublishPayload publishes a payload received event to Watermill.
// This is called by the API server when it receives a valid payload upload.
func (m *Manager) PublishPayload(event PayloadEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal payload event: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	return m.publisher.Publish(TopicPayloadReceived, msg)
}

// StorePayload stores a payload in the payload store.
// Returns false if a payload already exists for this lease (conflict),
// or if the payload store is not configured.
func (m *Manager) StorePayload(leaseUUID string, payload []byte) bool {
	if m.payloadStore == nil {
		slog.Warn("payload store not configured, cannot store payload", "lease_uuid", leaseUUID)
		return false
	}
	return m.payloadStore.Store(leaseUUID, payload)
}

// HasPayload checks if a payload exists for a lease.
// Returns false if the payload store is not configured.
func (m *Manager) HasPayload(leaseUUID string) bool {
	if m.payloadStore == nil {
		return false
	}
	return m.payloadStore.Has(leaseUUID)
}

// DeletePayload removes a payload from the store.
// Used for rollback when publish fails after store succeeds.
// No-op if the payload store is not configured.
func (m *Manager) DeletePayload(leaseUUID string) {
	if m.payloadStore == nil {
		return
	}
	m.payloadStore.Delete(leaseUUID)
}

// PayloadStore returns the payload store for reconciliation access.
// May return nil if payload store is not configured.
func (m *Manager) PayloadStore() *PayloadStore {
	return m.payloadStore
}

// handlePayloadReceived processes payload upload events.
// This triggers provisioning for leases that were waiting for a payload.
func (m *Manager) handlePayloadReceived(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicPayloadReceived, err) }()

	// Guard against nil payloadStore - this shouldn't happen in normal operation
	// since payload events are only published after successful storage, but
	// handle it gracefully for robustness.
	if m.payloadStore == nil {
		slog.Error("payload store not configured, cannot process payload event")
		return nil // Don't retry - configuration issue
	}

	var event PayloadEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		metrics.MalformedMessagesTotal.WithLabelValues(TopicPayloadReceived).Inc()
		slog.Error("failed to unmarshal payload event",
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return nil // Don't retry malformed messages
	}

	slog.Info("processing payload received",
		"lease_uuid", event.LeaseUUID,
		"tenant", event.Tenant,
	)

	// Fetch lease details from chain to get SKU for routing
	lease, err := m.chainClient.GetLease(msg.Context(), event.LeaseUUID)
	if err != nil {
		slog.Error("failed to fetch lease details",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("failed to fetch lease %s: %w", event.LeaseUUID, err)
	}
	if lease == nil {
		slog.Warn("lease not found, cleaning up payload",
			"lease_uuid", event.LeaseUUID,
		)
		m.payloadStore.Delete(event.LeaseUUID)
		return nil
	}

	// Verify lease is still pending
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		slog.Warn("lease is no longer pending, skipping provisioning",
			"lease_uuid", event.LeaseUUID,
			"state", lease.State.String(),
		)
		// Clean up the stored payload
		m.payloadStore.Delete(event.LeaseUUID)
		return nil
	}

	// Extract primary SKU for routing
	sku := ExtractPrimarySKU(lease)

	// Route to appropriate backend based on SKU
	backendClient := m.router.Route(sku)
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, event.LeaseUUID)
	}

	// Atomically track in-flight BEFORE calling Provision
	if !m.TryTrackInFlight(event.LeaseUUID, event.Tenant, sku, backendClient.Name()) {
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", event.LeaseUUID,
		)
		return nil
	}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	payload := m.payloadStore.Get(event.LeaseUUID)
	if payload == nil {
		// This shouldn't happen in normal operation since payload is stored
		// before publishing the event, but handle it gracefully
		slog.Warn("payload not found in store, proceeding without payload",
			"lease_uuid", event.LeaseUUID,
		)
	}

	// Build provision request - only include PayloadHash when we have the actual payload.
	// This ensures backends never receive a hash without the corresponding data.
	req := backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		SKU:          sku,
		CallbackURL:  BuildCallbackURL(m.callbackBaseURL),
		Payload:      payload,
	}
	if payload != nil && event.MetaHashHex != "" {
		req.PayloadHash = event.MetaHashHex
	}

	// Start provisioning with payload
	err = backendClient.Provision(msg.Context(), req)
	if err != nil {
		// Clean up in-flight tracking on failure.
		// Keep payload in store so reconciliation can retry with it.
		m.UntrackInFlight(event.LeaseUUID)

		slog.Error("failed to start provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
			"backend", backendClient.Name(),
			"error", err,
		)
		return fmt.Errorf("%w: %w", ErrProvisioningFailed, err)
	}

	// Provision succeeded - now safe to delete the payload from store
	if payload != nil {
		m.payloadStore.Delete(event.LeaseUUID)
	}

	slog.Info("provisioning started with payload",
		"lease_uuid", event.LeaseUUID,
		"sku", sku,
		"backend", backendClient.Name(),
		"payload_size", len(payload),
	)

	return nil
}
