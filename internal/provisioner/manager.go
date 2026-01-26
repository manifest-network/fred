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
}

type inFlightProvision struct {
	LeaseUUID string
	Tenant    string
	SKU       string
	Backend   string
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
	}
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
	}
	return true
}

// UntrackInFlight removes a lease from the in-flight tracking.
func (m *Manager) UntrackInFlight(leaseUUID string) {
	m.inFlightMu.Lock()
	defer m.inFlightMu.Unlock()
	delete(m.inFlight, leaseUUID)
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
	ProviderUUID    string
	CallbackBaseURL string // Base URL for backend callbacks (e.g., "http://fred.example.com:8080")
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

	m := &Manager{
		providerUUID:    cfg.ProviderUUID,
		callbackBaseURL: cfg.CallbackBaseURL,
		router:          router,
		chainClient:     chainClient,
		publisher:       pubSub,
		wmRouter:        wmRouter,
		payloadStore:    NewPayloadStore(),
		inFlight:        make(map[string]inFlightProvision),
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

// Start begins the Watermill router.
func (m *Manager) Start(ctx context.Context) error {
	slog.Info("starting provision manager")
	return m.wmRouter.Run(ctx)
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
	return m.wmRouter.Close()
}

// InFlightCount returns the number of provisions currently in flight.
func (m *Manager) InFlightCount() int {
	m.inFlightMu.RLock()
	defer m.inFlightMu.RUnlock()
	return len(m.inFlight)
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

// handleLeaseCreated processes new lease events.
func (m *Manager) handleLeaseCreated(msg *message.Message) error {
	var event chain.LeaseEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
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
func (m *Manager) handleLeaseClosed(msg *message.Message) error {
	var event chain.LeaseEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		slog.Error("failed to unmarshal lease closed event",
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return nil // Don't retry malformed messages
	}

	slog.Info("processing lease closed", "lease_uuid", event.LeaseUUID)

	// Remove from in-flight if present (atomic check-and-delete)
	provision, exists := m.PopInFlight(event.LeaseUUID)

	// Get the backend (either from in-flight info or route by default)
	var backendClient backend.Backend
	if exists && provision.Backend != "" {
		backendClient = m.router.GetBackendByName(provision.Backend)
		if backendClient == nil {
			slog.Warn("backend not found by name, falling back to default",
				"lease_uuid", event.LeaseUUID,
				"backend_name", provision.Backend,
			)
		}
	}
	if backendClient == nil {
		backendClient = m.router.Default()
	}
	if backendClient == nil {
		slog.Error("no backend available for deprovision",
			"lease_uuid", event.LeaseUUID,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, event.LeaseUUID)
	}

	// Deprovision (idempotent)
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

	return nil
}

// handleLeaseExpired processes lease expiration events.
func (m *Manager) handleLeaseExpired(msg *message.Message) error {
	// Same handling as closed - deprovision the resource
	return m.handleLeaseClosed(msg)
}

// handleBackendCallback processes callbacks from backends.
func (m *Manager) handleBackendCallback(msg *message.Message) error {
	var callback backend.CallbackPayload
	if err := json.Unmarshal(msg.Payload, &callback); err != nil {
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

		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.LeaseUUID,
			"acknowledged", acknowledged,
			"tx_hashes", txHashes,
		)

	case backend.CallbackStatusFailed:
		// Remove from in-flight - this is a terminal state, no retry needed
		m.UntrackInFlight(callback.LeaseUUID)

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
// Returns false if a payload already exists for this lease (conflict).
func (m *Manager) StorePayload(leaseUUID string, payload []byte) bool {
	return m.payloadStore.Store(leaseUUID, payload)
}

// HasPayload checks if a payload exists for a lease.
func (m *Manager) HasPayload(leaseUUID string) bool {
	return m.payloadStore.Has(leaseUUID)
}

// DeletePayload removes a payload from the store.
// Used for rollback when publish fails after store succeeds.
func (m *Manager) DeletePayload(leaseUUID string) {
	m.payloadStore.Delete(leaseUUID)
}

// PayloadStore returns the payload store for reconciliation access.
func (m *Manager) PayloadStore() *PayloadStore {
	return m.payloadStore
}

// handlePayloadReceived processes payload upload events.
// This triggers provisioning for leases that were waiting for a payload.
func (m *Manager) handlePayloadReceived(msg *message.Message) error {
	var event PayloadEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
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

	// Pop the payload from the store
	payload := m.payloadStore.Pop(event.LeaseUUID)
	if payload == nil {
		// This shouldn't happen in normal operation since payload is stored
		// before publishing the event, but handle it gracefully
		slog.Warn("payload not found in store, proceeding without payload",
			"lease_uuid", event.LeaseUUID,
		)
	}

	// Start provisioning with payload
	err = backendClient.Provision(msg.Context(), backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		SKU:          sku,
		CallbackURL:  BuildCallbackURL(m.callbackBaseURL),
		Payload:      payload,
		PayloadHash:  event.MetaHashHex,
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

	slog.Info("provisioning started with payload",
		"lease_uuid", event.LeaseUUID,
		"sku", sku,
		"backend", backendClient.Name(),
		"payload_size", len(payload),
	)

	return nil
}
