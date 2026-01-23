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

// Watermill topic names for internal event routing.
const (
	TopicLeaseCreated    = "events.lease.created"
	TopicLeaseClosed     = "events.lease.closed"
	TopicLeaseExpired    = "events.lease.expired"
	TopicBackendCallback = "events.backend.callback"
)

// CallbackPath is the path suffix for backend provision callbacks.
const CallbackPath = "/callbacks/provision"

// ChainClient defines the chain operations needed by the provisioner.
type ChainClient interface {
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
}

// Manager handles the provisioning lifecycle using Watermill for event routing.
type Manager struct {
	providerUUID    string
	callbackBaseURL string
	router          *backend.Router
	chainClient     ChainClient
	publisher       message.Publisher
	wmRouter        *message.Router

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

// callbackURL returns the callback URL for backend provisioning.
func (m *Manager) callbackURL() string {
	return m.callbackBaseURL + CallbackPath
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

	return m, nil
}

// Start begins the Watermill router.
func (m *Manager) Start(ctx context.Context) error {
	slog.Info("starting provision manager")
	return m.wmRouter.Run(ctx)
}

// Close shuts down the provision manager.
func (m *Manager) Close() error {
	return m.wmRouter.Close()
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

	// TODO(phase-3): Implement SKU-based routing.
	// Currently we always use the default backend. To route by SKU prefix
	// (as documented in config), we need to fetch lease details from chain
	// to get the SKU, then call m.router.Route(sku). For now, all leases
	// go to the default backend regardless of sku_prefix configuration.
	backendClient := m.router.Default()
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", event.LeaseUUID,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, event.LeaseUUID)
	}

	// Track in-flight provision
	m.TrackInFlight(event.LeaseUUID, event.Tenant, "", backendClient.Name())

	// Start provisioning (async - backend will call back)
	err := backendClient.Provision(msg.Context(), backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		CallbackURL:  m.callbackURL(),
	})
	if err != nil {
		slog.Error("failed to start provisioning",
			"lease_uuid", event.LeaseUUID,
			"backend", backendClient.Name(),
			"error", err,
		)
		// Remove from in-flight
		m.UntrackInFlight(event.LeaseUUID)

		// Watermill will retry; TODO(phase-3): Add lease rejection after max retries
		return fmt.Errorf("%w: %v", ErrProvisioningFailed, err)
	}

	slog.Info("provisioning started",
		"lease_uuid", event.LeaseUUID,
		"backend", backendClient.Name(),
	)

	return nil
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
		return fmt.Errorf("%w: lease %s: %v", ErrDeprovisionFailed, event.LeaseUUID, err)
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
			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
			// Keep in-flight tracking for retry - Watermill will retry this message
			return fmt.Errorf("%w: lease %s: %v", ErrAcknowledgeFailed, callback.LeaseUUID, err)
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

		// TODO(phase-3): Add RejectLease to chain client to reject failed provisions
		// For now, just log the failure; the lease will eventually expire
		slog.Warn("provisioning failed",
			"lease_uuid", callback.LeaseUUID,
			"error", callback.Error,
		)

	default:
		slog.Warn("ignoring callback with unknown status",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
	}

	return nil
}
