package provisioner

import (
	"context"
	"encoding/json"
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

// Watermill topic names for internal event routing.
const (
	TopicLeaseCreated   = "events.lease.created"
	TopicLeaseClosed    = "events.lease.closed"
	TopicLeaseExpired   = "events.lease.expired"
	TopicBackendCallback = "events.backend.callback"
)

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
	inFlightMu sync.Mutex
}

type inFlightProvision struct {
	LeaseUUID string
	Tenant    string
	SKU       string
	Backend   string
}

// ManagerConfig configures the provision manager.
type ManagerConfig struct {
	ProviderUUID    string
	CallbackBaseURL string // Base URL for backend callbacks (e.g., "http://fred.example.com:8080")
}

// NewManager creates a new provision manager with Watermill routing.
func NewManager(cfg ManagerConfig, router *backend.Router, chainClient ChainClient) (*Manager, error) {
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
		slog.Error("failed to unmarshal lease created event", "error", err)
		return nil // Don't retry malformed messages
	}

	slog.Info("processing lease created",
		"lease_uuid", event.LeaseUUID,
		"tenant", event.Tenant,
	)

	// Route to appropriate backend based on SKU
	// Phase 2: Fetch lease details from chain to get SKU for routing
	// For now, use the default backend
	backendClient := m.router.Default()

	// Track in-flight provision
	m.inFlightMu.Lock()
	m.inFlight[event.LeaseUUID] = inFlightProvision{
		LeaseUUID: event.LeaseUUID,
		Tenant:    event.Tenant,
		Backend:   backendClient.Name(),
	}
	m.inFlightMu.Unlock()

	// Build callback URL
	callbackURL := fmt.Sprintf("%s/callbacks/provision", m.callbackBaseURL)

	// Start provisioning (async - backend will call back)
	err := backendClient.Provision(msg.Context(), backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		CallbackURL:  callbackURL,
	})
	if err != nil {
		slog.Error("failed to start provisioning",
			"lease_uuid", event.LeaseUUID,
			"backend", backendClient.Name(),
			"error", err,
		)
		// Remove from in-flight
		m.inFlightMu.Lock()
		delete(m.inFlight, event.LeaseUUID)
		m.inFlightMu.Unlock()

		// Watermill will retry; Phase 2 will add lease rejection after max retries
		return err
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
		slog.Error("failed to unmarshal lease closed event", "error", err)
		return nil
	}

	slog.Info("processing lease closed", "lease_uuid", event.LeaseUUID)

	// Remove from in-flight if present
	m.inFlightMu.Lock()
	provision, exists := m.inFlight[event.LeaseUUID]
	delete(m.inFlight, event.LeaseUUID)
	m.inFlightMu.Unlock()

	// Get the backend (either from in-flight info or route by default)
	var backendClient backend.Backend
	if exists {
		backendClient = m.router.GetBackendByName(provision.Backend)
	}
	if backendClient == nil {
		backendClient = m.router.Default()
	}

	// Deprovision (idempotent)
	if err := backendClient.Deprovision(msg.Context(), event.LeaseUUID); err != nil {
		slog.Error("failed to deprovision",
			"lease_uuid", event.LeaseUUID,
			"backend", backendClient.Name(),
			"error", err,
		)
		return err // Retry
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
		slog.Error("failed to unmarshal backend callback", "error", err)
		return nil
	}

	slog.Info("processing backend callback",
		"lease_uuid", callback.LeaseUUID,
		"status", callback.Status,
	)

	// Remove from in-flight
	m.inFlightMu.Lock()
	delete(m.inFlight, callback.LeaseUUID)
	m.inFlightMu.Unlock()

	switch callback.Status {
	case "success":
		// Acknowledge the lease on chain
		acknowledged, txHashes, err := m.chainClient.AcknowledgeLeases(msg.Context(), []string{callback.LeaseUUID})
		if err != nil {
			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
			return err // Retry
		}

		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.LeaseUUID,
			"acknowledged", acknowledged,
			"tx_hashes", txHashes,
		)

	case "failed":
		// Phase 2: Add RejectLease to chain client to reject failed provisions
		// For now, just log the failure; the lease will eventually expire
		slog.Warn("provisioning failed",
			"lease_uuid", callback.LeaseUUID,
			"error", callback.Error,
		)
	}

	return nil
}
