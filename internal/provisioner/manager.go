package provisioner

import (
	"cmp"
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
	GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}

// Compile-time check that Manager implements InFlightTracker.
var _ InFlightTracker = (*Manager)(nil)

// Manager handles the provisioning lifecycle using Watermill for event routing.
type Manager struct {
	providerUUID    string
	callbackBaseURL string
	router          *backend.Router
	chainClient     ChainClient
	publisher       message.Publisher
	wmRouter        *message.Router
	payloadStore    *PayloadStore
	ackBatcher      *AckBatcher

	// Track in-flight provisions (ephemeral - recovered via reconciliation)
	inFlight   map[string]inFlightProvision
	inFlightMu sync.RWMutex

	// Callback timeout handling
	callbackTimeout      time.Duration
	timeoutCheckInterval time.Duration
}

// ManagerConfig configures the provision manager.
type ManagerConfig struct {
	ProviderUUID         string
	CallbackBaseURL      string        // Base URL for backend callbacks (e.g., "http://fred.example.com:8080")
	PayloadStore         *PayloadStore // Optional external payload store (if nil, manager won't handle payloads)
	CallbackTimeout      time.Duration // Timeout for backend callbacks (default: 10 minutes, 0 = disabled)
	TimeoutCheckInterval time.Duration // How often to check for timeouts (default: 1 minute)
	AckBatchInterval     time.Duration // How long to wait before flushing ack batch (default: DefaultAckBatchInterval)
	AckBatchSize         int           // Maximum acks to batch before flushing (default: DefaultAckBatchSize)
}

// NewManager creates a new provision manager with Watermill routing.
func NewManager(cfg ManagerConfig, router *backend.Router, chainClient ChainClient) (*Manager, error) {
	if router == nil {
		return nil, errors.New("backend router is required")
	}
	if chainClient == nil {
		return nil, errors.New("chain client is required")
	}
	if cfg.ProviderUUID == "" {
		return nil, errors.New("provider UUID is required")
	}
	if cfg.CallbackBaseURL == "" {
		return nil, errors.New("callback base URL is required")
	}

	// Apply defaults for callback timeout using cmp.Or
	callbackTimeout := cmp.Or(cfg.CallbackTimeout, 10*time.Minute)
	timeoutCheckInterval := cmp.Or(cfg.TimeoutCheckInterval, 1*time.Minute)

	// Create Watermill logger adapter
	wmLogger := watermill.NewSlogLogger(slog.Default())

	// Create in-memory pub/sub (ephemeral - messages don't survive crash)
	pubSub := gochannel.NewGoChannel(gochannel.Config{}, wmLogger)

	// Create Watermill router
	wmRouter, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		return nil, fmt.Errorf("create router: %w", err)
	}

	// Add middleware for retries and panic recovery
	wmRouter.AddMiddleware(
		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     time.Second,
			Multiplier:      2,
			Logger:          wmLogger,
		}.Middleware,
		middleware.Recoverer,
	)

	// Create ack batcher (NewAckBatcher applies defaults internally via cmp.Or)
	ackBatcher := NewAckBatcher(chainClient, AckBatcherConfig{
		ProviderUUID:  cfg.ProviderUUID,
		BatchInterval: cfg.AckBatchInterval,
		BatchSize:     cfg.AckBatchSize,
	})
	// Start ack batcher immediately so handlers can use it without waiting for Start()
	// This uses a background context; Stop() will still properly shut it down.
	ackBatcher.Start(context.Background())

	m := &Manager{
		providerUUID:         cfg.ProviderUUID,
		callbackBaseURL:      cfg.CallbackBaseURL,
		router:               router,
		chainClient:          chainClient,
		publisher:            pubSub,
		wmRouter:             wmRouter,
		payloadStore:         cfg.PayloadStore,
		ackBatcher:           ackBatcher,
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

	// Note: ack batcher is started in NewManager() so handlers can use it immediately

	// Start callback timeout checker in background
	go m.runTimeoutChecker(ctx)

	// Run Watermill router (blocks until ctx canceled)
	return m.wmRouter.Run(ctx)
}

// Running returns a channel that is closed when the router is running.
// This can be used to wait for the manager to be ready before publishing events.
func (m *Manager) Running() chan struct{} {
	return m.wmRouter.Running()
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

	// Stop ack batcher first to flush pending acks
	if m.ackBatcher != nil {
		m.ackBatcher.Stop()
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
