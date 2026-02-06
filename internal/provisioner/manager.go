package provisioner

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// Compile-time check that Manager implements ReconcilerTracker.
var _ ReconcilerTracker = (*Manager)(nil)

// Manager handles the provisioning lifecycle using Watermill for event routing.
type Manager struct {
	providerUUID    string
	callbackBaseURL string
	router          *backend.Router
	chainClient     ChainClient
	publisher       message.Publisher
	wmRouter        *message.Router
	payloadStore    *payload.Store
	ackBatcher      *AckBatcher

	// Track in-flight provisions (ephemeral - recovered via reconciliation)
	tracker InFlightTracker

	// Orchestrator for provisioning coordination
	orchestrator *ProvisionOrchestrator

	// Handler set for Watermill message handlers
	handlers *HandlerSet

	// Timeout checker for callback timeouts
	timeoutChecker *TimeoutChecker

	// Callback timeout handling (stored for external access if needed)
	callbackTimeout      time.Duration
	timeoutCheckInterval time.Duration
}

// ManagerConfig configures the provision manager.
type ManagerConfig struct {
	ProviderUUID         string
	CallbackBaseURL      string         // Base URL for backend callbacks (e.g., "http://fred.example.com:8080")
	PayloadStore         *payload.Store // Optional external payload store (if nil, manager won't handle payloads)
	CallbackTimeout      time.Duration  // Timeout for backend callbacks (default: 10 minutes, 0 = disabled)
	TimeoutCheckInterval time.Duration  // How often to check for timeouts (default: 1 minute)
	AckBatchInterval     time.Duration  // How long to wait before flushing ack batch (default: DefaultAckBatchInterval)
	AckBatchSize         int            // Maximum acks to batch before flushing (default: DefaultAckBatchSize)
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

	// Add poison queue middleware to catch messages that exhaust all retries.
	// This prevents infinite retry loops: after Retry exhausts its attempts,
	// PoisonQueue intercepts the error, publishes the message to a dead-letter
	// topic, and returns nil — acknowledging the message and breaking the loop.
	poisonQueue, err := middleware.PoisonQueue(pubSub, "events.poison")
	if err != nil {
		return nil, fmt.Errorf("create poison queue middleware: %w", err)
	}

	// Add middleware: poison queue (outermost) → retry → recoverer (innermost)
	wmRouter.AddMiddleware(
		poisonQueue,
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

	tracker := NewInFlightTracker()
	orchestrator := NewProvisionOrchestrator(cfg.ProviderUUID, cfg.CallbackBaseURL, router, tracker)
	handlers := NewHandlerSet(HandlerDeps{
		ChainClient:  chainClient,
		Orchestrator: orchestrator,
		Tracker:      tracker,
		Acknowledger: ackBatcher,
		PayloadStore: cfg.PayloadStore,
	})
	timeoutChecker := NewTimeoutChecker(TimeoutCheckerConfig{
		Tracker:       tracker,
		Rejecter:      chainClient,
		Timeout:       callbackTimeout,
		CheckInterval: timeoutCheckInterval,
	})

	m := &Manager{
		providerUUID:         cfg.ProviderUUID,
		callbackBaseURL:      cfg.CallbackBaseURL,
		router:               router,
		chainClient:          chainClient,
		publisher:            pubSub,
		wmRouter:             wmRouter,
		payloadStore:         cfg.PayloadStore,
		ackBatcher:           ackBatcher,
		tracker:              tracker,
		orchestrator:         orchestrator,
		handlers:             handlers,
		timeoutChecker:       timeoutChecker,
		callbackTimeout:      callbackTimeout,
		timeoutCheckInterval: timeoutCheckInterval,
	}

	// Register handlers
	wmRouter.AddNoPublisherHandler(
		"handle_lease_created",
		TopicLeaseCreated,
		pubSub,
		handlers.HandleLeaseCreated,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_lease_closed",
		TopicLeaseClosed,
		pubSub,
		handlers.HandleLeaseClosed,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_lease_expired",
		TopicLeaseExpired,
		pubSub,
		handlers.HandleLeaseExpired,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_backend_callback",
		TopicBackendCallback,
		pubSub,
		handlers.HandleBackendCallback,
	)

	wmRouter.AddNoPublisherHandler(
		"handle_payload_received",
		TopicPayloadReceived,
		pubSub,
		handlers.HandlePayloadReceived,
	)

	// Handle poisoned messages: log and drop them to prevent infinite loops
	wmRouter.AddNoPublisherHandler(
		"handle_poison_queue",
		"events.poison",
		pubSub,
		func(msg *message.Message) error {
			slog.Error("message moved to poison queue after all retries exhausted",
				"message_uuid", msg.UUID,
				"poisoned_topic", msg.Metadata.Get(middleware.PoisonedTopicKey),
				"poisoned_handler", msg.Metadata.Get(middleware.PoisonedHandlerKey),
				"reason", msg.Metadata.Get(middleware.ReasonForPoisonedKey),
			)
			metrics.PoisonedMessagesTotal.Inc()
			return nil
		},
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

	// Start callback timeout checker in background.
	// This goroutine exits when ctx is canceled, which happens before Close() in production.
	go m.timeoutChecker.Start(ctx)

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

	// Close Watermill router FIRST to drain in-progress handlers.
	// Handlers may call AckBatcher.Acknowledge() which sends to b.requests.
	// If we stopped the batcher first, its batchLoop would close b.requests,
	// and any handler still running would panic on send-to-closed-channel.
	if err := m.wmRouter.Close(); err != nil {
		return err
	}

	// Stop ack batcher AFTER all handlers have finished.
	// This flushes any pending ack requests that were queued by handlers.
	if m.ackBatcher != nil {
		m.ackBatcher.Stop()
	}

	// Note: The timeout checker goroutine exits when its context is canceled.
	// In production, the context is canceled before Close() is called,
	// so the goroutine will have already exited or will exit promptly.
	// We don't wait here because tests may call Close() without canceling the context.

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

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), data)
	return m.publisher.Publish(topic, msg)
}

// PublishCallback publishes a backend callback to Watermill.
// This is called by the API server when it receives a callback.
func (m *Manager) PublishCallback(callback backend.CallbackPayload) error {
	data, err := json.Marshal(callback)
	if err != nil {
		return fmt.Errorf("marshal callback: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), data)
	return m.publisher.Publish(TopicBackendCallback, msg)
}

// PublishPayload publishes a payload received event to Watermill.
// This is called by the API server when it receives a valid payload upload.
func (m *Manager) PublishPayload(event payload.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal payload event: %w", err)
	}

	msg := message.NewMessage(watermill.NewUUID(), data)
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
func (m *Manager) HasPayload(leaseUUID string) (bool, error) {
	if m.payloadStore == nil {
		return false, nil
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
func (m *Manager) PayloadStore() *payload.Store {
	return m.payloadStore
}
