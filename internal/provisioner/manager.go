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

// poisonTopic is the Watermill dead-letter topic for messages that exhaust retries.
const poisonTopic = "events.poison"

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
	placementStore  PlacementStore
	ackBatcher      *AckBatcher

	// stopCtx bounds work that outlives the call which started it — today the
	// ack batcher's lanes. It is created in NewManager and rooted at
	// context.Background(), deliberately NOT derived from the ctx passed to
	// Start. Rooting it at Background preserves the lanes' pre-ENG-723 lifetime
	// exactly (NewManager used to start them on a bare context.Background()):
	// the only thing that ends them is Close(). Deriving from Start's ctx would
	// instead couple lane teardown to a context main cancels partway through
	// its shutdown sequence, several steps before it calls Close(). stopCancel
	// fires it (ENG-723; same ownership shape as ENG-592). Mirrors
	// internal/backend/docker.Backend.stopCtx.
	stopCtx    context.Context
	stopCancel context.CancelFunc

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

	// leaseEventSink receives lease status events for real-time delivery (e.g., WebSocket)
	leaseEventSink LeaseEventSink
}

// LeaseEventSink receives lease status events for real-time delivery (e.g., WebSocket).
type LeaseEventSink interface {
	Publish(event backend.LeaseStatusEvent)
}

// ManagerConfig configures the provision manager.
type ManagerConfig struct {
	ProviderUUID         string
	CallbackBaseURL      string         // Base URL for backend callbacks (e.g., "http://fred.example.com:8080")
	PayloadStore         *payload.Store // Optional external payload store (if nil, manager won't handle payloads)
	PlacementStore       PlacementStore // Optional placement store for round-robin routing (nil = disabled)
	LeaseEventSink       LeaseEventSink // Optional sink for real-time lease events (nil = disabled)
	CallbackTimeout      time.Duration  // Timeout for backend callbacks (default: 10 minutes, 0 = disabled)
	TimeoutCheckInterval time.Duration  // How often to check for timeouts (default: 1 minute)
	AckBatchInterval     time.Duration  // How long to wait before flushing ack batch (default: DefaultAckBatchInterval)
	AckBatchSize         int            // Maximum acks to batch before flushing (default: DefaultAckBatchSize)
	AckLaneCount         int            // Number of parallel ack lanes (default: 1)
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
	poisonQueue, err := middleware.PoisonQueue(pubSub, poisonTopic)
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
		LaneCount:     cfg.AckLaneCount,
	})
	// The batcher is deliberately NOT started here: a long-lived goroutine set
	// must be owned by a lifecycle, not by a constructor. Start() launches it
	// (see the ordering note there). Nothing can reach the Acknowledger before
	// then — the only two callers are the Watermill backend-callback handler,
	// which does not exist until wmRouter.Run subscribes it, and the reconciler,
	// whose first ack is gated behind <-Running() in cmd/providerd/main.go.

	tracker := NewInFlightTracker()
	orchestrator := NewProvisionOrchestrator(cfg.ProviderUUID, cfg.CallbackBaseURL, router, tracker, cfg.PlacementStore)
	handlers := NewHandlerSet(HandlerDeps{
		ChainClient:   chainClient,
		Orchestrator:  orchestrator,
		Tracker:       tracker,
		Acknowledger:  ackBatcher,
		PayloadStore:  cfg.PayloadStore,
		Publisher:     pubSub,
		BackendRouter: router,
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
		placementStore:       cfg.PlacementStore,
		ackBatcher:           ackBatcher,
		tracker:              tracker,
		orchestrator:         orchestrator,
		handlers:             handlers,
		timeoutChecker:       timeoutChecker,
		callbackTimeout:      callbackTimeout,
		timeoutCheckInterval: timeoutCheckInterval,
		leaseEventSink:       cfg.LeaseEventSink,
	}

	m.stopCtx, m.stopCancel = context.WithCancel(context.Background())

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

	// Forward lease events to event sink (if configured)
	if cfg.LeaseEventSink != nil {
		wmRouter.AddNoPublisherHandler(
			"forward_lease_events",
			TopicLeaseEvent,
			pubSub,
			m.forwardToEventSink,
		)
	}

	// Handle poisoned messages: log and drop them to prevent infinite loops
	wmRouter.AddNoPublisherHandler(
		"handle_poison_queue",
		poisonTopic,
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

// forwardToEventSink is a Watermill handler that deserializes LeaseStatusEvent messages
// and forwards them to the event sink for real-time client delivery.
func (m *Manager) forwardToEventSink(msg *message.Message) error {
	var event backend.LeaseStatusEvent
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		slog.Warn("failed to unmarshal lease event", "error", err)
		return nil // Don't retry malformed messages
	}

	m.leaseEventSink.Publish(event)
	return nil
}

// Start begins the Watermill router and callback timeout checker.
func (m *Manager) Start(ctx context.Context) error {
	slog.Info("starting provision manager",
		"callback_timeout", m.callbackTimeout,
		"timeout_check_interval", m.timeoutCheckInterval,
	)

	// Start the ack batcher before wmRouter.Run below, which is what subscribes
	// the handlers that call Acknowledge(). Watermill's Running() gate is not
	// what protects this: Router.Run calls RunHandlers(ctx) — which subscribes
	// each handler and spawns its goroutine — and only then closes the running
	// channel, so a message can already be in a handler before any waiter
	// observes Running().
	//
	// It runs on m.stopCtx, not on ctx: ctx is canceled partway through main's
	// shutdown sequence, several steps before Close(), and the batcher's
	// lifetime belongs to Close(). Start is once-only; a second call is a no-op.
	m.ackBatcher.Start(m.stopCtx)

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

// AckBatcher returns the batcher as an Acknowledger for use by the reconciler.
func (m *Manager) AckBatcher() Acknowledger {
	return m.ackBatcher
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
	// Handlers may still be inside AckBatcher.Acknowledge(); stopping the
	// batcher first would fail those acks (and any Watermill retry of them)
	// against lanes that are already winding down.
	//
	// Every step below runs even when this one fails. Returning early here
	// would skip the batcher shutdown and the lifecycle-context cancellation,
	// leaking exactly the goroutines this method exists to reclaim.
	routerErr := m.wmRouter.Close()

	// Stop ack batcher AFTER all handlers have finished.
	// Stop() cancels the lanes' context, so each batchLoop takes its shutdown
	// path: one last flush, then it fails everything still queued so no caller
	// is left blocked on a result. That final flush is best-effort only — it
	// issues its GetPendingLeases/AcknowledgeLeases on the context Stop() just
	// canceled, so against the real gRPC chain client those calls fail. That is
	// pre-existing behavior; nothing here depends on the flush landing.
	// Safe when Start() was never called: Stop() nil-guards the cancel and
	// waits on an empty WaitGroup.
	if m.ackBatcher != nil {
		m.ackBatcher.Stop()
	}

	// Release the lifecycle context. Stop() above already canceled the batcher's
	// own derived context and waited for its lanes; this cancels the parent so
	// anything else later rooted at m.stopCtx is bounded by Close() too.
	if m.stopCancel != nil {
		m.stopCancel()
	}

	// Note: The timeout checker goroutine exits when its context is canceled.
	// In production, the context is canceled before Close() is called,
	// so the goroutine will have already exited or will exit promptly.
	// We don't wait here because tests may call Close() without canceling the context.

	// Close payload store if configured
	var payloadErr error
	if m.payloadStore != nil {
		if payloadErr = m.payloadStore.Close(); payloadErr != nil {
			slog.Error("failed to close payload store", "error", payloadErr)
		}
	}

	return errors.Join(routerErr, payloadErr)
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

// OverwritePayload replaces the stored payload for a lease, recording the new
// payload's own hash alongside it (ENG-619).
//
// This is the durable half of a tenant /update: without it the update reaches
// the backend but not the store the reconciler replays from, so the next
// reprovision — a reboot, a crash-restart, a host failure — silently reverts the
// tenant to the manifest they created the lease with.
//
// Returns ErrPayloadStoreUnavailable when no payload store is configured. That
// is not a no-op worth swallowing: the caller has already applied the update to
// the backend, and reporting success would repeat the exact lie this ticket
// exists to remove.
func (m *Manager) OverwritePayload(leaseUUID string, payload []byte) error {
	if m.payloadStore == nil {
		return ErrPayloadStoreUnavailable
	}
	return m.payloadStore.Put(leaseUUID, payload)
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
