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
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

// poisonTopic is the Watermill dead-letter topic for messages that exhaust retries.
const poisonTopic = "events.poison"

var errCallbackRuntimeUnavailable = errors.New("backend callback runtime is not accepting callbacks")

// Manager is the typed production runtime and owns the single process-local
// operation registry shared through narrow consumer capability ports.

// Manager handles the provisioning lifecycle. Chain and payload events use
// Watermill; backend callbacks use a synchronous application path so the
// backend's durable per-lease ordering survives provider ingress.
type Manager struct {
	providerUUID    string
	publisher       message.Publisher
	callbackHandler func(context.Context, hmacauth.VerifiedRequest) error
	wmRouter        *message.Router
	payloadStore    *payload.Store
	ackBatcher      *AckBatcher

	// callbackAdmissionMu closes the admission gate atomically with respect to
	// callbackWG.Add. Close can therefore wait for every admitted callback
	// before stopping the ack batcher without racing a late HTTP request.
	callbackAdmissionMu sync.Mutex
	callbackAccepting   bool
	callbackClosed      bool
	callbackWG          sync.WaitGroup
	callbackStopCtx     context.Context
	callbackStopCancel  context.CancelFunc

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

	// timeoutWG owns the checker started by Start. timeoutMu closes the narrow
	// Start/Close publication race around its derived cancellation function, so
	// Close always joins the goroutine even when the caller forgot to cancel the
	// Start context.
	timeoutMu         sync.Mutex
	timeoutStarted    bool
	timeoutClosed     bool
	timeoutStopCancel context.CancelFunc
	timeoutWG         sync.WaitGroup

	operationRuntime     operation.RuntimeController
	executionCoordinator *placement.ExecutionCoordinator
	handlers             *HandlerSet

	// Timeout checker for callback timeouts
	timeoutChecker *TimeoutChecker

	// Callback timeout handling (stored for external access if needed)
	callbackTimeout      time.Duration
	timeoutCheckInterval time.Duration

	// leaseEventSink receives lease status events for real-time delivery (e.g., WebSocket)
	leaseEventSink LeaseEventSink
}

// ManagerChainClient is the single chain capability shared by every
// provisioner application assembled by Manager, including reconciliation's
// ACTIVE inventory. Narrow consumers such as AckBatcher still receive only
// ChainClient.
type ManagerChainClient interface {
	ChainClient
	GetActiveLeasesByProvider(context.Context, string) ([]billingtypes.Lease, error)
}

// managerProviderControlPlane is the sole broad placement composition port.
// It joins the one production chain client to the acknowledgement batcher
// built over that same client; only this value crosses into placement wiring.
type managerProviderControlPlane struct {
	chain ManagerChainClient
	ack   *AckBatcher
}

func newManagerProviderControlPlane(
	chainClient ManagerChainClient,
	cfg AckBatcherConfig,
) (*managerProviderControlPlane, *AckBatcher, error) {
	if util.IsNilInterface(chainClient) {
		return nil, nil, errors.New("manager chain client is required")
	}
	ackBatcher := NewAckBatcher(chainClient, cfg)
	return &managerProviderControlPlane{chain: chainClient, ack: ackBatcher}, ackBatcher, nil
}

func (control *managerProviderControlPlane) GetLease(
	ctx context.Context, leaseUUID string,
) (*billingtypes.Lease, error) {
	lease, err := control.chain.GetLease(ctx, leaseUUID)
	// The concrete gRPC client historically represents query NotFound as an
	// empty successful response. Normalize that transport convention only at
	// this production adapter boundary; the placement observation algebra keeps
	// arbitrary nil,nil responses uncertain.
	if lease == nil && err == nil {
		return nil, billingtypes.ErrLeaseNotFound
	}
	return lease, err
}
func (control *managerProviderControlPlane) GetPendingLeases(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	return control.chain.GetPendingLeases(ctx, providerUUID)
}
func (control *managerProviderControlPlane) GetActiveLeasesByProvider(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	return control.chain.GetActiveLeasesByProvider(ctx, providerUUID)
}
func (control *managerProviderControlPlane) RejectLeases(
	ctx context.Context, leaseUUIDs []string, reason string,
) (uint64, []string, error) {
	return control.chain.RejectLeases(ctx, leaseUUIDs, reason)
}
func (control *managerProviderControlPlane) CloseLeases(
	ctx context.Context, leaseUUIDs []string, reason string,
) (uint64, []string, error) {
	return control.chain.CloseLeases(ctx, leaseUUIDs, reason)
}
func (control *managerProviderControlPlane) Acknowledge(
	ctx context.Context, leaseUUID string,
) (bool, string, error) {
	return control.ack.Acknowledge(ctx, leaseUUID)
}

var _ placement.ProviderControlPlane = (*managerProviderControlPlane)(nil)

// LeaseEventSink receives lease status events for real-time delivery (e.g., WebSocket).
type LeaseEventSink interface {
	Publish(event backend.LeaseStatusEvent)
}

// ManagerConfig configures the provision manager.
type ManagerConfig struct {
	ProviderUUID          string
	PayloadStore          *payload.Store                 // Optional external payload store (if nil, manager won't handle payloads)
	PlacementStore        *placement.Store               // Required durable multi-backend placement authority
	LeaseEventSink        LeaseEventSink                 // Optional sink for real-time lease events (nil = disabled)
	CallbackTimeout       time.Duration                  // Timeout for backend callbacks (default: 10 minutes, 0 = disabled)
	TimeoutCheckInterval  time.Duration                  // How often to check for timeouts (default: 1 minute)
	AckBatchInterval      time.Duration                  // How long to wait before flushing ack batch (default: DefaultAckBatchInterval)
	AckBatchSize          int                            // Maximum acks to batch before flushing (default: DefaultAckBatchSize)
	AckLaneCount          int                            // Number of parallel ack lanes (default: 1)
	CallbackProofConsumer hmacauth.CallbackProofConsumer // Exact API verifier boundary accepted for settlement.
}

// NewManager creates a new provision manager with Watermill routing.
func NewManager(cfg ManagerConfig, router *backend.Router, chainClient ManagerChainClient) (*Manager, error) {
	if router == nil {
		return nil, errors.New("backend router is required")
	}
	if util.IsNilInterface(chainClient) {
		return nil, errors.New("chain client is required")
	}
	if cfg.ProviderUUID == "" {
		return nil, errors.New("provider UUID is required")
	}
	if cfg.PlacementStore == nil {
		return nil, ErrPlacementStoreUnavailable
	}
	if err := cfg.PlacementStore.VerifyProviderUUID(cfg.ProviderUUID); err != nil {
		return nil, fmt.Errorf("verify placement provider authority: %w", err)
	}
	// Manager is an independently constructible event-driven runtime. It may
	// verify the provider- and identity-bearing topology committed by the
	// composition root, but must never mutate either from configuration alone.
	if err := cfg.PlacementStore.VerifyBackendTopology(
		backendTopologyNames(router),
	); err != nil {
		return nil, fmt.Errorf("verify placement backend topology: %w", err)
	}
	if !cfg.CallbackProofConsumer.Valid() {
		return nil, errors.New("callback proof consumer is required")
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

	// Construct the one provider control plane and its lifecycle-owned batcher
	// together, so chain reads/writes can never be paired with an acknowledger
	// built over another client.
	providerControlPlane, ackBatcher, err := newManagerProviderControlPlane(chainClient, AckBatcherConfig{
		ProviderUUID:  cfg.ProviderUUID,
		BatchInterval: cfg.AckBatchInterval,
		BatchSize:     cfg.AckBatchSize,
		LaneCount:     cfg.AckLaneCount,
	})
	if err != nil {
		return nil, fmt.Errorf("bind provider control plane: %w", err)
	}
	// The batcher is deliberately NOT started here: a long-lived goroutine set
	// must be owned by a lifecycle, not by a constructor. Start() launches it
	// (see the ordering note there). The synchronous callback admission gate is
	// opened only after that launch, and the reconciler's first ack is gated
	// behind <-Running() in cmd/providerd/main.go.

	operationCoordinator, err := cfg.PlacementStore.BindOperationCoordinator(func(count int) {
		metrics.InFlightProvisions.Set(float64(count))
	})
	if err != nil {
		return nil, fmt.Errorf("bind operation settlement coordinator: %w", err)
	}
	executionCoordinator, err := operationCoordinator.BindBackendRuntime(router, providerControlPlane)
	if err != nil {
		return nil, fmt.Errorf("bind backend execution coordinator: %w", err)
	}
	provisionCoordinator, err := executionCoordinator.ProvisionCoordinatorWithPayloads(
		placement.ProvisionStartObserver(func(leaseUUID, _ string) {
			// Provision start and callback completion use the same synchronous
			// sink. Sending only the terminal side directly would let a queued
			// Watermill Provisioning event overtake Ready/Failed at subscribers.
			publishLeaseStatusToSink(
				cfg.LeaseEventSink, leaseUUID, backend.ProvisionStatusProvisioning, "",
			)
		}),
		cfg.PayloadStore,
	)
	if err != nil {
		return nil, fmt.Errorf("bind provision execution coordinator: %w", err)
	}
	orchestrator, err := NewProvisionOrchestrator(provisionCoordinator)
	if err != nil {
		return nil, fmt.Errorf("create provision orchestrator: %w", err)
	}
	var callbackEvents CallbackEventSink
	if cfg.LeaseEventSink != nil {
		callbackEvents = callbackEventSinkFunc(func(
			leaseUUID string, status backend.ProvisionStatus, failure string,
		) {
			// Callback application is synchronous so the backend's per-lease
			// outbox order reaches subscribers unchanged. Routing these events
			// back through Watermill would reintroduce concurrent handler
			// execution after the callback itself had already been ordered.
			publishLeaseStatusToSink(cfg.LeaseEventSink, leaseUUID, status, failure)
		})
	}
	callbackCoordinator, err := executionCoordinator.AuthenticatedCallbackCoordinator(
		cfg.CallbackProofConsumer,
	)
	if err != nil {
		return nil, fmt.Errorf("bind authenticated callback coordinator: %w", err)
	}
	deprovisionObserver := provisionCoordinator.DeprovisionCompletionObserver()
	callbacks, err := NewCallbackService(CallbackServiceConfig{
		Coordinator:         callbackCoordinator,
		Payloads:            cfg.PayloadStore,
		Events:              callbackEvents,
		Backends:            router,
		DeprovisionObserver: deprovisionObserver,
	})
	if err != nil {
		return nil, fmt.Errorf("create callback service: %w", err)
	}
	handlers, err := NewHandlerSet(HandlerDeps{
		Events:       orchestrator.HandlerEvents(),
		PayloadStore: cfg.PayloadStore,
		Publisher:    pubSub,
		Callbacks:    callbacks,
	})
	if err != nil {
		return nil, fmt.Errorf("create handler set: %w", err)
	}
	timeoutCoordinator, err := executionCoordinator.TimeoutCoordinator()
	if err != nil {
		return nil, fmt.Errorf("bind callback timeout coordinator: %w", err)
	}
	timeoutChecker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator:   timeoutCoordinator,
		Timeout:       callbackTimeout,
		CheckInterval: timeoutCheckInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("create callback timeout checker: %w", err)
	}

	m := &Manager{
		providerUUID:         cfg.ProviderUUID,
		publisher:            pubSub,
		callbackHandler:      handlers.HandleBackendCallbackEvidence,
		wmRouter:             wmRouter,
		payloadStore:         cfg.PayloadStore,
		ackBatcher:           ackBatcher,
		operationRuntime:     operationCoordinator.RuntimeController(),
		executionCoordinator: executionCoordinator,
		handlers:             handlers,
		timeoutChecker:       timeoutChecker,
		callbackTimeout:      callbackTimeout,
		timeoutCheckInterval: timeoutCheckInterval,
		leaseEventSink:       cfg.LeaseEventSink,
	}

	m.stopCtx, m.stopCancel = context.WithCancel(context.Background())
	m.callbackStopCtx, m.callbackStopCancel = context.WithCancel(context.Background())

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

// backendTopologyNames returns the router's exact durable storage identities.
// It intentionally preserves the router's inventory boundary; canonical
// sorting and validation belong to the placement store that persists it.
func backendTopologyNames(router backendRouter) []string {
	if util.IsNilInterface(router) {
		return nil
	}
	backends := router.Backends()
	names := make([]string, 0, len(backends))
	for _, candidate := range backends {
		if candidate == nil {
			names = append(names, "")
			continue
		}
		names = append(names, candidate.Name())
	}
	return names
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

// PublishProvisionStarting implements the narrow start-event capability used
// by reconciliation. Publishing remains best-effort and uses the same direct,
// synchronous sink as event-driven provisioning start events.
func (m *Manager) PublishProvisionStarting(leaseUUID string) {
	publishLeaseStatusToSink(
		m.leaseEventSink, leaseUUID, backend.ProvisionStatusProvisioning, "",
	)
}

// publishLeaseStatusToSink applies a subscriber-visible status synchronously.
// Provision start and callback completion deliberately share this boundary so
// their program order cannot be inverted by Watermill's concurrent handlers.
func publishLeaseStatusToSink(
	sink LeaseEventSink,
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	if sink == nil {
		return
	}
	sink.Publish(backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
		Timestamp: time.Now(),
	})
}

// Start begins the Watermill router and callback timeout checker.
func (m *Manager) Start(ctx context.Context) error {
	slog.Info("starting provision manager",
		"callback_timeout", m.callbackTimeout,
		"timeout_check_interval", m.timeoutCheckInterval,
	)

	// Start the ack batcher before opening synchronous callback admission or
	// running the Watermill handlers. Watermill's Running() gate cannot protect
	// the direct callback path, so admission is an explicit lifecycle boundary.
	//
	// It runs on m.stopCtx, not on ctx: ctx is canceled partway through main's
	// shutdown sequence, several steps before Close(), and the batcher's
	// lifetime belongs to Close(). Start is once-only; a second call is a no-op.
	m.ackBatcher.Start(m.stopCtx)
	m.openCallbackAdmission()
	defer m.pauseCallbackAdmission()

	// The timeout checker is Manager-owned, panic-contained at its external
	// reject boundary, and joined by Close. The caller context still stops it
	// during normal orchestration shutdown; m.stopCtx covers direct Close calls.
	m.timeoutMu.Lock()
	if !m.timeoutStarted && !m.timeoutClosed {
		timeoutCtx, timeoutCancel := context.WithCancel(m.stopCtx)
		stopOnCallerCancel := context.AfterFunc(ctx, timeoutCancel)
		m.timeoutStarted = true
		m.timeoutStopCancel = timeoutCancel
		m.timeoutWG.Go(func() {
			defer stopOnCallerCancel()
			m.timeoutChecker.Start(timeoutCtx)
		})
	}
	m.timeoutMu.Unlock()

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
	// Close may be called directly by tests or by a composition root that did
	// not perform the graceful WaitForDrain sequence. Establish the same
	// irreversible ordinary-work admission barrier defensively before closing
	// any of the settlement paths below.
	m.BeginDrain()

	// Reject new callback requests before draining either execution path. Direct
	// callbacks are not owned by Watermill, so the router alone cannot account
	// for them during shutdown.
	m.closeCallbackAdmission()
	// Cancel every admitted callback before waiting. API shutdown is bounded,
	// and a chain node may otherwise keep an HTTP-derived callback context alive
	// far beyond that bound. The backend retains the durable outbox head on the
	// resulting 503/canceled response and retries after restart or recovery.
	if m.callbackStopCancel != nil {
		m.callbackStopCancel()
	}
	// Stop timeout settlement before draining handlers. It holds exact operation
	// claims while calling the chain; its bounded context releases those claims
	// before stores and other manager-owned lifecycle components are closed.
	m.timeoutMu.Lock()
	m.timeoutClosed = true
	if m.timeoutStopCancel != nil {
		m.timeoutStopCancel()
	}
	m.timeoutMu.Unlock()
	m.timeoutWG.Wait()

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

	// Every callback admitted before the gate closed may still be using the ack
	// batcher. Drain them before stopping its lanes.
	m.callbackWG.Wait()

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

// PublishCallback applies a backend callback synchronously. The docker
// backend's durable outbox waits for this call's HTTP response before sending
// the next callback for the lease; returning only after application preserves
// exact-completion-before-lifecycle ordering end to end. Chain and payload
// events remain on Watermill, but its router intentionally starts each message
// handler in a separate goroutine and therefore cannot provide this ordering
// boundary.
func (m *Manager) PublishCallback(ctx context.Context, callback hmacauth.VerifiedRequest) error {
	if !m.admitCallback() {
		return errCallbackRuntimeUnavailable
	}
	defer m.callbackWG.Done()

	if m.callbackHandler == nil {
		return errCallbackOperationsUnavailable
	}

	// Merge request cancellation with Manager ownership. Close cancels the
	// latter before callbackWG.Wait, so shutdown cannot wedge behind a chain RPC
	// whose client disconnected or whose request context was detached.
	callbackCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.callbackStopCtx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	return m.callbackHandler(callbackCtx, callback)
}

// openCallbackAdmission makes the synchronous callback path available only
// after its ack dependency has started. A manager that has begun Close remains
// permanently closed even if Start is called out of lifecycle order.
func (m *Manager) openCallbackAdmission() {
	m.callbackAdmissionMu.Lock()
	defer m.callbackAdmissionMu.Unlock()
	if !m.callbackClosed {
		m.callbackAccepting = true
	}
}

// pauseCallbackAdmission rejects callbacks after Start's runtime has exited.
// Admitted work remains owned by callbackWG and is drained by Close.
func (m *Manager) pauseCallbackAdmission() {
	m.callbackAdmissionMu.Lock()
	m.callbackAccepting = false
	m.callbackAdmissionMu.Unlock()
}

// closeCallbackAdmission permanently closes the gate. Holding the mutex while
// changing callbackAccepting orders the change against callbackWG.Add in
// admitCallback, making the subsequent Wait safe.
func (m *Manager) closeCallbackAdmission() {
	m.callbackAdmissionMu.Lock()
	m.callbackClosed = true
	m.callbackAccepting = false
	m.callbackAdmissionMu.Unlock()
}

func (m *Manager) admitCallback() bool {
	m.callbackAdmissionMu.Lock()
	defer m.callbackAdmissionMu.Unlock()
	if !m.callbackAccepting {
		return false
	}
	m.callbackWG.Add(1)
	return true
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
