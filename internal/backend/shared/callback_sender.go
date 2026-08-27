package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const (
	// CallbackMaxAttempts is the number of times to attempt callback delivery.
	CallbackMaxAttempts = 3

	// DefaultCallbackReplayInterval bounds how long an exhausted callback stays
	// dormant while the backend process remains up.
	DefaultCallbackReplayInterval = 30 * time.Second

	// callbackReplayWorkerLimit bounds replay fan-out during a large outage.
	// Multiple workers preserve per-lease isolation without creating one
	// goroutine and one simultaneous HTTP retry chain per queued lease.
	callbackReplayWorkerLimit = 16
)

// defaultCallbackBackoff defines the default delay before each retry attempt.
var defaultCallbackBackoff = [CallbackMaxAttempts]time.Duration{0, 1 * time.Second, 5 * time.Second}

// CallbackSender handles HMAC-signed callback delivery with retry and persistence.
type CallbackSender struct {
	store          *CallbackStore
	httpClient     *http.Client
	secret         string
	logger         *slog.Logger
	stopCtx        context.Context
	backoff        [CallbackMaxAttempts]time.Duration
	attemptTimeout time.Duration
	replayInterval time.Duration
	onDelivery     func(outcome string) // nil-safe; injected by the caller for metrics
	onStoreError   func()               // nil-safe; called when bbolt persistence fails
	onReplayPanic  func(any)            // nil-safe; called when one lease replay panics
	// deliveryLocks serialize enqueue + FIFO drain for exactly one lease. Each
	// entry is reference-counted and deleted after its final holder unlocks, so
	// tenant-controlled lease IDs cannot grow the registry indefinitely and an
	// unhealthy lease cannot head-of-line block an unrelated one.
	deliveryLocksMu *sync.Mutex
	deliveryLocks   map[string]*callbackLeaseLock
}

type callbackLeaseLock struct {
	mu   sync.Mutex
	refs uint64
}

// CallbackSenderConfig configures a CallbackSender.
type CallbackSenderConfig struct {
	Store          *CallbackStore
	HTTPClient     *http.Client
	Secret         string
	Logger         *slog.Logger
	StopCtx        context.Context
	OnDelivery     func(outcome string)                // optional metrics callback
	OnStoreError   func()                              // optional; called when bbolt persistence fails
	OnReplayPanic  func(any)                           // optional; called after recovering a lease replay panic
	Backoff        *[CallbackMaxAttempts]time.Duration // retry delays; nil uses default {0, 1s, 5s}
	AttemptTimeout time.Duration                       // per-request timeout; zero uses backend.DefaultCallbackDeliveryTimeout
	ReplayInterval time.Duration                       // zero uses DefaultCallbackReplayInterval
}

// NewCallbackSender creates a new CallbackSender.
// Panics if HTTPClient, Logger, or StopCtx is nil (programming error).
// Store may be nil to disable callback persistence (callbacks will not survive restarts).
func NewCallbackSender(cfg CallbackSenderConfig) *CallbackSender {
	if cfg.HTTPClient == nil {
		panic("shared.NewCallbackSender: HTTPClient must not be nil")
	}
	if cfg.Logger == nil {
		panic("shared.NewCallbackSender: Logger must not be nil")
	}
	if cfg.StopCtx == nil {
		panic("shared.NewCallbackSender: StopCtx must not be nil")
	}
	if cfg.ReplayInterval < 0 {
		panic("shared.NewCallbackSender: ReplayInterval must not be negative")
	}
	if cfg.AttemptTimeout < 0 {
		panic("shared.NewCallbackSender: AttemptTimeout must not be negative")
	}

	backoff := defaultCallbackBackoff
	if cfg.Backoff != nil {
		backoff = *cfg.Backoff
	}
	replayInterval := cfg.ReplayInterval
	if replayInterval == 0 {
		replayInterval = DefaultCallbackReplayInterval
	}
	attemptTimeout := cfg.AttemptTimeout
	if attemptTimeout == 0 {
		attemptTimeout = backend.DefaultCallbackDeliveryTimeout
	}

	deliveryLocksMu := &sync.Mutex{}
	deliveryLocks := make(map[string]*callbackLeaseLock)
	if cfg.Store != nil {
		deliveryLocksMu = cfg.Store.deliveryLocksMu
		deliveryLocks = cfg.Store.deliveryLocks
	}

	return &CallbackSender{
		store:           cfg.Store,
		httpClient:      cfg.HTTPClient,
		secret:          cfg.Secret,
		logger:          cfg.Logger,
		stopCtx:         cfg.StopCtx,
		backoff:         backoff,
		attemptTimeout:  attemptTimeout,
		replayInterval:  replayInterval,
		onDelivery:      cfg.OnDelivery,
		onStoreError:    cfg.OnStoreError,
		onReplayPanic:   cfg.OnReplayPanic,
		deliveryLocksMu: deliveryLocksMu,
		deliveryLocks:   deliveryLocks,
	}
}

// SendOperationCallback sends an exact requested-operation completion with an
// HMAC signature. It persists the callback before delivery and drains the
// lease FIFO, so a newer observation can never overtake this completion.
// The caller must provide the callbackURL (resolved from its own state) and
// the backendName (so Fred can label metrics per-backend without a placement
// lookup, which is often already deleted for intentional deprovisions).
func (s *CallbackSender) SendOperationCallback(leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string) {
	s.sendOperationCallback(context.Background(), leaseUUID, callbackURL, backendName, status, errMsg)
}

// SendOperationCallbackContext is the cancellation-aware operation API for a
// worker whose owning lease may be torn down before enqueue. Cancellation is
// checked after acquiring the sender's keyed lease lock, closing the race where
// a deprovision removes the queue just before a stale worker persists its exact
// completion. Once persisted, the durable outbox remains the delivery owner.
func (s *CallbackSender) SendOperationCallbackContext(ctx context.Context, leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string) {
	if ctx == nil {
		s.logger.Error("refusing operation callback with nil ownership context", "lease_uuid", leaseUUID)
		return
	}
	s.sendOperationCallback(ctx, leaseUUID, callbackURL, backendName, status, errMsg)
}

func (s *CallbackSender) sendOperationCallback(ctx context.Context, leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string) {
	if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed {
		s.logger.Error("refusing invalid operation callback status",
			"status", status,
			"lease_uuid", leaseUUID,
		)
		return
	}
	if callbackURL != "" {
		if err := backend.ValidateOperationCallbackURL(callbackURL); err != nil {
			s.logger.Error("refusing invalid operation callback URL",
				"error", err,
				"lease_uuid", leaseUUID,
			)
			return
		}
	}
	s.enqueueAndDrain(ctx, leaseUUID, callbackURL, backendName, status, errMsg, false, CallbackDeliveryKindOperation)
}

// SendLifecycleCallback sends a typed, observation-only lifecycle callback.
// Successful maintenance completion, autonomous failure, and teardown
// observations belong on this route; provision/restore completion requires
// the exact operation callback URL. Enqueue atomically coalesces older typed
// lifecycle observations, but never operation completions or protected
// legacy/unknown records.
func (s *CallbackSender) SendLifecycleCallback(leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string, retained bool) {
	if status != backend.CallbackStatusSuccess &&
		status != backend.CallbackStatusFailed &&
		status != backend.CallbackStatusDeprovisioned {
		s.logger.Error("refusing invalid lifecycle callback status",
			"status", status,
			"lease_uuid", leaseUUID,
		)
		return
	}
	if retained && status != backend.CallbackStatusDeprovisioned {
		s.logger.Error("refusing retained flag on non-deprovision lifecycle callback",
			"status", status,
			"lease_uuid", leaseUUID,
		)
		return
	}
	if callbackURL != "" {
		if err := backend.ValidateLifecycleCallbackURL(callbackURL); err != nil {
			s.logger.Error("refusing invalid lifecycle callback URL",
				"error", err,
				"lease_uuid", leaseUUID,
			)
			return
		}
	}
	s.enqueueAndDrain(context.Background(), leaseUUID, callbackURL, backendName, status, errMsg, retained, CallbackDeliveryKindLifecycle)
}

func (s *CallbackSender) enqueueAndDrain(ownerCtx context.Context, leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string, retained bool, kind CallbackDeliveryKind) {
	if callbackURL == "" {
		s.logger.Warn("no callback URL for lease", "lease_uuid", leaseUUID)
		return
	}

	entry := CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  callbackURL,
		DeliveryKind: kind,
		Success:      status != backend.CallbackStatusFailed,
		Status:       status,
		Backend:      backendName,
		Error:        errMsg,
		Retained:     retained,
		CreatedAt:    time.Now(),
	}

	unlock := s.lockLease(leaseUUID)
	defer unlock()
	if ownerCtx != nil {
		if err := ownerCtx.Err(); err != nil {
			s.logger.Debug("suppressing callback enqueue for canceled lease operation",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			return
		}
	}

	if s.store == nil {
		body, err := callbackEntryPayload(entry)
		if err != nil {
			s.logger.Error("failed to marshal callback payload", "error", err, "lease_uuid", leaseUUID)
			return
		}
		s.DeliverCallback(leaseUUID, callbackURL, body)
		return
	}

	// Persist callback before attempting delivery so it survives restarts.
	// Success remains populated with the pre-Status encoding for schema
	// continuity. New entries live in the v2 queue, which v0.13 deliberately does
	// not read; see DEPLOYMENT.md for the callback-store rollback boundary.
	if _, err := s.store.storeEntryLocked(entry); err != nil {
		s.logger.Error("failed to persist callback; suppressing delivery past unknown durable state",
			"error", err,
			"lease_uuid", leaseUUID,
		)
		s.reportStoreError()
		return
	}

	s.drainLeaseLocked(leaseUUID)
}

// CancelLeaseCallbacks serializes lease teardown with enqueue, replay, and HTTP
// delivery, then removes the lease's durable queue. A caller should cancel the
// ownership context passed to SendOperationCallbackContext before invoking this
// method so a worker that acquires the keyed lock later cannot re-persist stale
// completion state.
func (s *CallbackSender) CancelLeaseCallbacks(leaseUUID string) error {
	unlock := s.lockLease(leaseUUID)
	defer unlock()
	if s.store == nil {
		return nil
	}
	if err := s.store.removeLeaseLocked(leaseUUID); err != nil {
		s.reportStoreError()
		return err
	}
	return nil
}

// DeliverCallback attempts to deliver a callback with retries.
// Returns true if delivery succeeded.
func (s *CallbackSender) DeliverCallback(leaseUUID, callbackURL string, body []byte) bool {
	for attempt := range CallbackMaxAttempts {
		if attempt > 0 {
			// Wait with backoff, but abort if shutting down
			select {
			case <-s.stopCtx.Done():
				s.logger.Warn("callback retry aborted by shutdown",
					"lease_uuid", leaseUUID,
					"attempt", attempt+1,
				)
				s.reportDelivery("failure")
				return false
			case <-time.After(s.backoff[attempt]):
			}
		}

		if s.trySendCallback(leaseUUID, callbackURL, body) {
			s.reportDelivery("success")
			return true
		}
	}

	s.reportDelivery("failure")
	s.logger.Error("callback delivery failed after retries",
		"lease_uuid", leaseUUID,
		"attempts", CallbackMaxAttempts,
	)
	return false
}

// trySendCallback makes a single callback attempt. Returns true on success.
func (s *CallbackSender) trySendCallback(leaseUUID, callbackURL string, body []byte) bool {
	// The request context is the single authoritative deadline. Bundled backend
	// clients deliberately leave http.Client.Timeout unset so a shorter client-
	// wide cap cannot race Fred's synchronous application timeout.
	ctx, cancel := context.WithTimeout(s.stopCtx, s.attemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		s.logger.Error("failed to create callback request", "error", err, "lease_uuid", leaseUUID)
		return false
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.SignRequest(s.secret, req, body))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Warn("callback attempt failed",
			"error", err,
			"lease_uuid", leaseUUID,
		)
		return false
	}

	// Always read and close the response body to allow connection reuse.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.logger.Debug("callback sent", "lease_uuid", leaseUUID)
		return true
	}

	s.logger.Warn("callback returned error status",
		"status", resp.StatusCode,
		"lease_uuid", leaseUUID,
		"body", string(respBody),
	)
	return false
}

// ReplayPendingCallbacks drains callbacks that remain durable after a failed
// live delivery or previous shutdown. The initial snapshot discovers lease
// IDs only; each worker re-lists one lease under its keyed delivery lock.
func (s *CallbackSender) ReplayPendingCallbacks() {
	if s.store == nil || s.stopCtx.Err() != nil {
		return
	}

	entries, err := s.store.ListPending()
	if err != nil {
		s.logger.Error("failed to list pending callbacks", "error", err)
		s.reportStoreError()
		return
	}
	if len(entries) == 0 {
		return
	}

	s.logger.Info("replaying pending callbacks", "count", len(entries))
	seenLeases := make(map[string]struct{}, len(entries))
	leaseUUIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if _, seen := seenLeases[entry.LeaseUUID]; seen {
			continue
		}
		seenLeases[entry.LeaseUUID] = struct{}{}
		leaseUUIDs = append(leaseUUIDs, entry.LeaseUUID)
	}

	jobs := make(chan string, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		jobs <- leaseUUID
	}
	close(jobs)

	workerCount := min(callbackReplayWorkerLimit, len(leaseUUIDs))
	var workers sync.WaitGroup
	for range workerCount {
		workers.Go(func() {
			for leaseUUID := range jobs {
				if s.stopCtx.Err() != nil {
					return
				}
				s.replayLease(leaseUUID)
			}
		})
	}
	workers.Wait()
}

func (s *CallbackSender) replayLease(leaseUUID string) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("panic while replaying callback outbox",
				"panic", recovered,
				"lease_uuid", leaseUUID,
			)
			s.reportReplayPanic(recovered)
		}
	}()

	unlock := s.lockLease(leaseUUID)
	defer unlock()
	// The all-leases snapshot is discovery only. Re-list under the lease lock
	// so concurrent sends cannot make replay use a stale queue.
	s.drainLeaseLocked(leaseUUID)
}

// RunReplayLoop drains the durable callback outbox once at startup and then
// periodically until the sender lifecycle context is canceled. Backends run
// one tracked instance so the initial replay cannot delay readiness, while
// Stop still cancels and waits for any in-flight delivery before closing the
// store.
func (s *CallbackSender) RunReplayLoop() {
	if s.store == nil {
		return
	}
	s.ReplayPendingCallbacks()
	timer := time.NewTimer(s.replayInterval)
	defer timer.Stop()
	for {
		select {
		case <-s.stopCtx.Done():
			return
		case <-timer.C:
			s.ReplayPendingCallbacks()
			timer.Reset(s.replayInterval)
		}
	}
}

// drainLeaseLocked delivers one lease's durable outbox in sequence order. A
// failed delivery or precise-removal failure is a barrier for that lease, but
// ReplayPendingCallbacks continues draining other leases.
func (s *CallbackSender) drainLeaseLocked(leaseUUID string) {
	entries, err := s.store.listPending(leaseUUID)
	if err != nil {
		s.logger.Error("failed to list pending callbacks for lease; suppressing delivery",
			"error", err,
			"lease_uuid", leaseUUID,
		)
		s.reportStoreError()
		return
	}

	for _, entry := range entries {
		body, marshalErr := callbackEntryPayload(entry)
		if marshalErr != nil {
			s.logger.Error("failed to marshal pending callback; stopping lease drain",
				"error", marshalErr,
				"lease_uuid", leaseUUID,
				"delivery_id", entry.DeliveryID,
			)
			return
		}
		if !s.DeliverCallback(entry.LeaseUUID, entry.CallbackURL, body) {
			return
		}
		if rmErr := s.store.removeEntryLocked(entry); rmErr != nil {
			s.logger.Error("failed to remove delivered callback; stopping lease drain",
				"error", rmErr,
				"lease_uuid", leaseUUID,
				"delivery_id", entry.DeliveryID,
			)
			s.reportStoreError()
			return
		}
	}
}

func callbackEntryPayload(entry CallbackEntry) ([]byte, error) {
	// Legacy entries have empty Status; fall back to the Success bool.
	status := entry.Status
	if status == "" {
		status = backend.CallbackStatusSuccess
		if !entry.Success {
			status = backend.CallbackStatusFailed
		}
	}
	return json.Marshal(backend.CallbackPayload{
		LeaseUUID: entry.LeaseUUID,
		Status:    status,
		Error:     entry.Error,
		Backend:   entry.Backend,
		Retained:  entry.Retained,
	})
}

func (s *CallbackSender) lockLease(leaseUUID string) func() {
	return lockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func lockCallbackLease(registryMu *sync.Mutex, registry map[string]*callbackLeaseLock, leaseUUID string) func() {
	registryMu.Lock()
	lock := registry[leaseUUID]
	if lock == nil {
		lock = &callbackLeaseLock{}
		registry[leaseUUID] = lock
	}
	lock.refs++
	registryMu.Unlock()

	lock.mu.Lock()
	return callbackLeaseUnlock(registryMu, registry, leaseUUID, lock)
}

// tryLockCallbackLease joins the ref-counted registry before trying the keyed
// mutex. Taking the reference first prevents the current holder from retiring
// the registry entry and a concurrent sender from creating a second mutex for
// the same lease. Cleanup uses this non-blocking form so an HTTP retry chain
// cannot stall the entire expiry pass.
func tryLockCallbackLease(registryMu *sync.Mutex, registry map[string]*callbackLeaseLock, leaseUUID string) (func(), bool) {
	registryMu.Lock()
	lock := registry[leaseUUID]
	if lock == nil {
		lock = &callbackLeaseLock{}
		registry[leaseUUID] = lock
	}
	lock.refs++
	registryMu.Unlock()

	if !lock.mu.TryLock() {
		callbackLeaseReleaseReference(registryMu, registry, leaseUUID, lock)
		return nil, false
	}
	return callbackLeaseUnlock(registryMu, registry, leaseUUID, lock), true
}

func callbackLeaseUnlock(registryMu *sync.Mutex, registry map[string]*callbackLeaseLock, leaseUUID string, lock *callbackLeaseLock) func() {
	return func() {
		lock.mu.Unlock()
		callbackLeaseReleaseReference(registryMu, registry, leaseUUID, lock)
	}
}

func callbackLeaseReleaseReference(registryMu *sync.Mutex, registry map[string]*callbackLeaseLock, leaseUUID string, lock *callbackLeaseLock) {
	registryMu.Lock()
	lock.refs--
	if lock.refs == 0 && registry[leaseUUID] == lock {
		delete(registry, leaseUUID)
	}
	registryMu.Unlock()
}

func (s *CallbackSender) reportStoreError() {
	if s.onStoreError != nil {
		s.onStoreError()
	}
}

func (s *CallbackSender) reportReplayPanic(recovered any) {
	if s.onReplayPanic == nil {
		return
	}
	// Metrics hooks are application code. Keep a faulty hook from terminating
	// the bounded worker that must continue with unrelated leases.
	defer func() {
		if hookPanic := recover(); hookPanic != nil {
			s.logger.Error("panic in callback replay panic hook", "panic", hookPanic)
		}
	}()
	s.onReplayPanic(recovered)
}

// reportDelivery calls the onDelivery hook if configured.
func (s *CallbackSender) reportDelivery(outcome string) {
	if s.onDelivery != nil {
		s.onDelivery(outcome)
	}
}
