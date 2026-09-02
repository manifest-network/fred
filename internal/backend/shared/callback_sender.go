package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
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

	// callbackIdentityVerificationTimeout bounds the read-only substrate proof
	// performed before an exact completion is persisted. A wedged Docker/K8s
	// control plane must defer delivery, not keep the short journal-mutation lock
	// forever or prevent the completion from reaching durable storage.
	callbackIdentityVerificationTimeout = 10 * time.Second
)

type callbackAttemptOutcome uint8

const (
	callbackAttemptRetry callbackAttemptOutcome = iota
	callbackAttemptDelivered
	// callbackAttemptDeferReplay means this attempt consumed its complete
	// request-context budget (or backend shutdown canceled it). Running the
	// remaining attempts would retain the same lease's wire-drain ownership for
	// another full budget each. The durable head remains for RunReplayLoop.
	callbackAttemptDeferReplay
)

// defaultCallbackBackoff defines the default delay before each retry attempt.
var defaultCallbackBackoff = [CallbackMaxAttempts]time.Duration{0, 1 * time.Second, 5 * time.Second}

// CallbackSender handles HMAC-signed callback delivery with retry and persistence.
type CallbackSender struct {
	store                       *CallbackStore
	httpClient                  *http.Client
	secret                      string
	logger                      *slog.Logger
	stopCtx                     context.Context
	backoff                     [CallbackMaxAttempts]time.Duration
	deliveryTimeout             time.Duration
	replayInterval              time.Duration
	identityVerificationTimeout time.Duration
	onDelivery                  func(outcome string) // nil-safe; injected by the caller for metrics
	onStoreError                func()               // nil-safe; called when bbolt persistence fails
	onReplayPanic               func(any)            // nil-safe; called when one lease replay panics
	beforeReplay                func(context.Context) error
	beforeDelivery              func(context.Context) error
	storageIdentity             backendidentity.ID
	// replayWake coalesces durable-outbox notifications. Journal owners only
	// publish facts; the tracked replay goroutine performs network I/O so a slow
	// callback cannot extend an API or startup-recovery critical section.
	replayWake chan struct{}
	// deliveryLocks serialize short FIFO journal mutations for exactly one
	// lease. They are released before callback HTTP so actor/API/recovery paths
	// can durably append while an older head is in flight.
	deliveryLocksMu *sync.Mutex
	deliveryLocks   map[string]*callbackLeaseLock
	// drainLocks elect one wire drainer per lease across every sender sharing a
	// CallbackStore. Drain ownership spans HTTP and retries; the drainer re-lists
	// the durable head after every precise removal so concurrent coalescing and
	// appends cannot invalidate a suffix snapshot.
	drainLocksMu *sync.Mutex
	drainLocks   map[string]*callbackLeaseLock
}

type callbackLeaseLock struct {
	mu   sync.Mutex
	refs uint64
}

// CallbackSenderConfig configures a CallbackSender.
type CallbackSenderConfig struct {
	Store                       *CallbackStore
	HTTPClient                  *http.Client
	Secret                      string
	Logger                      *slog.Logger
	StopCtx                     context.Context
	OnDelivery                  func(outcome string)                // optional metrics callback
	OnStoreError                func()                              // optional; called when bbolt persistence fails
	OnReplayPanic               func(any)                           // optional; called after recovering a lease replay panic
	BeforeReplay                func(context.Context) error         // required for durable senders; optional for ephemeral; fail-closed substrate re-attestation
	BeforeDelivery              func(context.Context) error         // required for durable senders; optional for ephemeral; runs before enqueue and every HTTP attempt
	StorageIdentity             backendidentity.ID                  // captured in every current durable row and delivered payload
	Backoff                     *[CallbackMaxAttempts]time.Duration // retry delays; nil uses default {0, 1s, 5s}
	DeliveryTimeout             time.Duration                       // total delivery-attempt-chain budget; zero uses backend.DefaultCallbackDeliveryTimeout
	ReplayInterval              time.Duration                       // zero uses DefaultCallbackReplayInterval
	IdentityVerificationTimeout time.Duration                       // zero uses the bounded 10s default
}

// RejectCallbackRedirect keeps an HMAC-signed callback bound to its exact
// configured destination. Following a 3xx could forward the POST body and
// signature to an attacker-controlled or internal URL, and the signature would
// no longer cover the redirected RequestURI.
func RejectCallbackRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// NewCallbackSender creates a durable CallbackSender. The durable store,
// sufficiently strong signing secret, and physical storage identity are
// mandatory: asynchronous operation completions must not be constructible
// without their outbox and exact backend authority. Its lifecycle owner must
// start exactly one tracked RunReplayLoop before reporting ready, cancel StopCtx
// during shutdown, and join that loop before closing Store.
func NewCallbackSender(cfg CallbackSenderConfig) (*CallbackSender, error) {
	if cfg.Store == nil {
		return nil, errors.New("callback sender: durable store is required")
	}
	if len(cfg.Secret) < hmacauth.MinSecretLength {
		return nil, fmt.Errorf(
			"callback sender: HMAC secret must be at least %d bytes, got %d",
			hmacauth.MinSecretLength,
			len(cfg.Secret),
		)
	}
	if !cfg.StorageIdentity.Valid() {
		return nil, errors.New("callback sender: backend storage identity is required")
	}
	if cfg.BeforeDelivery == nil {
		return nil, errors.New("callback sender: delivery storage re-attestation hook is required")
	}
	if cfg.BeforeReplay == nil {
		return nil, errors.New("callback sender: replay storage re-attestation hook is required")
	}
	return newCallbackSender(cfg)
}

// NewEphemeralCallbackSender explicitly constructs a non-durable sender for
// isolated tests in sibling internal packages. A repository-level invariant
// rejects production callers; backend request handlers must use
// NewCallbackSender so a process crash cannot erase acceptance or completion
// evidence.
func NewEphemeralCallbackSender(cfg CallbackSenderConfig) (*CallbackSender, error) {
	if cfg.Store != nil {
		return nil, errors.New("ephemeral callback sender: Store must be nil")
	}
	return newCallbackSender(cfg)
}

// MustNewCallbackSender is the explicit panic-on-programmer-error form. It is
// convenient in tests and static composition where the configuration is a
// literal; runtime backend constructors should use NewCallbackSender and
// propagate its error.
func MustNewCallbackSender(cfg CallbackSenderConfig) *CallbackSender {
	sender, err := NewCallbackSender(cfg)
	if err != nil {
		panic(err)
	}
	return sender
}

// MustNewEphemeralCallbackSender is the explicit panic-on-programmer-error
// counterpart for isolated non-durable tests.
func MustNewEphemeralCallbackSender(cfg CallbackSenderConfig) *CallbackSender {
	sender, err := NewEphemeralCallbackSender(cfg)
	if err != nil {
		panic(err)
	}
	return sender
}

func newCallbackSender(cfg CallbackSenderConfig) (*CallbackSender, error) {
	if cfg.HTTPClient == nil {
		return nil, errors.New("callback sender: HTTP client is required")
	}
	if cfg.Logger == nil {
		return nil, errors.New("callback sender: logger is required")
	}
	if cfg.StopCtx == nil {
		return nil, errors.New("callback sender: stop context is required")
	}
	if cfg.ReplayInterval < 0 {
		return nil, errors.New("callback sender: replay interval must not be negative")
	}
	if cfg.DeliveryTimeout < 0 {
		return nil, errors.New("callback sender: delivery timeout must not be negative")
	}
	if cfg.IdentityVerificationTimeout < 0 {
		return nil, errors.New("callback sender: identity verification timeout must not be negative")
	}

	backoff := defaultCallbackBackoff
	if cfg.Backoff != nil {
		backoff = *cfg.Backoff
	}
	replayInterval := cfg.ReplayInterval
	if replayInterval == 0 {
		replayInterval = DefaultCallbackReplayInterval
	}
	deliveryTimeout := cfg.DeliveryTimeout
	if deliveryTimeout == 0 {
		deliveryTimeout = backend.DefaultCallbackDeliveryTimeout
	}
	identityVerificationTimeout := cfg.IdentityVerificationTimeout
	if identityVerificationTimeout == 0 {
		identityVerificationTimeout = callbackIdentityVerificationTimeout
	}

	deliveryLocksMu := &sync.Mutex{}
	deliveryLocks := make(map[string]*callbackLeaseLock)
	drainLocksMu := &sync.Mutex{}
	drainLocks := make(map[string]*callbackLeaseLock)
	if cfg.Store != nil {
		deliveryLocksMu = cfg.Store.deliveryLocksMu
		deliveryLocks = cfg.Store.deliveryLocks
		drainLocksMu = cfg.Store.drainLocksMu
		drainLocks = cfg.Store.drainLocks
	}
	// Clone rather than mutate the caller's client. Redirect policy is part of
	// the sender's security boundary: an HMAC covers the original RequestURI,
	// and following a 3xx could forward that signature and body to a different
	// host. Callback delivery also needs no ambient cookie authority, so do not
	// let a caller-supplied Jar attach credentials for the destination. Preserve
	// the configured Transport (including TLS roots and proxy policy).
	httpClient := *cfg.HTTPClient
	httpClient.CheckRedirect = RejectCallbackRedirect
	httpClient.Jar = nil

	return &CallbackSender{
		store:                       cfg.Store,
		httpClient:                  &httpClient,
		secret:                      cfg.Secret,
		logger:                      cfg.Logger,
		stopCtx:                     cfg.StopCtx,
		backoff:                     backoff,
		deliveryTimeout:             deliveryTimeout,
		replayInterval:              replayInterval,
		identityVerificationTimeout: identityVerificationTimeout,
		onDelivery:                  cfg.OnDelivery,
		onStoreError:                cfg.OnStoreError,
		onReplayPanic:               cfg.OnReplayPanic,
		beforeReplay:                cfg.BeforeReplay,
		beforeDelivery:              cfg.BeforeDelivery,
		storageIdentity:             cfg.StorageIdentity,
		replayWake:                  make(chan struct{}, 1),
		deliveryLocksMu:             deliveryLocksMu,
		deliveryLocks:               deliveryLocks,
		drainLocksMu:                drainLocksMu,
		drainLocks:                  drainLocks,
	}, nil
}

// SendOperationCallback publishes an exact requested-operation completion to
// the durable outbox. The tracked replay loop owns HMAC-signed HTTP delivery;
// this caller only persists under the short journal-mutation lock and wakes
// that loop, so a slow endpoint cannot extend an actor or API critical section. An explicitly
// ephemeral sender still delivers inline because it has no durable owner.
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
	s.enqueueAndNotify(ctx, leaseUUID, callbackURL, backendName, status, errMsg, false, CallbackDeliveryKindOperation)
}

// SendLifecycleCallback sends a typed, observation-only lifecycle callback.
// Autonomous failure and teardown observations belong on this route;
// provision/restore and durable maintenance completion use their exact causal
// claims. Enqueue atomically coalesces older typed lifecycle observations, but
// never exact completions or protected legacy records.
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
	s.enqueueAndNotify(context.Background(), leaseUUID, callbackURL, backendName, status, errMsg, retained, CallbackDeliveryKindLifecycle)
}

// SendMaintenanceCallback atomically settles an exact maintenance intent into
// the durable FIFO, then wakes the tracked replay loop. A persistence or
// terminal identity error leaves the intent untouched for periodic or cold
// recovery; callers must never synthesize a second lifecycle observation.
func (s *CallbackSender) SendMaintenanceCallback(
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) error {
	if s.store == nil {
		return errors.New("durable callback store is required for maintenance settlement")
	}
	if !claim.Valid() {
		return errors.New("valid maintenance intent claim is required")
	}
	if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed {
		return fmt.Errorf("invalid maintenance callback status %q", status)
	}
	if s.beforeDelivery != nil {
		if err := s.verifyIdentityBounded(context.Background()); err != nil {
			if isTerminalStorageAuthorityError(err) {
				return fmt.Errorf("maintenance callback lost storage authority: %w", err)
			}
			s.logger.Warn("persisting maintenance callback while backend identity re-attestation is transiently unavailable",
				"error", err, "lease_uuid", claim.LeaseUUID())
		}
	}

	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()
	if s.beforeDelivery != nil {
		if err := s.verifyIdentityBounded(context.Background()); err != nil {
			if isTerminalStorageAuthorityError(err) {
				return fmt.Errorf("maintenance callback lost storage authority while waiting for FIFO: %w", err)
			}
			s.logger.Warn("persisting maintenance callback after transient post-FIFO identity re-attestation failure",
				"error", err, "lease_uuid", claim.LeaseUUID())
		}
	}
	deliveryID, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("allocate maintenance callback delivery ID: %w", err)
	}
	entry := callbackEntryForMaintenanceIntent(claim.entry, deliveryID.String(), status, errMsg)
	if err := validateNewCallbackEntry(entry, time.Now()); err != nil {
		return err
	}
	if _, err := s.store.resolveMaintenanceIntentLocked(claim, entry); err != nil {
		s.reportStoreError()
		return fmt.Errorf("persist maintenance callback: %w", err)
	}
	// The bbolt transaction above is the completion boundary. Network delivery
	// belongs exclusively to the tracked replay loop; waking it is non-blocking
	// and the durable row remains authoritative if no loop is running yet.
	s.NotifyPendingCallbacks()
	return nil
}

func (s *CallbackSender) enqueueAndNotify(ownerCtx context.Context, leaseUUID, callbackURL, backendName string, status backend.CallbackStatus, errMsg string, retained bool, kind CallbackDeliveryKind) {
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
		BackendStorageID: func() string {
			if s.storageIdentity.Valid() {
				return s.storageIdentity.String()
			}
			return ""
		}(),
		Error:     errMsg,
		Retained:  retained,
		CreatedAt: time.Now(),
	}

	if ownerCtx != nil {
		if err := ownerCtx.Err(); err != nil {
			s.logger.Debug("suppressing callback enqueue for canceled lease operation",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			return
		}
	}
	identityVerified := true
	if s.beforeDelivery != nil {
		err := s.verifyIdentityBounded(ownerCtx)
		if err != nil {
			identityVerified = false
			if isTerminalStorageAuthorityError(err) {
				s.logger.Error("suppressing callback from terminally unsafe backend storage",
					"error", err,
					"lease_uuid", leaseUUID,
				)
				return
			}
			s.logger.Warn("persisting callback but deferring delivery until backend identity is re-attested",
				"error", err,
				"lease_uuid", leaseUUID,
			)
		}
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
	// Recheck after joining the per-lease FIFO. A root can drift while this
	// sender waits behind an older callback; persisting after only the pre-lock
	// proof would let a replacement-substrate completion enter the durable
	// queue. The second probe is independently bounded.
	if s.beforeDelivery != nil {
		if err := s.verifyIdentityBounded(ownerCtx); err != nil {
			identityVerified = false
			if isTerminalStorageAuthorityError(err) {
				s.logger.Error("suppressing callback that lost storage authority while waiting for lease FIFO",
					"error", err, "lease_uuid", leaseUUID)
				return
			}
		} else {
			identityVerified = true
		}
	}
	// The owner can be canceled while the post-lock identity probe is in
	// progress. Recheck at the final pre-mutation boundary so deprovision cannot
	// race that bounded probe and leave a stale completion in the durable FIFO.
	// Once the transaction below commits, the durable row—not ownerCtx—remains
	// authoritative and replay must continue independently of this caller.
	if ownerCtx != nil {
		if err := ownerCtx.Err(); err != nil {
			s.logger.Debug("suppressing callback enqueue for canceled lease operation after identity verification",
				"lease_uuid", leaseUUID,
				"error", err,
			)
			return
		}
	}

	if s.store == nil {
		if !identityVerified {
			return
		}
		body, err := callbackEntryPayload(entry, s.storageIdentity)
		if err != nil {
			s.logger.Error("failed to marshal callback payload", "error", err, "lease_uuid", leaseUUID)
			return
		}
		s.DeliverCallback(leaseUUID, callbackURL, body)
		return
	}

	// Persist callback before notifying its delivery owner so it survives restarts.
	// Success remains populated with the pre-Status encoding for schema
	// continuity. New entries live in the v2 queue, which v0.13 deliberately does
	// not read; see DEPLOYMENT.md for the callback-store rollback boundary.
	var persistErr error
	if kind == CallbackDeliveryKindOperation {
		persistErr = s.store.settleOperationCallbackLocked(entry)
	} else {
		_, persistErr = s.store.storeEntryLocked(entry)
	}
	if persistErr != nil {
		if errors.Is(persistErr, errTerminalLifecyclePending) {
			s.logger.Debug("suppressing lifecycle observation behind pending terminal callback",
				"lease_uuid", leaseUUID,
				"status", status,
			)
			// The already-durable terminal head remains the replay loop's work.
			s.NotifyPendingCallbacks()
			return
		}
		s.logger.Error("failed to persist callback; suppressing delivery past unknown durable state",
			"error", persistErr,
			"lease_uuid", leaseUUID,
		)
		s.reportStoreError()
		return
	}

	// A durable sender never performs HTTP in the actor/API/recovery call stack.
	// Notify is deliberately non-blocking; the periodic replay sweep is the
	// level-triggered fallback if the tracked loop is not running or is busy.
	s.NotifyPendingCallbacks()
}

func isTerminalStorageAuthorityError(err error) bool {
	return errors.Is(err, backendidentity.ErrIdentityDrift) ||
		errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous)
}

func (s *CallbackSender) verifyIdentityBounded(ownerCtx context.Context) error {
	return s.verifyCallbackPreconditionBounded(ownerCtx, s.beforeDelivery)
}

func (s *CallbackSender) verifyCallbackPreconditionBounded(
	ownerCtx context.Context,
	check func(context.Context) error,
) error {
	verificationCtx, cancelVerification := context.WithTimeout(
		s.stopCtx, s.identityVerificationTimeout,
	)
	stopOwnerCancellation := func() bool { return false }
	if ownerCtx != nil {
		stopOwnerCancellation = context.AfterFunc(ownerCtx, cancelVerification)
	}
	err := check(verificationCtx)
	stopOwnerCancellation()
	cancelVerification()
	return err
}

// DeliverCallback attempts to deliver a callback with retries.
// Returns true if delivery succeeded.
func (s *CallbackSender) DeliverCallback(leaseUUID, callbackURL string, body []byte) bool {
	// Share one deadline across the complete retry chain, including
	// backoff. A slow 503 must not receive a fresh full application budget on
	// every retry and retain this lease's wire-drain ownership for
	// CallbackMaxAttempts times the configured timeout. Quick failures can still retry with whatever
	// budget remains; durable replay owns the head after this context expires.
	deliveryCtx, cancel := context.WithTimeout(s.stopCtx, s.deliveryTimeout)
	defer cancel()

	for attempt := range CallbackMaxAttempts {
		if attempt > 0 {
			// Keep retry backoff inside the same total delivery budget.
			timer := time.NewTimer(s.backoff[attempt])
			select {
			case <-deliveryCtx.Done():
				timer.Stop()
				s.logger.Warn("callback retry deferred after delivery context ended",
					"lease_uuid", leaseUUID,
					"attempt", attempt+1,
					"error", deliveryCtx.Err(),
				)
				s.reportDelivery("failure")
				return false
			case <-timer.C:
			}
		}
		if s.beforeDelivery != nil {
			if err := s.beforeDelivery(deliveryCtx); err != nil {
				s.logger.Warn("callback delivery deferred by backend identity verification",
					"error", err,
					"lease_uuid", leaseUUID,
				)
				s.reportDelivery("failure")
				return false
			}
		}

		switch s.trySendCallback(deliveryCtx, leaseUUID, callbackURL, body) {
		case callbackAttemptDelivered:
			s.reportDelivery("success")
			return true
		case callbackAttemptDeferReplay:
			s.reportDelivery("failure")
			s.logger.Warn("callback delivery deferred to durable replay after request context ended",
				"lease_uuid", leaseUUID,
				"attempt", attempt+1,
				"replay_interval", s.replayInterval,
			)
			return false
		case callbackAttemptRetry:
			// A quick transport or HTTP failure remains eligible for the
			// existing bounded retry chain.
		}
	}

	s.reportDelivery("failure")
	s.logger.Error("callback delivery failed after retries",
		"lease_uuid", leaseUUID,
		"attempts", CallbackMaxAttempts,
	)
	return false
}

// trySendCallback makes one request attempt and tells the caller whether to
// retry inline, finish, or leave the durable head for periodic replay.
func (s *CallbackSender) trySendCallback(
	ctx context.Context,
	leaseUUID, callbackURL string,
	body []byte,
) callbackAttemptOutcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		s.logger.Error("failed to create callback request", "error_type", fmt.Sprintf("%T", err), "lease_uuid", leaseUUID)
		return callbackAttemptRetry
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.SignRequest(s.secret, req, body))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Warn("callback attempt failed",
			// net/http's *url.Error includes the complete request URL. The
			// operation/lifecycle UUID in its query is a bearer capability and
			// must never cross into logs, even on a transport failure.
			"error_type", fmt.Sprintf("%T", err),
			"lease_uuid", leaseUUID,
		)
		if ctx.Err() != nil {
			return callbackAttemptDeferReplay
		}
		return callbackAttemptRetry
	}

	// Always read and close the response body to allow connection reuse.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		s.logger.Debug("callback sent", "lease_uuid", leaseUUID)
		return callbackAttemptDelivered
	}
	if ctx.Err() != nil {
		return callbackAttemptDeferReplay
	}

	s.logger.Warn("callback returned error status",
		"status", resp.StatusCode,
		"lease_uuid", leaseUUID,
		"body", string(respBody),
	)
	return callbackAttemptRetry
}

// ReplayPendingCallbacks drains callbacks that remain durable after a failed
// live delivery or previous shutdown. Discovery reads only lease-identifying
// keys; one elected drainer per lease re-lists the current durable head between
// sends. Corruption therefore quarantines the identifiable lease while healthy
// leases continue to drain, and CallbackStore.Healthy still reports the fault.
func (s *CallbackSender) ReplayPendingCallbacks() {
	if s.store == nil || s.stopCtx.Err() != nil {
		return
	}
	if s.beforeReplay != nil {
		if err := s.verifyCallbackPreconditionBounded(context.Background(), s.beforeReplay); err != nil {
			s.logger.Error("callback replay suppressed by backend identity verification", "error", err)
			return
		}
	}

	leaseUUIDs, err := s.store.callbackLeaseUUIDs()
	if err != nil {
		s.logger.Error("callback outbox discovery found durable corruption", "error", err)
		s.reportStoreError()
	}
	if len(leaseUUIDs) == 0 {
		return
	}

	s.logger.Info("replaying pending callback leases", "count", len(leaseUUIDs))

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

// NotifyPendingCallbacks asks the tracked replay loop to drain the durable
// outbox promptly. The notification is deliberately non-blocking and
// coalescing: the bbolt rows, not this in-memory signal, are the delivery
// authority, and the periodic sweep remains the fallback if no loop is running
// yet or another wake is already pending.
func (s *CallbackSender) NotifyPendingCallbacks() {
	if s.store == nil || s.stopCtx.Err() != nil {
		return
	}
	select {
	case s.replayWake <- struct{}{}:
	default:
	}
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

	// A coalesced wake or another sender over the same store may discover this
	// lease concurrently. Only one of them may own wire delivery; the durable
	// queue remains level-triggered work for the current drainer or next sweep.
	unlockDrain, acquired := s.tryLockDrainLease(leaseUUID)
	if !acquired {
		return
	}
	drainReleased := false
	defer func() {
		if !drainReleased {
			unlockDrain()
			// With multiple tracked senders, another loop may consume the original
			// commit edge and lose this drain election. Ordinary delivery failure
			// deliberately waits for periodic replay, but a canceled owner is
			// retiring and cannot consume that fallback. Publish a handoff only
			// after releasing drain ownership so a surviving loop can take over.
			if s.stopCtx.Err() != nil {
				s.store.notifyReplaySubscribers()
			}
		}
	}()
	s.drainLease(leaseUUID, func() {
		// Release drain ownership while the mutation lock still proves the
		// queue empty. A concurrent enqueue can only commit and notify after
		// this handoff, so a second sender cannot consume that wake while the
		// retiring drainer still appears busy.
		drainReleased = true
		unlockDrain()
	})
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
	// Subscribe before the initial replay. A concurrent direct journal
	// settlement is then either observed by that initial level-triggered read or
	// leaves a coalesced wake for the next pass; there is no gap between them.
	unsubscribe := s.store.subscribeReplayWake(s.replayWake)
	defer unsubscribe()
	s.ReplayPendingCallbacks()
	timer := time.NewTimer(s.replayInterval)
	defer timer.Stop()
	for {
		select {
		case <-s.stopCtx.Done():
			return
		case <-s.replayWake:
			s.ReplayPendingCallbacks()
		case <-timer.C:
			s.ReplayPendingCallbacks()
			timer.Reset(s.replayInterval)
		}
	}
}

// drainLease delivers one lease's durable outbox in sequence order while the
// caller owns its drain lock. Journal mutation locks cover only head selection
// and precise removal; callback HTTP runs outside them so live settlement can
// append promptly. Re-listing after every outcome is load-bearing because a
// concurrent lifecycle enqueue may coalesce a previously observed suffix.
func (s *CallbackSender) drainLease(leaseUUID string, releaseEmptyDrain func()) {
	for {
		entry, found, err := s.nextPendingCallback(leaseUUID, releaseEmptyDrain)
		if err != nil {
			s.logger.Error("failed to list pending callbacks for lease; suppressing delivery",
				"error", err,
				"lease_uuid", leaseUUID,
			)
			s.reportStoreError()
			return
		}
		if !found {
			return
		}
		body, marshalErr := callbackEntryPayload(entry, s.storageIdentity)
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
		if rmErr := s.removeDeliveredCallback(entry); rmErr != nil {
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

func (s *CallbackSender) nextPendingCallback(
	leaseUUID string,
	releaseEmptyDrain func(),
) (CallbackEntry, bool, error) {
	unlock := s.lockLease(leaseUUID)
	defer unlock()
	entries, err := s.store.listPending(leaseUUID)
	if err != nil {
		return CallbackEntry{}, false, err
	}
	if len(entries) == 0 {
		releaseEmptyDrain()
		return CallbackEntry{}, false, nil
	}
	return entries[0], true, nil
}

func (s *CallbackSender) removeDeliveredCallback(entry CallbackEntry) error {
	unlock := s.lockLease(entry.LeaseUUID)
	defer unlock()
	return s.store.removeEntryLocked(entry)
}

func callbackEntryPayload(entry CallbackEntry, storageIdentity backendidentity.ID) ([]byte, error) {
	if entry.BackendStorageID == "" && entry.storageVersion == callbackStorageLegacy {
		// A v0.13 row contains no storage-lineage evidence. The supported stopped
		// upgrade drains this bucket before sealing an identity, so reaching it in
		// a current sender is an operator-repair condition, not permission to bind
		// old evidence to whichever substrate happens to be mounted now.
		return nil, fmt.Errorf("legacy v0.13 callback lacks backend storage identity; drain it before upgrade")
	}
	if entry.BackendStorageID == "" && entry.storageVersion == callbackStorageV2 {
		// Current durable decode already rejects this. Keep the payload boundary
		// independently fail-closed in case a future caller bypasses store reads.
		return nil, fmt.Errorf("current callback lacks backend storage identity")
	}

	// Explicit non-durable compatibility senders may still use the old
	// Success-only shape. Current durable rows always carry Status.
	status := entry.Status
	if status == "" {
		status = backend.CallbackStatusSuccess
		if !entry.Success {
			status = backend.CallbackStatusFailed
		}
	}
	payload := backend.CallbackPayload{
		LeaseUUID: entry.LeaseUUID,
		Status:    status,
		Error:     entry.Error,
		Backend:   entry.Backend,
		Retained:  entry.Retained,
	}
	if entry.BackendStorageID != "" {
		parsed, err := backendidentity.Parse(entry.BackendStorageID)
		if err != nil {
			return nil, fmt.Errorf("parse persisted callback backend storage identity: %w", err)
		}
		if storageIdentity.Valid() && parsed != storageIdentity {
			return nil, fmt.Errorf("%w: persisted callback belongs to %s, current backend is %s",
				backendidentity.ErrIdentityDrift, parsed, storageIdentity)
		}
		payload.BackendStorageID = parsed.String()
	}
	return json.Marshal(payload)
}

func (s *CallbackSender) lockLease(leaseUUID string) func() {
	return lockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func (s *CallbackSender) tryLockDrainLease(leaseUUID string) (func(), bool) {
	return tryLockCallbackLease(s.drainLocksMu, s.drainLocks, leaseUUID)
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
