package shared

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

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
	// callbackReplayLeaseQuantum prevents one lease with a long successful
	// suffix from monopolizing a replay worker. Per-lease FIFO remains enforced
	// by drain ownership; a remaining suffix is requeued behind other leases.
	callbackReplayLeaseQuantum = 1

	// callbackResponseDrainLimit bounds work spent on an unused peer response.
	// Reading through EOF within this small prefix preserves HTTP connection
	// reuse; a larger or endless body is closed immediately after the prefix.
	callbackResponseDrainLimit int64 = 4 << 10
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

type callbackReplayWakeKind uint8

const (
	callbackReplayWakeInvalid callbackReplayWakeKind = iota
	callbackReplayWakeCommit
	callbackReplayWakeHandoff
)

// callbackReplayWake is a store-minted scheduling fact for exactly one lease.
// Commit preserves an already-dormant failed head; handoff is stronger and may
// retry it because a retiring drainer can no longer consume periodic fallback.
type callbackReplayWake struct {
	leaseUUID string
	kind      callbackReplayWakeKind
}

func newCallbackReplayCommitWake(leaseUUID string) callbackReplayWake {
	return callbackReplayWake{leaseUUID: leaseUUID, kind: callbackReplayWakeCommit}
}

func newCallbackReplayHandoffWake(leaseUUID string) callbackReplayWake {
	return callbackReplayWake{leaseUUID: leaseUUID, kind: callbackReplayWakeHandoff}
}

func (wake callbackReplayWake) valid() bool {
	return (wake.kind == callbackReplayWakeCommit || wake.kind == callbackReplayWakeHandoff) &&
		validateCanonicalLeaseUUID(wake.leaseUUID) == nil
}

// callbackReplayMailbox coalesces repeated scheduling facts without discarding
// the identities of other leases. The one-slot signal is only readiness; the
// protected map is the complete pending fact set, with handoff dominating a
// normal commit for the same lease.
type callbackReplayMailbox struct {
	mu      sync.Mutex
	ready   chan struct{}
	pending map[string]callbackReplayWakeKind
	// runClaim is a process-lifetime ownership bit shared even by an accidental
	// value copy of CallbackSender because every copy retains this mailbox pointer.
	// A sender lifecycle has exactly one replay coordinator.
	runClaim atomic.Bool
}

func newCallbackReplayMailbox() *callbackReplayMailbox {
	return &callbackReplayMailbox{
		ready:   make(chan struct{}, 1),
		pending: make(map[string]callbackReplayWakeKind),
	}
}

func (mailbox *callbackReplayMailbox) publish(wake callbackReplayWake) {
	if mailbox == nil || mailbox.ready == nil || !wake.valid() {
		return
	}
	mailbox.mu.Lock()
	if mailbox.pending == nil {
		mailbox.mu.Unlock()
		return
	}
	if wake.kind > mailbox.pending[wake.leaseUUID] {
		mailbox.pending[wake.leaseUUID] = wake.kind
	}
	select {
	case mailbox.ready <- struct{}{}:
	default:
	}
	mailbox.mu.Unlock()
}

func (mailbox *callbackReplayMailbox) take() []callbackReplayWake {
	if mailbox == nil || mailbox.ready == nil {
		return nil
	}
	mailbox.mu.Lock()
	if mailbox.pending == nil {
		mailbox.mu.Unlock()
		return nil
	}
	pending := mailbox.pending
	mailbox.pending = make(map[string]callbackReplayWakeKind)
	mailbox.mu.Unlock()
	wakes := make([]callbackReplayWake, 0, len(pending))
	for leaseUUID, kind := range pending {
		wakes = append(wakes, callbackReplayWake{leaseUUID: leaseUUID, kind: kind})
	}
	return wakes
}

func (mailbox *callbackReplayMailbox) pendingCount() int {
	if mailbox == nil {
		return 0
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	return len(mailbox.pending)
}

// defaultCallbackBackoff defines the default delay before each retry attempt.
var defaultCallbackBackoff = [CallbackMaxAttempts]time.Duration{0, 1 * time.Second, 5 * time.Second}

// CallbackSender transports and replays already-durable callbacks with bounded
// retry and HMAC authentication. It has no semantic settlement authority;
// CallbackPublisher owns publication into its outbox.
type CallbackSender struct {
	store           *CallbackStore
	httpClient      *http.Client
	secret          string
	logger          *slog.Logger
	stopCtx         context.Context
	backoff         [CallbackMaxAttempts]time.Duration
	deliveryTimeout time.Duration
	replayInterval  time.Duration
	attestor        *CallbackStorageAttestor
	onDelivery      func(outcome string) // nil-safe; injected by the caller for metrics
	onStoreError    func()               // nil-safe; called when bbolt persistence fails
	onReplayPanic   func(any)            // nil-safe; called when one lease replay panics
	storageIdentity backendidentity.ID
	// replayWake coalesces lease-identified durable-outbox notifications. Journal
	// owners only publish facts; the tracked replay goroutine schedules the exact
	// affected lease without restarting unrelated deferred retry chains.
	replayWake *callbackReplayMailbox
	// replayRetry is the stronger operator/lifecycle nudge exposed by
	// NotifyPendingCallbacks. Keeping it distinct from commit discovery lets a
	// caller deliberately retry dormant work without making every ordinary
	// outbox commit churn an outage backlog.
	replayRetry chan struct{}
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
	gate *semaphore.Weighted
	refs uint64
}

// CallbackSenderConfig configures a CallbackSender.
type CallbackSenderConfig struct {
	// Store is the exact identity-bound outbox this transport drains.
	Store *CallbackStore
	// StorageAttestor binds Store to its runtime substrate verifier, storage
	// identity, authority gate, lifecycle, and finite verification budget.
	StorageAttestor *CallbackStorageAttestor
	HTTPClient      *http.Client
	Secret          string
	Logger          *slog.Logger
	OnDelivery      func(outcome string)                // optional metrics callback
	OnStoreError    func()                              // optional; called when bbolt persistence fails
	OnReplayPanic   func(any)                           // optional; called after recovering a lease replay panic
	Backoff         *[CallbackMaxAttempts]time.Duration // retry delays; nil uses default {0, 1s, 5s}
	DeliveryTimeout time.Duration                       // total delivery-attempt-chain budget; zero uses backend.DefaultCallbackDeliveryTimeout
	ReplayInterval  time.Duration                       // zero uses DefaultCallbackReplayInterval
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
// start exactly one tracked RunReplayLoop before reporting ready, cancel the
// StorageAttestor lifecycle during shutdown, and join that loop before closing
// Store.
func NewCallbackSender(cfg CallbackSenderConfig) (*CallbackSender, error) {
	if cfg.Store == nil || cfg.Store.boltStore == nil || cfg.Store.binding == nil ||
		cfg.Store.backendAuthorityGate == nil {
		return nil, errors.New("callback sender: exact identity-bound durable store is required")
	}
	// A durable sender inherits lineage solely from the marker-bound outbox;
	// there is no independent identity input that could restamp that authority.
	_, storeIdentity := cfg.Store.journalBackendIdentity("")
	if len(cfg.Secret) < hmacauth.MinSecretLength {
		return nil, fmt.Errorf(
			"callback sender: HMAC secret must be at least %d bytes, got %d",
			hmacauth.MinSecretLength,
			len(cfg.Secret),
		)
	}
	if !storeIdentity.Valid() {
		return nil, errors.New("callback sender: backend storage identity is required")
	}
	if cfg.StorageAttestor == nil || !cfg.StorageAttestor.validFor(cfg.Store) {
		return nil, errors.New("callback sender: exact callback storage attestor is required")
	}
	return newCallbackSender(cfg, storeIdentity)
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

func newCallbackSender(
	cfg CallbackSenderConfig,
	storageIdentity backendidentity.ID,
) (*CallbackSender, error) {
	if cfg.HTTPClient == nil {
		return nil, errors.New("callback sender: HTTP client is required")
	}
	if cfg.Logger == nil {
		return nil, errors.New("callback sender: logger is required")
	}
	if cfg.ReplayInterval < 0 {
		return nil, errors.New("callback sender: replay interval must not be negative")
	}
	if cfg.DeliveryTimeout < 0 {
		return nil, errors.New("callback sender: delivery timeout must not be negative")
	}
	if cfg.StorageAttestor == nil || cfg.StorageAttestor.stopCtx == nil ||
		cfg.StorageAttestor.stopCtx.Err() != nil {
		return nil, errors.New("callback sender: live storage attestor lifecycle is required")
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
		store:           cfg.Store,
		httpClient:      &httpClient,
		secret:          cfg.Secret,
		logger:          cfg.Logger,
		stopCtx:         cfg.StorageAttestor.stopCtx,
		backoff:         backoff,
		deliveryTimeout: deliveryTimeout,
		replayInterval:  replayInterval,
		attestor:        cfg.StorageAttestor,
		onDelivery:      cfg.OnDelivery,
		onStoreError:    cfg.OnStoreError,
		onReplayPanic:   cfg.OnReplayPanic,
		storageIdentity: storageIdentity,
		replayWake:      newCallbackReplayMailbox(),
		replayRetry:     make(chan struct{}, 1),
		deliveryLocksMu: deliveryLocksMu,
		deliveryLocks:   deliveryLocks,
		drainLocksMu:    drainLocksMu,
		drainLocks:      drainLocks,
	}, nil
}

func isTerminalStorageAuthorityError(err error) bool {
	return errors.Is(err, backendidentity.ErrIdentityDrift) ||
		errors.Is(err, backendidentity.ErrMutationOutcomeAmbiguous)
}

// deliverCallback attempts to deliver a callback with retries.
// Returns true if delivery succeeded.
func (s *CallbackSender) deliverCallback(leaseUUID, callbackURL string, body []byte) bool {
	// Share one deadline across the complete retry chain, including
	// backoff. A slow 503 must not receive a fresh full application budget on
	// every retry and retain this lease's wire-drain ownership for
	// CallbackMaxAttempts times the configured timeout. Quick failures can still
	// retry with whatever budget remains; durable replay owns the head after this
	// context expires.
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
		if err := s.attestor.verify(deliveryCtx); err != nil {
			s.logger.Warn("callback delivery deferred by backend identity verification",
				"error", err,
				"lease_uuid", leaseUUID,
			)
			s.reportDelivery("failure")
			return false
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

	// Drain only a small prefix of the unused response. An untrusted callback
	// peer must not make one lease's durable drainer consume an arbitrary body
	// for the full delivery budget. net/http reuses the connection only when
	// CopyN reaches EOF; oversized bodies are intentionally closed early.
	_, _ = io.CopyN(io.Discard, resp.Body, callbackResponseDrainLimit+1)
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
	)
	return callbackAttemptRetry
}

type callbackReplayOutcome uint8

const (
	callbackReplayEmpty callbackReplayOutcome = iota
	callbackReplayMore
	callbackReplayDeferred
)

type callbackReplayCompletion struct {
	leaseUUID string
	outcome   callbackReplayOutcome
}

func publishCallbackReplayCompletion(
	ctx context.Context,
	completions chan<- callbackReplayCompletion,
	completion callbackReplayCompletion,
) bool {
	select {
	case completions <- completion:
		return true
	case <-ctx.Done():
		return false
	}
}

// callbackReplayQueue is the process-local scheduling view of the durable
// outbox. Durable rows remain the authority. This queue exists only to bound
// concurrency, deduplicate lease work, and let a newly discovered lease move
// ahead of a large outage backlog without weakening per-lease FIFO.
type callbackReplayQueue struct {
	ready    *list.List
	queued   map[string]*list.Element
	inFlight map[string]struct{}
	dormant  map[string]struct{}
	dirty    map[string]struct{}
}

func newCallbackReplayQueue() *callbackReplayQueue {
	return &callbackReplayQueue{
		ready:    list.New(),
		queued:   make(map[string]*list.Element),
		inFlight: make(map[string]struct{}),
		dormant:  make(map[string]struct{}),
		dirty:    make(map[string]struct{}),
	}
}

func (q *callbackReplayQueue) discover(
	leaseUUIDs []string,
	retryDormant bool,
	prioritizeNew bool,
) {
	present := make(map[string]struct{}, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		if leaseUUID == "" {
			continue
		}
		present[leaseUUID] = struct{}{}
		if _, active := q.inFlight[leaseUUID]; active {
			// A commit wake can race the drainer's final empty check. Remember the
			// edge so completion rechecks this lease instead of consuming the wake.
			if prioritizeNew || retryDormant {
				q.dirty[leaseUUID] = struct{}{}
			}
			continue
		}
		if _, waiting := q.queued[leaseUUID]; waiting {
			continue
		}
		if _, deferred := q.dormant[leaseUUID]; deferred {
			if !retryDormant {
				continue
			}
			delete(q.dormant, leaseUUID)
			q.enqueueBack(leaseUUID)
			continue
		}
		if prioritizeNew {
			q.enqueueFront(leaseUUID)
		} else {
			q.enqueueBack(leaseUUID)
		}
	}
	// A failed delivery remains dormant only while a durable row still exists.
	// Retiring absent keys keeps a long-running loop's memory proportional to
	// the current outbox rather than its historical lease cardinality.
	for leaseUUID := range q.dormant {
		if _, exists := present[leaseUUID]; !exists {
			delete(q.dormant, leaseUUID)
		}
	}
}

func (q *callbackReplayQueue) wake(wake callbackReplayWake) {
	if q == nil || !wake.valid() {
		return
	}
	leaseUUID := wake.leaseUUID
	if _, active := q.inFlight[leaseUUID]; active {
		// Unlike a fleet-wide scan, this fact proves the active lease itself changed
		// while its worker was draining. Completion must therefore recheck it.
		q.dirty[leaseUUID] = struct{}{}
		return
	}
	if _, waiting := q.queued[leaseUUID]; waiting {
		return
	}
	if _, deferred := q.dormant[leaseUUID]; deferred {
		if wake.kind != callbackReplayWakeHandoff {
			// A newly appended suffix cannot overtake the failed durable head. Leave
			// ordinary commit work dormant until explicit or periodic retry.
			return
		}
		delete(q.dormant, leaseUUID)
	}
	q.enqueueFront(leaseUUID)
}

func (q *callbackReplayQueue) enqueueFront(leaseUUID string) {
	if leaseUUID == "" {
		return
	}
	if element := q.queued[leaseUUID]; element != nil {
		q.ready.MoveToFront(element)
		return
	}
	q.queued[leaseUUID] = q.ready.PushFront(leaseUUID)
}

func (q *callbackReplayQueue) enqueueBack(leaseUUID string) {
	if leaseUUID == "" || q.queued[leaseUUID] != nil {
		return
	}
	q.queued[leaseUUID] = q.ready.PushBack(leaseUUID)
}

func (q *callbackReplayQueue) next() (string, bool) {
	element := q.ready.Front()
	if element == nil {
		return "", false
	}
	leaseUUID, ok := element.Value.(string)
	return leaseUUID, ok && leaseUUID != ""
}

func (q *callbackReplayQueue) dispatched(leaseUUID string) {
	element := q.queued[leaseUUID]
	if element == nil {
		return
	}
	q.ready.Remove(element)
	delete(q.queued, leaseUUID)
	q.inFlight[leaseUUID] = struct{}{}
}

func (q *callbackReplayQueue) completed(completion callbackReplayCompletion) {
	leaseUUID := completion.leaseUUID
	delete(q.inFlight, leaseUUID)
	_, dirty := q.dirty[leaseUUID]
	delete(q.dirty, leaseUUID)

	switch completion.outcome {
	case callbackReplayMore:
		delete(q.dormant, leaseUUID)
		q.enqueueBack(leaseUUID)
	case callbackReplayDeferred:
		if dirty {
			delete(q.dormant, leaseUUID)
			q.enqueueFront(leaseUUID)
		} else {
			q.dormant[leaseUUID] = struct{}{}
		}
	case callbackReplayEmpty:
		delete(q.dormant, leaseUUID)
		if dirty {
			q.enqueueFront(leaseUUID)
		}
	}
}

func (s *CallbackSender) discoverReplayWork(
	queue *callbackReplayQueue,
	retryDormant bool,
	prioritizeNew bool,
) {
	if s.store == nil || s.stopCtx.Err() != nil {
		return
	}
	if err := s.attestor.verify(s.stopCtx); err != nil {
		s.logger.Error("callback replay suppressed by backend identity verification", "error", err)
		return
	}
	leaseUUIDs, err := s.store.callbackLeaseUUIDs()
	if err != nil {
		s.logger.Error("callback outbox discovery found durable corruption", "error", err)
		s.reportStoreError()
	}
	queue.discover(leaseUUIDs, retryDormant, prioritizeNew)
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
	case s.replayRetry <- struct{}{}:
	default:
	}
}

// replayLeaseWithLimit owns one wire-drain election and reports whether the
// lease is empty, has a successful suffix to schedule fairly, or must wait for
// an explicit/periodic retry. A zero limit preserves the synchronous full-drain
// helper used by focused tests and administrative benchmarks.
func (s *CallbackSender) replayLeaseWithLimit(
	leaseUUID string,
	limit int,
) (outcome callbackReplayOutcome) {
	outcome = callbackReplayDeferred
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("panic while replaying callback outbox",
				"panic", recovered,
				"lease_uuid", leaseUUID,
			)
			s.reportReplayPanic(recovered)
			outcome = callbackReplayDeferred
		}
	}()

	// A coalesced wake or another sender over the same store may discover this
	// lease concurrently. Only one of them may own wire delivery; the durable
	// queue remains level-triggered work for the current drainer or next sweep.
	unlockDrain, acquired := s.tryLockDrainLease(leaseUUID)
	if !acquired {
		return callbackReplayDeferred
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
				s.store.notifyReplayHandoff(leaseUUID)
			}
		}
	}()
	return s.drainLease(leaseUUID, limit, func() {
		// Release drain ownership while the mutation lock still proves the
		// queue empty. A concurrent enqueue can only commit and notify after
		// this handoff, so a second sender cannot consume that wake while the
		// retiring drainer still appears busy.
		drainReleased = true
		unlockDrain()
	})
}

// RunReplayLoop continuously schedules the durable callback outbox until the
// sender lifecycle context is canceled. At most callbackReplayWorkerLimit
// leases perform HTTP concurrently, and each worker yields after one delivered
// head. Unlike a sweep-wide worker batch, the scheduler keeps consuming commit
// wakes while those requests are in flight, so fresh healthy work cannot sit
// behind the complete cardinality of an outage backlog.
func (s *CallbackSender) RunReplayLoop() {
	if s == nil || s.replayWake == nil || !s.replayWake.runClaim.CompareAndSwap(false, true) {
		return
	}
	s.runReplayLoop(newCallbackReplayQueue())
}

func (s *CallbackSender) runReplayLoop(queue *callbackReplayQueue) {
	if s.store == nil {
		return
	}
	// Subscribe before the initial replay. A concurrent direct journal
	// settlement is then either observed by that initial level-triggered read or
	// leaves a coalesced wake for the next pass; there is no gap between them.
	unsubscribe := s.store.subscribeReplayWake(s.replayWake)
	defer unsubscribe()

	if queue == nil {
		return
	}
	s.discoverReplayWork(queue, false, false)
	jobs := make(chan string)
	completions := make(chan callbackReplayCompletion, callbackReplayWorkerLimit)
	var workers sync.WaitGroup
	for range callbackReplayWorkerLimit {
		workers.Go(func() {
			for leaseUUID := range jobs {
				completion := callbackReplayCompletion{
					leaseUUID: leaseUUID,
					outcome: s.replayLeaseWithLimit(
						leaseUUID, callbackReplayLeaseQuantum,
					),
				}
				if !publishCallbackReplayCompletion(s.stopCtx, completions, completion) {
					return
				}
			}
		})
	}

	timer := time.NewTimer(s.replayInterval)
	stopping := false
	for s.stopCtx.Err() == nil {
		leaseUUID, ready := queue.next()
		var dispatch chan<- string
		if ready {
			dispatch = jobs
		}
		select {
		case <-s.stopCtx.Done():
			stopping = true
		case <-s.replayWake.ready:
			wakes := s.replayWake.take()
			if err := s.attestor.verify(s.stopCtx); err != nil {
				s.logger.Error("callback replay wake suppressed by backend identity verification", "error", err)
				continue
			}
			for _, wake := range wakes {
				queue.wake(wake)
			}
		case <-s.replayRetry:
			s.discoverReplayWork(queue, true, true)
		case <-timer.C:
			s.discoverReplayWork(queue, true, false)
			timer.Reset(s.replayInterval)
		case dispatch <- leaseUUID:
			queue.dispatched(leaseUUID)
		case completion := <-completions:
			queue.completed(completion)
		}
		if stopping {
			break
		}
	}
	timer.Stop()
	close(jobs)
	workers.Wait()
}

// drainLease delivers one lease's durable outbox in sequence order while the
// caller owns its drain lock. Journal mutation locks cover only head selection
// and precise removal; callback HTTP runs outside them so live settlement can
// append promptly. Re-listing after every outcome is load-bearing because a
// concurrent lifecycle enqueue may coalesce a previously observed suffix.

func (s *CallbackSender) drainLease(
	leaseUUID string,
	limit int,
	releaseEmptyDrain func(),
) callbackReplayOutcome {
	delivered := 0
	for {
		entry, found, err := s.nextPendingCallback(leaseUUID, releaseEmptyDrain)
		if err != nil {
			if s.stopCtx.Err() != nil && errors.Is(err, s.stopCtx.Err()) {
				return callbackReplayDeferred
			}
			s.logger.Error("failed to list pending callbacks for lease; suppressing delivery",
				"error", err,
				"lease_uuid", leaseUUID,
			)
			s.reportStoreError()
			return callbackReplayDeferred
		}
		if !found {
			return callbackReplayEmpty
		}
		if limit > 0 && delivered >= limit {
			return callbackReplayMore
		}
		if s.expiredLifecycleObservation(entry, time.Now()) {
			if rmErr := s.removeDeliveredCallback(entry); rmErr != nil {
				if s.stopCtx.Err() != nil && errors.Is(rmErr, s.stopCtx.Err()) {
					return callbackReplayDeferred
				}
				s.logger.Error("failed to remove expired lifecycle callback; stopping lease drain",
					"error", rmErr,
					"lease_uuid", leaseUUID,
					"delivery_id", entry.DeliveryID,
				)
				s.reportStoreError()
				return callbackReplayDeferred
			}
			delivered++
			continue
		}
		body, marshalErr := callbackEntryPayload(entry, s.storageIdentity)
		if marshalErr != nil {
			s.logger.Error("failed to marshal pending callback; stopping lease drain",
				"error", marshalErr,
				"lease_uuid", leaseUUID,
				"delivery_id", entry.DeliveryID,
			)
			return callbackReplayDeferred
		}
		if !s.deliverCallback(entry.LeaseUUID, entry.CallbackURL, body) {
			return callbackReplayDeferred
		}
		if rmErr := s.removeDeliveredCallback(entry); rmErr != nil {
			if s.stopCtx.Err() != nil && errors.Is(rmErr, s.stopCtx.Err()) {
				return callbackReplayDeferred
			}
			s.logger.Error("failed to remove delivered callback; stopping lease drain",
				"error", rmErr,
				"lease_uuid", leaseUUID,
				"delivery_id", entry.DeliveryID,
			)
			s.reportStoreError()
			return callbackReplayDeferred
		}
		delivered++
	}
}

func (s *CallbackSender) expiredLifecycleObservation(
	entry CallbackEntry,
	now time.Time,
) bool {
	return s != nil && s.store != nil && s.store.maxAge > 0 &&
		entry.storageVersion == callbackStorageV2 &&
		entry.DeliveryKind == CallbackDeliveryKindLifecycle &&
		entry.CreatedAt.Before(now.Add(-s.store.maxAge))
}

func (s *CallbackSender) nextPendingCallback(
	leaseUUID string,
	releaseEmptyDrain func(),
) (CallbackEntry, bool, error) {
	// Cancellation bounds queueing behind another journal owner. Once this gate
	// is acquired, the authoritative bbolt read/write boundary is deliberately
	// allowed to finish: abandoning an attempted commit would make its outcome
	// ambiguous rather than make shutdown safer.
	unlock, err := s.lockLeaseContext(s.stopCtx, leaseUUID)
	if err != nil {
		return CallbackEntry{}, false, err
	}
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
	unlock, err := s.lockLeaseContext(s.stopCtx, entry.LeaseUUID)
	if err != nil {
		return err
	}
	defer unlock()
	return s.store.removeEntryLocked(entry)
}

func callbackEntryPayload(entry CallbackEntry, storageIdentity backendidentity.ID) ([]byte, error) {
	if entry.BackendStorageID == "" {
		// Current durable decode already rejects this. Keep the payload boundary
		// independently fail-closed in case a future caller bypasses store reads.
		return nil, fmt.Errorf("callback lacks backend storage identity")
	}

	payload := backend.CallbackPayload{
		LeaseUUID: entry.LeaseUUID,
		Status:    entry.Status,
		Error:     entry.Error,
		Backend:   entry.Backend,
		Retained:  entry.Retained,
	}
	parsed, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return nil, fmt.Errorf("parse persisted callback backend storage identity: %w", err)
	}
	if storageIdentity.Valid() && parsed != storageIdentity {
		return nil, fmt.Errorf("%w: persisted callback belongs to %s, current backend is %s",
			backendidentity.ErrIdentityDrift, parsed, storageIdentity)
	}
	payload.BackendStorageID = parsed.String()
	return json.Marshal(payload)
}

func (s *CallbackSender) lockLease(leaseUUID string) func() {
	return lockCallbackLease(s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func (s *CallbackSender) lockLeaseContext(
	ctx context.Context,
	leaseUUID string,
) (func(), error) {
	return lockCallbackLeaseContext(ctx, s.deliveryLocksMu, s.deliveryLocks, leaseUUID)
}

func (s *CallbackSender) tryLockDrainLease(leaseUUID string) (func(), bool) {
	return tryLockCallbackLease(s.drainLocksMu, s.drainLocks, leaseUUID)
}

func lockCallbackLease(
	registryMu *sync.Mutex,
	registry map[string]*callbackLeaseLock,
	leaseUUID string,
) func() {
	unlock, err := lockCallbackLeaseContext(
		context.Background(), registryMu, registry, leaseUUID,
	)
	if err != nil {
		panic(fmt.Sprintf("background callback lease lock failed: %v", err))
	}
	return unlock
}

func lockCallbackLeaseContext(
	ctx context.Context,
	registryMu *sync.Mutex,
	registry map[string]*callbackLeaseLock,
	leaseUUID string,
) (func(), error) {
	if ctx == nil {
		return nil, errors.New("callback lease lock requires a context")
	}
	registryMu.Lock()
	lock := registry[leaseUUID]
	if lock == nil {
		lock = &callbackLeaseLock{gate: semaphore.NewWeighted(1)}
		registry[leaseUUID] = lock
	}
	lock.refs++
	registryMu.Unlock()

	if err := lock.gate.Acquire(ctx, 1); err != nil {
		callbackLeaseReleaseReference(registryMu, registry, leaseUUID, lock)
		return nil, err
	}
	return callbackLeaseUnlock(registryMu, registry, leaseUUID, lock), nil
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
		lock = &callbackLeaseLock{gate: semaphore.NewWeighted(1)}
		registry[leaseUUID] = lock
	}
	lock.refs++
	registryMu.Unlock()

	if !lock.gate.TryAcquire(1) {
		callbackLeaseReleaseReference(registryMu, registry, leaseUUID, lock)
		return nil, false
	}
	return callbackLeaseUnlock(registryMu, registry, leaseUUID, lock), true
}

func callbackLeaseUnlock(registryMu *sync.Mutex, registry map[string]*callbackLeaseLock, leaseUUID string, lock *callbackLeaseLock) func() {
	return func() {
		lock.gate.Release(1)
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
	if s.onStoreError == nil {
		return
	}
	// Metrics/observer hooks are foreign application code. Their failure cannot
	// terminate the sole level-triggered replay owner or mutate durable facts.
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("panic in callback store-error hook", "panic", recovered)
		}
	}()
	s.onStoreError()
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
	if s.onDelivery == nil {
		return
	}
	// Delivery accounting is observational. A faulty hook must neither discard
	// a failed durable head nor stop later replay of unrelated leases.
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("panic in callback delivery hook", "panic", recovered)
		}
	}()
	s.onDelivery(outcome)
}
