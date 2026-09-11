package docker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// This file holds the Backend-side methods that orchestrate lease-actor
// routing: registry lookup, message delivery, ack/reply synchronization,
// metrics sampling, and operator-introspection. They were extracted from
// lease_actor.go at ENG-148 PR5b-2 checkpoint B0; at D the actor type
// itself moved to shared/leasesm so these methods now reach the actor
// via the exported leasesm API surface (LeaseActor, TryEnqueue, State,
// InboxDepth, InboxCap, CurrentMessageStart) rather than dot-accessing
// unexported fields.
//
// All methods in this file have `(b *Backend)` receivers and access
// fields on *Backend (b.actors, b.actorsMu, b.stopCtx, b.wg).

// actorForLocked returns the lease actor for leaseUUID, creating + starting
// one on first access. Caller MUST hold b.actorsMu. The whole point of the
// registry mutex: resolve-or-create and any subsequent state change (enqueue,
// exit) serialize through the same lock.
//
// At BC the spawn (b.wg.Go(run)) and ActorCreated metric moved into
// leasesm.NewLeaseActor's body, which newLeaseActor (in the factory)
// invokes from inside the actorsMu critical section — so registry-resolve
// and spawn remain atomic without actorForLocked needing to do either step
// directly.
func (b *Backend) actorForLocked(leaseUUID string) *leasesm.LeaseActor {
	if existing, ok := b.actors[leaseUUID]; ok {
		return existing
	}
	return newLeaseActor(b, leaseUUID)
}

// routeToLease is the ONLY way external code delivers a message to a lease's
// actor. It resolves-or-creates the actor AND enqueues atomically under
// actorsMu — so the stale-pointer race class (caller retained an actor
// reference while the actor was terminating) cannot occur by construction:
// callers never hold a *leasesm.LeaseActor pointer.
//
// Returns false if the backend is shutting down OR the inbox is full (the
// enqueue is non-blocking to avoid holding the registry mutex across a
// potentially-slow channel send). Fire-and-forget callers
// (containerEventLoop, reconcile) treat refusal as "reconciler will
// re-detect". Caller-facing API paths that need backpressure-retry
// semantics should use routeToLeaseBlocking instead.
func (b *Backend) routeToLease(leaseUUID string, msg leasesm.ActorCommand) bool {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.stopCtx.Err() != nil {
		return false
	}
	if b.actorRecoveryClaims[leaseUUID] != nil {
		return false
	}
	actor := b.actorForLocked(leaseUUID)
	return actor.TryEnqueueCommand(msg)
}

// routeActorObservation admits a substrate observation only for the exact
// durable runtime generation that produced it. The store-issued proof is
// re-attested before actor resolution and again by the actor at serial handling
// time. A stale event therefore cannot materialize an actor for a replacement
// generation, while a current event may lazily create the actor initialized
// from that exact Ready projection.
func (b *Backend) routeActorObservation(
	observation leasesm.ActorObservation,
) bool {
	leaseUUID := observation.LeaseUUID()
	if leaseUUID == "" {
		return false
	}
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.stopCtx.Err() != nil || b.actorRecoveryClaims[leaseUUID] != nil {
		return false
	}
	if !observation.Current(b.provisionStore) {
		return false
	}
	actor := b.actors[leaseUUID]
	created := false
	if actor == nil {
		actor = b.actorForLocked(leaseUUID)
		created = true
	}
	valid := actor.TryEnqueueObservation(observation)
	if !valid && created {
		if quiescent := actor.TryClaimQuiescence(); quiescent != nil {
			quiescent.Retire()
			quiescent.Release()
		}
	}
	if !valid {
		return false
	}
	return true
}

// captureRuntimeGenerationProofs snapshots observational authority before a
// recovery inventory begins. A later cohort classification may use only the
// proof captured here and only while its full release-row digest remains
// current. Legacy v0.13 rows without frozen runtime authority are skipped for
// this pass; recovery may backfill them from the observed cohort, and the next
// level-triggered pass can then capture a proof before its inventory.
func (b *Backend) captureRuntimeGenerationProofs() (
	map[string]shared.RuntimeGenerationProof,
	error,
) {
	proofs := make(map[string]shared.RuntimeGenerationProof)
	if b.releaseStore == nil {
		return proofs, nil
	}
	leaseUUIDs, err := b.releaseStore.LeaseUUIDs()
	if err != nil {
		return nil, fmt.Errorf("enumerate runtime generations: %w", err)
	}
	for _, leaseUUID := range leaseUUIDs {
		active, activeErr := b.releaseStore.LatestActive(leaseUUID)
		if activeErr != nil {
			return nil, fmt.Errorf("read active runtime generation for lease %q: %w", leaseUUID, activeErr)
		}
		if active == nil {
			continue
		}
		if _, hasAuthority := active.RuntimeIdentity(); !hasAuthority {
			continue
		}
		proof, proofErr := b.releaseStore.ProveRuntimeGeneration(leaseUUID)
		if proofErr != nil {
			return nil, fmt.Errorf("prove active runtime generation for lease %q: %w", leaseUUID, proofErr)
		}
		proofs[leaseUUID] = proof
	}
	return proofs, nil
}

// leaseActorRecoveryClaim is an opaque, copy-safe capability that reserves one
// actor-registry key and, when an actor exists, owns its quiescence claim. The
// shared state makes copied handles release exactly once.
type leaseActorRecoveryClaim struct{ state *leaseActorRecoveryClaimState }

type leaseActorRecoveryClaimState struct {
	backend    *Backend
	leaseUUID  string
	actor      *leasesm.LeaseActor
	actorClaim *leasesm.QuiescenceClaim
	once       sync.Once
}

// recoveryProjectionPublication is the sole capability that permits
// recoverState to replace or remove actor-visible provision generations. Its
// constructor acquires the registry and projection locks in their canonical
// order. An existing actor generation can be superseded only after
// ClaimReplacement proves that the actor has no accepted message or worker and
// holds both admission gates until publication completes.
//
// The capability is deliberately private and pointer-only. Callers cannot
// manufacture one, and Release is idempotent so an early publication failure
// cannot leak either lock or a quiescence claim.
type recoveryProjectionPublication struct {
	backend  *Backend
	claims   map[string]projectionActorReplacement
	released bool
}

type projectionActorReplacement struct {
	actor      *leasesm.LeaseActor
	quiescence *leasesm.QuiescenceClaim
}

func (b *Backend) beginRecoveryProjectionPublication() *recoveryProjectionPublication {
	b.actorsMu.Lock()
	b.provisionsMu.Lock()
	return &recoveryProjectionPublication{
		backend: b,
		claims:  make(map[string]projectionActorReplacement),
	}
}

// ClaimReplacement returns true when this publication capability owns the
// right to replace the lease's current projection generation. An absent actor
// needs no per-actor claim because actorsMu itself prevents construction until
// publication completes. A separate recovery owner, queued message, active
// handler, or worker makes the change ineligible for this pass.
func (c *recoveryProjectionPublication) ClaimReplacement(leaseUUID string) bool {
	if c == nil || c.backend == nil || c.released || leaseUUID == "" {
		return false
	}
	if _, claimed := c.claims[leaseUUID]; claimed {
		return true
	}
	if c.backend.actorRecoveryClaims[leaseUUID] != nil {
		return false
	}
	actor := c.backend.actors[leaseUUID]
	if actor == nil {
		return true
	}
	claim := actor.TryClaimQuiescence()
	if claim == nil {
		return false
	}
	c.claims[leaseUUID] = projectionActorReplacement{
		actor:      actor,
		quiescence: claim,
	}
	return true
}

// ClaimOrphanActor retires an idle registry actor for a lease that has no
// projection before or after recovery. Without this case, an old Ready FSM can
// survive a no-container recovery and reject every later fresh Provision for
// the same UUID. Active actors are left untouched and retried next sweep.
func (c *recoveryProjectionPublication) ClaimOrphanActor(leaseUUID string) bool {
	if c == nil || c.backend == nil || c.released || leaseUUID == "" {
		return false
	}
	if c.backend.actors[leaseUUID] == nil {
		return true
	}
	return c.ClaimReplacement(leaseUUID)
}

func (c *recoveryProjectionPublication) ActorLeaseUUIDs() []string {
	if c == nil || c.backend == nil || c.released {
		return nil
	}
	leaseUUIDs := make([]string, 0, len(c.backend.actors))
	for leaseUUID := range c.backend.actors {
		leaseUUIDs = append(leaseUUIDs, leaseUUID)
	}
	return leaseUUIDs
}

// RetireClaimedActors consumes the actor-side authority immediately before the
// corresponding projection publication. It must be called only after the pool
// accepted the matching accounting snapshot; an aborted pool update therefore
// leaves both the old projection and its actor live.
func (c *recoveryProjectionPublication) RetireClaimedActors() {
	if c == nil || c.released {
		return
	}
	for leaseUUID, replacement := range c.claims {
		replacement.quiescence.Retire()
		// Detach the proven-idle generation inside the same registry critical
		// section as projection publication. A post-publication observation can
		// then construct an actor from the new projection immediately instead of
		// being spuriously refused by an asynchronously retiring predecessor.
		if c.backend.actors[leaseUUID] == replacement.actor {
			delete(c.backend.actors, leaseUUID)
		}
	}
}

func (c *recoveryProjectionPublication) Release() {
	if c == nil || c.backend == nil || c.released {
		return
	}
	c.released = true
	for _, replacement := range c.claims {
		replacement.quiescence.Release()
	}
	c.backend.provisionsMu.Unlock()
	c.backend.actorsMu.Unlock()
}

func (c *leaseActorRecoveryClaim) Release() {
	if c == nil || c.state == nil || c.state.backend == nil {
		return
	}
	s := c.state
	s.once.Do(func() {
		s.backend.actorsMu.Lock()
		defer s.backend.actorsMu.Unlock()
		if s.actorClaim != nil {
			s.actorClaim.Release()
		}
		if s.backend.actorRecoveryClaims[s.leaseUUID] == s {
			delete(s.backend.actorRecoveryClaims, s.leaseUUID)
		}
	})
}

// RetireActor consumes the actor half of the capability after recovery has
// removed its projection. An absent-key reservation has no actor and is
// already complete. The registry reservation remains held until Release.
func (c *leaseActorRecoveryClaim) RetireActor() {
	if c == nil || c.state == nil || c.state.actorClaim == nil || c.state.backend == nil {
		return
	}
	s := c.state
	s.backend.actorsMu.Lock()
	defer s.backend.actorsMu.Unlock()
	if !s.actorClaim.Retire() {
		return
	}
	// The quiescence claim owns both actor admission gates and the registry
	// reservation prevents a replacement until recovery returns. Detaching the
	// exact claimed generation here makes recovery projection publication
	// target-free: recovery never re-enters an actor whose locks it already
	// owns, and the next live command constructs a fresh actor from the recovered
	// projection.
	if s.backend.actors[s.leaseUUID] == s.actor {
		delete(s.backend.actors, s.leaseUUID)
	}
}

// tryClaimLeaseActorQuiescence atomically reserves an absent registry key or
// proves an existing actor has no accepted message/worker. The returned typed
// capability keeps routeToLease from creating/admitting work until Release.
func (b *Backend) tryClaimLeaseActorQuiescence(leaseUUID string) *leaseActorRecoveryClaim {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.actorRecoveryClaims[leaseUUID] != nil {
		return nil
	}
	var (
		actor      *leasesm.LeaseActor
		actorClaim *leasesm.QuiescenceClaim
	)
	if actor = b.actors[leaseUUID]; actor != nil {
		actorClaim = actor.TryClaimQuiescence()
		if actorClaim == nil {
			return nil
		}
	}
	if b.actorRecoveryClaims == nil {
		b.actorRecoveryClaims = make(map[string]*leaseActorRecoveryClaimState)
	}
	state := &leaseActorRecoveryClaimState{
		backend: b, leaseUUID: leaseUUID, actor: actor, actorClaim: actorClaim,
	}
	b.actorRecoveryClaims[leaseUUID] = state
	return &leaseActorRecoveryClaim{state: state}
}

// withRecoveryLeaseExclusion is the construction-bound admission barrier used
// by the shared recovery coordinator. It holds both the per-lease command fence
// and the actor registry/quiescence reservation until run returns. Busy live
// work is a normal deferral so one lease cannot stall fleet convergence.
func (b *Backend) withRecoveryLeaseExclusion(
	ctx context.Context,
	leaseUUID string,
	run func() error,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	unlock, acquired := b.commandFence.TryLock(leaseUUID)
	if !acquired {
		return false, nil
	}
	defer unlock()
	actorClaim := b.tryClaimLeaseActorQuiescence(leaseUUID)
	if actorClaim == nil {
		return false, nil
	}
	defer actorClaim.Release()
	// Recovery already owns the actor's admission and activity gates. Retire
	// and detach that exact quiescent generation before invoking the recovery
	// callback so recovery can publish directly without trying to enqueue back
	// into the actor it excludes. A preserved projection will lazily construct a
	// fresh actor after this registry reservation is released.
	actorClaim.RetireActor()
	err := run()
	return true, err
}

func (b *Backend) validateActorCloseScope(
	value shared.ActorCloseScope,
	lineage shared.RecoveryLineage,
) (string, bool) {
	scope, ok := value.(leasesm.ActorCloseScope)
	if !ok {
		return "", false
	}
	leaseUUID := scope.LeaseUUID()
	if leaseUUID == "" {
		return "", false
	}
	b.actorsMu.Lock()
	actor := b.actors[leaseUUID]
	valid := scope.Matches(actor, lineage)
	b.actorsMu.Unlock()
	return leaseUUID, valid
}

// routeToLeaseBlocking wraps routeToLease with ctx-bounded retry so
// caller-facing API paths (Provision, Deprovision, Restart, Update)
// don't spuriously fail on transient inbox saturation. Returns nil on
// successful enqueue, ctx.Err() on caller cancellation, or a "backend
// shutting down" error when stopCtx fires. Polls on
// routeToLeaseRetryInterval while the inbox is full — a few ms of
// latency is acceptable for API calls; the alternative is turning
// backpressure into a 5xx.
//
// The up-front ctx / stopCtx check guarantees we don't enqueue a
// message the caller is about to abandon: without it, the first
// routeToLease could succeed and start async work while the caller
// returns ctx.Err() having seen nothing.
func (b *Backend) routeToLeaseBlocking(
	ctx context.Context,
	leaseUUID string,
	msg leasesm.ActorCommand,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.stopCtx.Err() != nil {
		return fmt.Errorf("backend shutting down")
	}
	for {
		if b.routeToLease(leaseUUID, msg) {
			return nil
		}
		if b.stopCtx.Err() != nil {
			return fmt.Errorf("backend shutting down")
		}
		select {
		case <-b.stopCtx.Done():
			return fmt.Errorf("backend shutting down")
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(routeToLeaseRetryInterval):
		}
	}
}

// routeToLeaseRetryInterval is the poll interval for routeToLeaseBlocking
// when the target inbox is momentarily full. Short enough that API-call
// latency from backpressure is negligible in normal operation (inbox
// rarely fills); long enough to avoid hot-spinning a contended actor.
const routeToLeaseRetryInterval = 10 * time.Millisecond

type asyncAcceptance uint8

const (
	asyncAcceptanceAccepted asyncAcceptance = iota + 1
	asyncAcceptanceRejected
	asyncAcceptanceUnknown
)

// awaitAsyncAcceptance classifies actor admission for a durable asynchronous
// operation. Once a message is enqueued, caller cancellation cannot prove that
// the actor did not accept and spawn its worker immediately after our final
// channel read. That
// arm is therefore Unknown: callers retain the write-ahead intent and all
// reservations for startup recovery. Only an explicit actor error authorizes
// rollback and intent cancellation.
func (b *Backend) awaitAsyncAcceptance(ctx context.Context, ack <-chan error) (asyncAcceptance, error) {
	select {
	case ackErr := <-ack:
		if ackErr != nil {
			return asyncAcceptanceRejected, ackErr
		}
		return asyncAcceptanceAccepted, nil
	case <-ctx.Done():
	case <-b.stopCtx.Done():
	}
	select {
	case ackErr := <-ack:
		if ackErr != nil {
			return asyncAcceptanceRejected, ackErr
		}
		return asyncAcceptanceAccepted, nil
	default:
	}
	if ctx.Err() != nil {
		return asyncAcceptanceUnknown, ctx.Err()
	}
	return asyncAcceptanceUnknown, fmt.Errorf("backend shutting down")
}

// waitForReply waits for an actor's reply channel on a caller-facing
// request whose semantics are "run the work, return the outcome"
// (e.g., Deprovision). This is the ONLY supported way to block on a
// lease actor's reply — the naive
// `select { case err := <-reply: ... case <-ctx.Done(): return ctx.Err() }`
// is unsafe because Go's select is pseudo-randomized when multiple
// arms are ready, so a caller ctx cancel racing the actor's committed
// outcome can return ctx.Err() for an operation that fully succeeded.
// (The final non-blocking read is the same race treatment used by
// awaitAsyncAcceptance for durable asynchronous commands.)
//
// Here the actor's reply IS the outcome of the work (it runs synchronously
// inside the handler), so there is no "accepted vs err" distinction: callers
// get the outcome or a cancellation error.
func (b *Backend) waitForReply(ctx context.Context, reply <-chan error) error {
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
	case <-b.stopCtx.Done():
	}
	select {
	case err := <-reply:
		return err
	default:
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("backend shutting down")
}

// ActorSnapshot is a point-in-time view of one lease actor's state for
// operator introspection. Safe to marshal to JSON for a /debug/actors
// endpoint when integrated with the HTTP layer.
type ActorSnapshot struct {
	LeaseUUID  string `json:"lease_uuid"`
	SMState    string `json:"sm_state"`    // current SM state
	InboxDepth int    `json:"inbox_depth"` // pending messages not yet processed
	InboxCap   int    `json:"inbox_cap"`
}

// actorMetricsSampleInterval paces sampleActorMetrics. Short enough for
// the stuck-actor gauge to react within an alerting window, long enough
// that walking the registry and sampling inbox depth stays negligible.
const actorMetricsSampleInterval = 5 * time.Second

// sampleActorMetrics walks every live actor, observing inbox depth into
// the histogram and finding the oldest in-flight handle() start across
// all actors for the stuck-seconds gauge. Called periodically from
// actorMetricsSampleLoop.
//
// Holds actorsMu only long enough to snapshot the actor list, then observes
// outside the lock. inbox len() and currentMessageStart are both safe to
// read concurrently with actor work (inbox len is racy-but-fine; atomic
// load for currentMessageStart).
func (b *Backend) sampleActorMetrics() {
	now := time.Now().UnixNano()
	b.actorsMu.Lock()
	actors := make([]*leasesm.LeaseActor, 0, len(b.actors))
	for _, actor := range b.actors {
		actors = append(actors, actor)
	}
	b.actorsMu.Unlock()

	var oldestStart int64
	for _, actor := range actors {
		leaseActorInboxDepth.Observe(float64(actor.InboxDepth()))
		if start := actor.CurrentMessageStart(); start != 0 {
			if oldestStart == 0 || start < oldestStart {
				oldestStart = start
			}
		}
	}
	if oldestStart == 0 {
		leaseActorStuckSeconds.Set(0)
	} else {
		leaseActorStuckSeconds.Set(float64(now-oldestStart) / float64(time.Second))
	}
}

// sampleCloseIntentMetrics projects the non-expiring close journal into two
// low-cardinality gauges. A read failure preserves the last known values: zeroing
// them would falsely report that destructive work completed. The callback-store
// error counter and health check carry the read failure itself.
func (b *Backend) sampleCloseIntentMetrics(now time.Time) {
	if b.callbackStore == nil {
		pendingCloseIntents.Set(0)
		oldestCloseIntentAgeSeconds.Set(0)
		return
	}
	claims, err := b.closeSettlement.ListCloseIntents()
	if err != nil {
		callbackStoreErrorsTotal.Inc()
		return
	}

	pendingCloseIntents.Set(float64(len(claims)))
	if len(claims) == 0 {
		oldestCloseIntentAgeSeconds.Set(0)
		return
	}
	oldest := claims[0].CreatedAt()
	for _, claim := range claims[1:] {
		if claim.CreatedAt().Before(oldest) {
			oldest = claim.CreatedAt()
		}
	}
	age := now.Sub(oldest)
	if age < 0 {
		age = 0
	}
	oldestCloseIntentAgeSeconds.Set(age.Seconds())
}

// sampleLeaseMutationCapacityMetrics projects both O(1) durable callback
// journal counters. A read failure preserves the last known values: reporting
// zero pressure would hide a callback-journal fault, while
// callbackStoreErrorsTotal and /health carry the failure itself.
func (b *Backend) sampleLeaseMutationCapacityMetrics() {
	if b.callbackStore == nil {
		leaseMutationUUIDSlots.Set(0)
		leaseMutationUUIDSlotLimit.Set(0)
		callbackReceiptReservations.Set(0)
		callbackReceiptReservationLimit.Set(0)
		return
	}
	uuidCapacity, err := b.callbackStore.LeaseMutationUUIDCapacity()
	if err != nil {
		callbackStoreErrorsTotal.Inc()
		return
	}
	receiptCapacity, err := b.callbackStore.CallbackReceiptCapacity()
	if err != nil {
		callbackStoreErrorsTotal.Inc()
		return
	}
	leaseMutationUUIDSlots.Set(float64(uuidCapacity.Reserved))
	leaseMutationUUIDSlotLimit.Set(float64(uuidCapacity.Limit))
	callbackReceiptReservations.Set(float64(receiptCapacity.Reserved))
	callbackReceiptReservationLimit.Set(float64(receiptCapacity.Limit))
}

// actorMetricsSampleLoop runs sampleActorMetrics on a ticker until the
// backend shuts down. Spawned once from Start() via b.wg.Go.
func (b *Backend) actorMetricsSampleLoop() {
	ticker := time.NewTicker(actorMetricsSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			b.sampleActorMetrics()
			b.sampleCloseIntentMetrics(time.Now())
			b.sampleLeaseMutationCapacityMetrics()
		}
	}
}

// DebugActors returns a snapshot of every live lease actor. The result
// is stable for the caller: it's a copy; the registry may grow or
// change state after return. Intended for ops introspection during
// incidents — pair with a /debug/actors HTTP handler that JSON-encodes
// the return.
func (b *Backend) DebugActors() []ActorSnapshot {
	b.actorsMu.Lock()
	actors := make(map[string]*leasesm.LeaseActor, len(b.actors))
	for uuid, actor := range b.actors {
		actors[uuid] = actor
	}
	b.actorsMu.Unlock()

	snapshots := make([]ActorSnapshot, 0, len(actors))
	for leaseUUID, actor := range actors {
		snapshots = append(snapshots, ActorSnapshot{
			LeaseUUID:  leaseUUID,
			SMState:    fmt.Sprintf("%v", actor.State()),
			InboxDepth: actor.InboxDepth(),
			InboxCap:   actor.InboxCap(),
		})
	}
	return snapshots
}
