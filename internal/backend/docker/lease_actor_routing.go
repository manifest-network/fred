package docker

import (
	"context"
	"fmt"
	"time"

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
	candidate := newLeaseActor(b, leaseUUID)
	b.actors[leaseUUID] = candidate
	return candidate
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
func (b *Backend) routeToLease(leaseUUID string, msg leasesm.LeaseMessage) bool {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.stopCtx.Err() != nil {
		return false
	}
	actor := b.actorForLocked(leaseUUID)
	return actor.TryEnqueue(msg)
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
func (b *Backend) routeToLeaseBlocking(ctx context.Context, leaseUUID string, msg leasesm.LeaseMessage) error {
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

// ackOrAbort waits for the actor's ack on a caller-facing request
// (Provision / Restart / Update). Returns (true, nil) if the actor
// acked success, (false, err) if the actor rejected or if we abandoned.
//
// Race fix: Go's select picks pseudo-randomly when multiple arms are
// ready. A naive `select { case err := <-ack: ... case <-ctx.Done():
// rollback ... }` can take the cancellation arm even though the actor
// already acked — resulting in a rollback while the actor proceeds to
// spawn a worker against the rolled-back state. Here we do a final
// non-blocking read of ack after observing cancellation; if the actor
// already committed, we honor its decision and skip the rollback.
//
// The caller is responsible for any compensating action (e.g.
// removeProvision on the Provision path) when accepted=false. The
// Restart/Update paths have nothing to compensate post-ENG-230 — they
// no longer write prov.Status/CallbackURL before the ack — so they just
// cancel the op context and return the error.
func (b *Backend) ackOrAbort(ctx context.Context, ack <-chan error) (accepted bool, err error) {
	select {
	case ackErr := <-ack:
		if ackErr != nil {
			return false, ackErr
		}
		return true, nil
	case <-ctx.Done():
	case <-b.stopCtx.Done():
	}
	// Cancellation fired. Final non-blocking check: if the actor acked
	// at the same instant, honor its decision instead of rolling back.
	select {
	case ackErr := <-ack:
		if ackErr != nil {
			return false, ackErr
		}
		return true, nil
	default:
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return false, fmt.Errorf("backend shutting down")
}

// waitForReply waits for an actor's reply channel on a caller-facing
// request whose semantics are "run the work, return the outcome"
// (e.g., Deprovision). This is the ONLY supported way to block on a
// lease actor's reply — the naive
// `select { case err := <-reply: ... case <-ctx.Done(): return ctx.Err() }`
// is unsafe because Go's select is pseudo-randomized when multiple
// arms are ready, so a caller ctx cancel racing the actor's committed
// outcome can return ctx.Err() for an operation that fully succeeded.
// (Same race class as ackOrAbort handles for Provision/Restart/Update.)
//
// Semantics differ from ackOrAbort: here the actor's reply IS the
// outcome of the work (it runs synchronously inside the handler), so
// there's no "accepted vs err" distinction — callers get the outcome
// or a cancellation error.
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
