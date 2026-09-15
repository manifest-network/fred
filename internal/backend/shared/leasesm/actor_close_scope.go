package leasesm

import (
	"sync/atomic"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// ActorCloseScope is an opaque, callback-lifetime proof minted only by a lease
// actor after its deprovision transition has drained mutation workers. Copies
// share revocation state and become invalid when the actor callback returns or
// panics.
type ActorCloseScope struct{ state *actorCloseScopeState }

type actorCloseScopeState struct {
	actor   *LeaseActor
	lineage shared.RecoveryLineage
	active  atomic.Bool
}

func newActorCloseScope(actor *LeaseActor) ActorCloseScope {
	state := &actorCloseScopeState{actor: actor, lineage: actor.cfg.RecoveryLineage}
	state.active.Store(true)
	return ActorCloseScope{state: state}
}

func (scope ActorCloseScope) revoke() {
	if scope.state != nil {
		scope.state.active.Store(false)
	}
}

func (scope ActorCloseScope) LeaseUUID() string {
	if scope.state == nil || scope.state.actor == nil || !scope.state.active.Load() {
		return ""
	}
	return scope.state.actor.leaseUUID
}

// Matches proves that scope belongs to the exact registered actor and recovery
// coordinator lineage supplied by the substrate composition root.
func (scope ActorCloseScope) Matches(
	actor *LeaseActor,
	lineage shared.RecoveryLineage,
) bool {
	return scope.state != nil && scope.state.actor == actor && actor != nil &&
		scope.state.lineage == lineage && lineage != (shared.RecoveryLineage{}) &&
		scope.state.active.Load()
}
