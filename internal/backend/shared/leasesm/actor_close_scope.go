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
	actor.activeClose.Store(state)
	return ActorCloseScope{state: state}
}

func (scope ActorCloseScope) revoke() {
	if scope.state != nil {
		scope.state.active.Store(false)
		scope.state.actor.activeClose.CompareAndSwap(scope.state, nil)
	}
}

// CloseOwnsObservation reports that a current runtime's death is already owned
// by this actor's active close callback. It is only a logging/routing decision:
// the existing close scope remains the sole destructive authority. A stored
// Deprovisioning status alone, or an unreadable/stale runtime proof, is not
// enough to classify a refused crash observation as expected.
func (a *LeaseActor) CloseOwnsObservation(observation ActorObservation) bool {
	if a == nil {
		return false
	}
	scope := ActorCloseScope{state: a.activeClose.Load()}
	if !scope.Matches(a, a.cfg.RecoveryLineage) {
		return false
	}
	death, ok := observation.message().(containerDiedMsg)
	if !ok || death.Runtime.LeaseUUID() != a.leaseUUID || death.Runtime.Reattest() != nil {
		return false
	}
	generation := a.cfg.ProvisionStore.ClassifyReadyRuntime(death.Runtime)
	return (generation == ObservationGenerationCurrent || generation == ObservationGenerationAdvanced) &&
		scope.Matches(a, a.cfg.RecoveryLineage)
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
