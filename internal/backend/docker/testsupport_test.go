package docker

// actorFor resolves the lease actor for leaseUUID, creating and starting
// one if absent. Test-only: production code uses routeToLease to deliver
// messages without ever exposing an actor pointer to the caller. Tests
// retain direct access for synthetic scenario setup (installing
// workers entries, poking SM state, asserting invariants) that can't
// go through the message path.
func (b *Backend) actorFor(leaseUUID string) *leaseActor {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	return b.actorForLocked(leaseUUID)
}

// handleContainerDeath synchronously dispatches a container death to the
// owning lease's actor and waits for processing to complete. Exists as a
// test helper so direct-call unit tests can keep their synchronous
// assertion style; production code routes die events via
// b.routeToLease(uuid, containerDiedMsg{...}).
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}
	done := make(chan struct{})
	if !b.routeToLease(leaseUUID, containerDiedMsg{containerID: containerID, done: done}) {
		return
	}
	select {
	case <-done:
	case <-b.stopCtx.Done():
	}
}
