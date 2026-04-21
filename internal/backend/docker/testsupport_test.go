package docker

// handleContainerDeath synchronously dispatches a container death to the
// owning lease's actor and waits for processing to complete. Exists as a
// test helper so direct-call unit tests can keep their synchronous
// assertion style; production code routes die events directly into the
// actor inbox via b.actorFor(...).send(containerDiedMsg{...}).
func (b *Backend) handleContainerDeath(containerID string) {
	leaseUUID, found := b.findLeaseByContainerID(containerID)
	if !found {
		return
	}
	done := make(chan struct{})
	if !b.actorFor(leaseUUID).send(containerDiedMsg{containerID: containerID, done: done}) {
		return
	}
	select {
	case <-done:
	case <-b.stopCtx.Done():
	}
}
