package k3s

import "context"

// RefreshState synchronizes in-memory provision state with the underlying
// K8s cluster. In the ENG-133 scaffold there is no cluster-side state to
// reconcile against — runStubProvisioner only writes to in-memory entries
// and the persisted stores, never to the cluster — so this is a no-op,
// mirroring mock-backend's stub (internal/backend/mock.go RefreshState).
//
// ENG-134+ will fill this in alongside the real provisioner: List Pods /
// Deployments owned by Fred, reconcile their status into b.provisions, and
// surface drift that the reconciler can act on (analogous to
// docker-backend's recoverState in internal/backend/docker/info.go).
//
// The method is kept in its own file (rather than provision_stub.go or
// backend.go) to match docker-backend's layout — info.go is where each
// backend's RefreshState lives — so the parallel between backends stays
// easy to follow when ENG-134+ lands the real logic.
func (b *Backend) RefreshState(ctx context.Context) error {
	_ = ctx
	return nil
}
