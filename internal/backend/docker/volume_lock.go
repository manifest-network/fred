package docker

// Per-volume-name serialization: the lock that makes the ownership check in
// volume_destroy.go survive to the delete it authorizes.
//
// The owner table is a snapshot. On its own that is enough to answer "who owns these
// bytes?" but not to act on the answer, because the writes that establish a claim run
// concurrently with the collectors that read it. The reachable interleaving was the
// retention sweep against a re-provision: the sweep reads a give-up tombstone's canonical
// name as unclaimed, the lease comes back (still ACTIVE on chain), setupVolBinds REUSES
// the abandoned directory — Create adopts a pre-existing one and reports created=false —
// and the sweep's loop then RemoveAll's it. Destroy is not instant on a large volume, so
// the create can also land midway through the removal, leaving the container on a tree
// that keeps shrinking underneath it. (ENG-681)
//
// So the two operations that can touch one volume name — the create-or-reuse here and
// volumeOp.destroyOne — take the same stripe, and the destroy re-reads the live claim
// while holding it. That is what makes a destroy's decision and its delete one step.
//
// Everything funnels through createManagedVolume so the lock cannot be forgotten at a
// future call site; the forbidigo rule in .golangci.yml (pattern `volumes\.Create`,
// excluded only for this file) is what keeps that true, exactly as its sibling rule does
// for the destroy capability.

import (
	"context"
	"hash/fnv"
	"sync"
)

// volumeNameStripeCount is the number of per-volume-name stripe mutexes. Sized and
// justified exactly like tenantNetworkStripeCount (lifecycle.go): a fixed array keeps
// memory constant against attacker-creatable keys — volume names embed lease UUIDs, so a
// map[name]*Mutex would grow with every lease the node has ever seen — and a collision
// only serializes two unrelated volumes' (infrequent) create/destroy with each other.
const volumeNameStripeCount = 256

// volumeNameMu returns the stripe mutex for a managed volume name.
//
// Lock ordering: volume-name stripe -> provisionsMu (RLock), matching the tenant-network
// rule documented on Backend. Neither holder takes provisionsMu on entry, and a volume
// stripe is never held across a tenant-network acquisition.
func (b *Backend) volumeNameMu(name string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return &b.volumeNameStripes[h.Sum32()%volumeNameStripeCount]
}

// createManagedVolume is the create-or-reuse half of the serialization: it is the only
// call to volumeManager.Create in the process, and it holds the name's stripe across it.
//
// Holding it makes the two operations order rather than interleave. A destroy either
// finishes first — and this then creates a fresh directory, which is the correct outcome
// once the control plane has said nobody owned the old one — or it finds the claim this
// lease published at its reservation and refuses.
func (b *Backend) createManagedVolume(ctx context.Context, volumeID string, sizeMB int64) (string, bool, error) {
	mu := b.volumeNameMu(volumeID)
	mu.Lock()
	defer mu.Unlock()
	return b.mutationAdapter().createVolume(ctx, volumeID, sizeMB)
}
