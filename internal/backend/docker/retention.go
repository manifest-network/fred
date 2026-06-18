package docker

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
)

// ListRetentions returns the leases this docker backend currently retains
// (soft-deleted, awaiting restore or grace-reap), read from the retention
// store. Used by fred's reconciler for restore backend affinity (ENG-333).
// Returns an empty slice when the retention store is not configured.
func (b *Backend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	if b.retentionStore == nil {
		return []backend.RetainedLease{}, nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return nil, err
	}
	out := make([]backend.RetainedLease, 0, len(entries))
	for _, e := range entries {
		out = append(out, backend.RetainedLease{LeaseUUID: e.OriginalLeaseUUID})
	}
	return out, nil
}
