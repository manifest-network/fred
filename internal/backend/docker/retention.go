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
	keys, err := b.retentionStore.Keys()
	if err != nil {
		return nil, err
	}
	out := make([]backend.RetainedLease, 0, len(keys))
	for _, k := range keys {
		out = append(out, backend.RetainedLease{LeaseUUID: k})
	}
	return out, nil
}
