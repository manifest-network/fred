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

// ListRetentionsPage returns one keyset page of retained lease UUIDs, served
// directly from the retention store's ordered bbolt index via a cursor Seek
// (O(limit) per page) rather than reading the whole set and paginating in
// memory. It is the paged sibling of ListRetentions used by the /retentions
// handler. limit is coerced down to backend.MaxPageLimit, mirroring
// PaginateRetentions; limit <= 0 is the unpaginated passthrough.
func (b *Backend) ListRetentionsPage(_ context.Context, after string, limit int) ([]backend.RetainedLease, string, error) {
	if b.retentionStore == nil {
		return []backend.RetainedLease{}, "", nil
	}
	if limit > backend.MaxPageLimit {
		limit = backend.MaxPageLimit
	}
	keys, next, err := b.retentionStore.KeysPage(after, limit)
	if err != nil {
		return nil, "", err
	}
	out := make([]backend.RetainedLease, 0, len(keys))
	for _, k := range keys {
		out = append(out, backend.RetainedLease{LeaseUUID: k})
	}
	return out, next, nil
}
