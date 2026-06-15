package docker

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// retainedVolumePrefix is the namespace soft-deleted volumes are renamed into.
// It keeps the leading "fred-" so listVolumeIDs still enumerates the dir, but the
// distinct "retained" token makes cleanupOrphanedVolumes' expected-set match miss it.
const retainedVolumePrefix = "fred-retained-"

// canonicalVolumeName is the live volume name a provision/restore mounts.
// MUST match setupVolBinds / deprovision / cleanupOrphanedVolumes exactly.
func canonicalVolumeName(leaseUUID, serviceName string, idx int) string {
	return fmt.Sprintf("fred-%s-%s-%d", leaseUUID, serviceName, idx)
}

// retainedName maps a canonical volume name to its retained-namespace name.
func retainedName(canonical string) string {
	return "fred-retained-" + strings.TrimPrefix(canonical, "fred-")
}

// isRetainedVolume reports whether a volume id is a soft-delete tombstone.
func isRetainedVolume(id string) bool {
	return strings.HasPrefix(id, retainedVolumePrefix)
}

// leaseVolumePrefix is the on-disk name prefix of all of a lease's canonical
// volumes (used to enumerate a closing lease's actual volumes). It cannot match
// "fred-retained-..." or another lease's volumes.
func leaseVolumePrefix(leaseUUID string) string {
	return "fred-" + leaseUUID + "-"
}

// destroyRetained hard-deletes one record's volumes then the record itself.
// Used by the per-tenant cap eviction and the grace reaper.
func (b *Backend) destroyRetained(ctx context.Context, e shared.RetentionEntry) error {
	for _, name := range e.RetainedVolumeNames {
		if err := b.volumes.Destroy(ctx, name); err != nil {
			return fmt.Errorf("destroy retained volume %s: %w", name, err)
		}
	}
	return b.retentionStore.Delete(e.OriginalLeaseUUID)
}

// evictRetentionsToCap hard-deletes the CLOSING TENANT's oldest ACTIVE records
// until at most (maxPerTenant-1) of that tenant's remain (making room for one more).
// Never touches another tenant's records. No-op when maxPerTenant<=0.
func (b *Backend) evictRetentionsToCap(ctx context.Context, tenant string, maxPerTenant int) error {
	if b.retentionStore == nil || maxPerTenant <= 0 || tenant == "" {
		return nil
	}
	mine, err := b.retentionStore.ListByTenant(tenant)
	if err != nil {
		return err
	}
	var active []shared.RetentionEntry
	for _, e := range mine {
		if e.Status == shared.RetentionStatusActive {
			active = append(active, e)
		}
	}
	if len(active) < maxPerTenant {
		return nil
	}
	sort.Slice(active, func(i, j int) bool { return active[i].CreatedAt.Before(active[j].CreatedAt) })
	// Evict oldest-first until maxPerTenant-1 remain (making room for one new entry).
	for i := 0; i <= len(active)-maxPerTenant; i++ {
		b.logger.Warn("evicting tenant's oldest retained lease to honor cap", "tenant", tenant, "lease_uuid", active[i].OriginalLeaseUUID)
		if err := b.destroyRetained(ctx, active[i]); err != nil {
			return err
		}
	}
	return nil
}
