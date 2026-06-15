package docker

import (
	"cmp"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
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

// canonicalFromRetained is the inverse of retainedName.
func canonicalFromRetained(retained string) string {
	return "fred-" + strings.TrimPrefix(retained, "fred-retained-")
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

// renameIfPresent is a best-effort reconcile rename: RenameVolume errors both
// for the benign no-op/conflict cases (both or neither name present) AND for a
// real Docker-daemon failure. It logs and RETURNS the error so callers can
// decide whether the failure is fatal to their step (e.g. the restoring-arm
// rollback must NOT advance the record if a re-quarantine rename actually
// failed, or the still-canonical volume would be reaped). reconcileRetentions'
// active arm tolerates the error because cleanupOrphanedVolumes independently
// protects retention-record canonicals.
func (b *Backend) renameIfPresent(oldName, newName string) error {
	if err := b.volumes.RenameVolume(oldName, newName); err != nil {
		b.logger.Warn("reconcile rename skipped", "old", oldName, "new", newName, "error", err)
		return err
	}
	return nil
}

// reconcileRetentions repairs crash-interrupted soft-deletes/restores. MUST run
// AFTER recoverState (so b.provisions reflects live containers) and BEFORE
// cleanupOrphanedVolumes (so a mid-rename canonical dir is moved back into the
// fred-retained- namespace before the orphan reaper could destroy it).
func (b *Backend) reconcileRetentions(ctx context.Context) error {
	if b.retentionStore == nil {
		return nil
	}
	all, err := b.retentionStore.List()
	if err != nil {
		return err
	}
	for _, e := range all {
		switch e.Status {
		case shared.RetentionStatusActive:
			// Crash after Put before rename: a canonical volume may still be on disk.
			// On rename failure we log and keep going — cleanupOrphanedVolumes
			// independently protects this record's canonical from the reaper.
			for _, retained := range e.RetainedVolumeNames {
				canonical := canonicalFromRetained(retained)
				if rerr := b.renameIfPresent(canonical, retained); rerr != nil {
					b.logger.Warn("reconcile: re-quarantine of active canonical failed (cleanup protection covers it)",
						"lease_uuid", e.OriginalLeaseUUID, "canonical", canonical, "error", rerr)
				}
			}
		case shared.RetentionStatusRestoring:
			b.reconcileRestoring(ctx, e)
		}
	}
	return nil
}

// reconcileRestoring finalizes or rolls back an interrupted/failed restore,
// conservatively (defers to an in-flight restore; generation-CAS rollback).
func (b *Backend) reconcileRestoring(ctx context.Context, e shared.RetentionEntry) {
	b.provisionsMu.RLock()
	p, live := b.provisions[e.NewLeaseUUID]
	var status backend.ProvisionStatus
	if live {
		status = p.Status
	}
	b.provisionsMu.RUnlock()

	if live && status == backend.ProvisionStatusReady {
		_ = b.retentionStore.Delete(e.OriginalLeaseUUID) // restore finished; drop leftover record
		return
	}
	if live && status == backend.ProvisionStatusRestarting {
		// Restarting only arises during the PERIODIC reaper sweep (Task 6) while
		// fred is running and a restore is genuinely in flight — doRestore's
		// terminal defer owns the record, so defer to it. At STARTUP this branch
		// cannot fire: recoverState rebuilds provisions from live containers and
		// never yields Restarting, so a restore that crashed mid-flight is
		// (correctly) rolled back by the orphaned-arm below.
		return
	}
	// Orphaned (crash/failed): tear down any orphaned project, re-quarantine the
	// adopted volumes back to the retained namespace, then CAS the record to active.
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if derr := b.compose.Down(ctx, composeProjectName(e.NewLeaseUUID), stopTimeout); derr != nil {
		b.logger.Warn("reconcile: compose down failed (continuing)", "lease_uuid", e.NewLeaseUUID, "error", derr)
	}
	// Re-quarantine each adopted volume. A REAL rename failure (not a benign
	// no-op) means the volume may still be canonical-named: we must NOT advance
	// the record to active or drop the provision, or cleanupOrphanedVolumes (and
	// future sweeps) could destroy still-live data. Leave the record restoring so
	// the next startup retries; the provision stays so its expected-set entry
	// keeps protecting the data in the interim.
	failed := false
	for _, item := range e.Items {
		for i := range item.Quantity {
			newCanonical := canonicalVolumeName(e.NewLeaseUUID, item.ServiceName, i)
			origRetained := retainedName(canonicalVolumeName(e.OriginalLeaseUUID, item.ServiceName, i))
			if rerr := b.renameIfPresent(newCanonical, origRetained); rerr != nil {
				failed = true
			}
		}
	}
	if failed {
		b.logger.Warn("reconcile: re-quarantine rename failed; leaving record restoring for next startup",
			"lease_uuid", e.OriginalLeaseUUID, "new_lease_uuid", e.NewLeaseUUID)
		return
	}
	if ok, err := b.retentionStore.RevertToActive(e.OriginalLeaseUUID, e.Generation); err != nil {
		b.logger.Error("reconcile: revert restoring->active failed", "lease_uuid", e.OriginalLeaseUUID, "error", err)
	} else if ok {
		b.removeProvision(e.NewLeaseUUID)
	}
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
