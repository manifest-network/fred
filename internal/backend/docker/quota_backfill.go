package docker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// reconcileVolumeQuotas re-applies each existing managed volume's quota (for
// xfs: project-tag + block (bhard) and inode (ihard) limits) at startup, so
// leases provisioned while the daemon lacked CAP_SYS_ADMIN (ENG-454) get
// their immutable effective quota enforced without a re-provision or data
// move. Effective quota is durable DiskMB or, for a physically present
// diskless writable-path volume, its mutually exclusive pinned ScratchDiskMB.
// It is idempotent and attempts every expected, present volume even when one
// fails. Any inventory, durable-authority, or quota-enforcement failure is
// returned after the walk so Start cannot report readiness while a tenant
// volume may be running without its immutable cap.
//
// It enumerates the volumes that SHOULD carry a quota — active lease instances
// (stateful, or ephemeral-with-writable-path) and active-status retained volumes
// — then re-applies only to those actually present on disk, via each backend's
// EnsureQuota primitive. For xfs, EnsureQuota re-tags the inode (project -s) AND
// re-applies the limit, healing a volume a pre-CAP_SYS_ADMIN daemon left
// untagged; unlike Create it never creates a missing volume. It must run after
// recoverState (so b.provisions is populated) and reconcileRetentions (so the
// fred-retained- namespace is settled), and before the serving loops.
func (b *Backend) reconcileVolumeQuotas(ctx context.Context) error {
	if b.cfg.VolumeDataPath == "" {
		return nil // noop backend: no quota-enforced volumes
	}

	existing, err := attestManagedVolumeInventory(ctx, b.volumes)
	if err != nil {
		return fmt.Errorf("attest managed volumes for quota reconciliation: %w", err)
	}

	// name → sizeMB for every volume expected to carry a disk quota.
	want := make(map[string]int64)
	var reconcileErrs []error

	// Active leases. Snapshot under RLock; do NOT hold it across the quota exec
	// calls below (mirrors cleanupOrphanedVolumes).
	b.provisionsMu.RLock()
	for leaseUUID, prov := range b.provisions {
		resourceProfiles := prov.ResourceProfiles
		var resolveErr error
		if len(resourceProfiles) == 0 {
			// Explicit v0.13 compatibility only. Normal startup recovery freezes
			// this value into the active Release before quota reconciliation.
			resourceProfiles, resolveErr = b.resolveResourceProfiles(prov.Items)
		}
		resourcesBySKU, profileErr := resourceSnapshotMap(prov.Items, resourceProfiles)
		if resolveErr != nil || profileErr != nil {
			authorityErr := errors.Join(resolveErr, profileErr)
			b.logger.Error("quota reconciliation: live lease has invalid resource authority",
				"lease_uuid", leaseUUID, "error", authorityErr)
			reconcileErrs = append(reconcileErrs, fmt.Errorf(
				"resolve live lease %q quota authority: %w", leaseUUID, authorityErr,
			))
			continue
		}
		for _, item := range prov.Items {
			resources, ok := resourcesBySKU[item.SKU]
			if !ok {
				reconcileErrs = append(reconcileErrs, fmt.Errorf(
					"resolve live lease %q SKU %q quota authority: missing validated resource snapshot",
					leaseUUID, item.SKU,
				))
				continue
			}
			// Mirror provision.go's sizing: a stateful item is capped at its
			// disk_mb; an ephemeral (disk_mb=0) item with an image writable-path
			// still gets an on-disk volume, sized at the tmpfs fallback — so it
			// must be re-capped too (else a pre-CAP writable volume grows
			// unbounded). Items with no on-disk volume are dropped by the
			// existence gate below.
			sizeMB, sizeErr := resources.EffectiveDiskMB()
			if sizeErr != nil {
				reconcileErrs = append(reconcileErrs, fmt.Errorf(
					"resolve live lease %q SKU %q effective quota: %w", leaseUUID, item.SKU, sizeErr,
				))
				continue
			}
			if sizeMB <= 0 {
				continue
			}
			for i := range item.Quantity {
				want[canonicalVolumeName(leaseUUID, item.ServiceName, i)] = sizeMB
			}
		}
	}
	b.provisionsMu.RUnlock()

	// Retained volumes — active status only. Restoring/reaping entries may have
	// their on-disk volumes renamed to the new lease or be mid-destroy.
	if b.retentionStore != nil {
		entries, lerr := b.retentionStore.List()
		if lerr != nil {
			b.logger.Error("quota reconciliation: cannot list retentions; active-lease volumes still processed", "error", lerr)
			reconcileErrs = append(reconcileErrs, fmt.Errorf("list retained quota authority: %w", lerr))
		} else {
			for _, e := range entries {
				if e.Status != shared.RetentionStatusActive {
					continue
				}
				resourceProfiles := e.ResourceProfiles
				var resolveErr error
				if len(resourceProfiles) == 0 {
					resourceProfiles, resolveErr = b.resolveResourceProfiles(e.Items)
				}
				resourcesBySKU, profileErr := resourceSnapshotMap(e.Items, resourceProfiles)
				if resolveErr != nil || profileErr != nil {
					authorityErr := errors.Join(resolveErr, profileErr)
					b.logger.Error("quota reconciliation: retention has invalid resource authority",
						"lease_uuid", e.OriginalLeaseUUID, "error", authorityErr)
					reconcileErrs = append(reconcileErrs, fmt.Errorf(
						"resolve retained lease %q quota authority: %w", e.OriginalLeaseUUID, authorityErr,
					))
					continue
				}
				retainedSet := make(map[string]struct{}, len(e.RetainedVolumeNames))
				for _, n := range e.RetainedVolumeNames {
					retainedSet[n] = struct{}{}
				}
				for _, item := range e.Items {
					resources, ok := resourcesBySKU[item.SKU]
					if !ok {
						reconcileErrs = append(reconcileErrs, fmt.Errorf(
							"resolve retained lease %q SKU %q quota authority: missing validated resource snapshot",
							e.OriginalLeaseUUID, item.SKU,
						))
						continue
					}
					sizeMB, sizeErr := resources.EffectiveDiskMB()
					if sizeErr != nil {
						reconcileErrs = append(reconcileErrs, fmt.Errorf(
							"resolve retained lease %q SKU %q effective quota: %w",
							e.OriginalLeaseUUID, item.SKU, sizeErr,
						))
						continue
					}
					if sizeMB <= 0 {
						continue
					}
					for i := range item.Quantity {
						name := retainedName(canonicalVolumeName(e.OriginalLeaseUUID, item.ServiceName, i))
						if _, ok := retainedSet[name]; ok {
							// Scratch remains non-retainable policy-wise, but a
							// conservatively retained exact name still occupies host disk
							// and must keep its original quota until it is destroyed.
							want[name] = sizeMB
						}
					}
				}
			}
		}
	}

	var applied, failed, absent int
	for _, name := range slices.Sorted(maps.Keys(want)) {
		sizeMB := want[name]
		if _, ok := existing[name]; !ok {
			absent++ // expected but not on disk (stateless instance, or already gone)
			continue
		}
		if cerr := b.mutationAdapter().ensureVolumeQuota(ctx, name, sizeMB); cerr != nil {
			failed++
			volumeQuotaBackfillTotal.WithLabelValues("failed").Inc()
			b.logger.Warn("quota backfill: failed to re-apply quota",
				"volume", name, "size_mb", sizeMB, "error", cerr)
			reconcileErrs = append(reconcileErrs, fmt.Errorf(
				"enforce %s quota for managed volume %q: %w", b.volumes.Kind(), name, cerr,
			))
			continue
		}
		applied++
		volumeQuotaBackfillTotal.WithLabelValues("applied").Inc()
	}
	if applied > 0 || failed > 0 {
		b.logger.Info("volume quota backfill complete", "backend", b.volumes.Kind(),
			"applied", applied, "failed", failed, "expected_absent", absent)
	}
	return errors.Join(reconcileErrs...)
}
