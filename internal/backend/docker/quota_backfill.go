package docker

import (
	"context"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// reconcileVolumeQuotas re-applies each existing managed volume's quota (for
// xfs: project-tag + block (bhard) and inode (ihard) limits) at startup, so
// leases provisioned while the daemon lacked CAP_SYS_ADMIN (ENG-454) get
// their disk_mb enforced without a re-provision or data move. It is
// idempotent and best-effort per volume: a single volume's failure (e.g. a
// concurrent deprovision) is logged and skipped, never fatal.
//
// It enumerates the volumes that SHOULD carry a quota — active lease instances
// (stateful, or ephemeral-with-writable-path) and active-status retained volumes
// — then re-applies only to those actually present on disk, via each backend's
// EnsureQuota primitive. For xfs, EnsureQuota re-tags the inode (project -s) AND
// re-applies the limit, healing a volume a pre-CAP_SYS_ADMIN daemon left
// untagged; unlike Create it never creates a missing volume. It must run after
// recoverState (so b.provisions is populated) and reconcileRetentions (so the
// fred-retained- namespace is settled), and before the serving loops.
func (b *Backend) reconcileVolumeQuotas(ctx context.Context) {
	if b.cfg.VolumeDataPath == "" {
		return // noop backend: no quota-enforced volumes
	}

	existingList, err := b.volumes.List()
	if err != nil {
		b.logger.Warn("quota backfill: cannot list volumes; skipping", "error", err)
		return
	}
	existing := make(map[string]struct{}, len(existingList))
	for _, n := range existingList {
		existing[n] = struct{}{}
	}

	// name → sizeMB for every volume expected to carry a disk quota.
	want := make(map[string]int64)

	// Active leases. Snapshot under RLock; do NOT hold it across the quota exec
	// calls below (mirrors cleanupOrphanedVolumes).
	b.provisionsMu.RLock()
	for leaseUUID, prov := range b.provisions {
		for _, item := range prov.Items {
			profile, perr := b.cfg.GetSKUProfile(item.SKU)
			if perr != nil {
				continue // unknown SKU: nothing we can size
			}
			// Mirror provision.go's sizing: a stateful item is capped at its
			// disk_mb; an ephemeral (disk_mb=0) item with an image writable-path
			// still gets an on-disk volume, sized at the tmpfs fallback — so it
			// must be re-capped too (else a pre-CAP writable volume grows
			// unbounded). Items with no on-disk volume are dropped by the
			// existence gate below.
			sizeMB := profile.DiskMB
			if sizeMB <= 0 {
				sizeMB = int64(b.cfg.GetTmpfsSizeMB())
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
			b.logger.Warn("quota backfill: cannot list retentions; active-lease volumes still processed", "error", lerr)
		} else {
			for _, e := range entries {
				if e.Status != shared.RetentionStatusActive {
					continue
				}
				retainedSet := make(map[string]struct{}, len(e.RetainedVolumeNames))
				for _, n := range e.RetainedVolumeNames {
					retainedSet[n] = struct{}{}
				}
				for _, item := range e.Items {
					profile, perr := b.cfg.GetSKUProfile(item.SKU)
					if perr != nil || profile.DiskMB <= 0 {
						continue
					}
					for i := range item.Quantity {
						name := retainedName(canonicalVolumeName(e.OriginalLeaseUUID, item.ServiceName, i))
						if _, ok := retainedSet[name]; ok {
							want[name] = profile.DiskMB
						}
					}
				}
			}
		}
	}

	var applied, failed, absent int
	for name, sizeMB := range want {
		if _, ok := existing[name]; !ok {
			absent++ // expected but not on disk (stateless instance, or already gone)
			continue
		}
		if cerr := b.volumes.EnsureQuota(ctx, name, sizeMB); cerr != nil {
			failed++
			volumeQuotaBackfillTotal.WithLabelValues("failed").Inc()
			b.logger.Warn("quota backfill: failed to re-apply quota",
				"volume", name, "size_mb", sizeMB, "error", cerr)
			continue
		}
		applied++
		volumeQuotaBackfillTotal.WithLabelValues("applied").Inc()
	}
	if applied > 0 || failed > 0 {
		b.logger.Info("volume quota backfill complete", "backend", b.volumes.Kind(),
			"applied", applied, "failed", failed, "expected_absent", absent)
	}
}
