package docker

import "os"

// isWritablePathOnly reports whether the managed volume named `name` holds ONLY
// writable-path scaffolding — a single _wp/ subtree — and no declared-VOLUME
// (stateful) data. Such a volume exists only because the image tripped
// read-only-rootfs writable-path auto-detection (needsWritableVolume) without
// declaring a VOLUME (needsStatefulVolume), so its content is ephemeral by the
// ENG-367 wipe-contract: _wp is wiped and reseeded from the image on every
// deploy including restore. Retaining it on close therefore preserves nothing
// restorable — it only pollutes the retention record, a per-tenant slot, the
// retained-disk budget, and leaves a fred-retained-* dir (ENG-406).
//
// The close-time refuse action gated by this predicate is DESTROY, so it is
// deliberately CONSERVATIVE toward RETAIN: it returns true only when the volume
// is PROVABLY writable-path-only. Any ambiguity — an empty host path, a ReadDir
// error, or any top-level entry other than _wp and known volume-manager
// housekeeping — yields false (retain). A stateful volume always has its
// declared-VOLUME subdir created directly under the volume root
// (buildStatefulVolumeBinds), so its presence is what this check looks for the
// absence of; misclassifying a stateful volume as writable-path-only would
// destroy tenant data, which this asymmetry-aware default prevents.
//
// The "a stateful volume always has its VOLUME subdir under the root" invariant
// holds because buildStatefulVolumeBinds runs whenever needsStatefulVolume
// (profile.DiskMB > 0 && the image declares a VOLUME). For a DiskMB<=0 SKU the
// declared VOLUME is NOT backed by this managed volume at all (it falls to an
// anonymous docker volume, reaped on teardown), so the managed volume genuinely
// holds only _wp and reclaiming it loses no durable data either way.
func (b *Backend) isWritablePathOnly(name string) bool {
	hostPath := b.volumes.HostPath(name)
	if hostPath == "" {
		return false // unknown location → retain
	}
	entries, err := os.ReadDir(hostPath)
	if err != nil {
		return false // can't inspect → retain
	}
	sawWritablePathDir := false
	for _, e := range entries {
		switch {
		case e.Name() == writablePathSubdir && e.IsDir():
			// The ephemeral writable-path scaffolding subtree. Require it to be a
			// real directory: setupWritablePathBinds always creates _wp as one, and
			// the container can't write the volume root, so a file/symlink named _wp
			// is unexpected — fall through to retain rather than destroy.
			sawWritablePathDir = true
		case e.Name() == projectIDFile && !e.IsDir():
			// xfs writes this quota marker FILE inside every volume root; it is
			// fred housekeeping, not tenant data. btrfs/zfs write nothing here.
			// Only a regular file is the real marker — a DIRECTORY of this name is
			// tenant data from a declared VOLUME (e.g. VOLUME /.fred-project-id) and
			// must NOT be whitelisted, or the volume would be misclassified and
			// destroyed. Such a dir falls through to the default arm → retain.
		default:
			// Any other top-level entry is a declared-VOLUME data subdir (created by
			// buildStatefulVolumeBinds), or an unexpected/non-directory _wp → stateful
			// or ambiguous → retain.
			return false
		}
	}
	// Only writable-path-only when the _wp subtree is actually present and
	// nothing stateful sits beside it. An empty/unexpected volume falls through
	// to false (retain) — the conservative direction given the destroy action.
	return sawWritablePathDir
}
