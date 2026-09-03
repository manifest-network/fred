package docker

import (
	"fmt"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// snapshotResourceProfiles converts the already-resolved profiles used by a
// Docker operation into canonical durable authority without consulting mutable
// configuration a second time. Every diskless Docker SKU conservatively holds
// one configured writable-path scratch allowance per instance. The allowance
// is pinned even when the current image needs no host writable-path volume:
// image inspection happens after the write-ahead operation intent, so exact
// conditional admission would require a mutable two-phase intent.
func (b *Backend) snapshotResourceProfiles(
	items []backend.LeaseItem,
	profiles map[string]SKUProfile,
) ([]shared.SKUResourceSnapshot, error) {
	return snapshotDockerResourceProfiles(items, profiles, b.cfg.GetTmpfsSizeMB())
}

func snapshotDockerResourceProfiles(
	items []backend.LeaseItem,
	profiles map[string]SKUProfile,
	tmpfsSizeMB int,
) ([]shared.SKUResourceSnapshot, error) {
	snapshot, err := shared.BuildSKUResourceSnapshot(items, func(sku string) (shared.SKUProfile, error) {
		profile, ok := profiles[sku]
		if !ok {
			return shared.SKUProfile{}, fmt.Errorf("resolved profile for SKU %q is missing", sku)
		}
		return profile, nil
	})
	if err != nil {
		return nil, err
	}
	scratchDiskMB := int64(tmpfsSizeMB)
	if scratchDiskMB <= 0 {
		return nil, fmt.Errorf("writable-path scratch size must be positive")
	}
	for i := range snapshot {
		if snapshot[i].DiskMB == 0 {
			snapshot[i].ScratchDiskMB = scratchDiskMB
		}
	}
	if err := validateDockerResourceProfiles(items, snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func resolveResourceProfilesForConfig(
	cfg Config,
	items []backend.LeaseItem,
) ([]shared.SKUResourceSnapshot, error) {
	profiles := make(map[string]SKUProfile)
	for _, item := range items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, err := cfg.GetSKUProfile(item.SKU)
		if err != nil {
			return nil, err
		}
		profiles[item.SKU] = profile
	}
	return snapshotDockerResourceProfiles(items, profiles, cfg.GetTmpfsSizeMB())
}

// validateDockerResourceProfiles adds Docker's host-volume invariant to the
// substrate-neutral shared schema. K3s legitimately persists scratch=0;
// Docker does not: every new diskless authority must carry the positive value
// captured before admission. Only a wholly absent v0.13 profile set may be
// resolved from current configuration by an explicit compatibility path.
func validateDockerResourceProfiles(
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) error {
	if err := shared.ValidateSKUResourceSnapshot(items, resourceProfiles); err != nil {
		return err
	}
	for _, row := range resourceProfiles {
		if row.DiskMB == 0 && row.ScratchDiskMB <= 0 {
			return fmt.Errorf("docker resource profile for diskless SKU %q has no pinned scratch disk", row.SKU)
		}
	}
	return nil
}

func resourceProfileMap(
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) (map[string]SKUProfile, error) {
	if err := validateDockerResourceProfiles(items, resourceProfiles); err != nil {
		return nil, err
	}
	profiles := make(map[string]SKUProfile, len(resourceProfiles))
	for _, row := range resourceProfiles {
		profiles[row.SKU] = row.Profile()
	}
	return profiles, nil
}

func resourceSnapshotMap(
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) (map[string]shared.SKUResourceSnapshot, error) {
	if err := validateDockerResourceProfiles(items, resourceProfiles); err != nil {
		return nil, err
	}
	resources := make(map[string]shared.SKUResourceSnapshot, len(resourceProfiles))
	for _, row := range resourceProfiles {
		resources[row.SKU] = row
	}
	return resources, nil
}

func (b *Backend) resolveResourceProfiles(
	items []backend.LeaseItem,
) ([]shared.SKUResourceSnapshot, error) {
	return resolveResourceProfilesForConfig(b.cfg, items)
}

// activeResourceProfiles returns the resource authority pinned to the active
// release. A v0.13 row has no snapshot; in that compatibility case the current
// configuration is resolved once and compare-and-swap backfilled before the
// value is returned, so later config changes cannot reprice the lease again.
func (b *Backend) activeResourceProfiles(
	leaseUUID string,
	items []backend.LeaseItem,
) ([]shared.SKUResourceSnapshot, error) {
	if b.releaseStore == nil {
		return b.resolveResourceProfiles(items)
	}
	active, err := b.releaseStore.LatestActive(leaseUUID)
	if err != nil {
		return nil, fmt.Errorf("read active release resource profiles: %w", err)
	}
	if active != nil && len(active.ResourceProfiles) > 0 {
		if !slices.Equal(active.Items, items) {
			return nil, fmt.Errorf("active release topology differs from live provision")
		}
		if err := validateDockerResourceProfiles(items, active.ResourceProfiles); err != nil {
			return nil, fmt.Errorf("active release resource profiles: %w", err)
		}
		return shared.CloneSKUResourceSnapshot(active.ResourceProfiles), nil
	}

	resourceProfiles, err := b.resolveResourceProfiles(items)
	if err != nil {
		return nil, fmt.Errorf("resolve legacy active release resource profiles: %w", err)
	}
	if active != nil && slices.Equal(active.Items, items) {
		if err := b.releaseStore.BackfillActiveResourceProfiles(
			leaseUUID, active.Version, active.Items, resourceProfiles,
		); err != nil {
			return nil, fmt.Errorf("freeze legacy active release resource profiles: %w", err)
		}
	}
	return resourceProfiles, nil
}
