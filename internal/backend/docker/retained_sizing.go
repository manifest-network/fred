package docker

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// errRetainedSizingUnfrozen reports a retention row that carries no immutable
// resource snapshot. Start freezes every such row before anything sizes a
// retained footprint, so a running backend observes one only if a row was
// written without sizing after startup, which is a bug.
var errRetainedSizingUnfrozen = errors.New("retention record carries no frozen resource snapshot")

// retainedResourceProfiles returns the immutable sizing authority frozen onto
// entry. It takes no Backend, so it cannot consult the live configuration: a
// retained footprint is sized only by its own row, and resizing or removing a
// SKU profile can neither change it nor stop the backend from starting.
func retainedResourceProfiles(entry shared.RetentionEntry) ([]shared.SKUResourceSnapshot, error) {
	if len(entry.ResourceProfiles) == 0 {
		return nil, fmt.Errorf("retention %s: %w", entry.OriginalLeaseUUID, errRetainedSizingUnfrozen)
	}
	if err := validateDockerResourceProfiles(entry.Items, entry.ResourceProfiles); err != nil {
		return nil, fmt.Errorf("retention %s resource profiles: %w", entry.OriginalLeaseUUID, err)
	}
	return shared.CloneSKUResourceSnapshot(entry.ResourceProfiles), nil
}

// freezeRetentionSizing freezes immutable sizing onto every retention row
// written before rows carried a resource snapshot (v0.13). Each such row's
// SKUs are resolved against the configuration exactly once, here; afterwards
// only retainedResourceProfiles sizes it. It must run at startup before any
// consumer of retained sizing.
//
// A row whose SKU the configuration cannot resolve fails startup, naming the
// lease and SKU. Its bytes are on disk with no recorded size, and sizing them
// as zero would undercount the admission pool. Restoring that SKU's profile
// once lets the row freeze; the profile can then be removed.
func (b *Backend) freezeRetentionSizing() (frozen int, err error) {
	if b.retentionStore == nil {
		return 0, nil
	}
	candidates, err := b.retentionStore.ListUnfrozenSizingCandidates()
	if err != nil {
		return 0, fmt.Errorf("list retention records without frozen sizing: %w", err)
	}
	var errs []error
	for _, candidate := range candidates {
		entry := candidate.Entry()
		profiles, resolveErr := b.resolveResourceProfiles(entry.Items)
		if resolveErr != nil {
			errs = append(errs, fmt.Errorf(
				"retention %s has no frozen sizing and its SKU no longer resolves; restore the SKU profile it was retained under so its sizing can be frozen: %w",
				entry.OriginalLeaseUUID, resolveErr,
			))
			continue
		}
		if freezeErr := b.retentionStore.FreezeResourceProfiles(candidate, profiles); freezeErr != nil {
			errs = append(errs, fmt.Errorf("freeze sizing for retention %s: %w", entry.OriginalLeaseUUID, freezeErr))
			continue
		}
		frozen++
	}
	return frozen, errors.Join(errs...)
}
