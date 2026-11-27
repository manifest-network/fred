package docker

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// errRetainedSizingUnfrozen reports a retention row that carries no immutable
// resource snapshot. Every close records one and Start refuses a journal that
// holds a row without it, so a running backend observes one only if a row was
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

// maxNamedUnsizedRetentions bounds how many leases one startup refusal names.
const maxNamedUnsizedRetentions = 8

// refuseUnsizedRetentions fails startup when a retention row records no
// sizing. Only a row retained under v0.13 lacks it, and only the 0.14 line
// records it from the configuration. The row's bytes are on disk with no
// recorded size, and sizing them as zero would undercount the admission pool.
// It must run at startup before any consumer of retained sizing.
func (b *Backend) refuseUnsizedRetentions() error {
	if b.retentionStore == nil {
		return nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return fmt.Errorf("list retention records: %w", err)
	}
	var unsized []string
	for _, entry := range entries {
		if len(entry.ResourceProfiles) == 0 {
			unsized = append(unsized, entry.OriginalLeaseUUID)
		}
	}
	if len(unsized) == 0 {
		return nil
	}
	slices.Sort(unsized)
	named := unsized[:min(len(unsized), maxNamedUnsizedRetentions)]
	return fmt.Errorf(
		"%d retention records, including %s: %w; start the final 0.14 release once to record their sizing",
		len(unsized), strings.Join(named, ", "), errRetainedSizingUnfrozen,
	)
}
