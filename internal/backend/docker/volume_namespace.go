package docker

import (
	"context"
	"fmt"
)

// mutateManagedVolumeNamespace keeps ambiguous Docker requests attached to
// their original path namespace. Destroying a directory and later reusing its
// name with a new inode must not evade the physical-volume launch journal.
func (b *Backend) mutateManagedVolumeNamespace(ctx context.Context, names []string, action func(context.Context) error) error {
	leases := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name, err := parseManagedVolumeName(raw)
		if err != nil {
			return fmt.Errorf("managed namespace mutation: %w", err)
		}
		leases[managedVolumeLeaseUUID(name)] = struct{}{}
	}
	return b.volumeAccess.mutateNamespace(ctx, func(ctx context.Context) error {
		for lease := range leases {
			if err := b.volumeLaunches.checkNamespace(lease); err != nil {
				return err
			}
		}
		return action(ctx)
	})
}
