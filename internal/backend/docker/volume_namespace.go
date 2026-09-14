package docker

import (
	"context"
	"fmt"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// mutateManagedVolumeNamespace keeps ambiguous Docker requests attached to
// their original path namespace. Destroying a directory and later reusing its
// name with a new inode must not evade the physical-volume launch journal.
func (b *Backend) mutateManagedVolumeNamespace(ctx context.Context, names []string, action func(context.Context) error) error {
	parsed := make([]managedVolumeName, 0, len(names))
	leases := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name, err := parseManagedVolumeName(raw)
		if err != nil {
			return fmt.Errorf("managed namespace mutation: %w", err)
		}
		parsed = append(parsed, name)
		leases[managedVolumeLeaseUUID(name)] = struct{}{}
	}
	return b.withManagedVolumeNamespace(ctx, parsed, func(ctx context.Context) error {
		for lease := range leases {
			if err := b.volumeLaunches.checkNamespace(lease); err != nil {
				return err
			}
		}
		return action(ctx)
	})
}

// Quota repair cannot replace a bind namespace. It shares the exact namespace
// and physical exclusion but remains possible while a launch outcome is unknown.
func (b *Backend) withManagedVolumeQuota(ctx context.Context, raw string, action func(context.Context) error) error {
	name, err := parseManagedVolumeName(raw)
	if err != nil {
		return err
	}
	return b.withManagedVolumeNamespace(ctx, []managedVolumeName{name}, action)
}

func (b *Backend) withManagedVolumeNamespace(ctx context.Context, names []managedVolumeName, action func(context.Context) error) error {
	return b.volumeAccess.mutateNamespace(ctx, names, func(ctx context.Context) error {
		var roots []*fsidentity.Directory
		defer func() {
			for _, root := range roots {
				_ = root.Close()
			}
		}()
		ids := make([]fsidentity.Identity, 0, len(names))
		for _, name := range names {
			root, err := b.volumes.PinNamespaceRoot(name)
			if err == nil && root == nil {
				// Create and replayed deletion may name an absent root. Holding
				// its lease namespace prevents local publication while we wait.
				continue
			}
			if err != nil {
				return fmt.Errorf("pin namespace mutation root: %w", err)
			}
			roots = append(roots, root)
			ids = append(ids, root.Identity())
		}
		reserved, err := b.volumeAccess.reserve(ctx, ids)
		if err != nil {
			return err
		}
		defer reserved.release()
		for _, root := range roots {
			if err := root.VerifyPath(); err != nil {
				return fmt.Errorf("namespace mutation root changed: %w", err)
			}
		}
		return action(ctx)
	})
}
