package docker

import (
	"context"
	"errors"
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
		reserved, err := b.reserveManagedNamespaceRoots(ctx, names)
		if err != nil {
			return err
		}
		defer reserved.reservation.release()
		return action(ctx)
	})
}

// managedVolumeReservation owns namespace-root observations and physical
// exclusion without retaining any directory descriptor. A launch may open its
// exact roots only after this owner has acquired and revalidated the reservation.
type managedVolumeReservation struct {
	reservation *reservedVolumeSet
	identities  map[managedVolumeName]fsidentity.Identity
}

func (r *managedVolumeReservation) openRoot(name managedVolumeName, path string) (*fsidentity.Directory, error) {
	if r == nil || r.reservation == nil || r.reservation.lifetime == nil || r.reservation.lifetime.released.Load() {
		return nil, errors.New("managed volume reservation is unavailable")
	}
	identity := r.identities[name]
	if !identity.Valid() {
		return nil, errors.New("launch volume was absent from its physical reservation")
	}
	return fsidentity.OpenBoundDirectory(path, identity)
}

// reserveManagedNamespaceRoots returns only exclusion, never a directory
// descriptor. A descriptor retained by either the active mutation or an alias
// waiter would keep an unlinked XFS project inode charged while Destroy waits
// for zero usage. Lease namespaces remain locked throughout this preparation
// and the caller's effect; physical reservations remain held after it returns.
func (b *Backend) reserveManagedNamespaceRoots(ctx context.Context, names []managedVolumeName) (ownership *managedVolumeReservation, err error) {
	observed := make(map[managedVolumeName]fsidentity.Identity, len(names))
	ids := make([]fsidentity.Identity, 0, len(names))
	for _, name := range names {
		identity, err := b.inspectManagedNamespaceRoot(name)
		if err != nil {
			return nil, fmt.Errorf("inspect namespace mutation root: %w", err)
		}
		observed[name] = identity
		if identity.Valid() {
			ids = append(ids, identity)
		}
	}
	reserved, err := b.volumeAccess.reserve(ctx, ids)
	if err != nil {
		return nil, err
	}
	defer func() {
		if ownership == nil {
			reserved.release()
		}
	}()
	// Discovery identities select lock keys only. Reobserve every name after
	// waiting, including positive absence: a newly appeared root must not enter
	// an effect without its physical reservation. The manager's own attestation
	// still determines whether the exact operation may mutate that root.
	for _, name := range names {
		identity, err := b.inspectManagedNamespaceRoot(name)
		if err != nil || identity != observed[name] {
			return nil, fmt.Errorf("namespace mutation root changed: %w", errors.Join(fsidentity.ErrDirectoryIdentityChanged, err))
		}
	}
	return &managedVolumeReservation{reservation: reserved, identities: observed}, nil
}

// inspectManagedNamespaceRoot owns and closes its probe before returning. The
// zero identity means the concrete manager proved absence (or has no physical
// namespace); it never means a failed or unreadable directory inspection.
func (b *Backend) inspectManagedNamespaceRoot(name managedVolumeName) (identity fsidentity.Identity, err error) {
	root, err := b.volumes.PinNamespaceRoot(name)
	if err != nil || root == nil {
		return fsidentity.Identity{}, err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	if err := root.VerifyPath(); err != nil {
		return fsidentity.Identity{}, err
	}
	return root.Identity(), nil
}
