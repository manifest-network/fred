package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func beginCloseRecoveryIntentWithProfiles(
	t *testing.T,
	stores closeRecoveryStores,
	cleanupOnly bool,
	profiles []shared.SKUResourceSnapshot,
) shared.CloseIntentClaim {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	_, _, _ = seedCloseRecoveryReleaseWithProfiles(t, stores, "", items, profiles)
	if cleanupOnly {
		request, err := stores.close.NewCleanupCloseRequest(closeRecoveryLeaseUUID)
		require.NoError(t, err)
		admission, err := stores.close.BeginCleanupClose(request)
		require.NoError(t, err)
		return admission.Claim()
	}
	request, err := stores.close.NewCloseRequest(closeRecoveryLeaseUUID, false)
	require.NoError(t, err)
	admission, err := stores.close.BeginClose(request)
	require.NoError(t, err)
	return admission.Claim()
}

func TestRecoverState_CloseIntentUsesImmutableResourcesAfterConfigChange(t *testing.T) {
	tests := []struct {
		name        string
		cleanupOnly bool
		mutate      func(*Backend)
	}{
		{
			name: "full close removed SKU", cleanupOnly: false,
			mutate: func(b *Backend) { delete(b.cfg.SKUProfiles, "docker-small") },
		},
		{
			name: "full close downsized SKU", cleanupOnly: false,
			mutate: func(b *Backend) {
				b.cfg.SKUProfiles["docker-small"] = SKUProfile{CPUCores: 0.25, MemoryMB: 128, DiskMB: 0}
			},
		},
		{
			name: "cleanup-only close removed SKU", cleanupOnly: true,
			mutate: func(b *Backend) { delete(b.cfg.SKUProfiles, "docker-small") },
		},
		{
			name: "cleanup-only close downsized SKU", cleanupOnly: true,
			mutate: func(b *Backend) {
				b.cfg.SKUProfiles["docker-small"] = SKUProfile{CPUCores: 0.25, MemoryMB: 128, DiskMB: 0}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
			volumeState := newVolumeSet(volumeName)
			volumeState.destroyFn = func(string) error { return errors.New("keep close pending") }
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())

			// Commit a nonzero immutable authority, then mutate the live config before
			// rebuilding the backend projection. Recovery must not consult the latter.
			profiles := []shared.SKUResourceSnapshot{{
				SKU: "docker-small", CPUCores: 1.5, MemoryMB: 768, DiskMB: 4096,
			}}
			claim := beginCloseRecoveryIntentWithProfiles(t, stores, tt.cleanupOnly, profiles)
			require.Equal(t, profiles, claim.ResourceProfiles())
			tt.mutate(b)

			require.NoError(t, b.recoverState(context.Background()))
			allocation := b.pool.GetAllocation(closeRecoveryLeaseUUID + "-app-0")
			require.NotNil(t, allocation)
			require.Equal(t, 1.5, allocation.CPUCores)
			require.Equal(t, int64(768), allocation.MemoryMB)
			require.Equal(t, int64(4096), allocation.DiskMB)
			if tt.cleanupOnly {
				require.Empty(t, allocation.Tenant)
			} else {
				require.Equal(t, "tenant-a", allocation.Tenant)
			}

			closeCloseRecoveryBackend(t, b, stores)
		})
	}
}

func TestRecoverState_CloseIntentUsesPinnedScratchAfterConfigRemoval(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
	volumeState := newVolumeSet(volumeName)
	volumeState.destroyFn = func(string) error { return errors.New("keep close pending") }
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())
	profiles := []shared.SKUResourceSnapshot{{
		SKU: "docker-small", CPUCores: 0.5, MemoryMB: 512, ScratchDiskMB: 73,
	}}
	claim := beginCloseRecoveryIntentWithProfiles(t, stores, false, profiles)
	require.Equal(t, int64(73), claim.ResourceProfiles()[0].ScratchDiskMB)
	require.Zero(t, claim.ResourceProfiles()[0].DiskMB)

	// Model a restart after the operator both resizes scratch and removes the
	// original SKU. Recovery must rebuild the close reservation from the journal.
	b.cfg.ContainerTmpfsSizeMB = 999
	delete(b.cfg.SKUProfiles, "docker-small")
	require.NoError(t, b.recoverState(context.Background()))
	allocation := b.pool.GetAllocation(closeRecoveryLeaseUUID + "-app-0")
	require.NotNil(t, allocation)
	require.Equal(t, int64(73), allocation.DiskMB)

	closeCloseRecoveryBackend(t, b, stores)
}
