package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/errdefs"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func imageCacheTestStorage(t *testing.T) backendidentity.VerifiedStorage {
	t.Helper()
	b := newBackendForTest(&mockDockerClient{}, nil)
	attachBoundOperationHandoffStores(t, b)
	return b.storageAuthority
}

func TestImageCacheOwnershipPersistsAcrossOwnersAndRejectsForeignLineage(t *testing.T) {
	firstID := imageCacheTestStorage(t)
	secondID := imageCacheTestStorage(t)
	var marker *volume.Volume
	creates := 0
	daemon := dockerSDKView{
		volumeInspect: func(context.Context, string) (volume.Volume, error) {
			if marker == nil {
				return volume.Volume{}, errdefs.NotFound(errors.New("unclaimed"))
			}
			return *marker, nil
		},
		volumeCreate: func(_ context.Context, options volume.CreateOptions) (volume.Volume, error) {
			creates++
			marker = &volume.Volume{Name: options.Name, Driver: options.Driver, Labels: options.Labels}
			return *marker, nil
		},
	}
	first, err := claimImageCacheOwnership(t.Context(), daemon, firstID)
	require.NoError(t, err)
	require.NoError(t, first.verify(t.Context()))
	// A stopped process's marker survives: a fresh owner object must prove the
	// same durable identity, not merely acquire an unlocked process mutex.
	restarted, err := claimImageCacheOwnership(t.Context(), daemon, firstID)
	require.NoError(t, err)
	require.NoError(t, restarted.verify(t.Context()))
	_, err = claimImageCacheOwnership(t.Context(), daemon, secondID)
	require.ErrorContains(t, err, "another backend storage lineage")
	require.Equal(t, 1, creates)
	marker.Labels[imageCacheOwnerStorageLabel] = secondID.ID().String()
	require.Error(t, first.verify(t.Context()))
}

func TestImageCacheOwnershipCreateRaceCannotReplaceWinner(t *testing.T) {
	id := imageCacheTestStorage(t)
	daemon := dockerSDKView{
		volumeInspect: func(context.Context, string) (volume.Volume, error) {
			return volume.Volume{}, errdefs.NotFound(errors.New("initially absent"))
		},
		volumeCreate: func(context.Context, volume.CreateOptions) (volume.Volume, error) {
			return volume.Volume{Name: imageCacheOwnerVolume, Driver: "local", Labels: map[string]string{
				imageCacheOwnerStorageLabel: "22222222-2222-4222-8222-222222222222", imageCacheOwnerBackendLabel: "winner", imageCacheOwnerModeLabel: "exclusive",
			}}, nil
		},
	}
	_, err := claimImageCacheOwnership(t.Context(), daemon, id)
	require.ErrorContains(t, err, "another backend storage lineage")
}

func TestImageCacheOwnershipSharedAndExclusiveModesCannotMix(t *testing.T) {
	storage := imageCacheTestStorage(t)
	var marker *volume.Volume
	daemon := dockerSDKView{
		volumeInspect: func(context.Context, string) (volume.Volume, error) {
			if marker == nil {
				return volume.Volume{}, errdefs.NotFound(errors.New("unclaimed"))
			}
			return *marker, nil
		},
		volumeCreate: func(_ context.Context, options volume.CreateOptions) (volume.Volume, error) {
			if marker == nil {
				marker = &volume.Volume{Name: options.Name, Driver: options.Driver, Labels: options.Labels}
			}
			return *marker, nil
		},
	}
	shared, err := claimSharedImageCache(t.Context(), daemon)
	require.NoError(t, err)
	require.NoError(t, shared.verify(t.Context()))
	_, err = claimSharedImageCache(t.Context(), daemon)
	require.NoError(t, err, "independent development backends may share noncollecting storage")
	_, err = claimImageCacheOwnership(t.Context(), daemon, storage)
	require.ErrorContains(t, err, "shared")
	// This models only an explicit drained offline mode transition. Runtime
	// code has no marker deletion or mode-replacement operation.
	marker = nil
	_, err = claimImageCacheOwnership(t.Context(), daemon, storage)
	require.NoError(t, err)
	_, err = claimSharedImageCache(t.Context(), daemon)
	require.ErrorContains(t, err, "exclusive ownership")
	require.Error(t, shared.verify(t.Context()))
}

func TestImageCacheOwnershipRejectsUnverifiedStorageBeforeDaemonCall(t *testing.T) {
	_, err := claimImageCacheOwnership(t.Context(), dockerSDKView{}, backendidentity.VerifiedStorage{})
	require.ErrorContains(t, err, "verified backend storage")
}
