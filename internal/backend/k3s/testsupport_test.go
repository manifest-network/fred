package k3s

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func testK3sResourceProfiles(
	t *testing.T,
	b *Backend,
	items []backend.LeaseItem,
) []shared.SKUResourceSnapshot {
	t.Helper()
	profiles, err := shared.BuildSKUResourceSnapshot(items, b.cfg.GetSKUProfile)
	require.NoError(t, err)
	return profiles
}

type testK3sStorageIdentity struct{}

func (testK3sStorageIdentity) resolve(_ context.Context, cfg Config) (backendidentity.VerifiedStorage, error) {
	const clusterUID = "test-kube-system-uid"
	markerPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
	anchorPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json"
	paths, err := bindK3sStorageInitializationPaths(cfg, markerPath, anchorPath)
	if err != nil {
		return backendidentity.VerifiedStorage{}, err
	}
	defer func() { _ = paths.Close() }()
	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			if err := shared.PrepareBoundCallbackStoreStorage(paths.callbacks, storage, profile); err != nil {
				return err
			}
			return shared.PrepareBoundReleaseStoreStorage(paths.releases, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := shared.CheckBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
				return err
			}
			return shared.CheckBoundReleaseStoreStorage(paths.releases, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundK3sAuthoritativeStoreSet(paths, storage)
		},
	}
	return paths.markers.InitializeWithStores(cfg.Name, clusterUID, hooks)
}

func newBackendWithTestIdentity(cfg Config, logger *slog.Logger) (*Backend, error) {
	b, err := newBackend(context.Background(), cfg, logger, testK3sStorageIdentity{})
	if err == nil {
		b.clusterIdentity = func(context.Context) (string, error) { return "test-kube-system-uid", nil }
	}
	return b, err
}

func bindK3sTestStorageIdentity(t *testing.T, b *Backend) {
	t.Helper()
	b.clusterIdentity = func(context.Context) (string, error) { return "test-kube-system-uid", nil }
}
