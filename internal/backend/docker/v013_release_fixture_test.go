package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// seedUpgradedV013ReleaseForBackendTest writes the historical, authorityless
// stack row at the file boundary and then drives the same typed backfills used
// by stopped production adoption. It intentionally does not expose a runtime
// writer capable of manufacturing legacy authority from a new release.
func seedUpgradedV013ReleaseForBackendTest(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	legacy shared.Release,
	items []backend.LeaseItem,
	profiles []shared.SKUResourceSnapshot,
	authority shared.LegacyRuntimeAuthority,
) shared.Release {
	t.Helper()
	attachBoundOperationHandoffStores(t, b)
	if legacy.Version == 0 {
		legacy.Version = 1
	}
	fixture := &fakeReleaseStore{
		Store:   b.releaseStore,
		backend: b,
		authority: &standaloneReleaseTestAuthority{
			config: shared.ReleaseStoreConfig{
				DBPath: b.cfg.ReleasesDBPath,
				MaxAge: b.cfg.ReleasesMaxAge,
			},
			storage: b.storageAuthority,
			gate:    b.storeAuthorityGate,
		},
	}
	fixture.SeedRelease(t, leaseUUID, legacy)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, operations, b.maintenanceSettlement,
	))
	backfiller, err := shared.NewReleaseBackfiller(b.callbackStore, fixture.Store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fixture.Store.Close() })

	require.NoError(t, backfiller.BackfillLegacyActiveAuthorityContext(
		context.Background(), leaseUUID, legacy, items, profiles,
	))
	backfilled, err := fixture.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, backfilled)
	require.NoError(t, backfiller.BackfillLegacyRuntimeAuthorityContext(
		context.Background(), leaseUUID, *backfilled, authority,
	))
	upgraded, err := fixture.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, upgraded)
	return *upgraded
}
