package docker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// seedUpgradedV013ReleaseForBackendTest writes the active release of a lease
// adopted from v0.13 as the first 0.14 start left it: the v0.13 row with its
// items, resource profiles and tokenless runtime authority recorded. Nothing in
// production writes this state any more; restart and update only carry it
// forward.
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
	require.Nil(t, legacy.RuntimeAuthority)
	require.False(t, legacy.OperationID.Valid())
	legacy.Items = items
	legacy.ResourceProfiles = profiles
	legacy.LegacyRuntimeAuthority = &authority
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
	fixture.seedReleaseRows(t, map[string]shared.Release{leaseUUID: legacy})
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	require.NoError(t, bindBackendTestPhysicalExecutors(
		t, b, operations, b.maintenanceSettlement,
	))
	t.Cleanup(func() { _ = fixture.Store.Close() })

	upgraded, err := fixture.Store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, upgraded)
	require.NotNil(t, upgraded.LegacyRuntimeAuthority)
	return *upgraded
}
