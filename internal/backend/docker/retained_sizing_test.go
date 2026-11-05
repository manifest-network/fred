package docker

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

const v013RetainedLease = "550e8400-e29b-41d4-a716-446655440000"

// reopenRetentionStoreWithV013Row writes entry as it was persisted before
// retention rows carried a resource snapshot, then points b at the reopened
// store. No current writer can produce such a row; only an adopted v0.13
// journal holds one. The close and restore settlements are rebuilt over the
// reopened store.
func reopenRetentionStoreWithV013Row(t *testing.T, b *Backend, entry shared.RetentionEntry) *shared.RetentionStore {
	t.Helper()
	require.Empty(t, entry.ResourceProfiles)
	path := b.cfg.RetentionDBPath
	require.NoError(t, b.retentionStore.Close())
	row, err := json.Marshal(struct {
		SchemaVersion uint8                 `json:"schema_version"`
		Entry         shared.RetentionEntry `json:"entry"`
	}{SchemaVersion: 1, Entry: entry})
	require.NoError(t, err)
	writeRawLegacyRetentionRow(t, path, entry.OriginalLeaseUUID, row)

	require.True(t, b.storageAuthority.Valid())
	reopened, err := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: path}, b.storageAuthority, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	b.retentionStore = reopened
	closeSettlement, err := shared.NewCloseSettlement(b.callbackStore, b.releaseStore, reopened)
	require.NoError(t, err)
	bindBackendTestCloseExecutor(t, b, closeSettlement)
	b.closeSettlement = closeSettlement
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	restore, err := shared.NewRestoreSettlement(operations, reopened)
	require.NoError(t, err)
	b.restoreSettlement = restore
	return reopened
}

func v013RetentionEntry(sku string) shared.RetentionEntry {
	return shared.RetentionEntry{
		OriginalLeaseUUID:   v013RetainedLease,
		Tenant:              "tenant-a",
		ProviderUUID:        nominalDockerProviderUUID,
		Items:               []backend.LeaseItem{{SKU: sku, Quantity: 1, ServiceName: "app"}},
		CallbackURL:         "https://fred.example/callbacks/provision",
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(v013RetainedLease, "app", 0))},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}
}

// startableBackendOverJournals builds a backend bound to the journals in dir,
// with profiles as its SKU configuration and the v0.13 row's retained volume
// on disk. seed, when set, writes the v0.13 row before the backend captures
// its inventory. It returns the quota each Start applies, by volume name.
func startableBackendOverJournals(
	t *testing.T,
	dir string,
	profiles map[string]SKUProfile,
	seed *shared.RetentionEntry,
) (*Backend, map[string]int64) {
	t.Helper()
	b, _ := newBackendWithRetention(t)
	mock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	mock.PingFn = func(context.Context) error { return nil }
	mock.CloseFn = func() error { return nil }
	bindTestStorageIdentityAt(t, b, mock, dir)
	attachRetentionStore(t, b)
	t.Cleanup(b.stopCancel)
	if seed != nil {
		reopenRetentionStoreWithV013Row(t, b, *seed)
	}

	b.cfg.SKUProfiles = maps.Clone(profiles)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	volumeName := retainedName(canonicalVolumeName(v013RetainedLease, "app", 0))
	quotas := map[string]int64{}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{volumeName}, nil },
		EnsureQuotaFn: func(_ context.Context, name string, sizeMB int64) error {
			quotas[name] = sizeMB
			return nil
		},
	}
	bindRetentionOrphanPrunerForTest(t, b)
	return b, quotas
}

var v013RetentionProfiles = map[string]SKUProfile{
	"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 100},
}

func startableBackendWithV013Retention(t *testing.T, sku string) (*Backend, map[string]int64) {
	t.Helper()
	seed := v013RetentionEntry(sku)
	return startableBackendOverJournals(t, t.TempDir(), v013RetentionProfiles, &seed)
}

// The first start freezes a v0.13 row's sizing. A full restart over the same
// journals, with the row's SKU profile removed, must then start and enforce
// the frozen size (ENG-1517 F-1).
func TestRestartAfterSKUProfileRemovalStartsOnFrozenV013RetentionSizing(t *testing.T) {
	ctx := context.Background()
	volumeName := retainedName(canonicalVolumeName(v013RetainedLease, "app", 0))
	journals := t.TempDir()
	seed := v013RetentionEntry("stateful")
	first, firstQuotas := startableBackendOverJournals(t, journals, v013RetentionProfiles, &seed)
	require.NoError(t, first.Start(ctx))
	assert.Equal(t, int64(100), firstQuotas[volumeName])
	require.NoError(t, first.Stop())

	second, secondQuotas := startableBackendOverJournals(t, journals, map[string]SKUProfile{}, nil)
	_, profileErr := second.cfg.GetSKUProfile("stateful")
	require.Error(t, profileErr, "the restarted backend no longer configures the row's SKU")
	require.NoError(t, second.Start(ctx), "a restart after the profile is removed must start")
	assert.Equal(t, int64(100), secondQuotas[volumeName], "the retained quota keeps its frozen size")
	retainedMB, count, _, err := second.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(100), retainedMB)

	stored, err := second.retentionStore.Get(v013RetainedLease)
	require.NoError(t, err)
	assert.Equal(t, []shared.SKUResourceSnapshot{{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100}},
		stored.ResourceProfiles, "the first start froze the row's sizing")
	frozen, err := second.freezeRetentionSizing()
	require.NoError(t, err)
	assert.Zero(t, frozen, "a frozen row is never resolved again")
}

// A v0.13 row whose SKU is already gone has bytes on disk and no recorded
// size. Sizing it as zero would undercount the admission pool, so start fails
// and names what to restore.
func TestStartRefusesAV013RetentionWhoseSKUNoLongerResolves(t *testing.T) {
	b, quotas := startableBackendWithV013Retention(t, "removed-v0.13-sku")
	err := b.Start(context.Background())
	require.ErrorContains(t, err, "freeze retained resource sizing")
	require.ErrorContains(t, err, v013RetainedLease)
	require.ErrorContains(t, err, "removed-v0.13-sku")
	require.ErrorContains(t, err, "restore the SKU profile it was retained under")
	assert.Empty(t, quotas, "start stops before any consumer sizes the row")
	stored, getErr := b.retentionStore.Get(v013RetainedLease)
	require.NoError(t, getErr)
	assert.Empty(t, stored.ResourceProfiles)
}

func TestRetainedResourceProfilesNeverFallsBackToConfiguration(t *testing.T) {
	entry := v013RetentionEntry("stateful")
	_, err := retainedResourceProfiles(entry)
	require.ErrorIs(t, err, errRetainedSizingUnfrozen)
	_, err = retentionEntryDiskMB(entry)
	require.ErrorIs(t, err, errRetainedSizingUnfrozen, "accounting sizes a retained row only from its frozen snapshot")

	entry.ResourceProfiles = []shared.SKUResourceSnapshot{{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100}}
	profiles, err := retainedResourceProfiles(entry)
	require.NoError(t, err)
	profiles[0].DiskMB = 1
	assert.Equal(t, int64(100), entry.ResourceProfiles[0].DiskMB, "callers receive a copy")
	mb, err := retentionEntryDiskMB(entry)
	require.NoError(t, err)
	assert.Equal(t, int64(100), mb)
}
