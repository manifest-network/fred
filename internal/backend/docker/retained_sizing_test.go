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

func startableBackendWithV013Retention(t *testing.T, sku string) (*Backend, map[string]int64) {
	t.Helper()
	b, _ := newBackendWithRetention(t)
	mock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	mock.PingFn = func(context.Context) error { return nil }
	bindTestStorageIdentity(t, b, mock)
	attachRetentionStore(t, b)
	t.Cleanup(b.stopCancel)
	reopenRetentionStoreWithV013Row(t, b, v013RetentionEntry(sku))

	b.cfg.SKUProfiles = maps.Clone(b.cfg.SKUProfiles)
	if b.cfg.SKUProfiles == nil {
		b.cfg.SKUProfiles = map[string]SKUProfile{}
	}
	b.cfg.SKUProfiles["stateful"] = SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 100}
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

// After the first start freezes a v0.13 row's sizing, removing its SKU
// profile can no longer fail the work every later start repeats: quota
// reconciliation and the retained-disk projection (ENG-1517 F-1).
func TestStartFreezesV013RetentionSizingSoProfileRemovalCannotBlockRestart(t *testing.T) {
	b, quotas := startableBackendWithV013Retention(t, "stateful")
	volumeName := retainedName(canonicalVolumeName(v013RetainedLease, "app", 0))
	require.NoError(t, b.Start(context.Background()))
	assert.Equal(t, int64(100), quotas[volumeName])

	delete(b.cfg.SKUProfiles, "stateful")
	clear(quotas)
	require.NoError(t, b.reconcileVolumeQuotas(context.Background()),
		"a restart after the profile is removed must still reconcile the retained quota")
	assert.Equal(t, int64(100), quotas[volumeName], "the retained quota keeps its frozen size")
	require.NoError(t, b.refreshRetentionAccountingChecked(),
		"a restart after the profile is removed must still size the retained projection")
	retainedMB, count, _, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(100), retainedMB)

	stored, err := b.retentionStore.Get(v013RetainedLease)
	require.NoError(t, err)
	assert.Equal(t, []shared.SKUResourceSnapshot{{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100}},
		stored.ResourceProfiles, "the first start froze the row's sizing")
	frozen, err := b.freezeRetentionSizing()
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
