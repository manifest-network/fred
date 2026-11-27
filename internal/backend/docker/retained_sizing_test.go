package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

const v013RetainedLease = "550e8400-e29b-41d4-a716-446655440000"

// reopenRetentionStoreWithRow writes entry straight into the retention journal,
// then points b at the reopened store. It is how a test places a row that
// records no sizing: no current writer can produce one, and only a journal
// that never started on the 0.14 line holds one. The close and restore
// settlements are rebuilt over the reopened store.
func reopenRetentionStoreWithRow(t *testing.T, b *Backend, entry shared.RetentionEntry) *shared.RetentionStore {
	t.Helper()
	path := b.cfg.RetentionDBPath
	require.NoError(t, b.retentionStore.Close())
	row, err := json.Marshal(struct {
		SchemaVersion uint8                 `json:"schema_version"`
		Entry         shared.RetentionEntry `json:"entry"`
	}{SchemaVersion: 1, Entry: entry})
	require.NoError(t, err)
	writeRawRetentionRow(t, path, entry.OriginalLeaseUUID, row)

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

// writeRawRetentionRow puts row under leaseUUID in the retention bucket of a
// closed retention journal.
func writeRawRetentionRow(t *testing.T, dbPath, leaseUUID string, row []byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("retention")).Put([]byte(leaseUUID), row)
	}))
	require.NoError(t, db.Close())
}

// frozenRetainedProfilesForTest returns the sizing a close records on a
// retained record of items under b's configuration.
func frozenRetainedProfilesForTest(t *testing.T, b *Backend, items []backend.LeaseItem) []shared.SKUResourceSnapshot {
	t.Helper()
	profiles, err := b.resolveResourceProfiles(items)
	require.NoError(t, err)
	return profiles
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
// with profiles as its SKU configuration and the seeded row's retained volume
// on disk. seed, when set, writes that row before the backend captures its
// inventory. It returns the quota each Start applies, by volume name.
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
		reopenRetentionStoreWithRow(t, b, *seed)
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

// A retained row is sized by the snapshot its close recorded. A full restart
// over the same journals, with the row's SKU profile removed, must start and
// enforce the recorded size (ENG-1517 F-1).
func TestRestartAfterSKUProfileRemovalStartsOnRecordedRetentionSizing(t *testing.T) {
	ctx := context.Background()
	volumeName := retainedName(canonicalVolumeName(v013RetainedLease, "app", 0))
	journals := t.TempDir()
	recorded := []shared.SKUResourceSnapshot{{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100}}
	seed := v013RetentionEntry("stateful")
	seed.ResourceProfiles = recorded
	first, firstQuotas := startableBackendOverJournals(t, journals, v013RetentionProfiles, &seed)
	require.NoError(t, first.Start(ctx))
	assert.Equal(t, int64(100), firstQuotas[volumeName])
	require.NoError(t, first.Stop())

	second, secondQuotas := startableBackendOverJournals(t, journals, map[string]SKUProfile{}, nil)
	_, profileErr := second.cfg.GetSKUProfile("stateful")
	require.Error(t, profileErr, "the restarted backend no longer configures the row's SKU")
	require.NoError(t, second.Start(ctx), "a restart after the profile is removed must start")
	assert.Equal(t, int64(100), secondQuotas[volumeName], "the retained quota keeps its recorded size")
	retainedMB, count, _, err := second.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(100), retainedMB)

	stored, err := second.retentionStore.Get(v013RetainedLease)
	require.NoError(t, err)
	assert.Equal(t, recorded, stored.ResourceProfiles, "no start rewrites the row's sizing")
}

// One refusal names a bounded number of leases and counts them all.
func TestRefuseUnsizedRetentionsBoundsTheNamedLeases(t *testing.T) {
	b, _ := startableBackendOverJournals(t, t.TempDir(), v013RetentionProfiles, nil)
	const rows = maxNamedUnsizedRetentions + 3
	for index := range rows {
		entry := v013RetentionEntry("stateful")
		entry.OriginalLeaseUUID = fmt.Sprintf("550e8400-e29b-41d4-a716-4466554400%02d", index)
		entry.RetainedVolumeNames = []string{retainedName(canonicalVolumeName(entry.OriginalLeaseUUID, "app", 0))}
		reopenRetentionStoreWithRow(t, b, entry)
	}
	err := b.refuseUnsizedRetentions()
	require.ErrorIs(t, err, errRetainedSizingUnfrozen)
	require.ErrorContains(t, err, fmt.Sprintf("%d retention records", rows))
	assert.Equal(t, maxNamedUnsizedRetentions, strings.Count(err.Error(), "550e8400-e29b-41d4-a716-4466554400"))
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
