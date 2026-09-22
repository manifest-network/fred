package placement

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const (
	preflightProviderUUID = "550e8400-e29b-41d4-a716-446655440000"
	preflightLeaseA       = "018f47a2-8b1c-7def-8123-456789abcdef"
	preflightLeaseB       = "018f47a2-8b1c-7def-8123-456789abcdee"
)

func boundExactBackupTarget(t *testing.T, path string) *ExactBackupTarget {
	t.Helper()
	target, err := BindExactBackupTarget(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.Close()) })
	return target
}

func corruptExactBackupInPlace(t *testing.T, target *ExactBackupTarget) {
	t.Helper()
	require.NotNil(t, target)
	file, err := os.OpenFile(target.Path(), os.O_RDWR, 0) // #nosec G304 -- test-owned path
	require.NoError(t, err)
	var first [1]byte
	_, err = file.ReadAt(first[:], 0)
	require.NoError(t, err)
	first[0] ^= 0xff
	_, err = file.WriteAt(first[:], 0)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
}

type preflightChainSnapshot struct {
	providerUUID string
	leaseUUIDs   []string
	leaseItems   map[string][]backend.LeaseItem
}

func (snapshot preflightChainSnapshot) Valid() bool { return snapshot.providerUUID != "" }
func (snapshot preflightChainSnapshot) ProviderUUID() string {
	return snapshot.providerUUID
}
func (snapshot preflightChainSnapshot) BlockHeight() int64      { return 117 }
func (snapshot preflightChainSnapshot) TotalLeases() int        { return len(snapshot.leaseUUIDs) }
func (snapshot preflightChainSnapshot) BlockingLeaseCount() int { return 0 }
func (snapshot preflightChainSnapshot) LeaseUUIDs() []string {
	leaseUUIDs := make([]string, len(snapshot.leaseUUIDs))
	copy(leaseUUIDs, snapshot.leaseUUIDs)
	return leaseUUIDs
}
func (snapshot preflightChainSnapshot) LeaseItems() map[string][]backend.LeaseItem {
	items := make(map[string][]backend.LeaseItem, len(snapshot.leaseItems))
	for leaseUUID, leaseItems := range snapshot.leaseItems {
		items[leaseUUID] = slices.Clone(leaseItems)
	}
	return items
}

type malformedPreflightChainSnapshot struct {
	valid        bool
	providerUUID string
	height       int64
	total        int
	leaseUUIDs   []string
	leaseItems   map[string][]backend.LeaseItem
}

func (snapshot malformedPreflightChainSnapshot) Valid() bool { return snapshot.valid }
func (snapshot malformedPreflightChainSnapshot) ProviderUUID() string {
	return snapshot.providerUUID
}
func (snapshot malformedPreflightChainSnapshot) BlockHeight() int64 { return snapshot.height }
func (snapshot malformedPreflightChainSnapshot) TotalLeases() int   { return snapshot.total }
func (snapshot malformedPreflightChainSnapshot) BlockingLeaseCount() int {
	return 0
}
func (snapshot malformedPreflightChainSnapshot) LeaseUUIDs() []string {
	return snapshot.leaseUUIDs
}
func (snapshot malformedPreflightChainSnapshot) LeaseItems() map[string][]backend.LeaseItem {
	return snapshot.leaseItems
}

func preflightChainProof(t *testing.T, leaseUUIDs ...string) LegacyUpgradeChainProof {
	t.Helper()
	return chainProofForProvider(t, preflightProviderUUID, leaseUUIDs...)
}

func legacyPreparationTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return ctx
}

func chainProofForProvider(
	t *testing.T,
	providerUUID string,
	leaseUUIDs ...string,
) LegacyUpgradeChainProof {
	t.Helper()
	observed := make([]string, len(leaseUUIDs))
	copy(observed, leaseUUIDs)
	items := make(map[string][]backend.LeaseItem, len(observed))
	for _, leaseUUID := range observed {
		items[leaseUUID] = []backend.LeaseItem{{
			SKU: "sku-test", Quantity: 1, ServiceName: legacyDefaultServiceName,
		}}
	}
	proof, err := NewLegacyUpgradeChainProof(preflightChainSnapshot{
		providerUUID: providerUUID,
		leaseUUIDs:   observed,
		leaseItems:   items,
	})
	require.NoError(t, err)
	return proof
}

func TestNewLegacyUpgradeChainProofRejectsManufacturedOrIncompleteEvidence(t *testing.T) {
	t.Parallel()

	validEmpty := malformedPreflightChainSnapshot{
		valid: true, providerUUID: preflightProviderUUID, height: 117,
		total: 0, leaseUUIDs: []string{}, leaseItems: map[string][]backend.LeaseItem{},
	}
	var typedNil *malformedPreflightChainSnapshot
	tests := map[string]ProviderLeaseMembershipSnapshot{
		"nil snapshot":             nil,
		"typed nil snapshot":       typedNil,
		"invalid snapshot":         &malformedPreflightChainSnapshot{},
		"noncanonical provider":    &malformedPreflightChainSnapshot{valid: true, providerUUID: "550E8400-E29B-41D4-A716-446655440000", height: 117, leaseUUIDs: []string{}},
		"zero height":              &malformedPreflightChainSnapshot{valid: true, providerUUID: preflightProviderUUID, leaseUUIDs: []string{}},
		"nil membership":           &malformedPreflightChainSnapshot{valid: true, providerUUID: preflightProviderUUID, height: 117},
		"membership count differs": &malformedPreflightChainSnapshot{valid: true, providerUUID: preflightProviderUUID, height: 117, total: 2, leaseUUIDs: []string{preflightLeaseA}},
		"noncanonical lease":       &malformedPreflightChainSnapshot{valid: true, providerUUID: preflightProviderUUID, height: 117, total: 1, leaseUUIDs: []string{"018F47A2-8B1C-7DEF-8123-456789ABCDEF"}},
		"duplicate lease":          &malformedPreflightChainSnapshot{valid: true, providerUUID: preflightProviderUUID, height: 117, total: 2, leaseUUIDs: []string{preflightLeaseA, preflightLeaseA}},
	}
	for name, snapshot := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			proof, err := NewLegacyUpgradeChainProof(snapshot)
			require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
			require.False(t, proof.valid())
		})
	}
	proof, err := NewLegacyUpgradeChainProof(validEmpty)
	require.NoError(t, err)
	require.True(t, proof.valid())
}

func TestLegacyUpgradeChainProofCarriesHistoricalItemsWithoutReinterpretingThem(t *testing.T) {
	t.Parallel()

	historical := []backend.LeaseItem{
		{SKU: "sku-a", Quantity: 900},
		{SKU: "sku-b", Quantity: 900},
	}
	snapshot := preflightChainSnapshot{
		providerUUID: preflightProviderUUID,
		leaseUUIDs:   []string{preflightLeaseA},
		leaseItems: map[string][]backend.LeaseItem{
			preflightLeaseA: historical,
		},
	}
	proof, err := NewLegacyUpgradeChainProof(snapshot)
	require.NoError(t, err,
		"terminal chain history may predate service names and current quantity bounds")
	require.True(t, proof.valid())

	// The proof owns a detached snapshot: neither the caller's slice nor map can
	// rewrite the capability evidence after construction.
	historical[0].SKU = "mutated"
	snapshot.leaseItems[preflightLeaseA][1].Quantity = 1
	delete(snapshot.leaseItems, preflightLeaseA)
	assert.Equal(t, "sku-a", proof.leaseItems[preflightLeaseA][0].SKU)
	assert.Equal(t, 900, proof.leaseItems[preflightLeaseA][1].Quantity)

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{})
	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	_, err = inspector.Check(
		preflightProviderUUID,
		[]string{"backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {},
		}),
		proof,
	)
	require.NoError(t, err,
		"an unobserved historical lease contributes membership only")
	require.NoError(t, inspector.Close())
}

func TestLegacyUpgradePreflightRejectsHistoricalOnlyItemShapeWhenItIsLive(t *testing.T) {
	t.Parallel()

	legacyItems := []backend.LeaseItem{
		{SKU: "sku-a", Quantity: 900},
		{SKU: "sku-b", Quantity: 900},
	}
	proof, err := NewLegacyUpgradeChainProof(preflightChainSnapshot{
		providerUUID: preflightProviderUUID,
		leaseUUIDs:   []string{preflightLeaseA},
		leaseItems: map[string][]backend.LeaseItem{
			preflightLeaseA: legacyItems,
		},
	})
	require.NoError(t, err)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte("backend-a"),
	})
	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	_, err = inspector.Check(
		preflightProviderUUID,
		[]string{"backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {
				Provisions: []string{preflightLeaseA},
				ProvisionItems: map[string][]backend.LeaseItem{
					preflightLeaseA: legacyItems,
				},
			},
		}),
		proof,
	)
	require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
	require.ErrorContains(t, err, "invalid workload observation")
	require.NoError(t, inspector.Close())
}

func TestComparePreflightWorkloadItemsPinsExactLiveCohort(t *testing.T) {
	t.Parallel()

	chainItems := []backend.LeaseItem{
		{SKU: "sku-web", Quantity: 2, ServiceName: "web", CustomDomain: "web.example"},
		{SKU: "sku-db", Quantity: 1, ServiceName: "db"},
	}
	reordered := []backend.LeaseItem{chainItems[1], chainItems[0]}
	require.NoError(t, comparePreflightWorkloadItems(chainItems, reordered),
		"wire order is not workload authority")
	withoutDeferredDomain := slices.Clone(reordered)
	withoutDeferredDomain[1].CustomDomain = ""
	require.NoError(t, comparePreflightWorkloadItems(chainItems, withoutDeferredDomain),
		"v0.13 could defer a requested custom domain until DNS was ready")

	for _, test := range []struct {
		name   string
		mutate func([]backend.LeaseItem)
		want   string
	}{
		{
			name: "quantity",
			mutate: func(items []backend.LeaseItem) {
				items[0].Quantity++
			},
			want: "service topology differs",
		},
		{
			name: "SKU",
			mutate: func(items []backend.LeaseItem) {
				items[0].SKU = "sku-other"
			},
			want: "service topology differs",
		},
		{
			name: "service",
			mutate: func(items []backend.LeaseItem) {
				items[0].ServiceName = "worker"
			},
			want: "service topology differs",
		},
		{
			name: "custom domain",
			mutate: func(items []backend.LeaseItem) {
				items[0].CustomDomain = "other.example"
			},
			want: "custom domain",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := slices.Clone(chainItems)
			test.mutate(observed)
			err := comparePreflightWorkloadItems(chainItems, observed)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestLegacyPlacementToolsRejectRelativeAuthorityPaths(t *testing.T) {
	t.Parallel()

	inspector, err := OpenLegacyUpgradeInspector("placements.db")
	require.ErrorContains(t, err, "absolute and clean")
	require.Nil(t, inspector)

	preparer, err := OpenLegacyUpgradePreparer("placements.db")
	require.ErrorContains(t, err, "absolute and clean")
	require.Nil(t, preparer)

	prepared, err := OpenPreparedAuthorityInspector("placements.db", preflightProviderUUID)
	require.ErrorContains(t, err, "absolute and clean")
	require.Nil(t, prepared)
}

func TestLegacyUpgradeInspectorRejectsChainProofForAnotherProviderWithoutMutation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	_, err = inspector.Check(
		preflightProviderUUID,
		[]string{"backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {Provisions: []string{preflightLeaseA}},
		}),
		chainProofForProvider(
			t, "1e1698c3-a922-460a-8296-70efdbc03032", preflightLeaseA,
		),
	)
	require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
	require.ErrorContains(t, err, `chain proof belongs to provider "1e1698c3-a922-460a-8296-70efdbc03032"`)
	require.NoError(t, inspector.Close())
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestLegacyUpgradeInspector_CleanDatabaseMatchesInventoryWithoutMutation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
		preflightLeaseB: []byte("backend-b"),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	summary, err := inspector.Check(
		preflightProviderUUID,
		[]string{"backend-b", "backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {Provisions: []string{preflightLeaseA}},
			"backend-b": {Retentions: []string{preflightLeaseB}},
		}),
		preflightChainProof(t, preflightLeaseA, preflightLeaseB),
	)
	require.NoError(t, err)
	assert.Equal(t, LegacyUpgradePreflightSummary{
		ConfiguredBackends: 2,
		PlacementRows:      2,
		InventoryLeases:    2,
	}, summary)

	// Holding the inspector prevents a writer from crossing the first-open
	// migration boundary between inventory collection and the final verdict.
	writer, writerErr := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 50 * time.Millisecond})
	if writer != nil {
		require.NoError(t, writer.Close())
	}
	require.ErrorIs(t, writerErr, bolt.ErrTimeout)
	require.NoError(t, inspector.Close())
	require.NoError(t, inspector.Close(), "close is idempotent")

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after, "read-only preflight must not change database bytes")

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(lifecycleCapabilityBucketName))
		assert.Nil(t, tx.Bucket(metadataBucketName))
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestLegacyUpgradeInspector_RejectsWrongOrMixedProvisionProvidersWithoutMutation(
	t *testing.T,
) {
	t.Parallel()

	const otherProvider = "1e1698c3-a922-460a-8296-70efdbc03032"
	tests := []struct {
		name      string
		rows      map[string][]byte
		providers map[string]string
		want      string
	}{
		{
			name: "wrong provider",
			rows: map[string][]byte{
				preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
			},
			providers: map[string]string{preflightLeaseA: otherProvider},
			want:      `provision "` + preflightLeaseA + `" reports provider "` + otherProvider + `"`,
		},
		{
			name: "mixed providers",
			rows: map[string][]byte{
				preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
				preflightLeaseB: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:01:00Z"}`),
			},
			providers: map[string]string{
				preflightLeaseA: preflightProviderUUID,
				preflightLeaseB: otherProvider,
			},
			want: `provision "` + preflightLeaseB + `" reports provider "` + otherProvider + `"`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, test.rows)
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)

			leaseUUIDs := slices.Sorted(maps.Keys(test.rows))
			inspector, err := OpenLegacyUpgradeInspector(dbPath)
			require.NoError(t, err)
			_, err = inspector.Check(
				preflightProviderUUID,
				[]string{"backend-a"},
				withPreflightTestStorageIDs(map[string]BackendInventory{
					"backend-a": {
						Provisions:             leaseUUIDs,
						ProvisionProviderUUIDs: maps.Clone(test.providers),
					},
				}),
				preflightChainProof(t, leaseUUIDs...),
			)
			require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
			require.ErrorContains(t, err, test.want)
			require.NoError(t, inspector.Close())

			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after,
				"provider-binding failure must leave the stopped database byte-exact")
		})
	}
}

func TestLegacyUpgradeInspector_RequiresChainMembershipForRetentionOnlyLease(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	_, err = inspector.Check(
		preflightProviderUUID,
		[]string{"backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {Retentions: []string{preflightLeaseA}},
		}),
		preflightChainProof(t),
	)
	require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
	require.ErrorContains(t, err,
		`lease "`+preflightLeaseA+`" is absent from the height-117 all-state chain snapshot`)
	require.NoError(t, inspector.Close())

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"missing retention-only provider membership must leave the stopped database byte-exact")
}

func TestLegacyUpgradeInspector_RejectsPreparedMetadataWithoutMutation(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	replaceTopologyMetadataForTest(t, dbPath, []byte(`{"schema":2,"schema":1}`))
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	_, err = inspector.Check(
		preflightProviderUUID,
		[]string{"backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {
				StorageIdentity: testBackendStorageID("backend-a"),
				Provisions:      []string{},
				Retentions:      []string{},
			},
		}),
		preflightChainProof(t),
	)
	require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
	require.ErrorContains(t, err, `bucket "placement_metadata" already exists`)
	require.NoError(t, inspector.Close())

	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"read-only legacy preflight must never normalize a prepared metadata record")
}

func TestLegacyUpgradePreparer_PublishesExactBackupAndSealsIdentityBoundSchema(
	t *testing.T,
) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	ctx := legacyPreparationTestContext(t)
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	summary, err := preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.NoError(t, err)
	assert.Equal(t, LegacyUpgradePreflightSummary{
		ConfiguredBackends: 1,
		PlacementRows:      1,
		InventoryLeases:    1,
	}, summary)
	require.NoError(t, preparer.Close())

	backupInfo, err := os.Stat(backupPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), backupInfo.Mode().Perm())
	assert.Positive(t, backupInfo.Size())
	backupBytes, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, backupBytes,
		"rollback backup must preserve the exact stopped v0.13 database bytes")

	backupInspector, err := OpenLegacyUpgradeInspector(backupPath)
	require.NoError(t, err)
	backupSummary, err := backupInspector.Check(
		preflightProviderUUID, []string{"backend-a"}, inventories,
		preflightChainProof(t, preflightLeaseA),
	)
	require.NoError(t, err)
	assert.Equal(t, summary, backupSummary,
		"the published rollback artifact must preserve the validated v0.13 snapshot")
	require.NoError(t, backupInspector.Close())

	preparedBeforeRestart, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	store, err := OpenStore(dbPath, preflightProviderUUID)
	require.NoError(t, err)
	require.NoError(t, store.VerifyProviderUUID(preflightProviderUUID))
	require.ErrorIs(t,
		store.VerifyProviderUUID("1e1698c3-a922-460a-8296-70efdbc03032"),
		ErrProviderAuthorityMismatch,
	)
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a"}))
	observed, ok := store.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, ok)
	assert.Equal(t, inventories["backend-a"].StorageIdentity, observed)
	assert.False(t, store.CurrentAdmissionBaseline().Valid(),
		"offline preparation must not manufacture a complete runtime baseline")
	require.NoError(t, store.Close())
	preparedAfterRestart, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, preparedBeforeRestart, preparedAfterRestart,
		"opening the prepared provider-bound authority on restart must not rewrite bytes")
}

func TestLegacyUpgradePreparer_RejectsInPlaceBackupMutationBeforeCommit(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = preparer.Close() })
	target := boundExactBackupTarget(t, backupPath)
	ctx := legacyPreparationTestContext(t)
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, target, LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	preparer.createExactBackup = func(
		db *bolt.DB,
		sourceInfo os.FileInfo,
		backupTarget *ExactBackupTarget,
	) error {
		if backupErr := writeExactBackup(db, sourceInfo, backupTarget); backupErr != nil {
			return backupErr
		}
		corruptExactBackupInPlace(t, backupTarget)
		return nil
	}

	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, ErrExactBackupPublished)
	require.ErrorContains(t, err, "backup bytes changed")
	require.Error(t, target.VerifyPublished())

	sourceAfter, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, sourceBefore, sourceAfter,
		"an in-place-corrupted rollback image must fail before legacy preparation commits")
}

func TestLegacyUpgradePreparer_MigratesHistoricalRawJSONScalarBackendNames(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	legacyRows := map[string][]byte{
		"00000000-0000-4000-8000-000000000001": []byte("null"),
		"00000000-0000-4000-8000-000000000002": []byte("true"),
		"00000000-0000-4000-8000-000000000003": []byte("123"),
		"00000000-0000-4000-8000-000000000004": []byte("[]"),
	}
	writeRawRecords(t, dbPath, legacyRows)
	legacyBytes, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inventories := make(map[string]BackendInventory, len(legacyRows))
	leaseUUIDs := make([]string, 0, len(legacyRows))
	for leaseUUID, rawBackend := range legacyRows {
		backendName := string(rawBackend)
		leaseUUIDs = append(leaseUUIDs, leaseUUID)
		inventories[backendName] = BackendInventory{Provisions: []string{leaseUUID}}
	}
	inventories = withPreflightTestStorageIDs(inventories)
	backendNames := slices.Sorted(maps.Keys(inventories))
	slices.Sort(leaseUUIDs)
	chainProof := preflightChainProof(t, leaseUUIDs...)
	ctx := legacyPreparationTestContext(t)

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	capability, err := preparer.AuthorizePreparation(
		ctx,
		preflightProviderUUID,
		backendNames,
		inventories,
		chainProof,
		boundExactBackupTarget(t, backupPath),
		LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx,
		preflightProviderUUID,
		backendNames,
		inventories,
		chainProof,
		capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())

	backupBytes, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, legacyBytes, backupBytes,
		"the rollback image must retain the byte-exact historical scalar values")

	store, err := OpenStore(dbPath, preflightProviderUUID)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	require.NoError(t, store.VerifyBackendTopology(backendNames))
	for leaseUUID, rawBackend := range legacyRows {
		placed := store.Lookup(leaseUUID)
		assert.Equal(t, StateConfirmed, placed.State(), leaseUUID)
		assert.Equal(t, string(rawBackend), placed.Backend, leaseUUID)
		assert.Positive(t, placed.Revision(), leaseUUID)
	}
}

func TestVerifyLegacyPreparationPostconditionRejectsExtraLifecycleAuthority(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})
	chainProof := preflightChainProof(t, preflightLeaseA)
	ctx := legacyPreparationTestContext(t)
	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	summary, err := preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.NoError(t, err)
	require.NoError(t, preparer.Close())

	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	extra, err := encodeLifecycleCapability(lifecycleCapability{backend: "backend-a"})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte(preflightLeaseB), extra)
	}))
	require.NoError(t, db.Close())

	db, err = bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	storageIDs := map[string]string{
		"backend-a": inventories["backend-a"].StorageIdentity.String(),
	}
	err = verifyLegacyPreparationPostcondition(
		db, preflightProviderUUID, []string{"backend-a"}, storageIDs, inventories, summary,
	)
	require.ErrorContains(t, err, "absent from the verified inventory")
	require.NoError(t, db.Close())
}

func TestLegacyUpgradePreparer_RequiresExactTargetBoundUnexpiredCausalCapability(
	t *testing.T,
) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})
	chainProof := preflightChainProof(t, preflightLeaseA)
	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = preparer.Close() })
	ctx := legacyPreparationTestContext(t)

	_, err = preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), "I only looked at inventory",
	)
	require.ErrorIs(t, err, ErrLegacyPreparationCapability)
	require.ErrorContains(t, err, "must exactly equal")
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = preparer.AuthorizePreparation(
		canceledCtx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.ErrorIs(t, err, ErrLegacyPreparationCapability)
	require.ErrorIs(t, err, context.Canceled)

	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)

	otherStorageID, err := backendidentity.New()
	require.NoError(t, err)
	changedStorage := maps.Clone(inventories)
	changedInventory := changedStorage["backend-a"]
	changedInventory.StorageIdentity = otherStorageID
	changedStorage["backend-a"] = changedInventory
	secondBackendID, err := backendidentity.New()
	require.NoError(t, err)
	changedTopology := maps.Clone(inventories)
	changedTopology["backend-b"] = BackendInventory{
		StorageIdentity:        secondBackendID,
		Provisions:             []string{},
		ProvisionProviderUUIDs: map[string]string{},
		Retentions:             []string{},
	}
	expired := capability
	expired.expiresAt = time.Now().Add(-time.Second)
	differentBackupCapability := capability
	differentBackupCapability.backupTarget = boundExactBackupTarget(
		t, filepath.Join(tempDir, "different-backup.db"),
	)
	transplantedPreparer := &LegacyUpgradePreparer{
		db: preparer.db, sourceInfo: preparer.sourceInfo, now: preparer.now,
	}

	tests := []struct {
		name         string
		target       *LegacyUpgradePreparer
		providerUUID string
		backends     []string
		inventories  map[string]BackendInventory
		chainProof   LegacyUpgradeChainProof
		capability   LegacyPreparationCapability
		wantDetail   string
	}{
		{
			name: "absent", target: preparer, providerUUID: preflightProviderUUID,
			backends: []string{"backend-a"}, inventories: inventories,
			chainProof: chainProof, wantDetail: "absent",
		},
		{
			name: "different provider", target: preparer,
			providerUUID: "1e1698c3-a922-460a-8296-70efdbc03032",
			backends:     []string{"backend-a"}, inventories: inventories,
			chainProof: chainProof, capability: capability,
		},
		{
			name: "different topology", target: preparer, providerUUID: preflightProviderUUID,
			backends: []string{"backend-a", "backend-b"}, inventories: changedTopology,
			chainProof: chainProof, capability: capability,
		},
		{
			name: "different storage identity", target: preparer, providerUUID: preflightProviderUUID,
			backends: []string{"backend-a"}, inventories: changedStorage,
			chainProof: chainProof, capability: capability,
		},
		{
			name: "different chain snapshot", target: preparer, providerUUID: preflightProviderUUID,
			backends: []string{"backend-a"}, inventories: inventories,
			chainProof: preflightChainProof(t, preflightLeaseB), capability: capability,
		},
		{
			name: "different backup destination", target: preparer,
			providerUUID: preflightProviderUUID, backends: []string{"backend-a"},
			inventories: inventories, chainProof: chainProof, capability: differentBackupCapability,
			wantDetail: "target changed",
		},
		{
			name: "different exclusive session", target: transplantedPreparer,
			providerUUID: preflightProviderUUID, backends: []string{"backend-a"},
			inventories: inventories, chainProof: chainProof, capability: capability,
			wantDetail: "another preparer",
		},
		{
			name: "expired", target: preparer, providerUUID: preflightProviderUUID,
			backends: []string{"backend-a"}, inventories: inventories,
			chainProof: chainProof, capability: expired,
			wantDetail: "capability expired",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, prepareErr := test.target.PrepareContext(
				ctx, test.providerUUID, test.backends, test.inventories,
				test.chainProof, test.capability,
			)
			require.ErrorIs(t, prepareErr, ErrLegacyPreparationCapability)
			if test.wantDetail != "" {
				require.ErrorContains(t, prepareErr, test.wantDetail)
			}
		})
	}

	_, backupErr := os.Lstat(backupPath)
	require.ErrorIs(t, backupErr, os.ErrNotExist)
	sourceAfter, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, sourceBefore, sourceAfter,
		"invalid causal authority must fail before backup publication or mutation")
}

func TestLegacyUpgradePreparer_CapabilityIsBoundToExactProofContext(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})
	chainProof := preflightChainProof(t, preflightLeaseA)
	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = preparer.Close() })
	proofCtx, cancelProof := context.WithCancel(t.Context())
	t.Cleanup(cancelProof)
	otherCtx, cancelOther := context.WithCancel(t.Context())
	t.Cleanup(cancelOther)

	capability, err := preparer.AuthorizePreparation(
		proofCtx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		otherCtx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, ErrLegacyPreparationCapability)
	require.ErrorContains(t, err, "exact proof cancellation scope")
	_, statErr := os.Lstat(backupPath)
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"context transplantation must fail before backup publication")

	_, err = preparer.PrepareContext(
		proofCtx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.NoError(t, err,
		"a rejection before publication admission must not consume the capability")
}

func TestLegacyUpgradePreparer_CopiedCapabilityCannotReplayPublicationAdmission(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})
	chainProof := preflightChainProof(t, preflightLeaseA)
	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = preparer.Close() })
	ctx := legacyPreparationTestContext(t)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	copied := capability
	backupErr := errors.New("synthetic failure after publication admission")
	preparer.createExactBackup = func(*bolt.DB, os.FileInfo, *ExactBackupTarget) error {
		return backupErr
	}

	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, backupErr)
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, copied,
	)
	require.ErrorIs(t, err, ErrLegacyPreparationCapability)
	require.ErrorContains(t, err, "not been durably published")
	_, statErr := os.Lstat(backupPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestLegacyUpgradePreparer_SyncFailureReportsCommittedPreparation(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	syncErr := errors.New("synthetic placement sync failure")
	preparer.syncDB = func() error { return syncErr }
	ctx := legacyPreparationTestContext(t)
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, ErrLegacyPreparationCommitted)
	require.ErrorIs(t, err, syncErr)
	require.ErrorContains(t, err, "sync prepared placement db")
	require.NoError(t, preparer.Close())

	store, openErr := OpenStore(dbPath, preflightProviderUUID)
	require.NoError(t, openErr,
		"the migration transaction committed before the injected sync verification failed")
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a"}))
	require.NoError(t, store.Close())
	_, backupErr := os.Stat(backupPath)
	require.NoError(t, backupErr)
}

func TestLegacyUpgradePreparer_CommitErrorReportsOutcomeUnknown(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	commitErr := errors.New("synthetic bbolt commit failure")
	preparer.updateDB = func(func(*bolt.Tx) error) error {
		return fmt.Errorf("%w: %w", errBoltCommitOutcomeUnknown, commitErr)
	}
	ctx := legacyPreparationTestContext(t)
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, ErrLegacyPreparationOutcomeUnknown)
	require.ErrorIs(t, err, commitErr)
	assert.NotErrorIs(t, err, ErrLegacyPreparationCommitted)
	require.NoError(t, preparer.Close())

	sourceAfter, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, sourceAfter,
		"the injected boundary does not establish whether a real Commit wrote bytes")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, backup)
}

func TestLegacyUpgradePreparer_ExpiredProofAfterBackupNeverCommits(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.expired-proof.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	realBackup := preparer.createExactBackup
	preparer.createExactBackup = func(
		db *bolt.DB,
		sourceInfo os.FileInfo,
		target *ExactBackupTarget,
	) error {
		backupErr := realBackup(db, sourceInfo, target)
		cancel()
		return backupErr
	}
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "proof expired while exact backup")
	require.ErrorContains(t, err, "no placement preparation transaction committed")
	require.ErrorContains(t, err, "rerun the read-only preflight")
	require.ErrorContains(t, err, "choose a new -backup path")
	require.NoError(t, preparer.Close())

	sourceAfter, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, sourceAfter,
		"proof expiry after backup must not modify the legacy placement authority")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, backup,
		"the published artifact must remain an exact rollback image")

	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	_, err = inspector.Check(
		preflightProviderUUID, []string{"backend-a"}, inventories,
		preflightChainProof(t, preflightLeaseA),
	)
	require.NoError(t, err,
		"the source must remain at the v0.13 read-only preflight boundary")
	require.NoError(t, inspector.Close())
}

func TestLegacyUpgradePreparer_ExpiredCausalCapabilityAfterBackupNeverCommits(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.expired-capability.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	currentTime := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	preparer.now = func() time.Time { return currentTime }
	ctx := legacyPreparationTestContext(t)
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	realBackup := preparer.createExactBackup
	preparer.createExactBackup = func(
		db *bolt.DB,
		sourceInfo os.FileInfo,
		target *ExactBackupTarget,
	) error {
		backupErr := realBackup(db, sourceInfo, target)
		currentTime = currentTime.Add(legacyPreparationCapabilityMaxAge)
		return backupErr
	}
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, ErrExactBackupPublished)
	require.ErrorIs(t, err, ErrLegacyPreparationCapability)
	require.ErrorContains(t, err, "capability expired or changed")
	require.ErrorContains(t, err, "no placement preparation transaction committed")
	require.NoError(t, preparer.Close())

	sourceAfter, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, sourceAfter,
		"capability expiry after backup must leave legacy authority byte-exact")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, backup)
}

func TestLegacyUpgradePreparer_CanceledCapabilityInsideMutationPreservesContextCause(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.canceled-capability.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, boundExactBackupTarget(t, backupPath), LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	realUpdate := preparer.updateDB
	preparer.updateDB = func(mutate func(*bolt.Tx) error) error {
		return realUpdate(func(tx *bolt.Tx) error {
			cancel()
			return mutate(tx)
		})
	}
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorIs(t, err, ErrExactBackupPublished)
	require.ErrorIs(t, err, ErrLegacyPreparationCapability)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "proof expired while exact backup")
	require.NoError(t, preparer.Close())

	sourceAfter, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, sourceAfter,
		"context cancellation at transaction admission must roll back all assembled writes")
	backup, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, backup)
}

func TestLegacyUpgradePreparer_ExistingBackupIsNeverOverwritten(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v013.bak")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	sourceBefore, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	target := boundExactBackupTarget(t, backupPath)
	const sentinel = "existing operator backup"
	require.NoError(t, os.WriteFile(backupPath, []byte(sentinel), 0o600))

	preparer, err := OpenLegacyUpgradePreparer(dbPath)
	require.NoError(t, err)
	ctx := legacyPreparationTestContext(t)
	inventories := withPreflightTestStorageIDs(map[string]BackendInventory{
		"backend-a": {Provisions: []string{preflightLeaseA}},
	})
	chainProof := preflightChainProof(t, preflightLeaseA)
	capability, err := preparer.AuthorizePreparation(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, target, LegacyPreparationDrainAttestation,
	)
	require.NoError(t, err)
	_, err = preparer.PrepareContext(
		ctx, preflightProviderUUID, []string{"backend-a"}, inventories,
		chainProof, capability,
	)
	require.ErrorContains(t, err, "backup destination already exists")
	require.NoError(t, preparer.Close())

	sourceAfter, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, sourceBefore, sourceAfter,
		"a refused backup destination must leave the legacy database untouched")
	backupAfter, err := os.ReadFile(backupPath)
	require.NoError(t, err)
	assert.Equal(t, sentinel, string(backupAfter))
}

func TestLegacyUpgradeInspector_RejectsInconsistentDatabaseOrInventory(t *testing.T) {
	tests := []struct {
		name        string
		mutateDB    func(*bolt.Tx) error
		backends    []string
		inventories map[string]BackendInventory
		want        string
	}{
		{
			name: "lifecycle boundary already exists",
			mutateDB: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket(lifecycleCapabilityBucketName)
				return err
			},
			want: `bucket "placement_lifecycle_capabilities" already exists`,
		},
		{
			name: "metadata boundary already exists",
			mutateDB: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket(metadataBucketName)
				return err
			},
			want: `bucket "placement_metadata" already exists`,
		},
		{
			name: "noncanonical placement lease key",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put(
					[]byte("018F47A2-8B1C-7DEF-8123-456789ABCDEE"),
					[]byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
				)
			},
			want: `placement row key "018F47A2-8B1C-7DEF-8123-456789ABCDEE" is not a canonical non-nil UUID`,
		},
		{
			name: "noncanonical inventory lease identity",
			inventories: map[string]BackendInventory{
				"backend-a": {Provisions: []string{"018F47A2-8B1C-7DEF-8123-456789ABCDEF"}},
			},
			want: `backend "backend-a" provisions inventory contains non-canonical lease identity "018F47A2-8B1C-7DEF-8123-456789ABCDEF"`,
		},
		{
			name: "nil inventory lease identity",
			inventories: map[string]BackendInventory{
				"backend-a": {Provisions: []string{"00000000-0000-0000-0000-000000000000"}},
			},
			want: `backend "backend-a" provisions inventory contains non-canonical lease identity "00000000-0000-0000-0000-000000000000"`,
		},
		{
			name: "nonzero revision globally disqualifies",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte(preflightLeaseA),
					[]byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z","revision":1}`))
			},
			want: "already revisioned (revision=1)",
		},
		{
			name: "undecodable revision header globally disqualifies",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte(preflightLeaseA),
					[]byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z","revision":"zero"}`))
			},
			want: "undecodable global revision header",
		},
		{
			name: "leading whitespace preserves the v013 raw owner boundary",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte(preflightLeaseA),
					[]byte(` {"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`))
			},
			want: `but placement names " {\"backend\":\"backend-a\"`,
		},
		{
			name: "duplicate JSON ownership field is ambiguous",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte(preflightLeaseA),
					[]byte(`{"backend":"backend-b","backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`))
			},
			want: `duplicate field "backend"`,
		},
		{
			name: "null set at was not emitted by the v013 timestamp encoder",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte(preflightLeaseA),
					[]byte(`{"backend":"backend-a","set_at":null}`))
			},
			want: `set_at is not a timestamp`,
		},
		{
			name: "attempt row is not v013",
			mutateDB: func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte(preflightLeaseA),
					[]byte(`{"attempt":"backend-a","set_at":"2026-08-25T15:00:00Z"}`))
			},
			want: `field "attempt" did not exist in the v0.13 placement schema`,
		},
		{
			name:     "inventory owner does not match row",
			backends: []string{"backend-a", "backend-b"},
			inventories: map[string]BackendInventory{
				"backend-a": {},
				"backend-b": {Provisions: []string{preflightLeaseA}},
			},
			want: `inventory lease "` + preflightLeaseA + `" is on backend "backend-b" but placement names "backend-a"`,
		},
		{
			name:     "inventory lease has two owners",
			backends: []string{"backend-a", "backend-b"},
			inventories: map[string]BackendInventory{
				"backend-a": {Provisions: []string{preflightLeaseA}},
				"backend-b": {Retentions: []string{preflightLeaseA}},
			},
			want: `inventory lease "` + preflightLeaseA + `" has ambiguous owners [backend-a backend-b]`,
		},
		{
			name:        "configured backend inventory is missing",
			inventories: map[string]BackendInventory{},
			want:        `configured backend "backend-a" has no complete inventory`,
		},
		{
			name:        "placement is absent from inventory",
			inventories: map[string]BackendInventory{"backend-a": {}},
			want:        `placement "` + preflightLeaseA + `" on backend "backend-a" is absent from complete backend inventory`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, map[string][]byte{
				preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
			})
			if test.mutateDB != nil {
				db, err := bolt.Open(dbPath, 0o600, nil)
				require.NoError(t, err)
				require.NoError(t, db.Update(test.mutateDB))
				require.NoError(t, db.Close())
			}
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)

			backends := test.backends
			if backends == nil {
				backends = []string{"backend-a"}
			}
			inventories := test.inventories
			if inventories == nil {
				inventories = map[string]BackendInventory{
					"backend-a": {Provisions: []string{preflightLeaseA}},
				}
			}
			inventories = withPreflightTestStorageIDs(inventories)
			inspector, err := OpenLegacyUpgradeInspector(dbPath)
			require.NoError(t, err)
			_, err = inspector.Check(
				preflightProviderUUID,
				backends,
				inventories,
				preflightChainProof(t, preflightLeaseA, preflightLeaseB),
			)
			require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
			assert.ErrorContains(t, err, test.want)
			require.NoError(t, inspector.Close())

			after, err := os.ReadFile(dbPath)
			require.NoError(t, err)
			assert.Equal(t, before, after, "failed preflight must remain read-only")
		})
	}
}

func TestLegacyUpgradeInspector_CheckContextCanceledDoesNotReturnVerdict(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		preflightLeaseA: []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	inspector, err := OpenLegacyUpgradeInspector(dbPath)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = inspector.CheckContext(ctx, preflightProviderUUID,
		[]string{"backend-a"},
		withPreflightTestStorageIDs(map[string]BackendInventory{
			"backend-a": {Provisions: []string{preflightLeaseA}},
		}),
		preflightChainProof(t, preflightLeaseA),
	)
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, errors.Is(err, ErrLegacyUpgradePreflight),
		"interruption is an incomplete proof, not a discrepancy verdict")
	require.NoError(t, inspector.Close())

	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func withPreflightTestStorageIDs(input map[string]BackendInventory) map[string]BackendInventory {
	result := make(map[string]BackendInventory, len(input))
	for backendName, inventory := range input {
		if !inventory.StorageIdentity.Valid() {
			id, err := backendidentity.New()
			if err != nil {
				panic(err)
			}
			inventory.StorageIdentity = id
		}
		if inventory.Provisions == nil {
			inventory.Provisions = []string{}
		}
		if inventory.Retentions == nil {
			inventory.Retentions = []string{}
		}
		if inventory.ProvisionProviderUUIDs == nil {
			inventory.ProvisionProviderUUIDs = make(map[string]string, len(inventory.Provisions))
			for _, leaseUUID := range inventory.Provisions {
				inventory.ProvisionProviderUUIDs[leaseUUID] = ""
			}
		}
		if inventory.ProvisionItems == nil {
			inventory.ProvisionItems = make(map[string][]backend.LeaseItem, len(inventory.Provisions))
			for _, leaseUUID := range inventory.Provisions {
				inventory.ProvisionItems[leaseUUID] = []backend.LeaseItem{{
					SKU: "sku-test", Quantity: 1, ServiceName: legacyDefaultServiceName,
				}}
			}
		}
		result[backendName] = inventory
	}
	return result
}

func TestDecodeV013PreflightPlacement_AcceptsEncodedZeroTimestamp(t *testing.T) {
	backendName, err := decodeV013PreflightPlacement(
		[]byte(`{"backend":"backend-a","set_at":"0001-01-01T00:00:00Z"}`),
	)
	require.NoError(t, err,
		"v0.13 could preserve a zero SetAt while rewriting an older raw record")
	assert.Equal(t, "backend-a", backendName)
}

func TestOpenLegacyUpgradeInspector_MissingPathDoesNotCreateDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing.db")
	_, err := OpenLegacyUpgradeInspector(dbPath)
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist))
	_, statErr := os.Stat(dbPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestOpenLegacyUpgradePreflightRejectsEmptyOrTruncatedFileWithoutMutation(t *testing.T) {
	t.Parallel()

	openers := []struct {
		name string
		open func(string) (func() error, error)
	}{
		{
			name: "inspector",
			open: func(path string) (func() error, error) {
				inspector, err := OpenLegacyUpgradeInspector(path)
				if err != nil {
					return nil, err
				}
				return inspector.Close, nil
			},
		},
		{
			name: "preparer",
			open: func(path string) (func() error, error) {
				preparer, err := OpenLegacyUpgradePreparer(path)
				if err != nil {
					return nil, err
				}
				return preparer.Close, nil
			},
		},
	}
	files := []struct {
		name     string
		contents []byte
	}{
		{name: "zero length", contents: []byte{}},
		{name: "truncated", contents: []byte{0x42, 0x4f, 0x4c, 0x54}},
	}
	for _, opener := range openers {
		opener := opener
		for _, file := range files {
			file := file
			t.Run(opener.name+"/"+file.name, func(t *testing.T) {
				t.Parallel()
				path := filepath.Join(t.TempDir(), "placements.db")
				require.NoError(t, os.WriteFile(path, file.contents, 0o600))
				before, err := os.ReadFile(path)
				require.NoError(t, err)

				closeHandle, err := opener.open(path)
				require.Error(t, err)
				require.Nil(t, closeHandle)
				if len(file.contents) == 0 {
					require.ErrorIs(t, err, ErrLegacyUpgradePreflight)
					require.ErrorContains(t, err, "placement database is empty")
				}

				after, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				assert.Equal(t, before, after)
			})
		}
	}
}
