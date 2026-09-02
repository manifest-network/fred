package placement

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func projectForBaselineTest(
	t *testing.T,
	s *Store,
	projection InventoryProjection,
) (ProjectionResult, error) {
	t.Helper()
	if projection.Complete && projection.BackendStorageIdentities == nil {
		s.mu.RLock()
		projection.BackendStorageIdentities = testBackendStorageIDs(s.backendTopology...)
		s.mu.RUnlock()
	}
	if projection.Complete && projection.EmptyBackends == nil {
		s.mu.RLock()
		projection.EmptyBackends = emptyBackendsForTest(s.backendTopology, projection)
		s.mu.RUnlock()
	}
	fence := s.BeginInventorySession()
	result, err := s.ProjectInventory(fence, projection)
	s.EndInventorySession(fence)
	return result, err
}

func completeBackendObservationForTest(
	t *testing.T,
	storageID backendidentity.ID,
	provisions []string,
	retentions []string,
) CompleteBackendObservation {
	t.Helper()
	observation, err := NewCompleteBackendObservation(storageID, provisions, retentions)
	require.NoError(t, err)
	return observation
}

func TestNewCompleteBackendObservationRejectsMissingEvidence(t *testing.T) {
	t.Parallel()

	validID := testBackendStorageID("backend-a")
	_, err := NewCompleteBackendObservation(backendidentity.ID{}, []string{}, []string{})
	require.ErrorIs(t, err, ErrBackendStorageIdentityUnbound)
	_, err = NewCompleteBackendObservation(validID, []string(nil), []string{})
	require.ErrorContains(t, err, "concrete provision and retention inventories")
	_, err = NewCompleteBackendObservation(validID, []string{}, []string(nil))
	require.ErrorContains(t, err, "concrete provision and retention inventories")
}

func requireAdmissionScope(
	t *testing.T,
	s *Store,
	baseline AdmissionBaseline,
	eligibleNames ...string,
) AdmissionScope {
	t.Helper()
	scope, err := s.ScopeAdmission(baseline, eligibleNames)
	require.NoError(t, err)
	require.True(t, scope.Valid())
	return scope
}

func TestStore_AdmissionBaselineRequiresConfiguredCompleteProjection(t *testing.T) {
	s := newTestStore(t)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.False(t, s.InventoryBootstrapped())

	_, err := projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.ErrorIs(t, err, ErrBackendTopologyNotConfigured)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())

	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-b", "backend-a"}))
	_, err = projectForBaselineTest(t, s, InventoryProjection{})
	require.NoError(t, err)
	assert.False(t, s.CurrentAdmissionBaseline().Valid(),
		"a partial projection cannot establish absence authority")
	assert.False(t, s.InventoryBootstrapped())

	_, err = projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.NoError(t, err)
	baseline := s.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	assert.True(t, s.InventoryBootstrapped())

	partialFence := s.BeginInventorySession()
	assert.True(t, s.CurrentAdmissionBaseline().Valid(),
		"starting another inventory invalidates projection proofs, not the baseline")
	_, err = s.ProjectInventory(partialFence, InventoryProjection{})
	require.NoError(t, err)
	s.EndInventorySession(partialFence)
	assert.Equal(t, baseline, s.CurrentAdmissionBaseline(),
		"a partial projection cannot erase a prior matching baseline")
}

func TestStore_AdmissionBaselineSurvivesReopenWithMatchingTopology(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-b", "backend-a"}))
	_, err = projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.NoError(t, err)
	oldProcessBaseline := s.CurrentAdmissionBaseline()
	require.True(t, oldProcessBaseline.Valid())
	oldProcessScope := requireAdmissionScope(t, s, oldProcessBaseline, "backend-a")
	require.NoError(t, s.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, []string{"backend-a", "backend-b"}, reopened.backendTopology)
	assert.True(t, reopened.InventoryBootstrapped())
	reopenedBaseline := reopened.CurrentAdmissionBaseline()
	require.True(t, reopenedBaseline.Valid())
	assert.NotEqual(t, oldProcessBaseline, reopenedBaseline,
		"capabilities are rebound to the reopened Store")

	require.NoError(t, configureBackendTopologyForTest(reopened, []string{"backend-b", "backend-a"}))
	assert.Equal(t, reopenedBaseline, reopened.CurrentAdmissionBaseline(),
		"canonical reordering is an idempotent topology configuration")

	token, applied, err := reopened.BeginNewAttempt(
		oldProcessScope, "foreign", "backend-a", requireOperationID(t, "9001"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9001")))

	require.ErrorIs(t, err, ErrInvalidAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	_, err = reopened.ScopeAdmission(oldProcessBaseline, []string{"backend-a"})
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
}

func TestStore_BackendTopologyChangesRequireRebaselineAndPreserveIdentityHistory(t *testing.T) {
	s := newTestStore(t)
	first := requireAdmissionBaseline(t, s, "backend-a")
	firstScope := requireAdmissionScope(t, s, first, "backend-a")

	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
	assert.False(t, s.InventoryBootstrapped())
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	token, applied, err := s.BeginNewAttempt(
		firstScope, "lease-stale-add", "backend-a", requireOperationID(t, "9010"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9010")))

	require.ErrorIs(t, err, ErrInvalidAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	_, err = s.ScopeAdmission(first, []string{"backend-a"})
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)

	_, err = projectForBaselineTest(t, s, InventoryProjection{})
	require.NoError(t, err)
	assert.False(t, s.InventoryBootstrapped(), "partial inventory cannot rebaseline")
	second := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	require.True(t, second.Valid())

	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a"}))
	assert.False(t, s.InventoryBootstrapped())
	third := requireAdmissionBaseline(t, s, "backend-a")
	require.True(t, third.Valid())

	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-b", "backend-a"}))
	assert.False(t, s.InventoryBootstrapped(),
		"reactivating the same identity is still a distinct topology generation")
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"},
		slices.Collect(maps.Keys(s.knownBackendNames)),
		"reactivation must preserve the durable identity history")
}

func TestStore_TopologyChangesRequireIdentityProbeAndCommitAtomically(t *testing.T) {
	s := newTestStore(t)
	idA := testBackendStorageID("backend-a")
	idB := testBackendStorageID("backend-b")
	replacementA := testBackendStorageID("replacement-a")

	requiresProbe, err := s.BackendTopologyRequiresIdentityProbe([]string{"backend-a"})
	require.NoError(t, err)
	assert.True(t, requiresProbe)
	require.NoError(t, s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": idA},
	))
	requireAdmissionBaseline(t, s, "backend-a")
	baseline := s.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())

	requiresProbe, err = s.BackendTopologyRequiresIdentityProbe([]string{"backend-a"})
	require.NoError(t, err)
	assert.False(t, requiresProbe)
	requiresProbe, err = s.BackendTopologyRequiresIdentityProbe(
		[]string{"backend-b", "backend-a"},
	)
	require.NoError(t, err)
	assert.True(t, requiresProbe)

	before := rawTopologyMetadataForTest(t, s)
	err = s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b"},
		map[string]backendidentity.ID{"backend-a": replacementA, "backend-b": idB},
	)
	require.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
	assert.Equal(t, before, rawTopologyMetadataForTest(t, s),
		"a failed proposed-topology probe must not mutate durable authority")
	assert.Equal(t, baseline, s.CurrentAdmissionBaseline())
	assert.Equal(t, []string{"backend-a"}, s.backendTopology)

	require.NoError(t, s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b"},
		map[string]backendidentity.ID{"backend-a": idA, "backend-b": idB},
	))
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.Equal(t, idA, s.backendStorageIDs["backend-a"])
	assert.Equal(t, idB, s.backendStorageIDs["backend-b"])

	err = s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": idA},
	)
	require.ErrorIs(t, err, ErrBackendTopologyInUse,
		"a topology membership change must not treat an old topology's inventory as drain proof")
	_, err = projectForBaselineTest(t, s, InventoryProjection{Complete: true})
	require.NoError(t, err)
	require.NoError(t, s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": idA},
	))
	assert.Equal(t, idB, s.backendStorageIDs["backend-b"],
		"removed backend identities remain immutable historical authority")

	before = rawTopologyMetadataForTest(t, s)
	err = s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b"},
		map[string]backendidentity.ID{
			"backend-a": idA,
			"backend-b": testBackendStorageID("replacement-b"),
		},
	)
	require.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
	assert.Equal(t, before, rawTopologyMetadataForTest(t, s))

	err = s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-c"},
		map[string]backendidentity.ID{"backend-a": idA, "backend-c": idA},
	)
	require.ErrorIs(t, err, ErrBackendStorageIdentityConflict)
	assert.Equal(t, before, rawTopologyMetadataForTest(t, s))

	require.NoError(t, s.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b"},
		map[string]backendidentity.ID{"backend-a": idA, "backend-b": idB},
	))
	assert.Equal(t, idB, s.backendStorageIDs["backend-b"])
}

func TestStore_CompleteTopologyObservationMakesEmptyAdditionRevertibleAcrossRestart(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	idA := testBackendStorageID("backend-a")
	idB := testBackendStorageID("backend-b")
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": idA},
	))
	_, err = projectForBaselineTest(t, store, InventoryProjection{Complete: true})
	require.NoError(t, err)
	require.True(t, store.CurrentAdmissionBaseline().Valid())

	require.NoError(t, store.ConfigureBackendTopologyWithCompleteObservations(
		[]string{"backend-a", "backend-b"},
		map[string]CompleteBackendObservation{
			"backend-a": completeBackendObservationForTest(t, idA, []string{}, []string{}),
			"backend-b": completeBackendObservationForTest(t, idB, []string{}, []string{}),
		},
	))
	assert.False(t, store.CurrentAdmissionBaseline().Valid(),
		"topology inventory is removal evidence, not admission authority")
	assert.Equal(t, store.topologyID, store.inventoryTopologyID)
	assert.Equal(t, map[string]struct{}{"backend-a": {}, "backend-b": {}}, store.emptyInventoryBackends)
	require.NoError(t, store.Close())

	reopened, err := OpenStore(dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.ConfigureBackendTopologyWithCompleteObservations(
		[]string{"backend-a"},
		map[string]CompleteBackendObservation{
			"backend-a": completeBackendObservationForTest(t, idA, []string{}, []string{}),
		},
	))
	assert.Equal(t, []string{"backend-a"}, reopened.backendTopology)
	assert.False(t, reopened.CurrentAdmissionBaseline().Valid())
}

func TestStore_CompleteTopologyObservationDoesNotMakeNonemptyAdditionRemovable(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	idA := testBackendStorageID("backend-a")
	idB := testBackendStorageID("backend-b")
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": idA},
	))
	require.NoError(t, store.ConfigureBackendTopologyWithCompleteObservations(
		[]string{"backend-a", "backend-b"},
		map[string]CompleteBackendObservation{
			"backend-a": completeBackendObservationForTest(t, idA, []string{}, []string{}),
			"backend-b": completeBackendObservationForTest(t, idB,
				[]string{"4d39880f-66ec-4f76-a60e-f33ab4aef8b4"}, []string{}),
		},
	))

	err := store.ConfigureBackendTopologyWithCompleteObservations(
		[]string{"backend-a"},
		map[string]CompleteBackendObservation{
			"backend-a": completeBackendObservationForTest(t, idA, []string{}, []string{}),
		},
	)
	require.ErrorIs(t, err, ErrBackendTopologyInUse)
	require.ErrorContains(t, err, `latest complete inventory did not prove backend "backend-b" empty`)
	assert.Equal(t, []string{"backend-a", "backend-b"}, store.backendTopology)
}

func TestStore_CompleteTopologyObservationCannotOverrideDurableBackendReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		placement  Placement
		capability *lifecycleCapability
	}{
		{
			name: "attempt",
			placement: Placement{
				Attempt:            "backend-b",
				attemptOperationID: requireOperationID(t, "9451"),
				revision:           1,
			},
		},
		{
			name: "conflict",
			placement: Placement{
				Backend:          "backend-a",
				Conflict:         true,
				ConflictBackends: []string{"backend-a", "backend-b"},
				revision:         1,
			},
		},
		{
			name:       "lifecycle",
			placement:  Placement{},
			capability: &lifecycleCapability{backend: "backend-b", id: requireLifecycleID(t, "9452")},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			idA := testBackendStorageID("backend-a")
			idB := testBackendStorageID("backend-b")
			require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
				[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": idA},
			))
			require.NoError(t, store.ConfigureBackendTopologyWithCompleteObservations(
				[]string{"backend-a", "backend-b"},
				map[string]CompleteBackendObservation{
					"backend-a": completeBackendObservationForTest(t, idA, []string{}, []string{}),
					"backend-b": completeBackendObservationForTest(t, idB, []string{}, []string{}),
				},
			))
			leaseUUID := "e87ba2cc-813e-4da1-adde-57897838dbd4"
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				if test.placement.revision != 0 {
					encoded, encodeErr := encodePlacement(test.placement)
					if encodeErr != nil {
						return encodeErr
					}
					if putErr := tx.Bucket(bucketName).Put([]byte(leaseUUID), encoded); putErr != nil {
						return putErr
					}
				}
				if test.capability != nil {
					encoded, encodeErr := encodeLifecycleCapability(*test.capability)
					if encodeErr != nil {
						return encodeErr
					}
					return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte(leaseUUID), encoded)
				}
				return nil
			}))

			err := store.ConfigureBackendTopologyWithCompleteObservations(
				[]string{"backend-a"},
				map[string]CompleteBackendObservation{
					"backend-a": completeBackendObservationForTest(t, idA, []string{}, []string{}),
				},
			)
			require.ErrorIs(t, err, ErrBackendTopologyInUse)
			assert.Equal(t, []string{"backend-a", "backend-b"}, store.backendTopology)
		})
	}
}

func TestStore_BackendRemovalRequiresEmptyInventoryAndNoLifecycleAuthority(t *testing.T) {
	t.Parallel()

	t.Run("no current complete inventory", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
		err := configureBackendTopologyForTest(s, []string{"backend-a"})
		require.ErrorIs(t, err, ErrBackendTopologyInUse)
		require.ErrorContains(t, err, "no complete current-topology inventory")
	})

	t.Run("latest inventory reports removed backend nonempty", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
		_, err := projectForBaselineTest(t, s, InventoryProjection{
			Complete: true,
			Placements: map[string]string{
				"11638ef8-1401-4f14-a355-1ae02afeb35b": "backend-b",
			},
		})
		require.NoError(t, err)
		err = configureBackendTopologyForTest(s, []string{"backend-a"})
		require.ErrorIs(t, err, ErrBackendTopologyInUse)
		require.ErrorContains(t, err, `latest complete inventory did not prove backend "backend-b" empty`)
	})

	t.Run("teardown-only lifecycle authority blocks empty backend", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
		_, err := projectForBaselineTest(t, s, InventoryProjection{Complete: true})
		require.NoError(t, err)
		capability := lifecycleCapability{
			backend: "backend-b",
			id:      requireLifecycleID(t, "9331"),
		}
		encoded, err := encodeLifecycleCapability(capability)
		require.NoError(t, err)
		require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(lifecycleCapabilityBucketName).Put(
				[]byte("11638ef8-1401-4f14-a355-1ae02afeb35b"), encoded,
			)
		}))

		err = configureBackendTopologyForTest(s, []string{"backend-a"})
		require.ErrorIs(t, err, ErrBackendTopologyInUse)
		require.ErrorContains(t, err, "still carries lifecycle authority")
	})

	t.Run("corrupt lifecycle authority blocks removal", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
		_, err := projectForBaselineTest(t, s, InventoryProjection{Complete: true})
		require.NoError(t, err)
		require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(lifecycleCapabilityBucketName).Put(
				[]byte("11638ef8-1401-4f14-a355-1ae02afeb35b"), []byte("{"),
			)
		}))

		err = configureBackendTopologyForTest(s, []string{"backend-a"})
		require.ErrorIs(t, err, ErrBackendTopologyInUse)
		require.ErrorContains(t, err, "uninterpretable durable lifecycle authority")
	})

	t.Run("complete empty inventory permits drained removal and reactivation", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
		_, err := projectForBaselineTest(t, s, InventoryProjection{Complete: true})
		require.NoError(t, err)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a"}))
		_, err = projectForBaselineTest(t, s, InventoryProjection{Complete: true})
		require.NoError(t, err)
		require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
	})
}

func rawTopologyMetadataForTest(t *testing.T, s *Store) []byte {
	t.Helper()
	var encoded []byte
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		require.NotNil(t, bucket)
		encoded = bytes.Clone(bucket.Get(metadataStateKey))
		return nil
	}))
	return encoded
}

func TestStore_BackendTopologyReactivationSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	initial, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(initial, []string{"backend-b", "backend-a"}))
	_, err = projectForBaselineTest(t, initial, InventoryProjection{Complete: true})
	require.NoError(t, err)
	require.True(t, initial.CurrentAdmissionBaseline().Valid())
	initialTopologyID := initial.topologyID
	require.NoError(t, initial.Close())

	reduced, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(reduced, []string{"backend-a"}))
	assert.Equal(t, initialTopologyID+1, reduced.topologyID)
	assert.False(t, reduced.CurrentAdmissionBaseline().Valid())
	_, err = projectForBaselineTest(t, reduced, InventoryProjection{Complete: true})
	require.NoError(t, err)
	require.True(t, reduced.CurrentAdmissionBaseline().Valid())
	reducedTopologyID := reduced.topologyID
	require.NoError(t, reduced.Close())

	reactivated, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(reactivated, []string{"backend-b", "backend-a"}))
	assert.Equal(t, reducedTopologyID+1, reactivated.topologyID)
	assert.Equal(t, []string{"backend-a", "backend-b"}, reactivated.backendTopology)
	assert.False(t, reactivated.CurrentAdmissionBaseline().Valid(),
		"the reduced topology's absence authority must not authorize the restored topology")
	_, err = projectForBaselineTest(t, reactivated, InventoryProjection{Complete: true})
	require.NoError(t, err)
	require.NoError(t, reactivated.Close())

	verified, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, verified.Close()) })
	assert.Equal(t, []string{"backend-a", "backend-b"}, verified.backendTopology)
	assert.True(t, verified.CurrentAdmissionBaseline().Valid(),
		"the restored topology's own complete baseline must survive another reopen")
}

func TestStore_ConfigureBackendTopologyRejectsRemovedDurableReferences(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *Store)
	}{
		{
			name: "confirmed Backend",
			prepare: func(t *testing.T, s *Store) {
				requireConfirmedPlacement(t, s, "lease", "backend-b")
			},
		},
		{
			name: "pending Attempt",
			prepare: func(t *testing.T, s *Store) {
				requireTestAdmission(t, s)
				requireTypedAttempt(t, s, "lease", "backend-b", requireOperationID(t, "9051"))
			},
		},
		{
			name: "quarantine ConflictBackends",
			prepare: func(t *testing.T, s *Store) {
				requireConflictPlacement(t, s, "lease", "backend-a", "backend-b")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
			tt.prepare(t, s)
			require.ErrorIs(t,
				configureBackendTopologyForTest(s, []string{"backend-a"}),
				ErrBackendTopologyInUse,
			)
			assert.Equal(t, []string{"backend-a", "backend-b"}, s.backendTopology)
		})
	}

	t.Run("v0.13 raw owner", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "placements.db")
		writeRawRecords(t, dbPath, map[string][]byte{"lease": []byte("backend-b")})
		s, err := newStore(dbPath, true)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		require.ErrorIs(t,
			configureBackendTopologyForTest(s, []string{"backend-a"}),
			ErrBackendTopologyInUse,
		)
	})

	for name, value := range map[string][]byte{
		"empty record":           {},
		"malformed JSON":         []byte(`{"backend":`),
		"identity-free object":   []byte(`{"set_at":"2026-08-25T15:00:00Z"}`),
		"unknown conflict owner": []byte(`{"conflict":true,"conflict_backends":["backend-a"]}`),
		"invalid legacy owner":   {0xff},
	} {
		t.Run(name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, map[string][]byte{"lease": value})
			s, err := newStore(dbPath, true)
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			err = configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"})
			require.ErrorIs(t, err, ErrBackendTopologyInUse)
			require.ErrorContains(t, err,
				`lease "lease" has uninterpretable durable placement`,
				"startup diagnostics must name the exact key that requires offline inspection",
			)
		})
	}
}

func TestStore_ConfigureBackendTopologyValidatesNamesAndProjectionMembership(t *testing.T) {
	tests := []struct {
		name  string
		names []string
	}{
		{name: "missing"},
		{name: "empty", names: []string{}},
		{name: "empty name", names: []string{""}},
		{name: "blank name", names: []string{" \t"}},
		{name: "leading whitespace", names: []string{" backend-a"}},
		{name: "forged log line", names: []string{"backend-a\nPASS: forged"}},
		{name: "duplicate", names: []string{"backend-a", "backend-a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			require.ErrorIs(t, configureBackendTopologyForTest(s, tt.names), ErrInvalidBackendTopology)
			assert.False(t, s.CurrentAdmissionBaseline().Valid())
		})
	}

	s := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a"}))
	_, err := projectForBaselineTest(t, s, InventoryProjection{
		Complete:   true,
		Placements: map[string]string{"lease": "backend-b"},
	})
	require.ErrorIs(t, err, ErrBackendNotInTopology)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())

	baseline := requireAdmissionBaseline(t, s, "backend-a")
	scope := requireAdmissionScope(t, s, baseline, "backend-a")
	token, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-b", requireOperationID(t, "9020"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9020")))

	require.ErrorIs(t, err, ErrBackendNotInTopology)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	_, err = s.ScopeAdmission(baseline, []string{"backend-b"})
	require.ErrorIs(t, err, ErrBackendNotInTopology)
}

func TestStore_NewStoreRejectsMalformedTopologyMetadata(t *testing.T) {
	tests := []struct {
		name    string
		encoded []byte
	}{
		{name: "invalid JSON", encoded: []byte("{")},
		{name: "unsupported schema", encoded: []byte(`{"schema":99}`)},
		{
			name: "fingerprint mismatch",
			encoded: []byte(`{"schema":1,"topology":["backend-a"],` +
				`"topology_fingerprint":"bad","known_backends":["backend-a"],"topology_id":1}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			s, err := newStoreForTest(dbPath)
			require.NoError(t, err)
			require.NoError(t, s.Close())

			db, err := bolt.Open(dbPath, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(metadataBucketName).Put(metadataStateKey, tt.encoded)
			}))
			require.NoError(t, db.Close())

			reopened, err := newStoreForTest(dbPath)
			require.Error(t, err)
			assert.Nil(t, reopened)
		})
	}
}

func TestDecodeTopologyMetadataRejectsAmbiguousOrNoncanonicalJSON(t *testing.T) {
	t.Parallel()

	validV2, err := decodeTopologyMetadata([]byte(`{"schema":2}`))
	require.NoError(t, err)
	assert.Equal(t, uint64(topologyMetadataSchema), validV2.Schema)

	tests := []struct {
		name    string
		encoded []byte
		want    string
	}{
		{
			name:    "duplicate field",
			encoded: []byte(`{"schema":2,"schema":1}`),
			want:    `duplicate placement metadata field "schema"`,
		},
		{
			name:    "escaped duplicate field",
			encoded: []byte(`{"schema":2,"\u0073chema":2}`),
			want:    `duplicate placement metadata field "schema"`,
		},
		{
			name:    "unknown current field",
			encoded: []byte(`{"schema":2,"surprise":true}`),
			want:    `unknown placement metadata schema 2 field "surprise"`,
		},
		{
			name: "escaped duplicate storage identity name",
			encoded: []byte(
				`{"schema":2,"known_backend_storage_ids":{"backend-a":"first","\u0062ackend-a":"second"}}`,
			),
			want: `duplicate placement storage identity name "backend-a"`,
		},
		{
			name:    "unsupported branch-only schema",
			encoded: []byte(`{"schema":1}`),
			want:    "unsupported placement metadata schema 1",
		},
		{
			name:    "missing schema",
			encoded: []byte(`{}`),
			want:    `missing placement metadata field "schema"`,
		},
		{
			name:    "wrong schema type",
			encoded: []byte(`{"schema":"2"}`),
			want:    "decode placement metadata schema",
		},
		{
			name:    "wrong optional field type",
			encoded: []byte(`{"schema":2,"provider_uuid":42}`),
			want:    "decode placement metadata fields",
		},
		{
			name:    "null optional field",
			encoded: []byte(`{"schema":2,"provider_uuid":null}`),
			want:    `placement metadata field "provider_uuid" must not be null`,
		},
		{
			name:    "trailing value",
			encoded: []byte(`{"schema":2}{"schema":2}`),
			want:    "trailing JSON value after placement metadata",
		},
		{
			name:    "invalid UTF-8",
			encoded: append([]byte(`{"schema":2,"provider_uuid":"`), 0xff),
			want:    "placement metadata is not valid UTF-8",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeTopologyMetadata(test.encoded)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestStore_CompleteProjectionDoesNotArmBaselineWhenTransactionFails(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a"}))
	oversizedLease := strings.Repeat("z", bolt.MaxKeySize+1)

	_, err = projectForBaselineTest(t, s, InventoryProjection{
		Complete:   true,
		Placements: map[string]string{oversizedLease: "backend-a"},
	})
	require.Error(t, err)
	assert.False(t, s.CurrentAdmissionBaseline().Valid())
	assert.False(t, s.InventoryBootstrapped())
	assert.Equal(t, StateAbsent, s.Lookup(oversizedLease).State())
	require.NoError(t, s.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.False(t, reopened.InventoryBootstrapped(),
		"the placement write and baseline metadata must roll back together")
}

func TestStore_AdmissionBaselineRejectsZeroForeignStaleAndForgedCapabilities(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	other := newTestStore(t)
	foreign := requireAdmissionBaseline(t, other, "backend-a")

	tests := []struct {
		name     string
		baseline AdmissionBaseline
	}{
		{name: "zero"},
		{name: "foreign", baseline: foreign},
		{
			name: "forged fingerprint",
			baseline: AdmissionBaseline{
				issuer: s, topologyID: baseline.topologyID, fingerprint: "forged",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.ScopeAdmission(tt.baseline, []string{"backend-a"})
			require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
		})
	}

	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
	_, err := s.ScopeAdmission(baseline, []string{"backend-a"})
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
}

func TestStore_ScopeAdmissionIsZeroSafeSubsetBoundAndDefensive(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b", "backend-c")

	var zero AdmissionScope
	assert.False(t, zero.Valid())
	assert.False(t, zero.Allows("backend-a"))
	assert.False(t, zero.Allows(""))

	eligible := []string{"backend-b", "backend-a"}
	scope, err := s.ScopeAdmission(baseline, eligible)
	require.NoError(t, err)
	require.True(t, scope.Valid())
	assert.True(t, scope.Allows("backend-a"))
	assert.True(t, scope.Allows("backend-b"))
	assert.False(t, scope.Allows("backend-c"))
	assert.False(t, scope.Allows(""))

	eligible[0] = "backend-c"
	assert.True(t, scope.Allows("backend-b"),
		"mutating the caller's slice must not attenuate an issued scope")
	assert.False(t, scope.Allows("backend-c"),
		"mutating the caller's slice must not widen an issued scope")

	empty, err := s.ScopeAdmission(baseline, nil)
	require.NoError(t, err)
	assert.True(t, empty.Valid(), "an explicitly issued deny-all scope is valid")
	assert.False(t, empty.Allows("backend-a"))
	token, applied, err := s.BeginNewAttempt(
		empty, "lease-empty", "backend-a", requireOperationID(t, "9030"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9030")))

	require.ErrorIs(t, err, ErrBackendOutsideAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("lease-empty").State())
}

func TestDurableBackendNamesUsesValidatedPlacementDecoder(t *testing.T) {
	invalidUTF8 := append([]byte(`{"backend":"backend-a","future":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	tests := []struct {
		name      string
		value     []byte
		wantNames []string
		wantErr   bool
	}{
		{
			name: "forward-compatible unknown field",
			value: []byte(`{"backend":"backend-a",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1,` +
				`"future":{"nested":true}}`),
			wantNames: []string{"backend-a"},
		},
		{
			name: "known conflict candidates",
			value: []byte(`{"conflict":true,` +
				`"conflict_backends":["backend-b","backend-a"],` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1}`),
			wantNames: []string{"backend-a", "backend-b"},
		},
		{name: "raw legacy backend", value: []byte("backend-a"), wantNames: []string{"backend-a"}},
		{
			name: "duplicate ownership field",
			value: []byte(`{"backend":"backend-a","backend":"backend-b",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1}`),
			wantErr: true,
		},
		{
			name: "attempt missing typed metadata",
			value: []byte(`{"attempt":"backend-a",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1}`),
			wantErr: true,
		},
		{
			name: "trailing value",
			value: []byte(`{"backend":"backend-a",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1} {}`),
			wantErr: true,
		},
		{name: "non-object root", value: []byte(`[]`), wantErr: true},
		{name: "invalid UTF-8", value: invalidUTF8, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			names, err := durableBackendNames("lease", test.value)
			if test.wantErr {
				require.Error(t, err)
				assert.Empty(t, names)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantNames, names)
		})
	}
}

func TestStore_ScopeAdmissionRejectsMalformedAndForeignBackends(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")

	for _, tt := range []struct {
		name     string
		eligible []string
		wantErr  error
	}{
		{name: "empty name", eligible: []string{""}, wantErr: ErrInvalidAdmissionScope},
		{name: "blank name", eligible: []string{" \t"}, wantErr: ErrInvalidAdmissionScope},
		{name: "duplicate", eligible: []string{"backend-a", "backend-a"}, wantErr: ErrInvalidAdmissionScope},
		{name: "outside topology", eligible: []string{"backend-c"}, wantErr: ErrBackendNotInTopology},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scope, err := s.ScopeAdmission(baseline, tt.eligible)
			require.ErrorIs(t, err, tt.wantErr)
			assert.False(t, scope.Valid())
		})
	}
}

func TestStore_BeginNewAttemptRequiresCurrentAdmissionScopeAndExactEligibility(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	scopeA := requireAdmissionScope(t, s, baseline, "backend-a")
	other := newTestStore(t)
	otherBaseline := requireAdmissionBaseline(t, other, "backend-a", "backend-b")
	foreign := requireAdmissionScope(t, other, otherBaseline, "backend-a")

	for i, tt := range []struct {
		name  string
		scope AdmissionScope
	}{
		{name: "zero"},
		{name: "foreign", scope: foreign},
		{
			name: "forged fingerprint",
			scope: AdmissionScope{
				issuer: s, topologyID: scopeA.topologyID, fingerprint: "forged",
				eligible: map[string]struct{}{"backend-a": {}},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token, applied, err := s.BeginNewAttempt(
				tt.scope, "invalid-scope-"+tt.name, "backend-a",
				requireOperationID(t, fmt.Sprint(9040+i)), PayloadFingerprint{},
				testBackendRequestSnapshot(t),
				testCallbackPair(requireOperationID(t, fmt.Sprint(9040+i))))

			require.ErrorIs(t, err, ErrInvalidAdmissionScope)
			assert.False(t, applied)
			assert.False(t, token.Valid())
		})
	}

	token, applied, err := s.BeginNewAttempt(
		scopeA, "outside-scope", "backend-b", requireOperationID(t, "9050"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9050")))

	require.ErrorIs(t, err, ErrBackendOutsideAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("outside-scope").State())

	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b", "backend-c"}))
	token, applied, err = s.BeginNewAttempt(
		scopeA, "stale-scope", "backend-a", requireOperationID(t, "9051"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9051")))

	require.ErrorIs(t, err, ErrInvalidAdmissionScope)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("stale-scope").State())
}

func TestStore_BeginNewAttemptIsExactInsertIfAbsentCAS(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	scope := requireAdmissionScope(t, s, baseline, "backend-a", "backend-b")

	// An observation inserted after a caller saw absence wins the admission CAS.
	require.Equal(t, StateAbsent, s.Lookup("toctou").State())
	requireConfirmedPlacement(t, s, "toctou", "backend-a")
	token, applied, err := s.BeginNewAttempt(
		scope, "toctou", "backend-a", requireOperationID(t, "9050"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9050")))

	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateConfirmed, s.Lookup("toctou").State())

	const contenders = 32
	type outcome struct {
		token   AttemptToken
		applied bool
		err     error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			opID, parseErr := testOperationID(uint64(9060 + i))
			if parseErr != nil {
				outcomes <- outcome{err: parseErr}
				return
			}
			token, applied, err := s.BeginNewAttempt(
				scope, "concurrent", "backend-a", opID, PayloadFingerprint{},
				testBackendRequestSnapshot(t), testCallbackPair(opID))

			outcomes <- outcome{token: token, applied: applied, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(outcomes)

	winners := 0
	for result := range outcomes {
		require.NoError(t, result.err)
		if result.applied {
			winners++
			assert.True(t, result.token.Valid())
		} else {
			assert.False(t, result.token.Valid())
		}
	}
	assert.Equal(t, 1, winners)
	assert.Equal(t, StateAttempting, s.Lookup("concurrent").State())
}

func TestStore_BeginOwnedAttemptRequiresExactConfirmedOwnerRevision(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "owned", "backend-a")
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	revision := s.Lookup("owned").RecordRevision()
	require.True(t, revision.Valid())

	token, applied, err := s.BeginOwnedAttempt(
		baseline, revision, "backend-a", requireOperationID(t, "9100"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9100")))

	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())
	assert.Equal(t, "backend-a", s.Lookup("owned").Attempt)

	second, applied, err := s.BeginOwnedAttempt(
		baseline, revision, "backend-a", requireOperationID(t, "9101"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9101")))

	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, second.Valid())
	require.True(t, mustRefuseAttempt(t, s, token))

	freshRevision := s.Lookup("owned").RecordRevision()
	wrongOwner, applied, err := s.BeginOwnedAttempt(
		baseline, freshRevision, "backend-b", requireOperationID(t, "9102"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9102")))

	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, wrongOwner.Valid())

	require.True(t, mustDeleteRecord(t, s, freshRevision))
	requireConfirmedPlacement(t, s, "owned", "backend-a")
	stale, applied, err := s.BeginOwnedAttempt(
		baseline, freshRevision, "backend-a", requireOperationID(t, "9103"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9103")))

	require.NoError(t, err)
	assert.False(t, applied)
	assert.False(t, stale.Valid())

	other := newTestStore(t)
	requireConfirmedPlacement(t, other, "owned", "backend-a")
	foreignRevision := other.Lookup("owned").RecordRevision()
	foreign, applied, err := s.BeginOwnedAttempt(
		baseline, foreignRevision, "backend-a", requireOperationID(t, "9104"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "9104")))

	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, applied)
	assert.False(t, foreign.Valid())
}

func mustRefuseAttempt(t *testing.T, s *Store, token AttemptToken) bool {
	t.Helper()
	applied, err := s.RefuseAttempt(token)
	require.NoError(t, err)
	return applied
}

func mustDeleteRecord(t *testing.T, s *Store, revision RecordRevision) bool {
	t.Helper()
	applied, err := s.DeleteRecord(revision)
	require.NoError(t, err)
	return applied
}

func TestStore_BeginRestoreRequiresCurrentAdmissionBaseline(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "source", "backend-a")
	baseline := requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	opID := requireOperationID(t, "9200")

	claim, err := s.beginRestore(
		AdmissionBaseline{}, "source", "target", opID,
		testBackendRequestSnapshot(t), testCallbackPair(opID),
	)
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
	assert.False(t, claim.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("target").State())

	require.NoError(t, configureBackendTopologyForTest(s,
		[]string{"backend-a", "backend-b", "backend-c"},
	))
	claim, err = s.beginRestore(
		baseline, "source", "target", opID,
		testBackendRequestSnapshot(t), testCallbackPair(opID),
	)
	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
	assert.False(t, claim.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("target").State())

	baseline = requireAdmissionBaseline(t, s, "backend-a", "backend-b", "backend-c")
	claim, err = s.beginRestore(
		baseline, "source", "target", opID,
		testBackendRequestSnapshot(t), testCallbackPair(opID),
	)
	require.NoError(t, err)
	assert.True(t, claim.Valid())
}

func TestStore_InventoryConflictPreservesPendingAttemptIdentity(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	opID := requireOperationID(t, "9300")
	payloadHash := sha256.Sum256([]byte("exact conflict payload"))
	fingerprint, err := NewPayloadFingerprint(payloadHash[:])
	require.NoError(t, err)
	scope := requireAdmissionScope(t, s, s.CurrentAdmissionBaseline(), "backend-a")
	token, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-a", opID, fingerprint,
		testBackendRequestSnapshot(t), testCallbackPair(opID))

	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())

	_, err = projectForBaselineTest(t, s, InventoryProjection{
		Conflicts: map[string][]string{"lease": {"backend-a", "backend-b"}},
	})
	require.NoError(t, err)
	conflict := s.Lookup("lease")
	assert.Equal(t, StateUnusable, conflict.State())
	assert.Equal(t, "backend-a", conflict.Attempt)
	assert.False(t, conflict.AttemptOperationID().Valid(),
		"conflict quarantine exposes no callback settlement authority")
	assert.Equal(t, opID, conflict.attemptOperationID,
		"conflict projection must retain the exact diagnostic operation identity")
	assert.Equal(t, fingerprint, conflict.attemptPayloadFingerprint)
	assert.Equal(t, testBackendRequestSnapshot(t), conflict.attemptRequestSnapshot)
	assert.Equal(t, testCallbackPair(opID), conflict.attemptCallbackPair)
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"}, conflict.ConflictBackends)

	_, err = projectForBaselineTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-b"},
	})
	require.NoError(t, err)
	observed := s.Lookup("lease")
	assert.Equal(t, StateUnusable, observed.State())
	assert.Empty(t, observed.Backend,
		"one later inventory report is not a conflict-repair capability")
	assert.Equal(t, "backend-a", observed.Attempt,
		"a different owner's observation cannot clear the pending attempt")
	assert.False(t, observed.AttemptOperationID().Valid(),
		"a quarantined attempt exposes no callback settlement authority")
	assert.Equal(t, opID, observed.attemptOperationID)
	assert.Equal(t, []string{"backend-a", "backend-b"}, observed.ConflictBackends)
}

func TestStore_CompleteProjectionKeepsPriorBaselineOnFailedRefresh(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	oversizedLease := strings.Repeat("x", bolt.MaxKeySize+1)

	_, err := projectForBaselineTest(t, s, InventoryProjection{
		Complete: true,
		Placements: map[string]string{
			"lease-written-before-failure": "backend-a",
			oversizedLease:                 "backend-a",
		},
	})
	require.Error(t, err)
	assert.Equal(t, baseline, s.CurrentAdmissionBaseline())
	assert.True(t, s.InventoryBootstrapped())
	assert.Equal(t, StateAbsent, s.Lookup("lease-written-before-failure").State(),
		"the placement mutation before the rejected key must roll back with metadata")
}

func TestStore_TopologyMetadataErrorClassification(t *testing.T) {
	// Keep the public errors usable with errors.Is after operation context is
	// added. This is a small regression guard for callers' fail-closed handling.
	s := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
	requireTestAdmission(t, s)
	requireTypedAttempt(t, s, "lease", "backend-b", requireOperationID(t, "9251"))
	err := configureBackendTopologyForTest(s, []string{"backend-a"})
	assert.True(t, errors.Is(err, ErrBackendTopologyInUse))
}
