package placement

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

func requireLifecycleID(t *testing.T, operationIDText string) lifecycle.ID {
	t.Helper()
	operationID := requireOperationID(t, operationIDText)
	id, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	return id
}

func lifecycleIDFromOperation(t testing.TB, operationID interface{ String() string }) lifecycle.ID {
	t.Helper()
	id, err := lifecycle.ParseID(operationID.String())
	require.NoError(t, err)
	return id
}

func requireLifecycleVerdict(
	t testing.TB,
	s *Store,
	leaseUUID string,
	id lifecycle.ID,
	want LifecycleVerdict,
) LifecycleAuthorization {
	t.Helper()
	result := s.AuthorizeLifecycle(leaseUUID, id)
	require.Equal(t, want, result.Verdict())
	return result
}

func requirePersistedUnusableLifecycle(
	t testing.TB,
	s *Store,
	leaseUUID string,
) {
	t.Helper()
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(leaseUUID))
		require.NotNil(t, encoded, "durable lifecycle quarantine must exist")
		capability, err := decodeLifecycleCapability(encoded)
		require.NoError(t, err)
		require.True(t, capability.unusable)
		require.False(t, capability.rawCorrupt)
		return nil
	}))
}

func TestStore_InitialLegacyOwnerAdoptsTokenlessThenRotatesOnExactOperation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease": []byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z"}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	assert.Equal(t, StateConfirmed, s.Lookup("lease").State())
	assert.NotZero(t, s.Lookup("lease").Revision(),
		"placement revision migration remains independent of lifecycle authority")
	legacy := requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictLegacy)
	assert.Equal(t, "backend-a", legacy.Backend())
	requireLifecycleVerdict(
		t, s, "lease", requireLifecycleID(t, "8101"), LifecycleVerdictStale,
	)
	assert.Equal(t, LifecycleVerdictLegacy, s.CurrentLifecycle("lease").Verdict())
	require.NoError(t, s.ConfigureBackendTopology([]string{"backend-a"}))
	requireAdmissionBaseline(t, s, "backend-a")

	operationID := requireOperationID(t, "8102")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictLegacy)
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictStale)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictAuthorized)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireLifecycleVerdict(
		t, reopened, "lease", id, LifecycleVerdictAuthorized,
	)
}

func TestStore_LegacyAdoptionEpochUsesOnlyRevisionEvidenceOrUnreadableJSON(t *testing.T) {
	tests := []struct {
		name                 string
		otherRecord          []byte
		wantGoodOwnerVerdict LifecycleVerdict
	}{
		{
			name:                 "unreadable JSON object disqualifies database",
			otherRecord:          []byte(`{"backend":`),
			wantGoodOwnerVerdict: LifecycleVerdictUnusable,
		},
		{
			name: "undecodable revision header disqualifies database",
			otherRecord: []byte(
				`{"backend":"backend-b","revision":"not-a-number"}`,
			),
			wantGoodOwnerVerdict: LifecycleVerdictUnusable,
		},
		{
			name: "nonzero revision disqualifies database",
			otherRecord: []byte(
				`{"backend":"backend-b","set_at":"2026-08-27T12:00:00Z","revision":7}`,
			),
			wantGoodOwnerVerdict: LifecycleVerdictUnusable,
		},
		{
			name:                 "invalid raw row remains lease local",
			otherRecord:          []byte{0xff},
			wantGoodOwnerVerdict: LifecycleVerdictLegacy,
		},
		{
			name: "semantically invalid revision-zero object remains lease local",
			otherRecord: []byte(
				`{"backend":"","set_at":"2026-08-27T12:00:00Z"}`,
			),
			wantGoodOwnerVerdict: LifecycleVerdictLegacy,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			writeRawRecords(t, dbPath, map[string][]byte{
				"good":  []byte("backend-a"),
				"other": test.otherRecord,
			})

			s, err := NewStore(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			requireLifecycleVerdict(
				t, s, "good", lifecycle.ID{}, test.wantGoodOwnerVerdict,
			)
			requireLifecycleVerdict(
				t, s, "other", lifecycle.ID{}, LifecycleVerdictUnusable,
			)
		})
	}
}

func TestStore_ExistingLifecycleBucketDoesNotBackfillDowngradeWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		if placements == nil {
			return errors.New("placements bucket missing")
		}
		if err := placements.Put([]byte("v013-owner"), []byte("backend-a")); err != nil {
			return err
		}
		return placements.Put(
			[]byte("revisioned-missing"),
			[]byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z","revision":9}`),
		)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(
		t, reopened, "v013-owner", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	assert.NotZero(t, reopened.Lookup("v013-owner").Revision())
	requireLifecycleVerdict(
		t, reopened, "revisioned-missing", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	requireLifecycleVerdict(
		t, reopened, "revisioned-missing", requireLifecycleID(t, "8110"),
		LifecycleVerdictUnusable,
	)
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(
		t, reopenedAgain, "v013-owner", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	requireLifecycleVerdict(
		t, reopenedAgain, "revisioned-missing", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
}

func TestStore_RecreatedLifecycleBucketCannotDowngradeTypedPlacements(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8111")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictAuthorized)
	require.NoError(t, s.Close())

	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(lifecycleCapabilityBucketName)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, StateConfirmed, reopened.Lookup("lease").State(),
		"placement routing remains available for operator recovery")
	requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictUnusable)
	requireLifecycleVerdict(
		t, reopened, "lease", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("lease")),
			"bucket recreation must not manufacture tokenless authority")
		return nil
	}))
}

func TestStore_RecreatedInitializationBucketsCannotAdoptMixedRevisionEpoch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")
	typedOperation := requireOperationID(t, "8112")
	typedID := lifecycleIDFromOperation(t, typedOperation)
	token := requireTypedAttempt(t, s, "typed", "backend-a", typedOperation)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	require.NoError(t, s.Close())

	// Simulate destructive bucket recreation beside a mixed placement epoch.
	// Even though both post-v0.13 buckets are now absent, the revisioned typed
	// row proves the database has already crossed the upgrade boundary, so the
	// injected revision-zero row cannot acquire tokenless authority.
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(lifecycleCapabilityBucketName); err != nil {
			return err
		}
		if err := tx.DeleteBucket(metadataBucketName); err != nil {
			return err
		}
		return tx.Bucket(bucketName).Put(
			[]byte("revision-zero"),
			[]byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z"}`),
		)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireLifecycleVerdict(t, reopened, "typed", typedID, LifecycleVerdictUnusable)
	requireLifecycleVerdict(
		t, reopened, "revision-zero", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	assert.NotZero(t, reopened.Lookup("revision-zero").Revision(),
		"placement revision migration remains independent of denied lifecycle adoption")
	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("revision-zero")))
		return nil
	}))
}

func TestStore_DeletedLifecycleBucketCannotAdoptLaterRevisionZeroWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	// A current store has already durably recorded its initialization epoch in
	// placement_metadata. Deleting only the lifecycle bucket and then writing a
	// v0.13-shaped row must not resurrect first-upgrade adoption.
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(metadataBucketName) == nil {
			return errors.New("placement metadata bucket missing")
		}
		if err := tx.DeleteBucket(lifecycleCapabilityBucketName); err != nil {
			return err
		}
		return tx.Bucket(bucketName).Put(
			[]byte("simulated-v013"),
			[]byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z"}`),
		)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireLifecycleVerdict(
		t, reopened, "simulated-v013", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	assert.NotZero(t, reopened.Lookup("simulated-v013").Revision())
	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("simulated-v013")))
		return nil
	}))
}

func TestStore_LifecycleAttemptPromotionRotationAndReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	scope := requireAdmissionScope(t, s, baseline, "backend-a")

	firstOperation := requireOperationID(t, "8201")
	firstID := lifecycleIDFromOperation(t, firstOperation)
	first, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-a", firstOperation,
	)
	require.NoError(t, err)
	require.True(t, applied)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictMissing)

	confirmed, err := s.ConfirmAttempt(first)
	require.NoError(t, err)
	require.True(t, confirmed)
	firstAuthorization := requireLifecycleVerdict(
		t, s, "lease", firstID, LifecycleVerdictAuthorized,
	)
	assert.Equal(t, "backend-a", firstAuthorization.Backend())
	assert.Equal(t, firstID, s.CurrentLifecycle("lease").ID())
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.ConfigureBackendTopology([]string{"backend-a"}))
	requireLifecycleVerdict(t, reopened, "lease", firstID, LifecycleVerdictAuthorized)

	secondOperation := requireOperationID(t, "8202")
	secondID := lifecycleIDFromOperation(t, secondOperation)
	current := reopened.Lookup("lease")
	second, applied, err := reopened.BeginOwnedAttempt(
		reopened.CurrentAdmissionBaseline(), current.RecordRevision(), "backend-a", secondOperation,
	)
	require.NoError(t, err)
	require.True(t, applied)
	requireLifecycleVerdict(t, reopened, "lease", firstID, LifecycleVerdictAuthorized)
	requireLifecycleVerdict(t, reopened, "lease", secondID, LifecycleVerdictStale)

	confirmed, err = reopened.ConfirmAttempt(second)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, reopened, "lease", firstID, LifecycleVerdictStale)
	requireLifecycleVerdict(t, reopened, "lease", secondID, LifecycleVerdictAuthorized)
}

func TestStore_PendingLifecycleAttemptSurvivesReopenAndPromotesExactly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	scope := requireAdmissionScope(t, s, baseline, "backend-a")
	operationID := requireOperationID(t, "8251")
	id := lifecycleIDFromOperation(t, operationID)
	_, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-a", operationID,
	)
	require.NoError(t, err)
	require.True(t, applied)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.NoError(t, reopened.ConfigureBackendTopology([]string{"backend-a"}))
	requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictMissing)
	confirmed, err := reopened.ConfirmOperation("lease", "backend-a", operationID)
	require.NoError(t, err)
	require.True(t, confirmed)
	result := requireLifecycleVerdict(
		t, reopened, "lease", id, LifecycleVerdictAuthorized,
	)
	assert.Equal(t, "backend-a", result.Backend())
}

func TestStore_ConfirmOperationPromotesAttemptAndRotatesMaintenance(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	firstOperation := requireOperationID(t, "8301")
	firstID := lifecycleIDFromOperation(t, firstOperation)
	first := requireTypedAttempt(t, s, "lease", "backend-a", firstOperation)
	confirmed, err := s.ConfirmAttempt(first)
	require.NoError(t, err)
	require.True(t, confirmed)

	secondOperation := requireOperationID(t, "8302")
	secondID := lifecycleIDFromOperation(t, secondOperation)
	requireTypedAttempt(t, s, "lease", "backend-a", secondOperation)
	confirmed, err = s.ConfirmOperation("lease", "backend-a", secondOperation)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictAuthorized)

	placementRevision := s.Lookup("lease").Revision()
	maintenanceOperation := requireOperationID(t, "8303")
	maintenanceID := lifecycleIDFromOperation(t, maintenanceOperation)
	confirmed, err = s.ConfirmOperation("lease", "backend-a", maintenanceOperation)
	require.NoError(t, err)
	require.True(t, confirmed)
	assert.Equal(t, placementRevision, s.Lookup("lease").Revision(),
		"maintenance rotation must not manufacture a placement mutation")
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictStale)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictStale)
	requireLifecycleVerdict(t, s, "lease", maintenanceID, LifecycleVerdictAuthorized)
}

func TestStore_RestoreLifecyclePromotionAndRefusal(t *testing.T) {
	s := newRestoreTestStore(t)
	operationID := requireOperationID(t, "8401")
	id := lifecycleIDFromOperation(t, operationID)
	claim, err := s.BeginRestore(
		s.CurrentAdmissionBaseline(), "source", "target", operationID,
	)
	require.NoError(t, err)
	requireLifecycleVerdict(t, s, "target", id, LifecycleVerdictMissing)

	confirmed, err := s.ConfirmRestore(claim)
	require.NoError(t, err)
	require.True(t, confirmed)
	authorized := requireLifecycleVerdict(
		t, s, "target", id, LifecycleVerdictAuthorized,
	)
	assert.Equal(t, "backend-a", authorized.Backend())

	refusedOperation := requireOperationID(t, "8402")
	refusedID := lifecycleIDFromOperation(t, refusedOperation)
	refusedClaim, err := s.BeginRestore(
		s.CurrentAdmissionBaseline(), "source", "refused-target", refusedOperation,
	)
	require.NoError(t, err)
	refused, err := s.RefuseRestore(refusedClaim)
	require.NoError(t, err)
	require.True(t, refused)
	assert.Equal(t, StateAbsent, s.Lookup("refused-target").State())
	requireLifecycleVerdict(t, s, "refused-target", refusedID, LifecycleVerdictMissing)
}

func TestStore_InventoryPromotesMarkedAttemptButCannotRepairMissingCapability(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	typedOperation := requireOperationID(t, "8501")
	typedID := lifecycleIDFromOperation(t, typedOperation)
	requireTypedAttempt(t, s, "typed", "backend-a", typedOperation)
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"typed": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "typed", typedID, LifecycleVerdictAuthorized)

	dbPath := filepath.Join(t.TempDir(), "missing-capability.db")
	missingOperation := requireOperationID(t, "8502")
	missingID := lifecycleIDFromOperation(t, missingOperation)
	writeRawRecords(t, dbPath, map[string][]byte{
		"missing-capability": []byte(`{"attempt":"backend-a","operation_id":"` +
			missingOperation.String() + `","set_at":"2026-08-27T12:00:00Z","revision":1}`),
	})
	missingStore, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = missingStore.Close() })
	require.NoError(t, missingStore.ConfigureBackendTopology([]string{"backend-a"}))
	fence := missingStore.BeginInventorySession()
	_, err = missingStore.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"missing-capability": "backend-a"},
	})
	missingStore.EndInventorySession(fence)
	require.NoError(t, err)
	requireLifecycleVerdict(
		t, missingStore, "missing-capability", lifecycle.ID{}, LifecycleVerdictUnusable,
	)
	requireLifecycleVerdict(
		t, missingStore, "missing-capability", missingID, LifecycleVerdictUnusable,
	)
	requirePersistedUnusableLifecycle(t, missingStore, "missing-capability")
}

func TestStore_InventoryPersistsAttemptMarkerMismatchQuarantineAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")
	oldOperation := requireOperationID(t, "8511")
	oldID := lifecycleIDFromOperation(t, oldOperation)
	oldAttempt := requireTypedAttempt(t, s, "lease", "backend-a", oldOperation)
	confirmed, err := s.ConfirmAttempt(oldAttempt)
	require.NoError(t, err)
	require.True(t, confirmed)

	newOperation := requireOperationID(t, "8512")
	newID := lifecycleIDFromOperation(t, newOperation)
	requireTypedAttempt(t, s, "lease", "backend-a", newOperation)
	require.NoError(t, s.Close())

	// Replace the durable row with an older, valid capability that omits the
	// newer attempt marker. On load it is a decodable binding mismatch, not raw
	// corruption, and therefore can be replaced by explicit fail-closed state.
	oldEncoded, err := encodeLifecycleCapability(lifecycleCapability{
		backend: "backend-a",
		id:      oldID,
	})
	require.NoError(t, err)
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte("lease"), oldEncoded)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(t, reopened, "lease", oldID, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, reopened, "lease", newID, LifecycleVerdictUnusable)
	projectInventoryForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	assert.Empty(t, reopened.Lookup("lease").Attempt,
		"positive inventory should still settle the placement attempt")
	requirePersistedUnusableLifecycle(t, reopened, "lease")
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(t, reopenedAgain, "lease", oldID, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, reopenedAgain, "lease", newID, LifecycleVerdictUnusable)
	requirePersistedUnusableLifecycle(t, reopenedAgain, "lease")
}

func TestStore_InventoryPersistsBackendBindingMismatchQuarantineAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	operationID := requireOperationID(t, "8513")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	require.NoError(t, s.Close())

	// This valid row initially conflicts with the backend-a placement. If its
	// cache-only quarantine were lost while inventory moved placement to
	// backend-b, reopening would incorrectly authorize this stale ID.
	mismatchedEncoded, err := encodeLifecycleCapability(lifecycleCapability{
		backend: "backend-b",
		id:      id,
	})
	require.NoError(t, err)
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Put(
			[]byte("lease"), mismatchedEncoded,
		)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictUnusable)
	projectInventoryForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{"lease": "backend-b"},
	})
	assert.Equal(t, "backend-b", reopened.Lookup("lease").Backend)
	requirePersistedUnusableLifecycle(t, reopened, "lease")
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(t, reopenedAgain, "lease", id, LifecycleVerdictUnusable)
	requirePersistedUnusableLifecycle(t, reopenedAgain, "lease")
}

func TestStore_ConflictBackendMismatchQuarantinePersistsAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	operationID := requireOperationID(t, "8515")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireConflictPlacement(t, s, "lease", "backend-a", "backend-b")
	conflict := s.Lookup("lease")
	require.Equal(t, StateUnusable, conflict.State())
	require.Equal(t, "backend-a", conflict.Backend)
	require.NoError(t, s.Close())

	// Inject a well-formed capability whose owner contradicts the non-empty
	// Backend retained by the conflict. Load must quarantine this binding even
	// though the placement is already unusable for an independent reason.
	mismatchedEncoded, err := encodeLifecycleCapability(lifecycleCapability{
		backend: "backend-b",
		id:      id,
	})
	require.NoError(t, err)
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Put(
			[]byte("lease"), mismatchedEncoded,
		)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	require.True(t, reopened.lifecycleCache["lease"].unusable,
		"a conflict must not hide a mismatched lifecycle owner")
	projectInventoryForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{"lease": "backend-b"},
	})
	require.Equal(t, StateConfirmed, reopened.Lookup("lease").State())
	requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictUnusable)
	requirePersistedUnusableLifecycle(t, reopened, "lease")
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(t, reopenedAgain, "lease", id, LifecycleVerdictUnusable)
	requirePersistedUnusableLifecycle(t, reopenedAgain, "lease")
}

func TestStore_InventoryPreservesRawCorruptLifecycleEvidence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	operationID := requireOperationID(t, "8514")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	require.NoError(t, s.Close())

	rawCorrupt := []byte(`{"backend":`)
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte("lease"), rawCorrupt)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictUnusable)
	projectInventoryForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{"lease": "backend-b"},
	})
	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, rawCorrupt,
			tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("lease")),
			"inventory must not erase undecodable operator evidence")
		return nil
	}))
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(t, reopenedAgain, "lease", id, LifecycleVerdictUnusable)
}

func TestStore_ExactOperationRepairsRawCorruptionAfterInventoryClearsAttempt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")

	oldOperationID := requireOperationID(t, "8515")
	oldID := lifecycleIDFromOperation(t, oldOperationID)
	oldToken := requireTypedAttempt(t, s, "lease", "backend-a", oldOperationID)
	confirmed, err := s.ConfirmAttempt(oldToken)
	require.NoError(t, err)
	require.True(t, confirmed)

	newOperationID := requireOperationID(t, "8516")
	newID := lifecycleIDFromOperation(t, newOperationID)
	requireTypedAttempt(t, s, "lease", "backend-a", newOperationID)
	assert.Equal(t, "backend-a", s.Lookup("lease").Attempt)
	require.NoError(t, s.Close())

	rawCorrupt := []byte(`{"backend":`)
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte("lease"), rawCorrupt)
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(t, reopened, "lease", oldID, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, reopened, "lease", newID, LifecycleVerdictUnusable)
	projectInventoryForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	assert.Empty(t, reopened.Lookup("lease").Attempt,
		"positive inventory settles the matching placement attempt")
	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, rawCorrupt,
			tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("lease")),
			"inventory must preserve raw evidence until exact settlement")
		return nil
	}))

	repaired, err := reopened.ConfirmOperation("lease", "backend-a", newOperationID)
	require.NoError(t, err)
	require.True(t, repaired)
	requireLifecycleVerdict(t, reopened, "lease", newID, LifecycleVerdictAuthorized)
	requireLifecycleVerdict(t, reopened, "lease", oldID, LifecycleVerdictStale)
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(t, reopenedAgain, "lease", newID, LifecycleVerdictAuthorized)
	requireLifecycleVerdict(t, reopenedAgain, "lease", oldID, LifecycleVerdictStale)
}

func TestStore_LifecycleCapabilitySurvivesPlacementDeleteAndPrunesOnExactRetirement(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8601")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)

	requireDeleteRecord(t, s, "lease")
	assert.Equal(t, StateAbsent, s.Lookup("lease").State())
	teardown := requireLifecycleVerdict(
		t, s, "lease", id, LifecycleVerdictTeardownOnly,
	)
	assert.Equal(t, "backend-a", teardown.Backend())
	assert.Equal(t, LifecycleVerdictTeardownOnly, s.CurrentLifecycle("lease").Verdict())
	assert.False(t, s.CurrentLifecycle("lease").ID().Valid(),
		"teardown-only authority must not be reissued for maintenance")
	staleID := requireLifecycleID(t, "8602")
	requireLifecycleVerdict(t, s, "lease", staleID, LifecycleVerdictStale)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictTeardownOnly)

	retired, err := reopened.RetireLifecycle("lease", id)
	require.NoError(t, err)
	assert.True(t, retired.Retired())
	assert.True(t, retired.RetiredNow())
	assert.Equal(t, "backend-a", retired.Backend())
	assert.Equal(t, LifecycleVerdictMissing, reopened.CurrentLifecycle("lease").Verdict())
	retired, err = reopened.RetireLifecycle("lease", id)
	require.NoError(t, err)
	assert.Equal(t, LifecycleVerdictMissing, retired.Verdict())
	assert.False(t, retired.RetiredNow(), "a duplicate cannot cross the durable delete boundary")
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(t, reopenedAgain, "lease", id, LifecycleVerdictMissing)
}

func TestStore_InventoryRecreationPreservesRetainedTypedLifecycle(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8621")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)

	requireDeleteRecord(t, s, "lease")
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictTeardownOnly)
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictAuthorized)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictStale)

	retired, err := s.RetireLifecycle("lease", id)
	require.NoError(t, err)
	require.True(t, retired.Retired())
	requireDeleteRecord(t, s, "lease")
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictMissing)
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictUnusable)
}

func TestStore_ConflictResolutionPreservesMatchingRetainedTypedLifecycle(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	operationID := requireOperationID(t, "8622")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)

	// Recreate quarantine after the placement was pruned. This leaves the
	// conflict with no single Backend field while the typed lifecycle record is
	// deliberately retained out-of-line.
	requireDeleteRecord(t, s, "lease")
	requireConflictPlacement(t, s, "lease", "backend-a", "backend-b")
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictUnusable)

	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictAuthorized)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictStale)
}

func TestStore_ConflictResolutionAfterReopenPreservesOnlyMatchingTypedLifecycle(t *testing.T) {
	tests := []struct {
		name            string
		resolvedBackend string
		wantVerdict     LifecycleVerdict
		wantQuarantined bool
	}{
		{
			name:            "matching owner",
			resolvedBackend: "backend-a",
			wantVerdict:     LifecycleVerdictAuthorized,
		},
		{
			name:            "different owner",
			resolvedBackend: "backend-b",
			wantVerdict:     LifecycleVerdictUnusable,
			wantQuarantined: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			s, err := NewStore(dbPath)
			require.NoError(t, err)
			requireAdmissionBaseline(t, s, "backend-a", "backend-b")
			operationID := requireOperationID(t, "8623")
			id := lifecycleIDFromOperation(t, operationID)
			token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
			confirmed, err := s.ConfirmAttempt(token)
			require.NoError(t, err)
			require.True(t, confirmed)
			requireConflictPlacement(t, s, "lease", "backend-a", "backend-b")
			requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictUnusable)
			require.NoError(t, s.Close())

			reopened, err := NewStore(dbPath)
			require.NoError(t, err)
			placement := reopened.Lookup("lease")
			require.Equal(t, StateUnusable, placement.State())
			require.True(t, placement.Conflict)
			requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictUnusable)
			capability := reopened.lifecycleCache["lease"]
			assert.False(t, capability.unusable,
				"placement quarantine must gate, not discard, valid lifecycle authority")
			assert.Equal(t, "backend-a", capability.backend)
			assert.Equal(t, id, capability.id)

			projectInventoryForTest(t, reopened, InventoryProjection{
				Placements: map[string]string{"lease": test.resolvedBackend},
			})
			placement = reopened.Lookup("lease")
			require.Equal(t, StateConfirmed, placement.State())
			require.Equal(t, test.resolvedBackend, placement.Backend)
			requireLifecycleVerdict(t, reopened, "lease", id, test.wantVerdict)
			if test.wantQuarantined {
				requirePersistedUnusableLifecycle(t, reopened, "lease")
			} else {
				persisted := reopened.lifecycleCache["lease"]
				assert.False(t, persisted.unusable)
				assert.Equal(t, "backend-a", persisted.backend)
				assert.Equal(t, id, persisted.id)
			}
			require.NoError(t, reopened.Close())

			reopenedAgain, err := NewStore(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopenedAgain.Close() })
			requireLifecycleVerdict(
				t, reopenedAgain, "lease", id, test.wantVerdict,
			)
			if test.wantQuarantined {
				requirePersistedUnusableLifecycle(t, reopenedAgain, "lease")
			}
		})
	}
}

func TestStore_MatchingInventoryRepairsUnusablePlacementWithoutLosingTypedLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		record       []byte
		wantRevision uint64
		wantSetAt    bool
	}{
		{
			name:   "malformed JSON",
			record: []byte(`{"backend":`),
		},
		{
			name: "structurally empty",
			record: []byte(
				`{"backend":"","set_at":"2026-08-28T12:00:00Z","revision":7}`,
			),
			wantRevision: 7,
			wantSetAt:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			s, err := NewStore(dbPath)
			require.NoError(t, err)
			requireAdmissionBaseline(t, s, "backend-a")
			operationID := requireOperationID(t, "8624")
			id := lifecycleIDFromOperation(t, operationID)
			token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
			confirmed, err := s.ConfirmAttempt(token)
			require.NoError(t, err)
			require.True(t, confirmed)
			require.NoError(t, s.Close())

			db, err := bolt.Open(dbPath, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte("lease"), test.record)
			}))
			require.NoError(t, db.Close())

			reopened, err := NewStore(dbPath)
			require.NoError(t, err)
			placement := reopened.Lookup("lease")
			require.Equal(t, StateUnusable, placement.State())
			assert.Equal(t, test.wantRevision, placement.Revision())
			assert.Equal(t, test.wantSetAt, !placement.SetAt.IsZero())
			requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictUnusable)
			capability := reopened.lifecycleCache["lease"]
			assert.False(t, capability.unusable,
				"unusable placement must not flatten an independently valid capability")
			assert.Equal(t, "backend-a", capability.backend)
			assert.Equal(t, id, capability.id)

			projectInventoryForTest(t, reopened, InventoryProjection{
				Placements: map[string]string{"lease": "backend-a"},
			})
			requireLifecycleVerdict(t, reopened, "lease", id, LifecycleVerdictAuthorized)
			require.NoError(t, reopened.Close())

			reopenedAgain, err := NewStore(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopenedAgain.Close() })
			requireLifecycleVerdict(
				t, reopenedAgain, "lease", id, LifecycleVerdictAuthorized,
			)
		})
	}
}

func TestStore_UnreadablePlacementWithOutstandingAttemptRemainsFailClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")

	currentOperation := requireOperationID(t, "8625")
	currentID := lifecycleIDFromOperation(t, currentOperation)
	current := requireTypedAttempt(t, s, "lease", "backend-a", currentOperation)
	confirmed, err := s.ConfirmAttempt(current)
	require.NoError(t, err)
	require.True(t, confirmed)

	pendingOperation := requireOperationID(t, "8626")
	pendingID := lifecycleIDFromOperation(t, pendingOperation)
	requireTypedAttempt(t, s, "lease", "backend-a", pendingOperation)
	require.NoError(t, s.Close())

	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte("lease"), []byte(`{"backend":`))
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	require.Equal(t, StateUnusable, reopened.Lookup("lease").State())
	requireLifecycleVerdict(t, reopened, "lease", currentID, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, reopened, "lease", pendingID, LifecycleVerdictUnusable)
	assert.True(t, reopened.lifecycleCache["lease"].unusable,
		"the lost placement attempt must quarantine the in-memory binding")
	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		encoded := tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("lease"))
		capability, decodeErr := decodeLifecycleCapability(encoded)
		require.NoError(t, decodeErr)
		assert.False(t, capability.unusable,
			"opening must not overwrite recoverable durable evidence")
		assert.Equal(t, "backend-a", capability.backend)
		assert.Equal(t, currentID, capability.id)
		assert.Equal(t, "backend-a", capability.attemptBackend)
		assert.Equal(t, pendingID, capability.attemptID)
		return nil
	}))

	// The unreadable placement erased the exact attempt pairing. A positive
	// owner observation can repair routing, but cannot prove which lifecycle ID
	// the backend received, so passive repair must persist fail-closed authority.
	projectInventoryForTest(t, reopened, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	require.Equal(t, StateConfirmed, reopened.Lookup("lease").State())
	requireLifecycleVerdict(t, reopened, "lease", currentID, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, reopened, "lease", pendingID, LifecycleVerdictUnusable)
	requirePersistedUnusableLifecycle(t, reopened, "lease")
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(
		t, reopenedAgain, "lease", currentID, LifecycleVerdictUnusable,
	)
	requireLifecycleVerdict(
		t, reopenedAgain, "lease", pendingID, LifecycleVerdictUnusable,
	)
}

func TestStore_RefusedRecreationRetainsPriorLifecycleWithoutWedge(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	firstOperation := requireOperationID(t, "8631")
	firstID := lifecycleIDFromOperation(t, firstOperation)
	first := requireTypedAttempt(t, s, "lease", "backend-a", firstOperation)
	confirmed, err := s.ConfirmAttempt(first)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireDeleteRecord(t, s, "lease")

	second := requireTypedAttempt(
		t, s, "lease", "backend-a", requireOperationID(t, "8632"),
	)
	assert.Equal(t, StateAttempting, s.Lookup("lease").State())
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictTeardownOnly)
	assert.Equal(t, LifecycleVerdictTeardownOnly, s.CurrentLifecycle("lease").Verdict(),
		"an attempt-only record must not restore runtime authority to the retained owner")
	refused, err := s.RefuseAttempt(second)
	require.NoError(t, err)
	require.True(t, refused)
	assert.Equal(t, StateAbsent, s.Lookup("lease").State())
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictTeardownOnly)
}

func TestStore_DetachedRetirementPreservesConcurrentNewAttemptMarker(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	firstOperation := requireOperationID(t, "8634")
	firstID := lifecycleIDFromOperation(t, firstOperation)
	first := requireTypedAttempt(t, s, "lease", "backend-a", firstOperation)
	confirmed, err := s.ConfirmAttempt(first)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireDeleteRecord(t, s, "lease")

	secondOperation := requireOperationID(t, "8635")
	secondID := lifecycleIDFromOperation(t, secondOperation)
	second := requireTypedAttempt(t, s, "lease", "backend-a", secondOperation)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictTeardownOnly)

	retired, err := s.RetireLifecycle("lease", firstID)
	require.NoError(t, err)
	require.True(t, retired.RetiredNow())
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictRetired)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictStale)

	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictStale)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictAuthorized)
}

func TestStore_ReopenPrunesDetachedRetiredButRetainsOutstandingTeardown(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")

	activeOperation := requireOperationID(t, "8636")
	activeID := lifecycleIDFromOperation(t, activeOperation)
	active := requireTypedAttempt(t, s, "active", "backend-a", activeOperation)
	confirmed, err := s.ConfirmAttempt(active)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireDeleteRecord(t, s, "active")

	retiredOperation := requireOperationID(t, "8637")
	retiredID := lifecycleIDFromOperation(t, retiredOperation)
	retiredToken := requireTypedAttempt(t, s, "retired", "backend-a", retiredOperation)
	confirmed, err = s.ConfirmAttempt(retiredToken)
	require.NoError(t, err)
	require.True(t, confirmed)
	retired, err := s.RetireLifecycle("retired", retiredID)
	require.NoError(t, err)
	require.True(t, retired.RetiredNow())
	require.NoError(t, s.Close())

	// Reproduce the older store's leaked completed history: placement vanished,
	// but its already-consumed lifecycle row remained.
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte("retired"))
	}))
	require.NoError(t, db.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireLifecycleVerdict(t, reopened, "active", activeID, LifecycleVerdictTeardownOnly)
	requireLifecycleVerdict(t, reopened, "retired", retiredID, LifecycleVerdictMissing)

	require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(lifecycleCapabilityBucketName)
		assert.NotNil(t, bucket.Get([]byte("active")))
		assert.Nil(t, bucket.Get([]byte("retired")))
		return nil
	}))
}

func TestStore_ZeroIDCannotConsumeTypedTeardownCapability(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8633")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)

	requireDeleteRecord(t, s, "lease")
	teardown := requireLifecycleVerdict(
		t, s, "lease", id, LifecycleVerdictTeardownOnly,
	)
	assert.Equal(t, "backend-a", teardown.Backend())
	requireLifecycleVerdict(
		t, s, "lease", lifecycle.ID{}, LifecycleVerdictStale,
	)
	assert.Equal(t, LifecycleVerdictTeardownOnly, s.CurrentLifecycle("lease").Verdict())

	retired, err := s.RetireLifecycle("lease", lifecycle.ID{})
	require.NoError(t, err)
	assert.Equal(t, LifecycleVerdictStale, retired.Verdict())
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictTeardownOnly)
}

func TestStore_DetachedLegacyTeardownRetirementDeletesCapability(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"legacy": []byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z"}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(t, s, "legacy", lifecycle.ID{}, LifecycleVerdictLegacy)
	requireDeleteRecord(t, s, "legacy")
	teardown := requireLifecycleVerdict(
		t, s, "legacy", lifecycle.ID{}, LifecycleVerdictTeardownOnly,
	)
	assert.Equal(t, "backend-a", teardown.Backend())
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	requireLifecycleVerdict(
		t, reopened, "legacy", lifecycle.ID{}, LifecycleVerdictTeardownOnly,
	)
	retired, err := reopened.RetireLifecycle("legacy", lifecycle.ID{})
	require.NoError(t, err)
	require.True(t, retired.RetiredNow())
	assert.Equal(t, "backend-a", retired.Backend())
	requireLifecycleVerdict(t, reopened, "legacy", lifecycle.ID{}, LifecycleVerdictMissing)
	require.NoError(t, reopened.Close())

	reopenedAgain, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedAgain.Close() })
	requireLifecycleVerdict(
		t, reopenedAgain, "legacy", lifecycle.ID{}, LifecycleVerdictMissing,
	)
}

func TestStore_NewAttemptSupersedesRetiredCapabilityAfterPlacementDeletion(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	firstOperation := requireOperationID(t, "8641")
	firstID := lifecycleIDFromOperation(t, firstOperation)
	first := requireTypedAttempt(t, s, "lease", "backend-a", firstOperation)
	confirmed, err := s.ConfirmAttempt(first)
	require.NoError(t, err)
	require.True(t, confirmed)
	retired, err := s.RetireLifecycle("lease", firstID)
	require.NoError(t, err)
	require.True(t, retired.Retired())
	requireDeleteRecord(t, s, "lease")
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictMissing)

	secondOperation := requireOperationID(t, "8642")
	secondID := lifecycleIDFromOperation(t, secondOperation)
	second := requireTypedAttempt(t, s, "lease", "backend-a", secondOperation)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictMissing)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictMissing)
	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictStale)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictAuthorized)
}

func TestStore_LifecycleCapabilityWithdrawsAuthorityDuringPlacementConflict(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")
	operationID := requireOperationID(t, "8651")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictAuthorized)

	requireConflictPlacement(t, s, "lease", "backend-a", "backend-b")
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictUnusable)
	assert.Equal(t, LifecycleVerdictUnusable, s.CurrentLifecycle("lease").Verdict())

	retired, err := s.RetireLifecycle("lease", id)
	require.NoError(t, err)
	assert.Equal(t, LifecycleVerdictUnusable, retired.Verdict())
	assert.False(t, s.lifecycleCache["lease"].retired,
		"a conflicted backend cannot consume the capability before repair")

	requireDeleteRecord(t, s, "lease")
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictMissing)
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictUnusable)
}

func TestStore_LifecyclePromotionIsAtomicWithPlacement(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8701")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	before := s.Lookup("lease")

	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(lifecycleCapabilityBucketName)
	}))
	confirmed, err := s.ConfirmAttempt(token)
	require.Error(t, err)
	assert.False(t, confirmed)
	after := s.Lookup("lease")
	assert.Equal(t, before.Revision(), after.Revision())
	assert.Equal(t, before.Attempt, after.Attempt)
	assert.Equal(t, StateAttempting, after.State())
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictMissing)

	var persisted Placement
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		persisted = decodeRecord("lease", tx.Bucket(bucketName).Get([]byte("lease")))
		return nil
	}))
	assert.Equal(t, before.Revision(), persisted.Revision())
	assert.Equal(t, before.Attempt, persisted.Attempt)
}

func TestStore_LifecycleAttemptCreationIsAtomicWithPlacement(t *testing.T) {
	s := newTestStore(t)
	baseline := requireAdmissionBaseline(t, s, "backend-a")
	scope := requireAdmissionScope(t, s, baseline, "backend-a")
	operationID := requireOperationID(t, "8751")

	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(lifecycleCapabilityBucketName)
	}))
	token, applied, err := s.BeginNewAttempt(
		scope, "lease", "backend-a", operationID,
	)
	require.Error(t, err)
	assert.False(t, applied)
	assert.False(t, token.Valid())
	assert.Equal(t, StateAbsent, s.Lookup("lease").State())

	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(bucketName).Get([]byte("lease")))
		return nil
	}))
}

func TestStore_LifecycleRetireRacesAreSerialized(t *testing.T) {
	s := newTestStore(t, WithClock(func() time.Time {
		return time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	}))
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8801")
	id := lifecycleIDFromOperation(t, operationID)
	token := requireTypedAttempt(t, s, "lease", "backend-a", operationID)
	confirmed, err := s.ConfirmAttempt(token)
	require.NoError(t, err)
	require.True(t, confirmed)

	results := make(chan LifecycleAuthorization, 2)
	errors := make(chan error, 2)
	for range 2 {
		go func() {
			result, retireErr := s.RetireLifecycle("lease", id)
			results <- result
			errors <- retireErr
		}()
	}
	for range 2 {
		require.NoError(t, <-errors)
		assert.True(t, (<-results).Retired())
	}
}

func TestStore_RetiringCurrentLifecyclePreservesNewAttemptMarker(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	firstOperation := requireOperationID(t, "8901")
	firstID := lifecycleIDFromOperation(t, firstOperation)
	first := requireTypedAttempt(t, s, "lease", "backend-a", firstOperation)
	confirmed, err := s.ConfirmAttempt(first)
	require.NoError(t, err)
	require.True(t, confirmed)

	secondOperation := requireOperationID(t, "8902")
	secondID := lifecycleIDFromOperation(t, secondOperation)
	second := requireTypedAttempt(t, s, "lease", "backend-a", secondOperation)
	retired, err := s.RetireLifecycle("lease", firstID)
	require.NoError(t, err)
	require.True(t, retired.Retired())

	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictStale)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictAuthorized)
}

func TestStore_MalformedLifecycleCapabilitiesAreIsolatedPerLeaseOnReopen(t *testing.T) {
	tests := []struct {
		name       string
		capability []byte
	}{
		{
			name:       "malformed JSON",
			capability: []byte(`{"backend":`),
		},
		{
			name: "non-canonical typed ID",
			capability: []byte(`{"backend":"backend-a",` +
				`"id":"00000000-0000-4000-8000-0000000022C5"}`),
		},
		{
			name: "attempt marker without placement attempt",
			capability: []byte(`{"backend":"backend-a",` +
				`"attempt_backend":"backend-a",` +
				`"attempt_id":"00000000-0000-4000-8000-0000000022c5"}`),
		},
		{
			name:       "backend differs from placement",
			capability: []byte(`{"backend":"backend-b"}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			s, err := NewStore(dbPath)
			require.NoError(t, err)
			requireAdmissionBaseline(t, s, "backend-a")
			healthyOperation := requireOperationID(t, "8951")
			healthyID := lifecycleIDFromOperation(t, healthyOperation)
			healthy := requireTypedAttempt(
				t, s, "healthy", "backend-a", healthyOperation,
			)
			confirmed, confirmErr := s.ConfirmAttempt(healthy)
			require.NoError(t, confirmErr)
			require.True(t, confirmed)
			requireConfirmedPlacement(t, s, "lease", "backend-a")
			require.NoError(t, s.Close())

			db, err := bolt.Open(dbPath, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(lifecycleCapabilityBucketName)
				if bucket == nil {
					return errors.New("lifecycle capability bucket missing")
				}
				return bucket.Put([]byte("lease"), test.capability)
			}))
			require.NoError(t, db.Close())

			reopened, err := NewStore(dbPath)
			require.NoError(t, err)
			requireLifecycleVerdict(
				t, reopened, "healthy", healthyID, LifecycleVerdictAuthorized,
			)
			requireLifecycleVerdict(
				t, reopened, "lease", lifecycle.ID{}, LifecycleVerdictUnusable,
			)
			requireLifecycleVerdict(
				t, reopened, "lease", requireLifecycleID(t, "8952"),
				LifecycleVerdictUnusable,
			)
			require.NoError(t, reopened.Close())

			db, err = bolt.Open(dbPath, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				got := tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("lease"))
				assert.Equal(t, test.capability, got,
					"opening must preserve corrupt authority evidence byte-for-byte")
				return nil
			}))
			require.NoError(t, db.Close())
		})
	}
}

func TestStore_RevisionedConfirmedMissingCapabilityIsIsolatedAndNeverBackfilled(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, s, "backend-a")

	healthyOperation := requireOperationID(t, "8961")
	healthyID := lifecycleIDFromOperation(t, healthyOperation)
	healthy := requireTypedAttempt(t, s, "healthy", "backend-a", healthyOperation)
	confirmed, err := s.ConfirmAttempt(healthy)
	require.NoError(t, err)
	require.True(t, confirmed)

	missing := requireTypedAttempt(
		t, s, "missing", "backend-a", requireOperationID(t, "8962"),
	)
	confirmed, err = s.ConfirmAttempt(missing)
	require.NoError(t, err)
	require.True(t, confirmed)
	require.NoError(t, s.Close())

	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(lifecycleCapabilityBucketName).Delete([]byte("missing"))
	}))
	require.NoError(t, db.Close())

	for range 2 {
		reopened, openErr := NewStore(dbPath)
		require.NoError(t, openErr)
		requireLifecycleVerdict(
			t, reopened, "healthy", healthyID, LifecycleVerdictAuthorized,
		)
		requireLifecycleVerdict(
			t, reopened, "missing", lifecycle.ID{}, LifecycleVerdictUnusable,
		)
		requireLifecycleVerdict(
			t, reopened, "missing", requireLifecycleID(t, "8963"),
			LifecycleVerdictUnusable,
		)
		require.NoError(t, reopened.db.View(func(tx *bolt.Tx) error {
			assert.Nil(t, tx.Bucket(lifecycleCapabilityBucketName).Get([]byte("missing")),
				"revisioned missing authority must never be recreated as legacy")
			return nil
		}))
		require.NoError(t, reopened.Close())
	}

	repair, err := NewStore(dbPath)
	require.NoError(t, err)
	requireAdmissionBaseline(t, repair, "backend-a")
	refusedOperation := requireOperationID(t, "8964")
	refused := requireTypedAttempt(
		t, repair, "missing", "backend-a", refusedOperation,
	)
	requireLifecycleVerdict(
		t, repair, "missing", lifecycleIDFromOperation(t, refusedOperation),
		LifecycleVerdictUnusable,
	)
	refusedNow, err := repair.RefuseAttempt(refused)
	require.NoError(t, err)
	require.True(t, refusedNow)
	requireLifecycleVerdict(
		t, repair, "missing", lifecycle.ID{}, LifecycleVerdictUnusable,
	)

	repairOperation := requireOperationID(t, "8965")
	repairID := lifecycleIDFromOperation(t, repairOperation)
	repairToken := requireTypedAttempt(
		t, repair, "missing", "backend-a", repairOperation,
	)
	requireLifecycleVerdict(t, repair, "missing", repairID, LifecycleVerdictUnusable)
	confirmed, err = repair.ConfirmAttempt(repairToken)
	require.NoError(t, err)
	require.True(t, confirmed)
	requireLifecycleVerdict(t, repair, "missing", repairID, LifecycleVerdictAuthorized)
	require.NoError(t, repair.Close())

	repaired, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repaired.Close() })
	requireLifecycleVerdict(t, repaired, "missing", repairID, LifecycleVerdictAuthorized)
}
