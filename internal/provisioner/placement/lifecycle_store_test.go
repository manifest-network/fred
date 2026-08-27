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

func TestStore_LifecycleMigrationKeepsExistingOwnerLegacyAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"legacy": []byte(`{"backend":"backend-a","set_at":"2026-08-27T12:00:00Z"}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	assert.Equal(t, StateConfirmed, s.Lookup("legacy").State())
	legacy := requireLifecycleVerdict(
		t, s, "legacy", lifecycle.ID{}, LifecycleVerdictLegacy,
	)
	assert.Equal(t, "backend-a", legacy.Backend(),
		"legacy observation routing comes from durable placement, never callback JSON")
	assert.Equal(t, LifecycleVerdictLegacy, s.CurrentLifecycle("legacy").Verdict())
	assert.False(t, s.CurrentLifecycle("legacy").ID().Valid())
	requireLifecycleVerdict(
		t, s, "legacy", requireLifecycleID(t, "8101"), LifecycleVerdictStale,
	)
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	requireLifecycleVerdict(
		t, reopened, "legacy", lifecycle.ID{}, LifecycleVerdictLegacy,
	)

	retired, err := reopened.RetireLifecycle("legacy", lifecycle.ID{})
	require.NoError(t, err)
	assert.True(t, retired.Retired())
	assert.Equal(t, "backend-a", retired.Backend())
	retiredAgain, err := reopened.RetireLifecycle("legacy", lifecycle.ID{})
	require.NoError(t, err)
	assert.True(t, retiredAgain.Retired())
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

func TestStore_InventoryPromotesTypedAttemptButLegacyAttemptStaysLegacy(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	typedOperation := requireOperationID(t, "8501")
	typedID := lifecycleIDFromOperation(t, typedOperation)
	requireTypedAttempt(t, s, "typed", "backend-a", typedOperation)
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"typed": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "typed", typedID, LifecycleVerdictAuthorized)

	dbPath := filepath.Join(t.TempDir(), "legacy-attempt.db")
	legacyOperation := requireOperationID(t, "8502")
	legacyID := lifecycleIDFromOperation(t, legacyOperation)
	writeRawRecords(t, dbPath, map[string][]byte{
		"legacy-attempt": []byte(`{"attempt":"backend-a","operation_id":"` +
			legacyOperation.String() + `","set_at":"2026-08-27T12:00:00Z","revision":1}`),
	})
	legacyStore, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = legacyStore.Close() })
	require.NoError(t, legacyStore.ConfigureBackendTopology([]string{"backend-a"}))
	fence := legacyStore.BeginInventorySession()
	_, err = legacyStore.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"legacy-attempt": "backend-a"},
	})
	legacyStore.EndInventorySession(fence)
	require.NoError(t, err)
	requireLifecycleVerdict(
		t, legacyStore, "legacy-attempt", lifecycle.ID{}, LifecycleVerdictLegacy,
	)
	requireLifecycleVerdict(
		t, legacyStore, "legacy-attempt", legacyID, LifecycleVerdictStale,
	)
}

func TestStore_LifecycleCapabilitySurvivesPlacementDeleteAndRetiresExactly(t *testing.T) {
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

	retired, err := s.RetireLifecycle("lease", id)
	require.NoError(t, err)
	assert.True(t, retired.Retired())
	assert.True(t, retired.RetiredNow())
	assert.Equal(t, LifecycleVerdictRetired, s.CurrentLifecycle("lease").Verdict())
	retired, err = s.RetireLifecycle("lease", id)
	require.NoError(t, err)
	assert.True(t, retired.Retired())
	assert.False(t, retired.RetiredNow(), "idempotent retirement is not a new consume")
	require.NoError(t, s.Close())

	reopened, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	retired = requireLifecycleVerdict(
		t, reopened, "lease", id, LifecycleVerdictRetired,
	)
	assert.Equal(t, "backend-a", retired.Backend())
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
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictRetired)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictStale)
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

func TestStore_LegacyCapabilityBecomesTeardownOnlyAfterPlacementDelete(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a")
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"legacy": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "legacy", lifecycle.ID{}, LifecycleVerdictLegacy)

	requireDeleteRecord(t, s, "legacy")
	teardown := requireLifecycleVerdict(
		t, s, "legacy", lifecycle.ID{}, LifecycleVerdictTeardownOnly,
	)
	assert.Equal(t, "backend-a", teardown.Backend())
	requireLifecycleVerdict(
		t, s, "legacy", requireLifecycleID(t, "8633"), LifecycleVerdictStale,
	)
	assert.Equal(t, LifecycleVerdictTeardownOnly, s.CurrentLifecycle("legacy").Verdict())

	retired, err := s.RetireLifecycle("legacy", lifecycle.ID{})
	require.NoError(t, err)
	assert.True(t, retired.RetiredNow())
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

	secondOperation := requireOperationID(t, "8642")
	secondID := lifecycleIDFromOperation(t, secondOperation)
	second := requireTypedAttempt(t, s, "lease", "backend-a", secondOperation)
	requireLifecycleVerdict(t, s, "lease", firstID, LifecycleVerdictRetired)
	requireLifecycleVerdict(t, s, "lease", secondID, LifecycleVerdictStale)
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
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictRetired)
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
	})
	requireLifecycleVerdict(t, s, "lease", id, LifecycleVerdictRetired)
	requireLifecycleVerdict(t, s, "lease", lifecycle.ID{}, LifecycleVerdictStale)
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

func TestStore_MalformedLifecycleCapabilitiesFailClosedOnReopen(t *testing.T) {
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
			require.Error(t, err)
			assert.Nil(t, reopened)
		})
	}
}
