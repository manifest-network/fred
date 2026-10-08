package placement

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

// lifecycleRowForTest returns the durable lifecycle row of a lease.
func lifecycleRowForTest(t *testing.T, s *Store, leaseUUID string) []byte {
	t.Helper()
	var row []byte
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		row = append([]byte(nil), tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(leaseUUID))...)
		return nil
	}))
	require.NotEmpty(t, row, "lease %q has a durable lifecycle row", leaseUUID)
	return row
}

// lifecycleViewForTest is everything an authority decision can read about one
// lease's lifecycle capability.
type lifecycleViewForTest struct {
	cached     lifecycleCapability
	cachedSet  bool
	byID       LifecycleVerdict
	byZeroID   LifecycleVerdict
	current    LifecycleAuthorization
	revision   uint64
	attempt    string
	ownBackend string
}

func lifecycleViewOf(t *testing.T, s *Store, leaseUUID string, id lifecycle.ID) lifecycleViewForTest {
	t.Helper()
	s.mu.RLock()
	cached, cachedSet := s.lifecycleCache[leaseUUID]
	s.mu.RUnlock()
	placement := s.Lookup(leaseUUID)
	byID, err := s.authorizeLifecycle(leaseUUID, id)
	require.NoError(t, err)
	byZeroID, err := s.authorizeLifecycle(leaseUUID, lifecycle.ID{})
	require.NoError(t, err)
	return lifecycleViewForTest{
		cached:     cached,
		cachedSet:  cachedSet,
		byID:       byID.Verdict(),
		byZeroID:   byZeroID.Verdict(),
		current:    s.CurrentLifecycle(leaseUUID),
		revision:   placement.Revision(),
		attempt:    placement.Attempt,
		ownBackend: placement.Backend,
	}
}

// requireRefusedOnlyEvidenceIsTheWrittenSentinel checks the state a refusal
// leaves when the refused attempt was the capability's only evidence, then
// that the next identical inventory projection writes nothing and that
// reopening the store reads back exactly what the running store held.
func requireRefusedOnlyEvidenceIsTheWrittenSentinel(
	t *testing.T,
	s *Store,
	dbPath, leaseUUID string,
	id lifecycle.ID,
	unknown InventoryProjection,
) {
	t.Helper()
	confirmed := s.Lookup(leaseUUID)
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)

	row := lifecycleRowForTest(t, s, leaseUUID)
	assert.JSONEq(t, evidenceFreeSentinelRow, string(row))
	decoded, err := decodeLifecycleCapability(row)
	require.NoError(t, err)
	s.mu.RLock()
	cached := s.lifecycleCache[leaseUUID]
	s.mu.RUnlock()
	assert.Equal(t, decoded, cached, "the cache holds the form its row decodes to")
	assert.Equal(t, quarantinedLifecycle(), cached)
	requireLifecycleVerdict(t, s, leaseUUID, id, LifecycleVerdictUnusable)
	requireLifecycleVerdict(t, s, leaseUUID, lifecycle.ID{}, LifecycleVerdictUnusable)

	before := lifecycleViewOf(t, s, leaseUUID, id)
	projectInventoryForTest(t, s, unknown)
	after := lifecycleViewOf(t, s, leaseUUID, id)
	assert.Equal(t, before, after,
		"an identical projection neither rewrites the sentinel nor bumps the placement revision")
	assert.Equal(t, row, lifecycleRowForTest(t, s, leaseUUID))

	require.NoError(t, s.Close())
	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, after, lifecycleViewOf(t, reopened, leaseUUID, id),
		"reopening reads back the cache entry and every verdict the running store held")
}

// TestRefusedAttemptThatWasTheOnlyEvidenceCachesTheWrittenSentinel drives the
// typed attempt refusal end to end. Inventory confirms a first attempt's
// backend without a callback generation (an Unknown observation), so the
// placement keeps both its owner and the exact attempt while the capability
// still holds only the attempt marker. Refusing that attempt clears the
// capability's only evidence.
func TestRefusedAttemptThatWasTheOnlyEvidenceCachesTheWrittenSentinel(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	requireAdmissionBaseline(t, s, "backend-a")
	operationID := requireOperationID(t, "8601")
	id := lifecycleIDFromOperation(t, operationID)
	requireTypedAttempt(t, s, "lease", "backend-a", operationID)

	unknown := InventoryProjection{
		Placements: map[string]string{"lease": "backend-a"},
		lifecycles: map[string]LifecycleObservation{"lease": {Kind: LifecycleObservationUnknown}},
	}
	projectInventoryForTest(t, s, unknown)
	pending := s.Lookup("lease")
	require.Equal(t, "backend-a", pending.Backend)
	require.Equal(t, "backend-a", pending.Attempt)
	s.mu.RLock()
	attemptOnly := s.lifecycleCache["lease"]
	s.mu.RUnlock()
	require.Equal(t, lifecycleCapability{attemptBackend: "backend-a", attemptID: id}, attemptOnly,
		"the capability's only evidence is the attempt marker")

	refused, err := refuseOperationForTest(s, "lease", "backend-a", operationID)
	require.NoError(t, err)
	require.True(t, refused, "the refusal commits rather than failing to encode")

	requireRefusedOnlyEvidenceIsTheWrittenSentinel(t, s, dbPath, "lease", id, unknown)
}

// TestRefusedRestoreThatWasTheOnlyEvidenceCachesTheWrittenSentinel covers the
// restore refusal. A restore target starts absent, and every confirmation of
// it (inventory or promotion) moves its revision past the one the restore
// claim holds, so through the store's API the refusal cannot reach a
// confirmed target: the exact inventory observation wins and the refusal
// leaves it untouched. The second case arranges the state refuseRestore's
// confirmed-target branch guards (the target confirmed on the source backend
// at the claim's revision, with only the restore attempt marker as lifecycle
// evidence) and checks that the refusal leaves the same written sentinel as a
// typed attempt refusal.
func TestRefusedRestoreThatWasTheOnlyEvidenceCachesTheWrittenSentinel(t *testing.T) {
	unknown := InventoryProjection{
		Placements: map[string]string{"target": "backend-a"},
		lifecycles: map[string]LifecycleObservation{"target": {Kind: LifecycleObservationUnknown}},
	}
	beginTargetRestore := func(t *testing.T, number string) (*Store, string, RestoreClaim, lifecycle.ID) {
		t.Helper()
		dbPath := filepath.Join(t.TempDir(), "placements.db")
		s, err := newStoreForTest(dbPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		requireConfirmedPlacement(t, s, "source", "backend-a")
		requireAdmissionBaseline(t, s, "backend-a", "backend-b")
		operationID := requireOperationID(t, number)
		claim, err := s.beginRestore(
			s.CurrentAdmissionBaseline(), "source", "target", operationID,
			testBackendRequestSnapshot(t), testCallbackPair(operationID),
		)
		require.NoError(t, err)
		return s, dbPath, claim, lifecycleIDFromOperation(t, operationID)
	}
	attemptOnly := func(id lifecycle.ID) lifecycleCapability {
		return lifecycleCapability{attemptBackend: "backend-a", attemptID: id}
	}

	t.Run("an inventory confirmation wins over the refusal", func(t *testing.T) {
		s, _, claim, id := beginTargetRestore(t, "8602")
		projectInventoryForTest(t, s, unknown)
		before := lifecycleViewOf(t, s, "target", id)
		require.Equal(t, "backend-a", before.ownBackend)
		require.Equal(t, "backend-a", before.attempt)
		require.Equal(t, attemptOnly(id), before.cached)

		refused, err := s.refuseRestore(claim)
		require.NoError(t, err)
		require.True(t, refused, "the refusal consumes the source claim")
		assert.Equal(t, before, lifecycleViewOf(t, s, "target", id),
			"the refusal no longer matches the target's revision, so it changes nothing")
		assert.Equal(t, LifecycleVerdictMissing, before.byID)
	})

	t.Run("a target confirmed at the claim's revision", func(t *testing.T) {
		s, dbPath, claim, id := beginTargetRestore(t, "8603")
		s.mu.Lock()
		target := s.cache["target"]
		target.Backend = "backend-a"
		s.cache["target"] = target
		require.Equal(t, attemptOnly(id), s.lifecycleCache["target"])
		s.mu.Unlock()
		require.Equal(t, StateConfirmed, s.Lookup("target").State())

		refused, err := s.refuseRestore(claim)
		require.NoError(t, err)
		require.True(t, refused)

		requireRefusedOnlyEvidenceIsTheWrittenSentinel(t, s, dbPath, "target", id, unknown)
	})
}
