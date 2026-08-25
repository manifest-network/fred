package placement

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func writeRawRecords(t *testing.T, dbPath string, records map[string][]byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for key, value := range records {
			if err := b.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestPlacement_StateAndString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		placement Placement
		want      State
		wantText  string
	}{
		{name: "zero is absent", want: StateAbsent, wantText: "absent"},
		{
			name:      "attempt without owner is attempting",
			placement: Placement{Attempt: "backend-a"},
			want:      StateAttempting,
			wantText:  "attempting",
		},
		{
			name:      "backend is confirmed",
			placement: Placement{Backend: "backend-a"},
			want:      StateConfirmed,
			wantText:  "confirmed",
		},
		{
			name:      "confirmed wins when an attempt coexists",
			placement: Placement{Backend: "backend-a", Attempt: "backend-a"},
			want:      StateConfirmed,
			wantText:  "confirmed",
		},
		{
			name:      "timestamp without a fact is unusable",
			placement: Placement{SetAt: time.Unix(1, 0)},
			want:      StateUnusable,
			wantText:  "unusable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.placement.State())
			assert.Equal(t, tt.wantText, tt.placement.State().String())
		})
	}
	assert.Equal(t, "State(99)", State(99).String())
}

func TestStore_AttemptConfirmAndClearLifecycle(t *testing.T) {
	fixed := time.Date(2026, 8, 25, 12, 30, 0, 0, time.FixedZone("EDT", -4*60*60))
	s := newTestStore(t, WithClock(func() time.Time { return fixed }))

	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Zero(t, s.SnapshotRevision())

	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	attempting := s.Lookup("lease-1")
	assert.Equal(t, StateAttempting, attempting.State())
	assert.Empty(t, attempting.Backend)
	assert.Equal(t, "backend-a", attempting.Attempt)
	assert.Equal(t, fixed.UTC(), attempting.SetAt)
	assert.NotZero(t, attempting.Revision())
	assert.Equal(t, attempting.Revision(), s.SnapshotRevision())
	assert.Empty(t, s.Get("lease-1"), "an attempt is not confirmed affinity")

	before := s.List()
	beforeRevision := s.SnapshotRevision()
	err := s.SetAttempting("lease-1", "backend-a")
	require.ErrorIs(t, err, ErrAttemptConflict)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.SetAttempting("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrAttemptConflict)
	err = s.Confirm("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrAttemptMismatch)
	assert.Equal(t, attempting, s.Lookup("lease-1"))

	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	confirmed := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.Equal(t, attempting.SetAt, confirmed.SetAt)
	assert.Greater(t, confirmed.Revision(), attempting.Revision())
	assert.Equal(t, "backend-a", s.Get("lease-1"))

	idempotentRevision := s.SnapshotRevision()
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	assert.Equal(t, idempotentRevision, s.SnapshotRevision())

	err = s.SetAttempting("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrBackendConflict)
	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	confirmedAttempt := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmedAttempt.State())
	assert.Equal(t, "backend-a", confirmedAttempt.Backend)
	assert.Equal(t, "backend-a", confirmedAttempt.Attempt)

	err = s.ClearAttempt("lease-1", "backend-b")
	require.ErrorIs(t, err, ErrAttemptMismatch)
	assert.Equal(t, confirmedAttempt, s.Lookup("lease-1"))

	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
	cleared := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, cleared.State())
	assert.Equal(t, "backend-a", cleared.Backend)
	assert.Empty(t, cleared.Attempt)
	assert.Greater(t, cleared.Revision(), confirmedAttempt.Revision())

	clearNoopRevision := s.SnapshotRevision()
	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
	assert.Equal(t, clearNoopRevision, s.SnapshotRevision())
}

func TestStore_ConfirmAbsentCreatesPositiveObservation(t *testing.T) {
	fixed := time.Date(2026, 8, 25, 17, 0, 0, 0, time.UTC)
	s := newTestStore(t, WithClock(func() time.Time { return fixed }))

	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	p := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, p.State())
	assert.Equal(t, "backend-a", p.Backend)
	assert.Empty(t, p.Attempt)
	assert.Equal(t, fixed, p.SetAt)
	assert.NotZero(t, p.Revision())
}

func TestStore_ClearAttemptOnlyDeletesRecord(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Zero(t, s.Count())

	// Clearing an already-absent matching attempt is idempotent.
	require.NoError(t, s.ClearAttempt("lease-1", "backend-a"))
}

func TestStore_PersistsAllStatesAndRevisions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	fixed := time.Date(2026, 8, 25, 17, 11, 15, 0, time.UTC)

	s1, err := NewStore(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	require.NoError(t, s1.SetAttempting("attempting", "backend-a"))
	require.NoError(t, s1.Confirm("confirmed", "backend-b"))
	require.NoError(t, s1.Confirm("confirmed-attempt", "backend-c"))
	require.NoError(t, s1.SetAttempting("confirmed-attempt", "backend-c"))
	want := s1.List()
	wantRevision := s1.SnapshotRevision()
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	assert.Equal(t, want, s2.List())
	assert.Equal(t, wantRevision, s2.SnapshotRevision())
	assert.Equal(t, StateAttempting, s2.Lookup("attempting").State())
	assert.Equal(t, StateConfirmed, s2.Lookup("confirmed").State())
	assert.Equal(t, "backend-c", s2.Lookup("confirmed-attempt").Attempt)
}

func TestStore_DeletePersists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Confirm("lease-1", "backend-a"))
	require.NoError(t, s1.Confirm("lease-2", "backend-b"))
	require.NoError(t, s1.Delete("lease-1"))
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	assert.Equal(t, StateAbsent, s2.Lookup("lease-1").State())
	assert.Equal(t, "backend-b", s2.Lookup("lease-2").Backend)
	assert.Equal(t, 1, s2.Count())
}

func TestStore_SetBatchMergesInventoryAtomically(t *testing.T) {
	t0 := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	clock := &fakeClock{now: t0}
	s := newTestStore(t, WithClock(clock.Now))

	require.NoError(t, s.Confirm("confirmed", "backend-a"))
	require.NoError(t, s.SetAttempting("attempting", "backend-a"))
	require.NoError(t, s.Confirm("mixed", "backend-a"))
	require.NoError(t, s.SetAttempting("mixed", "backend-a"))
	cutoff := s.SnapshotRevision()

	clock.now = t1
	require.NoError(t, s.SetBatch(map[string]string{
		"confirmed":  "backend-b", // positive inventory replaces old affinity
		"attempting": "backend-a", // matching inventory confirms the attempt
		"mixed":      "backend-b", // mismatched attempt remains unresolved
		"new":        "backend-c",
	}))

	confirmed := s.Lookup("confirmed")
	assert.Equal(t, "backend-b", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.Equal(t, t0, confirmed.SetAt)

	attempting := s.Lookup("attempting")
	assert.Equal(t, StateConfirmed, attempting.State())
	assert.Equal(t, "backend-a", attempting.Backend)
	assert.Empty(t, attempting.Attempt)
	assert.Equal(t, t0, attempting.SetAt)

	mixed := s.Lookup("mixed")
	assert.Equal(t, StateConfirmed, mixed.State())
	assert.Equal(t, "backend-b", mixed.Backend)
	assert.Equal(t, "backend-a", mixed.Attempt)
	assert.Equal(t, t0, mixed.SetAt)

	added := s.Lookup("new")
	assert.Equal(t, StateConfirmed, added.State())
	assert.Equal(t, "backend-c", added.Backend)
	assert.Equal(t, t1, added.SetAt)

	for leaseUUID, p := range s.List() {
		assert.Greater(t, p.Revision(), cutoff, leaseUUID)
	}
	assert.Equal(t, s.Lookup("new").Revision(), s.SnapshotRevision(),
		"sorted batch keys make the final revision deterministic")

	before := s.List()
	beforeRevision := s.SnapshotRevision()
	err := s.SetBatch(map[string]string{"valid": "backend-a", "invalid": ""})
	require.ErrorIs(t, err, ErrInvalidPlacement)
	assert.Equal(t, before, s.List(), "validation failure must reject the whole batch")
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	require.NoError(t, s.SetBatch(nil))
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
}

func TestStore_SetBatchIfNotNewerPreservesConcurrentAttempt(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("old-record", "backend-a"))
	cutoff := s.SnapshotRevision()

	// This write models a provision/restore that starts after backend inventory
	// fetching began. Even a matching stale positive must not clear its attempt.
	require.NoError(t, s.SetAttempting("new-attempt", "backend-b"))
	newAttempt := s.Lookup("new-attempt")
	require.Greater(t, newAttempt.Revision(), cutoff)

	require.NoError(t, s.SetBatchIfNotNewer(map[string]string{
		"old-record":  "backend-b",
		"new-attempt": "backend-b",
	}, cutoff))

	old := s.Lookup("old-record")
	assert.Equal(t, "backend-b", old.Backend, "records covered by the inventory still converge")
	assert.Greater(t, old.Revision(), cutoff)
	assert.Equal(t, newAttempt, s.Lookup("new-attempt"),
		"inventory fetched before the attempt must not confirm or overwrite it")
}

func TestStore_SetCompatibilityHelperPreservesOverwriteSemantics(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, s.Set("lease-1", "backend-a"))
	firstSetAt, ok := s.SetAt("lease-1")
	require.True(t, ok)
	require.NoError(t, s.Set("lease-1", "backend-b"))
	assert.Equal(t, "backend-b", s.Get("lease-1"))
	secondSetAt, ok := s.SetAt("lease-1")
	require.True(t, ok)
	assert.Equal(t, firstSetAt, secondSetAt)
}

func TestStore_LoadsLegacyAndOldJSONRecords(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	setAt := "2026-08-25T15:00:00Z"
	writeRawRecords(t, dbPath, map[string][]byte{
		"legacy":   []byte("backend-legacy"),
		"old-json": []byte(`{"backend":"backend-json","set_at":"` + setAt + `"}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s.Close()

	legacy := s.Lookup("legacy")
	assert.Equal(t, StateConfirmed, legacy.State())
	assert.Equal(t, "backend-legacy", legacy.Backend)
	assert.True(t, legacy.SetAt.IsZero())
	assert.Zero(t, legacy.Revision())

	oldJSON := s.Lookup("old-json")
	assert.Equal(t, StateConfirmed, oldJSON.State())
	assert.Equal(t, "backend-json", oldJSON.Backend)
	assert.Equal(t, setAt, oldJSON.SetAt.Format(time.RFC3339))
	assert.Zero(t, oldJSON.Revision())
	assert.Zero(t, s.SnapshotRevision())

	// The first mutation of a legacy record moves it onto the revisioned schema.
	require.NoError(t, s.SetAttempting("legacy", "backend-legacy"))
	assert.NotZero(t, s.Lookup("legacy").Revision())
}

func TestStore_MalformedRecordsRemainUnusable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"empty":            {},
		"malformed-json":   []byte(`{not-json`),
		"empty-object":     []byte(`{}`),
		"structured-empty": []byte(`{"backend":"","set_at":"2026-08-25T15:00:00Z","revision":7}`),
		"legacy":           []byte("backend-legacy"),
	})

	fixed := time.Date(2026, 8, 25, 18, 0, 0, 0, time.UTC)
	s, err := NewStore(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	defer s.Close()

	for _, leaseUUID := range []string{"empty", "malformed-json", "empty-object", "structured-empty"} {
		assert.Equal(t, StateUnusable, s.Lookup(leaseUUID).State(), leaseUUID)
		assert.Contains(t, s.List(), leaseUUID, "unusable records remain visible in snapshots")
	}
	assert.Equal(t, 5, s.Count())
	assert.Equal(t, uint64(7), s.SnapshotRevision())
	assert.Equal(t, uint64(7), s.Lookup("structured-empty").Revision())

	err = s.SetAttempting("empty-object", "backend-a")
	require.ErrorIs(t, err, ErrUnusablePlacement)
	err = s.Confirm("empty-object", "backend-a")
	require.ErrorIs(t, err, ErrUnusablePlacement)
	err = s.ClearAttempt("empty-object", "backend-a")
	require.ErrorIs(t, err, ErrUnusablePlacement)

	// Positive fleet inventory may repair a corrupt derived-index entry.
	require.NoError(t, s.SetBatch(map[string]string{"empty-object": "backend-a"}))
	repaired := s.Lookup("empty-object")
	assert.Equal(t, StateConfirmed, repaired.State())
	assert.Equal(t, "backend-a", repaired.Backend)
	assert.Equal(t, fixed, repaired.SetAt)
	assert.Greater(t, repaired.Revision(), uint64(7))

	deleted, err := s.DeleteIfRevision("malformed-json", 0)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, StateAbsent, s.Lookup("malformed-json").State())
	require.NoError(t, s.Delete("structured-empty"))
}

func TestStore_ListReturnsIndependentAtomicSnapshot(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	require.NoError(t, s.SetAttempting("lease-2", "backend-b"))

	snapshot := s.List()
	assert.Len(t, snapshot, 2)
	delete(snapshot, "lease-1")
	p := snapshot["lease-2"]
	p.Attempt = "mutated"
	snapshot["lease-2"] = p

	assert.Equal(t, "backend-a", s.Lookup("lease-1").Backend)
	assert.Equal(t, "backend-b", s.Lookup("lease-2").Attempt)
	assert.Equal(t, 2, s.Count())
}

func TestStore_DurableWriteFailuresLeaveCacheAndRevisionUnchanged(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("confirmed", "backend-a"))
	before := s.List()
	beforeRevision := s.SnapshotRevision()
	require.NoError(t, s.Close())

	err := s.SetAttempting("confirmed", "backend-a")
	require.Error(t, err)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.Confirm("new", "backend-b")
	require.Error(t, err)
	assert.Equal(t, StateAbsent, s.Lookup("new").State())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.SetBatch(map[string]string{"batch": "backend-c"})
	require.Error(t, err)
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.SetBatch(nil)
	require.Error(t, err, "an empty inventory must not report a durable sync against a closed store")
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())

	err = s.Delete("confirmed")
	require.Error(t, err)
	assert.Equal(t, before, s.List(), "failed durable delete must not evict the cache")

	deleted, err := s.DeleteIfRevision("confirmed", before["confirmed"].Revision())
	require.Error(t, err)
	assert.False(t, deleted)
	assert.Equal(t, before, s.List())
}

func TestStore_FailedClearLeavesAttemptCached(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	before := s.Lookup("lease-1")
	require.NoError(t, s.Close())

	err := s.ClearAttempt("lease-1", "backend-a")
	require.Error(t, err)
	assert.Equal(t, before, s.Lookup("lease-1"))

	cleared, err := s.ClearAttemptIfRevision("lease-1", "backend-a", before.Revision())
	require.Error(t, err)
	assert.False(t, cleared)
	assert.Equal(t, before, s.Lookup("lease-1"))
}

func TestStore_EncodingFailureLeavesCacheAndRevisionUnchanged(t *testing.T) {
	invalidJSONTime := time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTestStore(t, WithClock(func() time.Time { return invalidJSONTime }))

	err := s.Confirm("lease-1", "backend-a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode placement")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Zero(t, s.SnapshotRevision())

	err = s.SetBatch(map[string]string{"lease-2": "backend-b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode batch placements")
	assert.Equal(t, StateAbsent, s.Lookup("lease-2").State())
	assert.Zero(t, s.SnapshotRevision())
}

func TestStore_SemanticFailuresDoNotMutate(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	before := s.List()
	beforeRevision := s.SnapshotRevision()

	for _, err := range []error{
		s.SetAttempting("lease-1", "backend-a"),
		s.SetAttempting("lease-1", "backend-b"),
		s.Confirm("lease-1", "backend-b"),
		s.ClearAttempt("lease-1", "backend-b"),
		s.SetAttempting("", "backend-a"),
		s.Confirm("lease-2", ""),
	} {
		require.Error(t, err)
	}
	assert.Equal(t, before, s.List())
	assert.Equal(t, beforeRevision, s.SnapshotRevision())
}

func TestStore_RevisionConditionalMutationsRejectStaleSnapshots(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	confirmed := s.Lookup("lease-1")

	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	withAttempt := s.Lookup("lease-1")
	require.Greater(t, withAttempt.Revision(), confirmed.Revision())

	deleted, err := s.DeleteIfRevision("lease-1", confirmed.Revision())
	require.NoError(t, err)
	assert.False(t, deleted)
	assert.Equal(t, withAttempt, s.Lookup("lease-1"))

	cleared, err := s.ClearAttemptIfRevision(
		"lease-1", "backend-a", confirmed.Revision(),
	)
	require.NoError(t, err)
	assert.False(t, cleared)
	assert.Equal(t, withAttempt, s.Lookup("lease-1"))

	cleared, err = s.ClearAttemptIfRevision(
		"lease-1", "backend-b", withAttempt.Revision(),
	)
	require.ErrorIs(t, err, ErrAttemptMismatch)
	assert.False(t, cleared)

	cleared, err = s.ClearAttemptIfRevision(
		"lease-1", "backend-a", withAttempt.Revision(),
	)
	require.NoError(t, err)
	assert.True(t, cleared)
	current := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, current.State())
	assert.Empty(t, current.Attempt)
	assert.Greater(t, current.Revision(), withAttempt.Revision())

	deleted, err = s.DeleteIfRevision("lease-1", withAttempt.Revision())
	require.NoError(t, err)
	assert.False(t, deleted)
	deleted, err = s.DeleteIfRevision("lease-1", current.Revision())
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
}

func TestStore_ConfirmAttemptIfRevisionNeverRecreatesOrOverwrites(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	first := s.Lookup("lease-1")

	// A fast failure callback can delete the attempt before the synchronous 202
	// response is delivered. Its stale settlement must not recreate ownership.
	require.NoError(t, s.Delete("lease-1"))
	confirmed, err := s.ConfirmAttemptIfRevision("lease-1", "backend-a", first.Revision())
	require.NoError(t, err)
	assert.False(t, confirmed)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())

	// Nor may an older response overwrite a newer attempt.
	require.NoError(t, s.SetAttempting("lease-1", "backend-b"))
	newer := s.Lookup("lease-1")
	confirmed, err = s.ConfirmAttemptIfRevision("lease-1", "backend-a", first.Revision())
	require.NoError(t, err)
	assert.False(t, confirmed)
	assert.Equal(t, newer, s.Lookup("lease-1"))

	confirmed, err = s.ConfirmAttemptIfRevision("lease-1", "backend-b", newer.Revision())
	require.NoError(t, err)
	assert.True(t, confirmed)
	assert.Equal(t, "backend-b", s.Lookup("lease-1").Backend)
	assert.Empty(t, s.Lookup("lease-1").Attempt)
}

func TestStore_SetBatchIfNotNewerHonorsPostSnapshotDeleteTombstone(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))
	cutoff := s.SnapshotRevision()
	require.NoError(t, s.Delete("lease-1"))
	require.Greater(t, s.SnapshotRevision(), cutoff)

	require.NoError(t, s.SetBatchIfNotNewer(
		map[string]string{"lease-1": "backend-a"}, cutoff,
	))
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State(),
		"stale positive inventory must not recreate a record deleted after fetch began")
}

func TestStore_ConflictQuarantineSurvivesRestartAndResolvesOnCompleteEvidence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	cutoff := s.SnapshotRevision()
	require.NoError(t, s.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-b", "backend-a"},
	}, cutoff))
	p := s.Lookup("lease-1")
	assert.True(t, p.Conflict)
	assert.Equal(t, StateUnusable, p.State())
	assert.Empty(t, p.Backend)
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)
	require.NoError(t, s.Close())

	s, err = NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	p = s.Lookup("lease-1")
	assert.True(t, p.Conflict, "quarantine must survive a providerd restart")
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)

	cutoff = s.SnapshotRevision()
	require.NoError(t, s.SetBatchIfNotNewer(map[string]string{"lease-1": "backend-b"}, cutoff))
	p = s.Lookup("lease-1")
	assert.False(t, p.Conflict)
	assert.Equal(t, StateConfirmed, p.State())
	assert.Equal(t, "backend-b", p.Backend)

	cutoff = s.SnapshotRevision()
	require.NoError(t, s.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, cutoff))
	cutoff = s.SnapshotRevision() // a later complete inventory proves absence
	require.NoError(t, s.ClearConflictsIfNotNewer(map[string]struct{}{"lease-1": {}}, cutoff))
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
}

func TestStore_LegacyConflictWithoutCandidatesLoadsFailClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy-conflict":  []byte(`{"backend":"","set_at":"2026-08-25T12:00:00Z","revision":7,"conflict":true}`),
		"lease-partial-conflict": []byte(`{"backend":"","set_at":"2026-08-25T12:00:00Z","revision":8,"conflict":true,"conflict_backends":["backend-a"]}`),
	})

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for _, leaseUUID := range []string{"lease-legacy-conflict", "lease-partial-conflict"} {
		p := s.Lookup(leaseUUID)
		require.Equal(t, StateUnusable, p.State())
		assert.True(t, p.Conflict)
		assert.True(t, p.ConflictOwnersUnknown,
			"incomplete conflict records must not treat the current router as the complete historical owner set")
	}

	cutoff := s.SnapshotRevision()
	require.NoError(t, s.SetConflictsIfNotNewer(map[string][]string{
		"lease-partial-conflict": {"backend-a", "backend-b"},
	}, cutoff))
	p := s.Lookup("lease-partial-conflict")
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.True(t, p.ConflictOwnersUnknown,
		"later reports cannot prove which candidate was omitted from an incomplete historical record")
}

func TestStore_StaleConflictSnapshotCannotOverwriteNewAttempt(t *testing.T) {
	s := newTestStore(t)
	cutoff := s.SnapshotRevision()
	require.NoError(t, s.SetAttempting("lease-1", "backend-a"))

	require.NoError(t, s.SetConflictsIfNotNewer(map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}, cutoff))
	p := s.Lookup("lease-1")
	assert.False(t, p.Conflict)
	assert.Equal(t, StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
}

func TestStore_DeleteCASCannotLoseRacingAttempt(t *testing.T) {
	s := newTestStore(t)

	for i := range 32 {
		leaseUUID := fmt.Sprintf("lease-%d", i)
		require.NoError(t, s.Confirm(leaseUUID, "backend-a"))
		stale := s.Lookup(leaseUUID)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var deleted bool
		var deleteErr, attemptErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			deleted, deleteErr = s.DeleteIfRevision(leaseUUID, stale.Revision())
		}()
		go func() {
			defer wg.Done()
			<-start
			attemptErr = s.SetAttempting(leaseUUID, "backend-a")
		}()
		close(start)
		wg.Wait()

		require.NoError(t, deleteErr)
		require.NoError(t, attemptErr)
		p := s.Lookup(leaseUUID)
		assert.NotEqual(t, StateAbsent, p.State(),
			"a racing write-ahead attempt must never be lost")
		if deleted {
			assert.Equal(t, StateAttempting, p.State())
			assert.Empty(t, p.Backend)
		} else {
			assert.Equal(t, StateConfirmed, p.State())
			assert.Equal(t, "backend-a", p.Backend)
		}
		assert.Equal(t, "backend-a", p.Attempt)
		require.NoError(t, s.Delete(leaseUUID))
	}
}

func TestStore_ConcurrentIndependentTransitions(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 50
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for i := range goroutines {
		go func(id int) {
			defer wg.Done()
			leaseUUID := fmt.Sprintf("lease-concurrent-%d", id)
			if err := s.SetAttempting(leaseUUID, "backend-a"); err != nil {
				errs <- err
				return
			}
			if err := s.Confirm(leaseUUID, "backend-a"); err != nil {
				errs <- err
				return
			}
			_ = s.Lookup(leaseUUID)
			_ = s.List()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, goroutines, s.Count())
}

func TestStore_HealthCloseAndConstruction(t *testing.T) {
	_, err := NewStore("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "placement db path is required")

	s := newTestStore(t, WithClock(nil))
	require.NoError(t, s.Confirm("lease-1", "backend-a"))
	assert.False(t, s.Lookup("lease-1").SetAt.IsZero())
	assert.NoError(t, s.Healthy())
	require.NoError(t, s.Close())
	assert.Error(t, s.Healthy())
	assert.NoError(t, s.Close(), "Close must be idempotent")
}

func TestErrorsAreDistinctSemanticSentinels(t *testing.T) {
	sentinels := []error{
		ErrInvalidPlacement,
		ErrAttemptConflict,
		ErrBackendConflict,
		ErrUnusablePlacement,
		ErrAttemptMismatch,
	}
	for i, left := range sentinels {
		for j, right := range sentinels {
			if i == j {
				assert.True(t, errors.Is(left, right))
				continue
			}
			assert.False(t, errors.Is(left, right))
		}
	}
}

// fakeClock is a trivial settable clock for deterministic transition tests.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
