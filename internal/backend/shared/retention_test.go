package shared

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func newTestRetentionStore(t *testing.T) *RetentionStore {
	t.Helper()
	dir := t.TempDir()
	s, err := NewRetentionStore(RetentionStoreConfig{DBPath: dir + "/retention.db"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// getRaw returns the raw stored bytes for a key (test-only, white-box) so a test
// can assert a record is byte-identical/untouched after a refused write.
func (s *RetentionStore) getRaw(orig string) ([]byte, error) {
	var out []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(retentionBucketName).Get([]byte(orig))
		out = append([]byte(nil), raw...)
		return nil
	})
	return out, err
}

func sampleEntry(orig string) RetentionEntry {
	return RetentionEntry{
		OriginalLeaseUUID:   orig,
		Tenant:              "tenant-a",
		ProviderUUID:        "provider-1",
		Items:               []backend.LeaseItem{{SKU: "sku-1", Quantity: 2}},
		StackManifest:       &manifest.StackManifest{},
		CallbackURL:         "https://example.com/cb",
		RetainedVolumeNames: []string{"vol-a", "vol-b"},
		Status:              RetentionStatusActive,
		Generation:          0,
		CreatedAt:           time.Now(),
	}
}

// TestRetentionStore_CRUD covers Put/Get/Delete + idempotent Delete + Get-absent returns nil,nil.
func TestRetentionStore_CRUD(t *testing.T) {
	s := newTestRetentionStore(t)

	// Get absent → nil, nil
	got, err := s.Get("nonexistent")
	require.NoError(t, err)
	assert.Nil(t, got)

	e := sampleEntry("lease-1")
	require.NoError(t, s.Put(e))

	// Get existing
	got, err = s.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "lease-1", got.OriginalLeaseUUID)
	assert.Equal(t, "tenant-a", got.Tenant)
	assert.Equal(t, RetentionStatusActive, got.Status)
	assert.Equal(t, []string{"vol-a", "vol-b"}, got.RetainedVolumeNames)

	// Delete
	require.NoError(t, s.Delete("lease-1"))
	got, err = s.Get("lease-1")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Idempotent delete (no error if absent)
	require.NoError(t, s.Delete("lease-1"))
}

// TestRetentionStore_ClaimForRestore covers the atomic active→restoring transition.
func TestRetentionStore_ClaimForRestore(t *testing.T) {
	s := newTestRetentionStore(t)

	// Absent → ErrNoRetention
	_, err := s.ClaimForRestore("nope", "new-lease-1", 0)
	assert.ErrorIs(t, err, ErrNoRetention)

	// Successful claim: active → restoring
	e := sampleEntry("lease-1")
	e.Generation = 0
	require.NoError(t, s.Put(e))

	claimed, err := s.ClaimForRestore("lease-1", "new-lease-42", 0)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, RetentionStatusRestoring, claimed.Status)
	assert.Equal(t, "new-lease-42", claimed.NewLeaseUUID)
	assert.Equal(t, 1, claimed.Generation)
	assert.False(t, claimed.RestoringSince.IsZero())

	// Persisted correctly
	got, err := s.Get("lease-1")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusRestoring, got.Status)
	assert.Equal(t, 1, got.Generation)

	// Second claim → ErrNotRestorable (already restoring)
	_, err = s.ClaimForRestore("lease-1", "new-lease-99", 0)
	assert.ErrorIs(t, err, ErrNotRestorable)

	// Active but older than maxAge → ErrNoRetention (about to be reaped)
	old := sampleEntry("lease-old")
	old.CreatedAt = time.Now().Add(-100 * 24 * time.Hour) // 100 days ago
	require.NoError(t, s.Put(old))
	_, err = s.ClaimForRestore("lease-old", "new-lease-x", 90*24*time.Hour)
	assert.ErrorIs(t, err, ErrNoRetention)
}

// TestRetentionStore_ReapIfExpired_Guards verifies the reap guards: expired
// active records are reaped, reaping is idempotent, a record forced into the
// restoring state is never reaped, and a live ClaimForRestore round-trip
// protects a fresh record from reaping.
func TestRetentionStore_ReapIfExpired_Guards(t *testing.T) {
	s := newTestRetentionStore(t)

	maxAge := 90 * 24 * time.Hour

	// Insert an active entry that is "expired"
	e := sampleEntry("lease-exp")
	e.CreatedAt = time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, s.Put(e))

	// Claim it first
	_, err := s.ClaimForRestore("lease-exp", "new-lease-99", maxAge)
	// It's expired so ClaimForRestore should return ErrNoRetention
	assert.ErrorIs(t, err, ErrNoRetention)

	// Since ClaimForRestore failed, the record is still active+expired.
	// Now verify ReapIfExpired reaps it.
	names, err := s.ReapIfExpired("lease-exp", maxAge)
	require.NoError(t, err)
	assert.Equal(t, []string{"vol-a", "vol-b"}, names)

	// Gone
	got, err := s.Get("lease-exp")
	require.NoError(t, err)
	assert.Nil(t, got)

	// A second call is idempotent
	names, err = s.ReapIfExpired("lease-exp", maxAge)
	require.NoError(t, err)
	assert.Nil(t, names)

	// Now test the non-reapable case: a restoring record should NOT be reaped.
	e2 := sampleEntry("lease-restoring")
	e2.CreatedAt = time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, s.Put(e2))
	// Force it into restoring state directly via Put
	e2.Status = RetentionStatusRestoring
	require.NoError(t, s.Put(e2))

	names, err = s.ReapIfExpired("lease-restoring", maxAge)
	require.NoError(t, err)
	assert.Nil(t, names, "restoring records must not be reaped")

	// Still present
	got, err = s.Get("lease-restoring")
	require.NoError(t, err)
	assert.NotNil(t, got)

	// End-to-end atomicity: a FRESH (non-expired) active record that is
	// successfully claimed via the real ClaimForRestore round-trip must not be
	// reaped, even though the same maxAge is passed to ReapIfExpired. This
	// proves the live active->restoring transition (not a Put-forced state)
	// protects the record from concurrent reaping.
	fresh := sampleEntry("lease-fresh")
	fresh.CreatedAt = time.Now()
	require.NoError(t, s.Put(fresh))

	claimed, err := s.ClaimForRestore("lease-fresh", "new-lease-fresh", maxAge)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, RetentionStatusRestoring, claimed.Status)

	names, err = s.ReapIfExpired("lease-fresh", maxAge)
	require.NoError(t, err)
	assert.Nil(t, names, "a claimed (restoring) record must never be reaped")

	// Still present and still restoring
	got, err = s.Get("lease-fresh")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, RetentionStatusRestoring, got.Status)
}

// TestRetentionStore_RevertToActive_CAS verifies generation-CAS transitions.
func TestRetentionStore_RevertToActive_CAS(t *testing.T) {
	s := newTestRetentionStore(t)

	// Setup: put a restoring record at Generation=5
	e := sampleEntry("lease-cas")
	e.Status = RetentionStatusRestoring
	e.Generation = 5
	e.NewLeaseUUID = "new-lease-x"
	e.RestoringSince = time.Now()
	require.NoError(t, s.Put(e))

	// Correct generation → true, status=active, Generation bumped to 6
	ok, err := s.RevertToActive("lease-cas", 5)
	require.NoError(t, err)
	assert.True(t, ok)

	got, err := s.Get("lease-cas")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, RetentionStatusActive, got.Status)
	assert.Equal(t, 6, got.Generation)
	assert.Empty(t, got.NewLeaseUUID)
	assert.True(t, got.RestoringSince.IsZero())

	// Stale generation → false, no error
	ok, err = s.RevertToActive("lease-cas", 5) // gen is now 6, 5 is stale
	require.NoError(t, err)
	assert.False(t, ok)

	// Absent → false
	ok, err = s.RevertToActive("nonexistent", 0)
	require.NoError(t, err)
	assert.False(t, ok)

	// Active record (not restoring) → false
	e2 := sampleEntry("lease-active")
	e2.Generation = 3
	require.NoError(t, s.Put(e2))
	ok, err = s.RevertToActive("lease-active", 3)
	require.NoError(t, err)
	assert.False(t, ok, "non-restoring record must not be reverted")
}

// TestRetentionStore_ListExpired_ActiveOnly verifies restoring+expired records
// are NOT returned by ListExpired.
func TestRetentionStore_ListExpired_ActiveOnly(t *testing.T) {
	s := newTestRetentionStore(t)

	maxAge := 90 * 24 * time.Hour

	// Active + expired
	e1 := sampleEntry("lease-exp-active")
	e1.CreatedAt = time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, s.Put(e1))

	// Restoring + expired (should NOT appear)
	e2 := sampleEntry("lease-exp-restoring")
	e2.Status = RetentionStatusRestoring
	e2.CreatedAt = time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, s.Put(e2))

	// Active + fresh (should NOT appear)
	e3 := sampleEntry("lease-fresh-active")
	e3.CreatedAt = time.Now()
	require.NoError(t, s.Put(e3))

	expired, err := s.ListExpired(maxAge)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	assert.Equal(t, "lease-exp-active", expired[0].OriginalLeaseUUID)
}

// TestRetentionStore_ListByTenant verifies tenant filtering.
func TestRetentionStore_ListByTenant(t *testing.T) {
	s := newTestRetentionStore(t)

	e1 := sampleEntry("lease-t1-a")
	e1.Tenant = "tenant-1"
	require.NoError(t, s.Put(e1))

	e2 := sampleEntry("lease-t1-b")
	e2.Tenant = "tenant-1"
	require.NoError(t, s.Put(e2))

	e3 := sampleEntry("lease-t2-a")
	e3.Tenant = "tenant-2"
	require.NoError(t, s.Put(e3))

	t1, err := s.ListByTenant("tenant-1")
	require.NoError(t, err)
	assert.Len(t, t1, 2)

	t2, err := s.ListByTenant("tenant-2")
	require.NoError(t, err)
	assert.Len(t, t2, 1)

	t3, err := s.ListByTenant("tenant-none")
	require.NoError(t, err)
	assert.Empty(t, t3)
}

// TestRetentionStore_Persistence verifies data survives close + reopen.
func TestRetentionStore_Persistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/retention.db"

	// Open, write, close
	s1, err := NewRetentionStore(RetentionStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	require.NoError(t, s1.Put(sampleEntry("lease-persist")))
	require.NoError(t, s1.Close())

	// Reopen and read
	s2, err := NewRetentionStore(RetentionStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	got, err := s2.Get("lease-persist")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "lease-persist", got.OriginalLeaseUUID)
	assert.Equal(t, "tenant-a", got.Tenant)
}

// TestRetentionStore_EmptyPath verifies that an empty DBPath returns an error.
func TestRetentionStore_EmptyPath(t *testing.T) {
	_, err := NewRetentionStore(RetentionStoreConfig{DBPath: ""})
	require.Error(t, err)
}

// TestRetentionStore_ListRestoring verifies that only restoring records are returned.
func TestRetentionStore_ListRestoring(t *testing.T) {
	s := newTestRetentionStore(t)

	e1 := sampleEntry("lease-r1")
	e1.Status = RetentionStatusRestoring
	require.NoError(t, s.Put(e1))

	e2 := sampleEntry("lease-r2")
	e2.Status = RetentionStatusRestoring
	require.NoError(t, s.Put(e2))

	e3 := sampleEntry("lease-a1")
	e3.Status = RetentionStatusActive
	require.NoError(t, s.Put(e3))

	restoring, err := s.ListRestoring()
	require.NoError(t, err)
	assert.Len(t, restoring, 2)
	for _, r := range restoring {
		assert.Equal(t, RetentionStatusRestoring, r.Status)
	}

	all, err := s.List()
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

// TestDeleteIfActive_DeletesActive verifies the atomic cap-eviction primitive:
// an ACTIVE record is removed in-txn and its retained volume names are returned
// for the caller to destroy after commit.
func TestDeleteIfActive_DeletesActive(t *testing.T) {
	s := newTestRetentionStore(t)

	e := sampleEntry("lease-active")
	require.NoError(t, s.Put(e))

	names, deleted, err := s.DeleteIfActive("lease-active")
	require.NoError(t, err)
	assert.True(t, deleted, "active record must be deleted")
	assert.Equal(t, []string{"vol-a", "vol-b"}, names, "retained names returned for post-commit destroy")

	got, err := s.Get("lease-active")
	require.NoError(t, err)
	assert.Nil(t, got, "record must be gone after DeleteIfActive")
}

// TestDeleteIfActive_SkipsRestoring verifies the TOCTOU guard: a record that was
// concurrently claimed for restore (Status=restoring) is NOT deleted, so cap
// eviction can never race a restore that already owns the record.
func TestDeleteIfActive_SkipsRestoring(t *testing.T) {
	s := newTestRetentionStore(t)

	e := sampleEntry("lease-restoring")
	e.Status = RetentionStatusRestoring
	e.NewLeaseUUID = "new-lease"
	e.Generation = 5
	require.NoError(t, s.Put(e))

	names, deleted, err := s.DeleteIfActive("lease-restoring")
	require.NoError(t, err)
	assert.False(t, deleted, "restoring record must NOT be deleted")
	assert.Nil(t, names, "no names returned when not deleted")

	got, err := s.Get("lease-restoring")
	require.NoError(t, err)
	require.NotNil(t, got, "restoring record must remain untouched")
	assert.Equal(t, RetentionStatusRestoring, got.Status)
	assert.Equal(t, "new-lease", got.NewLeaseUUID)
	assert.Equal(t, 5, got.Generation)
}

// TestDeleteIfActive_AbsentNoOp verifies DeleteIfActive on a missing key is a
// no-op: deleted=false, nil names, no error.
func TestDeleteIfActive_AbsentNoOp(t *testing.T) {
	s := newTestRetentionStore(t)

	names, deleted, err := s.DeleteIfActive("nonexistent")
	require.NoError(t, err)
	assert.False(t, deleted)
	assert.Nil(t, names)
}

// TestMarkReapingIfActive verifies active→reaping is atomic, returns the volume
// names, stamps ReapingSince, and refuses non-active records (ok=false, untouched).
func TestMarkReapingIfActive(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(sampleEntry("lease-a"))) // active, vols [vol-a vol-b]

	names, ok, err := s.MarkReapingIfActive("lease-a")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"vol-a", "vol-b"}, names)

	got, err := s.Get("lease-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, RetentionStatusReaping, got.Status)
	assert.False(t, got.ReapingSince.IsZero(), "ReapingSince must be stamped")

	// Second call: already reaping → ok=false, no error.
	_, ok, err = s.MarkReapingIfActive("lease-a")
	require.NoError(t, err)
	assert.False(t, ok)

	// Absent key → ok=false, no error.
	_, ok, err = s.MarkReapingIfActive("nonexistent")
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestPutActiveMerged_AbsentWritesFresh verifies that PutActiveMerged on an
// absent key writes the base entry verbatim and returns ok=true.
func TestPutActiveMerged_AbsentWritesFresh(t *testing.T) {
	s := newTestRetentionStore(t)

	createdAt := time.Now().Round(time.Millisecond)
	base := RetentionEntry{
		OriginalLeaseUUID:   "lease-1",
		Tenant:              "tenant-a",
		ProviderUUID:        "prov-1",
		Items:               []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}},
		StackManifest:       &manifest.StackManifest{},
		CallbackURL:         "https://example.com/cb",
		RetainedVolumeNames: []string{"fred-retained-lease-1-app-0"},
		Status:              RetentionStatusActive,
		Generation:          0,
		CreatedAt:           createdAt,
	}

	ok, err := s.PutActiveMerged(base)
	require.NoError(t, err)
	assert.True(t, ok, "absent key must be written fresh (ok=true)")

	got, err := s.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, RetentionStatusActive, got.Status)
	assert.Equal(t, 0, got.Generation)
	assert.True(t, got.CreatedAt.Equal(createdAt))
	assert.Equal(t, []string{"fred-retained-lease-1-app-0"}, got.RetainedVolumeNames)
	assert.Equal(t, "tenant-a", got.Tenant)
}

// TestPutActiveMerged_ActiveMergesAndPreservesCreatedAtGen verifies that against
// an existing ACTIVE record PutActiveMerged preserves the stored CreatedAt and
// Generation and writes the dedup-union of the stored and base RetainedVolumeNames.
func TestPutActiveMerged_ActiveMergesAndPreservesCreatedAtGen(t *testing.T) {
	s := newTestRetentionStore(t)

	past := time.Now().Add(-30 * 24 * time.Hour).Round(time.Millisecond)
	require.NoError(t, s.Put(RetentionEntry{
		OriginalLeaseUUID:   "lease-1",
		Tenant:              "tenant-a",
		Status:              RetentionStatusActive,
		RetainedVolumeNames: []string{"a"},
		Generation:          3,
		CreatedAt:           past,
	}))

	// Base carries fresh CreatedAt/Gen=0 and an overlapping+new name set; the
	// stored CreatedAt/Generation must win and the names must be unioned.
	base := RetentionEntry{
		OriginalLeaseUUID:   "lease-1",
		Tenant:              "tenant-a",
		Status:              RetentionStatusActive,
		RetainedVolumeNames: []string{"a", "b"},
		Generation:          0,
		CreatedAt:           time.Now(),
	}
	ok, err := s.PutActiveMerged(base)
	require.NoError(t, err)
	assert.True(t, ok)

	got, err := s.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.CreatedAt.Equal(past), "stored CreatedAt must be preserved; got %v want %v", got.CreatedAt, past)
	assert.Equal(t, 3, got.Generation, "stored Generation must be preserved")
	assert.ElementsMatch(t, []string{"a", "b"}, got.RetainedVolumeNames, "names must be the dedup union")
}

// TestPutActiveMerged_ActivePreservesStoredManifestWhenBaseNil guards the
// soft-delete retry path: a first close attempt may persist a non-nil
// StackManifest (from the provision or hydrated from the release store), then a
// later retry can recompute base.StackManifest == nil (e.g. a transient release-
// store read/parse failure, or the release reaped between attempts). The merge
// must NOT let that nil clobber the stored manifest — Restore rejects nil
// manifests, so a clobber would make an otherwise-restorable lease permanently
// un-restorable. CreatedAt/Generation are still preserved and names still union.
func TestPutActiveMerged_ActivePreservesStoredManifestWhenBaseNil(t *testing.T) {
	s := newTestRetentionStore(t)

	past := time.Now().Add(-10 * 24 * time.Hour).Round(time.Millisecond)
	require.NoError(t, s.Put(RetentionEntry{
		OriginalLeaseUUID: "lease-1",
		Tenant:            "tenant-a",
		Status:            RetentionStatusActive,
		StackManifest: &manifest.StackManifest{
			Services: map[string]*manifest.Manifest{
				manifest.DefaultServiceName: {Image: "nginx:latest"},
			},
		},
		RetainedVolumeNames: []string{"vol-a"},
		Generation:          2,
		CreatedAt:           past,
	}))

	// Retry whose hydration regressed to nil; everything else as before.
	base := RetentionEntry{
		OriginalLeaseUUID:   "lease-1",
		Tenant:              "tenant-a",
		Status:              RetentionStatusActive,
		StackManifest:       nil,
		RetainedVolumeNames: []string{"vol-a", "vol-b"},
		Generation:          0,
		CreatedAt:           time.Now(),
	}
	ok, err := s.PutActiveMerged(base)
	require.NoError(t, err)
	assert.True(t, ok)

	got, err := s.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.StackManifest, "stored manifest must NOT be clobbered by a nil base manifest")
	require.NotNil(t, got.StackManifest.Services[manifest.DefaultServiceName], "the specific stored service must survive")
	assert.Equal(t, "nginx:latest", got.StackManifest.Services[manifest.DefaultServiceName].Image)
	assert.True(t, got.CreatedAt.Equal(past), "stored CreatedAt must still be preserved")
	assert.Equal(t, 2, got.Generation, "stored Generation must still be preserved")
	assert.ElementsMatch(t, []string{"vol-a", "vol-b"}, got.RetainedVolumeNames, "names must still union")
}

// TestPutActiveMerged_RestoringRefuses verifies that against an existing
// NON-active (restoring) record PutActiveMerged writes NOTHING, returns
// ok=false + nil err, and leaves the stored record byte-identical.
func TestPutActiveMerged_RestoringRefuses(t *testing.T) {
	s := newTestRetentionStore(t)

	stored := RetentionEntry{
		OriginalLeaseUUID:   "lease-1",
		Tenant:              "tenant-a",
		Status:              RetentionStatusRestoring,
		NewLeaseUUID:        "new-lease",
		RetainedVolumeNames: []string{"a"},
		Generation:          5,
		RestoringSince:      time.Now().Round(time.Millisecond),
		CreatedAt:           time.Now().Add(-time.Hour).Round(time.Millisecond),
	}
	require.NoError(t, s.Put(stored))

	before, err := s.getRaw("lease-1")
	require.NoError(t, err)

	base := RetentionEntry{
		OriginalLeaseUUID:   "lease-1",
		Tenant:              "tenant-a",
		Status:              RetentionStatusActive,
		RetainedVolumeNames: []string{"b"},
		Generation:          0,
		CreatedAt:           time.Now(),
	}
	ok, err := s.PutActiveMerged(base)
	require.NoError(t, err, "refusing a restoring record is not an error")
	assert.False(t, ok, "a restoring record must not be overwritten (ok=false)")

	after, err := s.getRaw("lease-1")
	require.NoError(t, err)
	assert.Equal(t, before, after, "the restoring record's bytes must be untouched")

	got, err := s.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, RetentionStatusRestoring, got.Status)
	assert.Equal(t, 5, got.Generation)
	assert.Equal(t, "new-lease", got.NewLeaseUUID)
}
