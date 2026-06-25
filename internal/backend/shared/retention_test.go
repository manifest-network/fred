package shared

import (
	"fmt"
	"sort"
	"sync"
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

// TestMarkReapingIfExpired verifies the reap mark only fires for active+expired
// records, stamps ReapingSince, returns names, and guards fresh/non-active/zero-age.
func TestMarkReapingIfExpired(t *testing.T) {
	s := newTestRetentionStore(t)
	maxAge := time.Hour

	expired := sampleEntry("lease-exp")
	expired.CreatedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, s.Put(expired))

	names, ok, err := s.MarkReapingIfExpired("lease-exp", maxAge)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"vol-a", "vol-b"}, names)
	got, err := s.Get("lease-exp")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusReaping, got.Status)
	assert.False(t, got.ReapingSince.IsZero())

	// Fresh record → not reaped.
	fresh := sampleEntry("lease-fresh") // CreatedAt = now
	require.NoError(t, s.Put(fresh))
	_, ok, err = s.MarkReapingIfExpired("lease-fresh", maxAge)
	require.NoError(t, err)
	assert.False(t, ok)

	// maxAge<=0 → no-op.
	_, ok, err = s.MarkReapingIfExpired("lease-exp", 0)
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestListReaping returns only reaping records.
func TestListReaping(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(sampleEntry("active-1"))) // active
	reaping := sampleEntry("reaping-1")
	reaping.Status = RetentionStatusReaping
	require.NoError(t, s.Put(reaping))
	restoring := sampleEntry("restoring-1")
	restoring.Status = RetentionStatusRestoring
	require.NoError(t, s.Put(restoring))

	got, err := s.ListReaping()
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "reaping-1", got[0].OriginalLeaseUUID)
}

// TestPutReaping writes a fresh reaping tombstone, refuses to clobber an
// active/restoring record (ok=false), and unions names + preserves ReapingSince
// when an existing reaping record is present (idempotent re-leak).
func TestPutReaping(t *testing.T) {
	s := newTestRetentionStore(t)

	base := sampleEntry("lease-x")
	base.RetainedVolumeNames = []string{"fred-lease-x-app-0"}
	ok, err := s.PutReaping(base)
	require.NoError(t, err)
	assert.True(t, ok)
	got, err := s.Get("lease-x")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusReaping, got.Status)
	assert.False(t, got.ReapingSince.IsZero())
	first := got.ReapingSince

	// Re-leak with an extra volume → union, ReapingSince preserved.
	base2 := sampleEntry("lease-x")
	base2.RetainedVolumeNames = []string{"fred-lease-x-app-0", "fred-lease-x-app-1"}
	ok, err = s.PutReaping(base2)
	require.NoError(t, err)
	assert.True(t, ok)
	got, err = s.Get("lease-x")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fred-lease-x-app-0", "fred-lease-x-app-1"}, got.RetainedVolumeNames)
	assert.Equal(t, first, got.ReapingSince, "ReapingSince preserved across re-leak")

	// Refuse to clobber an ACTIVE record.
	require.NoError(t, s.Put(sampleEntry("lease-active")))
	ok, err = s.PutReaping(sampleEntry("lease-active"))
	require.NoError(t, err)
	assert.False(t, ok)
	got, err = s.Get("lease-active")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusActive, got.Status, "active record must be untouched")
}

// TestPutReaping_ReLeakPreservesStoredAccounting verifies a re-leak with PARTIAL base
// data does not clobber the stored entry's accounting/identity fields — only newly
// discovered volume names are unioned in. Guards a future caller from under-counting the
// reaping footprint by overwriting Items/Tenant/ProviderUUID (Copilot review). ENG-376.
func TestPutReaping_ReLeakPreservesStoredAccounting(t *testing.T) {
	s := newTestRetentionStore(t)

	first := sampleEntry("lease-y") // Items [{sku-1, qty 2}], Tenant tenant-a, ProviderUUID provider-1
	first.RetainedVolumeNames = []string{"vol-a", "vol-b"}
	ok, err := s.PutReaping(first)
	require.NoError(t, err)
	require.True(t, ok)

	// A future caller re-leaks with PARTIAL data (empty Items/Tenant/ProviderUUID) plus
	// a newly discovered volume name.
	partial := RetentionEntry{
		OriginalLeaseUUID:   "lease-y",
		RetainedVolumeNames: []string{"vol-c"},
	}
	ok, err = s.PutReaping(partial)
	require.NoError(t, err)
	require.True(t, ok)

	got, err := s.Get("lease-y")
	require.NoError(t, err)
	// Accounting/identity fields preserved from the first (full) write — NOT clobbered.
	assert.Equal(t, first.Items, got.Items, "Items must survive a partial re-leak (else reaping footprint under-counts)")
	assert.Equal(t, "tenant-a", got.Tenant, "Tenant preserved")
	assert.Equal(t, "provider-1", got.ProviderUUID, "ProviderUUID preserved")
	// The newly discovered volume name is unioned in.
	assert.ElementsMatch(t, []string{"vol-a", "vol-b", "vol-c"}, got.RetainedVolumeNames)
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

// TestMarkReaping_VsClaimForRestore_Concurrent races mark-reaping against a restore
// claim on the SAME record many times and asserts the two atomic transitions never
// both win: a record is never both reaping AND restoring, and exactly one wins each
// round (whichever bbolt serializes first; the loser observes it and no-ops). ENG-376.
func TestMarkReaping_VsClaimForRestore_Concurrent(t *testing.T) {
	s := newTestRetentionStore(t)
	for iter := 0; iter < 200; iter++ {
		require.NoError(t, s.Put(sampleEntry("lease-c"))) // reset to active each round

		var (
			wg       sync.WaitGroup
			reapOK   bool
			reapErr  error
			claimErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, reapOK, reapErr = s.MarkReapingIfActive("lease-c")
		}()
		go func() {
			defer wg.Done()
			_, claimErr = s.ClaimForRestore("lease-c", "new", time.Hour)
		}()
		wg.Wait()

		require.NoError(t, reapErr)
		claimOK := claimErr == nil
		if !claimOK {
			require.ErrorIs(t, claimErr, ErrNotRestorable, "claim loses only because the record is reaping")
		}
		assert.False(t, reapOK && claimOK, "mark-reaping and claim-for-restore must never both succeed")
		assert.True(t, reapOK || claimOK, "exactly one transition must win each round")

		got, err := s.Get("lease-c")
		require.NoError(t, err)
		if reapOK {
			assert.Equal(t, RetentionStatusReaping, got.Status)
		} else {
			assert.Equal(t, RetentionStatusRestoring, got.Status)
		}
	}
}

// TestStatusAudit_ClaimForRestore_RejectsReaping ensures a reaping record cannot be restored.
func TestStatusAudit_ClaimForRestore_RejectsReaping(t *testing.T) {
	s := newTestRetentionStore(t)
	r := sampleEntry("lease-r")
	r.Status = RetentionStatusReaping
	require.NoError(t, s.Put(r))
	_, err := s.ClaimForRestore("lease-r", "new", time.Hour)
	require.Error(t, err, "ClaimForRestore must reject a reaping record")
}

// TestStatusAudit_ListExpired_ExcludesReaping ensures the reaper never re-marks a reaping record.
func TestStatusAudit_ListExpired_ExcludesReaping(t *testing.T) {
	s := newTestRetentionStore(t)
	r := sampleEntry("lease-r")
	r.Status = RetentionStatusReaping
	r.CreatedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, s.Put(r))
	got, err := s.ListExpired(time.Hour)
	require.NoError(t, err)
	assert.Empty(t, got, "ListExpired returns active-only")
}

// indexSnapshot returns a deep, per-partition SORTED copy of the in-memory index
// for white-box assertions (sorted so equality checks are order-deterministic).
func (s *RetentionStore) indexSnapshot() (map[string][]string, map[string][]string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := func(m map[string]map[string]struct{}) map[string][]string {
		out := map[string][]string{}
		for k, set := range m {
			for u := range set {
				out[k] = append(out[k], u)
			}
			sort.Strings(out[k])
		}
		return out
	}
	return cp(s.byTenant), cp(s.byStatus)
}

func TestRetentionIndex_RebuildOnOpen(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/retention.db"
	s1, err := NewRetentionStore(RetentionStoreConfig{DBPath: path})
	require.NoError(t, err)
	a := sampleEntry("lease-a") // tenant-a, active
	b := sampleEntry("lease-b")
	b.Tenant = "tenant-b"
	b.Status = RetentionStatusReaping
	require.NoError(t, s1.Put(a))
	require.NoError(t, s1.Put(b))
	require.NoError(t, s1.Close())

	s2, err := NewRetentionStore(RetentionStoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	byTenant, byStatus := s2.indexSnapshot()
	assert.ElementsMatch(t, []string{"lease-a"}, byTenant["tenant-a"])
	assert.ElementsMatch(t, []string{"lease-b"}, byTenant["tenant-b"])
	assert.ElementsMatch(t, []string{"lease-a"}, byStatus[RetentionStatusActive])
	assert.ElementsMatch(t, []string{"lease-b"}, byStatus[RetentionStatusReaping])
}

// A malformed record must fail the open (fail-closed) — a corrupt retention record is a
// data-integrity event, and silently skipping it would drop the only restore handle from
// the index while the bytes persist on disk.
func TestRetentionIndex_RebuildFailsClosedOnMalformedRecord(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/retention.db"
	s1, err := NewRetentionStore(RetentionStoreConfig{DBPath: path})
	require.NoError(t, err)
	require.NoError(t, s1.Put(sampleEntry("good")))
	// Write garbage bytes directly under a key (white-box).
	require.NoError(t, s1.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).Put([]byte("bad"), []byte("{not json"))
	}))
	require.NoError(t, s1.Close())

	_, err = NewRetentionStore(RetentionStoreConfig{DBPath: path})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "retention index")
}

func TestRetentionIndex_Apply(t *testing.T) {
	s := newTestRetentionStore(t)
	s.mu.Lock()
	a := sampleEntry("u1") // tenant-a / active
	s.indexApply(nil, &a)
	moved := a
	moved.Status = RetentionStatusReaping
	s.indexApply(&a, &moved)  // active -> reaping
	s.indexApply(&moved, nil) // delete
	s.mu.Unlock()
	byTenant, byStatus := s.indexSnapshot()
	assert.Empty(t, byTenant)
	assert.Empty(t, byStatus)
}

// assertIndexConsistent asserts the live index equals a freshly-derived rebuild (per-key, order-insensitive).
func assertIndexConsistent(t *testing.T, s *RetentionStore) {
	t.Helper()
	liveT, liveS := s.indexSnapshot()
	byTenant, byStatus, _, err := s.scanIndex()
	require.NoError(t, err)
	want := func(m map[string]map[string]struct{}) map[string][]string {
		out := map[string][]string{}
		for k, set := range m {
			for u := range set {
				out[k] = append(out[k], u)
			}
		}
		return out
	}
	wantT, wantS := want(byTenant), want(byStatus)
	require.Equal(t, len(wantT), len(liveT))
	for k := range wantT {
		assert.ElementsMatch(t, wantT[k], liveT[k], "byTenant[%q]", k)
	}
	require.Equal(t, len(wantS), len(liveS))
	for k := range wantS {
		assert.ElementsMatch(t, wantS[k], liveS[k], "byStatus[%q]", k)
	}
}

func TestRetentionIndex_TransitionMutatorsStayConsistent(t *testing.T) {
	s := newTestRetentionStore(t)

	ok, err := s.PutActiveMerged(sampleEntry("u1"))
	require.NoError(t, err)
	require.True(t, ok)
	assertIndexConsistent(t, s)

	_, err = s.ClaimForRestore("u1", "newlease", time.Hour) // active -> restoring
	require.NoError(t, err)
	assertIndexConsistent(t, s)

	swapped, err := s.RevertToActive("u1", 1) // restoring -> active (gen=1 after claim)
	require.NoError(t, err)
	require.True(t, swapped)
	assertIndexConsistent(t, s)

	_, ok, err = s.MarkReapingIfActive("u1") // active -> reaping
	require.NoError(t, err)
	require.True(t, ok)
	assertIndexConsistent(t, s)

	okr, err := s.PutReaping(sampleEntry("u2")) // fresh-insert reaping
	require.NoError(t, err)
	require.True(t, okr)
	assertIndexConsistent(t, s)

	// Refused write must not change the index (PutReaping over an active record → ok=false).
	// Seed the active record via PutActiveMerged (an index-maintaining mutator wired in this
	// task) rather than blind Put — Put is not index-coupled until Task 4, so seeding with it
	// here would leave u3 on disk but absent from the live index, failing assertIndexConsistent
	// for a reason unrelated to PutReaping's refusal path.
	okSeed, err := s.PutActiveMerged(sampleEntry("u3"))
	require.NoError(t, err)
	require.True(t, okSeed)
	okr, err = s.PutReaping(sampleEntry("u3"))
	require.NoError(t, err)
	require.False(t, okr)
	assertIndexConsistent(t, s) // sole equality oracle — do NOT compare two raw indexSnapshot maps with assert.Equal (order-randomized)

	// DeleteIfActive on a non-active record is a no-op (u1 is reaping) → index unchanged.
	_, deleted, err := s.DeleteIfActive("u1")
	require.NoError(t, err)
	require.False(t, deleted)
	assertIndexConsistent(t, s)

	// DeleteIfActive on a fresh active record removes it from both partitions.
	okSeed, err = s.PutActiveMerged(sampleEntry("u4"))
	require.NoError(t, err)
	require.True(t, okSeed)
	_, deleted, err = s.DeleteIfActive("u4")
	require.NoError(t, err)
	require.True(t, deleted)
	assertIndexConsistent(t, s)

	// MarkReapingIfExpired: active+expired → reaping.
	expired := sampleEntry("u5")
	expired.CreatedAt = time.Now().Add(-48 * time.Hour)
	okSeed, err = s.PutActiveMerged(expired)
	require.NoError(t, err)
	require.True(t, okSeed)
	_, okExp, err := s.MarkReapingIfExpired("u5", time.Hour)
	require.NoError(t, err)
	require.True(t, okExp)
	assertIndexConsistent(t, s)
}

func TestRetentionIndex_ReIndexRebuildsAndFiresHook(t *testing.T) {
	dir := t.TempDir()
	var gotTrigger string
	var gotCount int
	s, err := NewRetentionStore(RetentionStoreConfig{
		DBPath:    dir + "/retention.db",
		OnReindex: func(count int, _ time.Duration, trigger string) { gotCount, gotTrigger = count, trigger },
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.NoError(t, s.Put(sampleEntry("u1")))
	require.NoError(t, s.ReIndex())
	assert.Equal(t, "manual", gotTrigger)
	assert.Equal(t, 1, gotCount)
	byTenant, _ := s.indexSnapshot()
	assert.ElementsMatch(t, []string{"u1"}, byTenant["tenant-a"])
}

func TestRetentionIndex_PutOverwriteAndDeleteStayConsistent(t *testing.T) {
	s := newTestRetentionStore(t)
	e := sampleEntry("u1")
	require.NoError(t, s.Put(e))
	assertIndexConsistent(t, s)

	e2 := e
	e2.Status = RetentionStatusReaping
	require.NoError(t, s.Put(e2)) // overwrite same UUID, different status
	_, byStatus := s.indexSnapshot()
	assert.NotContains(t, byStatus[RetentionStatusActive], "u1")
	assert.ElementsMatch(t, []string{"u1"}, byStatus[RetentionStatusReaping])
	assertIndexConsistent(t, s)

	require.NoError(t, s.Delete("u1"))
	assertIndexConsistent(t, s)
	byTenant, byStatus := s.indexSnapshot()
	assert.Empty(t, byTenant)
	assert.Empty(t, byStatus)

	require.NoError(t, s.Delete("nope")) // absent → no-op
	assertIndexConsistent(t, s)
}

func TestRetentionIndex_ListByIndexEquivalence(t *testing.T) {
	s := newTestRetentionStore(t)
	for i, st := range []string{RetentionStatusActive, RetentionStatusRestoring, RetentionStatusReaping, RetentionStatusActive} {
		e := sampleEntry(fmt.Sprintf("u%d", i))
		e.Status = st
		if i == 3 {
			e.Tenant = "tenant-b"
		}
		require.NoError(t, s.Put(e))
	}
	byT, err := s.ListByTenant("tenant-a")
	require.NoError(t, err)
	assert.Len(t, byT, 3)
	for _, e := range byT {
		assert.Equal(t, "tenant-a", e.Tenant)
	}
	restoring, err := s.ListRestoring()
	require.NoError(t, err)
	assert.Len(t, restoring, 1)
	assert.Equal(t, RetentionStatusRestoring, restoring[0].Status)
}

func TestRetentionIndex_ReadDropsStaleIndexEntry(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(sampleEntry("u1"))) // active

	// Force a stale index entry: claim → revert leaves on-disk status=active, but seed
	// byStatus[restoring] with u1 to simulate an index snapshot lagging a revert.
	s.mu.Lock()
	idxAdd(s.byStatus, RetentionStatusRestoring, "u1")
	s.mu.Unlock()

	got, err := s.ListRestoring()
	require.NoError(t, err)
	assert.Empty(t, got, "stale restoring index entry must be dropped: on-disk status is active")
}

func TestRetentionStore_Keys(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(sampleEntry("u1")))
	require.NoError(t, s.Put(sampleEntry("u2")))
	keys, err := s.Keys()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"u1", "u2"}, keys)
}

func TestRetentionIndex_ConcurrentReadersWriters(t *testing.T) {
	s := newTestRetentionStore(t)
	for i := 0; i < 50; i++ {
		require.NoError(t, s.Put(sampleEntry(fmt.Sprintf("u%d", i))))
	}

	stop := make(chan struct{})
	errCh := make(chan error, 64)
	fail := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			id := fmt.Sprintf("u%d", i%50)
			if _, err := s.ClaimForRestore(id, "n", time.Hour); err == nil {
				_, _ = s.RevertToActive(id, 1)
			}
			_, _, _ = s.MarkReapingIfActive(id)
		}
	}()

	var readersWG sync.WaitGroup
	for r := 0; r < 4; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for j := 0; j < 200; j++ {
				rs, err := s.ListRestoring()
				if err != nil {
					fail(err)
					return
				}
				for k := range rs {
					if rs[k].Status != RetentionStatusRestoring {
						fail(fmt.Errorf("ListRestoring returned status %q", rs[k].Status))
						return
					}
				}
				bt, err := s.ListByTenant("tenant-a")
				if err != nil {
					fail(err)
					return
				}
				for k := range bt {
					if bt[k].Tenant != "tenant-a" {
						fail(fmt.Errorf("ListByTenant returned tenant %q", bt[k].Tenant))
						return
					}
				}
				if j%50 == 0 {
					if err := s.ReIndex(); err != nil {
						fail(err)
						return
					}
				}
			}
		}()
	}

	readersWG.Wait() // readers run fixed loops
	close(stop)      // then stop the writer
	writerWG.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err) // assert on the test goroutine, after joins
	}

	assertIndexConsistent(t, s) // no permanent drift after churn
}
