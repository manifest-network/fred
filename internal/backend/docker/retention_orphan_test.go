package docker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestVolumeRootUnverifiable(t *testing.T) {
	// present root → verifiable (do not skip)
	assert.False(t, volumeRootUnverifiable(true, nil))
	// absent root (pathExists → false,nil) → unverifiable (skip)
	assert.True(t, volumeRootUnverifiable(false, nil))
	// unreadable root (non-ENOENT stat error → false,err) → unverifiable (skip).
	// This pins the branch so a future "IsNotExist-only" simplification fails.
	assert.True(t, volumeRootUnverifiable(false, errors.New("permission denied")))
}

func TestAllVolumesAbsent(t *testing.T) {
	present := map[string]bool{"fred-retained-u1-app-0": true}
	// every name present → not absent
	assert.False(t, allVolumesAbsent([]string{"fred-retained-u1-app-0"}, present))
	// a name missing → absent
	assert.True(t, allVolumesAbsent([]string{"fred-retained-u2-app-0"}, present))
	// mixed (one present) → not absent
	assert.False(t, allVolumesAbsent([]string{"fred-retained-u1-app-0", "fred-retained-u2-app-0"}, present))
	// empty name set → vacuously absent
	assert.True(t, allVolumesAbsent(nil, present))
}

// newOrphanReconcileBackend builds a Backend with a real retention store, a
// controllable volume manager, and orphan pruning enabled (N=confirmations).
// presentVolumes is what volumes.List() returns; listErr (if set) makes it fail.
// rootExists controls whether cfg.VolumeDataPath points at a real dir.
func newOrphanReconcileBackend(t *testing.T, confirmations int, rootExists bool, presentVolumes []string, listErr error) (*Backend, *shared.RetentionStore) {
	t.Helper()
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = confirmations
	if rootExists {
		b.cfg.VolumeDataPath = t.TempDir() // exists → G2 passes
	} else {
		b.cfg.VolumeDataPath = filepath.Join(t.TempDir(), "missing") // absent → G2 skips
	}
	b.orphanStreaks = make(map[string]int)
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if listErr != nil {
				return nil, listErr
			}
			return presentVolumes, nil
		},
	}
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	return b, s
}

func putActiveRetention(t *testing.T, s *shared.RetentionStore, lease string, volumeNames []string) {
	t.Helper()
	require.NoError(t, s.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "t1",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: volumeNames,
		CreatedAt:           time.Now(),
	}))
}

// Test #1 + #12: absent volumes prune exactly at sweep N; present ones never do.
func TestReconcileOrphaned_PrunesAfterNSweeps(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 3, true, []string{"fred-retained-uB-app-0"}, nil)
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"}) // absent
	putActiveRetention(t, s, "uB", []string{"fred-retained-uB-app-0"}) // present

	before := testutil.ToFloat64(retentionOrphansPrunedTotal)

	// Sweeps 1 and 2: not yet confirmed.
	for i := 0; i < 2; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
		got, _ := s.Get("uA")
		assert.NotNil(t, got, "uA must survive before N sweeps")
	}
	// Sweep 3: confirmed → pruned.
	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)

	goneA, _ := s.Get("uA")
	assert.Nil(t, goneA, "uA pruned after N sweeps")
	keptB, _ := s.Get("uB")
	assert.NotNil(t, keptB, "uB (present volume) never pruned")
	assert.NotContains(t, b.orphanStreaks, "uB", "present-volume record must not accumulate a streak")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphansPrunedTotal))
}

// Test #3: a volume reappearing mid-streak resets confirmation.
func TestReconcileOrphaned_ReappearanceResetsStreak(t *testing.T) {
	present := []string{} // start absent
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = 3
	b.cfg.VolumeDataPath = t.TempDir()
	b.orphanStreaks = make(map[string]int)
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return present, nil }}
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})

	_, _ = b.reconcileOrphanedRetentions()         // streak 1
	_, _ = b.reconcileOrphanedRetentions()         // streak 2
	present = []string{"fred-retained-uA-app-0"}   // volume reappears
	_, _ = b.reconcileOrphanedRetentions()         // streak reset → 0
	present = []string{}                           // absent again
	pruned, err := b.reconcileOrphanedRetentions() // streak 1, NOT >= 3
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "reset streak must prevent prune")
}

// Test #4: a restoring record with absent volumes is never pruned.
func TestReconcileOrphaned_SkipsRestoringRecords(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, nil) // N=1: would prune immediately if active
	require.NoError(t, s.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "uR",
		Tenant:              "t1",
		Status:              shared.RetentionStatusRestoring,
		NewLeaseUUID:        "uNew",
		RetainedVolumeNames: []string{"fred-retained-uR-app-0"},
		CreatedAt:           time.Now(),
	}))
	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uR")
	assert.NotNil(t, got, "restoring record must never be pruned")
}

// Test #5: a missing volume root skips the whole pass (fail-safe) forever.
func TestReconcileOrphaned_MissingRootSkips(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, false /*root missing*/, nil, nil)
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})
	before := testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipMissingRoot))

	for i := 0; i < 5; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "missing root must prevent any prune")
	assert.Equal(t, before+5, testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipMissingRoot)))
}

// Test #7: a List() error skips the pass (fail-safe) and surfaces the error.
func TestReconcileOrphaned_ListErrorSkips(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, errors.New("list boom"))
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})
	before := testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipListError))

	pruned, err := b.reconcileOrphanedRetentions()
	require.Error(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "list error must prevent prune")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipListError)))
}

// Test #9: an empty-name (legacy zero-volume) record prunes after N regardless of root.
func TestReconcileOrphaned_EmptyNamesPruned(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, nil)
	putActiveRetention(t, s, "uLegacy", nil) // no volume names
	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)
	got, _ := s.Get("uLegacy")
	assert.Nil(t, got, "legacy zero-volume record pruned")
}

// Test #10: a volume-bearing record under an UNCONFIGURED root is never pruned.
func TestReconcileOrphaned_UnconfiguredRootSkipsVolumeRecords(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = 1
	b.cfg.VolumeDataPath = "" // noop manager / unconfigured
	b.orphanStreaks = make(map[string]int)
	b.volumes = &noopVolumeManager{} // List() → (nil,nil)
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	putActiveRetention(t, s, "uV", []string{"fred-retained-uV-app-0"}) // has volumes

	for i := 0; i < 3; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got, _ := s.Get("uV")
	assert.NotNil(t, got, "volume-bearing record unverifiable without a root → never pruned")
}

// Test #11: N=0 is the kill-switch — never prunes, records the disabled skip.
func TestReconcileOrphaned_DisabledKillSwitch(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 0 /*disabled*/, true, nil, nil)
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})
	before := testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipDisabled))

	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "kill-switch must prevent prune")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipDisabled)))
}

// A multi-volume active record with ONE volume still present is never pruned
// (guards the all-volumes-absent semantics at the reconcile level, not just the helper).
func TestReconcileOrphaned_PartialPresenceNeverPruned(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, []string{"fred-retained-uM-app-1"}, nil) // only instance 1 present
	putActiveRetention(t, s, "uM", []string{"fred-retained-uM-app-0", "fred-retained-uM-app-1"})
	for i := 0; i < 3; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got, _ := s.Get("uM")
	assert.NotNil(t, got, "a record with any present volume must never be pruned")
}

// Test #8: a real restore (ClaimForRestore active→restoring) racing the reconcile's
// DeleteIfActive must never corrupt state — the record ends EITHER pruned (reconcile
// won) OR restoring under the new lease (restore won), never deleted-while-restoring.
//
// The post-state either/or assertion (testify) is the proof of correctness; -race is
// a regression guard for any future shared backend state (today both contended methods
// run inside bolt.DB.Update, which serializes them, so -race rarely fires). The loop
// re-creates a clean ACTIVE record each iteration so both bbolt-serialized interleavings
// recur within a single run; N=1 so one reconcile sweep attempts the delete.
func TestReconcileOrphaned_ConcurrentRestoreRace(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil /*all absent*/, nil)

	var sawPruned, sawRestored bool
	for i := 0; i < 100; i++ {
		require.NoError(t, s.Put(shared.RetentionEntry{
			OriginalLeaseUUID:   "uA",
			Tenant:              "t1",
			Status:              shared.RetentionStatusActive,
			RetainedVolumeNames: []string{"fred-retained-uA-app-0"},
			CreatedAt:           time.Now(),
		}))
		b.orphanStreaks = make(map[string]int) // start the streak fresh so N=1 deletes this sweep

		var wg sync.WaitGroup
		wg.Add(2)
		var claimErr error
		go func() { defer wg.Done(); _, _ = b.reconcileOrphanedRetentions() }()
		go func() { defer wg.Done(); _, claimErr = s.ClaimForRestore("uA", "uNew", time.Hour) }()
		wg.Wait()

		rec, err := s.Get("uA")
		require.NoError(t, err)
		if claimErr == nil {
			// Restore won the CAS: record survives in restoring state — never deleted-while-restoring.
			require.NotNil(t, rec, "restore claimed the record; reconcile must not have deleted it")
			assert.Equal(t, shared.RetentionStatusRestoring, rec.Status)
			assert.Equal(t, "uNew", rec.NewLeaseUUID)
			sawRestored = true
		} else {
			// Reconcile won: record pruned, claim observed it gone.
			require.Nil(t, rec, "reconcile pruned the record; claim must have failed")
			sawPruned = true
		}
	}
	// Both interleavings preserve the invariant; over 100 iterations we expect each at
	// least once. Logged, not hard-asserted, to avoid scheduler-dependent flakiness.
	t.Logf("concurrent race: observed pruned=%v restored=%v", sawPruned, sawRestored)
}

// Wiring: runRetentionSweep invokes the orphan reconcile. With N=1 and an absent
// volume, a single sweep prunes the orphaned record.
func TestRunRetentionSweep_PrunesOrphans(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil /*absent*/, nil)
	b.cfg.RetentionMaxAge = 90 * 24 * time.Hour // keep the grace reaper a no-op for this record
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})

	require.NoError(t, b.runRetentionSweep(context.Background()))

	got, _ := s.Get("uA")
	assert.Nil(t, got, "runRetentionSweep must prune the orphaned record")
}
