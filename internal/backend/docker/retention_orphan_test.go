package docker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

const (
	orphanLeaseA      = "0192f1a0-4111-4abc-8def-000000000741"
	orphanLeaseB      = "0192f1a0-4222-4abc-8def-000000000742"
	orphanLeaseM      = "0192f1a0-4333-4abc-8def-000000000743"
	orphanLeaseV      = "0192f1a0-4444-4abc-8def-000000000744"
	orphanRestoreFrom = "0192f1a0-4555-4abc-8def-000000000745"
	orphanRestoreTo   = "0192f1a0-4666-4abc-8def-000000000746"
)

func orphanRetainedVolume(leaseUUID string, index int) string {
	return retainedName(canonicalVolumeName(leaseUUID, "app", index))
}

func bindRetentionOrphanPrunerForTest(t *testing.T, b *Backend) {
	t.Helper()
	pruner, err := newRetentionOrphanPruner(b)
	require.NoError(t, err)
	b.orphanPruner = pruner
}

// newOrphanReconcileBackend builds a Backend with a real retention store, a
// controllable volume manager, and orphan pruning enabled (N=confirmations).
// presentVolumes is what volumes.List() returns; listErr (if set) makes it fail.
// rootExists controls whether cfg.VolumeDataPath points at a real dir.
func newOrphanReconcileBackend(t *testing.T, confirmations int, rootExists bool, presentVolumes []string, listErr error) (*Backend, *shared.RetentionStore) {
	t.Helper()
	if !rootExists && listErr == nil {
		// Production ListForProof reports a configured missing root as uncertainty;
		// the mock must model that capability contract rather than a confident empty.
		listErr = os.ErrNotExist
	}
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = confirmations
	if rootExists {
		b.cfg.VolumeDataPath = t.TempDir() // exists → G2 passes
	} else {
		b.cfg.VolumeDataPath = filepath.Join(t.TempDir(), "missing") // absent → G2 skips
	}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if listErr != nil {
				return nil, listErr
			}
			return presentVolumes, nil
		},
	}
	s := attachRetentionStore(t, b)
	bindRetentionOrphanPrunerForTest(t, b)
	return b, s
}

func putActiveRetention(t *testing.T, s *shared.RetentionStore, lease string, volumeNames []string) {
	t.Helper()
	require.NoError(t, putRetentionForTest(t, s, shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "t1",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: volumeNames,
		CreatedAt:           time.Now(),
	}))
}

// getRetention fetches a record and asserts the store read itself succeeded, so a
// Get error can't masquerade as "record pruned" (a nil result on error). Use this
// instead of `rec, _ := s.Get(...)` for prune assertions.
func getRetention(t *testing.T, s *shared.RetentionStore, lease string) *shared.RetentionEntry {
	t.Helper()
	rec, err := s.Get(canonicalRetentionFixtureUUID(lease))
	require.NoError(t, err)
	return rec
}

// Test #1 + #12: absent volumes prune exactly at sweep N; present ones never do.
func TestReconcileOrphaned_PrunesAfterNSweeps(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 3, true, []string{orphanRetainedVolume(orphanLeaseB, 0)}, nil)
	putActiveRetention(t, s, orphanLeaseA, []string{orphanRetainedVolume(orphanLeaseA, 0)}) // absent
	putActiveRetention(t, s, orphanLeaseB, []string{orphanRetainedVolume(orphanLeaseB, 0)}) // present

	before := testutil.ToFloat64(retentionOrphansPrunedTotal)

	// Sweeps 1 and 2: not yet confirmed.
	for i := 0; i < 2; i++ {
		pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
		got := getRetention(t, s, orphanLeaseA)
		assert.NotNil(t, got, "uA must survive before N sweeps")
	}
	// Sweep 3: confirmed → pruned.
	pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)

	goneA := getRetention(t, s, orphanLeaseA)
	assert.Nil(t, goneA, "uA pruned after N sweeps")
	keptB := getRetention(t, s, orphanLeaseB)
	assert.NotNil(t, keptB, "uB (present volume) never pruned")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphansPrunedTotal))
}

// Test #3: a volume reappearing mid-streak resets confirmation.
func TestReconcileOrphaned_ReappearanceResetsStreak(t *testing.T) {
	present := []string{} // start absent
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = 3
	b.cfg.VolumeDataPath = t.TempDir()
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return present, nil }}
	s := attachRetentionStore(t, b)
	bindRetentionOrphanPrunerForTest(t, b)
	putActiveRetention(t, s, orphanLeaseA, []string{orphanRetainedVolume(orphanLeaseA, 0)})

	_, err := b.reconcileOrphanedRetentionsUsing(context.Background()) // streak 1
	require.NoError(t, err)
	_, err = b.reconcileOrphanedRetentionsUsing(context.Background()) // streak 2
	require.NoError(t, err)
	present = []string{orphanRetainedVolume(orphanLeaseA, 0)}         // volume reappears
	_, err = b.reconcileOrphanedRetentionsUsing(context.Background()) // streak reset → 0
	require.NoError(t, err)
	present = []string{}                                                    // absent again
	pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background()) // streak 1, NOT >= 3
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got := getRetention(t, s, orphanLeaseA)
	assert.NotNil(t, got, "reset streak must prevent prune")
}

// Test #4: a restoring record with absent volumes is never pruned.
func TestReconcileOrphaned_SkipsRestoringRecords(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, nil) // N=1: would prune immediately if active
	putRestoringRetention(t, s, shared.RetentionEntry{
		OriginalLeaseUUID:   orphanRestoreFrom,
		Tenant:              "t1",
		Status:              shared.RetentionStatusRestoring,
		NewLeaseUUID:        orphanRestoreTo,
		RetainedVolumeNames: []string{orphanRetainedVolume(orphanRestoreFrom, 0)},
		CreatedAt:           time.Now(),
	})
	pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got := getRetention(t, s, orphanRestoreFrom)
	assert.NotNil(t, got, "restoring record must never be pruned")
}

// Test #5: a missing volume root skips the whole pass (fail-safe) forever.
func TestReconcileOrphaned_MissingRootSkips(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, false /*root missing*/, nil, nil)
	putActiveRetention(t, s, orphanLeaseA, []string{orphanRetainedVolume(orphanLeaseA, 0)})
	before := testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError))

	for i := 0; i < 5; i++ {
		pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
		require.Error(t, err)
		assert.Equal(t, 0, pruned)
	}
	got := getRetention(t, s, orphanLeaseA)
	assert.NotNil(t, got, "missing root must prevent any prune")
	assert.Equal(t, before+5, testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError)))
}

// Test #7: a List() error skips the pass (fail-safe) and surfaces the error.
func TestReconcileOrphaned_ListErrorSkips(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, errors.New("list boom"))
	putActiveRetention(t, s, orphanLeaseA, []string{orphanRetainedVolume(orphanLeaseA, 0)})
	before := testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError))

	pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
	require.Error(t, err)
	assert.Equal(t, 0, pruned)
	got := getRetention(t, s, orphanLeaseA)
	assert.NotNil(t, got, "list error must prevent prune")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError)))
}

// Test #10: a volume-bearing record under an UNCONFIGURED root is never pruned.
func TestReconcileOrphaned_UnconfiguredRootSkipsVolumeRecords(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = 1
	b.cfg.VolumeDataPath = ""        // noop manager / unconfigured
	b.volumes = &noopVolumeManager{} // List() → (nil,nil)
	s := attachRetentionStore(t, b)
	bindRetentionOrphanPrunerForTest(t, b)
	putActiveRetention(t, s, orphanLeaseV, []string{orphanRetainedVolume(orphanLeaseV, 0)}) // has volumes

	for i := 0; i < 3; i++ {
		pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got := getRetention(t, s, orphanLeaseV)
	assert.NotNil(t, got, "volume-bearing record unverifiable without a root → never pruned")
}

// Test #11: N=0 is the kill-switch — never prunes, records the disabled skip.
func TestReconcileOrphaned_DisabledKillSwitch(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 0 /*disabled*/, true, nil, nil)
	putActiveRetention(t, s, orphanLeaseA, []string{orphanRetainedVolume(orphanLeaseA, 0)})
	before := testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipDisabled))

	pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got := getRetention(t, s, orphanLeaseA)
	assert.NotNil(t, got, "kill-switch must prevent prune")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphanSkipsTotal.WithLabelValues(orphanSkipDisabled)))
}

// A multi-volume active record with ONE volume still present is never pruned
// (guards the all-volumes-absent semantics at the reconcile level, not just the helper).
func TestReconcileOrphaned_PartialPresenceNeverPruned(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, []string{orphanRetainedVolume(orphanLeaseM, 1)}, nil) // only instance 1 present
	putActiveRetention(t, s, orphanLeaseM, []string{orphanRetainedVolume(orphanLeaseM, 0), orphanRetainedVolume(orphanLeaseM, 1)})
	for i := 0; i < 3; i++ {
		pruned, err := b.reconcileOrphanedRetentionsUsing(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got := getRetention(t, s, orphanLeaseM)
	assert.NotNil(t, got, "a record with any present volume must never be pruned")
}

// Wiring: runRetentionSweep invokes the orphan reconcile. With N=1 and an absent
// volume, a single sweep prunes the orphaned record.
func TestRunRetentionSweep_PrunesOrphans(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil /*absent*/, nil)
	b.cfg.RetentionMaxAge = 90 * 24 * time.Hour // keep the grace reaper a no-op for this record
	putActiveRetention(t, s, orphanLeaseA, []string{orphanRetainedVolume(orphanLeaseA, 0)})

	require.NoError(t, b.runRetentionSweep(context.Background()))

	got := getRetention(t, s, orphanLeaseA)
	assert.Nil(t, got, "runRetentionSweep must prune the orphaned record")
}
