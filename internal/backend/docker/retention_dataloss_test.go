package docker

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// N-03 / ENG-501: the orphan-record pruner must not treat a give-up-diverged
// record as orphaned. The record lists the fred-retained-* names, but on a
// persistent rename failure the volume is still on disk under its canonical
// fred-{lease}-* name — checking only the retained name would prune the record
// and let a later boot destroy the still-intact data.
func TestAllVolumesAbsent_ProtectsDivergedCanonical(t *testing.T) {
	retained := "fred-retained-u1-app-0"
	canonical := canonicalFromRetained(retained)
	require.NotEqual(t, retained, canonical)

	assert.False(t, allVolumesAbsent([]string{retained}, map[string]bool{canonical: true}),
		"canonical present ⇒ the record's data is on disk ⇒ not orphaned (ENG-501)")
	assert.False(t, allVolumesAbsent([]string{retained}, map[string]bool{retained: true}),
		"retained name present ⇒ not orphaned")
	assert.True(t, allVolumesAbsent([]string{retained}, map[string]bool{}),
		"neither present ⇒ genuinely absent")
}

// N-06 / ENG-512: reconcileRestoring must defer for a live provision in the
// Updating state (a running new lease whose restore record merely lingered past
// a failed terminal Delete) rather than tearing it down via the orphaned arm.
func TestReconcileRestoring_DefersForUpdating(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2",
			Status:    backend.ProvisionStatusUpdating,
		}},
	})
	rs := attachRetentionStore(t, b)

	downCalled := false
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { downCalled = true; return nil },
	}
	b.volumes = &mockVolumeManager{}

	e := shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		NewLeaseUUID:        "u2",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          3,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
	}
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	assert.False(t, downCalled,
		"a live lease at Updating is not a crashed restore; compose Down must NOT be called (ENG-512)")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusRestoring, entry.Status)
	assert.Equal(t, 3, entry.Generation, "orphaned arm / RevertToActive must NOT fire")

	b.provisionsMu.RLock()
	_, hasU2 := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.True(t, hasU2, "the live provision must NOT be removed")
}

// M-03 / ENG-505: cleanupOrphanedVolumes must not destroy the volume of a lease
// that still has an active release (successfully provisioned, containers removed
// out-of-band) — only a genuine create-crash leak (no release) is reaped.
func TestCleanupOrphanedVolumes_ProtectsLiveLeaseWithActiveRelease(t *testing.T) {
	live := "0192f1a0-1111-7abc-8def-000000000001" // active release, containers removed
	leak := "0192f1a0-2222-7abc-8def-000000000002" // create-crash leak, no release
	liveVol := "fred-" + live + "-app-0"
	leakVol := "fred-" + leak + "-app-0"

	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "rel.db")})
	require.NoError(t, err)
	defer relStore.Close()
	require.NoError(t, relStore.Append(live, shared.Release{
		Manifest: []byte(`{"image":"nginx:1.25"}`), Image: "nginx", Status: "active", CreatedAt: time.Now(),
	}))

	var destroyed []string
	b := newBackendForTest(&mockDockerClient{}, nil) // no provisions (containers gone)
	b.releaseStore = relStore
	attachRetentionStore(t, b) // empty retention store
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{liveVol, leakVol}, nil },
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))

	assert.NotContains(t, destroyed, liveVol,
		"a live lease's volume (active release) must NOT be destroyed (ENG-505)")
	assert.Contains(t, destroyed, leakVol,
		"a genuine create-crash leak (no active release) is still reaped")
}

// leaseUUIDFromVolumeName must match ONLY canonical managed names
// (fred-{uuid}-{service}-{idx}), not a bare fred-{uuid}- prefix, so the reaper
// can't mistake an unrelated directory for a protected lease volume (ENG-505).
func TestLeaseUUIDFromVolumeName(t *testing.T) {
	u := "0192f1a0-1111-7abc-8def-000000000001"
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"fred-" + u + "-app-0", u, true},
		{"fred-" + u + "-web-1-0", u, true}, // hyphenated service name
		{"fred-" + u + "-", "", false},      // missing service + idx
		{"fred-" + u + "-foo", "", false},   // missing numeric idx
		{"fred-" + u + "-app-x", "", false}, // non-numeric idx
		{"fred-not-a-uuid-app-0", "", false},
		{"other-" + u + "-app-0", "", false},
		{"fred-retained-" + u + "-app-0", "", false},
	}
	for _, c := range cases {
		got, ok := leaseUUIDFromVolumeName(c.in)
		assert.Equal(t, c.ok, ok, c.in)
		assert.Equal(t, c.want, got, c.in)
	}
}

func eng523RestoringRecord(orig, newLease string) shared.RetentionEntry {
	return shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		NewLeaseUUID:        newLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		StackManifest:       restoreStackManifest(), // realistic, parseable payload (not a nil "null" marshal)
		RetainedVolumeNames: []string{"fred-retained-" + orig + "-app-0"},
	}
}

// ENG-523: on a successful restore whose release Append FAILS, finalizeRestoredLease
// must NOT delete the retention record. The restoring record is the adopted volume's
// finalizer — cleanupOrphanedVolumes protects its canonical volume and
// reconcileRestoring finalizes it once the lease is Ready. Dropping it would leave the
// lease with neither a release nor a retention record, so a later boot's orphan reaper
// would destroy live tenant data (the exact gap ENG-505 closes for the common case).
func TestFinalizeRestoredLease_KeepsFinalizerWhenReleaseAppendFails(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000001"
	newLease := "0192f1a0-2222-7abc-8def-000000000002"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)

	// A release store whose Append will fail: open then Close so db.Update returns
	// ErrDatabaseNotOpen — a realistic transient bbolt write failure.
	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "rel.db")})
	require.NoError(t, err)
	require.NoError(t, relStore.Close())
	b.releaseStore = relStore

	e := eng523RestoringRecord(orig, newLease)
	require.NoError(t, rs.Put(e))

	before := testutil.ToFloat64(restoreFinalizerPendingTotal)
	b.finalizeRestoredLease(newLease, &e, slog.Default())

	got, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, got,
		"ENG-523: retention record (the adopted volume's finalizer) must NOT be deleted when the release Append failed")
	assert.Equal(t, shared.RetentionStatusRestoring, got.Status)
	assert.Equal(t, before+1, testutil.ToFloat64(restoreFinalizerPendingTotal),
		"a kept-pending finalizer must be counted for observability (ENG-523)")
}

// ENG-523 (reaper half): a lingering restoring record — the exact state a restore
// leaves when its release Append failed and finalizeRestoredLease kept the record as
// the adopted volume's finalizer — must protect the adopted new-lease canonical
// volume from cleanupOrphanedVolumes even with NO active release. Together with
// TestFinalizeRestoredLease_KeepsFinalizerWhenReleaseAppendFails this closes the
// ENG-523 window end-to-end: the record is kept, and the reaper honors it.
func TestCleanupOrphanedVolumes_ProtectsAdoptedVolumeOfLingeringRestoringRecord(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000005"
	newLease := "0192f1a0-2222-7abc-8def-000000000006"
	retained := "fred-retained-" + orig + "-app-0"
	adopted := retainedToNewCanonical(retained, orig, newLease) // fred-{newLease}-app-0
	leak := "fred-0192f1a0-3333-7abc-8def-000000000007-app-0"   // unrelated create-crash leak

	var destroyed []string
	b := newBackendForTest(&mockDockerClient{}, nil) // no live provisions, no release store
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		NewLeaseUUID:        newLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		RetainedVolumeNames: []string{retained},
	}))
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{adopted, leak}, nil },
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))

	assert.NotContains(t, destroyed, adopted,
		"ENG-523: the adopted volume of a lingering restoring record (finalizer) must NOT be reaped")
	assert.Contains(t, destroyed, leak,
		"an unrelated create-crash leak (no record, no release) is still reaped")
}

// Companion happy path: when the release IS durably recorded, the finalizer (retention
// record) is dropped and the new lease carries an active release.
func TestFinalizeRestoredLease_DropsFinalizerWhenReleaseRecorded(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000003"
	newLease := "0192f1a0-2222-7abc-8def-000000000004"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)

	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "rel.db")})
	require.NoError(t, err)
	defer relStore.Close()
	b.releaseStore = relStore

	e := eng523RestoringRecord(orig, newLease)
	require.NoError(t, rs.Put(e))

	b.finalizeRestoredLease(newLease, &e, slog.Default())

	got, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, got, "retention record must be dropped once the release is durably recorded")

	rel, err := relStore.LatestActive(newLease)
	require.NoError(t, err)
	require.NotNil(t, rel, "the new lease must have an active release recorded")
}

// ENG-523 (Copilot #174): reconcileRestoring is the retry path for the finalizer. When
// the new lease is Ready but no active release is durable yet (finalizeRestoredLease
// kept the record because Append failed) and the release store is STILL failing, it
// must KEEP the record restoring — a bare Delete here would re-open the ENG-523 window
// (no release + no record → reapable on the next boot).
func TestReconcileRestoring_ReadyButReleaseUnrecordable_KeepsFinalizer(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000008"
	newLease := "0192f1a0-2222-7abc-8def-000000000009"

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{LeaseUUID: newLease, Status: backend.ProvisionStatusReady}},
	})
	rs := attachRetentionStore(t, b)

	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "rel.db")})
	require.NoError(t, err)
	require.NoError(t, relStore.Close()) // Append + LatestActive fail
	b.releaseStore = relStore

	e := eng523RestoringRecord(orig, newLease)
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	got, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, got,
		"reconcileRestoring must keep the finalizer when the release can't be durably recorded (ENG-523)")
	assert.Equal(t, shared.RetentionStatusRestoring, got.Status)
}

// Companion: when the release store works, reconcileRestoring's Ready path RECORDS the
// missing release (self-heal) and only then drops the finalizer.
func TestReconcileRestoring_ReadyRecordsMissingReleaseThenDropsFinalizer(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-00000000000a"
	newLease := "0192f1a0-2222-7abc-8def-00000000000b"

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{LeaseUUID: newLease, Status: backend.ProvisionStatusReady}},
	})
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b) // working, empty: no active release yet (Append had failed)

	e := eng523RestoringRecord(orig, newLease)
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	got, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, got, "finalizer dropped once the release is recorded")

	rel, err := relStore.LatestActive(newLease)
	require.NoError(t, err)
	require.NotNil(t, rel, "reconcileRestoring must record the missing active release before dropping the finalizer")
}

// finalizeRestoredLease is idempotent: when an active release already exists (doRestore
// recorded it but its record Delete failed), the retry drops the record WITHOUT
// appending a duplicate active release.
func TestFinalizeRestoredLease_IdempotentWhenReleaseAlreadyRecorded(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-00000000000c"
	newLease := "0192f1a0-2222-7abc-8def-00000000000d"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b)
	require.NoError(t, relStore.Append(newLease, shared.Release{
		Manifest: []byte(`{"services":{}}`), Image: "stack", Status: "active", CreatedAt: time.Now(),
	}))

	e := eng523RestoringRecord(orig, newLease)
	require.NoError(t, rs.Put(e))

	b.finalizeRestoredLease(newLease, &e, slog.Default())

	got, err := rs.Get(orig)
	require.NoError(t, err)
	assert.Nil(t, got, "record dropped once the release is durable")

	releases, err := relStore.List(newLease)
	require.NoError(t, err)
	assert.Len(t, releases, 1, "must not append a duplicate active release (idempotent)")
}

// M-01 (PR #174 review): reconcileRestoring's retry must re-record the LIVE provision's
// CURRENT manifest, not the retention record's frozen (pre-Update) one. Otherwise a
// sweep landing after a tenant Update (whose own best-effort release write also failed)
// records a stale release and silently reverts the update on the next restart.
func TestReconcileRestoring_ReRecordsLiveManifestNotFrozenRecord(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-00000000000e"
	newLease := "0192f1a0-2222-7abc-8def-00000000000f"

	// The lease was Updated after the restore: its live provision carries a NEW manifest
	// (redis:7), distinct from the retention record's frozen restore manifest (nginx:latest).
	updated := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		manifest.DefaultServiceName: {Image: "redis:7"},
	}}
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: newLease, Status: backend.ProvisionStatusReady, StackManifest: updated,
		}},
	})
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b)

	e := eng523RestoringRecord(orig, newLease) // e.StackManifest = restoreStackManifest() = nginx:latest (frozen)
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	rel, err := relStore.LatestActive(newLease)
	require.NoError(t, err)
	require.NotNil(t, rel, "reconcileRestoring must record the missing release")
	assert.Contains(t, string(rel.Manifest), "redis:7",
		"must record the LIVE (updated) manifest, not the frozen record manifest (M-01)")
	assert.NotContains(t, string(rel.Manifest), "nginx",
		"must NOT re-record the retention record's frozen pre-Update manifest (M-01)")
}

// --- ENG-659: the reaping finalizer re-checks ownership at DESTROY time ----------------
//
// A reaping tombstone is a name-keyed scheduled destroy persisted in bbolt. ENG-647 (PR
// #217) stopped recordGiveUpLeak from WRITING an in-flight restore's adopted volume into
// one, but records written by an older binary survive the upgrade and nothing rewrites
// them — so the reader must re-check. These pin the reader half.

// seedClaimedTombstone wires the collision: a RESTORING record for orig adopted into
// newLease, plus a REAPING tombstone keyed at newLease that names both the adopted volume
// (which is orig's data wearing newLease's canonical name) and newLease's own leak — the
// exact shape a pre-ENG-647 give-up produced by prefix-collecting fred-{newLease}-*.
// Returns (adopted, ownLeak).
func seedClaimedTombstone(t *testing.T, rs *shared.RetentionStore, orig, newLease string) (string, string) {
	t.Helper()
	retained := "fred-retained-" + orig + "-app-0"
	adopted := retainedToNewCanonical(retained, orig, newLease)
	ownLeak := canonicalVolumeName(newLease, "app", 1)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		NewLeaseUUID:        newLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		RetainedVolumeNames: []string{retained},
	}))
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   newLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: []string{adopted, ownLeak},
	}))
	return adopted, ownLeak
}

// The core pin: executing a legacy tombstone must not destroy the volume an in-flight
// restore adopted, and must leave that restore's record intact and restorable.
func TestDestroyReapingVolumes_SkipsVolumeClaimedByRestoringRecord(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000010"
	newLease := "0192f1a0-2222-7abc-8def-000000000011"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	adopted, ownLeak := seedClaimedTombstone(t, rs, orig, newLease)

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	skipBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipRestoreClaimed))

	ok := b.destroyReapingVolumes(context.Background(), newLease, []string{adopted, ownLeak})

	assert.False(t, ok, "a skipped name means the record was not fully reaped")
	assert.NotContains(t, destroyed, adopted,
		"the volume an in-flight restore adopted is another lease's retained data — "+
			"destroying it is unrecoverable and kills that restore (ENG-659)")
	assert.Equal(t, []string{ownLeak}, destroyed,
		"this lease's OWN leak is still reaped; the skip is per-name, not per-record")

	tomb, err := rs.Get(newLease)
	require.NoError(t, err)
	require.NotNil(t, tomb, "the tombstone is the retry vehicle and must survive the skip")
	assert.Equal(t, shared.RetentionStatusReaping, tomb.Status)

	restoring, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, restoring, "the restoring record must be untouched, so the restore is still possible")
	assert.Equal(t, shared.RetentionStatusRestoring, restoring.Status)
	assert.Equal(t, []string{"fred-retained-" + orig + "-app-0"}, restoring.RetainedVolumeNames)

	assert.Equal(t, leakBefore, testutil.ToFloat64(retentionLeakedTotal),
		"a deliberate skip is not a leak: counting it would arm BackendRetentionLeaked on a healthy self-heal")
	assert.Equal(t, skipBefore+1, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipRestoreClaimed)))
}

// The same collision through the BOOT arm (reconcileRetentions), which is the actual
// upgrade path: the first sweep after the upgrade is where a legacy tombstone is executed.
// The new lease gets a live Provisioning provision so reconcileRestoring returns at its
// non-Failed guard — the restoring record then stays claimed for the whole pass regardless
// of which bbolt key order the two records are visited in.
func TestReconcileRetentions_BootReapingArm_SkipsRestoreClaimedVolume(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000012"
	newLease := "0192f1a0-2222-7abc-8def-000000000013"

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		newLease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: newLease, Tenant: "tenant-a", Status: backend.ProvisionStatusProvisioning, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	adopted, ownLeak := seedClaimedTombstone(t, rs, orig, newLease)

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	require.NoError(t, b.reconcileRetentions(context.Background()))

	assert.NotContains(t, destroyed, adopted,
		"the boot reaping arm executes stored records verbatim — it must honor the claim too (ENG-659)")
	assert.Contains(t, destroyed, ownLeak)

	tomb, err := rs.Get(newLease)
	require.NoError(t, err)
	assert.NotNil(t, tomb, "tombstone kept for the next sweep")
	restoring, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, restoring)
	assert.Equal(t, shared.RetentionStatusRestoring, restoring.Status)
}

// Fail-safe error branch: an unreadable retention store means ownership cannot be proven
// for ANY name, so nothing is destroyed — the same posture cleanupOrphanedVolumes and
// recordGiveUpLeak take. Over-keeping is recoverable; over-destroying is not.
func TestDestroyReapingVolumes_ClaimLookupError_DestroysNothing(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000014"
	newLease := "0192f1a0-2222-7abc-8def-000000000015"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	adopted, ownLeak := seedClaimedTombstone(t, rs, orig, newLease)
	require.NoError(t, rs.Close()) // every retention read now fails

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when the claim set is unreadable — it may be another lease's data", id)
			return nil
		},
	}
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	skipBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	ok := b.destroyReapingVolumes(context.Background(), newLease, []string{adopted, ownLeak})

	assert.False(t, ok, "nothing was destroyed, so the record cannot be dropped")
	assert.Equal(t, skipBefore+1, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
	assert.Equal(t, leakBefore, testutil.ToFloat64(retentionLeakedTotal),
		"a fail-safe bailout abandons nothing; the record still counts the footprint")
}

// No-regression control: the ordinary reap. An evicted/expired record carries only the
// fred-retained-* names PutActiveMerged wrote, no restore is in flight, and the record is
// destroyed and dropped exactly as before ENG-659.
func TestDestroyReapingVolumes_NormalReapingRecordStillFullyReaped(t *testing.T) {
	lease := "0192f1a0-3333-7abc-8def-000000000016"
	names := []string{"fred-retained-" + lease + "-app-0", "fred-retained-" + lease + "-app-1"}

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: names,
	}))

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}
	claimedBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipRestoreClaimed))
	unreadableBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	assert.True(t, b.destroyReapingVolumes(context.Background(), lease, names))
	assert.Equal(t, names, destroyed)

	rec, err := rs.Get(lease)
	require.NoError(t, err)
	assert.Nil(t, rec, "a fully reaped record is deleted")
	assert.Equal(t, claimedBefore, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipRestoreClaimed)))
	assert.Equal(t, unreadableBefore, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)))
}

// The claim set is keyed on canonical fred-{lease}-* names (retainedToNewCanonical never
// emits a fred-retained-* name), so an all-retained name list cannot match one and the
// store read is skipped entirely. This keeps the guard off the hot reap path — notably
// evictOldest's up-to-32 records inside a synchronous close. Pinned by closing the store:
// without the pre-filter this would bail at claim_unreadable and destroy nothing.
func TestDestroyReapingVolumes_RetainedOnlyNames_SkipTheClaimLookup(t *testing.T) {
	lease := "0192f1a0-3333-7abc-8def-000000000017"
	names := []string{"fred-retained-" + lease + "-app-0"}

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close())

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}
	unreadableBefore := testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable))

	b.destroyReapingVolumes(context.Background(), lease, names)

	assert.Equal(t, names, destroyed,
		"no fred-{lease}-* name in the list ⇒ no claim can match ⇒ no store read, destroy proceeds")
	assert.Equal(t, unreadableBefore, testutil.ToFloat64(retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable)),
		"the pre-filter must not be reached by way of the fail-safe bailout")
}

// Convergence: the skip defers, it does not wedge. Once the restore's rollback
// re-quarantines the volume (record back to ACTIVE, canonical name gone), the very next
// pass reaps the tombstone and drops it. This is the test that stops a future reader from
// "simplifying" the skip into a Delete.
func TestDestroyReapingVolumes_ConvergesAfterRestoreRollback(t *testing.T) {
	orig := "0192f1a0-1111-7abc-8def-000000000018"
	newLease := "0192f1a0-2222-7abc-8def-000000000019"

	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	adopted, ownLeak := seedClaimedTombstone(t, rs, orig, newLease)
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return nil }}

	require.False(t, b.destroyReapingVolumes(context.Background(), newLease, []string{adopted, ownLeak}),
		"precondition: the claim holds the tombstone")

	// The rollback: reconcileRestoring renames fred-{newLease}-app-0 back into the
	// retained namespace and CASes the record to active. The claim is gone with it.
	reverted, err := rs.RevertToActive(orig, 1)
	require.NoError(t, err)
	require.True(t, reverted)

	assert.True(t, b.destroyReapingVolumes(context.Background(), newLease, []string{adopted, ownLeak}),
		"claim cleared ⇒ the destroy is an idempotent no-op on the now-absent name and the record drops")
	rec, err := rs.Get(newLease)
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// TestDestroyReapingVolumes_RefusesAVolumeALiveProvisionHolds covers a second way a
// tombstone can name data it does not own, which the per-record claim check could not see
// (ENG-658). A give-up leaves a tombstone naming fred-{lease}-* and deletes the provision;
// the lease is still ACTIVE on chain, so the reconciler re-provisions it and a fresh
// volume appears under the very name the tombstone carries. The finalizer now asks the
// owner table — which knows about live provisions, not just restores — and refuses.
//
// This is the ENG-505 class reached through the finalizer instead of the orphan reaper.
func TestDestroyReapingVolumes_RefusesAVolumeALiveProvisionHolds(t *testing.T) {
	lease := "0192f1a0-3333-7abc-8def-000000000012"
	live := canonicalVolumeName(lease, "app", 0)
	staleLeak := canonicalVolumeName(lease, "app", 1)

	// The lease is tracked again: the give-up deleted the provision, but the chain still
	// says ACTIVE, so the reconciler re-provisioned it.
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusReaping,
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		RetainedVolumeNames: []string{live, staleLeak},
	}))

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	ok := b.destroyReapingVolumes(context.Background(), lease, []string{live, staleLeak})

	assert.False(t, ok, "a refused name means the record is not fully reaped and must be kept")
	assert.NotContains(t, destroyed, live,
		"the re-provisioned lease is running on this volume; a stale tombstone must not reap it (ENG-505 class)")
	assert.Equal(t, []string{staleLeak}, destroyed,
		"the genuinely abandoned name is still reaped — the refusal is per name")

	tomb, err := rs.Get(lease)
	require.NoError(t, err)
	require.NotNil(t, tomb, "the tombstone survives so the remaining leak stays counted and retryable")
	assert.Equal(t, shared.RetentionStatusReaping, tomb.Status)
}

// TestCleanupOrphanedVolumes_LiveProvisionProtectedWithoutAReleaseStore pins that a
// tracked lease's volume is protected by the OWNER TABLE alone, with no release record to
// fall back on. The two gates are independent by design and only one of them is a claim:
// leaseHasActiveRelease is a per-name release-store probe kept out of the table
// deliberately (folding it in would make a give-up tombstone whose purgeReleaseHistory
// failed permanently unreapable). This is also what keeps the release probe off the hot
// path — a healthy node's volumes are all claimed, so none of them reach it (ENG-658).
func TestCleanupOrphanedVolumes_LiveProvisionProtectedWithoutAReleaseStore(t *testing.T) {
	lease := "0192f1a0-4444-7abc-8def-000000000013"
	live := canonicalVolumeName(lease, "app", 0)

	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	require.Nil(t, b.releaseStore, "precondition: no release store, so leaseHasActiveRelease cannot protect anything")

	var destroyed []string
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{live, "fred-genuine-orphan-0"}, nil },
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))

	assert.NotContains(t, destroyed, live,
		"a tracked lease's volume is claimed, and a claimed volume is not an orphan")
	assert.Equal(t, []string{"fred-genuine-orphan-0"}, destroyed,
		"the unclaimed volume is still reaped — over-keeping everything would make the sweep useless")
}
