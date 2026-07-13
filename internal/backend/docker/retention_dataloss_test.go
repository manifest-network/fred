package docker

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

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

	b.finalizeRestoredLease(newLease, &e, slog.Default())

	got, err := rs.Get(orig)
	require.NoError(t, err)
	require.NotNil(t, got,
		"ENG-523: retention record (the adopted volume's finalizer) must NOT be deleted when the release Append failed")
	assert.Equal(t, shared.RetentionStatusRestoring, got.Status)
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
