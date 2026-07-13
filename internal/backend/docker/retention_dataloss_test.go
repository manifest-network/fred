package docker

import (
	"context"
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
