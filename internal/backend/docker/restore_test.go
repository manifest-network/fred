package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// attachRetentionStore wires a real RetentionStore (backed by a temp bbolt DB)
// into the Backend and registers a cleanup to close it.
func attachRetentionStore(t *testing.T, b *Backend) *shared.RetentionStore {
	t.Helper()
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	return s
}

// TestDeprovision_RetainRenamesExactlyExistingVolumes verifies that with
// RetainOnClose=true the deprovision path:
//   - lists actual on-disk volumes,
//   - renames exactly this lease's canonical volumes (fred-u1-*) to the
//     retained namespace,
//   - does NOT rename or destroy volumes belonging to other leases or already-retained
//     volumes, and
//   - writes an active retention record with the correct retained names + tenant.
func TestDeprovision_RetainRenamesExactlyExistingVolumes(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			Quantity:  2,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: manifest.DefaultServiceName}},
		}},
	})

	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)

	type renameCall struct{ old, new string }
	var mu sync.Mutex
	var renames []renameCall

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{
				"fred-u1-app-0",
				"fred-u1-app-1",
				"fred-OTHER-app-0",
				"fred-retained-zzz-app-0",
			}, nil
		},
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, renameCall{old, new})
			mu.Unlock()
			return nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called in RetainOnClose=true path, got %q", id)
			return nil
		},
	}

	err := b.Deprovision(context.Background(), "u1")
	require.NoError(t, err)

	// Poll for the retention record (Deprovision is async through the lease actor).
	var entry *shared.RetentionEntry
	require.Eventually(t, func() bool {
		e, err := rs.Get("u1")
		if err != nil || e == nil {
			return false
		}
		entry = e
		return true
	}, 5*time.Second, 20*time.Millisecond, "retention record for u1 must appear")

	// Verify rename calls: exactly u1's two canonical volumes.
	mu.Lock()
	gotRenames := append([]renameCall(nil), renames...)
	mu.Unlock()

	require.Len(t, gotRenames, 2, "exactly two renames for u1's two volumes")
	assert.Contains(t, gotRenames, renameCall{"fred-u1-app-0", "fred-retained-u1-app-0"})
	assert.Contains(t, gotRenames, renameCall{"fred-u1-app-1", "fred-retained-u1-app-1"})

	// Verify the retention record.
	assert.Equal(t, "u1", entry.OriginalLeaseUUID)
	assert.Equal(t, "tenant-a", entry.Tenant)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.ElementsMatch(t, []string{"fred-retained-u1-app-0", "fred-retained-u1-app-1"}, entry.RetainedVolumeNames)
}

// TestDeprovision_RetainRecordWrittenBeforeRename verifies record-first
// durability: even when RenameVolume returns an error, the retention record for
// the lease MUST already exist in the store (written before any rename attempt).
func TestDeprovision_RetainRecordWrittenBeforeRename(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			Quantity:  1,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		}},
	})

	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{"fred-u1-app-0"}, nil
		},
		RenameVolumeFn: func(old, new string) error {
			// Simulate rename failure.
			return assert.AnError
		},
	}

	// Deprovision must fail because the rename fails (flows through volumeErrs),
	// pinning the rename-error-propagation invariant.
	err := b.Deprovision(context.Background(), "u1")
	require.Error(t, err, "deprovision must fail when rename fails")

	// The retention record MUST have been written before the rename was attempted.
	require.Eventually(t, func() bool {
		e, err := rs.Get("u1")
		return err == nil && e != nil
	}, 5*time.Second, 20*time.Millisecond, "retention record must exist even when rename fails (record-first)")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
}

// TestDeprovision_PerTenantCapEvictsOwnOldest verifies that when
// MaxRetainedLeasesPerTenant=1, closing a new lease for a tenant evicts that
// tenant's oldest active record (and destroys its volumes) while leaving
// another tenant's record untouched.
func TestDeprovision_PerTenantCapEvictsOwnOldest(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"new-lease": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "new-lease",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			Quantity:  1,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		}},
	})

	b.cfg.RetainOnClose = true
	b.cfg.MaxRetainedLeasesPerTenant = 1
	rs := attachRetentionStore(t, b)

	// Pre-seed: tenant-a has an existing active record (the "old" one to be evicted).
	oldEntry := shared.RetentionEntry{
		OriginalLeaseUUID:   "old-lease",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-old-lease-app-0"},
		CreatedAt:           time.Now().Add(-time.Hour), // older
	}
	require.NoError(t, rs.Put(oldEntry))

	// Pre-seed: tenant-b has an active record (must NOT be evicted).
	otherEntry := shared.RetentionEntry{
		OriginalLeaseUUID:   "other-tenant-lease",
		Tenant:              "tenant-b",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-other-tenant-lease-app-0"},
		CreatedAt:           time.Now().Add(-2 * time.Hour),
	}
	require.NoError(t, rs.Put(otherEntry))

	var mu sync.Mutex
	destroyed := make(map[string]bool)

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{"fred-new-lease-app-0"}, nil
		},
		RenameVolumeFn: func(old, new string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			mu.Lock()
			destroyed[id] = true
			mu.Unlock()
			return nil
		},
	}

	err := b.Deprovision(context.Background(), "new-lease")
	require.NoError(t, err)

	// Wait for the new record to appear (signals the eviction + rename path ran).
	require.Eventually(t, func() bool {
		e, err := rs.Get("new-lease")
		return err == nil && e != nil
	}, 5*time.Second, 20*time.Millisecond, "new-lease retention record must appear")

	// tenant-a's old record must have been evicted.
	mu.Lock()
	gotDestroyed := make(map[string]bool)
	for k, v := range destroyed {
		gotDestroyed[k] = v
	}
	mu.Unlock()

	assert.True(t, gotDestroyed["fred-retained-old-lease-app-0"],
		"tenant-a's old retained volume must be destroyed (evicted)")

	// tenant-b's record must NOT have been evicted.
	assert.False(t, gotDestroyed["fred-retained-other-tenant-lease-app-0"],
		"tenant-b's retained volume must NOT be destroyed")

	// old-lease record must be deleted from the store.
	evicted, err := rs.Get("old-lease")
	require.NoError(t, err)
	assert.Nil(t, evicted, "old-lease retention record must be deleted after eviction")

	// other-tenant record must still exist.
	other, err := rs.Get("other-tenant-lease")
	require.NoError(t, err)
	assert.NotNil(t, other, "other-tenant-lease retention record must remain")
}

// TestDeprovision_RetainOff_DestroysAsBefore verifies that when
// RetainOnClose=false the existing volume-destroy behaviour is unchanged:
// Destroy is called for the lease's volumes and RenameVolume is never called.
func TestDeprovision_RetainOff_DestroysAsBefore(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			Quantity:  1,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		}},
	})

	// RetainOnClose defaults false; be explicit.
	b.cfg.RetainOnClose = false

	var mu sync.Mutex
	var destroyedIDs []string

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			mu.Lock()
			destroyedIDs = append(destroyedIDs, id)
			mu.Unlock()
			return nil
		},
		RenameVolumeFn: func(old, new string) error {
			t.Fatalf("RenameVolume must NOT be called when RetainOnClose=false, got old=%q new=%q", old, new)
			return nil
		},
	}

	err := b.Deprovision(context.Background(), "u1")
	require.NoError(t, err)

	// Allow async path time to complete.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(destroyedIDs) > 0
	}, 5*time.Second, 20*time.Millisecond, "volume Destroy must be called")

	mu.Lock()
	got := append([]string(nil), destroyedIDs...)
	mu.Unlock()

	// Expect the canonical volume name for u1's single instance.
	assert.Contains(t, got, canonicalVolumeName("u1", manifest.DefaultServiceName, 0))
}

// ---------------------------------------------------------------------------
// Part A + B + ordering tests (Task 5: orphan exclusion + reconciliation)
// ---------------------------------------------------------------------------

// TestCleanupOrphanedVolumes_SkipsRetained verifies that fred-retained- volumes
// are never destroyed by the orphan reaper, regardless of whether they appear
// in the expected set.
func TestCleanupOrphanedVolumes_SkipsRetained(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		"live": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "live",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			Quantity:  1,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		}},
	})

	var mu sync.Mutex
	var destroyedIDs []string

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{
				"fred-live-app-0",
				"fred-retained-u1-app-0",
				"fred-stale-app-0",
			}, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			mu.Lock()
			destroyedIDs = append(destroyedIDs, id)
			mu.Unlock()
			return nil
		},
	}

	err := b.cleanupOrphanedVolumes(context.Background())
	require.NoError(t, err)

	mu.Lock()
	got := append([]string(nil), destroyedIDs...)
	mu.Unlock()

	assert.NotContains(t, got, "fred-retained-u1-app-0", "retained volume must never be destroyed by orphan reaper")
	assert.NotContains(t, got, "fred-live-app-0", "expected live volume must not be destroyed")
	assert.Contains(t, got, "fred-stale-app-0", "orphaned stale volume must be destroyed")
}

// TestReconcileRetentions_RequarantinesActive verifies that an active retention
// record whose canonical volume is still present on disk (crashed mid-soft-delete)
// gets the canonical volume renamed back to the retained namespace.
func TestReconcileRetentions_RequarantinesActive(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	type renameCall struct{ old, new string }
	var mu sync.Mutex
	var renames []renameCall

	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, renameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
		Generation:          1,
	}))

	err := b.reconcileRetentions(context.Background())
	require.NoError(t, err)

	mu.Lock()
	got := append([]renameCall(nil), renames...)
	mu.Unlock()

	assert.Contains(t, got, renameCall{"fred-u1-app-0", "fred-retained-u1-app-0"},
		"canonical volume must be renamed back to retained namespace")
}

// TestReconcileRestoring_RollsBackOrphan verifies that a restoring record with
// no live provision (crashed restore) is fully rolled back: compose Down is called
// for the new lease's project, volumes are re-quarantined to the retained namespace,
// the record reverts to active (Generation bumped, NewLeaseUUID cleared), and the
// orphaned provision is removed from b.provisions.
func TestReconcileRestoring_RollsBackOrphan(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	var mu sync.Mutex
	var downProjects []string
	type renameCall struct{ old, new string }
	var renames []renameCall

	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, projectName string, _ time.Duration) error {
			mu.Lock()
			downProjects = append(downProjects, projectName)
			mu.Unlock()
			return nil
		},
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, renameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

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

	mu.Lock()
	gotDown := append([]string(nil), downProjects...)
	gotRenames := append([]renameCall(nil), renames...)
	mu.Unlock()

	// Compose Down must be called for the new lease's project.
	assert.Contains(t, gotDown, composeProjectName("u2"),
		"compose Down must be called for the new lease's project")

	// Volume must be re-quarantined from new canonical → original retained name.
	assert.Contains(t, gotRenames, renameCall{
		old: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
		new: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
	}, "volume must be renamed from new canonical to original retained name")

	// The record must have reverted to active with Generation bumped and NewLeaseUUID cleared.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry, "retention record for u1 must still exist after rollback")
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.Equal(t, 4, entry.Generation, "generation must be bumped by RevertToActive")
	assert.Empty(t, entry.NewLeaseUUID, "NewLeaseUUID must be cleared after rollback")

	// The orphaned provision for u2 must be removed.
	b.provisionsMu.RLock()
	_, hasU2 := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, hasU2, "orphaned provision for u2 must be removed")
}

// TestReconcileRestoring_DefersToInFlight verifies that when b.provisions["u2"]
// has status Restarting (restore is in flight), reconcileRestoring is a no-op:
// no compose Down, no rename, store record remains restoring.
func TestReconcileRestoring_DefersToInFlight(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2",
			Status:    backend.ProvisionStatusRestarting,
		}},
	})
	rs := attachRetentionStore(t, b)

	downCalled := false
	renameCalled := false
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			downCalled = true
			return nil
		},
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error {
			renameCalled = true
			return nil
		},
	}

	e := shared.RetentionEntry{
		OriginalLeaseUUID: "u1",
		NewLeaseUUID:      "u2",
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusRestoring,
		Generation:        3,
		Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
	}
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	assert.False(t, downCalled, "compose Down must NOT be called for an in-flight restore")
	assert.False(t, renameCalled, "RenameVolume must NOT be called for an in-flight restore")

	// Record must still be in restoring state.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusRestoring, entry.Status,
		"record must remain restoring when restore is in flight")
}

// TestReconcileRestoring_DeletesOnReady verifies that when b.provisions["u2"]
// has status Ready (restore completed successfully), the retention record for
// the original lease is deleted from the store.
func TestReconcileRestoring_DeletesOnReady(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2",
			Status:    backend.ProvisionStatusReady,
		}},
	})
	rs := attachRetentionStore(t, b)

	e := shared.RetentionEntry{
		OriginalLeaseUUID: "u1",
		NewLeaseUUID:      "u2",
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusRestoring,
		Generation:        2,
		Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
	}
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	// Record must be deleted (restore finished, leftover record cleaned up).
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	assert.Nil(t, entry, "retention record must be deleted when restore is already done (Ready provision)")
}

// TestStart_ReconcilesBeforeOrphanReap pins the invariant that
// reconcileRetentions runs BEFORE cleanupOrphanedVolumes. A canonical volume
// (fred-u1-app-0) that survived a crash mid-soft-delete would be destroyed by
// the orphan reaper unless reconcileRetentions first renames it to the retained
// namespace. This test drives the two functions in Start's order and asserts
// the canonical volume is never destroyed.
func TestStart_ReconcilesBeforeOrphanReap(t *testing.T) {
	// Start's ordering guarantee (Part C): reconcileRetentions THEN cleanupOrphanedVolumes.
	// If the order is reversed, the canonical volume gets destroyed before it can be
	// re-quarantined. This test calls them in Start's order to pin the invariant.

	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	// active record: crash happened after Put, before canonical→retained rename.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
		Generation:          1,
	}))

	// Simulate the canonical volume being present on disk (crash mid-rename).
	// The ListFn is called by cleanupOrphanedVolumes; after reconcile renames it,
	// the retained volume is no longer an orphan candidate.
	var mu sync.Mutex
	volumes := []string{"fred-u1-app-0"}
	var destroyedIDs []string

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), volumes...), nil
		},
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			// Update the volume list to reflect the rename.
			for i, v := range volumes {
				if v == old {
					volumes[i] = new
					break
				}
			}
			mu.Unlock()
			return nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			mu.Lock()
			destroyedIDs = append(destroyedIDs, id)
			mu.Unlock()
			return nil
		},
	}

	ctx := context.Background()

	// Drive reconcileRetentions THEN cleanupOrphanedVolumes — the same order Start uses.
	// (Part C guarantees this order in production; this test pins it for regression.)
	err := b.reconcileRetentions(ctx)
	require.NoError(t, err)

	err = b.cleanupOrphanedVolumes(ctx)
	require.NoError(t, err)

	mu.Lock()
	got := append([]string(nil), destroyedIDs...)
	mu.Unlock()

	assert.NotContains(t, got, "fred-u1-app-0",
		"canonical volume must not be destroyed: reconcile must have renamed it to retained before orphan reap")
	assert.NotContains(t, got, "fred-retained-u1-app-0",
		"retained volume must never be destroyed by orphan reaper (Part A exclusion)")
}

// TestCleanupOrphanedVolumes_ProtectsRetentionCanonical verifies that
// cleanupOrphanedVolumes does NOT destroy a retention record's canonical volume
// even when it is still canonical-named on disk (a reconcile rename failed or
// crashed). Without the retention-aware protection in cleanupOrphanedVolumes,
// this canonical (not fred-retained-, not in any live provision) would be
// destroyed = permanent data loss. Covers both the active and restoring arms.
func TestCleanupOrphanedVolumes_ProtectsRetentionCanonical(t *testing.T) {
	t.Run("active record canonical", func(t *testing.T) {
		mock := &mockDockerClient{}
		b := newBackendForTest(mock, nil)
		rs := attachRetentionStore(t, b)

		// Active record whose canonical (fred-u1-app-0) is still on disk because
		// the reconcile rename failed/crashed.
		require.NoError(t, rs.Put(shared.RetentionEntry{
			OriginalLeaseUUID:   "u1",
			Tenant:              "tenant-a",
			Status:              shared.RetentionStatusActive,
			RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
			Generation:          1,
		}))

		var mu sync.Mutex
		var destroyedIDs []string
		b.volumes = &mockVolumeManager{
			ListFn: func() ([]string, error) {
				return []string{"fred-u1-app-0"}, nil // canonical still on disk
			},
			DestroyFn: func(_ context.Context, id string) error {
				mu.Lock()
				destroyedIDs = append(destroyedIDs, id)
				mu.Unlock()
				return nil
			},
		}

		require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))

		mu.Lock()
		got := append([]string(nil), destroyedIDs...)
		mu.Unlock()
		assert.NotContains(t, got, "fred-u1-app-0",
			"active retention record's canonical must be protected from the orphan reaper")
	})

	t.Run("restoring record new-lease canonical", func(t *testing.T) {
		mock := &mockDockerClient{}
		b := newBackendForTest(mock, nil)
		rs := attachRetentionStore(t, b)

		// Restoring record: the new lease's canonical (fred-u2-app-0) holds the
		// adopted/in-flight data and must not be reaped.
		require.NoError(t, rs.Put(shared.RetentionEntry{
			OriginalLeaseUUID: "u1",
			NewLeaseUUID:      "u2",
			Tenant:            "tenant-a",
			Status:            shared.RetentionStatusRestoring,
			Generation:        2,
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		}))

		var mu sync.Mutex
		var destroyedIDs []string
		b.volumes = &mockVolumeManager{
			ListFn: func() ([]string, error) {
				return []string{"fred-u2-app-0"}, nil // adopted data still on disk
			},
			DestroyFn: func(_ context.Context, id string) error {
				mu.Lock()
				destroyedIDs = append(destroyedIDs, id)
				mu.Unlock()
				return nil
			},
		}

		require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))

		mu.Lock()
		got := append([]string(nil), destroyedIDs...)
		mu.Unlock()
		assert.NotContains(t, got, "fred-u2-app-0",
			"restoring record's new-lease canonical (adopted data) must be protected from the orphan reaper")
	})
}

func TestVolumeNameHelpers(t *testing.T) {
	if got := canonicalVolumeName("u1", "app", 0); got != "fred-u1-app-0" {
		t.Errorf("canonicalVolumeName: got %q, want %q", got, "fred-u1-app-0")
	}
	if got := retainedName("fred-u1-app-0"); got != "fred-retained-u1-app-0" {
		t.Errorf("retainedName: got %q, want %q", got, "fred-retained-u1-app-0")
	}
	if got := isRetainedVolume("fred-retained-u1-app-0"); !got {
		t.Errorf("isRetainedVolume(retained): got false, want true")
	}
	if got := isRetainedVolume("fred-u1-app-0"); got {
		t.Errorf("isRetainedVolume(canonical): got true, want false")
	}
	if got := leaseVolumePrefix("u1"); got != "fred-u1-" {
		t.Errorf("leaseVolumePrefix: got %q, want %q", got, "fred-u1-")
	}
}

// ---------------------------------------------------------------------------
// Task 6: grace reaper + periodic restore reconcile
// ---------------------------------------------------------------------------

// TestReapExpiredRetentions verifies the grace reaper:
//   - Destroys volumes and removes the store record for an expired ACTIVE entry.
//   - Leaves a fresh ACTIVE entry alone (not yet expired).
//   - Leaves a RESTORING entry alone even when old (not eligible for reaping).
//   - Returns count == 1 (only the expired active entry was reaped).
func TestReapExpiredRetentions(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.RetentionMaxAge = 90 * 24 * time.Hour // 90 days

	// (a) expired ACTIVE entry — should be reaped.
	expiredActive := shared.RetentionEntry{
		OriginalLeaseUUID:   "old-active",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-old-app-0"},
		CreatedAt:           time.Now().Add(-100 * 24 * time.Hour),
	}
	require.NoError(t, rs.Put(expiredActive))

	// (b) fresh ACTIVE entry — should NOT be reaped.
	freshActive := shared.RetentionEntry{
		OriginalLeaseUUID:   "fresh-active",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-fresh-app-0"},
		CreatedAt:           time.Now(),
	}
	require.NoError(t, rs.Put(freshActive))

	// (c) expired RESTORING entry — should NOT be reaped (only active entries are eligible).
	expiredRestoring := shared.RetentionEntry{
		OriginalLeaseUUID:   "old-restoring",
		Tenant:              "tenant-a",
		NewLeaseUUID:        "u2",
		Status:              shared.RetentionStatusRestoring,
		RetainedVolumeNames: []string{"fred-retained-old-restoring-app-0"},
		CreatedAt:           time.Now().Add(-100 * 24 * time.Hour),
	}
	require.NoError(t, rs.Put(expiredRestoring))

	var mu sync.Mutex
	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			mu.Lock()
			destroyed = append(destroyed, id)
			mu.Unlock()
			return nil
		},
	}

	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "exactly one expired active entry must be reaped")

	mu.Lock()
	gotDestroyed := append([]string(nil), destroyed...)
	mu.Unlock()

	// Only the expired active volume is destroyed.
	assert.Contains(t, gotDestroyed, "fred-retained-old-app-0", "expired active volume must be destroyed")
	assert.NotContains(t, gotDestroyed, "fred-retained-fresh-app-0", "fresh active volume must NOT be destroyed")
	assert.NotContains(t, gotDestroyed, "fred-retained-old-restoring-app-0", "restoring volume must NOT be destroyed")

	// The expired active record must be gone from the store.
	entry, err := rs.Get("old-active")
	require.NoError(t, err)
	assert.Nil(t, entry, "expired active record must be removed from store")

	// The fresh active record must remain.
	fresh, err := rs.Get("fresh-active")
	require.NoError(t, err)
	assert.NotNil(t, fresh, "fresh active record must remain in store")

	// The restoring record must remain.
	restoring, err := rs.Get("old-restoring")
	require.NoError(t, err)
	assert.NotNil(t, restoring, "restoring record must remain in store")
}

// TestReapExpiredRetentions_DisabledWhenMaxAgeZero verifies that the reaper
// is a no-op when RetentionMaxAge==0, even with an old record present.
func TestReapExpiredRetentions_DisabledWhenMaxAgeZero(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.RetentionMaxAge = 0 // disabled

	// Seed an old record that would normally be reaped.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "old-lease",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-old-lease-app-0"},
		CreatedAt:           time.Now().Add(-365 * 24 * time.Hour),
	}))

	destroyCalled := false
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error {
			destroyCalled = true
			return nil
		},
	}

	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n, "no entries must be reaped when RetentionMaxAge==0")
	assert.False(t, destroyCalled, "Destroy must NOT be called when RetentionMaxAge==0")
}

// TestReapExpiredRetentions_DestroyFailureReRecordsForRetry verifies the
// self-heal path: when a Destroy fails after ReapIfExpired atomically removed
// the record, the (still-expired) entry is re-recorded so the next sweep retries
// the destroy — avoiding a permanent orphaned-volume disk leak. A subsequent
// sweep with a succeeding Destroy then fully reaps it.
func TestReapExpiredRetentions_DestroyFailureReRecordsForRetry(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.RetentionMaxAge = 90 * 24 * time.Hour

	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "old-active",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-old-app-0"},
		CreatedAt:           time.Now().Add(-100 * 24 * time.Hour),
	}))

	var mu sync.Mutex
	failDestroy := true
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error {
			mu.Lock()
			fail := failDestroy
			mu.Unlock()
			if fail {
				return errors.New("docker destroy failed")
			}
			return nil
		},
	}

	// First sweep: Destroy fails -> record must be re-recorded for retry.
	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n, "a Destroy failure must NOT count as reaped")

	entry, err := rs.Get("old-active")
	require.NoError(t, err)
	require.NotNil(t, entry, "record must be re-recorded after Destroy failure so the next sweep retries")
	assert.Equal(t, shared.RetentionStatusActive, entry.Status, "re-recorded entry must stay active+expired")
	assert.Equal(t, []string{"fred-retained-old-app-0"}, entry.RetainedVolumeNames)

	// Second sweep: Destroy succeeds -> record fully reaped.
	mu.Lock()
	failDestroy = false
	mu.Unlock()

	n, err = b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n, "retry with a succeeding Destroy must reap the record")

	entry, err = rs.Get("old-active")
	require.NoError(t, err)
	assert.Nil(t, entry, "record must be removed after a successful retry")
}

// TestRunRetentionSweep_ReconcilesRestoring verifies that the periodic sweep
// rolls back an orphaned restoring record (no live provision for the new lease).
// This mirrors TestReconcileRestoring_RollsBackOrphan but exercises runRetentionSweep
// to confirm it invokes reconcileRestoring for each restoring record.
func TestRunRetentionSweep_ReconcilesRestoring(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.RetentionMaxAge = 90 * 24 * time.Hour

	var mu sync.Mutex
	var downProjects []string
	type renameCall struct{ old, new string }
	var renames []renameCall

	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, projectName string, _ time.Duration) error {
			mu.Lock()
			downProjects = append(downProjects, projectName)
			mu.Unlock()
			return nil
		},
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, renameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

	// Seed a restoring record with no live provision for u2 (orphaned).
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

	err := b.runRetentionSweep(context.Background())
	require.NoError(t, err)

	mu.Lock()
	gotDown := append([]string(nil), downProjects...)
	gotRenames := append([]renameCall(nil), renames...)
	mu.Unlock()

	// reconcileRestoring must have been invoked: compose Down for u2's project.
	assert.Contains(t, gotDown, composeProjectName("u2"),
		"compose Down must be called for the orphaned restore's new lease")

	// Volume must be re-quarantined.
	assert.Contains(t, gotRenames, renameCall{
		old: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
		new: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
	}, "volume must be renamed from new canonical to original retained name")

	// Record must have reverted to active.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.Equal(t, 4, entry.Generation, "generation must be bumped")
	assert.Empty(t, entry.NewLeaseUUID)
}

// TestStartRetentionReaper_NoopWhenDisabled verifies that startRetentionReaper
// returns immediately when RetentionMaxAge==0.
func TestStartRetentionReaper_NoopWhenDisabled(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	attachRetentionStore(t, b)
	b.cfg.RetentionMaxAge = 0 // disabled

	// Must not block, must not panic.
	done := make(chan struct{})
	go func() {
		b.startRetentionReaper()
		close(done)
	}()

	select {
	case <-done:
		// good: returned without blocking
	case <-time.After(time.Second):
		t.Fatal("startRetentionReaper blocked when RetentionMaxAge==0 — expected immediate return")
	}
}

// ---------------------------------------------------------------------------
// Task 7b: Backend.Restore docker lifecycle
// ---------------------------------------------------------------------------

// restoreStackManifest is the canonical 1-service ("app") stack manifest used by
// the restore tests (image passes the default registry allowlist).
func restoreStackManifest() *manifest.StackManifest {
	return &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			manifest.DefaultServiceName: {Image: "nginx:latest"},
		},
	}
}

// seedActiveRetained writes an ACTIVE retained record for original lease `orig`
// (tenant-a, 1×docker-small/"app") and returns it.
func seedActiveRetained(t *testing.T, rs *shared.RetentionStore, orig string) shared.RetentionEntry {
	t.Helper()
	e := shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		Tenant:              "tenant-a",
		ProviderUUID:        "prov-1",
		Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		StackManifest:       restoreStackManifest(),
		CallbackURL:         "http://localhost/callback",
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(orig, manifest.DefaultServiceName, 0))},
		Status:              shared.RetentionStatusActive,
		Generation:          1,
		CreatedAt:           time.Now(),
	}
	require.NoError(t, rs.Put(e))
	return e
}

// restoreRequest builds a RestoreRequest matching seedActiveRetained's shape.
func restoreRequest(newLease, fromLease, callbackURL string) backend.RestoreRequest {
	return backend.RestoreRequest{
		LeaseUUID:     newLease,
		FromLeaseUUID: fromLease,
		Tenant:        "tenant-a",
		ProviderUUID:  "prov-1",
		Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		CallbackURL:   callbackURL,
	}
}

// renameCall captures a RenameVolume(old, new) invocation.
type restoreRenameCall struct{ old, new string }

// happyComposeMock returns a compose executor that brings the project Up and
// reports one running container for service "app". upErr (if non-nil) makes Up
// fail to drive the restore-failure path. Down is recorded into downProjects.
func happyComposeMock(mu *sync.Mutex, downProjects *[]string, upErr error) *mockComposeExecutor {
	return &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			return upErr
		},
		PSFn: func(_ context.Context, _ string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "container-1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
		DownFn: func(_ context.Context, projectName string, _ time.Duration) error {
			mu.Lock()
			*downProjects = append(*downProjects, projectName)
			mu.Unlock()
			return nil
		},
	}
}

// TestRestore_PreludeRejectsWhenNotRetained: an empty store yields ErrNotRetained
// and creates no provision entry.
func TestRestore_PreludeRejectsWhenNotRetained(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	attachRetentionStore(t, b)

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://localhost/cb"))
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrNotRetained)

	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "no provision entry must be created on a not-retained restore")
}

// TestRestore_PreludeRejectsConcurrentLiveProvision: pre-putting b.provisions[u2]
// makes the reservation fail with ErrAlreadyProvisioned; the retained record is
// untouched (still active) and no rename happens.
func TestRestore_PreludeRejectsConcurrentLiveProvision(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "u2", Status: backend.ProvisionStatusReady}},
	})
	rs := attachRetentionStore(t, b)
	seedActiveRetained(t, rs, "u1")

	renameCalled := false
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { renameCalled = true; return nil },
	}

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://localhost/cb"))
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned)

	assert.False(t, renameCalled, "no volume rename must occur when the new lease is already provisioned")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status, "retained record must remain active")
	assert.Equal(t, 1, entry.Generation, "retained record generation must be unchanged")
}

// TestRestore_ItemsMismatch_Validation: a new-lease item set whose shape differs
// from the retained set yields ErrValidation; the record stays active with no
// provision/rename.
func TestRestore_ItemsMismatch_Validation(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	seedActiveRetained(t, rs, "u1")

	renameCalled := false
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { renameCalled = true; return nil },
	}

	req := restoreRequest("u2", "u1", "http://localhost/cb")
	req.Items = []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: manifest.DefaultServiceName}} // qty differs

	err := b.Restore(context.Background(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)

	assert.False(t, renameCalled, "no volume rename must occur on a validation rejection")

	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "no provision entry must be created on a validation rejection")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.Equal(t, 1, entry.Generation)
}

// TestRestore_ProviderMismatch_Validation: a request whose ProviderUUID differs
// from the retained record's is rejected with ErrValidation (defensive
// cross-check); the record stays active, no provision/rename.
func TestRestore_ProviderMismatch_Validation(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	seedActiveRetained(t, rs, "u1") // ProviderUUID: "prov-1"

	renameCalled := false
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { renameCalled = true; return nil },
	}

	req := restoreRequest("u2", "u1", "http://localhost/cb")
	req.ProviderUUID = "prov-OTHER" // differs from the retained record's "prov-1"

	err := b.Restore(context.Background(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)

	assert.False(t, renameCalled, "no volume rename must occur on a provider-mismatch rejection")

	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "no provision entry must be created on a provider-mismatch rejection")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.Equal(t, 1, entry.Generation)
}

// TestRestore_Success_DeletesRecord drives a restore all the way to Ready and
// asserts: the retained record for u1 is DELETED, the volume was renamed
// retained→canonical(u2), and u2's provision reaches Ready.
func TestRestore_Success_DeletesRecord(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var downProjects []string
	var renames []restoreRenameCall
	b.compose = happyComposeMock(&mu, &downProjects, nil)
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, restoreRenameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

	callbackReceived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer server.Close()

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", server.URL))
	require.NoError(t, err)

	<-callbackReceived

	// u2 must reach Ready.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 5*time.Second, 20*time.Millisecond, "u2 must reach Ready")

	// The retained record for u1 must be deleted.
	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		return gerr == nil && e == nil
	}, 5*time.Second, 20*time.Millisecond, "retained record for u1 must be deleted on successful restore")

	// The adopt rename retained(u1)→canonical(u2) must have happened.
	mu.Lock()
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()
	assert.Contains(t, gotRenames, restoreRenameCall{
		old: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
		new: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
	}, "adopt must rename retained(u1) → canonical(u2)")

	b.stopCancel()
	b.wg.Wait()
}

// TestRestore_Failure_RollsBackInline makes the downstream compose Up FAIL and
// asserts the C2+N1 behavior: the lease ends Failed (NOT recovered) — proven by
// the FAILED callback firing — compose.Down(fred-u2) was called, the volume was
// renamed BACK to retained, the retention record is active again (Generation
// bumped), and the pool is released. A restore that fails terminates Failed
// because NoComposeRollback keeps Restored=false (no false "recovered"); the
// provision settles as a Failed entry (the actor's onEnterFailedFromReplace owns
// the Status flip + callback, reading CallbackURL from the still-present record).
func TestRestore_Failure_RollsBackInline(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var downProjects []string
	var renames []restoreRenameCall
	b.compose = happyComposeMock(&mu, &downProjects, errors.New("compose up boom"))
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, restoreRenameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

	// Capture the callback STATUS so we can prove the lease ended Failed (not a
	// success/recovered callback).
	var gotStatus atomic.Value
	callbackReceived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		gotStatus.Store(string(payload.Status))
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer server.Close()

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", server.URL))
	require.NoError(t, err) // route+ack succeed; the failure is asynchronous

	<-callbackReceived

	// Terminal-Failed proof: the callback status is "failed", NOT "success".
	assert.Equal(t, string(backend.CallbackStatusFailed), gotStatus.Load(),
		"a failed restore must emit a FAILED callback (terminal Failed, not recovered)")

	// The lease must settle Failed (NOT recovered/Ready).
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusFailed
	}, 5*time.Second, 20*time.Millisecond, "u2 must settle Failed (no false recovered)")

	// The retained record must revert to ACTIVE (NOT deleted), Generation bumped.
	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		return gerr == nil && e != nil && e.Status == shared.RetentionStatusActive
	}, 5*time.Second, 20*time.Millisecond, "retained record must revert to active on restore failure")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.Equal(t, 3, entry.Generation, "ClaimForRestore bumped 1→2, RevertToActive bumped 2→3")
	assert.Empty(t, entry.NewLeaseUUID, "NewLeaseUUID must be cleared after revert")

	mu.Lock()
	gotDown := append([]string(nil), downProjects...)
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()

	// compose.Down for the new lease's project must have run (N1: before re-quarantine).
	assert.Contains(t, gotDown, composeProjectName("u2"), "compose Down(fred-u2) must be called during rollback")

	// The volume must be renamed BACK: canonical(u2) → retained(u1).
	assert.Contains(t, gotRenames, restoreRenameCall{
		old: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
		new: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
	}, "rollback must re-quarantine canonical(u2) → retained(u1)")

	// Pool must be released: re-allocating u2's slot must succeed.
	allocErr := b.pool.TryAllocate("u2-"+manifest.DefaultServiceName+"-0", "docker-small", "tenant-a")
	assert.NoError(t, allocErr, "pool slot must be free after rollback release")
	b.pool.Release("u2-" + manifest.DefaultServiceName + "-0")

	b.stopCancel()
	b.wg.Wait()
}

// TestRestore_WorkerPanic_RollsBackAndKeepsRecord induces a panic in the work
// path (compose Up panics) and asserts the record is reverted to active (NOT
// deleted) and the volume is re-quarantined. doRestore's panic-recovery defer
// converts the panic into an errored ReplaceResult (Restored=false) and runs the
// compensating rollback; the actor then drives the lease to Failed. A panic must
// NEVER be mistaken for success (which would delete the retained record).
func TestRestore_WorkerPanic_RollsBackAndKeepsRecord(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var renames []restoreRenameCall
	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			panic("induced restore worker panic")
		},
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, restoreRenameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://127.0.0.1:0/cb"))
	require.NoError(t, err) // route+ack succeed; the panic is asynchronous

	// The record must revert to active (NOT deleted) — the panic must not be
	// mistaken for success.
	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		return gerr == nil && e != nil && e.Status == shared.RetentionStatusActive
	}, 5*time.Second, 20*time.Millisecond, "record must revert to active after a restore worker panic")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry, "record must NOT be deleted on panic")
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)

	// The lease must settle Failed (actor fires evReplaceFailed on the errored
	// panic result; doRestore keeps the provision so the Status flip can run).
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusFailed
	}, 5*time.Second, 20*time.Millisecond, "u2 must settle Failed after panic rollback")

	// The volume must be re-quarantined back to the retained namespace.
	mu.Lock()
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()
	assert.Contains(t, gotRenames, restoreRenameCall{
		old: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
		new: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
	}, "panic rollback must re-quarantine canonical(u2) → retained(u1)")

	b.stopCancel()
	b.wg.Wait()
}

// TestRestore_RouteFailure_RollsBackSynchronously cancels b.stopCtx so
// routeToLeaseBlocking fails; the synchronous rollback must run (record active,
// provision gone, volume re-quarantined) WITHOUT a worker ever executing.
func TestRestore_RouteFailure_RollsBackSynchronously(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var renames []restoreRenameCall
	upCalled := false
	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			upCalled = true
			return nil
		},
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, restoreRenameCall{old, new})
			mu.Unlock()
			return nil
		},
	}

	// Cancel the backend so routeToLeaseBlocking returns "backend shutting down".
	b.stopCancel()

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://localhost/cb"))
	require.Error(t, err, "Restore must fail synchronously when the backend is shutting down")

	assert.False(t, upCalled, "no worker must run on a route failure")

	// Record reverted to active synchronously.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status, "record must be reverted to active on synchronous rollback")
	assert.Empty(t, entry.NewLeaseUUID)

	// Provision removed.
	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "provision for u2 must be removed on synchronous rollback")

	// Volume re-quarantined: adopt rename happened then was reversed.
	mu.Lock()
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()
	assert.Contains(t, gotRenames, restoreRenameCall{
		old: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
		new: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
	}, "synchronous rollback must re-quarantine canonical(u2) → retained(u1)")
}

// TestRestore_RaceWithProvision interleaves Restore(u2 from u1) with a concurrent
// Provision(u2). Exactly one must win the reservation; the loser must get
// ErrAlreadyProvisioned. The retention record is never lost (either deleted on a
// winning restore-to-Ready, or left active if restore lost). Run under -race.
func TestRestore_RaceWithProvision(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var downProjects []string
	b.compose = happyComposeMock(&mu, &downProjects, nil)
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	var restoreErr, provisionErr atomic.Value
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		if err := b.Restore(context.Background(), restoreRequest("u2", "u1", server.URL)); err != nil {
			restoreErr.Store(err)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		req := newProvisionRequest("u2", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
		req.CallbackURL = server.URL
		if err := b.Provision(context.Background(), req); err != nil {
			provisionErr.Store(err)
		}
	}()
	close(start)
	wg.Wait()

	// Exactly one of the two reservations must have failed with ErrAlreadyProvisioned.
	rErr, _ := restoreErr.Load().(error)
	pErr, _ := provisionErr.Load().(error)
	bothSucceeded := rErr == nil && pErr == nil
	assert.False(t, bothSucceeded, "Restore and Provision must not both win the u2 reservation")
	if rErr != nil {
		assert.ErrorIs(t, rErr, backend.ErrAlreadyProvisioned, "the losing Restore must report ErrAlreadyProvisioned")
	}
	if pErr != nil {
		assert.ErrorIs(t, pErr, backend.ErrAlreadyProvisioned, "the losing Provision must report ErrAlreadyProvisioned")
	}

	// The retention record must never be lost: it is either still present
	// (restore lost, or in-flight) or deleted (restore won and reached Ready).
	// Give the winner a moment to settle, then assert: if u1 is gone, u2 must be
	// a Ready restore; if u1 is present, it must be active.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		p, ok := b.provisions["u2"]
		settled := ok && (p.Status == backend.ProvisionStatusReady || p.Status == backend.ProvisionStatusFailed)
		b.provisionsMu.RUnlock()
		return settled
	}, 5*time.Second, 20*time.Millisecond, "u2 must settle")

	e, gerr := rs.Get("u1")
	require.NoError(t, gerr)
	if e != nil {
		assert.Contains(t, []string{shared.RetentionStatusActive, shared.RetentionStatusRestoring}, e.Status,
			"retention record must be active or restoring, never a corrupt state")
	}

	b.stopCancel()
	b.wg.Wait()
}
