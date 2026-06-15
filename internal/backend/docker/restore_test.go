package docker

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
