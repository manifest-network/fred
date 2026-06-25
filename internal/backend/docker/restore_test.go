package docker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestDeprovision_Retain_MergesPriorRecordOnRetry verifies that a retry of the
// soft-delete path (after a partial rename on attempt 1) MERGES the existing
// record's RetainedVolumeNames with the still-canonical volumes instead of
// overwriting them. Without the merge, b.volumes.List on the retry no longer
// returns the already-renamed fred-retained-u1-app-0, so Put would shrink the
// record to only the still-canonical one (leaking the already-retained volume).
func TestDeprovision_Retain_MergesPriorRecordOnRetry(t *testing.T) {
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

	// Simulate attempt 1: instance 0 was already renamed into the retained
	// namespace and recorded; instance 1's rename failed and was retried.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
		CreatedAt:           time.Now().Add(-time.Minute),
	}))

	b.volumes = &mockVolumeManager{
		// On the RETRY, List no longer returns fred-u1-app-0 (already retained);
		// only the still-canonical fred-u1-app-1 remains.
		ListFn: func() ([]string, error) {
			return []string{"fred-u1-app-1"}, nil
		},
		RenameVolumeFn: func(old, new string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called in RetainOnClose=true path, got %q", id)
			return nil
		},
	}

	err := b.Deprovision(context.Background(), "u1")
	require.NoError(t, err)

	// Wait for the merged record: both names must be present.
	var entry *shared.RetentionEntry
	require.Eventually(t, func() bool {
		e, err := rs.Get("u1")
		if err != nil || e == nil {
			return false
		}
		entry = e
		return len(e.RetainedVolumeNames) == 2
	}, 5*time.Second, 20*time.Millisecond, "merged retention record for u1 must contain both volumes")

	assert.ElementsMatch(t,
		[]string{"fred-retained-u1-app-0", "fred-retained-u1-app-1"},
		entry.RetainedVolumeNames,
		"retry must MERGE the prior record's retained names with the still-canonical one, not overwrite")
}

// TestDeprovision_Retain_PreservesCreatedAtAndGenerationOnRetry verifies that a
// soft-delete RETRY does NOT slide the 90d grace clock forward (CreatedAt) and
// does NOT clobber a CAS-bumped Generation (zero-value would). The retry merges
// the still-canonical volume into the prior ACTIVE record while preserving its
// CreatedAt and Generation.
func TestDeprovision_Retain_PreservesCreatedAtAndGenerationOnRetry(t *testing.T) {
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

	// Pre-seed an ACTIVE record from a prior attempt with a FIXED past CreatedAt
	// and a non-zero Generation (simulating a CAS bump that must be preserved).
	fixedCreatedAt := time.Now().Add(-30 * 24 * time.Hour).Round(time.Millisecond)
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
		Generation:          2,
		CreatedAt:           fixedCreatedAt,
	}))

	b.volumes = &mockVolumeManager{
		// On retry, instance 0 is already retained; only fred-u1-app-1 is canonical.
		ListFn:         func() ([]string, error) { return []string{"fred-u1-app-1"}, nil },
		RenameVolumeFn: func(old, new string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called in RetainOnClose=true path, got %q", id)
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "u1"))

	var entry *shared.RetentionEntry
	require.Eventually(t, func() bool {
		e, err := rs.Get("u1")
		if err != nil || e == nil {
			return false
		}
		entry = e
		return len(e.RetainedVolumeNames) == 2
	}, 5*time.Second, 20*time.Millisecond, "merged record with both volumes must appear")

	// Grace clock unchanged (NOT reset to time.Now()) and Generation preserved.
	assert.True(t, entry.CreatedAt.Equal(fixedCreatedAt),
		"CreatedAt must be preserved across retry (grace clock not slid forward); got %v want %v", entry.CreatedAt, fixedCreatedAt)
	assert.Equal(t, 2, entry.Generation, "Generation must be preserved (not clobbered to zero) across retry")
	assert.ElementsMatch(t,
		[]string{"fred-retained-u1-app-0", "fred-retained-u1-app-1"},
		entry.RetainedVolumeNames, "both retained names must be merged")
}

// TestDeprovision_Retain_DoesNotClobberRestoringRecord verifies that a soft-delete
// retry whose record was concurrently CLAIMED for restore (Status=restoring) does
// NOT blindly Put (which would revert it to active, reset Generation, drop
// NewLeaseUUID, and break the restore rollback CAS) and does NOT rename the
// canonical volume. The retry surfaces an error so the lease stays Failed and
// re-attempts after the restore resolves.
func TestDeprovision_Retain_DoesNotClobberRestoringRecord(t *testing.T) {
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

	// A concurrent restore claimed this lease's record: restoring, gen=5, new lease.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		NewLeaseUUID:        "new-lease",
		Generation:          5,
		RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
		CreatedAt:           time.Now().Add(-time.Hour),
	}))

	var mu sync.Mutex
	var renames []string
	b.volumes = &mockVolumeManager{
		// The canonical volume is still on disk (the prior soft-delete didn't finish).
		ListFn: func() ([]string, error) { return []string{"fred-u1-app-1"}, nil },
		RenameVolumeFn: func(old, _ string) error {
			mu.Lock()
			renames = append(renames, old)
			mu.Unlock()
			return nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called in RetainOnClose=true path, got %q", id)
			return nil
		},
	}

	// Deprovision must surface an error (volume cleanup failed → lease stays Failed).
	err := b.Deprovision(context.Background(), "u1")
	require.Error(t, err, "deprovision must fail while the record is being restored")

	// The restoring record must be untouched.
	got, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, shared.RetentionStatusRestoring, got.Status, "record must still be restoring (not reverted to active)")
	assert.Equal(t, 5, got.Generation, "Generation must be untouched")
	assert.Equal(t, "new-lease", got.NewLeaseUUID, "NewLeaseUUID must be untouched")

	// The canonical volume must NOT have been renamed (re-quarantine skipped).
	mu.Lock()
	gotRenames := append([]string(nil), renames...)
	mu.Unlock()
	assert.NotContains(t, gotRenames, "fred-u1-app-1",
		"the canonical volume must NOT be renamed while a restore owns the record")

	// The lease must be left Failed (not deprovisioned away).
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u1"]
		return ok && p.Status == backend.ProvisionStatusFailed
	}, 5*time.Second, 20*time.Millisecond, "u1 must be left Failed for the volume-cleanup retry")
}

// TestEvictRetentionsToCap_ExcludesClosingLease verifies that the cap eviction
// never destroys the CLOSING lease's own retention record (which can exist from
// a prior soft-delete attempt). With cap=1 and the closing lease's own record as
// the only active record, evict with excludeLease set must be a no-op.
func TestEvictRetentionsToCap_ExcludesClosingLease(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	// The closing lease's OWN active record from a prior attempt.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "closing",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: []string{"fred-retained-closing-app-0"},
		CreatedAt:           time.Now().Add(-time.Hour),
	}))

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("evict must NOT destroy the closing lease's own record; got Destroy(%q)", id)
			return nil
		},
	}

	// cap=1: without the exclusion this lone record would be (wrongly) evicted.
	err := b.evictRetentionsToCap(context.Background(), "tenant-a", 1, "closing")
	require.NoError(t, err)

	// The closing lease's record must still be present.
	got, err := rs.Get("closing")
	require.NoError(t, err)
	require.NotNil(t, got, "closing lease's own retention record must NOT be evicted")
}

// TestEvict_DestroyFail_LeavesReapingCounted verifies cap-eviction marks the
// evicted record reaping (removing it from the active cap set), and a destroy
// failure leaves it reaping + counted in the pool, never under-counting. ENG-376.
func TestEvict_DestroyFail_LeavesReapingCounted(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedLeasesPerTenant = 1
	rs := attachRetentionStore(t, b)

	// Two active records for the same tenant → evicting to cap (1) removes the oldest.
	old := retentionEntryFixture("lease-old", "t1", time.Now().Add(-2*time.Hour))
	old.RetainedVolumeNames = []string{"fred-retained-lease-old-app-0"}
	require.NoError(t, rs.Put(old))
	newer := retentionEntryFixture("lease-new", "t1", time.Now())
	newer.RetainedVolumeNames = []string{"fred-retained-lease-new-app-0"} // UUID-derived; avoid the fixture's fixed default colliding with lease-old
	require.NoError(t, rs.Put(newer))

	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") }}

	err := b.evictRetentionsToCap(context.Background(), "t1", 1, "lease-new")
	require.NoError(t, err)

	got, err := rs.Get("lease-old")
	require.NoError(t, err)
	require.NotNil(t, got, "evicted record kept as reaping tombstone on destroy fail")
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	// Pool still counts both footprints (active lease-new + reaping lease-old).
	assert.Equal(t, int64(2*2048), b.pool.Stats().RetainedDiskMB)
}

// TestCleanupOrphanedVolumes_FailsSafeOnRetentionReadError verifies the
// fail-safe: when the retention store cannot be read (so the protected-canonical
// set cannot be built), orphan destruction is skipped entirely — nothing is
// destroyed — rather than failing open and risking a retained canonical.
func TestCleanupOrphanedVolumes_FailsSafeOnRetentionReadError(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	attachRetentionStore(t, b)
	// Close the store so List() returns an error (bolt.ErrDatabaseNotOpen).
	// attachRetentionStore's Cleanup also closes it; Close is idempotent.
	require.Error(t, func() error {
		_ = b.retentionStore.Close()
		_, err := b.retentionStore.List()
		return err
	}(), "closed store List must error (precondition for the fail-safe)")

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			// A volume that would otherwise be an orphan (no live provision).
			return []string{"fred-stale-app-0"}, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called when the retention store is unreadable; got %q", id)
			return nil
		},
	}

	// Must NOT error (so Start doesn't crash) and must NOT destroy anything.
	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))
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

// TestReconcileRestoring_RenameFailureLeavesRestoring is the data-safety arm: an
// orphaned restoring record whose re-quarantine rename FAILS must NOT advance.
// Because a real rename failure means the volume may still carry the new canonical
// name, advancing the record (RevertToActive) or dropping the provision would let
// cleanupOrphanedVolumes destroy still-live data. The reconcile must leave the
// record restoring (Generation unchanged = RevertToActive's CAS bump did NOT fire)
// and keep the provision (removeProvision skipped) so the next startup retries.
func TestReconcileRestoring_RenameFailureLeavesRestoring(t *testing.T) {
	mock := &mockDockerClient{}
	// Seed an ORPHANED provision for u2 in a genuinely-orphaned live state (Failed
	// — the status a crashed restore's recovered-from-containers provision lands at)
	// so the orphaned arm is taken AND the "provision must NOT be removed" assertion
	// is meaningful. (Provisioning/Restarting now defer as in-flight, so they would
	// NOT reach the orphaned arm.)
	b := newBackendForTest(mock, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2",
			Status:    backend.ProvisionStatusFailed,
		}},
	})
	rs := attachRetentionStore(t, b)

	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	// Force the re-quarantine rename to fail with a real (non-benign) error.
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return assert.AnError },
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

	// The record must STILL be restoring with Generation UNCHANGED: a successful
	// RevertToActive would have flipped it to active and bumped Generation to 4.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry, "record must still exist")
	assert.Equal(t, shared.RetentionStatusRestoring, entry.Status,
		"a failed re-quarantine must leave the record restoring")
	assert.Equal(t, 3, entry.Generation,
		"generation must be unchanged (RevertToActive's CAS bump must NOT have fired)")
	assert.Equal(t, "u2", entry.NewLeaseUUID, "NewLeaseUUID must be retained for the retry")

	// The provision must STILL be present (removeProvision was skipped) so its
	// expected-set entry keeps protecting the data until the next attempt.
	b.provisionsMu.RLock()
	_, hasU2 := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.True(t, hasU2, "provision for u2 must NOT be removed when re-quarantine failed")
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

// TestReconcileRestoring_DefersToInFlightProvisioning verifies the Provisioning
// window is treated as in-flight (NOT orphaned). During a live Restore() the new-
// lease provision sits at Provisioning (reserved at step b) from ClaimForRestore
// (record -> restoring, step d) until the actor fires evRestoreRequested (-> Restarting).
// A PERIODIC sweep landing in that window must DEFER — re-quarantining the just-
// adopted volumes and reverting the record would spuriously fail the in-flight restore.
func TestReconcileRestoring_DefersToInFlightProvisioning(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		"u2": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u2",
			Status:    backend.ProvisionStatusProvisioning,
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

	assert.False(t, downCalled, "compose Down must NOT be called for an in-flight (Provisioning) restore")
	assert.False(t, renameCalled, "RenameVolume must NOT be called for an in-flight (Provisioning) restore")

	// Record must remain restoring with Generation unchanged (orphaned arm / RevertToActive must NOT fire).
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusRestoring, entry.Status,
		"record must remain restoring during the Provisioning window")
	assert.Equal(t, 3, entry.Generation, "Generation must be unchanged (orphaned arm must NOT fire)")
	assert.Equal(t, "u2", entry.NewLeaseUUID, "NewLeaseUUID must be retained")

	// The in-flight provision must NOT be removed.
	b.provisionsMu.RLock()
	_, hasU2 := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.True(t, hasU2, "in-flight provision for u2 must NOT be removed")
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
		// adopted/in-flight data and must not be reaped. The restoring arm protects
		// via the AUTHORITATIVE RetainedVolumeNames (FIX C), so the record carries
		// fred-retained-u1-app-0, which maps to fred-u2-app-0 under the new lease.
		require.NoError(t, rs.Put(shared.RetentionEntry{
			OriginalLeaseUUID:   "u1",
			NewLeaseUUID:        "u2",
			Tenant:              "tenant-a",
			Status:              shared.RetentionStatusRestoring,
			Generation:          2,
			RetainedVolumeNames: []string{"fred-retained-u1-app-0"},
			Items:               []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
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

// TestCleanupOrphanedVolumes_ProtectsRestoringFromRetainedNames verifies that the
// restoring arm protects the adopted canonical via the AUTHORITATIVE
// RetainedVolumeNames (matching adoptRetainedVolumes), not an Items×Quantity
// re-derivation. If service_name normalization diverged between the two, the
// derived name would miss the adopted volume and the reaper would destroy live
// restore data. Here the retained name carries a service token ("app") that a
// naive Items×Quantity derivation off NewLeaseUUID would still produce — so we
// drive the divergence through the on-disk name actually adopted.
func TestCleanupOrphanedVolumes_ProtectsRestoringFromRetainedNames(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	// Restoring record driven off RetainedVolumeNames (authoritative). The adopted
	// canonical is retainedToNewCanonical("fred-retained-orig-app-0", "orig", "new")
	// = "fred-new-app-0".
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "orig",
		NewLeaseUUID:        "new",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          2,
		RetainedVolumeNames: []string{"fred-retained-orig-app-0"},
		// Intentionally NO Items: the old Items×Quantity arm would protect nothing
		// here, so this also guards against regressing to that derivation.
	}))

	var mu sync.Mutex
	var destroyedIDs []string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{"fred-new-app-0"}, nil // the adopted canonical on disk
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
	assert.NotContains(t, got, "fred-new-app-0",
		"restoring arm must protect the adopted canonical via RetainedVolumeNames")
}

// TestCleanupOrphanedVolumes_RestoringProtectsOriginalCanonical verifies HOLE 2:
// while a record is restoring, an ORIGINAL-lease canonical volume that still
// exists on disk (e.g. a partial soft-delete left it un-retained before the
// record was claimed for restore) must be protected. Previously the restoring
// arm only protected the new-lease adopted canonical, so the original canonical
// — neither fred-retained- nor in any live provision — would be destroyed = data
// loss. The restoring arm must protect BOTH placements.
func TestCleanupOrphanedVolumes_RestoringProtectsOriginalCanonical(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	// Restoring record; the ORIGINAL canonical (fred-orig-app-0) is still on disk,
	// NOT yet adopted to the new lease (a partial soft-delete left it un-retained).
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "orig",
		NewLeaseUUID:        "new",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          2,
		RetainedVolumeNames: []string{"fred-retained-orig-app-0"},
	}))

	var mu sync.Mutex
	var destroyedIDs []string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{"fred-orig-app-0"}, nil // original canonical, NOT yet adopted
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
	assert.NotContains(t, got, "fred-orig-app-0",
		"restoring arm must also protect the un-adopted ORIGINAL-lease canonical from the orphan reaper")
}

// TestRollbackRestoreAdoption_RenameFailureLeavesRecordRestoring verifies FIX F:
// when a re-quarantine rename FAILS, rollbackRestoreAdoption must NOT revert the
// record to active and must NOT remove the provision — leaving the record
// restoring for the reconcile sweep (the provision's expected-set entry protects
// the canonical meanwhile). A success-case sibling subtest confirms the normal
// path still reverts + drops the provision.
func TestRollbackRestoreAdoption_RenameFailureLeavesRecordRestoring(t *testing.T) {
	t.Run("rename failure leaves record restoring", func(t *testing.T) {
		mock := &mockDockerClient{}
		b := newBackendForProvisionTest(t, mock, map[string]*provision{
			"u2": {ProvisionState: leasesm.ProvisionState{
				LeaseUUID: "u2",
				Tenant:    "tenant-a",
				Status:    backend.ProvisionStatusProvisioning,
				Quantity:  1,
				Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
			}},
		})
		rs := attachRetentionStore(t, b)

		rec := shared.RetentionEntry{
			OriginalLeaseUUID:   "u1",
			NewLeaseUUID:        "u2",
			Tenant:              "tenant-a",
			Status:              shared.RetentionStatusRestoring,
			Generation:          7,
			RetainedVolumeNames: []string{retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0))},
		}
		require.NoError(t, rs.Put(rec))

		b.compose = &mockComposeExecutor{
			DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		}
		b.volumes = &mockVolumeManager{
			RenameVolumeFn: func(_, _ string) error { return assert.AnError }, // re-quarantine fails
		}

		b.rollbackRestoreAdoption(context.Background(), "u2", nil, &rec, true, slog.Default())

		// Record must STILL be restoring (RevertToActive NOT applied).
		got, err := rs.Get("u1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, shared.RetentionStatusRestoring, got.Status, "record must remain restoring after rename failure")
		assert.Equal(t, 7, got.Generation, "Generation must be unchanged (no RevertToActive bump)")

		// Provision must NOT be removed despite dropProvision=true.
		b.provisionsMu.RLock()
		_, present := b.provisions["u2"]
		b.provisionsMu.RUnlock()
		assert.True(t, present, "provision must be kept so its expected-set entry protects the canonical")
	})

	t.Run("rename ok reverts record and drops provision", func(t *testing.T) {
		mock := &mockDockerClient{}
		b := newBackendForProvisionTest(t, mock, map[string]*provision{
			"u2": {ProvisionState: leasesm.ProvisionState{
				LeaseUUID: "u2",
				Tenant:    "tenant-a",
				Status:    backend.ProvisionStatusProvisioning,
				Quantity:  1,
				Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
			}},
		})
		rs := attachRetentionStore(t, b)

		rec := shared.RetentionEntry{
			OriginalLeaseUUID:   "u1",
			NewLeaseUUID:        "u2",
			Tenant:              "tenant-a",
			Status:              shared.RetentionStatusRestoring,
			Generation:          7,
			RetainedVolumeNames: []string{retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0))},
		}
		require.NoError(t, rs.Put(rec))

		b.compose = &mockComposeExecutor{
			DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		}
		b.volumes = &mockVolumeManager{
			RenameVolumeFn: func(_, _ string) error { return nil }, // re-quarantine succeeds
		}

		b.rollbackRestoreAdoption(context.Background(), "u2", nil, &rec, true, slog.Default())

		// Record reverts to active, Generation bumped (CAS 7→8).
		got, err := rs.Get("u1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, shared.RetentionStatusActive, got.Status, "record must revert to active on clean rollback")
		assert.Equal(t, 8, got.Generation, "RevertToActive bumps Generation 7→8")

		// Provision removed (dropProvision=true).
		b.provisionsMu.RLock()
		_, present := b.provisions["u2"]
		b.provisionsMu.RUnlock()
		assert.False(t, present, "provision must be dropped on a clean synchronous rollback")
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

// TestReap_DestroyFail_LeavesReapingCounted verifies the finalizer fix: when a
// volume Destroy fails during reap, the record is left in the REAPING state (NOT
// deleted, NOT re-recorded) so computeRetainedDiskMB-via-pool keeps counting the
// footprint, the leak counter increments, and no under-count occurs. ENG-376 AC#1.
func TestReap_DestroyFail_LeavesReapingCounted(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	b.cfg.RetentionMaxAge = time.Hour
	rs := attachRetentionStore(t, b)

	exp := retentionEntryFixture("lease-exp", "t1", time.Now().Add(-2*time.Hour)) // expired, qty 2
	exp.RetainedVolumeNames = []string{"fred-retained-lease-exp-app-0"}
	require.NoError(t, rs.Put(exp))

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n, "destroy failed → not counted as reaped")

	got, err := rs.Get("lease-exp")
	require.NoError(t, err)
	require.NotNil(t, got, "record must NOT be deleted on destroy failure")
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)

	// Footprint still counted in the admission pool (no under-count).
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}

// TestReap_DestroySuccess_DeletesRecord verifies the happy path: destroy succeeds
// → record deleted → projection drops to 0.
func TestReap_DestroySuccess_DeletesRecord(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	b.cfg.RetentionMaxAge = time.Hour
	rs := attachRetentionStore(t, b)

	exp := retentionEntryFixture("lease-exp", "t1", time.Now().Add(-2*time.Hour))
	exp.RetainedVolumeNames = []string{"fred-retained-lease-exp-app-0"}
	require.NoError(t, rs.Put(exp))

	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return nil }}

	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	got, err := rs.Get("lease-exp")
	require.NoError(t, err)
	assert.Nil(t, got, "record deleted after confirmed destroy")
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
}

// TestRetryReapingRecords_ReclaimsWhenDestroyRecovers verifies a stuck reaping
// record is auto-reclaimed by a later sweep once Destroy starts succeeding.
func TestRetryReapingRecords_ReclaimsWhenDestroyRecovers(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b)

	reaping := retentionEntryFixture("lease-r", "t1", time.Now())
	reaping.Status = shared.RetentionStatusReaping
	reaping.RetainedVolumeNames = []string{"fred-retained-lease-r-app-0"}
	require.NoError(t, rs.Put(reaping))

	var fail atomic.Bool
	fail.Store(true)
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error {
			if fail.Load() {
				return errors.New("EBUSY")
			}
			return nil
		},
	}

	// First sweep: destroy fails → record stays reaping.
	require.NoError(t, b.retryReapingRecords(context.Background()))
	got, err := rs.Get("lease-r")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)

	// Destroy recovers; next sweep reclaims + deletes.
	fail.Store(false)
	require.NoError(t, b.retryReapingRecords(context.Background()))
	got, err = rs.Get("lease-r")
	require.NoError(t, err)
	assert.Nil(t, got, "reaping record deleted after destroy recovers")
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
// returns immediately when RetentionMaxAge==0 AND RetainOnClose is off (nothing
// to reap and nothing to reconcile).
func TestStartRetentionReaper_NoopWhenDisabled(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	attachRetentionStore(t, b)
	b.cfg.RetentionMaxAge = 0   // reaping disabled
	b.cfg.RetainOnClose = false // and retention not in use

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
		t.Fatal("startRetentionReaper blocked when reaping disabled and RetainOnClose off — expected immediate return")
	}
}

// TestRetentionSweepGating verifies HOLE 4's gating predicate:
//   - reaping enabled (RetentionMaxAge>0): sweep runs (regardless of RetainOnClose).
//   - reaping disabled (RetentionMaxAge==0) but RetainOnClose=true: sweep STILL runs
//     so restoring-record reconcile happens at runtime (a failed restore rollback's
//     "next sweep retries" promise is honored without a process restart).
//   - reaping disabled AND RetainOnClose=false: sweep does NOT run (nothing to do).
//   - no retention store: never runs.
//
// retentionSweepInterval also encodes the interval default: hourly when
// RetentionMaxAge==0 and RetentionReapInterval unset.
func TestRetentionSweepGating(t *testing.T) {
	cases := []struct {
		name          string
		hasStore      bool
		maxAge        time.Duration
		retainOnClose bool
		reapInterval  time.Duration
		wantEnabled   bool
		wantInterval  time.Duration
	}{
		{"reaping enabled, retain off", true, 90 * 24 * time.Hour, false, time.Hour, true, time.Hour},
		{"reaping enabled, reap-interval unset → falls back to maxAge", true, 48 * time.Hour, false, 0, true, 48 * time.Hour},
		{"reaping disabled but retain on → reconcile hourly", true, 0, true, 0, true, time.Hour},
		{"reaping disabled but retain on, explicit reap-interval honored", true, 0, true, 15 * time.Minute, true, 15 * time.Minute},
		{"reaping disabled and retain off → no sweep", true, 0, false, 0, false, 0},
		{"no store → no sweep", false, 90 * 24 * time.Hour, true, time.Hour, false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockDockerClient{}
			b := newBackendForTest(mock, nil)
			if tc.hasStore {
				attachRetentionStore(t, b)
			} else {
				b.retentionStore = nil
			}
			b.cfg.RetentionMaxAge = tc.maxAge
			b.cfg.RetainOnClose = tc.retainOnClose
			b.cfg.RetentionReapInterval = tc.reapInterval

			interval, enabled := b.retentionSweepInterval()
			assert.Equal(t, tc.wantEnabled, enabled, "sweep enabled gating")
			if tc.wantEnabled {
				assert.Equal(t, tc.wantInterval, interval, "sweep interval")
			}
		})
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

// TestRestore_NilServiceEntry_RejectedNotPanic: a corrupt retained record whose
// StackManifest.Services carries a nil service entry (the shape a tampered/legacy
// `{"services":{"app":null}}` payload deserializes to) must be rejected with
// ErrValidation, NOT crash the backend. Restore() runs synchronously with no panic
// recovery before the service loop, so an unguarded nil-deref on m.Image would take
// down the backend goroutine. Only reachable via store corruption — provision and
// recovery both run ParsePayload->Validate — so this is defense-in-depth that
// completes the existing "reject rather than nil-deref" corruption guard.
func TestRestore_NilServiceEntry_RejectedNotPanic(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)

	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: "u1",
		Tenant:            "tenant-a",
		ProviderUUID:      "prov-1",
		Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		StackManifest: &manifest.StackManifest{
			Services: map[string]*manifest.Manifest{manifest.DefaultServiceName: nil}, // corrupt: nil entry
		},
		CallbackURL:         "http://localhost/callback",
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0))},
		Status:              shared.RetentionStatusActive,
		Generation:          1,
		CreatedAt:           time.Now(),
	}))

	renameCalled := false
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { renameCalled = true; return nil },
	}

	// Must reject cleanly, not panic.
	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://localhost/cb"))
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)

	assert.False(t, renameCalled, "no volume rename must occur on a corrupt-record rejection")

	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "no provision entry must be created on a corrupt-record rejection")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status, "retained record must remain active")
}

// TestRestore_TenantMismatch_CollapsesToNotRetained: a request whose Tenant does
// not match the retained record's is DELIBERATELY collapsed into ErrNotRetained
// (NOT ErrValidation) so a cross-tenant caller cannot distinguish "exists but not
// yours" from "does not exist" — a no-info-leak guard. The record stays untouched:
// no rename, no provision entry, status/generation unchanged.
func TestRestore_TenantMismatch_CollapsesToNotRetained(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	seedActiveRetained(t, rs, "u1") // Tenant: "tenant-a"

	renameCalled := false
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { renameCalled = true; return nil },
	}

	req := restoreRequest("u2", "u1", "http://localhost/cb")
	req.Tenant = "tenant-b" // differs from the retained record's "tenant-a"

	err := b.Restore(context.Background(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrNotRetained, "cross-tenant must collapse to ErrNotRetained")
	assert.False(t, errors.Is(err, backend.ErrValidation),
		"must NOT be ErrValidation: that would leak that the record exists")

	assert.False(t, renameCalled, "no volume rename must occur on a tenant-mismatch rejection")

	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "no provision entry must be created on a tenant-mismatch rejection")

	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.Equal(t, 1, entry.Generation, "generation must be unchanged (no claim happened)")
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

// TestRestore_NormalizesLegacyUnnamedItem_Succeeds: a legacy single-service lease
// arrives from the chain with ServiceName="" while the retained record was
// normalized to "app" at Provision time. Restore must normalize the request
// before the shape check (same boundary contract as Provision/Update), so the
// restore SUCCEEDS instead of deterministically failing ErrValidation. Without the
// fix this single-service restore (the common case) is impossible.
func TestRestore_NormalizesLegacyUnnamedItem_Succeeds(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1") // retained record has ServiceName="app"

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

	// Legacy chain shape: a single item with an EMPTY ServiceName.
	req := restoreRequest("u2", "u1", server.URL)
	req.Items = []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: ""}}

	err := b.Restore(context.Background(), req)
	require.NoError(t, err, "legacy unnamed-item restore must normalize to \"app\" and pass the shape check")

	<-callbackReceived

	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 5*time.Second, 20*time.Millisecond, "u2 must reach Ready")

	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		return gerr == nil && e == nil
	}, 5*time.Second, 20*time.Millisecond, "retained record for u1 must be deleted on successful restore")

	// The normalized "app" service name must flow into the adopt rename.
	mu.Lock()
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()
	assert.Contains(t, gotRenames, restoreRenameCall{
		old: retainedName(canonicalVolumeName("u1", manifest.DefaultServiceName, 0)),
		new: canonicalVolumeName("u2", manifest.DefaultServiceName, 0),
	}, "adopt must rename retained(u1) → canonical(u2) using the normalized service name")

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

// TestRestore_WorkerPanic_PopulatesFailureCallback verifies FIX E: doRestore's
// panic defer populates ReplaceResult.Failure.{CallbackErr,LastError} (and the
// top-level CallbackErr), so the actor's evReplaceFailed fires a NON-empty tenant
// callback. The panic is induced through the real DI seam (compose.UpFn panics);
// the callback's Error field is the user-visible symptom that was empty before
// the fix. info.CallbackErr flows verbatim into the callback Error (lease_sm.go).
func TestRestore_WorkerPanic_PopulatesFailureCallback(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			panic("induced restore worker panic")
		},
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	var gotError atomic.Value
	var gotStatus atomic.Value
	callbackReceived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		gotError.Store(payload.Error)
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
	require.NoError(t, err) // route+ack succeed; the panic is asynchronous

	<-callbackReceived

	// The failure callback must carry a NON-EMPTY error (FIX E: pre-fix this was
	// empty because the panic defer left Failure.CallbackErr unset).
	assert.Equal(t, string(backend.CallbackStatusFailed), gotStatus.Load(), "panic must yield a FAILED callback")
	assert.Equal(t, leasesm.ErrMsgInternal, gotError.Load(),
		"panic callback Error must be the canonical internal-error message, not empty")

	// The provision's LastError (set from info.LastError) must be non-empty and
	// name the panic.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusFailed && p.LastError != ""
	}, 5*time.Second, 20*time.Millisecond, "u2 must settle Failed with a non-empty LastError")

	b.provisionsMu.RLock()
	lastErr := b.provisions["u2"].LastError
	b.provisionsMu.RUnlock()
	assert.Contains(t, lastErr, "restore panic", "LastError must come from the panic defer's Failure.LastError")

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

// ---------------------------------------------------------------------------
// ENG-325 fix: drive restore volume ops off RetainedVolumeNames (not Items×Qty)
// ---------------------------------------------------------------------------

// mixedStackManifest is a TWO-service stack: "db" (stateful) and "web"
// (stateless). Both images pass the default registry allowlist and carry no
// active health check so verifyStartup falls through to the inspect path.
func mixedStackManifest() *manifest.StackManifest {
	return &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"db":  {Image: "nginx:latest"},
			"web": {Image: "nginx:latest"},
		},
	}
}

// mixedItems is the lease item set for the mixed stack: db (stateful) + web
// (stateless), each Quantity 1.
func mixedItems() []backend.LeaseItem {
	return []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
	}
}

// seedMixedRetained writes an ACTIVE retained record for a MIXED lease: two
// services (db stateful, web stateless) but only ONE retained volume
// (fred-retained-<orig>-db-0) — the stateless "web" service has no managed
// volume on disk. This is the exact shape that breaks an Items×Quantity volume
// derivation (it would invent a non-existent fred-retained-<orig>-web-0).
func seedMixedRetained(t *testing.T, rs *shared.RetentionStore, orig string) shared.RetentionEntry {
	t.Helper()
	e := shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		Tenant:              "tenant-a",
		ProviderUUID:        "prov-1",
		Items:               mixedItems(),
		StackManifest:       mixedStackManifest(),
		CallbackURL:         "http://localhost/callback",
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(orig, "db", 0))}, // only db has a volume
		Status:              shared.RetentionStatusActive,
		Generation:          1,
		CreatedAt:           time.Now(),
	}
	require.NoError(t, rs.Put(e))
	return e
}

// mixedRestoreRequest builds a RestoreRequest matching seedMixedRetained's shape.
func mixedRestoreRequest(newLease, fromLease, callbackURL string) backend.RestoreRequest {
	return backend.RestoreRequest{
		LeaseUUID:     newLease,
		FromLeaseUUID: fromLease,
		Tenant:        "tenant-a",
		ProviderUUID:  "prov-1",
		Items:         mixedItems(),
		CallbackURL:   callbackURL,
	}
}

// happyMixedComposeMock is happyComposeMock for a 2-service stack: PS reports a
// running container for BOTH "db" and "web" so verifyStartup passes per-service.
func happyMixedComposeMock(mu *sync.Mutex, downProjects *[]string, upErr error) *mockComposeExecutor {
	return &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			return upErr
		},
		PSFn: func(_ context.Context, _ string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "container-db", Service: "db", State: "running"},
				{ID: "container-web", Service: "web", State: "running"},
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

// TestRestore_MixedStatefulStatelessLease is the regression test for the HIGH bug:
// a lease with a stateful service ("db") AND a stateless service ("web", no
// volume) must restore SUCCESSFULLY. The fix drives the adopt rename off the
// record's RetainedVolumeNames, so exactly the db volume is renamed and NO
// phantom rename of a non-existent web volume is ever attempted. The
// RenameVolumeFn ERRORS for any name not in RetainedVolumeNames to prove the
// phantom is never requested (an Items×Quantity derivation would request
// fred-retained-u1-web-0 → hard error → whole restore fails).
func TestRestore_MixedStatefulStatelessLease(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedMixedRetained(t, rs, "u1")

	retainedSet := map[string]bool{retainedName(canonicalVolumeName("u1", "db", 0)): true}

	var mu sync.Mutex
	var downProjects []string
	var renames []restoreRenameCall
	b.compose = happyMixedComposeMock(&mu, &downProjects, nil)
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(old, new string) error {
			mu.Lock()
			renames = append(renames, restoreRenameCall{old, new})
			mu.Unlock()
			// Prove no phantom is ever attempted: error for any source name that is
			// not an actually-retained volume.
			if !retainedSet[old] {
				return errors.New("phantom volume rename attempted for non-retained source: " + old)
			}
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

	err := b.Restore(context.Background(), mixedRestoreRequest("u2", "u1", server.URL))
	require.NoError(t, err)

	<-callbackReceived

	// u2 must reach Ready (the restore SUCCEEDED despite the stateless service).
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 5*time.Second, 20*time.Millisecond, "mixed-lease restore must reach Ready")

	// The retained record for u1 must be deleted on success.
	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		return gerr == nil && e == nil
	}, 5*time.Second, 20*time.Millisecond, "retained record for u1 must be deleted on successful mixed restore")

	mu.Lock()
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()

	// Exactly ONE rename happened: the db volume retained(u1) → canonical(u2).
	require.Len(t, gotRenames, 1, "exactly one rename (db) must happen; the stateless web service has no volume")
	assert.Equal(t, restoreRenameCall{
		old: retainedName(canonicalVolumeName("u1", "db", 0)),
		new: canonicalVolumeName("u2", "db", 0),
	}, gotRenames[0], "the sole rename must adopt the db volume retained(u1) → canonical(u2)")

	// No rename for the web service was ever attempted (no phantom source/target).
	for _, r := range gotRenames {
		assert.NotContains(t, r.old, "-web-", "no rename of a web-service source must be attempted")
		assert.NotContains(t, r.new, "-web-", "no rename targeting a web-service canonical must be attempted")
	}

	b.stopCancel()
	b.wg.Wait()
}

// TestReconcileRestoring_MixedLease_RollsBackWithoutWedging proves the
// reconcile-rollback arm no longer wedges a mixed lease: a restoring record with
// the mixed shape (db stateful + web stateless) and no live provision for the
// new lease must re-quarantine ONLY the db volume (driven off RetainedVolumeNames)
// and successfully revert to active — NOT get stuck in restoring forever. Before
// the fix, the Items×Quantity derivation tried to rename a non-existent
// fred-u2-web-0, RenameVolume erred, failed=true, and the record stayed restoring.
func TestReconcileRestoring_MixedLease_RollsBackWithoutWedging(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	rs := attachRetentionStore(t, b)

	retainedSet := map[string]bool{retainedName(canonicalVolumeName("u1", "db", 0)): true}

	var mu sync.Mutex
	var downProjects []string
	var renames []restoreRenameCall
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
			renames = append(renames, restoreRenameCall{old, new})
			mu.Unlock()
			// Error for any non-retained target so a phantom web rename would set
			// failed=true and wedge the record — exactly the bug we are guarding.
			if !retainedSet[new] { // re-quarantine target is the retained name
				return errors.New("phantom re-quarantine attempted, target not retained: " + new)
			}
			return nil
		},
	}

	e := shared.RetentionEntry{
		OriginalLeaseUUID:   "u1",
		NewLeaseUUID:        "u2",
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          3,
		Items:               mixedItems(),
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName("u1", "db", 0))}, // only db
	}
	require.NoError(t, rs.Put(e))

	b.reconcileRestoring(context.Background(), e)

	mu.Lock()
	gotDown := append([]string(nil), downProjects...)
	gotRenames := append([]restoreRenameCall(nil), renames...)
	mu.Unlock()

	// Compose Down ran for the new lease's project.
	assert.Contains(t, gotDown, composeProjectName("u2"), "compose Down must run for the orphaned restore")

	// Exactly ONE re-quarantine: db canonical(u2) → retained(u1). No web phantom.
	require.Len(t, gotRenames, 1, "exactly one re-quarantine (db) must happen; web has no volume")
	assert.Equal(t, restoreRenameCall{
		old: canonicalVolumeName("u2", "db", 0),
		new: retainedName(canonicalVolumeName("u1", "db", 0)),
	}, gotRenames[0], "the sole re-quarantine must move db canonical(u2) → retained(u1)")

	// The record must have reverted to active (NOT wedged in restoring), with
	// Generation bumped and NewLeaseUUID cleared.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry, "retention record for u1 must still exist after rollback")
	assert.Equal(t, shared.RetentionStatusActive, entry.Status, "record must revert to active, NOT stay wedged in restoring")
	assert.Equal(t, 4, entry.Generation, "generation must be bumped by RevertToActive")
	assert.Empty(t, entry.NewLeaseUUID, "NewLeaseUUID must be cleared after rollback")
}

// attachReleaseStore wires a real ReleaseStore (backed by a temp bbolt DB) into
// the Backend and registers a cleanup to close it.
func attachReleaseStore(t *testing.T, b *Backend) *shared.ReleaseStore {
	t.Helper()
	s, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: filepath.Join(t.TempDir(), "releases.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.releaseStore = s
	return s
}

// TestRestore_Success_RefreshesResourceMetrics drives a restore to Ready (mirroring
// TestRestore_Success_DeletesRecord) and asserts the resource-allocation gauge is
// refreshed on the SUCCESS path — not only on the failure/rollback paths. The CPU
// gauge is package-global, so it is reset to a sentinel that the docker-small
// allocation (0.5 CPU / 8.0 total = 0.0625) cannot coincidentally equal, then
// asserted to reflect that allocation after the synchronous Restore prelude returns.
func TestRestore_Success_RefreshesResourceMetrics(t *testing.T) {
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
	b.compose = happyComposeMock(&mu, &downProjects, nil)
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	// Reset the CPU gauge to a sentinel the real allocation cannot equal. With
	// DefaultConfig's 8.0 total cores, one docker-small (0.5) yields 0.0625.
	const sentinel = 0.99
	resourceCPUAllocatedRatio.Set(sentinel)
	require.InDelta(t, sentinel, testutil.ToFloat64(resourceCPUAllocatedRatio), 0.0001,
		"sanity: gauge reset to sentinel before restore")

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

	// The success-path refresh runs synchronously inside Restore (right after the
	// allocation loop), so the gauge already reflects the docker-small allocation.
	expectedCPURatio := 0.5 / b.cfg.TotalCPUCores // 0.5 / 8.0 = 0.0625; distinct from the 0.99 sentinel
	assert.InDelta(t, expectedCPURatio, testutil.ToFloat64(resourceCPUAllocatedRatio), 0.0001,
		"resource gauge must be refreshed on the restore success path (moved off the %v sentinel)", sentinel)

	<-callbackReceived
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 5*time.Second, 20*time.Millisecond, "u2 must reach Ready")

	b.stopCancel()
	b.wg.Wait()
}

// TestDeprovision_Retain_HydratesNilManifestFromReleaseStore verifies that when a
// recovered provision carries a nil StackManifest (cold-start recover left it nil),
// the soft-delete path hydrates it from the release store's latest ACTIVE release
// before writing the retention record — so the retained data stays API-restorable
// (Restore rejects nil-manifest records as corrupt).
func TestDeprovision_Retain_HydratesNilManifestFromReleaseStore(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     "u1",
			Tenant:        "tenant-a",
			Status:        backend.ProvisionStatusReady,
			Quantity:      1,
			Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
			StackManifest: nil, // recovered provision: manifest not restored from labels
		}},
	})

	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)
	relStore := attachReleaseStore(t, b)

	// Seed the latest ACTIVE release whose Manifest parses to a stack. ParsePayload
	// wraps a legacy flat payload under DefaultServiceName, matching recover.go.
	require.NoError(t, relStore.Append("u1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.27"}`),
		Image:     "nginx:1.27",
		Status:    "active",
		CreatedAt: time.Now(),
	}))

	b.volumes = &mockVolumeManager{
		ListFn:         func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called in RetainOnClose=true path, got %q", id)
			return nil
		},
	}

	err := b.Deprovision(context.Background(), "u1")
	require.NoError(t, err)

	var entry *shared.RetentionEntry
	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		if gerr != nil || e == nil {
			return false
		}
		entry = e
		return true
	}, 5*time.Second, 20*time.Millisecond, "retention record for u1 must appear")

	// The record's manifest must be HYDRATED (non-nil) from the release store.
	require.NotNil(t, entry.StackManifest, "StackManifest must be hydrated from the release store")
	require.Contains(t, entry.StackManifest.Services, manifest.DefaultServiceName,
		"hydrated manifest must carry the default service")
	assert.Equal(t, "nginx:1.27", entry.StackManifest.Services[manifest.DefaultServiceName].Image,
		"hydrated manifest must reflect the latest active release image")
}

// TestDeprovision_Retain_NilManifestNoRelease_StillRetains verifies that when the
// recovered provision's StackManifest is nil AND there is no release to hydrate
// from, the soft-delete path STILL writes the retention record (preserving the
// data for manual recovery) with a nil manifest and without panicking — the warn
// path. The data must never be destroyed just because it is un-restorable.
func TestDeprovision_Retain_NilManifestNoRelease_StillRetains(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     "u1",
			Tenant:        "tenant-a",
			Status:        backend.ProvisionStatusReady,
			Quantity:      1,
			Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
			StackManifest: nil,
		}},
	})

	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)
	attachReleaseStore(t, b) // empty: no release to hydrate from

	b.volumes = &mockVolumeManager{
		ListFn:         func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Fatalf("Destroy must NOT be called in RetainOnClose=true path, got %q", id)
			return nil
		},
	}

	err := b.Deprovision(context.Background(), "u1")
	require.NoError(t, err)

	var entry *shared.RetentionEntry
	require.Eventually(t, func() bool {
		e, gerr := rs.Get("u1")
		if gerr != nil || e == nil {
			return false
		}
		entry = e
		return true
	}, 5*time.Second, 20*time.Millisecond, "retention record for u1 must STILL be written (data preserved)")

	// Data preserved: the record exists with the retained volume tracked.
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	assert.ElementsMatch(t, []string{"fred-retained-u1-app-0"}, entry.RetainedVolumeNames,
		"the volume must be retained even though the manifest is nil")
	// Manifest stays nil (no release to hydrate from) — the warn path, no panic.
	assert.Nil(t, entry.StackManifest, "manifest stays nil when no release exists to hydrate from")
}

// TestRestore_AdoptInsufficientResources_RollsBack (G3) verifies that when
// TryAllocateAdopt fails because the pool lacks sufficient CPU or memory
// headroom (CPU gate fires; disk is NOT re-gated on adopt), Restore returns
// ErrInsufficientResources, the pool has no live allocation leak, and the
// reservation (provision entry) is removed.
//
// Setup: rebuild b.pool with TotalCPUCores=0.1, which is less than the
// docker-small profile's 0.5 cores — so TryAllocateAdopt's CPU check fires.
// (TryAllocateAdopt skips the disk gate but still gates CPU and memory.)
func TestRestore_AdoptInsufficientResources_RollsBack(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	rs := attachRetentionStore(t, b)
	seedActiveRetained(t, rs, "u1") // docker-small qty=1 → 0.5 CPU, 512 MB, 1024 MB disk

	// Rebuild the pool with insufficient CPU headroom (0.1 < 0.5 required by
	// docker-small). DiskMB is generous (TryAllocateAdopt skips that gate anyway).
	b.cfg.TotalCPUCores = 0.1
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	renameCalled := false
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { renameCalled = true; return nil },
	}

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://localhost/cb"))

	// Must surface ErrInsufficientResources.
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInsufficientResources,
		"TryAllocateAdopt CPU failure must map to ErrInsufficientResources")

	// Pool must be fully released — no allocation leaked.
	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB,
		"pool must be fully released after TryAllocateAdopt failure (no live leak)")
	assert.Equal(t, float64(0), s.AllocatedCPU,
		"CPU must be fully released after TryAllocateAdopt failure")

	// Provision entry must have been removed by the rollback.
	b.provisionsMu.RLock()
	_, has := b.provisions["u2"]
	b.provisionsMu.RUnlock()
	assert.False(t, has, "provision entry must be removed after TryAllocateAdopt failure rollback")

	// No rename must have happened — adopt only runs after TryAllocateAdopt succeeds.
	assert.False(t, renameCalled, "no volume rename must occur before TryAllocateAdopt fails")

	// The retained record for u1 must remain active (no claim happened).
	entry, rerr := rs.Get("u1")
	require.NoError(t, rerr)
	require.NotNil(t, entry, "retained record must remain after TryAllocateAdopt failure")
	assert.Equal(t, shared.RetentionStatusActive, entry.Status,
		"retained record must stay active (no ClaimForRestore was attempted)")
	assert.Equal(t, 1, entry.Generation, "generation must be unchanged (no claim)")
}

// TestRollback_RevertStoreError_KeepsLiveCounted verifies make-before-break: when
// RevertToActive returns a STORE ERROR during rollback, the live allocation is NOT
// released (F stays counted as live, no under-count), the leak counter increments,
// and reconcileRestoring later releases it after a successful revert. ENG-376 site 4.
func TestRollback_RevertStoreError_KeepsLiveCounted(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b)

	// A restoring record + a live allocation for the new lease (qty 1 → 1024 MB).
	rec := retentionEntryFixture("orig", "t1", time.Now())
	rec.Items = []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}}
	rec.Status = shared.RetentionStatusRestoring
	rec.NewLeaseUUID = "new"
	rec.Generation = 2
	require.NoError(t, rs.Put(rec))
	require.NoError(t, b.pool.TryAllocateAdopt("new-app-0", "docker-micro", "t1"))

	b.volumes = &mockVolumeManager{RenameVolumeFn: func(_, _ string) error { return nil }} // re-quarantine succeeds

	// Capture the live footprint AFTER allocation but BEFORE forcing the revert error.
	// (Assert against this delta, not a literal MB — the pool sizes the live allocation
	// from its own SKU profiles, which differ from b.cfg.GetSKUProfile's withMicroSKU view.)
	allocBefore := b.pool.Stats().AllocatedDiskMB
	require.Greater(t, allocBefore, int64(0), "sanity: live allocation is counted")
	require.NoError(t, rs.Close()) // force RevertToActive to ERROR

	allocated := []string{"new-app-0"}
	recCopy := rec
	b.rollbackRestoreAdoption(context.Background(), "new", allocated, &recCopy, true, b.logger)

	// Live allocation NOT released on a revert store-error → still counted (no under-count).
	assert.Equal(t, allocBefore, b.pool.Stats().AllocatedDiskMB, "live stays counted on revert store-error")
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}

// TestStatusAudit_Cleanup_DoesNotProtectReapingCanonical ensures a reaping record's
// canonical volume is NOT added to the orphan-cleanup protected set (it must be reaped).
func TestStatusAudit_Cleanup_DoesNotProtectReapingCanonical(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	rs := attachRetentionStore(t, b)

	reaping := retentionEntryFixture("lease-r", "t1", time.Now())
	reaping.Status = shared.RetentionStatusReaping
	reaping.RetainedVolumeNames = []string{"fred-retained-lease-r-app-0"}
	require.NoError(t, rs.Put(reaping))

	var destroyed []string
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{"fred-lease-r-app-0"}, nil }, // a stray CANONICAL
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))
	assert.Contains(t, destroyed, "fred-lease-r-app-0", "reaping canonical must NOT be protected from orphan cleanup")
}
