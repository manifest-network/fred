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
