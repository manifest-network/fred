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
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// newBackendWithRetention returns a test backend wired with a real on-disk
// RetentionStore (no provisions in the map). The store is a genuine DI seam, not
// a test-only hook: production attaches the same store in NewBackend.
func newBackendWithRetention(t *testing.T) (*Backend, *shared.RetentionStore) {
	t.Helper()
	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	b.retentionStore = rs
	return b, rs
}

// retentionEntryFixture builds an active retention record for a stack lease.
func retentionEntryFixture(leaseUUID, tenant string, createdAt time.Time) shared.RetentionEntry {
	return shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            tenant,
		ProviderUUID:      "prov-1",
		Items: []backend.LeaseItem{
			{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
		},
		StackManifest: &manifest.StackManifest{
			Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}},
		},
		RetainedVolumeNames: []string{"fred-retained-lease-web-0"},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           createdAt,
	}
}

// TestGetProvision_RetainedIncludesPartition asserts the retained ProvisionInfo
// carries the record's Partition, so it can flow to the owner-only retained API
// responses. A non-partitioned record (Partition "") leaves the field empty.
func TestGetProvision_RetainedIncludesPartition(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	e := retentionEntryFixture("lease-p", "tenant-a", time.Now().Add(-time.Hour))
	e.Partition = "cust-a"
	require.NoError(t, rs.Put(e))

	info, err := b.GetProvision(context.Background(), "lease-p")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusRetained, info.Status)
	require.Equal(t, "cust-a", info.Partition)
}

// TestGetProvision_Retained_Active asserts that a soft-deleted lease (active
// retention record, no in-memory provision) surfaces Status=retained with a
// correct RetainedUntil (CreatedAt + RetentionMaxAge), Items, and Tenant.
func TestGetProvision_Retained_Active(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	createdAt := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-r", "tenant-a", createdAt)))

	info, err := b.GetProvision(context.Background(), "lease-r")
	require.NoError(t, err)
	assert.Equal(t, "lease-r", info.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusRetained, info.Status)
	assert.Equal(t, "prov-1", info.ProviderUUID)
	assert.Equal(t, "tenant-a", info.Tenant, "Tenant must be populated for the authz fallback")
	assert.Equal(t, b.cfg.Name, info.BackendName)
	assert.WithinDuration(t, createdAt.Add(b.cfg.RetentionMaxAge), info.RetainedUntil, time.Second)
	require.Len(t, info.Items, 1)
	assert.Equal(t, "web", info.Items[0].ServiceName)
	assert.Equal(t, "docker-micro", info.Items[0].SKU)
	assert.Equal(t, 2, info.Items[0].Quantity)
}

// TestGetProvision_Retained_Restoring asserts that a record in the restoring
// state (a tenant polling during their own restore) still resolves to retained,
// not a 404.
func TestGetProvision_Retained_Restoring(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	entry := retentionEntryFixture("lease-r", "tenant-a", time.Now())
	entry.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(entry))

	info, err := b.GetProvision(context.Background(), "lease-r")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusRetained, info.Status)
}

// TestStatusAudit_GetProvision_ReapingNotReported pins the §E "apply it evenly"
// audit for the GetProvision retained-status reader (info.go): a record in the
// reaping (pending-destroy) state must NOT be reported as retained. A reaping
// tombstone is an expired/evicted record being reaped OR a deprovision give-up's
// leaked footprint — never restorable — so reporting it as retained would tell a
// tenant their closed/expired lease is restorable. With no provision/diagnostics
// it must fall through to ErrNotProvisioned. (ENG-376)
func TestStatusAudit_GetProvision_ReapingNotReported(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	entry := retentionEntryFixture("lease-r", "tenant-a", time.Now())
	entry.Status = shared.RetentionStatusReaping
	require.NoError(t, rs.Put(entry))

	_, err := b.GetProvision(context.Background(), "lease-r")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned,
		"a reaping record must not be reported as retained; it falls through")
}

// TestGetProvision_NoRetentionRecord_NotProvisioned asserts that with a
// retention store present but no record (and no provision/diagnostics), the
// result is ErrNotProvisioned.
func TestGetProvision_NoRetentionRecord_NotProvisioned(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	_, err := b.GetProvision(context.Background(), "absent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

// TestGetProvision_RetentionPrecedesDiagnostics pins the ENG-329 invariant: when
// BOTH a stale Failed diagnostics entry AND an active retention record exist for
// the same lease, GetProvision returns retained — it never regresses to failed.
func TestGetProvision_RetentionPrecedesDiagnostics(t *testing.T) {
	b, rs := newBackendWithRetention(t)

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "diag.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = diagStore.Close() })
	b.diagnosticsStore = diagStore

	// Seed a stale Failed diagnostics entry...
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    "lease-r",
		ProviderUUID: "prov-1",
		Error:        "old failure before close",
		FailCount:    3,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	}))
	// ...and an active retention record for the same lease.
	require.NoError(t, rs.Put(retentionEntryFixture("lease-r", "tenant-a", time.Now())))

	info, err := b.GetProvision(context.Background(), "lease-r")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusRetained, info.Status,
		"retention must take precedence over the stale Failed diagnostics entry")
	assert.Empty(t, info.Reason, "retained response must not carry the stale diagnostics reason")
	assert.Empty(t, info.Message, "retained response must not carry the stale diagnostics message")
}

// TestGetProvision_NilRetentionStore_FallsBackToDiagnostics confirms the nil
// guard: a non-retaining config (retentionStore == nil) is unaffected and still
// falls back to diagnostics.
func TestGetProvision_NilRetentionStore_FallsBackToDiagnostics(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	b.retentionStore = nil

	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "diag.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = diagStore.Close() })
	b.diagnosticsStore = diagStore
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    "lease-d",
		ProviderUUID: "prov-1",
		Error:        "image pull failed: registry unreachable at /data/fred/...",
		Reason:       backend.ReasonImagePullFailed,
		Message:      "image pull failed",
		FailCount:    1,
		CreatedAt:    time.Now(),
	}))

	info, err := b.GetProvision(context.Background(), "lease-d")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, backend.ReasonImagePullFailed, info.Reason)
	assert.Equal(t, "image pull failed", info.Message)
	assert.NotContains(t, info.Message, "/data/fred", "verbose operator detail must not leak into Message")
}

// TestGetProvision_RetentionStoreError_FailsClosed verifies (FIX 2) the
// fail-closed invariant: a transient retention-store error must NOT fall through
// to the diagnostics fallback (which could return Status=failed) and misreport a
// RETAINED lease as failed. GetProvision must surface the error.
//
// The error is forced WITHOUT any prod-only seam: retentionStore is a concrete
// *shared.RetentionStore, so we Close() its bbolt db — a closed db returns an
// error on Get/View. A stale Failed diagnostics entry is also seeded to prove
// the function does NOT silently degrade to it.
func TestGetProvision_RetentionStoreError_FailsClosed(t *testing.T) {
	b, rs := newBackendWithRetention(t)

	// Seed a stale Failed diagnostics entry for the same lease — if the
	// retention error were swallowed, GetProvision would return this.
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "diag.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = diagStore.Close() })
	b.diagnosticsStore = diagStore
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    "lease-x",
		ProviderUUID: "prov-1",
		Error:        "old failure",
		FailCount:    2,
		CreatedAt:    time.Now(),
	}))

	// Close the retention store so its bbolt Get returns a real error. (Close is
	// idempotent via closeOnce, so the t.Cleanup double-close is harmless.)
	require.NoError(t, rs.Close())

	info, err := b.GetProvision(context.Background(), "lease-x")
	require.Error(t, err, "a retention-store error must be surfaced, not swallowed")
	assert.NotErrorIs(t, err, backend.ErrNotProvisioned, "must not collapse a store error into ErrNotProvisioned")
	assert.Nil(t, info, "no ProvisionInfo (especially not a Failed diagnostics entry) on a retention-store error")
}
