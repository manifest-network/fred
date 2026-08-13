package docker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestDoDeprovision_ContainerlessLease_PurgesStrandedReleaseHistory proves ENG-410's
// close-time fix: a lease whose container was already gone at on-chain close has release
// history but no provision entry (recoverState rebuilds b.provisions from live containers
// only), so a deprovision RPC hits the !exists short-circuit before the terminal
// releaseStore.Delete and leaves a stale "active" record that audit-lease-status flags
// until the 90-day RemoveOlderThan TTL. The short-circuit must still purge that history.
func TestDoDeprovision_ContainerlessLease_PurgesStrandedReleaseHistory(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	rel := attachReleaseStore(t, b)
	require.NoError(t, rel.Append("u1", shared.Release{Image: "stack", Status: "active", CreatedAt: time.Now()}))

	// No provision entry for u1 → doDeprovision takes the !exists path.
	require.NoError(t, b.doDeprovision(context.Background(), "u1"))

	releases, err := rel.List("u1")
	require.NoError(t, err)
	assert.Empty(t, releases, "containerless deprovision must purge stranded release history (ENG-410)")
}

// TestDeprovisionGiveUp_WritesReapingTombstone verifies a give-up (max volume
// cleanup attempts) writes a reaping tombstone for the leaked canonical volumes so
// the footprint keeps counting + the sweep auto-retries, instead of a silent
// uncounted leak. ENG-376 site 3.
func TestDeprovisionGiveUp_WritesReapingTombstone(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b) // RetainOnClose stays false → non-retain destroy arm

	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	// The give-up branch returns nil to the actor (it abandons to manual cleanup and
	// fires a failed callback), so do not assert on Deprovision's return value here —
	// the load-bearing assertions are the tombstone + the leak counter below.
	_ = b.Deprovision(context.Background(), "u1")

	// Poll for the reaping tombstone.
	var got *shared.RetentionEntry
	require.Eventually(t, func() bool {
		g, e := rs.Get("u1")
		if e != nil || g == nil {
			return false
		}
		got = g
		return true
	}, 5*time.Second, 20*time.Millisecond, "reaping tombstone for u1 must be written at give-up")

	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.ElementsMatch(t, []string{"fred-u1-app-0"}, got.RetainedVolumeNames)
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}

// TestDeprovisionGiveUp_ListFails_RecordsBothNamespaces verifies the recordGiveUpLeak
// fallback (volumes.List error): the tombstone records BOTH the canonical name and the
// fred-retained-* name per item. A retain-path partial rename may have moved a volume
// into the retained namespace before failing, so recording only the canonical name would
// let the sweep "succeed" against the (idempotent) non-existent canonical name, drop the
// tombstone, and leave the fred-retained-* volume on disk and untracked. ENG-376.
func TestDeprovisionGiveUp_ListFails_RecordsBothNamespaces(t *testing.T) {
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b)

	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return nil, errors.New("statfs EIO") }, // force the fallback
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	_ = b.Deprovision(context.Background(), "u1")

	var got *shared.RetentionEntry
	require.Eventually(t, func() bool {
		g, e := rs.Get("u1")
		if e != nil || g == nil {
			return false
		}
		got = g
		return true
	}, 5*time.Second, 20*time.Millisecond, "reaping tombstone for u1 must be written at give-up")

	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.ElementsMatch(t,
		[]string{"fred-u1-app-0", "fred-retained-u1-app-0"},
		got.RetainedVolumeNames,
		"fallback must record BOTH the canonical and the fred-retained- name so whichever exists is destroyed before the tombstone is deleted")
}

// TestDeprovision_PartialFailure_AuthorsCleanupFailed drives the partial-failure
// branch (a stuck container that RemoveContainer cannot remove) and verifies the
// provision is left with the curated ReasonCleanupFailed / MsgCleanupFailed pair,
// while the verbose detail is retained operator-side in LastError (ENG-508).
func TestDeprovision_PartialFailure_AuthorsCleanupFailed(t *testing.T) {
	const lease = "u1"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error {
			return errors.New("container removal blocked: device or resource busy")
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
			ContainerIDs: []string{"c-stuck"},
		}},
	})
	// Force compose.Down to fail so teardown falls back to per-container removal,
	// where the mocked RemoveContainer error populates errs → partial-failure branch.
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose project metadata missing")
		},
	}

	err := b.doDeprovision(context.Background(), lease)
	require.Error(t, err, "partial container-removal failure must surface an error")

	prov, ok := b.provisions[lease]
	require.True(t, ok, "partial failure must keep the provision visible for retry")
	assert.Equal(t, backend.ReasonCleanupFailed, prov.Reason,
		"partial deprovision failure must author ReasonCleanupFailed")
	assert.Equal(t, backend.MsgCleanupFailed, prov.Message,
		"Message must be the curated MsgCleanupFailed const (tenant-facing)")
	assert.Contains(t, prov.LastError, "deprovision partially failed",
		"verbose detail must be retained operator-side in LastError")
	assert.Contains(t, prov.LastError, "container removal blocked",
		"LastError must retain the underlying (operator-only) failure text")
	assert.NotEqual(t, prov.Message, prov.LastError,
		"tenant-facing Message must be the curated const, distinct from the verbose LastError")
}

// TestDoDeprovision_ComposeDownFails_RemovesEveryRecordedContainer pins the
// compensating behaviour that keeps ENG-372 closed when compose Down does not
// finish what it started.
//
// docker/compose v5 changed teardown to run its per-container removals on the
// errgroup's DERIVED context (`eg, ctx := errgroup.WithContext(ctx)` in
// pkg/compose/down.go) where v2 discarded it (`eg, _ :=`). Under v2, one
// container failing to be removed left its siblings to finish; under v5 the
// first failure cancels the group, so siblings can be aborted part-way through
// ContainerStop/ContainerRemove — and a container that is never removed never
// has its anonymous volumes reaped. That is exactly the leak ENG-372 exists to
// prevent, and it is invisible: Down returns an error, teardown continues, and
// the volume simply accumulates.
//
// fred's compensation is the per-container fallback below, which is only
// sufficient if it covers EVERY container in the record rather than stopping at
// the first one, or at the one compose happened to fail on. This test pins that
// coverage. It is deliberately a unit test: the cancellation lives inside
// compose, and inducing a deterministic mid-flight removal failure against a
// real daemon would be racy. What fred owns — and what actually keeps the leak
// closed — is the fan-out asserted here. The reaping itself (RemoveContainer
// passing RemoveVolumes:true) is pinned live by
// TestIntegration_Docker_RemoveContainer_RemovesAnonymousVolumes; together the
// two cover the whole path.
//
// TestDeprovision_PartialFailure_AuthorsCleanupFailed covers the arm where that
// fallback itself fails; this one covers the arm where it must succeed.
func TestDoDeprovision_ComposeDownFails_RemovesEveryRecordedContainer(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000d"
	recorded := []string{"c-web-0", "c-web-1", "c-sidecar-0"}

	var removed []string
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, id string) error {
			removed = append(removed, id)
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusReady,
			Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 3, ServiceName: "app"}},
			ContainerIDs: recorded,
		}},
	})

	// Simulate v5 aborting teardown part-way: Down reports failure, and the
	// containers it did not get to are still present for the fallback to reap.
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose down canceled after first removal failed")
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), lease),
		"a failed Down whose fallback removes every container is a successful deprovision")

	assert.ElementsMatch(t, recorded, removed,
		"every recorded container must be removed individually when compose Down fails; "+
			"a container skipped here keeps its anonymous volumes forever (ENG-372)")
}

// TestDoDeprovision_Success_ReleasesPoolReservation locks the Release-pairing
// invariant the pool-authoritative recoverState rule depends on (ENG-567): a
// successful (non-retain) deprovision must release the lease's pool reservation
// and remove it from b.provisions, so recoverState never preserves a phantom key
// for an untracked lease.
func TestDoDeprovision_Success_ReleasesPoolReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000b"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusReady,
			Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			ContainerIDs: []string{"c1"},
		}},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	require.NoError(t, b.doDeprovision(context.Background(), lease))

	got := b.pool.Stats()
	assert.Equal(t, int64(0), got.AllocatedDiskMB, "deprovision must release the pool reservation")
	assert.Equal(t, 0, got.AllocationCount)
	b.provisionsMu.RLock()
	_, stillTracked := b.provisions[lease]
	b.provisionsMu.RUnlock()
	assert.False(t, stillTracked, "deprovisioned lease must be removed from b.provisions")
}

// TestDoDeprovision_GiveUp_ReleasesPoolReservation locks the give-up arm of the
// Release-pairing invariant: when volume cleanup fails maxVolumeCleanupAttempts
// times, doDeprovision gives up — it must releaseLive() and Delete the lease,
// so no untracked lease is left holding a pool key.
func TestDoDeprovision_GiveUp_ReleasesPoolReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000c"
	existing := map[string]*provision{
		lease: {
			ProvisionState: leasesm.ProvisionState{
				LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusFailed,
				Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			},
			VolumeCleanupAttempts: 2, // next attempt (3) == maxVolumeCleanupAttempts → give up
		},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, existing)
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error {
		return errors.New("volume stuck")
	}}
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	// Give-up returns nil (provision deleted, manual cleanup logged).
	require.NoError(t, b.doDeprovision(context.Background(), lease))

	got := b.pool.Stats()
	assert.Equal(t, int64(0), got.AllocatedDiskMB, "give-up must release the reservation before deleting the lease")
	assert.Equal(t, 0, got.AllocationCount)
	b.provisionsMu.RLock()
	_, stillTracked := b.provisions[lease]
	b.provisionsMu.RUnlock()
	assert.False(t, stillTracked, "given-up lease must be removed from b.provisions")
}
