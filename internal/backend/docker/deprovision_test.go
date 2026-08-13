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
		// The fallback re-discovers by label (ENG-647); an empty listing keeps this
		// test's subject the RECORDED container, exactly as before.
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
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
		// Daemon reports nothing (labels lost / already delisted), so the recorded list
		// is the only source left. That is the case this test exists to pin; the
		// complementary case — an EMPTY record and a daemon that does report the
		// containers — is TestDoDeprovision_ComposeDownFails_RemovesDiscoveredContainersWhenRecordIsEmpty.
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
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

// TestDoDeprovision_ComposeDownFails_RemovesDiscoveredContainersWhenRecordIsEmpty is
// the complement of TestDoDeprovision_ComposeDownFails_RemovesEveryRecordedContainer,
// and the reason the fallback had to stop trusting the record (ENG-647).
//
// A lease that never reached Ready carries NO ContainerIDs — Restore reserves the
// entry empty and only the success paths fill it in, and recoverState's status-keyed
// merge preserves an existing Failed entry by pointer, so it is never backfilled while
// the process runs. Walking that list removes nothing precisely when a container
// leaked. Discovery by fred label is what closes it.
func TestDoDeprovision_ComposeDownFails_RemovesDiscoveredContainersWhenRecordIsEmpty(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-00000000000e"

	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c-web-0", LeaseUUID: lease, SKU: "docker-small"},
				{ContainerID: "c-web-1", LeaseUUID: lease, SKU: "docker-small"},
				{ContainerID: "c-other", LeaseUUID: "someone-else", SKU: "docker-small"},
			}, nil
		},
		RemoveContainerFn: func(_ context.Context, id string) error {
			removed = append(removed, id)
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusFailed,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
			// ContainerIDs deliberately absent: the failed-restore shape.
		}},
	})
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return errors.New("compose down canceled after first removal failed")
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), lease),
		"a failed Down whose fallback removes every container is a successful deprovision")

	assert.ElementsMatch(t, []string{"c-web-0", "c-web-1"}, removed,
		"the lease's containers must be found by label when the record names none; "+
			"anything missed here keeps its anonymous volumes forever (ENG-372)")
}

// ---------------------------------------------------------------------------
// ENG-647 retryability guard: a close must not eat an in-flight restore's data.
//
// While a record is restoring, the ORIGINAL lease's retained data lives under the NEW
// lease's canonical names, so closing the new lease sees volumes that look like its
// own. Destroying them, or re-retaining them under the closing lease, permanently
// kills the original lease's restore.
// ---------------------------------------------------------------------------

// seedRestoringInto puts a restoring record whose data has been adopted into
// newLease's namespace, and returns the canonical name that adoption produced.
//
// The record's SKU is docker-micro so that the accounting tests below — which pin
// their backend to a single known profile via withMicroSKU — can resolve its
// footprint. A SKU the profile map does not carry silently sizes the record at 0 MB,
// which would make a "the bytes stay counted" assertion pass vacuously.
func seedRestoringInto(t *testing.T, rs *shared.RetentionStore, orig, newLease string) string {
	t.Helper()
	retained := retainedName(canonicalVolumeName(orig, "app", 0))
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   orig,
		NewLeaseUUID:        newLease,
		Tenant:              "tenant-a",
		Status:              shared.RetentionStatusRestoring,
		Generation:          3,
		Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		RetainedVolumeNames: []string{retained},
	}))
	return retainedToNewCanonical(retained, orig, newLease)
}

func TestDoDeprovision_RetainPath_SkipsVolumesClaimedByRestoringRecord(t *testing.T) {
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		}},
	})
	b.cfg.RetainOnClose = true
	rs := attachRetentionStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease)

	var renames [][2]string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			// app-0 is u1's data adopted into u2's namespace; app-1 is genuinely u2's.
			return []string{claimedVol, canonicalVolumeName(lease, "app", 1)}, nil
		},
		RenameVolumeFn: func(old, new string) error {
			renames = append(renames, [2]string{old, new})
			return nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("retain path must not destroy %q", id)
			return nil
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), lease))

	assert.Equal(t, [][2]string{{
		canonicalVolumeName(lease, "app", 1),
		retainedName(canonicalVolumeName(lease, "app", 1)),
	}}, renames,
		"only the closing lease's OWN volume may be retained; re-retaining the adopted one "+
			"under u2 leaves u1's record pointing at names that no longer exist (ENG-647)")

	orig, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, orig, "the original record must be untouched")
	assert.Equal(t, shared.RetentionStatusRestoring, orig.Status)
	assert.Equal(t, []string{retainedName(canonicalVolumeName("u1", "app", 0))}, orig.RetainedVolumeNames,
		"and must still name the data reconcileRestoring will re-quarantine")
}

// TestDoDeprovision_NonRetainPath_DoesNotDestroyVolumesClaimedByRestoringRecord is the
// data-loss pin: the destroy arm names volumes by convention (canonicalVolumeName),
// so without the guard it deletes the adopted data outright and unrecoverably.
func TestDoDeprovision_NonRetainPath_DoesNotDestroyVolumesClaimedByRestoringRecord(t *testing.T) {
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}},
		}},
	})
	// RetainOnClose stays false — the pure destroy path.
	rs := attachRetentionStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease)

	var destroyed []string
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), lease))

	assert.Equal(t, []string{canonicalVolumeName(lease, "app", 1)}, destroyed,
		"the volume claimed by u1's in-flight restore must NOT be destroyed — that is "+
			"unrecoverable loss of the data u1's record exists to protect (ENG-647)")
	assert.NotContains(t, destroyed, claimedVol)
}

// TestDoDeprovision_RestoringClaimLookupFails_DoesNotDestroy pins the fail-safe: if the
// retention store cannot be read we cannot tell our volumes from an in-flight
// restore's, so we destroy neither and let the lease retry.
func TestDoDeprovision_RestoringClaimLookupFails_DoesNotDestroy(t *testing.T) {
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	})
	rs := attachRetentionStore(t, b)
	require.NoError(t, rs.Close()) // every retention read now fails

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("must not destroy %q when the claim lookup failed — the volume may belong to an in-flight restore", id)
			return nil
		},
	}

	err := b.doDeprovision(context.Background(), lease)
	require.Error(t, err, "an unreadable retention store must keep the lease Failed for retry, not report success")
}

// ---------------------------------------------------------------------------
// ENG-647 (PR #217 review): leaving a claimed volume on disk must not also hand
// its capacity back.
//
// A restoring record is counted by NEITHER projection — computeRetainedDiskMB skips
// every non-active status, and the admission pool is active+reaping — so while a
// restore is in flight the ONLY thing counting those bytes is the new lease's live
// pool allocation. The guard above stops the close from destroying the volume; if the
// close still released that allocation, the footprint would be counted by nobody and
// admission could over-commit against bytes that are physically present.
// ---------------------------------------------------------------------------

// claimedCloseBackend builds a lease whose canonical volume is claimed by an in-flight
// restore of another lease, with the restore's live allocation held in the pool.
func claimedCloseBackend(t *testing.T, retainOnClose bool) (*Backend, *shared.RetentionStore, string) {
	t.Helper()
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	withMicroSKU(b, 512)
	b.cfg.RetainOnClose = retainOnClose
	rs := attachRetentionStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease)

	// The restore's live allocation — the only thing counting the adopted bytes.
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-micro", "tenant-a"))
	b.refreshRetentionAccounting()
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "precondition: the restore's bytes are counted live")
	require.Zero(t, b.pool.Stats().RetainedDiskMB, "precondition: a restoring record counts as retained NOWHERE")

	return b, rs, claimedVol
}

func TestDoDeprovision_NonRetainPath_KeepsAllocationReservedForClaimedVolume(t *testing.T) {
	b, _, claimedVol := claimedCloseBackend(t, false)

	var destroyed []string
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, id string) error {
		destroyed = append(destroyed, id)
		return nil
	}}

	require.NoError(t, b.doDeprovision(context.Background(), "u2"))

	assert.NotContains(t, destroyed, claimedVol, "the claimed volume must survive the close")
	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB,
		"the bytes are still on disk, so they must still be reserved — releasing here counts a real "+
			"footprint nowhere and lets admission over-commit against it (ENG-647)")
	assert.Equal(t, 1, b.pool.Stats().AllocationCount)
}

func TestDoDeprovision_RetainPath_KeepsAllocationReservedForClaimedVolume(t *testing.T) {
	b, _, claimedVol := claimedCloseBackend(t, true)

	b.volumes = &mockVolumeManager{
		ListFn:         func() ([]string, error) { return []string{claimedVol}, nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("retain path must not destroy %q", id)
			return nil
		},
	}

	require.NoError(t, b.doDeprovision(context.Background(), "u2"))

	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB,
		"same on the retain arm: the claimed volume is excluded from THIS lease's retained record, "+
			"so nothing else counts it (ENG-647)")
}

// TestDoDeprovision_ClaimedVolumeAllocation_ReleasedByRestoreRollback proves the
// reservation is a HAND-OFF, not a leak: once reconcileRestoring completes the
// rollback the volume is back under the original record, the retained projection
// counts it, and only then is the live allocation released.
func TestDoDeprovision_ClaimedVolumeAllocation_ReleasedByRestoreRollback(t *testing.T) {
	b, rs, _ := claimedCloseBackend(t, false)
	b.volumes = &mockVolumeManager{
		DestroyFn:      func(_ context.Context, _ string) error { return nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	require.NoError(t, b.doDeprovision(context.Background(), "u2"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "close holds the reservation")

	// The restore rollback now runs (boot or retention sweep) and finishes the job.
	entry, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	b.reconcileRestoring(context.Background(), *entry)

	reverted, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, reverted)
	require.Equal(t, shared.RetentionStatusActive, reverted.Status, "the rollback completed")

	assert.Zero(t, b.pool.Stats().AllocatedDiskMB, "the live allocation is released only now")
	assert.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB,
		"and the same bytes are counted as retained instead — re-counted BEFORE released, never a gap")
	assertRetentionAccountingConsistent(t, b, "accounting must be consistent after the hand-off")
}

// TestDoDeprovision_NoClaimedVolume_StillReleasesAllocation is the control: the
// reservation is held because of the claimed volume, not because the close stopped
// releasing. Without this the tests above would pass against a release that never runs.
func TestDoDeprovision_NoClaimedVolume_StillReleasesAllocation(t *testing.T) {
	const lease = "u2"
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}},
	})
	withMicroSKU(b, 512)
	attachRetentionStore(t, b) // no restoring record → nothing is claimed
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-micro", "tenant-a"))
	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return nil }}

	require.NoError(t, b.doDeprovision(context.Background(), lease))

	assert.Zero(t, b.pool.Stats().AllocatedDiskMB,
		"an ordinary close destroys its volumes and must still return their capacity")
	assert.Zero(t, b.pool.Stats().AllocationCount)
}

// TestDeprovisionGiveUp_ExcludesVolumesClaimedByRestoringRecord closes the delayed
// half of the data-loss hole (ENG-647, PR #217 review).
//
// The close's volume branches refuse to destroy a volume an in-flight restore adopted
// into this lease's namespace — but a give-up after maxVolumeCleanupAttempts writes a
// REAPING tombstone, and a tombstone is a scheduled destroy: destroyReapingVolumes
// RemoveAll's every name it carries. recordGiveUpLeak collects by prefix
// (fred-{lease}-*), which matches the adopted volume exactly, so without this the
// guard merely deferred the destruction by one sweep — and made it look accounted-for
// on the way.
func TestDeprovisionGiveUp_ExcludesVolumesClaimedByRestoringRecord(t *testing.T) {
	const lease = "u2"
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 2,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "app"}},
		}, VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 512)
	rs := attachRetentionStore(t, b)
	claimedVol := seedRestoringInto(t, rs, "u1", lease) // fred-u2-app-0 IS u1's adopted data
	ownVol := canonicalVolumeName(lease, "app", 1)

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{claimedVol, ownVol}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			if id == ownVol {
				return errors.New("EBUSY") // drives the retry → give-up arm
			}
			t.Errorf("the close must never destroy the claimed volume %q", id)
			return nil
		},
	}

	_ = b.doDeprovision(context.Background(), lease) // give-up returns nil by contract

	tomb, err := rs.Get(lease)
	require.NoError(t, err)
	require.NotNil(t, tomb, "the give-up must still tombstone this lease's OWN leaked volume")
	assert.Equal(t, shared.RetentionStatusReaping, tomb.Status)
	assert.Equal(t, []string{ownVol}, tomb.RetainedVolumeNames,
		"the tombstone must name only this lease's own leak; naming the adopted volume would "+
			"schedule destroyReapingVolumes to RemoveAll another lease's retained data (ENG-647)")
	assert.NotContains(t, tomb.RetainedVolumeNames, claimedVol)

	// And the original record is untouched, so its restore is still possible.
	orig, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, orig)
	assert.Equal(t, shared.RetentionStatusRestoring, orig.Status)
}

// TestDoDeprovision_ClaimedVolume_AccountingAcrossRecoveryAndSweep is a
// CHARACTERIZATION test: it walks the three real events that follow a close which
// left a restore-claimed volume behind, and pins what the accounting does at each —
// including a KNOWN GAP that is deliberately not closed in this PR.
//
// The gap: holding the reservation (see the tests above) is only durable while the
// lease is still tracked. The close deletes the provision, and recoverState's
// pool-authoritative rebuild preserves a key only for a lease still in b.provisions
// (ENG-567), so the next reconcile tick drops it — default reconcile_interval 5m,
// against an exposure that lasts until the retention sweep re-quarantines the volume,
// default retention_reap_interval 1h. Between those the bytes are on disk and counted
// by nobody, and admission can over-commit against them.
//
// This is pinned rather than described so the window is visible in the suite and a
// future durable fix has to update this test deliberately instead of silently
// changing an unobserved behaviour. See the ENG-647 follow-up.
func TestDoDeprovision_ClaimedVolume_AccountingAcrossRecoveryAndSweep(t *testing.T) {
	b, rs, claimedVol := claimedCloseBackend(t, false)
	b.volumes = &mockVolumeManager{
		DestroyFn:      func(_ context.Context, _ string) error { return nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
		ListFn:         func() ([]string, error) { return []string{claimedVol}, nil },
	}

	// (1) The close: the reservation is held, because the bytes are still on disk and
	// a restoring record is counted by neither projection.
	require.NoError(t, b.doDeprovision(context.Background(), "u2"))
	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "close holds the reservation")
	assert.Zero(t, b.pool.Stats().RetainedDiskMB)

	// (2) The next reconcile tick. recoverState rebuilds the pool from live containers
	// plus the keys of still-tracked leases; this lease was deleted by the close, so
	// its key is dropped even though its volume is still on disk. THIS IS THE KNOWN GAP.
	b.docker = &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	require.NoError(t, b.recoverState(context.Background()))
	assert.Zero(t, b.pool.Stats().AllocatedDiskMB,
		"KNOWN GAP (ENG-647 follow-up): the rebuild is ownership-keyed, so an untracked "+
			"lease's reservation is dropped while its volume is still on disk")
	assert.Zero(t, b.pool.Stats().RetainedDiskMB,
		"and the restoring record still counts nowhere — this is the over-admission window")

	// (3) The retention sweep — the real end of the window. It re-quarantines the
	// volume, reverts the record, and the bytes become retained-counted again.
	require.NoError(t, b.runRetentionSweep(context.Background()))

	reverted, err := rs.Get("u1")
	require.NoError(t, err)
	require.NotNil(t, reverted)
	assert.Equal(t, shared.RetentionStatusActive, reverted.Status, "the sweep completed the rollback")
	assert.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB,
		"the bytes are counted again — the window closes here, not at the reconcile tick")
	assertRetentionAccountingConsistent(t, b, "accounting is consistent once the sweep has run")
}
