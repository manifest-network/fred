package docker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// heldDeleteErr is what a manager answers for a held deletion.
func heldDeleteErr(name string) error {
	return fmt.Errorf("%w (phase=removal reason=undeletable): xfs volume %q is held", ErrVolumeDeleteHeld, name)
}

func pendingDeletes(names ...string) volumeDeleteHoldSnapshot {
	snapshot := volumeDeleteHoldSnapshot{
		pending: make(map[string]struct{}, len(names)),
		holds:   make(map[string]volumeDeleteHoldView, len(names)),
	}
	for _, name := range names {
		parsed, err := parseManagedVolumeName(name)
		if err != nil {
			panic(err)
		}
		snapshot.pending[name] = struct{}{}
		snapshot.holds[name] = volumeDeleteHoldView{volume: parsed, reason: holdReasonUndeletable}
	}
	return snapshot
}

func residualDeletes(footprintMB int64, names ...string) volumeDeleteHoldSnapshot {
	snapshot := pendingDeletes(names...)
	for name, hold := range snapshot.holds {
		hold.residual, hold.footprintMB, hold.reason = true, footprintMB, holdReasonUsageNonzero
		snapshot.holds[name] = hold
	}
	return snapshot
}

// A held deletion is an ordinary failed destroy: the storage-mutation bracket
// passes it through and never latches the backend.
func TestStorageMutationGuard_VolumeDeleteHeldDoesNotLatch(t *testing.T) {
	t.Parallel()

	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	storeAuthorityGate, err := backendidentity.NewStorageAuthorityGate(func(error) { stop() })
	require.NoError(t, err)
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	b := &Backend{
		volumes: &mockVolumeManager{DestroyFn: func(context.Context, string) error {
			return heldDeleteErr(name)
		}},
		stopCtx: stopCtx, stopCancel: stop,
		storeAuthorityGate: storeAuthorityGate,
	}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	installTestStorageMutationAdapters(b)

	sink := b.volumes.(volumeDestroyer)
	err = b.mutationAdapter().destroyVolume(context.Background(), sink, name)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	require.NotErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.NoError(t, b.terminalStorageAuthorityError(), "a held deletion must never latch")
	require.NoError(t, b.stopCtx.Err(), "a held deletion must never stop the backend")
}

// A Create refused because the name's deletion is pending is an ordinary,
// per-volume refusal through the same bracket: no latch.
func TestCreateRefusedForDeletePendingVolumeDoesNotLatch(t *testing.T) {
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())
	logPath := installLoggingXFSQuota(t)

	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	storeAuthorityGate, err := backendidentity.NewStorageAuthorityGate(func(error) { stop() })
	require.NoError(t, err)
	b := &Backend{volumes: mgr, stopCtx: stopCtx, stopCancel: stop, storeAuthorityGate: storeAuthorityGate}
	installMutationTestVerifier(t, b, func(context.Context) error { return nil })
	installTestStorageMutationAdapters(b)

	_, _, err = b.mutationAdapter().createVolume(context.Background(), stage.volumeID.value(), 100)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	require.NotErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	require.NoError(t, b.terminalStorageAuthorityError())
	require.NoError(t, b.stopCtx.Err())
	assert.NoFileExists(t, logPath, "the refusal precedes every quota command")
	assert.NoDirExists(t, stage.volumeID.hostPath(dataPath))
}

// The held twin of TestVolumeRecoveryPendingPreventsHiddenDeleteStageReaperBypass.
// In the removal phase the manager keeps the name in ListForProof, so the reaper
// keeps the REAPING record (and its accounting), never latches, and simply
// retries on the next pass.
func TestVolumeDeleteHeldKeepsReapingRecordWithoutLatch(t *testing.T) {
	t.Parallel()

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440321"
	name := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: "app", Quantity: 1,
		}},
		RetainedVolumeNames: []string{name},
		CreatedAt:           time.Now(),
	}))

	var destroyCalls atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{name}, nil },
		DestroyFn: func(context.Context, string) error {
			destroyCalls.Add(1)
			return heldDeleteErr(name)
		},
	}

	proof := reapingProofForTest(t, b.retentionStore, leaseUUID)
	for pass := range 2 {
		assert.False(t, b.destroyReapingVolumes(t.Context(), b.newManagedVolumeIndex(), proof),
			"pass %d: a held deletion keeps the record", pass)
		require.NoError(t, b.terminalStorageAuthorityError(), "pass %d: a held deletion must never latch", pass)
		record, err := rs.Get(leaseUUID)
		require.NoError(t, err)
		require.NotNil(t, record, "pass %d: the record still accounts for the bytes", pass)
		assert.Equal(t, shared.RetentionStatusReaping, record.Status)
	}
	assert.Equal(t, int32(2), destroyCalls.Load(), "every pass retries the destroy")
}

// In the residual phase the volume is durably gone, so the reaper drops the
// record, and the project's footprint is counted in admission instead.
func TestVolumeDeleteResidualSettlesReapingRecordAndCountsInAdmission(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440322"
	name := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: "app", Quantity: 1,
		}},
		RetainedVolumeNames: []string{name},
		CreatedAt:           time.Now(),
	}))
	const footprintMB = 777
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return nil, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return residualDeletes(footprintMB, name) },
	}
	require.NoError(t, b.refreshRetentionAccountingChecked())
	withRecord := b.pool.Stats().RetainedDiskMB

	proof := reapingProofForTest(t, b.retentionStore, leaseUUID)
	assert.True(t, b.destroyReapingVolumes(t.Context(), b.newManagedVolumeIndex(), proof))
	record, err := rs.Get(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, record, "a residual deletion settles the reaping record")
	require.NoError(t, b.refreshRetentionAccountingChecked())
	withoutRecord := b.pool.Stats().RetainedDiskMB
	assert.GreaterOrEqual(t, withoutRecord, int64(footprintMB),
		"the held project's footprint stays counted after its record is gone")
	assert.Less(t, withoutRecord, withRecord, "only the record's own footprint left the pool")
	assert.Equal(t, float64(footprintMB), testutil.ToFloat64(volumeDeleteHeldResidualMB))
}

// The admission pool counts every residual hold's footprint, and a transition
// re-publishes the term without re-reading the retention store.
func TestHeldResidualFootprintCountsInAdmission(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	attachRetentionStore(t, b)
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	var snapshot atomic.Value
	snapshot.Store(volumeDeleteHoldSnapshot{})
	b.volumes = &mockVolumeManager{
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return snapshot.Load().(volumeDeleteHoldSnapshot) },
	}
	require.NoError(t, b.refreshRetentionAccountingChecked())
	base := b.pool.Stats().RetainedDiskMB

	snapshot.Store(residualDeletes(300, name))
	require.NoError(t, b.afterVolumeDestroy(nil))
	assert.Equal(t, base+300, b.pool.Stats().RetainedDiskMB, "a residual hold is counted before its caller settles")
	assert.Equal(t, float64(300), testutil.ToFloat64(volumeDeleteHeldResidualMB))

	snapshot.Store(pendingDeletes(name))
	require.ErrorIs(t, b.afterVolumeDestroy(heldDeleteErr(name)), ErrVolumeDeleteHeld,
		"the destroy's own answer passes through")
	assert.Equal(t, base, b.pool.Stats().RetainedDiskMB, "a removal-phase hold is its caller's to account")
}

// Re-provisioning a lease whose own volume is still being deleted is refused
// before any Step, with a curated reason authored at the source.
func TestProvisionRefusesDeletePendingVolumeWithCuratedReason(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot {
		return pendingDeletes(canonicalVolumeName(leaseUUID, "web", 1))
	}}
	req := backend.ProvisionRequest{LeaseUUID: leaseUUID, Tenant: "tenant-a",
		Items: []backend.LeaseItem{{SKU: "docker-small", ServiceName: "web", Quantity: 2}}}

	err := b.doProvisionPhysical(&storageMutations{}, t.Context(), req, nil, nil, b.logger)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	callback, reason := operationFailureDetails(err)
	assert.Equal(t, backend.ReasonVolumeDeletePending, reason)
	assert.Equal(t, backend.MsgVolumeDeletePending, callback)

	b.volumes = &mockVolumeManager{}
	assert.Empty(t, b.leaseVolumesPendingDeletion(leaseUUID, req.Items))
}

// Startup quota reconciliation skips a wanted volume whose deletion is held,
// counting it as its own outcome rather than "applied" (finding G6).
func TestReconcileVolumeQuotasCountsDeletePending(t *testing.T) {
	const lease = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	b.cfg.SKUProfiles = map[string]SKUProfile{"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 100}}
	items := []backend.LeaseItem{{SKU: "stateful", Quantity: 2, ServiceName: "web"}}
	b.provisions[lease] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: lease, Items: items,
		ResourceProfiles: []shared.SKUResourceSnapshot{{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100}},
	}}
	held := canonicalVolumeName(lease, "web", 0)
	live := canonicalVolumeName(lease, "web", 1)
	var ensured []string
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{held, live}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(held) },
		EnsureQuotaFn: func(_ context.Context, id string, _ int64) error {
			ensured = append(ensured, id)
			return nil
		},
	}
	counter := func(outcome string) float64 {
		return testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues(outcome))
	}
	pendingBefore, appliedBefore := counter(quotaBackfillDeletePending), counter("applied")
	require.NoError(t, b.reconcileVolumeQuotas(context.Background()))
	assert.Equal(t, []string{live}, ensured, "a held deletion's limits belong to its delete authority")
	assert.Equal(t, pendingBefore+1, counter(quotaBackfillDeletePending))
	assert.Equal(t, appliedBefore+1, counter("applied"))
}

// Start's quota reconciliation against the real XFS manager: a recovered held
// deletion whose final directory already lost its marker, still named by a
// closing lease's projection, is skipped rather than failing startup
// (finding G6).
func TestStartQuotaReconcileSkipsAHeldMarkerlessXFSVolume(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	require.NoError(t, os.Mkdir(stage.volumeID.hostPath(dataPath), 0o700)) // marker-less
	mgr := newXfsManagerForTest(dataPath)
	require.NoError(t, mgr.loadProjectIDs())
	b.volumes = mgr
	installTestStorageMutationAdapters(b)
	b.cfg.VolumeDataPath = dataPath
	leaseUUID := managedVolumeLeaseUUID(stage.volumeID)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	b.provisions[leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: leaseUUID, Items: items, ResourceProfiles: testResourceProfiles(t, items),
		Status: backend.ProvisionStatusDeprovisioning,
	}}
	logPath := installLoggingXFSQuota(t)
	pending := volumeQuotaBackfillTotal.WithLabelValues(quotaBackfillDeletePending)
	before := testutil.ToFloat64(pending)

	require.NoError(t, b.reconcileVolumeQuotas(t.Context()), "startup quota reconciliation succeeds")
	assert.Equal(t, before+1, testutil.ToFloat64(pending))
	assert.NoFileExists(t, logPath, "no limit is touched for a held deletion")
	assert.True(t, mgr.VolumeDeleteHolds().removalHeld(stage.volumeID.value()))
}

// The held twin of TestVolumeRecoveryPendingPreservesCloseFinalizersAndRejectsLiveRetry.
// A held deletion keeps the close pending and observable (503 lifecycle_pending)
// without latching; once the hold completes, the next attempt completes the close.
func TestVolumeDeleteHeldKeepsClosePendingThenCompletes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var held atomic.Bool
	held.Store(true)
	var destroyCalls atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if held.Load() {
				return []string{name}, nil
			}
			return nil, nil
		},
		DestroyFn: func(context.Context, string) error {
			destroyCalls.Add(1)
			if held.Load() {
				return heldDeleteErr(name)
			}
			return nil
		},
	}

	err := b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID)
	require.Error(t, err)
	require.True(t, shared.IsLifecyclePending(err), "a held close answers 503 lifecycle_pending: %v", err)
	require.NoError(t, b.terminalStorageAuthorityError(), "a held deletion must never latch")
	_, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr, "authoritative reads stay available")
	require.True(t, found, "the close intent stays pending")
	b.provisionsMu.RLock()
	projected := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projected)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
	assert.Equal(t, backend.ReasonCleanupFailed, projected.Reason)

	// The hold executor finishes the deletion; the name leaves the listing and
	// the next attempt classifies the close as destroyed.
	held.Store(false)
	require.NoError(t, b.Deprovision(t.Context(), closeDeprovisionLeaseUUID))
	_, found, readErr = stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	assert.False(t, found, "the close completes once the deletion has")
	assert.Equal(t, int32(1), destroyCalls.Load(), "only the first attempt needed a destroy")
	closeCloseRecoveryBackend(t, b, stores)
}

// A retaining close never retains a name whose deletion is pending (finding
// G7): it re-drives the deletion and stays pending until that completes.
func TestDeprovisionRetainArmNeverRetainsDeletePendingVolume(t *testing.T) {
	server, _ := retainCloseServer(t)
	rs, err := newBoundRetentionStoreForTest(t, shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName(durableCallbackTestLeaseUUID, "web", 0)
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: durableCallbackTestLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   testOperationCallbackURL(server.URL + "/callbacks/provision"),
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "redis:7"}}},
		}},
	})
	withMicroSKU(b, 512)
	bindBackendToRetentionFixtureStore(t, b, rs)
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	b.cfg.RetainOnClose = true
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b, server.Client())
	startRetainCloseReplay(t, b)
	require.NoError(t, b.pool.TryAllocate(durableCallbackTestLeaseUUID+"-web-0", "docker-micro", "tenant-a"))

	// A stateful volume (it would be retained) whose deletion is pending.
	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{canonical0: {"data"}})
	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir:          volRoot,
		ListFn:              func() ([]string, error) { return []string{canonical0}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(canonical0) },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return heldDeleteErr(id)
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	err = b.Deprovision(context.Background(), durableCallbackTestLeaseUUID)
	require.Error(t, err, "the close stays pending while the deletion is held")
	assert.True(t, shared.IsLifecyclePending(err), "%v", err)
	assert.Empty(t, renamed, "a delete-pending volume must never be retained")
	assert.Equal(t, []string{canonical0}, destroyed, "the pending deletion is re-driven instead")
	rec, err := rs.Get(durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, rec, "no retention record may name a volume whose deletion is pending")
	require.NoError(t, b.terminalStorageAuthorityError())
	assert.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "the close keeps its reservation while pending")
}

// A deletion that finishes between the listing and the per-name check leaves a
// positively absent name, which is skipped: neither retained nor an error.
func TestDeprovisionRetainArmSkipsAVolumeWhoseDeletionFinishedAfterTheListing(t *testing.T) {
	server, callbackDone := retainCloseServer(t)
	rs, err := newBoundRetentionStoreForTest(t, shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName(durableCallbackTestLeaseUUID, "web", 0)
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: durableCallbackTestLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   testOperationCallbackURL(server.URL + "/callbacks/provision"),
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "redis:7"}}},
		}},
	})
	withMicroSKU(b, 512)
	bindBackendToRetentionFixtureStore(t, b, rs)
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	b.cfg.RetainOnClose = true
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b, server.Client())
	startRetainCloseReplay(t, b)
	require.NoError(t, b.pool.TryAllocate(durableCallbackTestLeaseUUID+"-web-0", "docker-micro", "tenant-a"))

	// The first listing still names the volume; by the per-name check its
	// deletion has finished, and every later listing agrees.
	var lists atomic.Int32
	var destroyed, renamed []string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if lists.Add(1) == 1 {
				return []string{canonical0}, nil
			}
			return nil, nil
		},
		PrecheckDestroyFn: func(name managedVolumeName) (destroyPrecheckVerdict, error) {
			if name.value() == canonical0 {
				return destroyPrecheckGone, nil
			}
			return destroyPrecheckNeedsLock, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, _ string) error {
			renamed = append(renamed, old)
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), durableCallbackTestLeaseUUID))
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}
	assert.Empty(t, renamed, "an absent name must never be retained")
	assert.Empty(t, destroyed, "an absent name needs no destroy")
	rec, err := rs.Get(durableCallbackTestLeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, rec, "no retention record may name a volume that is gone")
}

// The production destroy entry point publishes the residual term before its
// answer reaches the caller: by the reaper's confirmation read, which follows
// the destroy and precedes dropping the record, the footprint is counted.
func TestAfterVolumeDestroyPublishesBeforeTheCallerSettles(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440323"
	name := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: "app", Quantity: 1,
		}},
		RetainedVolumeNames: []string{name},
		CreatedAt:           time.Now(),
	}))
	var residual atomic.Bool
	var lists atomic.Int32
	var atConfirm atomic.Int64
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if lists.Add(1) == 1 {
				return []string{name}, nil
			}
			atConfirm.Store(b.pool.Stats().RetainedDiskMB)
			return nil, nil
		},
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot {
			if residual.Load() {
				return residualDeletes(64, name)
			}
			return volumeDeleteHoldSnapshot{}
		},
		DestroyFn: func(context.Context, string) error {
			residual.Store(true) // the manager just moved the hold to residual
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	require.NoError(t, b.refreshRetentionAccountingChecked())
	base := b.pool.Stats().RetainedDiskMB

	// The sweep's other stages may report unrelated fixture gaps; only the
	// reaping finalizer, which runs the production destroy entry point, matters.
	_ = b.runRetentionSweep(t.Context())
	record, err := rs.Get(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, record, "the residual deletion settled the reaping record")
	assert.Equal(t, base+64, atConfirm.Load(),
		"by the time the destroy answered nil, the residual footprint was counted")
	require.NoError(t, b.terminalStorageAuthorityError())
}

// The reaper's production destroy entry point answers a held retained volume
// from the pre-lock check: it never waits behind the executor's slice on the
// lease's namespace, and the REAPING record stays for the next pass.
func TestReaperAnswersAHeldVolumeWithoutTheLock(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440324"
	name := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	b := newBackendForTest(&mockDockerClient{}, nil)
	rs := attachRetentionStore(t, b)
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            "tenant-a",
		Status:            shared.RetentionStatusReaping,
		Items: []backend.LeaseItem{{
			SKU: "docker-small", ServiceName: "app", Quantity: 1,
		}},
		RetainedVolumeNames: []string{name},
		CreatedAt:           time.Now(),
	}))
	var destroys atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{name}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(name) },
		PrecheckDestroyFn: func(managedVolumeName) (destroyPrecheckVerdict, error) {
			return destroyPrecheckHeld, heldDeleteErr(name)
		},
		DestroyFn: func(context.Context, string) error {
			destroys.Add(1)
			return nil
		},
	}
	installTestStorageMutationAdapters(b)

	parsed, err := parseManagedVolumeName(name)
	require.NoError(t, err)
	holding := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = b.volumeAccess.mutateNamespace(context.Background(), []managedVolumeName{parsed},
			func(context.Context) error {
				close(holding)
				<-release
				return nil
			})
	})
	<-holding
	defer func() { close(release); wg.Wait() }()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := time.Now()
	// The sweep's other stages may report unrelated fixture gaps; only the
	// reaping finalizer, which runs the production destroy entry point, matters.
	_ = b.runRetentionSweep(ctx)
	assert.Less(t, time.Since(started), 5*time.Second, "the reaper must not wait behind the executor's slice")
	assert.Zero(t, destroys.Load(), "a held deletion is answered without Destroy")
	record, err := rs.Get(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, record, "the record still accounts for the bytes")
	assert.Equal(t, shared.RetentionStatusReaping, record.Status)
	require.NoError(t, b.terminalStorageAuthorityError())
}
