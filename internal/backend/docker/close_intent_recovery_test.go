package docker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

const (
	closeRecoveryLeaseUUID    = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	closeRecoveryProviderUUID = "3d65ee7e-e1ec-49a7-96e4-98a8de99b609"
)

type closeRecoveryStores struct {
	callbacks *shared.CallbackStore
	releases  *shared.ReleaseStore
}

func openCloseRecoveryBackend(
	t *testing.T,
	dir string,
	mock *mockDockerClient,
	volumes volumeManager,
) (*Backend, closeRecoveryStores) {
	t.Helper()
	const daemonID = "close-recovery-daemon"
	callbackPath := filepath.Join(dir, "callbacks.db")
	if mock.ListManagedContainersFn == nil {
		// Close admission now inventories exact same-lease rollback remnants
		// before it publishes an immutable cleanup intent. Most recovery tests
		// intentionally exercise an empty Docker substrate; make that expected
		// read explicit while preserving per-test overrides for remnant cases.
		mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		}
	}
	mock.DaemonInfoFn = func(context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: daemonID}, nil
	}
	b := newBackendForTest(mock, nil)
	b.cfg.CallbackDBPath = callbackPath
	b.cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	// These tests exercise journal recovery, not mount discovery. A zero-disk
	// profile is the production-supported stateless configuration and avoids
	// manufacturing a host mount merely to verify the independent storage ID.
	for name, profile := range b.cfg.SKUProfiles {
		profile.DiskMB = 0
		b.cfg.SKUProfiles[name] = profile
	}
	if volumes != nil {
		b.volumes = volumes
	}
	storageID, err := initializeTestMarkerPair(
		callbackPath+".storage-identity.json",
		callbackPath+".storage-identity-anchor.json",
		b.Name(),
		daemonID,
	)
	require.NoError(t, err)
	b.storageIdentity = storageID

	callbackStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath})
	require.NoError(t, err)
	b.callbackStore = callbackStore
	b.operationIntents = callbackStore
	b.releaseStore = releaseStore
	return b, closeRecoveryStores{callbacks: callbackStore, releases: releaseStore}
}

func beginCloseRecoveryIntent(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	cleanupOnly bool,
	callbackURL string,
) shared.CloseIntentClaim {
	t.Helper()
	items, payload, release := seedCloseRecoveryRelease(t, stores)
	version, digest, err := closeReleaseFence(release)
	require.NoError(t, err)
	lifecycleCallbackURL := ""
	if callbackURL != "" {
		lifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(callbackURL, "")
		require.NoError(t, err)
	}
	resourceProfiles, err := b.resolveResourceProfiles(items)
	require.NoError(t, err)
	spec := shared.CloseIntentSpec{
		LeaseUUID:             closeRecoveryLeaseUUID,
		Backend:               b.Name(),
		BackendStorageID:      b.storageIdentity,
		Tenant:                "tenant-a",
		ProviderUUID:          closeRecoveryProviderUUID,
		Items:                 items,
		ResourceProfiles:      resourceProfiles,
		Manifest:              payload,
		ActiveReleaseVersion:  version,
		ActiveReleaseDigest:   digest,
		CleanupOnly:           cleanupOnly,
		CallbackURL:           callbackURL,
		LifecycleCallbackURL:  lifecycleCallbackURL,
		RetainOnClose:         false,
		LegacyRollbackTargets: nil,
	}
	if cleanupOnly {
		spec.Tenant = ""
		spec.ProviderUUID = ""
	}
	admission, err := stores.callbacks.BeginCloseIntent(spec)
	require.NoError(t, err)
	return admission.Claim
}

func seedCloseRecoveryRelease(
	t *testing.T,
	stores closeRecoveryStores,
) ([]backend.LeaseItem, []byte, *shared.Release) {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	resourceProfiles := testResourceProfiles(t, items)
	require.NoError(t, stores.releases.Append(closeRecoveryLeaseUUID, shared.Release{
		Manifest:         payload,
		Image:            "stack",
		Items:            items,
		ResourceProfiles: resourceProfiles,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
	release, err := stores.releases.LatestActive(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, release)
	return items, payload, release
}

func seedCloseRecoveryProjection(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	callbackURL string,
) {
	t.Helper()
	items, _, _ := seedCloseRecoveryRelease(t, stores)
	lifecycleCallbackURL := ""
	var err error
	if callbackURL != "" {
		lifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(callbackURL, "")
		require.NoError(t, err)
	}
	b.provisionsMu.Lock()
	b.provisions[closeRecoveryLeaseUUID] = &provision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            closeRecoveryLeaseUUID,
			Tenant:               "tenant-a",
			ProviderUUID:         closeRecoveryProviderUUID,
			SKU:                  items[0].SKU,
			Status:               backend.ProvisionStatusFailed,
			Quantity:             1,
			CreatedAt:            time.Now(),
			FailCount:            0,
			LastError:            "",
			Reason:               "",
			Message:              "",
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleCallbackURL,
			Items:                items,
			ResourceProfiles:     testResourceProfiles(t, items),
			ContainerIDs:         nil,
			StackManifest:        nil,
			ServiceContainers:    nil,
		},
		ResourceProfiles:      testResourceProfiles(t, items),
		VolumeCleanupAttempts: 0,
	}
	b.provisionsMu.Unlock()
}

func closeCloseRecoveryBackend(t *testing.T, b *Backend, stores closeRecoveryStores) {
	t.Helper()
	b.stopCancel()
	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
}

func TestRecoverState_CloseIntentConvergesZeroSurvivorRelease(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	beginCloseRecoveryIntent(t, b, stores, false, "")

	// Without the durable close authority, an exact release with no survivors
	// is intentionally fatal. With it, recovery rebuilds a conservative retry
	// owner, completes teardown, and retires both durable records.
	require.NoError(t, b.recoverState(context.Background()))
	_, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.False(t, found)
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)
	b.provisionsMu.RLock()
	_, found = b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, found)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_CleanupOnlyClosePublishesNoProjection(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	beginCloseRecoveryIntent(t, b, stores, true, "")

	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)
	_, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.False(t, found)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_CleanupOnlyFailureKeepsJournalAndPoolAccounting(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
	volumeState := newVolumeSet(volumeName)
	volumeState.destroyFn = func(string) error { return errors.New("injected volume failure") }
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())
	beginCloseRecoveryIntent(t, b, stores, true, "")

	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)
	claim, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, claim.CleanupAttempts())
	require.Equal(t, 1, b.pool.Stats().AllocationCount,
		"unprojected substrate remains conservatively reserved while close retries")
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, releases)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_CloseCleanupAttemptsSurviveBackendRestart(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
	volumeState := newVolumeSet(volumeName)
	volumeState.destroyFn = func(string) error { return errors.New("injected volume failure") }
	newMock := func() *mockDockerClient {
		return &mockDockerClient{
			ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
	}

	b, stores := openCloseRecoveryBackend(t, dir, newMock(), volumeState.manager())
	beginCloseRecoveryIntent(t, b, stores, false, "")
	require.NoError(t, b.recoverState(context.Background()))
	claim, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, claim.CleanupAttempts())
	closeCloseRecoveryBackend(t, b, stores)

	// Reconstruct the backend and both bbolt stores over the same files. The
	// volatile provision is gone, but the close claim recreates it with attempt
	// one and the next failed cleanup advances durably to attempt two.
	b, stores = openCloseRecoveryBackend(t, dir, newMock(), volumeState.manager())
	require.NoError(t, b.recoverState(context.Background()))
	claim, found, err = stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, claim.CleanupAttempts())
	b.provisionsMu.RLock()
	projection := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	require.Equal(t, 2, projection.VolumeCleanupAttempts)
	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_CloseIntentConvergesAfterReleaseDeleteBeforeOutbox(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
	volumeState := newVolumeSet(volumeName)
	newMock := func() *mockDockerClient {
		return &mockDockerClient{
			ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
	}

	var callbackStore *shared.CallbackStore
	volumeState.destroyFn = func(string) error {
		// Model a process dying after substrate teardown but before the close
		// transaction can remove the journal and enqueue its lifecycle event.
		return callbackStore.Close()
	}
	b, stores := openCloseRecoveryBackend(t, dir, newMock(), volumeState.manager())
	callbackStore = stores.callbacks
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	seedCloseRecoveryProjection(t, b, stores, operationURL)

	// Drive the production finalizer from before admission. It publishes the
	// close journal, tears down substrate, retires the release, then fails to
	// atomically replace the journal with its lifecycle outbox entry because the
	// callback DB became unavailable in the volume-destroy crash hook.
	require.ErrorContains(t, b.doDeprovision(context.Background(), closeRecoveryLeaseUUID),
		"resolve durable close intent")
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases, "release retirement must precede the failed close resolution")
	closeCloseRecoveryBackend(t, b, stores)

	// The reconstructed backend sees no release and no containers. The close
	// claim alone remains sufficient to recreate cleanup authority and atomically
	// replace itself with the terminal lifecycle outbox entry.
	volumeState.destroyFn = nil
	b, stores = openCloseRecoveryBackend(t, dir, newMock(), volumeState.manager())
	_, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, b.recoverState(context.Background()))
	_, found, err = stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.False(t, found)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, closeRecoveryLeaseUUID, pending[0].LeaseUUID)
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
	require.Equal(t, shared.CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_SerializesInventorySnapshotWithConcurrentClose(t *testing.T) {
	dir := t.TempDir()
	volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
	volumeState := newVolumeSet(volumeName)
	stale := ContainerInfo{
		ContainerID:   "stale-container",
		LeaseUUID:     closeRecoveryLeaseUUID,
		Tenant:        "tenant-a",
		ProviderUUID:  closeRecoveryProviderUUID,
		SKU:           "docker-small",
		ServiceName:   "app",
		InstanceIndex: 0,
		Image:         "docker.io/library/nginx:1.27",
		Status:        "running",
		CreatedAt:     time.Now(),
	}
	var (
		listCalls        atomic.Int32
		destroySignal    sync.Once
		inventoryRead    = make(chan struct{})
		continueRecovery = make(chan struct{})
		destroyStarted   = make(chan struct{})
		continueDestroy  = make(chan struct{})
	)
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if listCalls.Add(1) != 1 {
				return nil, nil
			}
			close(inventoryRead)
			<-continueRecovery
			return []ContainerInfo{stale}, nil
		},
	}
	volumeState.destroyFn = func(string) error {
		destroySignal.Do(func() { close(destroyStarted) })
		<-continueDestroy
		return nil
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())
	seedCloseRecoveryProjection(t, b, stores, "")

	recoverDone := make(chan error, 1)
	go func() { recoverDone <- b.recoverState(context.Background()) }()
	<-inventoryRead
	closeDone := make(chan error, 1)
	go func() { closeDone <- b.Deprovision(context.Background(), closeRecoveryLeaseUUID) }()

	// While recovery owns its authority snapshot, the live close cannot even
	// reach substrate teardown. This is the former stale-inventory window: if
	// Deprovision did not share recoverMu, it could resolve its journal here and
	// recovery would publish the stale row after that terminal decision.
	select {
	case <-destroyStarted:
		t.Fatal("concurrent close mutated substrate during the recovery authority snapshot")
	case <-time.After(50 * time.Millisecond):
	}
	close(continueRecovery)
	require.NoError(t, <-recoverDone)
	select {
	case <-destroyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not resume after recovery released its authority snapshot")
	}
	close(continueDestroy)
	require.NoError(t, <-closeDone)

	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected, "the serialized close must remove recovery's earlier projection")
	require.Equal(t, 0, b.pool.Stats().AllocationCount)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_AdmittedSlowCloseDoesNotBlockSnapshotOrLeakRepublishedAllocation(t *testing.T) {
	for _, cleanupOnly := range []bool{false, true} {
		name := "full close"
		if cleanupOnly {
			name = "cleanup-only close"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			volumeName := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
			volumeState := newVolumeSet(volumeName)
			stale := ContainerInfo{
				ContainerID:   "pre-close-container",
				LeaseUUID:     closeRecoveryLeaseUUID,
				Tenant:        "tenant-a",
				ProviderUUID:  closeRecoveryProviderUUID,
				SKU:           "docker-small",
				ServiceName:   "app",
				InstanceIndex: 0,
				Image:         "docker.io/library/nginx:1.27",
				Status:        "running",
				CreatedAt:     time.Now(),
			}
			var (
				listCalls        atomic.Int32
				destroyOnce      sync.Once
				destroyStarted   = make(chan struct{})
				continueDestroy  = make(chan struct{})
				inventoryRead    = make(chan struct{})
				continueRecovery = make(chan struct{})
			)
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					// Close admission takes the first read to freeze any exact
					// rollback remnants. This fixture has none. The recovery
					// snapshot is the second read and deliberately blocks.
					if listCalls.Add(1) == 1 {
						return nil, nil
					}
					close(inventoryRead)
					<-continueRecovery
					return []ContainerInfo{stale}, nil
				},
			}
			volumeState.destroyFn = func(string) error {
				destroyOnce.Do(func() { close(destroyStarted) })
				<-continueDestroy
				return nil
			}
			b, stores := openCloseRecoveryBackend(t, dir, mock, volumeState.manager())
			if cleanupOnly {
				seedCloseRecoveryRelease(t, stores)
			} else {
				seedCloseRecoveryProjection(t, b, stores, "")
			}
			require.NoError(t, b.pool.TryAllocate(
				closeRecoveryLeaseUUID+"-app-0",
				"docker-small",
				"tenant-a",
			))

			closeDone := make(chan error, 1)
			go func() { closeDone <- b.Deprovision(context.Background(), closeRecoveryLeaseUUID) }()
			select {
			case <-destroyStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("close did not reach slow physical cleanup")
			}
			_, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
			require.NoError(t, err)
			require.True(t, found, "physical cleanup must start only after journal admission")

			recoverDone := make(chan error, 1)
			go func() { recoverDone <- b.recoverState(context.Background()) }()
			select {
			case <-inventoryRead:
				// The close holds no global recovery lock while substrate cleanup is slow.
			case <-time.After(2 * time.Second):
				t.Fatal("admitted slow close blocked the recovery inventory snapshot")
			}

			// Let physical cleanup finish while recovery still owns the write snapshot.
			// The close must wait at terminal settlement; recovery then republishes its
			// conservative allocation, and the terminal R section must release it again.
			close(continueDestroy)
			select {
			case err := <-closeDone:
				t.Fatalf("close settled across an in-progress recovery snapshot: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(continueRecovery)
			require.NoError(t, <-closeDone)
			require.NoError(t, <-recoverDone)

			_, found, err = stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
			require.NoError(t, err)
			require.False(t, found)
			releases, err := stores.releases.List(closeRecoveryLeaseUUID)
			require.NoError(t, err)
			require.Empty(t, releases)
			b.provisionsMu.RLock()
			_, projected := b.provisions[closeRecoveryLeaseUUID]
			b.provisionsMu.RUnlock()
			require.False(t, projected)
			require.Zero(t, b.pool.Stats().AllocationCount,
				"terminal settlement must release the allocation republished by recovery")

			closeCloseRecoveryBackend(t, b, stores)
		})
	}
}

func TestResumeRecoveredClose_SkipsSettledClaimAfterReplacementGenerationWins(t *testing.T) {
	dir := t.TempDir()
	var composeDownCalls atomic.Int32
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		composeDownCalls.Add(1)
		return nil
	}}
	oldClaim := beginCloseRecoveryIntent(t, b, stores, true, "")
	require.NoError(t, b.purgeCloseReleaseHistory(oldClaim))
	require.NoError(t, b.resolveCloseIntent(
		oldClaim,
		backend.CallbackStatusDeprovisioned,
		"",
		false,
		nil,
		nil,
	))

	// Model a new provision generation admitted after the old close settled but
	// before a recovery worker resumed the claim it read from its earlier snapshot.
	seedCloseRecoveryProjection(t, b, stores, "")
	require.NoError(t, b.resumeRecoveredClose(context.Background(), oldClaim))
	require.Zero(t, composeDownCalls.Load(),
		"a stale recovered capability must not mutate the replacement generation")
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.True(t, projected)
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 1)

	closeCloseRecoveryBackend(t, b, stores)
}
