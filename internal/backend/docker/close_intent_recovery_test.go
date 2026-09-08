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
	callbacks   *shared.CallbackStore
	releases    *shared.ReleaseStore
	retentions  *shared.RetentionStore
	operations  *shared.OperationSettlement
	restore     *shared.RestoreSettlement
	maintenance *shared.MaintenanceSettlement
	close       *shared.CloseSettlement
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
	callbackStore, releaseStore, retentionStore := openBoundCloseStoresForBackendTest(
		t, b, dir, daemonID,
	)
	operationSettlement, err := shared.NewOperationSettlement(callbackStore, releaseStore)
	require.NoError(t, err)
	restoreSettlement, err := shared.NewRestoreSettlement(operationSettlement, retentionStore)
	require.NoError(t, err)
	maintenanceSettlement, err := shared.NewMaintenanceSettlement(callbackStore, releaseStore)
	require.NoError(t, err)
	closeSettlement, err := shared.NewCloseSettlement(callbackStore, releaseStore, retentionStore)
	require.NoError(t, err)
	b.callbackStore = callbackStore
	b.operationSettlement = operationSettlement
	b.releaseStore = releaseStore
	b.retentionStore = retentionStore
	b.operationSettlement = operationSettlement
	b.restoreSettlement = restoreSettlement
	b.maintenanceSettlement = maintenanceSettlement
	b.closeSettlement = closeSettlement
	ops, err := storageMutationOperationsForTest(b)
	require.NoError(t, err)
	// Construct the operation and maintenance executors before any release
	// fixture is seeded. Otherwise commitOperationSuccessForTest has to bind its
	// seed-only executor permanently, leaving later recovery unable to classify
	// the real Docker substrate—a wiring state production cannot create.
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, operationSettlement, maintenanceSettlement,
	))
	require.NoError(t, shared.BindCloseSubstrateExecutor(
		closeSettlement,
		b.authorizeStorageMutation,
		b.completeStorageMutation,
		buildCloseSubstrate(b, ops),
		runCloseSubstrate,
		b.classifyClosePhysical,
	))
	rebuildCallbackSender(b, testCallbackClient)
	require.NotNil(t, b.callbackPublisher)
	retentionFixtureAuthorities.Store(retentionStore, &retentionFixtureAuthority{
		backend: b, callbacks: callbackStore, releases: releaseStore,
		operations: operationSettlement, restore: restoreSettlement, close: closeSettlement,
	})
	registerExistingOperationTestAuthority(
		b, callbackStore, releaseStore, retentionStore,
		operationSettlement, restoreSettlement, closeSettlement,
	)
	bindBackendRecoveryCoordinatorForTest(t, b)
	return b, closeRecoveryStores{
		callbacks: callbackStore, releases: releaseStore,
		retentions: retentionStore, operations: operationSettlement, restore: restoreSettlement,
		maintenance: maintenanceSettlement, close: closeSettlement,
	}
}

func completeDestroyedCloseForTest(
	t *testing.T,
	b *Backend,
	settlement *shared.CloseSettlement,
	claim shared.CloseIntentClaim,
) {
	t.Helper()
	execution, err := settlement.StartCloseExecution(claim)
	require.NoError(t, err)
	outcome := settlement.ExecuteClose(context.Background(), execution)
	destroyed, ok := outcome.(shared.CloseExecutionDestroyed)
	if pending, pendingOutcome := outcome.(shared.CloseExecutionPending); pendingOutcome {
		require.FailNowf(t, "close outcome remained pending", "%s", pending.Error())
	}
	require.True(t, ok, "close outcome = %T, want destroyed", outcome)
	_, err = settlement.CompleteClose(destroyed)
	require.NoError(t, err)
}

func beginCloseRecoveryIntent(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	cleanupOnly bool,
	callbackURL string,
) shared.CloseIntentClaim {
	t.Helper()
	_, _, _ = seedCloseRecoveryReleaseWithCallback(t, stores, callbackURL)
	var admission shared.CloseIntentAdmission
	var err error
	if cleanupOnly {
		request, requestErr := stores.close.NewCleanupCloseRequest(closeRecoveryLeaseUUID)
		require.NoError(t, requestErr)
		admission, err = stores.close.BeginCleanupClose(request)
	} else {
		request, requestErr := stores.close.NewCloseRequest(closeRecoveryLeaseUUID, false)
		require.NoError(t, requestErr)
		admission, err = stores.close.BeginClose(request)
	}
	require.NoError(t, err)
	return admission.Claim()
}

func seedCloseRecoveryRelease(
	t *testing.T,
	stores closeRecoveryStores,
) ([]backend.LeaseItem, []byte, *shared.Release) {
	t.Helper()
	return seedCloseRecoveryReleaseWithCallback(t, stores, "")
}

func seedCloseRecoveryReleaseWithCallback(
	t *testing.T,
	stores closeRecoveryStores,
	callbackURL string,
) ([]backend.LeaseItem, []byte, *shared.Release) {
	t.Helper()
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	return seedCloseRecoveryReleaseWithProfiles(
		t, stores, callbackURL, items, testResourceProfiles(t, items),
	)
}

func seedCloseRecoveryReleaseWithProfiles(
	t *testing.T,
	stores closeRecoveryStores,
	callbackURL string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) ([]backend.LeaseItem, []byte, *shared.Release) {
	t.Helper()
	if callbackURL == "" {
		callbackURL = testOperationCallbackURL("https://callbacks.invalid/callbacks/provision")
	}
	payload := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	operationID := mustTestOperationIDFromCallbackURL(t, callbackURL)
	authority, authorityErr := shared.NewReleaseRuntimeAuthority(
		operationID,
		"tenant-a",
		closeRecoveryProviderUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, authorityErr)
	seeded := seedProvisionReleaseForLeaseTest(
		t, stores.callbacks, stores.releases, stores.operations,
		closeRecoveryLeaseUUID, shared.Release{
			Manifest:         payload,
			Image:            "stack",
			OperationID:      operationID,
			Items:            items,
			ResourceProfiles: resourceProfiles,
			RuntimeAuthority: &authority,
			Status:           "active",
			CreatedAt:        time.Now(),
		})
	return items, payload, &seeded
}

func seedCloseRecoveryProjection(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	callbackURL string,
) {
	t.Helper()
	items, _, _ := seedCloseRecoveryReleaseWithCallback(t, stores, callbackURL)
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
			ActiveReleaseVersion: 0,
			ActiveOperationID:    shared.OperationID{},
			Items:                items,
			ResourceProfiles:     testResourceProfiles(t, items),
			ContainerIDs:         nil,
			StackManifest:        nil,
			ServiceContainers:    nil,
		},
	}
	b.provisionsMu.Unlock()
}

func closeCloseRecoveryBackend(t *testing.T, b *Backend, stores closeRecoveryStores) {
	t.Helper()
	b.stopCancel()
	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	require.NoError(t, stores.retentions.Close())
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

func TestRecoverState_ClosedLeaseReceiptRemovesLateContainerWithoutRepublishing(t *testing.T) {
	dir := t.TempDir()
	visible := false
	late := true
	lateContainer := ContainerInfo{
		ContainerID:  "late-after-close",
		LeaseUUID:    closeRecoveryLeaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: closeRecoveryProviderUUID,
		ServiceName:  "app",
		SKU:          "docker-small",
	}
	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !visible {
				return nil, nil
			}
			if !late {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed = append(removed, containerID)
			late = false
			return nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, false, "")
	completeDestroyedCloseForTest(t, b, stores.close, claim)

	// A successful close is a permanent UUID retirement, not merely a finite
	// observation that Docker was empty. Model the Create becoming visible only
	// after this sweep's strict cleanup read: the newer ordinary inventory still
	// excludes it from projection, and the next sweep removes it.
	require.NoError(t, b.recoverState(context.Background()))
	require.Empty(t, removed)
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)

	visible = true
	require.NoError(t, b.recoverState(context.Background()))
	require.Equal(t, []string{lateContainer.ContainerID}, removed)
	b.provisionsMu.RLock()
	_, projected = b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)
	receipts, err := stores.callbacks.LookupClosedLeaseReceipts([]string{closeRecoveryLeaseUUID})
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.Equal(t, closeRecoveryLeaseUUID, receipts[0].LeaseUUID())

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_ClosedLeaseReceiptRejectsDivergentPrincipal(t *testing.T) {
	dir := t.TempDir()
	visible := false
	lateContainer := ContainerInfo{
		ContainerID:  "late-after-close",
		LeaseUUID:    closeRecoveryLeaseUUID,
		Tenant:       "different-tenant",
		ProviderUUID: closeRecoveryProviderUUID,
		ServiceName:  "app",
		SKU:          "docker-small",
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !visible {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(context.Context, string) error {
			t.Fatal("divergent principal must never grant cleanup authority")
			return nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, false, "")
	completeDestroyedCloseForTest(t, b, stores.close, claim)

	visible = true
	require.ErrorContains(t, b.recoverState(context.Background()), "divergent principal identity")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_ClosedLeaseRemovalFailureRetriesWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	visible := false
	removalBlocked := true
	lateContainer := ContainerInfo{
		ContainerID:  "late-remove-failure",
		LeaseUUID:    closeRecoveryLeaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: closeRecoveryProviderUUID,
		ServiceName:  "app",
		SKU:          "docker-small",
	}
	firstMock := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !visible {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(context.Context, string) error {
			if removalBlocked {
				return errors.New("injected late-container removal failure")
			}
			visible = false
			return nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, firstMock, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, false, "")
	completeDestroyedCloseForTest(t, b, stores.close, claim)

	visible = true
	require.NoError(t, b.recoverState(context.Background()))
	require.NoError(t, b.stopCtx.Err(), "ordinary cleanup failure must not stop the backend")
	require.NoError(t, b.terminalStorageAuthorityError())
	require.True(t, b.pool.Stats().AccountingHeld)
	require.Zero(t, b.pool.Stats().AvailableCPU())
	require.Zero(t, b.pool.Stats().AvailableMemoryMB())
	require.Zero(t, b.pool.Stats().AvailableDiskMB())
	require.ErrorIs(t, b.Health(context.Background()), shared.ErrResourceAccountingIncomplete)
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected, "failed cleanup must never republish a closed UUID")
	receipts, receiptErr := stores.callbacks.LookupClosedLeaseReceipts([]string{closeRecoveryLeaseUUID})
	require.NoError(t, receiptErr)
	require.Len(t, receipts, 1, "retry authority remains readable in the same process")
	removalBlocked = false
	require.NoError(t, b.recoverState(context.Background()))
	require.False(t, b.pool.Stats().AccountingHeld)
	require.NoError(t, b.Health(context.Background()))
	closeCloseRecoveryBackend(t, b, stores)

	late := true
	var removed []string
	retryMock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !late {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed = append(removed, containerID)
			late = false
			return nil
		},
	}
	b, stores = openCloseRecoveryBackend(t, dir, retryMock, nil)
	receipts, receiptErr = stores.callbacks.LookupClosedLeaseReceipts([]string{closeRecoveryLeaseUUID})
	require.NoError(t, receiptErr)
	require.Len(t, receipts, 1, "failed cleanup must retain permanent retry authority")
	require.NoError(t, b.recoverState(context.Background()))
	require.Equal(t, []string{lateContainer.ContainerID}, removed)
	b.provisionsMu.RLock()
	_, projected = b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)
	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_ClosedLeasePersistentSurvivorRetriesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	visible := false
	lateContainer := ContainerInfo{
		ContainerID:  "late-persistent-survivor",
		LeaseUUID:    closeRecoveryLeaseUUID,
		Tenant:       "tenant-a",
		ProviderUUID: closeRecoveryProviderUUID,
		ServiceName:  "app",
		SKU:          "docker-small",
	}
	removeCalls := 0
	firstMock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !visible {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			require.Equal(t, lateContainer.ContainerID, containerID)
			removeCalls++
			return nil // Docker acknowledged removal, but both postchecks disprove it.
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, firstMock, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, false, "")
	completeDestroyedCloseForTest(t, b, stores.close, claim)

	visible = true
	require.NoError(t, b.recoverState(context.Background()))
	require.NoError(t, b.stopCtx.Err())
	require.True(t, b.pool.Stats().AccountingHeld,
		"two disproved removes retain the footprint without terminating recovery")
	require.Equal(t, 2, removeCalls)
	receipts, receiptErr := stores.callbacks.LookupClosedLeaseReceipts([]string{closeRecoveryLeaseUUID})
	require.NoError(t, receiptErr)
	require.Len(t, receipts, 1, "disproved removal must retain permanent retry authority")
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected, "a persistent survivor must remain excluded from projection")
	closeCloseRecoveryBackend(t, b, stores)

	late := true
	var removed []string
	retryMock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !late {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed = append(removed, containerID)
			late = false
			return nil
		},
	}
	b, stores = openCloseRecoveryBackend(t, dir, retryMock, nil)
	require.NoError(t, b.recoverState(context.Background()))
	require.Equal(t, []string{lateContainer.ContainerID}, removed)
	b.provisionsMu.RLock()
	_, projected = b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)
	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_CleanupOnlyOrphanReceiptUsesLeaseAndStorageAuthority(t *testing.T) {
	dir := t.TempDir()
	late := false
	lateContainer := ContainerInfo{
		ContainerID:  "late-cleanup-only-orphan",
		LeaseUUID:    closeRecoveryLeaseUUID,
		Tenant:       "tenant-observed-only-after-close",
		ProviderUUID: "4d65ee7e-e1ec-49a7-96e4-98a8de99b609",
		BackendName:  DefaultConfig().Name,
		SKU:          "docker-small",
		ServiceName:  "app",
		Status:       "running",
	}
	var removed []string
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !late {
				return nil, nil
			}
			return []ContainerInfo{lateContainer}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			removed = append(removed, containerID)
			late = false
			return nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	claim := beginCloseRecoveryIntent(t, b, stores, true, "")
	require.Empty(t, claim.Tenant())
	require.Empty(t, claim.ProviderUUID())
	completeDestroyedCloseForTest(t, b, stores.close, claim)
	late = true // the survivor appears only after terminal close settlement

	// This is the intentionally weaker sealed variant: no principal witness
	// existed at admission. Cleanup is authorized by the exact retired UUID,
	// reserved managed-container labels, and the attested backend/storage pair.
	require.NoError(t, b.recoverState(context.Background()))
	require.Equal(t, []string{lateContainer.ContainerID}, removed)
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestAcquireCleanupOnlyCloseDoesNotCarryReleasePrincipalAuthority(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	const callbackURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	seedCloseRecoveryReleaseWithCallback(t, stores, callbackURL)

	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		closeRecoveryLeaseUUID,
		false,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, claim.CleanupOnly())
	require.Empty(t, claim.Tenant())
	require.Empty(t, claim.ProviderUUID())
	require.Empty(t, claim.CallbackURL(), "cleanup-only authority never emits a lifecycle callback")

	closeCloseRecoveryBackend(t, b, stores)
}

func TestAcquireCleanupOnlyCloseDoesNotImportSubstratePrincipalAuthority(t *testing.T) {
	dir := t.TempDir()
	witness := ContainerInfo{
		ContainerID:   "orphan-principal-witness",
		Name:          "fred-" + closeRecoveryLeaseUUID + "-app-0",
		LeaseUUID:     closeRecoveryLeaseUUID,
		Tenant:        "tenant-from-substrate",
		ProviderUUID:  closeRecoveryProviderUUID,
		BackendName:   "docker",
		SKU:           "docker-small",
		ServiceName:   "app",
		InstanceIndex: 0,
		Status:        "running",
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{witness}, nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	seedCloseRecoveryRelease(t, stores)

	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		closeRecoveryLeaseUUID,
		false,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, claim.CleanupOnly())
	require.Empty(t, claim.Tenant())
	require.Empty(t, claim.ProviderUUID())

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
	require.Equal(t, 1, claim.ExecutionGeneration().Number())
	require.Equal(t, 1, b.pool.Stats().AllocationCount,
		"unprojected substrate remains conservatively reserved while close retries")
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, releases)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_CloseExecutionGenerationSurvivesBackendRestart(t *testing.T) {
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
	require.Equal(t, 1, claim.ExecutionGeneration().Number())
	closeCloseRecoveryBackend(t, b, stores)

	// Reconstruct the backend and both bbolt stores over the same files. The
	// volatile projection is gone, but the close claim retains its Started
	// generation and recovery advances only after strict inventory proves retry safe.
	b, stores = openCloseRecoveryBackend(t, dir, newMock(), volumeState.manager())
	require.NoError(t, b.recoverState(context.Background()))
	claim, found, err = stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, claim.ExecutionGeneration().Number())
	b.provisionsMu.RLock()
	projection := b.provisions[closeRecoveryLeaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projection)
	closeCloseRecoveryBackend(t, b, stores)
}

func TestRecoverState_AmbiguousDestroyPreservesReleaseAndConvergesAfterRestart(t *testing.T) {
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

	// Losing journal access during a physical Step makes the outcome ambiguous.
	// The close must preserve its exact release fence; it cannot guess that the
	// substrate was destroyed and advance settlement.
	require.Error(t, b.doDeprovisionForTest(t, context.Background(), closeRecoveryLeaseUUID))
	releases, err := stores.releases.List(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, releases, "ambiguous physical work must preserve release authority")
	closeCloseRecoveryBackend(t, b, stores)

	// A reconstructed backend independently classifies, retries, and atomically
	// replaces the close journal with the terminal lifecycle outbox entry.
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
	type recoveryInventoryContextKey struct{}
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
				destroyOnce      sync.Once
				destroyStarted   = make(chan struct{})
				continueDestroy  = make(chan struct{})
				inventoryRead    = make(chan struct{})
				continueRecovery = make(chan struct{})
				recoveryReadOnce sync.Once
			)
			mock := &mockDockerClient{
				ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
					// Identify the recovery inventory by its caller-owned context,
					// not by a fragile read count: close classification and ownership
					// attestation may add their own strict inventory reads.
					if ctx.Value(recoveryInventoryContextKey{}) == nil {
						return nil, nil
					}
					firstRead := false
					recoveryReadOnce.Do(func() {
						firstRead = true
						close(inventoryRead)
						<-continueRecovery
					})
					if firstRead {
						return []ContainerInfo{stale}, nil
					}
					return nil, nil
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
			recoveryCtx := context.WithValue(
				context.Background(), recoveryInventoryContextKey{}, true,
			)
			go func() { recoverDone <- b.recoverState(recoveryCtx) }()
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
