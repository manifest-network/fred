package docker

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	closeDeprovisionLeaseUUID    = "123e4567-e89b-42d3-a456-426614174000"
	closeDeprovisionProviderUUID = "22222222-2222-4222-8222-222222222222"
)

func seedCloseDeprovisionLease(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
) {
	t.Helper()
	seedCloseDeprovisionLeaseWithCallback(
		t, b, stores,
		testOperationCallbackURL("https://callbacks.invalid/callbacks/provision"),
	)
}

func seedCloseDeprovisionLeaseWithCallback(
	t *testing.T,
	b *Backend,
	stores closeRecoveryStores,
	callbackURL string,
) {
	t.Helper()
	if callbackURL == "" {
		callbackURL = testOperationCallbackURL("https://callbacks.invalid/callbacks/provision")
	}
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	payload := validStackManifestJSON(map[string]string{
		"app": "docker.io/library/nginx:1.27",
	})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)
	resourceProfiles := testResourceProfiles(t, items)
	var lifecycleCallbackURL string
	var operationID shared.OperationID
	var runtimeAuthority *shared.ReleaseRuntimeAuthority
	lifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	operationID = mustTestOperationIDFromCallbackURL(t, callbackURL)
	authority, authorityErr := shared.NewReleaseRuntimeAuthority(
		operationID,
		"tenant-a",
		closeDeprovisionProviderUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, authorityErr)
	runtimeAuthority = &authority
	b.provisionsMu.Lock()
	b.provisions[closeDeprovisionLeaseUUID] = &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            closeDeprovisionLeaseUUID,
			Tenant:               "tenant-a",
			ProviderUUID:         closeDeprovisionProviderUUID,
			Items:                items,
			Quantity:             1,
			StackManifest:        stack,
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleCallbackURL,
			ActiveOperationID:    operationID,
			ResourceProfiles:     resourceProfiles,
		},
	}
	b.provisionsMu.Unlock()
	release := shared.Release{
		Manifest:         payload,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: runtimeAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}
	seedProvisionReleaseForLeaseTest(
		t, stores.callbacks, stores.releases, stores.operations,
		closeDeprovisionLeaseUUID, release,
	)
}

func TestDoDeprovision_CommitsCloseIntentBeforeTeardown(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)

	b.compose = &mockComposeExecutor{DownFn: func(
		context.Context,
		string,
		time.Duration,
	) error {
		_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
		require.NoError(t, err)
		require.True(t, found,
			"the durable close barrier must commit before the first substrate mutation")
		return nil
	}}

	require.NoError(t, b.doDeprovisionForTest(t, context.Background(), closeDeprovisionLeaseUUID))
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.False(t, found, "terminal settlement must consume the exact close capability")
	releases, err := stores.releases.List(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestAcquireCloseIntentUsesFencedReleaseTopology(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)

	// Model the actor-drain boundary: replacement substrate and its Release have
	// committed, but the queued terminal actor event has not yet promoted the
	// in-memory source projection. Close must retain the generation named by its
	// release fence, not this stale projection.
	targetItems := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
		CustomDomain: "new.example.test",
	}}
	targetPayload := validStackManifestJSON(map[string]string{
		"app": "docker.io/library/nginx:1.28",
	})
	sourceRelease, err := stores.releases.LatestActive(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, sourceRelease)
	activateMaintenanceReleaseForTest(
		t, stores.maintenance, closeDeprovisionLeaseUUID,
		shared.MaintenanceIntentUpdate, shared.Release{
			Manifest:         targetPayload,
			Image:            "stack",
			OperationID:      sourceRelease.OperationID,
			Items:            targetItems,
			ResourceProfiles: testResourceProfiles(t, targetItems),
			RuntimeAuthority: sourceRelease.RuntimeAuthority,
			Status:           "deploying",
			CreatedAt:        time.Now().Add(time.Second),
		})

	b.provisionsMu.RLock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	require.NotNil(t, projection)
	oldItems := append([]backend.LeaseItem(nil), projection.Items...)
	b.provisionsMu.RUnlock()
	require.NotEqual(t, targetItems, oldItems)

	// Production settles an already-active maintenance target before close
	// admission so the maintenance callback records Success and the close fence
	// is derived from that target generation rather than its superseded source.
	require.NoError(t, b.settleMaintenanceBeforeClose(closeDeprovisionLeaseUUID))
	claim, found, err := b.acquireCloseIntent(
		context.Background(),
		closeDeprovisionLeaseUUID,
		true,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, targetItems, claim.Items())
	require.JSONEq(t, string(targetPayload), string(claim.Manifest()))
	stack, err := manifest.ParsePayload(claim.Manifest())
	require.NoError(t, err)
	require.Equal(t, "docker.io/library/nginx:1.28", stack.Services["app"].Image)

	active, err := stores.releases.LatestActive(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, active.Version, claim.ActiveReleaseVersion())

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_CloseSettlementDoesNotPerformCallbackIOInline(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	const operationURL = "https://fred.example/callbacks/provision?operation_id=9a72fbc1-38c8-4f31-87f7-f689979b9324"
	seedCloseDeprovisionLeaseWithCallback(t, b, stores, operationURL)
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(operationURL, "")
	require.NoError(t, err)
	require.NotEmpty(t, lifecycleURL)
	var requests atomic.Int32
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store: stores.callbacks,
		StorageAttestor: callbackStorageAttestorForTest(
			t, stores.callbacks, b.stopCtx, allowTestCallbackDelivery,
		),
		Secret: "test-secret-that-is-at-least-32-bytes",
		Logger: b.logger,

		HTTPClient: &http.Client{Transport: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return nil, errors.New("callback transport must not run in close actor")
		})},
	})

	require.NoError(t, b.doDeprovisionForTest(t, context.Background(), closeDeprovisionLeaseUUID))
	require.Zero(t, requests.Load(), "durable settlement must only wake the tracked replay worker")
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)

	closeCloseRecoveryBackend(t, b, stores)
}

func TestDoDeprovision_CleanupOnlyClaimCleansUnprojectedSubstrate(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		destroyErr   error
		wantAttempts int
		wantPending  bool
	}{
		{name: "success retires the journal"},
		{
			name:         "uncertain volume cleanup remains durably retryable",
			destroyErr:   errors.New("injected volume EIO"),
			wantAttempts: 1,
			wantPending:  true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			var composeDown bool
			var destroyed []string
			b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{
				DestroyFn: func(_ context.Context, name string) error {
					destroyed = append(destroyed, name)
					return testCase.destroyErr
				},
			})
			b.compose = &mockComposeExecutor{DownFn: func(
				context.Context,
				string,
				time.Duration,
			) error {
				composeDown = true
				return nil
			}}
			items := []backend.LeaseItem{{
				SKU: "docker-small", ServiceName: "app", Quantity: 1,
			}}
			require.NoError(t, b.pool.TryAllocate(
				closeDeprovisionLeaseUUID+"-app-0",
				"docker-small",
				"tenant-a",
			))
			payload := validStackManifestJSON(map[string]string{
				"app": "docker.io/library/nginx:1.27",
			})
			resourceProfiles := testResourceProfiles(t, items)
			operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
			runtimeAuthority, authorityErr := shared.NewReleaseRuntimeAuthority(
				operationID, "tenant-a", closeDeprovisionProviderUUID,
				callbackURL, lifecycleCallbackURL,
			)
			require.NoError(t, authorityErr)
			seedProvisionReleaseForLeaseTest(
				t, stores.callbacks, stores.releases, stores.operations,
				closeDeprovisionLeaseUUID, shared.Release{
					Manifest:         payload,
					Image:            "stack",
					OperationID:      operationID,
					Items:            items,
					ResourceProfiles: resourceProfiles,
					RuntimeAuthority: &runtimeAuthority,
					Status:           "active",
					CreatedAt:        time.Now(),
				})

			err := b.doDeprovisionForTest(t, context.Background(), closeDeprovisionLeaseUUID)
			if testCase.wantPending {
				require.ErrorContains(t, err, testCase.destroyErr.Error())
			} else {
				require.NoError(t, err)
			}
			require.True(t, composeDown,
				"projection absence must not bypass Compose discovery/teardown")
			require.Equal(t, []string{
				canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0),
			}, destroyed)

			claim, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
			require.NoError(t, readErr)
			require.Equal(t, testCase.wantPending, found)
			if found {
				require.True(t, claim.CleanupOnly())
				require.Equal(t, testCase.wantAttempts, claim.ExecutionGeneration().Number())
			}
			releases, readErr := stores.releases.List(closeDeprovisionLeaseUUID)
			require.NoError(t, readErr)
			if testCase.wantPending {
				require.Len(t, releases, 1,
					"uncertain cleanup must preserve its exact release authority")
				require.Equal(t, 1, b.pool.Stats().AllocationCount,
					"pending cleanup-only authority must keep its topology reserved")
			} else {
				require.Empty(t, releases)
				require.Zero(t, b.pool.Stats().AllocationCount,
					"terminal cleanup-only settlement must release its topology")
			}

			closeCloseRecoveryBackend(t, b, stores)
		})
	}
}

func TestDoDeprovision_FailedInitialOperationBecomesCleanupOnlyCloseAndPermanentFence(t *testing.T) {
	dir := t.TempDir()
	visible := true
	late := ContainerInfo{
		ContainerID:   "late-initial-failed-operation",
		LeaseUUID:     closeDeprovisionLeaseUUID,
		Tenant:        "tenant-a",
		ProviderUUID:  closeDeprovisionProviderUUID,
		BackendName:   DefaultConfig().Name,
		SKU:           "docker-small",
		ServiceName:   "app",
		InstanceIndex: 0,
		Status:        "running",
	}
	removed := 0
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if !visible {
				return nil, nil
			}
			return []ContainerInfo{late}, nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			require.Equal(t, late.ContainerID, containerID)
			require.True(t, visible)
			copy := late
			return &copy, nil
		},
		StopContainerFn: func(_ context.Context, containerID string, timeout time.Duration) error {
			require.Equal(t, late.ContainerID, containerID)
			require.Positive(t, timeout)
			return nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			require.Equal(t, late.ContainerID, containerID)
			removed++
			visible = false
			return nil
		},
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	late.CallbackURL, late.LifecycleCallbackURL = callbackURL, lifecycleCallbackURL
	candidate, err := stores.operations.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            closeDeprovisionLeaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Tenant:               "tenant-a",
		ProviderUUID:         closeDeprovisionProviderUUID,
		Items:                items,
		ResourceProfiles:     testResourceProfiles(t, items),
		Manifest: validStackManifestJSON(map[string]string{
			"app": "docker.io/library/nginx:1.27",
		}),
	})
	require.NoError(t, err)
	admission, err := stores.operations.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim, created := admission.CreatedClaim()
	require.True(t, created)
	require.Equal(t, operationID, claim.OperationID())
	uncommitted := commitPreEffectOperationFailureForTest(t, stores.operations, claim)
	require.NoError(t, b.callbackPublisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "initial operation definitively refused",
	))

	require.NoError(t, b.doDeprovisionForTest(
		t, context.Background(), closeDeprovisionLeaseUUID,
	))
	require.Equal(t, 1, removed, "close retires the exact captured interrupted cohort")
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.False(t, found, "cleanup must consume the failed-operation head")
	closed, err := stores.callbacks.LookupClosedLeaseReceipts(
		[]string{closeDeprovisionLeaseUUID},
	)
	require.NoError(t, err)
	require.Len(t, closed, 1)
	require.Equal(t, shared.ClosedLeaseAuthorityOrphan, closed[0].AuthorityKind())

	// A still-later daemon Create is owned by the permanent close receipt, not
	// by the now-archived failure witness, and is removed before projection.
	visible = true
	require.NoError(t, b.recoverState(context.Background()))
	require.Equal(t, 2, removed, "the permanent close fence also retires a later arrival")
	b.provisionsMu.RLock()
	_, projected := b.provisions[closeDeprovisionLeaseUUID]
	b.provisionsMu.RUnlock()
	require.False(t, projected)

	closeCloseRecoveryBackend(t, b, stores)
}
