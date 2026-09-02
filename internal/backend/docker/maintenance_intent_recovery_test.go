package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type maintenanceInventory struct {
	mu            sync.Mutex
	containers    []ContainerInfo
	removed       []string
	inspectErr    error
	inspectErrFor map[string]error
}

func (inventory *maintenanceInventory) list(context.Context) ([]ContainerInfo, error) {
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	return slices.Clone(inventory.containers), nil
}

func (inventory *maintenanceInventory) inspect(_ context.Context, id string) (*ContainerInfo, error) {
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.inspectErr != nil {
		return nil, inventory.inspectErr
	}
	if err := inventory.inspectErrFor[id]; err != nil {
		return nil, err
	}
	for _, container := range inventory.containers {
		if container.ContainerID == id {
			copy := container
			return &copy, nil
		}
	}
	return nil, errors.New("container not found")
}

func (inventory *maintenanceInventory) remove(_ context.Context, id string) error {
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	for index, container := range inventory.containers {
		if container.ContainerID != id {
			continue
		}
		inventory.containers = slices.Delete(inventory.containers, index, index+1)
		inventory.removed = append(inventory.removed, id)
		return nil
	}
	return nil
}

type maintenanceRecoveryHarness struct {
	t             *testing.T
	b             *Backend
	inventory     *maintenanceInventory
	leaseUUID     string
	releasePath   string
	callbackPath  string
	releases      *shared.ReleaseStore
	callbacks     *shared.CallbackStore
	source        shared.Release
	sourceClaim   shared.ReleaseClaim
	intent        shared.MaintenanceIntentClaim
	appendClaim   shared.MaintenanceAppendClaim
	target        shared.MaintenanceReleaseClaim
	targetRelease shared.Release
}

func newMaintenanceRecoveryHarness(t *testing.T) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentRestart)
}

func newMaintenanceRecoveryHarnessForKind(
	t *testing.T,
	kind shared.MaintenanceIntentKind,
) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForAuthority(t, kind, false)
}

func newLegacyMaintenanceRecoveryHarnessForKind(
	t *testing.T,
	kind shared.MaintenanceIntentKind,
) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForAuthority(t, kind, true)
}

func newLegacyMaintenanceRecoveryHarnessForKindAtCallback(
	t *testing.T,
	kind shared.MaintenanceIntentKind,
	callbackURL string,
) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForAuthorityAtCallback(
		t, kind, true, callbackURL,
	)
}

func newMaintenanceRecoveryHarnessForAuthority(
	t *testing.T,
	kind shared.MaintenanceIntentKind,
	legacy bool,
) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForAuthorityAtCallback(t, kind, legacy, "")
}

func newMaintenanceRecoveryHarnessForAuthorityAtCallback(
	t *testing.T,
	kind shared.MaintenanceIntentKind,
	legacy bool,
	targetCallbackURL string,
) *maintenanceRecoveryHarness {
	t.Helper()
	dir := t.TempDir()
	inventory := &maintenanceInventory{}
	mock := &mockDockerClient{
		ListManagedContainersFn: inventory.list,
		InspectContainerFn:      inventory.inspect,
		RemoveContainerFn:       inventory.remove,
	}
	b := newBackendForTest(mock, nil)
	b.cfg.Name = "docker-a"
	b.cfg.StartupVerifyDuration = time.Millisecond
	leaseUUID := uuid.NewString()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	b.releaseStore = releases
	b.callbackStore = callbacks

	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "web"}}
	profiles := testResourceProfiles(t, items)
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"web": {Image: "docker.io/library/nginx:1.27"},
	}}
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	source := shared.Release{
		Manifest: manifestBytes, Image: "stack", Items: items,
		ResourceProfiles: profiles, Status: "active", CreatedAt: time.Now(),
	}
	if legacy {
		callbackURL = "https://fred.example/callbacks/provision"
		lifecycleCallbackURL = callbackURL
		authority, authorityErr := shared.NewLegacyRuntimeAuthority(
			"tenant-a", "22222222-2222-4222-8222-222222222222",
			callbackURL, lifecycleCallbackURL,
		)
		require.NoError(t, authorityErr)
		source.LegacyRuntimeAuthority = &authority
	} else {
		source.OperationID = operationID
		source.RuntimeAuthority = mustTestReleaseRuntimeAuthority(
			t, operationID, "tenant-a", "22222222-2222-4222-8222-222222222222",
			callbackURL, lifecycleCallbackURL,
		)
	}
	require.NoError(t, releases.AppendActive(leaseUUID, source))
	source, sourceClaim, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := source
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	target.Items = slices.Clone(items)
	target.ResourceProfiles = shared.CloneSKUResourceSnapshot(profiles)
	if targetCallbackURL != "" {
		require.True(t, legacy, "only legacy callback rotation is used by this harness")
		sourceAuthority := mustDockerReleaseRuntimeIdentity(t, source)
		targetAuthority, authorityErr := shared.NewLegacyRuntimeAuthority(
			sourceAuthority.Tenant(), sourceAuthority.ProviderUUID(),
			targetCallbackURL, targetCallbackURL,
		)
		require.NoError(t, authorityErr)
		target.LegacyRuntimeAuthority = &targetAuthority
	}
	admission, err := callbacks.BeginMaintenanceIntent(shared.MaintenanceIntentSpec{
		Kind:             kind,
		SourceRelease:    sourceClaim,
		TargetRelease:    target,
		Backend:          b.Name(),
		BackendStorageID: b.storageIdentity,
	})
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	intent := appendClaim.Intent()

	harness := &maintenanceRecoveryHarness{
		t:            t,
		b:            b,
		inventory:    inventory,
		leaseUUID:    leaseUUID,
		releasePath:  releasePath,
		callbackPath: callbackPath,
		releases:     releases,
		callbacks:    callbacks,
		source:       source,
		sourceClaim:  sourceClaim,
		intent:       intent,
		appendClaim:  appendClaim,
	}
	t.Cleanup(func() {
		b.stopCancel()
		require.NoError(t, harness.callbacks.Close())
		require.NoError(t, harness.releases.Close())
	})
	return harness
}

func (h *maintenanceRecoveryHarness) appendTarget(bind bool) {
	h.t.Helper()
	target, err := h.releases.AppendMaintenance(h.appendClaim)
	require.NoError(h.t, err)
	h.target = target
	targetRelease, _, found, err := h.releases.FindMaintenanceRelease(h.leaseUUID, h.intent.MaintenanceID())
	require.NoError(h.t, err)
	require.True(h.t, found)
	h.targetRelease = targetRelease
	if bind {
		h.intent, err = h.callbacks.BindMaintenanceIntentTarget(h.intent, target)
		require.NoError(h.t, err)
	}
}

func (h *maintenanceRecoveryHarness) reopen() {
	h.t.Helper()
	require.NoError(h.t, h.callbacks.Close())
	require.NoError(h.t, h.releases.Close())
	var err error
	h.releases, err = shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: h.releasePath})
	require.NoError(h.t, err)
	h.callbacks, err = shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: h.callbackPath})
	require.NoError(h.t, err)
	h.b.releaseStore = h.releases
	h.b.callbackStore = h.callbacks
}

func (h *maintenanceRecoveryHarness) containersFor(release shared.Release, count int, status string, health HealthStatus) []ContainerInfo {
	h.t.Helper()
	authority, ok := release.RuntimeIdentity()
	require.True(h.t, ok)
	containers := make([]ContainerInfo, 0, count)
	for index := range count {
		containers = append(containers, ContainerInfo{
			ContainerID:          release.MaintenanceID.String() + "-container-" + string(rune('a'+index)),
			LeaseUUID:            h.leaseUUID,
			Tenant:               authority.Tenant(),
			ProviderUUID:         authority.ProviderUUID(),
			BackendName:          h.b.Name(),
			SKU:                  release.Items[0].SKU,
			ServiceName:          release.Items[0].ServiceName,
			InstanceIndex:        index,
			CallbackURL:          authority.CallbackURL(),
			LifecycleCallbackURL: authority.LifecycleCallbackURL(),
			MaintenanceID:        release.MaintenanceID,
			Image:                "docker.io/library/nginx:1.27",
			Status:               status,
			Health:               health,
			CreatedAt:            time.Now().Add(-time.Minute),
			CustomDomain:         release.Items[0].CustomDomain,
			Name:                 "fred-" + h.leaseUUID + "-web-" + string(rune('0'+index)),
		})
	}
	return containers
}

func (h *maintenanceRecoveryHarness) assertSettled(status backend.CallbackStatus) {
	h.t.Helper()
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(h.t, err)
	assert.Empty(h.t, intents)
	pending, err := h.callbacks.ListPending()
	require.NoError(h.t, err)
	require.Len(h.t, pending, 1)
	assert.Equal(h.t, shared.CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)
	assert.Equal(h.t, status, pending[0].Status)
}

func (h *maintenanceRecoveryHarness) assertCommittedRuntimeFailureSettled() {
	h.t.Helper()
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(h.t, err)
	assert.Empty(h.t, intents)
	pending, err := h.callbacks.ListPending()
	require.NoError(h.t, err)
	require.Len(h.t, pending, 2)
	assert.Equal(h.t, shared.CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)
	assert.Equal(h.t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(h.t, shared.CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)
	assert.Equal(h.t, backend.CallbackStatusFailed, pending[1].Status)
	assert.Equal(h.t, leasesm.ErrMsgCohortDiverged, pending[1].Error)
	assert.Less(h.t, pending[0].Sequence, pending[1].Sequence)
}

func TestRecoverMaintenanceIntentAcrossEveryDurableCrashBoundary(t *testing.T) {
	t.Run("intent before target append resolves failure", func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
		h.reopen()
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		h.assertSettled(backend.CallbackStatusFailed)
		active, err := h.releases.LatestActive(h.leaseUUID)
		require.NoError(t, err)
		require.NotNil(t, active)
		assert.Empty(t, active.MaintenanceID)
	})

	for _, bind := range []bool{false, true} {
		name := "append before bind"
		if bind {
			name = "bound deploying target"
		}
		t.Run(name+" exact cohort commits success", func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(bind)
			h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
			h.reopen()
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertSettled(backend.CallbackStatusSuccess)
			active, err := h.releases.LatestActive(h.leaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
		})
	}

	t.Run("active target with zero survivors preserves success then reports runtime failure", func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		require.NoError(t, h.releases.ActivateMaintenance(h.target))
		h.reopen()
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		h.assertCommittedRuntimeFailureSettled()
		active, err := h.releases.LatestActive(h.leaseUUID)
		require.NoError(t, err)
		require.NotNil(t, active)
		assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
	})
}

func TestRecoverLegacyMaintenanceTargetAcrossColdRestart(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentRestart,
		shared.MaintenanceIntentUpdate,
		shared.MaintenanceIntentCustomDomain,
	} {
		t.Run(string(kind), func(t *testing.T) {
			h := newLegacyMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(false)
			h.inventory.containers = h.containersFor(
				h.targetRelease, 2, "running", HealthStatusNone,
			)
			h.reopen()

			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertSettled(backend.CallbackStatusSuccess)
			active, err := h.releases.LatestActive(h.leaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
			assert.Empty(t, active.OperationID)
			assert.Nil(t, active.RuntimeAuthority)
			require.NotNil(t, active.LegacyRuntimeAuthority)
			assert.Equal(t, shared.ReleaseAuthorityLegacy,
				mustDockerReleaseRuntimeIdentity(t, *active).Class())
		})
	}
}

func TestRecoverLegacyMaintenanceCallbackBaseAcrossColdRestart(t *testing.T) {
	const movedCallbackURL = "https://moved.example/callbacks/provision"
	h := newLegacyMaintenanceRecoveryHarnessForKindAtCallback(
		t, shared.MaintenanceIntentRestart, movedCallbackURL,
	)
	h.appendTarget(false)
	h.inventory.containers = h.containersFor(
		h.targetRelease, 2, "running", HealthStatusNone,
	)
	h.reopen()

	// recoverState is the cold-start path: it settles the durable maintenance
	// intent before projecting the exact target cohort into memory.
	require.NoError(t, h.b.recoverState(t.Context()))
	h.assertSettled(backend.CallbackStatusSuccess)

	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, movedCallbackURL, pending[0].CallbackURL,
		"cold settlement must notify the callback route persisted by the target intent")

	active, err := h.releases.LatestActive(h.leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	activeAuthority := mustDockerReleaseRuntimeIdentity(t, *active)
	assert.Equal(t, shared.ReleaseAuthorityLegacy, activeAuthority.Class())
	assert.Equal(t, movedCallbackURL, activeAuthority.CallbackURL())
	assert.Equal(t, movedCallbackURL, activeAuthority.LifecycleCallbackURL())
	assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
	assert.Empty(t, active.OperationID)
	assert.Nil(t, active.RuntimeAuthority)

	h.b.provisionsMu.RLock()
	projected := h.b.provisions[h.leaseUUID]
	h.b.provisionsMu.RUnlock()
	require.NotNil(t, projected)
	assert.Equal(t, backend.ProvisionStatusReady, projected.Status)
	assert.Equal(t, movedCallbackURL, projected.CallbackURL)
	assert.Equal(t, movedCallbackURL, projected.LifecycleCallbackURL)
}

func mustDockerReleaseRuntimeIdentity(
	t *testing.T,
	release shared.Release,
) shared.ReleaseRuntimeIdentity {
	t.Helper()
	authority, ok := release.RuntimeIdentity()
	require.True(t, ok)
	return authority
}

func TestRecoverMaintenancePreservesUpdateImagePullFailurePolicy(t *testing.T) {
	h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	h.appendTarget(true)
	require.NoError(t, h.releases.FailMaintenance(
		h.target, backend.ReasonImagePullFailed, backend.MsgImagePullFailed,
	))
	h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
	h.reopen()

	require.NoError(t, h.b.RefreshState(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.MsgImagePullFailed, pending[0].Error)
	projected, found := h.b.provisionStore.Get(h.leaseUUID)
	require.True(t, found)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
	assert.Equal(t, backend.ReasonImagePullFailed, projected.Reason)
	assert.Equal(t, backend.MsgImagePullFailed, projected.Message)
	assert.Equal(t, h.source.Items, projected.Items)
	assert.Equal(t, h.source.ResourceProfiles, projected.ResourceProfiles)
}

func TestRecoverMaintenanceSettlesWhileLeaseCallbackDeliveryIsSlow(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var requestOnce sync.Once
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestOnce.Do(func() { close(requestStarted) })
		select {
		case <-releaseRequest:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    req,
			}, nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	sender, err := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:           h.callbacks,
		HTTPClient:      client,
		Secret:          durableCallbackTestSecret,
		StorageIdentity: h.b.storageIdentity,
		BeforeDelivery:  allowTestCallbackDelivery,
		BeforeReplay:    allowTestCallbackDelivery,
		Logger:          h.b.logger,
		StopCtx:         stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
	})
	require.NoError(t, err)
	sender.SendLifecycleCallback(
		h.leaseUUID,
		h.intent.LifecycleCallbackURL(),
		h.b.Name(),
		backend.CallbackStatusFailed,
		"earlier observation",
		false,
	)
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.ReplayPendingCallbacks()
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("callback replay did not acquire the lease delivery lock")
	}

	recoverDone := make(chan error, 1)
	go func() { recoverDone <- h.b.recoverMaintenanceIntents(t.Context()) }()
	select {
	case recoverErr := <-recoverDone:
		require.NoError(t, recoverErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("maintenance recovery blocked behind callback HTTP")
	}
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "settlement must consume the WAL without waiting for callback HTTP")
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, shared.CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	assert.Equal(t, shared.CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusFailed, pending[1].Status)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)

	close(releaseRequest)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("callback replay did not release the lease delivery lock")
	}
	pending, err = h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestRecoverMaintenanceBindsTargetWhileLeaseCallbackDeliveryIsSlow(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(false)
	h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var requestOnce sync.Once
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := &http.Client{Transport: dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestOnce.Do(func() { close(requestStarted) })
		select {
		case <-releaseRequest:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    req,
			}, nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	sender, err := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:           h.callbacks,
		HTTPClient:      client,
		Secret:          durableCallbackTestSecret,
		StorageIdentity: h.b.storageIdentity,
		BeforeDelivery:  allowTestCallbackDelivery,
		BeforeReplay:    allowTestCallbackDelivery,
		Logger:          h.b.logger,
		StopCtx:         stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
	})
	require.NoError(t, err)
	sender.SendLifecycleCallback(
		h.leaseUUID,
		h.intent.LifecycleCallbackURL(),
		h.b.Name(),
		backend.CallbackStatusFailed,
		"earlier observation",
		false,
	)
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.ReplayPendingCallbacks()
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("callback replay did not acquire the lease delivery lock")
	}

	recoverDone := make(chan error, 1)
	go func() { recoverDone <- h.b.recoverMaintenanceIntents(t.Context()) }()
	select {
	case recoverErr := <-recoverDone:
		require.NoError(t, recoverErr)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("maintenance target binding blocked behind callback HTTP")
	}
	stored, found, err := h.callbacks.GetMaintenanceIntent(h.leaseUUID)
	require.NoError(t, err)
	assert.False(t, found, "successful binding and settlement must consume the WAL")
	assert.False(t, stored.Valid())
	targetRelease, _, found, err := h.releases.FindMaintenanceRelease(
		h.leaseUUID, h.intent.MaintenanceID(),
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "active", targetRelease.Status)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, shared.CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	assert.Equal(t, shared.CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[1].Status)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)

	close(releaseRequest)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("callback replay did not release the lease delivery lock")
	}
	pending, err = h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	active, err := h.releases.LatestActive(h.leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
}

func TestRecoverMaintenancePartialTargetRemovesOnlyExactGeneration(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	sourceSurvivor := h.containersFor(h.source, 1, "running", HealthStatusNone)
	targetSurvivor := h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
	h.inventory.containers = append(sourceSurvivor, targetSurvivor...)
	h.reopen()

	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
	assert.Equal(t, []string{targetSurvivor[0].ContainerID}, h.inventory.removed)
	remaining, err := h.inventory.list(t.Context())
	require.NoError(t, err)
	require.Equal(t, sourceSurvivor, remaining)
	releases, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 2)
	assert.Equal(t, "active", releases[0].Status)
	assert.Equal(t, "failed", releases[1].Status)
}

func TestRecoverMaintenancePreservesWALWhenSourceReadinessIsIndeterminate(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	source := h.containersFor(h.source, 2, "running", HealthStatusNone)
	target := h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
	h.inventory.containers = append(slices.Clone(source), target...)
	h.inventory.inspectErrFor = map[string]error{
		source[0].ContainerID: errors.New("source inspect transport failed"),
	}
	h.reopen()

	require.ErrorContains(t, h.b.recoverMaintenanceIntents(t.Context()), "source readiness is indeterminate")
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	assert.Equal(t, []string{target[0].ContainerID}, h.inventory.removed)
	release, _, found, err := h.releases.FindMaintenanceRelease(
		h.leaseUUID, h.intent.MaintenanceID(),
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "failed", release.Status)

	h.inventory.inspectErrFor = nil
	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
}

func TestRecoverMaintenanceFailsClosedOnUnreadableOrDivergentTarget(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*maintenanceRecoveryHarness)
	}{
		{name: "inspect unreadable", mutate: func(h *maintenanceRecoveryHarness) {
			h.inventory.inspectErr = errors.New("daemon read failed")
		}},
		{name: "runtime identity divergent", mutate: func(h *maintenanceRecoveryHarness) {
			h.inventory.containers[0].Tenant = "tenant-b"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			h.inventory.containers = h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
			test.mutate(h)
			h.reopen()
			require.Error(t, h.b.recoverMaintenanceIntents(t.Context()))
			intents, err := h.callbacks.ListMaintenanceIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1)
			assert.Empty(t, h.inventory.removed)
			release, _, found, err := h.releases.FindMaintenanceRelease(h.leaseUUID, h.intent.MaintenanceID())
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, "deploying", release.Status)
		})
	}
}

func TestRefreshStateSkipsLiveMaintenanceThenRetriesTerminalSettlement(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	h.inventory.containers = append(
		h.containersFor(h.source, 2, "running", HealthStatusNone),
		h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)...,
	)
	h.b.provisions[h.leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID:            h.leaseUUID,
		Tenant:               h.source.RuntimeAuthority.Tenant(),
		ProviderUUID:         h.source.RuntimeAuthority.ProviderUUID(),
		Status:               backend.ProvisionStatusReady,
		CallbackURL:          h.source.RuntimeAuthority.CallbackURL(),
		LifecycleCallbackURL: h.source.RuntimeAuthority.LifecycleCallbackURL(),
		Items:                slices.Clone(h.source.Items),
		StackManifest:        h.targetReleaseStack(),
	}, ResourceProfiles: shared.CloneSKUResourceSnapshot(h.source.ResourceProfiles)}

	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.NoError(t, h.b.routeToLeaseBlocking(t.Context(), h.leaseUUID, leasesm.RestartRequestedMsg{
		Cancel:               func() {},
		CallbackURL:          h.intent.CallbackURL(),
		LifecycleCallbackURL: h.intent.LifecycleCallbackURL(),
		Maintenance:          h.intent,
		Ack:                  ack,
		Work: func() leasesm.ReplaceResult {
			<-workerRelease
			return leasesm.ReplaceResult{
				Err: errors.New("simulated terminal settlement drop"),
				Failure: leasesm.ReplaceFailureInfo{
					Operation: "restart", CallbackErr: "restart interrupted",
					LastError: "restart interrupted", PreserveMaintenance: true,
				},
			}
		},
	}))
	require.NoError(t, <-ack)
	require.True(t, h.b.actorOwnsMaintenance(h.leaseUUID, h.intent.MaintenanceID()))

	require.NoError(t, h.b.RefreshState(t.Context()))
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Empty(t, h.inventory.removed)

	// The worker finishes but its terminal callback deliberately preserves the
	// WAL. Exact ownership clears only after the terminal event is applied; the
	// next sweep cleans only the target-ID remnant, proves the full source cohort,
	// corrects the actor Failed->Ready, then resolves the intent.
	close(workerRelease)
	require.Eventually(t, func() bool {
		return !h.b.actorOwnsMaintenance(h.leaseUUID, h.intent.MaintenanceID())
	}, time.Second, time.Millisecond)
	require.NoError(t, h.b.RefreshState(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
	require.Equal(t, backend.ProvisionStatusReady, h.b.actors[h.leaseUUID].State())
	projected, found := h.b.provisionStore.Get(h.leaseUUID)
	require.True(t, found)
	require.Equal(t, backend.ProvisionStatusReady, projected.Status)

	// The actor is no longer stale-busy, but the exact completion deliberately
	// remains the subscriber-ordering fence until synchronous delivery precisely
	// removes it. Model that successful delivery before the subsequent command.
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(t, h.callbacks.RemoveEntry(pending[0]))

	// A subsequent exact maintenance command is then admitted by the same actor
	// without a process restart.
	_, sourceClaim, err := h.releases.ClaimLatestActive(h.leaseUUID)
	require.NoError(t, err)
	template := h.intent.TargetRelease()
	template.Version = 0
	template.MaintenanceID = ""
	template.Status = "deploying"
	template.CreatedAt = time.Now()
	nextIntent, _, err := h.b.admitMaintenance(shared.MaintenanceIntentRestart, sourceClaim, template)
	require.NoError(t, err)
	nextAck := make(chan error, 1)
	require.NoError(t, h.b.routeToLeaseBlocking(t.Context(), h.leaseUUID, leasesm.RestartRequestedMsg{
		Cancel:               func() {},
		CallbackURL:          nextIntent.CallbackURL(),
		LifecycleCallbackURL: nextIntent.LifecycleCallbackURL(),
		Maintenance:          nextIntent,
		Ack:                  nextAck,
		Work: func() leasesm.ReplaceResult {
			return leasesm.ReplaceResult{Err: errors.New("test cleanup"), Failure: leasesm.ReplaceFailureInfo{
				Operation: "restart", PreserveMaintenance: true,
			}}
		},
	}))
	require.NoError(t, <-nextAck)
}

func TestRecoverCommittedMaintenancePromotesTargetProfilesThroughLiveActor(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
	staleProfiles := shared.CloneSKUResourceSnapshot(h.source.ResourceProfiles)
	staleProfiles[0].CPUCores += 99
	h.b.provisions[h.leaseUUID] = &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            h.leaseUUID,
			Tenant:               h.source.RuntimeAuthority.Tenant(),
			ProviderUUID:         h.source.RuntimeAuthority.ProviderUUID(),
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          h.source.RuntimeAuthority.CallbackURL(),
			LifecycleCallbackURL: h.source.RuntimeAuthority.LifecycleCallbackURL(),
			Items:                slices.Clone(h.source.Items),
			StackManifest:        h.targetReleaseStack(),
		},
		ResourceProfiles: staleProfiles,
	}

	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.NoError(t, h.b.routeToLeaseBlocking(t.Context(), h.leaseUUID, leasesm.RestartRequestedMsg{
		Cancel:               func() {},
		CallbackURL:          h.intent.CallbackURL(),
		LifecycleCallbackURL: h.intent.LifecycleCallbackURL(),
		Maintenance:          h.intent,
		Ack:                  ack,
		Work: func() leasesm.ReplaceResult {
			<-workerRelease
			return leasesm.ReplaceResult{
				Err: errors.New("ambiguous activation acknowledgement"),
				Failure: leasesm.ReplaceFailureInfo{
					Operation: "restart", CallbackErr: "restart failed",
					LastError: "ambiguous activation acknowledgement", PreserveMaintenance: true,
				},
			}
		},
	}))
	require.NoError(t, <-ack)
	require.NoError(t, h.releases.ActivateMaintenance(h.target))
	close(workerRelease)
	require.Eventually(t, func() bool {
		return !h.b.actorOwnsMaintenance(h.leaseUUID, h.intent.MaintenanceID()) &&
			h.b.actors[h.leaseUUID].State() == backend.ProvisionStatusFailed
	}, time.Second, time.Millisecond)

	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	h.assertSettled(backend.CallbackStatusSuccess)
	require.Equal(t, backend.ProvisionStatusReady, h.b.actors[h.leaseUUID].State())
	h.b.provisionsMu.RLock()
	projected := h.b.provisions[h.leaseUUID]
	require.NotNil(t, projected)
	assert.Equal(t, h.targetRelease.ResourceProfiles, projected.ResourceProfiles)
	assert.Equal(t, h.targetRelease.ResourceProfiles, projected.ProvisionState.ResourceProfiles)
	assert.Equal(t, h.targetRelease.Items, projected.Items)
	h.b.provisionsMu.RUnlock()
}

func (h *maintenanceRecoveryHarness) targetReleaseStack() *manifest.StackManifest {
	h.t.Helper()
	stack, err := manifest.ParsePayload(h.targetRelease.Manifest)
	require.NoError(h.t, err)
	return stack
}

func TestRecoverMaintenanceDoesNotActivateUnreadyExactCohort(t *testing.T) {
	for _, test := range []struct {
		name   string
		status string
		health HealthStatus
	}{
		{name: "exited", status: "exited", health: HealthStatusNone},
		{name: "unhealthy", status: "running", health: HealthStatusUnhealthy},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			h.inventory.containers = h.containersFor(h.targetRelease, 2, test.status, test.health)
			h.reopen()
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertSettled(backend.CallbackStatusFailed)
			assert.Len(t, h.inventory.removed, 2)
			active, err := h.releases.LatestActive(h.leaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Empty(t, active.MaintenanceID)
		})
	}
}

func TestRecoverActiveMaintenancePreservesWALOnIndeterminateReadiness(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	require.NoError(t, h.releases.ActivateMaintenance(h.target))
	h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
	h.inventory.inspectErr = errors.New("docker inspect transport failed")
	h.reopen()

	require.ErrorContains(t, h.b.recoverMaintenanceIntents(t.Context()), "readiness is indeterminate")
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	assert.Empty(t, h.inventory.removed)
	active, err := h.releases.LatestActive(h.leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
}
