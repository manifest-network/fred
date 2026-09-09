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
	"github.com/manifest-network/fred/internal/backendidentity"
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
	retentionPath string
	releases      *shared.ReleaseStore
	callbacks     *shared.CallbackStore
	retentions    *shared.RetentionStore
	operations    *shared.OperationSettlement
	source        shared.Release
	sourceClaim   shared.MaintenanceSourceClaim
	intent        shared.MaintenanceIntentClaim
	appendClaim   shared.MaintenanceAppendClaim
	target        shared.MaintenanceReleaseClaim
	targetRelease shared.Release
}

func newMaintenanceRecoveryHarness(t *testing.T) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentRestart)
}

// newMaintenanceRecoveryHarnessWithPriorLifecycleCallback queues the older
// observation while the lease is still in its Ready mutation phase, then
// admits maintenance. Runtime observation authority intentionally cannot be
// manufactured after the maintenance head exists.
func newMaintenanceRecoveryHarnessWithPriorLifecycleCallback(
	t *testing.T,
) *maintenanceRecoveryHarness {
	return newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(
		t, shared.MaintenanceIntentRestart, false, "", true,
	)
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
	return newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(
		t, kind, legacy, targetCallbackURL, false,
	)
}

func newMaintenanceRecoveryHarnessForAuthorityAtCallbackOptions(
	t *testing.T,
	kind shared.MaintenanceIntentKind,
	legacy bool,
	targetCallbackURL string,
	queuePriorLifecycle bool,
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
	retentionPath := filepath.Join(dir, "retention.db")
	b.cfg.CallbackDBPath = callbackPath
	b.cfg.ReleasesDBPath = releasePath
	b.cfg.RetentionDBPath = retentionPath
	dockerClient, volumes := fullStorageClientsForTest(b)
	storage, err := (testDockerStorageIdentity{}).resolve(
		context.Background(), b.cfg, dockerClient, volumes,
	)
	require.NoError(t, err)
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	releases, err := shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: releasePath}, storage, gate,
	)
	require.NoError(t, err)
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: callbackPath}, storage, gate,
	)
	require.NoError(t, err)
	retentions, err := shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: retentionPath}, storage, gate,
	)
	require.NoError(t, err)
	b.releaseStore = releases
	b.callbackStore = callbacks
	b.retentionStore = retentions
	operations, err := shared.NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	b.operationSettlement = operations
	b.restoreSettlement, err = shared.NewRestoreSettlement(operations, retentions)
	require.NoError(t, err)
	b.maintenanceSettlement, err = shared.NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	b.closeSettlement, err = shared.NewCloseSettlement(callbacks, releases, retentions)
	require.NoError(t, err)
	b.releaseCapacityPlanner = b.operationSettlement
	b.storageIdentity = storage.ID()
	b.storageAuthority = storage
	b.storeAuthorityGate = gate
	b.storageVerifier = testDockerRuntimeStorageVerifier{id: storage.ID()}
	require.NoError(t, bindBackendTestPhysicalExecutors(
		b, operations, b.maintenanceSettlement,
	))
	bindBackendRecoveryCoordinatorForTest(t, b)
	rebuildCallbackSender(b, testCallbackClient)

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
	if legacy {
		legacySource := source
		legacySource.Items = nil
		legacySource.ResourceProfiles = nil
		legacySource.LegacyRuntimeAuthority = nil
		seedUpgradedV013ReleaseForBackendTest(
			t, b, leaseUUID, legacySource, items, profiles,
			*source.LegacyRuntimeAuthority,
		)
		releases = b.releaseStore
		var ok bool
		operations, ok = concreteOperationSettlementForTest(b.operationSettlement)
		require.True(t, ok)
	} else {
		seedProvisionReleaseForLeaseTest(
			t, callbacks, releases, operations, leaseUUID, source,
		)
	}
	if queuePriorLifecycle {
		require.False(t, legacy, "typed lifecycle authority is required by this fixture")
		runtimeProof, proofErr := releases.ProveRuntimeGeneration(leaseUUID)
		require.NoError(t, proofErr)
		runtimePermit, permitErr := b.callbackPublisher.AuthorizeRuntimeObservationContext(
			context.Background(), runtimeProof,
		)
		require.NoError(t, permitErr)
		require.NoError(t, b.callbackPublisher.PublishLifecycleFailureContext(
			context.Background(), runtimePermit, "earlier observation",
		))
	}
	source, sourceClaim, err := b.maintenanceSettlement.ClaimLatestActive(leaseUUID)
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
	payload := []byte(nil)
	if kind != shared.MaintenanceIntentRestart {
		payload = target.Manifest
	}
	request, err := b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		mustParseMaintenanceID(t, uuid.NewString()), kind, leaseUUID,
		mustDockerReleaseRuntimeIdentity(t, target).LifecycleCallbackURL(),
		payload,
	)
	require.NoError(t, err)
	candidate, err := b.maintenanceSettlement.NewMaintenanceIntentCandidate(request, sourceClaim, target)
	require.NoError(t, err)
	admission, err := b.maintenanceSettlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := b.maintenanceSettlement.StartMaintenanceAppend(
		createdTestMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	intent := appendClaim.Intent()

	harness := &maintenanceRecoveryHarness{
		t:             t,
		b:             b,
		inventory:     inventory,
		leaseUUID:     leaseUUID,
		releasePath:   releasePath,
		callbackPath:  callbackPath,
		retentionPath: retentionPath,
		releases:      releases,
		callbacks:     callbacks,
		retentions:    retentions,
		operations:    operations,
		source:        source,
		sourceClaim:   sourceClaim,
		intent:        intent,
		appendClaim:   appendClaim,
	}
	t.Cleanup(func() {
		b.stopCancel()
		require.NoError(t, harness.callbacks.Close())
		require.NoError(t, harness.releases.Close())
		require.NoError(t, harness.retentions.Close())
	})
	return harness
}

func (h *maintenanceRecoveryHarness) appendTarget(bind bool) {
	h.t.Helper()
	target, err := h.b.maintenanceSettlement.AppendMaintenance(h.appendClaim)
	require.NoError(h.t, err)
	h.target = target
	targetRelease, _, found, err := h.b.maintenanceSettlement.FindMaintenanceRelease(
		h.leaseUUID, h.intent.MaintenanceID(),
	)
	require.NoError(h.t, err)
	require.True(h.t, found)
	h.targetRelease = targetRelease
	if bind {
		h.target, err = h.b.maintenanceSettlement.BindMaintenanceIntentTarget(target)
		require.NoError(h.t, err)
		h.intent = h.target.Intent()
	}
}

func (h *maintenanceRecoveryHarness) reopen() {
	h.t.Helper()
	require.NoError(h.t, h.callbacks.Close())
	require.NoError(h.t, h.releases.Close())
	require.NoError(h.t, h.retentions.Close())
	dockerClient, volumes := fullStorageClientsForTest(h.b)
	storage, err := (testDockerStorageIdentity{}).resolve(
		context.Background(), h.b.cfg, dockerClient, volumes,
	)
	require.NoError(h.t, err)
	h.releases, err = shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: h.releasePath}, storage, h.b.storeAuthorityGate,
	)
	require.NoError(h.t, err)
	h.callbacks, err = shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: h.callbackPath}, storage, h.b.storeAuthorityGate,
	)
	require.NoError(h.t, err)
	h.retentions, err = shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: h.retentionPath}, storage, h.b.storeAuthorityGate,
	)
	require.NoError(h.t, err)
	h.b.releaseStore = h.releases
	h.b.callbackStore = h.callbacks
	h.b.retentionStore = h.retentions
	h.operations, err = shared.NewOperationSettlement(h.callbacks, h.releases)
	require.NoError(h.t, err)
	h.b.operationSettlement = h.operations
	h.b.restoreSettlement, err = shared.NewRestoreSettlement(h.operations, h.retentions)
	require.NoError(h.t, err)
	h.b.maintenanceSettlement, err = shared.NewMaintenanceSettlement(h.callbacks, h.releases)
	require.NoError(h.t, err)
	h.b.closeSettlement, err = shared.NewCloseSettlement(h.callbacks, h.releases, h.retentions)
	require.NoError(h.t, err)
	h.b.releaseCapacityPlanner = h.b.operationSettlement
	h.b.storageIdentity = storage.ID()
	h.b.storageAuthority = storage
	h.b.storageVerifier = testDockerRuntimeStorageVerifier{id: storage.ID()}
	require.NoError(h.t, bindBackendTestPhysicalExecutors(
		h.b, h.operations, h.b.maintenanceSettlement,
	))
	bindBackendRecoveryCoordinatorForTest(h.t, h.b)
	rebuildCallbackSender(h.b, testCallbackClient)
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
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
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
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
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

	t.Run("append before bind resolves failure without physical authority", func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(false)
		h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
		h.reopen()
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		h.assertSettled(backend.CallbackStatusFailed)
	})

	t.Run("started deploying target exact cohort commits success", func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
		require.NoError(t, err)
		h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
		h.reopen()
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		h.assertSettled(backend.CallbackStatusSuccess)
		active, err := h.releases.LatestActive(h.leaseUUID)
		require.NoError(t, err)
		require.NotNil(t, active)
		assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
	})

	t.Run("active target with zero survivors preserves success then reports runtime failure", func(t *testing.T) {
		h := newMaintenanceRecoveryHarness(t)
		h.appendTarget(true)
		_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
		require.NoError(t, err)
		h.reopen()
		require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
		h.assertCommittedRuntimeFailureSettled()
		active, err := h.releases.LatestActive(h.leaseUUID)
		require.NoError(t, err)
		require.NotNil(t, active)
		assert.Equal(t, h.intent.MaintenanceID(), active.MaintenanceID)
	})
}

func TestRecoverStartedMaintenanceDefersYoungNonterminalInventory(t *testing.T) {
	for _, test := range []struct {
		name   string
		source bool
	}{
		{name: "empty"},
		{name: "source only", source: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newMaintenanceRecoveryHarness(t)
			h.appendTarget(true)
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			if test.source {
				h.inventory.containers = h.containersFor(
					h.source, 2, "running", HealthStatusNone,
				)
			}
			h.reopen()

			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1)
			pending, err := h.callbacks.ListPending()
			require.NoError(t, err)
			assert.Empty(t, pending)
			assert.Empty(t, h.inventory.removed)

			// A target which becomes visible on the next level-triggered pass is
			// committed, rather than being deleted by the first incomplete view.
			h.inventory.containers = h.containersFor(
				h.targetRelease, 2, "running", HealthStatusNone,
			)
			require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
			h.assertSettled(backend.CallbackStatusSuccess)
		})
	}
}

func TestFailedMaintenanceReceiptRemovesLateExactTarget(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	h.b.cfg.ProvisionTimeout = time.Nanosecond
	source := h.containersFor(h.source, 2, "running", HealthStatusNone)
	h.inventory.containers = slices.Clone(source)
	h.reopen()

	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
	receipts, err := h.b.maintenanceSettlement.ListFailedMaintenanceReceipts()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	acknowledgePendingCallbacksForTest(t, h.callbacks)

	late := h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
	h.inventory.containers = append(h.inventory.containers, late...)
	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	assert.Equal(t, []string{late[0].ContainerID}, h.inventory.removed)
	remaining, err := h.inventory.list(t.Context())
	require.NoError(t, err)
	require.Equal(t, source, remaining)
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestRecoverLegacyMaintenanceTargetAcrossColdRestart(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{
		shared.MaintenanceIntentRestart,
		shared.MaintenanceIntentUpdate,
		shared.MaintenanceIntentCustomDomain,
	} {
		t.Run(string(kind), func(t *testing.T) {
			h := newLegacyMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
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
			assert.True(t, active.OperationID.IsZero())
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
	h.appendTarget(true)
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
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
	assert.True(t, active.OperationID.IsZero())
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
	_, err := failMaintenanceForTest(
		t, h.b.maintenanceSettlement, h.target,
		backend.ReasonImagePullFailed, backend.MsgImagePullFailed, false,
	)

	require.NoError(t, err)
	h.inventory.containers = h.containersFor(h.source, 2, "running", HealthStatusNone)
	h.reopen()

	require.NoError(t, h.b.RefreshState(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.MsgImagePullFailed, pending[0].Error)
	h.b.provisionsMu.RLock()
	projected, found := h.b.provisions[h.leaseUUID]
	h.b.provisionsMu.RUnlock()
	require.True(t, found)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
	assert.Equal(t, backend.ReasonImagePullFailed, projected.Reason)
	assert.Equal(t, backend.MsgImagePullFailed, projected.Message)
	assert.Equal(t, h.source.Items, projected.Items)
	assert.Equal(t, h.source.ResourceProfiles, projected.ResourceProfiles)
}

func TestRecoverMaintenanceSettlesWhileLeaseCallbackDeliveryIsSlow(t *testing.T) {
	h := newMaintenanceRecoveryHarnessWithPriorLifecycleCallback(t)
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
	attestor := callbackStorageAttestorForTest(
		t, h.callbacks, stopCtx, allowTestCallbackDelivery,
	)
	sender, err := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:           h.callbacks,
		StorageAttestor: attestor,
		HTTPClient:      client,
		Secret:          durableCallbackTestSecret,
		Logger:          h.b.logger,

		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
	})
	require.NoError(t, err)
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.RunReplayLoop()
	}()
	sender.NotifyPendingCallbacks()
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
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
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
	require.Eventually(t, func() bool {
		remaining, listErr := h.callbacks.ListPending()
		return listErr == nil && len(remaining) == 0
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("callback replay did not release the lease delivery lock")
	}
	pending, err = h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestRecoverMaintenanceSuccessWhileLeaseCallbackDeliveryIsSlow(t *testing.T) {
	h := newMaintenanceRecoveryHarnessWithPriorLifecycleCallback(t)
	h.appendTarget(true)
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
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
	attestor := callbackStorageAttestorForTest(
		t, h.callbacks, stopCtx, allowTestCallbackDelivery,
	)
	sender, err := shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:           h.callbacks,
		StorageAttestor: attestor,
		HTTPClient:      client,
		Secret:          durableCallbackTestSecret,
		Logger:          h.b.logger,

		Backoff:         &zeroBackoff,
		DeliveryTimeout: time.Second,
	})
	require.NoError(t, err)
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.RunReplayLoop()
	}()
	sender.NotifyPendingCallbacks()
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
	stored, found, err := h.b.maintenanceSettlement.GetMaintenanceIntent(h.leaseUUID)
	require.NoError(t, err)
	assert.False(t, found, "successful binding and settlement must consume the WAL")
	assert.False(t, stored.Valid())
	history, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	var targetRelease shared.Release
	for _, release := range history {
		if release.MaintenanceID == h.intent.MaintenanceID() {
			targetRelease = release
			break
		}
	}
	require.Equal(t, h.intent.MaintenanceID(), targetRelease.MaintenanceID)
	assert.Equal(t, "active", targetRelease.Status)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, shared.CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	assert.Equal(t, shared.CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[1].Status)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)

	close(releaseRequest)
	require.Eventually(t, func() bool {
		remaining, listErr := h.callbacks.ListPending()
		return listErr == nil && len(remaining) == 0
	}, time.Second, time.Millisecond)
	cancel()
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
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	h.b.cfg.ProvisionTimeout = time.Nanosecond
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
	_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	h.b.cfg.ProvisionTimeout = time.Nanosecond
	source := h.containersFor(h.source, 2, "running", HealthStatusNone)
	target := h.containersFor(h.targetRelease, 1, "running", HealthStatusNone)
	h.inventory.containers = append(slices.Clone(source), target...)
	h.inventory.inspectErrFor = map[string]error{
		source[0].ContainerID: errors.New("source inspect transport failed"),
	}
	h.reopen()

	require.ErrorContains(t, h.b.recoverMaintenanceIntents(t.Context()), "source inspect transport failed")
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	assert.Equal(t, []string{target[0].ContainerID}, h.inventory.removed)
	release, _, found, err := h.b.maintenanceSettlement.FindMaintenanceRelease(
		h.leaseUUID, h.intent.MaintenanceID(),
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "deploying", release.Status,
		"an indeterminate post-cleanup classification must not mint terminal failure authority")

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
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			h.b.cfg.ProvisionTimeout = time.Nanosecond
			count := 1
			if test.name == "inspect unreadable" {
				count = 2
			}
			h.inventory.containers = h.containersFor(h.targetRelease, count, "running", HealthStatusNone)
			test.mutate(h)
			h.reopen()
			require.Error(t, h.b.recoverMaintenanceIntents(t.Context()))
			intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1)
			assert.Empty(t, h.inventory.removed)
			release, _, found, err := h.b.maintenanceSettlement.FindMaintenanceRelease(
				h.leaseUUID, h.intent.MaintenanceID(),
			)
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
		ActiveOperationID:    h.source.OperationID,
		Items:                slices.Clone(h.source.Items),
		ResourceProfiles:     shared.CloneSKUResourceSnapshot(h.source.ResourceProfiles),
		StackManifest:        h.targetReleaseStack(),
	}}

	workerRelease := make(chan struct{})
	workerStarted := make(chan struct{}, 1)
	cleanup := registerMaintenanceExecutionForTest(
		t, h.b.maintenanceSettlement, h.target,
		maintenanceSeedAmbiguous, workerStarted, workerRelease,
	)
	defer cleanup()
	command, reply, err := leasesm.NewRestartCommand(t.Context(), h.target)
	require.NoError(t, err)
	require.NoError(t, h.b.routeToLeaseBlocking(t.Context(), h.leaseUUID, command))
	require.NoError(t, <-reply.Result())
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("construction-bound maintenance worker did not start")
	}
	require.True(t, h.b.actorOwnsMaintenance(h.leaseUUID, h.intent.MaintenanceID()))

	require.NoError(t, h.b.RefreshState(t.Context()))
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Empty(t, h.inventory.removed)

	// The worker finishes but its terminal callback deliberately preserves the
	// WAL. Exact ownership clears only after the terminal event is applied; the
	// next sweep cleans only the target-ID remnant, proves the full source cohort,
	// corrects the actor Failed->Ready, then resolves the intent.
	close(workerRelease)
	// Clearing the observational worker ID precedes the terminal handler's
	// activity release. Recovery needs the actual quiescence capability.
	awaitProvisionWorkerQuiescence(t, h.b, h.leaseUUID)
	require.False(t, h.b.actorOwnsMaintenance(h.leaseUUID, h.intent.MaintenanceID()))
	h.b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, h.b.RefreshState(t.Context()))
	h.assertSettled(backend.CallbackStatusFailed)
	require.Equal(t, backend.ProvisionStatusReady, h.b.actorFor(h.leaseUUID).State())
	h.b.provisionsMu.RLock()
	projected, found := h.b.provisions[h.leaseUUID]
	h.b.provisionsMu.RUnlock()
	require.True(t, found)
	require.Equal(t, backend.ProvisionStatusReady, projected.Status)

	// The actor is no longer stale-busy, but the exact completion deliberately
	// remains the subscriber-ordering fence until synchronous delivery precisely
	// removes it. Model that successful delivery before the subsequent command.
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	acknowledgePendingCallbacksForTest(t, h.callbacks)

	// A subsequent exact maintenance command is then admitted by the same actor
	// without a process restart.
	_, sourceClaim, err := h.b.maintenanceSettlement.ClaimLatestActive(h.leaseUUID)
	require.NoError(t, err)
	template := h.intent.TargetRelease()
	template.Version = 0
	template.MaintenanceID = shared.MaintenanceID{}
	template.Status = "deploying"
	template.CreatedAt = time.Now()
	nextID := mustParseMaintenanceID(t, uuid.NewString())
	nextRequest, err := h.b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		nextID, shared.MaintenanceIntentRestart, h.leaseUUID,
		h.intent.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	nextAdmission, err := h.b.admitMaintenance(nextRequest, sourceClaim, template)
	require.NoError(t, err)
	require.True(t, nextAdmission.created())
	nextCleanup := registerMaintenanceExecutionForTest(
		t, h.b.maintenanceSettlement, nextAdmission.target,
		maintenanceSeedAmbiguous, nil, nil,
	)
	defer nextCleanup()
	command, reply, err = leasesm.NewRestartCommand(t.Context(), nextAdmission.target)
	require.NoError(t, err)
	require.NoError(t, h.b.routeToLeaseBlocking(t.Context(), h.leaseUUID, command))
	require.NoError(t, <-reply.Result())
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
			_, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			h.b.cfg.ProvisionTimeout = time.Nanosecond
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
	_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
	require.NoError(t, err)
	h.inventory.containers = h.containersFor(h.targetRelease, 2, "running", HealthStatusNone)
	h.inventory.inspectErr = errors.New("docker inspect transport failed")
	h.reopen()

	require.ErrorContains(t, h.b.recoverMaintenanceIntents(t.Context()), "readiness is indeterminate")
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
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
