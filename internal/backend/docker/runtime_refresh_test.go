package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestParseRuntimeContract(t *testing.T) {
	for _, tt := range []struct {
		name   string
		labels map[string]string
		want   runtimeContract
	}{
		{name: "no label predates the contract", labels: map[string]string{}, want: 0},
		{name: "nil labels", labels: nil, want: 0},
		{name: "unreadable value predates the contract", labels: map[string]string{LabelRuntimeContract: "one"}, want: 0},
		{name: "negative value", labels: map[string]string{LabelRuntimeContract: "-1"}, want: 0},
		{name: "current", labels: map[string]string{LabelRuntimeContract: currentRuntimeContract.label()}, want: currentRuntimeContract},
		{name: "newer", labels: map[string]string{LabelRuntimeContract: "7"}, want: 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseRuntimeContract(tt.labels))
		})
	}
	assert.True(t, runtimeContract(0).predatesCurrent())
	assert.False(t, currentRuntimeContract.predatesCurrent())
	assert.False(t, (currentRuntimeContract + 1).predatesCurrent(), "a newer release's container is never recreated by this one")
}

func TestSelectRuntimeRefreshCandidates(t *testing.T) {
	ready := map[string]struct{}{"lease-b": {}, "lease-a": {}, "lease-current": {}, "lease-no-containers": {}}
	containers := []ContainerInfo{
		{LeaseUUID: "lease-b", RuntimeContract: currentRuntimeContract},
		{LeaseUUID: "lease-b", RuntimeContract: 0}, // one stale container makes the lease stale
		{LeaseUUID: "lease-a", RuntimeContract: 0},
		{LeaseUUID: "lease-a", RuntimeContract: 0},
		{LeaseUUID: "lease-current", RuntimeContract: currentRuntimeContract},
		{LeaseUUID: "lease-not-ready", RuntimeContract: 0},
	}
	assert.Equal(t, []string{"lease-a", "lease-b"}, selectRuntimeRefreshCandidates(ready, containers))
	assert.Empty(t, selectRuntimeRefreshCandidates(nil, containers))
	assert.Empty(t, selectRuntimeRefreshCandidates(ready, nil), "a lease without a listed container is not a candidate")
}

func TestStartRuntimeRefreshRefusesWhenDisabled(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()
	disabled := 0
	b.cfg.RuntimeRefreshMaxConcurrency = &disabled
	_, err := b.StartRuntimeRefresh(t.Context())
	require.ErrorIs(t, err, ErrRuntimeRefreshDisabled)
}

func TestRuntimeRefreshStatusReportsInventoryErrors(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, errors.New("docker unavailable")
		},
	}, nil)
	defer b.stopCancel()
	_, err := b.RuntimeRefreshStatus(t.Context())
	require.Error(t, err, "an inventory error must never read as zero stale leases")
}

func TestRuntimeRefreshStatusJSONContract(t *testing.T) {
	status := RuntimeRefreshStatus{
		Contract: 1, Active: true, Stale: 2, InFlight: 1, Refreshed: 1, Failed: 1, Skipped: 1,
		Leases: []RuntimeRefreshLease{
			{LeaseUUID: "a", MaintenanceID: "m-1", Outcome: RuntimeRefreshRefreshed},
			{LeaseUUID: "b", Outcome: RuntimeRefreshSkipped},
		},
	}
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"contract": 1, "active": true, "stale": 2, "in_flight": 1,
		"refreshed": 1, "failed": 1, "skipped": 1,
		"leases": [
			{"lease_uuid": "a", "maintenance_id": "m-1", "outcome": "refreshed"},
			{"lease_uuid": "b", "outcome": "skipped"}
		]
	}`, string(encoded))
	empty, err := json.Marshal(RuntimeRefreshStatus{Leases: []RuntimeRefreshLease{}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"contract":0,"active":false,"stale":0,"in_flight":0,"refreshed":0,"failed":0,"skipped":0,"leases":[]}`,
		string(empty))
}

// runtimeRefreshHarness is a Ready stack lease whose only container predates
// the current runtime contract, with real durable maintenance stores.
type runtimeRefreshHarness struct {
	b         *Backend
	leaseUUID string
	mu        sync.Mutex
	substrate ContainerInfo
	upOpts    []composeUpOpts
	project   *composetypes.Project
	callbacks []backend.CallbackPayload
}

func newRuntimeRefreshHarness(t *testing.T, status backend.ProvisionStatus) *runtimeRefreshHarness {
	t.Helper()
	h := &runtimeRefreshHarness{leaseUUID: "550e8400-e29b-41d4-a716-446655440077"}
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{"app": {Image: "nginx:latest"}}}
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		w.WriteHeader(http.StatusOK)
		h.mu.Lock()
		h.callbacks = append(h.callbacks, payload)
		h.mu.Unlock()
	}))
	t.Cleanup(callbackServer.Close)
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	operationID := mustDockerOperationID(lifecycleID)
	operationURL := callbackServer.URL + "?operation_id=" + lifecycleID
	lifecycleURL := callbackServer.URL + "?lifecycle_id=" + lifecycleID
	h.substrate = ContainerInfo{
		ContainerID: "old-app", LeaseUUID: h.leaseUUID, BackendName: "docker", SKU: "docker-small",
		Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		CallbackURL: operationURL, LifecycleCallbackURL: lifecycleURL,
		ServiceName: "app", Image: "nginx:latest", Status: "running",
		CreatedAt: time.Now().Add(-time.Minute), Name: "fred-" + h.leaseUUID + "-app-0",
		// No runtime-contract label: created before the contract existed.
		RuntimeContract: 0,
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return []ContainerInfo{h.substrate}, nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			copied := h.substrate
			copied.ContainerID = containerID
			return &copied, nil
		},
	}
	composeMock := &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, opts composeUpOpts) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.project = project
			h.upOpts = append(h.upOpts, opts)
			service := project.Services["app"]
			h.substrate.ContainerID = "new-app"
			h.substrate.MaintenanceID = mustParseMaintenanceID(t, service.Labels[LabelMaintenanceID])
			h.substrate.RuntimeContract = parseRuntimeContract(service.Labels)
			imageReference, err := containerImageReference(service.Image, service.Image, service.Labels)
			if err != nil {
				return err
			}
			h.substrate.Image = imageReference
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-app", Service: "app", State: "running"}}, nil
		},
	}
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	provisions := map[string]*provision{
		h.leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: h.leaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			SKU: "docker-small", Status: status, StackManifest: stack, FailCount: 2,
			ContainerIDs: []string{"old-app"}, ServiceContainers: map[string][]string{"app": {"old-app"}},
			CallbackURL: operationURL, LifecycleCallbackURL: lifecycleURL, Items: items, Quantity: 1,
		}},
	}
	h.b = newBackendForProvisionTest(t, mock, provisions)
	h.b.compose = composeMock
	seedCustomDomainMaintenanceAuthority(
		t, h.b, h.leaseUUID, stack, items, operationID, operationURL, lifecycleURL, callbackServer.Client(),
	)
	h.b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	h.b.cfg.NetworkIsolation = ptrBool(true)
	return h
}

func (h *runtimeRefreshHarness) waitForRunToFinish(t *testing.T) RuntimeRefreshStatus {
	t.Helper()
	var status RuntimeRefreshStatus
	require.Eventually(t, func() bool {
		var err error
		status, err = h.b.RuntimeRefreshStatus(t.Context())
		return err == nil && !status.Active
	}, 30*time.Second, 50*time.Millisecond)
	return status
}

func TestRuntimeRefreshRecreatesAStaleLeaseAndAttestsTheActiveRelease(t *testing.T) {
	h := newRuntimeRefreshHarness(t, backend.ProvisionStatusReady)
	refreshedBefore := testutil.ToFloat64(runtimeRefreshTotal.WithLabelValues(string(RuntimeRefreshRefreshed)))

	before, err := h.b.RuntimeRefreshStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, before.Stale)
	assert.False(t, before.Active)

	started, err := h.b.StartRuntimeRefresh(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint32(currentRuntimeContract), started.Contract)
	status := h.waitForRunToFinish(t)

	h.mu.Lock()
	upOpts := append([]composeUpOpts(nil), h.upOpts...)
	project := h.project
	container := h.substrate
	h.mu.Unlock()
	require.Len(t, upOpts, 1)
	assert.True(t, upOpts[0].ForceRecreate, "a refresh recreates every container")
	require.NotNil(t, project)
	assert.Equal(t, currentRuntimeContract.label(), project.Services["app"].Labels[LabelRuntimeContract])
	assert.Contains(t, project.Networks, leaseNetworkKey, "the refreshed lease runs on its own network")

	assert.Equal(t, 0, status.Stale)
	assert.Equal(t, 1, status.Refreshed)
	assert.Zero(t, status.Failed)
	assert.Zero(t, status.InFlight)
	require.Equal(t, []RuntimeRefreshLease{{
		LeaseUUID: h.leaseUUID, MaintenanceID: container.MaintenanceID.String(), Outcome: RuntimeRefreshRefreshed,
	}}, status.Leases, "the attestation names the maintenance its new containers carry")

	active, err := h.b.releaseStore.LatestActive(h.leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, container.MaintenanceID, active.MaintenanceID, "only a durably active release is attested")
	h.b.provisionsMu.RLock()
	provision := *h.b.provisions[h.leaseUUID]
	h.b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, provision.Status)
	assert.Equal(t, 2, provision.FailCount, "a refresh is not a failure")
	assert.Equal(t, refreshedBefore+1, testutil.ToFloat64(runtimeRefreshTotal.WithLabelValues(string(RuntimeRefreshRefreshed))))

	// A second run finds nothing stale and attests nothing.
	_, err = h.b.StartRuntimeRefresh(t.Context())
	require.NoError(t, err)
	second := h.waitForRunToFinish(t)
	assert.Empty(t, second.Leases)
	h.mu.Lock()
	assert.Len(t, h.upOpts, 1, "a lease under the current contract is never recreated")
	h.mu.Unlock()
}

func TestRuntimeRefreshSkipsALeaseWithACommandInProgress(t *testing.T) {
	h := newRuntimeRefreshHarness(t, backend.ProvisionStatusReady)
	unlock := h.b.commandFence.Lock(h.leaseUUID)
	defer unlock()
	h.b.runtimeRefresh.inFlight = map[string]struct{}{}
	attestation := h.b.refreshLeaseRuntime(t.Context(), h.leaseUUID)
	assert.Equal(t, RuntimeRefreshLease{LeaseUUID: h.leaseUUID, Outcome: RuntimeRefreshSkipped}, attestation)
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "a busy lease journals nothing")
	h.mu.Lock()
	assert.Empty(t, h.upOpts)
	h.mu.Unlock()
}

func TestRuntimeRefreshRouteRefusesAFailedLease(t *testing.T) {
	h := newRuntimeRefreshHarness(t, backend.ProvisionStatusFailed)
	request, err := h.b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		mustParseMaintenanceID(t, "6ba7b812-9dad-41d1-80b4-00c04fd430c8"), shared.MaintenanceIntentRuntimeRefresh,
		h.leaseUUID, h.substrate.LifecycleCallbackURL, nil,
	)
	require.NoError(t, err)
	err = h.b.routeReplaceRestart(t.Context(), request, runtimeRefreshRoute())
	require.ErrorIs(t, err, backend.ErrInvalidState, "a refresh recreates only a healthy lease")
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)

	// The kind and the route must agree.
	err = h.b.routeReplaceRestart(t.Context(), request, tenantRestartRoute())
	require.ErrorIs(t, err, backend.ErrValidation)
	err = h.b.routeReplaceRestart(t.Context(), request, replaceRestartRoute{})
	require.ErrorIs(t, err, backend.ErrValidation, "the zero route admits nothing")
}

func TestReplaceRestartRoutes(t *testing.T) {
	_, err := customDomainRoute(nil)
	require.Error(t, err, "a custom-domain redeploy needs a domain change")
	route, err := customDomainRoute(map[string]string{"app": "a.example.com"})
	require.NoError(t, err)
	assert.True(t, route.valid())
	assert.True(t, route.waitsForLeaseCommands())
	assert.True(t, route.admitsFailedSource())
	assert.True(t, tenantRestartRoute().valid())
	refresh := runtimeRefreshRoute()
	assert.True(t, refresh.valid())
	assert.False(t, refresh.waitsForLeaseCommands())
	assert.False(t, refresh.admitsFailedSource())
	assert.False(t, replaceRestartRoute{}.valid())
	assert.True(t, maintenanceForcesRecreate(shared.MaintenanceIntentRestart))
	assert.True(t, maintenanceForcesRecreate(shared.MaintenanceIntentRuntimeRefresh))
	assert.False(t, maintenanceForcesRecreate(shared.MaintenanceIntentUpdate))
	assert.False(t, maintenanceForcesRecreate(shared.MaintenanceIntentCustomDomain))
}

// A run stays active, and refuses a second POST, from its start until it has
// finished every lease, including while it waits to retry a busy lease. Its
// rows always agree with its counters.
func TestRuntimeRefreshRunStaysActiveWhileALeaseIsBusy(t *testing.T) {
	h := newRuntimeRefreshHarness(t, backend.ProvisionStatusReady)
	unlock := h.b.commandFence.Lock(h.leaseUUID)
	defer unlock()

	started, err := h.b.StartRuntimeRefresh(t.Context())
	require.NoError(t, err)
	assert.True(t, started.Active)
	assert.Equal(t, 1, started.Stale)

	var status RuntimeRefreshStatus
	require.Eventually(t, func() bool {
		status, err = h.b.RuntimeRefreshStatus(t.Context())
		return err == nil && len(status.Leases) == 1
	}, 10*time.Second, 20*time.Millisecond)
	assert.True(t, status.Active, "a run waiting to retry a busy lease is still active")
	assert.Equal(t, 1, status.Stale)
	assert.Equal(t, 1, status.Skipped)
	assert.Equal(t, []RuntimeRefreshLease{{LeaseUUID: h.leaseUUID, Outcome: RuntimeRefreshSkipped}}, status.Leases)

	_, err = h.b.StartRuntimeRefresh(t.Context())
	require.ErrorIs(t, err, ErrRuntimeRefreshActive)
	h.mu.Lock()
	assert.Empty(t, h.upOpts, "a busy lease is never recreated")
	h.mu.Unlock()
}
