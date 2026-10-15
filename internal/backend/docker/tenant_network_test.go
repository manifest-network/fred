package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestRemoveTenantNetworkReportsActualDockerOutcome(t *testing.T) {
	for _, tc := range []struct {
		name          string
		inspectStatus int
		inspectBody   string
		removeStatus  int
		want          networkRemoval
		wantError     bool
	}{
		{name: "removed", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{}}`, removeStatus: http.StatusNoContent, want: networkRemoved},
		{name: "already absent", inspectStatus: http.StatusNotFound, inspectBody: `{"message":"network not found"}`, want: networkAbsent},
		{name: "in use", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{"container-id":{"Name":"active"}}}`, want: networkInUse},
		{name: "removed concurrently", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{}}`, removeStatus: http.StatusNotFound, want: networkAbsent},
		{name: "inspect failed", inspectStatus: http.StatusInternalServerError, inspectBody: `{"message":"inspect failed"}`, want: networkRemovalUnknown, wantError: true},
		{name: "remove failed", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{}}`, removeStatus: http.StatusConflict, want: networkRemovalUnknown, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			removals := 0
			transport := dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method {
				case http.MethodGet:
					require.Equal(t, "/v1.51/networks/"+TenantNetworkName("tenant-x"), req.URL.Path)
					return imageSecurityResponse(tc.inspectStatus, tc.inspectBody), nil
				case http.MethodDelete:
					removals++
					require.Equal(t, "/v1.51/networks/network-id", req.URL.Path)
					return imageSecurityResponse(tc.removeStatus, `{"message":"remove response"}`), nil
				default:
					t.Fatalf("unexpected Docker request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			})
			sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sdk.Close()) })
			docker := &DockerClient{client: newDockerSDKView(sdk)}
			outcome, err := docker.RemoveTenantNetworkIfEmpty(t.Context(), "tenant-x")
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, outcome)
			if tc.removeStatus == 0 {
				require.Zero(t, removals, "absence and connected containers must never dispatch removal")
			} else {
				require.Equal(t, 1, removals)
			}
		})
	}
}

func TestListIdleManagedNetworksUsesOneFilteredCandidateQuery(t *testing.T) {
	requests := 0
	transport := dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		require.Equal(t, "/v1.51/networks", req.URL.Path,
			"candidate inventory must not inspect each network before selecting cleanup work")
		var filters map[string]map[string]bool
		require.NoError(t, json.Unmarshal([]byte(req.URL.Query().Get("filters")), &filters))
		require.True(t, filters["dangling"]["true"])
		require.True(t, filters["label"][LabelManaged+"=true"])
		require.True(t, filters["label"][LabelBackendName+"=backend-a"])
		return imageSecurityResponse(http.StatusOK, `[{"Id":"first","Labels":{"fred.tenant":"tenant-a"}}]`), nil
	})
	sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sdk.Close()) })
	docker := &DockerClient{client: newDockerSDKView(sdk), backendName: "backend-a"}
	networks, err := docker.ListIdleManagedNetworks(t.Context())
	require.NoError(t, err)
	require.Len(t, networks, 1)
	require.Equal(t, "first", networks[0].ID)
	require.Equal(t, 1, requests)
}

func TestListIdleManagedNetworksRejectsCanceledInventory(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	requests := 0
	transport := dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		require.Equal(t, "/v1.51/networks", req.URL.Path)
		cancel()
		return imageSecurityResponse(http.StatusOK, `[{"Id":"first"}]`), nil
	})
	sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sdk.Close()) })
	docker := &DockerClient{client: newDockerSDKView(sdk)}
	networks, err := docker.ListIdleManagedNetworks(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, networks, "an interrupted inventory must not be reported as a successful empty sweep")
	require.Equal(t, 1, requests)
}

// TestReleaseTenantNetwork_SkipsWhenOtherLeaseActive covers the race that
// motivated this helper: a concurrent provision of lease B on the same
// tenant must prevent lease A's deprovision from removing the network.
func TestReleaseTenantNetwork_SkipsWhenOtherLeaseActive(t *testing.T) {
	removeCalled := false
	mock := &mockDockerClient{
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (networkRemoval, error) {
			removeCalled = true
			return networkRemoved, nil
		},
	}

	// Another lease on the same tenant is still live.
	b := newBackendForTest(mock, map[string]*provision{
		"lease-b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-b", Tenant: "tenant-x", Status: backend.ProvisionStatusProvisioning}},
	})

	err := b.releaseTenantNetwork(context.Background(), "tenant-x")
	require.NoError(t, err)
	assert.False(t, removeCalled, "removal must be skipped while another lease references the tenant")
}

// TestReleaseTenantNetwork_RemovesWhenNoActiveLease verifies the last
// lease for a tenant triggers actual removal.
func TestReleaseTenantNetwork_RemovesWhenNoActiveLease(t *testing.T) {
	var removedTenants []string
	mock := &mockDockerClient{
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (networkRemoval, error) {
			removedTenants = append(removedTenants, tenant)
			return networkRemoved, nil
		},
	}

	// No provisions for tenant-x in the map.
	b := newBackendForTest(mock, nil)

	err := b.releaseTenantNetwork(context.Background(), "tenant-x")
	require.NoError(t, err)
	assert.Equal(t, []string{"tenant-x"}, removedTenants)
}

// TestReleaseTenantNetwork_DifferentTenantsDoNotBlock verifies that a
// provision for a different tenant does not veto removal.
func TestReleaseTenantNetwork_DifferentTenantsDoNotBlock(t *testing.T) {
	var removedTenants []string
	mock := &mockDockerClient{
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (networkRemoval, error) {
			removedTenants = append(removedTenants, tenant)
			return networkRemoved, nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-other": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-other", Tenant: "tenant-y", Status: backend.ProvisionStatusReady}},
	})

	err := b.releaseTenantNetwork(context.Background(), "tenant-x")
	require.NoError(t, err)
	assert.Equal(t, []string{"tenant-x"}, removedTenants)
}

func TestRemoveLeaseNetworkRequiresOwnershipLabels(t *testing.T) {
	const lease = "88888888-8888-4888-8888-888888888888"
	owned := `{"Id":"network-id","Containers":{},"Labels":{"fred.managed":"true","fred.lease_uuid":"` + lease +
		`","fred.network_scope":"lease","fred.backend_name":"backend-a"}}`
	for _, tc := range []struct {
		name        string
		inspectBody string
		want        networkRemoval
		wantRemove  bool
		wantError   bool
	}{
		{name: "owned and empty", inspectBody: owned, want: networkRemoved, wantRemove: true},
		{name: "another lease's labels", inspectBody: `{"Id":"network-id","Containers":{},"Labels":{"fred.managed":"true","fred.lease_uuid":"other","fred.network_scope":"lease","fred.backend_name":"backend-a"}}`, want: networkRemovalUnknown, wantError: true},
		{name: "another backend's network", inspectBody: `{"Id":"network-id","Containers":{},"Labels":{"fred.managed":"true","fred.lease_uuid":"` + lease + `","fred.network_scope":"lease","fred.backend_name":"backend-b"}}`, want: networkRemovalUnknown, wantError: true},
		{name: "unlabeled same-named network", inspectBody: `{"Id":"network-id","Containers":{}}`, want: networkRemovalUnknown, wantError: true},
		{name: "connected", inspectBody: `{"Id":"network-id","Containers":{"c":{"Name":"x"}},"Labels":{"fred.managed":"true","fred.lease_uuid":"` + lease + `","fred.network_scope":"lease","fred.backend_name":"backend-a"}}`, want: networkInUse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			removals := 0
			transport := dockerReplayRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method {
				case http.MethodGet:
					require.Equal(t, "/v1.51/networks/"+leaseNetworkName(lease), req.URL.Path)
					return imageSecurityResponse(http.StatusOK, tc.inspectBody), nil
				case http.MethodDelete:
					removals++
					require.Equal(t, "/v1.51/networks/network-id", req.URL.Path)
					return imageSecurityResponse(http.StatusNoContent, ``), nil
				default:
					t.Fatalf("unexpected Docker request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			})
			sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"), client.WithHTTPClient(&http.Client{Transport: transport}))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sdk.Close()) })
			docker := &DockerClient{client: newDockerSDKView(sdk), backendName: "backend-a"}
			outcome, err := docker.RemoveLeaseNetworkIfEmpty(t.Context(), lease)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, outcome)
			if tc.wantRemove {
				require.Equal(t, 1, removals)
			} else {
				require.Zero(t, removals, "only an empty network carrying this lease's and backend's labels may be removed")
			}
		})
	}
}

func TestReleaseLeaseNetwork_KeepsTrackedLease(t *testing.T) {
	const lease = "99999999-9999-4999-8999-999999999999"
	removeCalled := false
	mock := &mockDockerClient{
		RemoveLeaseNetworkIfEmptyFn: func(context.Context, string) (networkRemoval, error) {
			removeCalled = true
			return networkRemoved, nil
		},
	}
	// A failed or still-provisioning lease keeps its network: compensation and
	// re-provision reattach containers to it by name.
	b := newBackendForTest(mock, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{LeaseUUID: lease, Tenant: "tenant-x", Status: backend.ProvisionStatusFailed}},
	})
	outcome, err := b.releaseLeaseNetwork(t.Context(), lease)
	require.NoError(t, err)
	assert.Equal(t, networkOwned, outcome)
	assert.False(t, removeCalled)
}

func TestReleaseLeaseNetwork_KeepsWhileFenceHeld(t *testing.T) {
	const lease = "99999999-9999-4999-8999-999999999998"
	removeCalled := false
	mock := &mockDockerClient{
		RemoveLeaseNetworkIfEmptyFn: func(context.Context, string) (networkRemoval, error) {
			removeCalled = true
			return networkRemoved, nil
		},
	}
	b := newBackendForTest(mock, nil)
	unlock := b.commandFence.Lock(lease)
	outcome, err := b.releaseLeaseNetwork(t.Context(), lease)
	unlock()
	require.NoError(t, err)
	assert.Equal(t, networkBusy, outcome)
	assert.False(t, removeCalled, "an admission holding the fence may be about to use the network")

	outcome, err = b.releaseLeaseNetwork(t.Context(), lease)
	require.NoError(t, err)
	assert.Equal(t, networkRemoved, outcome)
	assert.True(t, removeCalled)
}
