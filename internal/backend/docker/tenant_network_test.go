package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		want          tenantNetworkRemoval
		wantError     bool
	}{
		{name: "removed", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{}}`, removeStatus: http.StatusNoContent, want: tenantNetworkRemoved},
		{name: "already absent", inspectStatus: http.StatusNotFound, inspectBody: `{"message":"network not found"}`, want: tenantNetworkAbsent},
		{name: "in use", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{"container-id":{"Name":"active"}}}`, want: tenantNetworkInUse},
		{name: "removed concurrently", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{}}`, removeStatus: http.StatusNotFound, want: tenantNetworkAbsent},
		{name: "inspect failed", inspectStatus: http.StatusInternalServerError, inspectBody: `{"message":"inspect failed"}`, want: tenantNetworkRemovalUnknown, wantError: true},
		{name: "remove failed", inspectStatus: http.StatusOK, inspectBody: `{"Id":"network-id","Containers":{}}`, removeStatus: http.StatusConflict, want: tenantNetworkRemovalUnknown, wantError: true},
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (tenantNetworkRemoval, error) {
			removeCalled = true
			return tenantNetworkRemoved, nil
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (tenantNetworkRemoval, error) {
			removedTenants = append(removedTenants, tenant)
			return tenantNetworkRemoved, nil
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (tenantNetworkRemoval, error) {
			removedTenants = append(removedTenants, tenant)
			return tenantNetworkRemoved, nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-other": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-other", Tenant: "tenant-y", Status: backend.ProvisionStatusReady}},
	})

	err := b.releaseTenantNetwork(context.Background(), "tenant-x")
	require.NoError(t, err)
	assert.Equal(t, []string{"tenant-x"}, removedTenants)
}

// TestTenantNetwork_SerializesEnsureAndRelease verifies that for a given
// tenant, EnsureTenantNetwork and RemoveTenantNetworkIfEmpty are never in
// flight at the same time. This is the property that prevents the race.
func TestTenantNetwork_SerializesEnsureAndRelease(t *testing.T) {
	var inFlight atomic.Int32
	var overlap atomic.Bool

	observe := func() {
		if inFlight.Add(1) > 1 {
			overlap.Store(true)
		}
		// Keep the fake op long enough to produce observable overlap
		// if serialization ever broke.
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
	}

	mock := &mockDockerClient{
		EnsureTenantNetworkFn: func(ctx context.Context, tenant string) (string, error) {
			observe()
			return "net-id", nil
		},
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (tenantNetworkRemoval, error) {
			observe()
			return tenantNetworkRemoved, nil
		},
	}

	b := newBackendForTest(mock, nil)

	var wg sync.WaitGroup
	ctx := context.Background()
	for range 20 {
		wg.Go(func() {
			_ = runTenantStorageMutationForTest(t, b, stackFixtureLeaseUUID, "tenant-x",
				func(mutations *storageMutations) error {
					return b.ensureTenantNetworkWith(mutations, ctx, "tenant-x")
				})
		})
		wg.Go(func() {
			_ = b.releaseTenantNetwork(ctx, "tenant-x")
		})
	}
	wg.Wait()

	assert.False(t, overlap.Load(), "ensure and release must never overlap for the same tenant")
}

// TestTenantNetwork_DifferentTenantsRunInParallel verifies that the
// per-tenant mutex does NOT serialize operations across different
// tenants — that would regress throughput.
func TestTenantNetwork_DifferentTenantsRunInParallel(t *testing.T) {
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	mock := &mockDockerClient{
		EnsureTenantNetworkFn: func(ctx context.Context, tenant string) (string, error) {
			n := inFlight.Add(1)
			for {
				cur := maxInFlight.Load()
				if n <= cur || maxInFlight.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			return "net-id", nil
		},
	}

	b := newBackendForTest(mock, nil)

	var wg sync.WaitGroup
	ctx := context.Background()
	for i := range 10 {
		tenant := string(rune('a'+i)) + "-tenant"
		wg.Go(func() {
			_ = runTenantStorageMutationForTest(t, b, stackFixtureLeaseUUID, tenant,
				func(mutations *storageMutations) error {
					return b.ensureTenantNetworkWith(mutations, ctx, tenant)
				})
		})
	}
	wg.Wait()

	assert.Greater(t, maxInFlight.Load(), int32(1),
		"different tenants should run in parallel; got max_in_flight=%d", maxInFlight.Load())
}

// TestTenantNetwork_RaceScenario pins the tenant-network removal
// invariant: when lease A deprovisions while lease B provisions on
// the same tenant, A must observe B's entry in b.provisions and skip
// network removal, so B's ContainerCreate doesn't fail on a missing
// network between Ensure and Create.
func TestTenantNetwork_RaceScenario(t *testing.T) {
	var removeCalled atomic.Bool
	mock := &mockDockerClient{
		EnsureTenantNetworkFn: func(ctx context.Context, tenant string) (string, error) {
			// Widen the window between Ensure and ContainerCreate so
			// a concurrent Deprovision can race through the removal path.
			time.Sleep(5 * time.Millisecond)
			return "net-id", nil
		},
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) (tenantNetworkRemoval, error) {
			removeCalled.Store(true)
			return tenantNetworkRemoved, nil
		},
	}

	// Lease B exists in b.provisions (as it would after Provision()'s
	// synchronous reservation phase) while its doProvision is running.
	b := newBackendForTest(mock, map[string]*provision{
		"lease-b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-b", Tenant: "tenant-x", Status: backend.ProvisionStatusProvisioning}},
	})

	ctx := context.Background()
	var wg sync.WaitGroup

	// Lease B's doProvision-style ensure call.
	wg.Go(func() {
		err := runTenantStorageMutationForTest(t, b, stackFixtureLeaseUUID, "tenant-x",
			func(mutations *storageMutations) error {
				return b.ensureTenantNetworkWith(mutations, ctx, "tenant-x")
			})
		require.NoError(t, err)
	})

	// Lease A's deprovision has already removed its own entry and now
	// tries to release the tenant network. B's entry is still present,
	// so release must be skipped.
	wg.Go(func() {
		err := b.releaseTenantNetwork(ctx, "tenant-x")
		require.NoError(t, err)
	})

	wg.Wait()
	assert.False(t, removeCalled.Load(),
		"with lease-b still present in b.provisions, release must not remove the network")
}
