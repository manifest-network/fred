package docker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// TestReleaseTenantNetwork_SkipsWhenOtherLeaseActive covers the race that
// motivated this helper: a concurrent provision of lease B on the same
// tenant must prevent lease A's deprovision from removing the network.
func TestReleaseTenantNetwork_SkipsWhenOtherLeaseActive(t *testing.T) {
	removeCalled := false
	mock := &mockDockerClient{
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			removeCalled = true
			return nil
		},
	}

	// Another lease on the same tenant is still live.
	b := newBackendForTest(mock, map[string]*provision{
		"lease-b": {LeaseUUID: "lease-b", Tenant: "tenant-x", Status: backend.ProvisionStatusProvisioning},
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			removedTenants = append(removedTenants, tenant)
			return nil
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			removedTenants = append(removedTenants, tenant)
			return nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-other": {LeaseUUID: "lease-other", Tenant: "tenant-y", Status: backend.ProvisionStatusReady},
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			observe()
			return nil
		},
	}

	b := newBackendForTest(mock, nil)

	var wg sync.WaitGroup
	ctx := context.Background()
	for range 20 {
		wg.Go(func() {
			_, _ = b.ensureTenantNetwork(ctx, "tenant-x")
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
			_, _ = b.ensureTenantNetwork(ctx, tenant)
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
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			removeCalled.Store(true)
			return nil
		},
	}

	// Lease B exists in b.provisions (as it would after Provision()'s
	// synchronous reservation phase) while its doProvision is running.
	b := newBackendForTest(mock, map[string]*provision{
		"lease-b": {LeaseUUID: "lease-b", Tenant: "tenant-x", Status: backend.ProvisionStatusProvisioning},
	})

	ctx := context.Background()
	var wg sync.WaitGroup

	// Lease B's doProvision-style ensure call.
	wg.Go(func() {
		netID, err := b.ensureTenantNetwork(ctx, "tenant-x")
		require.NoError(t, err)
		assert.Equal(t, "net-id", netID)
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
