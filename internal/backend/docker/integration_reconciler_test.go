//go:build integration

package docker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// testReconcilerTracker adapts InFlightTracker + PayloadStore to satisfy ReconcilerTracker.
type testReconcilerTracker struct {
	provisioner.InFlightTracker
	store *payload.Store
}

func (t *testReconcilerTracker) HasPayload(leaseUUID string) (bool, error) {
	return t.store.Has(leaseUUID)
}

func (t *testReconcilerTracker) PayloadStore() *payload.Store {
	return t.store
}

// reconcilerTestEnv holds all components for a full-stack reconciler integration test.
type reconcilerTestEnv struct {
	backend      *Backend
	reconciler   *provisioner.Reconciler
	tracker      *testReconcilerTracker
	chainClient  *chain.MockClient
	callbackCh   <-chan backend.CallbackPayload
	callbackURL  string
	providerUUID string
}

// testReconcilerSetup creates a full-stack test environment:
// real docker backend + reconciler + mock chain + tracker + payload store.
func testReconcilerSetup(t *testing.T, chainClient *chain.MockClient) *reconcilerTestEnv {
	t.Helper()

	callbackServer, callbackCh := startCallbackServer(t)

	// Create a backend with fast reconcile for detection
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 2 * time.Second
	})

	// Create the router wrapping our real docker backend
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)

	// Create payload store in temp directory
	store, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// Create in-flight tracker + reconciler tracker adapter
	inFlight := provisioner.NewInFlightTracker()
	tracker := &testReconcilerTracker{
		InFlightTracker: inFlight,
		store:           store,
	}

	providerUUID := "test-provider"

	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			ProviderUUID:           providerUUID,
			CallbackBaseURL:        callbackServer.URL,
			Interval:               1 * time.Hour, // manual RunOnce only
			MaxReprovisionAttempts: 3,
		},
		chainClient,
		router,
		tracker,
	)
	require.NoError(t, err)

	return &reconcilerTestEnv{
		backend:      b,
		reconciler:   reconciler,
		tracker:      tracker,
		chainClient:  chainClient,
		callbackCh:   callbackCh,
		callbackURL:  callbackServer.URL,
		providerUUID: providerUUID,
	}
}

// makeLease creates a billingtypes.Lease with a payload MetaHash.
func makeLease(leaseUUID, tenant, providerUUID, sku string, quantity uint64, metaHash []byte) billingtypes.Lease {
	return billingtypes.Lease{
		Uuid:         leaseUUID,
		Tenant:       tenant,
		ProviderUuid: providerUUID,
		Items: []billingtypes.LeaseItem{
			{
				SkuUuid:  sku,
				Quantity: quantity,
			},
		},
		MetaHash: metaHash,
	}
}

func TestIntegration_Reconciler_ContainerDied_ReProvisions(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-reprov-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	// Prepare the manifest payload
	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	// Set up mock chain state - starts as PENDING
	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	// Store payload in PayloadStore
	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// RunOnce → should see PENDING + not provisioned → start provisioning
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision success callback")
	}

	// Untrack in-flight (simulates what the handler would do on callback)
	env.tracker.UntrackInFlight(leaseUUID)

	// Transition lease to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// RunOnce to acknowledge (PENDING+Ready → acknowledge)
	// But we just set it to ACTIVE, so it should be ACTIVE+Ready → no action (healthy)
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Kill the container
	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	killContainer(t, containers[0].ID)

	// Wait for the docker backend's recoverState to detect failure
	waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	// Drain the failure callback from recoverState before triggering re-provision
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status, "should receive failure callback from recoverState")
	case <-time.After(10 * time.Second):
		// recoverState may have already fired it; continue
	}

	// RunOnce → sees ACTIVE + Failed → re-provisions
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for new success callback from re-provision
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for re-provision success callback")
	}

	// Verify new container is running
	info, err := env.backend.GetInfo(ctx, leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestIntegration_Reconciler_CrashLoop_ClosesLease(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-crash-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING
	var closedLeases []string

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			mu.Unlock()
			return 1, []string{"txhash-close"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// Initial provision
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	select {
	case cb := <-env.callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for initial provision callback")
	}
	env.tracker.UntrackInFlight(leaseUUID)

	// Transition to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// Kill → FailCount=1 → re-provision → Kill → FailCount=2 → re-provision → Kill → FailCount=3 → close
	for i := 1; i <= 3; i++ {
		// Kill the container
		containers := inspectProvisionContainers(t, leaseUUID)
		require.NotEmpty(t, containers, "expected running container for iteration %d", i)
		killContainer(t, containers[0].ID)

		// Wait for backend to detect failure
		waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

		// Drain the failure callback from recoverState
		select {
		case <-env.callbackCh:
		case <-time.After(10 * time.Second):
			// may not always get it if backend already detected
		}

		// RunOnce → reconciler sees ACTIVE + Failed
		err = env.reconciler.RunOnce(ctx)
		require.NoError(t, err)

		if i < 3 {
			// Should re-provision (FailCount < 3)
			select {
			case cb := <-env.callbackCh:
				assert.Equal(t, backend.CallbackStatusSuccess, cb.Status, "re-provision %d should succeed", i)
			case <-time.After(2 * time.Minute):
				t.Fatalf("timeout waiting for re-provision %d callback", i)
			}
			env.tracker.UntrackInFlight(leaseUUID)
		}
	}

	// After the 3rd kill, reconciler should have called CloseLeases
	mu.Lock()
	closed := closedLeases
	mu.Unlock()
	assert.Contains(t, closed, leaseUUID, "CloseLeases should have been called with the lease UUID")
}

func TestIntegration_Reconciler_OrphanCleanup(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-orphan-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseVisible := true
	var deprovisionedViaChain bool

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseVisible {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			deprovisionedViaChain = true
			mu.Unlock()
			return 1, []string{"txhash-close"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// Provision via reconciler
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	select {
	case cb := <-env.callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}
	env.tracker.UntrackInFlight(leaseUUID)

	// Verify container exists
	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)

	// Make the lease disappear from chain (simulating close/expiry)
	mu.Lock()
	leaseVisible = false
	mu.Unlock()

	// RunOnce → orphan detection should deprovision
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Verify container is removed
	require.Eventually(t, func() bool {
		c := inspectProvisionContainers(t, leaseUUID)
		return len(c) == 0
	}, 30*time.Second, 500*time.Millisecond, "orphaned container should be removed")

	// Verify ListProvisions is empty for this lease
	provisions, err := env.backend.ListProvisions(ctx)
	require.NoError(t, err)
	for _, p := range provisions {
		assert.NotEqual(t, leaseUUID, p.LeaseUUID, "orphaned provision should be removed")
	}

	_ = deprovisionedViaChain // tracked but not asserted (deprovision is on the backend, not close on chain)
}

func TestIntegration_Reconciler_MultiContainer_PartialKill_Recovers(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-multi-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 2, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 2, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// RunOnce → PENDING + not provisioned → start provisioning (quantity=2)
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision success callback")
	}
	env.tracker.UntrackInFlight(leaseUUID)

	// Verify 2 containers are running
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 2, "should have 2 containers after initial provision")

	// Transition lease to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// RunOnce → ACTIVE + Ready → no action (healthy)
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Kill one of the two containers
	killContainer(t, containers[0].ID)

	// Wait for backend to detect failure
	waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	// Drain the failure callback from recoverState
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status, "should receive failure callback from recoverState")
	case <-time.After(10 * time.Second):
		// recoverState may have already fired it; continue
	}

	// RunOnce → sees ACTIVE + Failed → re-provisions
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback from re-provision
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for re-provision success callback")
	}

	// Verify 2 containers are running again after recovery
	newContainers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, newContainers, 2, "should have 2 running containers after re-provision")

	// Verify provision is back to Ready
	info := getProvisionInfo(t, env.backend, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, info.Status)
}

func TestIntegration_Reconciler_PendingReady_Acknowledges(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-ack-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	var acknowledgedLeases []string

	// Chain always returns PENDING (simulating missed ack)
	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
			lease.State = billingtypes.LEASE_STATE_PENDING
			return []billingtypes.Lease{lease}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			acknowledgedLeases = append(acknowledgedLeases, leaseUUIDs...)
			mu.Unlock()
			return 1, []string{"txhash-ack"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// First RunOnce → PENDING + not provisioned → start provisioning
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback (container ready)
	select {
	case cb := <-env.callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}
	env.tracker.UntrackInFlight(leaseUUID)

	// Second RunOnce → chain still returns PENDING but backend has it Ready
	// → should acknowledge
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	mu.Lock()
	acked := acknowledgedLeases
	mu.Unlock()

	assert.Contains(t, acked, leaseUUID, "AcknowledgeLeases should have been called with the lease UUID")

	// Cleanup
	err = env.backend.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

// TestIntegration_Reconciler_DetectsFailureWithoutRecoverState is a regression
// test for the reconciler ↔ backend state refresh race condition.
//
// Without RefreshState, the reconciler reads stale in-memory state from
// ListProvisions and misses container failures that happened between
// recoverState cycles. With RefreshState called before ListProvisions,
// the reconciler always sees fresh Docker state.
func TestIntegration_Reconciler_DetectsFailureWithoutRecoverState(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-refresh-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chain.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
	}

	// Use a backend with ReconcileInterval=1h so the background recoverState
	// loop effectively never runs during this test. The only way the reconciler
	// can see fresh state is through RefreshState being called inline.
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 1 * time.Hour // disable background recoverState
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)

	store, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	inFlight := provisioner.NewInFlightTracker()
	tracker := &testReconcilerTracker{
		InFlightTracker: inFlight,
		store:           store,
	}

	providerUUID := "test-provider"
	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			ProviderUUID:           providerUUID,
			CallbackBaseURL:        callbackServer.URL,
			Interval:               1 * time.Hour,
			MaxReprovisionAttempts: 3,
		},
		mockChain,
		router,
		tracker,
	)
	require.NoError(t, err)

	// Store payload
	stored := store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// 1. RunOnce → PENDING + not provisioned → start provisioning
	err = reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision success callback")
	}
	tracker.UntrackInFlight(leaseUUID)

	// 2. Transition lease to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// 3. Kill the container via Docker API
	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	killContainer(t, containers[0].ID)

	// 4. Do NOT wait for recoverState — with ReconcileInterval=1h it won't run.
	//    Immediately call RunOnce. The reconciler's fetchAllProvisions calls
	//    RefreshState (which delegates to recoverState) before ListProvisions,
	//    so it should see the container is dead.

	// Drain any failure callback that RefreshState might send
	drainCallbacks := func() {
		for {
			select {
			case <-callbackCh:
			case <-time.After(1 * time.Second):
				return
			}
		}
	}

	err = reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// 5. The reconciler should have detected the failure (via RefreshState)
	//    and re-provisioned. Wait for the success callback.
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status,
			"reconciler should detect failure via RefreshState and re-provision")
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for re-provision success callback — " +
			"RefreshState may not be called before ListProvisions")
	}

	drainCallbacks()

	// 6. Verify the new container is running
	newContainers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, newContainers, "should have running container after re-provision")

	info := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, info.Status)
}
