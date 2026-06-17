//go:build integration

package docker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// integrationAcknowledger wraps a chain client as an Acknowledger for integration tests.
type integrationAcknowledger struct {
	chainClient provisioner.ReconcilerChainClient
}

func (a *integrationAcknowledger) Acknowledge(ctx context.Context, leaseUUID string) (bool, string, error) {
	n, hashes, err := a.chainClient.AcknowledgeLeases(ctx, []string{leaseUUID})
	if err != nil {
		return false, "", err
	}
	var hash string
	if len(hashes) > 0 {
		hash = hashes[0]
	}
	return n > 0, hash, nil
}

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
	chainClient  *chaintest.MockClient
	callbackCh   <-chan backend.CallbackPayload
	callbackURL  string
	providerUUID string
}

// testReconcilerSetup creates a full-stack test environment:
// real docker backend + reconciler + mock chain + tracker + payload store.
func testReconcilerSetup(t *testing.T, chainClient *chaintest.MockClient, extraCfg ...func(*Config)) *reconcilerTestEnv {
	t.Helper()

	callbackServer, callbackCh := startCallbackServer(t)

	// Create a backend with fast reconcile for detection
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 2 * time.Second
		for _, fn := range extraCfg {
			fn(cfg)
		}
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

	// Create a simple acknowledger that delegates to chainClient for integration tests
	integrationAck := &integrationAcknowledger{chainClient: chainClient}

	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			ProviderUUID:           providerUUID,
			CallbackBaseURL:        callbackServer.URL,
			Interval:               1 * time.Hour, // manual RunOnce only
			MaxReprovisionAttempts: 3,
		},
		chainClient,
		integrationAck,
		router,
		tracker,
		nil,
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
	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	// Set up mock chain state - starts as PENDING
	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
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

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING
	var closedLeases []string

	mockChain := &chaintest.MockClient{
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

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseVisible := true
	var deprovisionedViaChain bool

	mockChain := &chaintest.MockClient{
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

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
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

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	var acknowledgedLeases []string

	// Chain always returns PENDING (simulating missed ack)
	mockChain := &chaintest.MockClient{
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
// recoverState cycles. With RefreshState called before ListProvisions, every
// reconciler tick re-lists live Docker state and hands a crashed container to
// the lease SM — so the failure is detected and re-provisioned even with the
// background recoverState loop disabled. Detection and re-provision span more
// than one tick: recoverState keeps the provision Ready and fires
// containerDiedMsg, but the SM's Ready→Failing→Failed transition is async
// (diagnostics are gathered off-actor), so the ListProvisions in that same tick
// can still observe Ready and skip re-provision. The assertion below therefore
// drives RunOnce on an interval — mirroring the production reconciler, which
// self-heals on its next tick — instead of betting on a single tick.
func TestIntegration_Reconciler_DetectsFailureWithoutRecoverState(t *testing.T) {
	leaseUUID := fmt.Sprintf("recon-refresh-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	manifestPayload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(manifestPayload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
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
	integrationAck2 := &integrationAcknowledger{chainClient: mockChain}
	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			ProviderUUID:           providerUUID,
			CallbackBaseURL:        callbackServer.URL,
			Interval:               1 * time.Hour,
			MaxReprovisionAttempts: 3,
		},
		mockChain,
		integrationAck2,
		router,
		tracker,
		nil,
	)
	require.NoError(t, err)

	// Store payload
	stored := store.Store(leaseUUID, manifestPayload)
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
	waitForContainerExited(t, containers[0].ID)

	// 4. Do NOT wait for the background recoverState loop (ReconcileInterval=1h,
	//    so it never fires). Drive the reconciler manually instead. Each RunOnce
	//    calls RefreshState (→ recoverState) before ListProvisions, so it re-lists
	//    live Docker state and hands the dead container to the lease SM.

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

	// 5. The reconciler re-provisions the crashed container; wait for the success
	//    callback. Detection and re-provision are NOT atomic within one RunOnce:
	//    RefreshState fires containerDiedMsg, but the SM's Ready→Failing→Failed
	//    transition is asynchronous, so the ListProvisions in that same tick can
	//    still observe a Ready provision and skip re-provision. In production the
	//    reconciler runs on an interval and self-heals on the next tick, so model
	//    that here by ticking RunOnce until the provision is re-provisioned —
	//    rather than betting on a single tick winning the race (which flaked on
	//    CI). Failed callbacks emitted while the SM settles are skipped.
	successTimeout := time.After(2 * time.Minute)
	retick := time.NewTicker(2 * time.Second)
	defer retick.Stop()
	gotSuccess := false
	for !gotSuccess {
		select {
		case cb := <-callbackCh:
			if cb.Status == backend.CallbackStatusSuccess {
				assert.Equal(t, leaseUUID, cb.LeaseUUID)
				gotSuccess = true
			}
		case <-retick.C:
			require.NoError(t, reconciler.RunOnce(ctx))
		case <-successTimeout:
			t.Fatal("timeout waiting for re-provision success callback after repeated reconciler ticks")
		}
	}

	drainCallbacks()

	// 6. Verify the new container is running
	newContainers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, newContainers, "should have running container after re-provision")

	info := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, info.Status)
}

func TestIntegration_Reconciler_RetainRestoreLifecycle(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := fmt.Sprintf("recon-retain-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	const sku = "docker-small" // stateful: DiskMB=1024

	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)
	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseVisible := true // flip false to simulate the on-chain close/auto-close
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(_ context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseVisible {
				l := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				l.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{l}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) { return nil, nil },
		AcknowledgeLeasesFunc:         func(_ context.Context, _ []string) (uint64, []string, error) { return 1, []string{"tx"}, nil },
	}

	env := testReconcilerSetup(t, mockChain, func(cfg *Config) {
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0 // reaper OFF: assert soft-delete persists
		cfg.RetentionReapInterval = 0
	})
	require.True(t, env.tracker.store.Store(leaseUUID, payload))
	ctx := context.Background()

	// 1. Chain-driven provision.
	require.NoError(t, env.reconciler.RunOnce(ctx))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, env.callbackCh, leaseUUID, 3*time.Minute).Status)
	env.tracker.UntrackInFlight(leaseUUID)

	// 2. Sentinel into the managed volume.
	cid := getContainerID(t, leaseUUID)
	require.True(t, containerHasBindMount(t, cid, "/data"))
	execInContainer(t, cid, []string{"sh", "-c", "echo recon-restore-sentinel > /data/sentinel.txt"})

	// 3. Lease vanishes from chain (auto-close / credit exhaustion) → orphan cleanup soft-deletes.
	mu.Lock()
	leaseVisible = false
	mu.Unlock()
	require.NoError(t, env.reconciler.RunOnce(ctx))

	retainedPath := filepath.Join(mountPath, retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)))
	require.Eventually(t, func() bool {
		rec, _ := env.backend.retentionStore.Get(leaseUUID)
		if rec == nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return statErr == nil
	}, 60*time.Second, 500*time.Millisecond, "reconciler orphan cleanup must soft-delete (retained volume + record)")
	data, err := os.ReadFile(filepath.Join(retainedPath, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "recon-restore-sentinel")

	// 4. Restore into a NEW lease.
	newLease := fmt.Sprintf("recon-restore-new-%d", time.Now().UnixNano())
	require.NoError(t, env.backend.Restore(ctx, backend.RestoreRequest{
		LeaseUUID: newLease, FromLeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: env.providerUUID,
		Items:       []backend.LeaseItem{{SKU: sku, Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		CallbackURL: env.callbackURL,
	}))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, env.callbackCh, newLease, 3*time.Minute).Status)

	// 5. Sentinel survived; record gone.
	out := execInContainer(t, getContainerID(t, newLease), []string{"cat", "/data/sentinel.txt"})
	assert.Contains(t, out, "recon-restore-sentinel")
	rec, err := env.backend.retentionStore.Get(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, rec)

	env.backend.cfg.RetainOnClose = false
	require.NoError(t, env.backend.Deprovision(ctx, newLease))
}

// testManagerSetup wires a real provisioner.Manager to the real docker backend +
// mock chain, for exercising the event-driven close/expiry deprovision path.
//
// NOTE: this deliberately collapses the production transport seam — prod wires the
// Manager to backends via backend.NewHTTPClient against the docker-backend process
// (cmd/providerd/main.go), whereas this injects the in-process *docker.Backend
// directly. It exercises the provisioner→backend LIFECYCLE logic, not the HTTP wire
// or HMAC auth (those are unit-tested separately).
func testManagerSetup(t *testing.T, mockChain *chaintest.MockClient, extraCfg ...func(*Config)) (*provisioner.Manager, *Backend, <-chan backend.CallbackPayload, string) {
	t.Helper()
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		for _, fn := range extraCfg {
			fn(cfg)
		}
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: b, IsDefault: true}},
	})
	require.NoError(t, err)
	mgr, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:    "test-provider",
		CallbackBaseURL: callbackServer.URL,
	}, router, mockChain)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = mgr.Close() })
	go func() { _ = mgr.Start(ctx) }()
	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for manager to start")
	}
	return mgr, b, callbackCh, callbackServer.URL
}

func TestIntegration_Manager_CloseEvent_RealSoftDelete(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := fmt.Sprintf("mgr-close-%d", time.Now().UnixNano())
	tenant := "test-tenant"
	const sku = "docker-small"
	const providerUUID = "test-provider"

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return chaintest.NewMockLeaseWithSKU(uuid, tenant, providerUUID, billingtypes.LEASE_STATE_ACTIVE, sku), nil
		},
	}
	mgr, b, callbackCh, callbackURL := testManagerSetup(t, mockChain, func(cfg *Config) {
		cfg.VolumeDataPath = mountPath // REQUIRED: empty would zero docker-small's DiskMB → stateless → nothing to retain
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0
		cfg.RetentionReapInterval = 0
	})
	ctx := context.Background()

	// Provision directly on the backend. This exercises the SKU-hint / default-route
	// close path (orchestrator Case 2): the lease is NOT Manager-tracked and has no
	// placement, mirroring a post-restart close of an already-active lease.
	payload, err := json.Marshal(manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}})
	require.NoError(t, err)
	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: providerUUID,
		Items: []backend.LeaseItem{{SKU: sku, Quantity: 1}}, CallbackURL: callbackURL, Payload: payload,
	}))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute).Status)

	execInContainer(t, getContainerID(t, leaseUUID), []string{"sh", "-c", "echo mgr-event-sentinel > /data/sentinel.txt"})

	// Drive the on-chain close as an event through the real Manager. LeaseExpired is
	// intentionally not a second btrfs run — HandleLeaseExpired calls processLeaseClose
	// verbatim (handler_set.go:126-136), so this also covers the expiry route.
	require.NoError(t, mgr.PublishLeaseEvent(chain.LeaseEvent{
		Type: chain.LeaseClosed, LeaseUUID: leaseUUID, ProviderUUID: providerUUID, Tenant: tenant,
	}))

	// The KEY assertion: the event route reached a REAL soft-delete (mock backends can't).
	retainedPath := filepath.Join(mountPath, retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)))
	require.Eventually(t, func() bool {
		rec, _ := b.retentionStore.Get(leaseUUID)
		if rec == nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return statErr == nil
	}, 60*time.Second, 500*time.Millisecond, "LeaseClosed event must reach a real soft-delete (retained volume + record)")
	data, err := os.ReadFile(filepath.Join(retainedPath, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "mgr-event-sentinel")
	// Retained volume + bbolt DB live under the loopback mount / t.TempDir → torn down automatically.
}
