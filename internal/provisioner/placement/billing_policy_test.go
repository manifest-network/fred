package placement

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const (
	testBillingOnlySKU   = "01a126cc-7edd-700d-9ac6-02ed34fa4bff"
	testBillingOnlyLease = "4c0b4a3e-6a1f-4f86-9d6b-2f6f1d3b9a10"
)

func testBillingOnlyPolicy(t testing.TB) *BillingPolicy {
	t.Helper()
	policy, err := NewBillingPolicy([]string{testBillingOnlySKU})
	require.NoError(t, err)
	return policy
}

func billingTestLease(skus ...string) *billingtypes.Lease {
	lease := &billingtypes.Lease{Uuid: testBillingOnlyLease, Tenant: "tenant-test", State: billingtypes.LEASE_STATE_PENDING}
	for _, sku := range skus {
		lease.Items = append(lease.Items, billingtypes.LeaseItem{SkuUuid: sku, Quantity: 1})
	}
	return lease
}

func TestNewBillingPolicy_Validation(t *testing.T) {
	_, err := NewBillingPolicy([]string{testBillingOnlySKU})
	require.NoError(t, err)

	empty, err := NewBillingPolicy(nil)
	require.NoError(t, err)
	_, deployable := empty.Classify(billingTestLease(testBillingOnlySKU)).Deployable()
	assert.True(t, deployable, "an empty list builds the explicit empty policy")

	for name, sku := range map[string]string{
		"uppercase":  "01A126CC-7EDD-700D-9AC6-02ED34FA4BFF",
		"braced":     "{" + testBillingOnlySKU + "}",
		"urn prefix": "urn:uuid:" + testBillingOnlySKU,
		"not a UUID": "casper-aoraki",
	} {
		t.Run(name+" is rejected", func(t *testing.T) {
			policy, err := NewBillingPolicy([]string{sku})
			require.ErrorContains(t, err, "lowercase canonical UUID")
			assert.Nil(t, policy)
		})
	}
	policy, err := NewBillingPolicy([]string{testBillingOnlySKU, testBillingOnlySKU})
	require.ErrorContains(t, err, "duplicate SKU")
	assert.Nil(t, policy)
}

func TestBillingPolicy_Classify(t *testing.T) {
	const deployableSKU = "01a08b4f-fe71-7008-a853-c34efec3dc74"
	policy := testBillingOnlyPolicy(t)

	billingOnly := policy.Classify(billingTestLease(testBillingOnlySKU, testBillingOnlySKU))
	assert.True(t, billingOnly.BillingOnly(), "every item is billing-only")
	_, ok := billingOnly.Deployable()
	assert.False(t, ok, "a billing-only lease is never deployable")

	for name, lease := range map[string]*billingtypes.Lease{
		"deployable":    billingTestLease(deployableSKU),
		"mixed":         billingTestLease(testBillingOnlySKU, deployableSKU),
		"without items": billingTestLease(),
	} {
		t.Run(name+" lease is deployable", func(t *testing.T) {
			class := policy.Classify(lease)
			assert.False(t, class.BillingOnly())
			deployable, ok := class.Deployable()
			require.True(t, ok)
			assert.Equal(t, lease.Uuid, deployable.Lease().Uuid)
		})
	}

	var none *BillingPolicy
	for name, class := range map[string]LeaseClass{
		"nil policy":         none.Classify(billingTestLease(deployableSKU)),
		"zero policy":        (&BillingPolicy{}).Classify(billingTestLease(deployableSKU)),
		"nil lease":          policy.Classify(nil),
		"lease without uuid": policy.Classify(&billingtypes.Lease{Items: billingTestLease(deployableSKU).Items}),
	} {
		t.Run(name+" is neither", func(t *testing.T) {
			assert.False(t, class.BillingOnly(), "nothing unclassified is acknowledged")
			_, ok := class.Deployable()
			assert.False(t, ok, "nothing unclassified is deployed")
		})
	}
}

func TestDeployableLease_IsDetachedFromTheCallersLease(t *testing.T) {
	lease := billingTestLease("sku-test")
	deployable, ok := NoBillingOnlySKUs().Classify(lease).Deployable()
	require.True(t, ok)

	lease.Items[0].SkuUuid = testBillingOnlySKU
	lease.Uuid = "lease-edited"

	got := deployable.Lease()
	assert.Equal(t, testBillingOnlyLease, got.Uuid)
	assert.Equal(t, "sku-test", got.Items[0].SkuUuid, "editing the caller's lease cannot change what was classified")
	got.Items[0].SkuUuid = "edited"
	assert.Equal(t, "sku-test", deployable.Lease().Items[0].SkuUuid, "the accessor returns a copy")
}

func TestBillingPolicy_SKUs(t *testing.T) {
	const otherSKU = "00f4c1de-1b2a-4c3d-8e9f-0a1b2c3d4e5f"
	policy, err := NewBillingPolicy([]string{testBillingOnlySKU, otherSKU})
	require.NoError(t, err)

	skus, ok := policy.SKUs()
	require.True(t, ok)
	assert.Equal(t, []string{otherSKU, testBillingOnlySKU}, skus, "listed in ascending order")
	skus[0] = "edited"
	again, _ := policy.SKUs()
	assert.Equal(t, []string{otherSKU, testBillingOnlySKU}, again, "each call returns a fresh copy")

	none, ok := NoBillingOnlySKUs().SKUs()
	require.True(t, ok)
	assert.NotNil(t, none, "the explicit empty policy lists no SKUs, not an absent list")
	assert.Empty(t, none)

	for name, invalid := range map[string]*BillingPolicy{"nil": nil, "zero value": {}} {
		t.Run(name+" policy has no list", func(t *testing.T) {
			skus, ok := invalid.SKUs()
			assert.False(t, ok)
			assert.Nil(t, skus)
		})
	}
}

func TestBindBackendRuntime_RequiresBillingPolicy(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, testBackendStorageIDs("backend-a"),
	))
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	runtime := newExecutionTestRuntime(&executionTestBackend{name: "backend-a"})
	control := &testProviderControlPlane{provider: store.providerUUID}

	// No implicit default: a provider without billing-only SKUs binds
	// NoBillingOnlySKUs() explicitly.
	_, err = base.BindBackendRuntime(runtime, control, nil)
	require.ErrorContains(t, err, "billing policy is required")
	_, err = base.BindBackendRuntime(runtime, control, &BillingPolicy{})
	require.ErrorContains(t, err, "billing policy is required")
	execution, err := base.BindBackendRuntime(runtime, control, NoBillingOnlySKUs())
	require.NoError(t, err)
	assert.True(t, execution.Valid())
}

// billingOnlyTestControl is the provider control plane for the billing-only
// event tests. It serves one lease, records acknowledgements, and fails the
// test on any rejection.
type billingOnlyTestControl struct {
	t *testing.T

	mu       sync.Mutex
	state    billingtypes.LeaseState
	metaHash []byte
	skus     []string
	acked    []string
	// ackErr fails Acknowledge; activateOnError still makes the lease ACTIVE,
	// modeling an RPC error after a committed acknowledgement.
	ackErr          error
	activateOnError bool
}

func (control *billingOnlyTestControl) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	lease := &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: control.state, MetaHash: control.metaHash,
	}
	for _, sku := range control.skus {
		lease.Items = append(lease.Items, billingtypes.LeaseItem{SkuUuid: sku, Quantity: 1})
	}
	return lease, nil
}

func (control *billingOnlyTestControl) Acknowledge(_ context.Context, leaseUUID string) (bool, string, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.acked = append(control.acked, leaseUUID)
	if control.ackErr != nil {
		if control.activateOnError {
			control.state = billingtypes.LEASE_STATE_ACTIVE
		}
		return false, "", control.ackErr
	}
	control.state = billingtypes.LEASE_STATE_ACTIVE
	return true, "tx-ack", nil
}

func (control *billingOnlyTestControl) RejectLeases(_ context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	control.t.Errorf("billing-only lease rejected on chain: %v (%s)", leaseUUIDs, reason)
	return 0, nil, nil
}

func (control *billingOnlyTestControl) acknowledged() []string {
	control.mu.Lock()
	defer control.mu.Unlock()
	return append([]string(nil), control.acked...)
}

// newBillingOnlyTestCoordinator binds a provision coordinator whose backend
// counts provision calls; the backend refuses nothing, so a billing-only
// lease reaching it would be visible as a call.
func newBillingOnlyTestCoordinator(
	t *testing.T, control *billingOnlyTestControl,
) (*ProvisionCoordinator, *Store, *int32) {
	t.Helper()
	execution, store, calls := newBillingOnlyTestExecution(t, control)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	return authority, store, calls
}

// newBillingOnlyTestExecution binds the billing-only test runtime and control.
func newBillingOnlyTestExecution(
	t *testing.T, control *billingOnlyTestControl,
) (*ExecutionCoordinator, *Store, *int32) {
	t.Helper()
	routes, err := NewCallbackRouteFactory("https://callbacks.provider.test")
	require.NoError(t, err)
	store := newTestStore(t, WithCallbackRouteFactory(routes))
	requireAdmissionBaseline(t, store, "backend-a")
	registry := operation.NewRegistry()
	base, err := newOperationCoordinator(store, registry)
	require.NoError(t, err)
	var mu sync.Mutex
	calls := new(int32)
	client := &executionTestBackend{name: "backend-a", provision: func(context.Context, backend.ProvisionRequest) error {
		mu.Lock()
		defer mu.Unlock()
		*calls++
		return nil
	}}
	execution := bindExecutionWithPolicyForTest(t, base, newExecutionTestRuntime(client), testBillingOnlyPolicy(t))
	setProviderControlPlaneForTest(t, execution, control)
	return execution, store, calls
}

// requireLeaseClaimReleased proves the event left no lifecycle claim behind.
func requireLeaseClaimReleased(t *testing.T, authority *ProvisionCoordinator, leaseUUID string) {
	t.Helper()
	claim := authority.coordinator.operations.TryClaimLeaseNow(leaseUUID)
	require.True(t, claim.Acquired(), "the lease claim is released")
	require.True(t, authority.coordinator.operations.ReleaseLease(claim.Claim()))
}

func TestExecuteCurrentLease_BillingOnlyLeaseIsAcknowledgedNotProvisioned(t *testing.T) {
	for name, metaHash := range map[string][]byte{
		"without meta_hash": nil,
		"with meta_hash":    {0x01, 0x02},
	} {
		t.Run(name, func(t *testing.T) {
			control := &billingOnlyTestControl{
				t: t, state: billingtypes.LEASE_STATE_PENDING, metaHash: metaHash,
				skus: []string{testBillingOnlySKU},
			}
			authority, store, calls := newBillingOnlyTestCoordinator(t, control)
			request, err := NewProvisionEventRequest(testBillingOnlyLease, "tenant-test")
			require.NoError(t, err)

			result := authority.ExecuteCurrentLease(t.Context(), request)

			require.Equal(t, ProvisionEventBillingOnlyAcknowledged, result.Disposition(), result.Err())
			state, ok := result.LeaseState()
			require.True(t, ok)
			assert.Equal(t, billingtypes.LEASE_STATE_ACTIVE, state)
			assert.Equal(t, []string{testBillingOnlyLease}, control.acknowledged(), "acknowledged once so billing starts")
			assert.Zero(t, *calls, "a billing-only lease is never provisioned")
			assert.Equal(t, StateAbsent, store.Lookup(testBillingOnlyLease).State(), "a billing-only lease is never placed")
			requireLeaseClaimReleased(t, authority, testBillingOnlyLease)
		})
	}
}

func TestExecuteCurrentLease_BillingOnlyAcknowledgeError(t *testing.T) {
	t.Run("a committed acknowledgement is confirmed by a fresh read", func(t *testing.T) {
		control := &billingOnlyTestControl{
			t: t, state: billingtypes.LEASE_STATE_PENDING, skus: []string{testBillingOnlySKU},
			ackErr: errors.New("rpc timeout after broadcast"), activateOnError: true,
		}
		authority, _, calls := newBillingOnlyTestCoordinator(t, control)
		request, err := NewProvisionEventRequest(testBillingOnlyLease, "tenant-test")
		require.NoError(t, err)

		result := authority.ExecuteCurrentLease(t.Context(), request)

		assert.Equal(t, ProvisionEventBillingOnlyAcknowledged, result.Disposition(), result.Err())
		assert.Zero(t, *calls)
	})

	t.Run("a lease still pending stays uncertain for a retry", func(t *testing.T) {
		control := &billingOnlyTestControl{
			t: t, state: billingtypes.LEASE_STATE_PENDING, skus: []string{testBillingOnlySKU},
			ackErr: errors.New("sequence mismatch"),
		}
		authority, _, calls := newBillingOnlyTestCoordinator(t, control)
		request, err := NewProvisionEventRequest(testBillingOnlyLease, "tenant-test")
		require.NoError(t, err)

		result := authority.ExecuteCurrentLease(t.Context(), request)

		assert.Equal(t, ProvisionEventUncertain, result.Disposition())
		require.ErrorContains(t, result.Err(), "sequence mismatch")
		assert.Zero(t, *calls, "an acknowledgement failure never falls through to provisioning")
		requireLeaseClaimReleased(t, authority, testBillingOnlyLease)
	})
}

func TestExecuteCurrentLease_MixedLeaseIsProvisioned(t *testing.T) {
	control := &billingOnlyTestControl{
		t: t, state: billingtypes.LEASE_STATE_PENDING,
		skus: []string{"sku-test", testBillingOnlySKU},
	}
	authority, _, calls := newBillingOnlyTestCoordinator(t, control)
	request, err := NewProvisionEventRequest(testBillingOnlyLease, "tenant-test")
	require.NoError(t, err)

	result := authority.ExecuteCurrentLease(t.Context(), request)

	assert.Equal(t, ProvisionEventStarted, result.Disposition(), result.Err())
	assert.Equal(t, int32(1), *calls, "one deployable item makes the lease deployable")
	assert.Empty(t, control.acknowledged(), "a mixed lease is acknowledged only after it is provisioned")
}

func TestRestoreApplication_RefusesBillingOnlyTarget(t *testing.T) {
	const sourceLease = "71638ef8-1401-4f14-a355-1ae02afeb35b"
	const targetLease = "81638ef8-1401-4f14-a355-1ae02afeb35b"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{sourceLease: "backend-a"},
	})
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	var calls atomic.Int32
	// The backend would accept the restore: the billing-only refusal is
	// Fred's own, never left to the backend.
	client := &executionTestBackend{name: "backend-a", restore: func(context.Context, backend.RestoreRequest) error {
		calls.Add(1)
		return nil
	}}
	execution := bindExecutionWithPolicyForTest(t, base, newExecutionTestRuntime(client), testBillingOnlyPolicy(t))
	setProviderControlPlaneForTest(t, execution, executionLeaseReaderFunc(func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		state := billingtypes.LEASE_STATE_PENDING
		if leaseUUID == sourceLease {
			state = billingtypes.LEASE_STATE_CLOSED
		}
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID, State: state,
			Items: []billingtypes.LeaseItem{{SkuUuid: testBillingOnlySKU, Quantity: 1}},
		}, nil
	}))
	authority, err := execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	request, err := NewRestoreApplicationRequest(targetLease, "tenant-test", sourceLease)
	require.NoError(t, err)

	result := authority.ExecuteApplication(t.Context(), request)

	assert.Equal(t, RestoreApplicationTargetBillingOnly, result.Disposition(), result.Err())
	assert.Zero(t, calls.Load(), "a billing-only lease never reaches the backend")
	assert.Equal(t, StateAbsent, store.Lookup(targetLease).State(), "nothing is admitted for the target")
}

func TestBillingPolicy_PermitsRequestItems(t *testing.T) {
	item := func(sku string) backend.LeaseItem { return backend.LeaseItem{SKU: sku, Quantity: 1} }
	policy := testBillingOnlyPolicy(t)
	assert.False(t, policy.permits([]backend.LeaseItem{item(testBillingOnlySKU)}), "every SKU is billing-only")
	assert.True(t, policy.permits([]backend.LeaseItem{item(testBillingOnlySKU), item("sku-deployable")}), "a mixed request")
	assert.True(t, policy.permits(nil), "request validation, not the policy, refuses a request without items")
	assert.True(t, NoBillingOnlySKUs().permits([]backend.LeaseItem{item(testBillingOnlySKU)}))
	var unbound *BillingPolicy
	assert.False(t, unbound.permits([]backend.LeaseItem{item("sku-deployable")}), "a nil policy permits nothing")
	assert.False(t, (&BillingPolicy{}).permits([]backend.LeaseItem{item("sku-deployable")}), "a zero policy permits nothing")
}

// TestAttestation_RefusesRequestsTheCurrentPolicyMakesBillingOnly pins the
// send-time check that invokeProvision and invokeRestore depend on: a request
// is judged by the policy bound now, whatever policy minted its snapshot.
func TestAttestation_RefusesRequestsTheCurrentPolicyMakesBillingOnly(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	billingOnly := []backend.LeaseItem{{SKU: testBillingOnlySKU, Quantity: 1}}
	deployable := []backend.LeaseItem{{SKU: "sku-deployable", Quantity: 1}}

	_, err = base.attestProvision(backend.ProvisionRequest{LeaseUUID: testBillingOnlyLease, Items: deployable})
	require.ErrorContains(t, err, "no billing policy is bound", "nothing is attested before a runtime is bound")

	bindExecutionWithPolicyForTest(t, base,
		newExecutionTestRuntime(&executionTestBackend{name: "backend-a"}), testBillingOnlyPolicy(t))
	_, err = base.attestProvision(backend.ProvisionRequest{LeaseUUID: testBillingOnlyLease, Items: billingOnly})
	require.ErrorIs(t, err, errNotDeployable)
	_, err = base.attestRestore(backend.RestoreRequest{LeaseUUID: testBillingOnlyLease, Items: billingOnly})
	require.ErrorIs(t, err, errNotDeployable)
	provision, err := base.attestProvision(backend.ProvisionRequest{LeaseUUID: testBillingOnlyLease, Items: deployable})
	require.NoError(t, err)
	assert.Equal(t, testBillingOnlyLease, provision.request.LeaseUUID)
	restore, err := base.attestRestore(backend.RestoreRequest{LeaseUUID: testBillingOnlyLease, Items: deployable})
	require.NoError(t, err)
	assert.Equal(t, testBillingOnlyLease, restore.request.LeaseUUID)
}

// TestProviderControlPlane_NeverEndsABillingOnlyLease pins the choke point
// every placement rejection and close goes through.
func TestProviderControlPlane_NeverEndsABillingOnlyLease(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionWithPolicyForTest(t, base,
		newExecutionTestRuntime(&executionTestBackend{name: "backend-a"}), testBillingOnlyPolicy(t))
	control := execution.controlPlane

	_, err = control.mayEnd(billingTestLease(testBillingOnlySKU))
	require.ErrorIs(t, err, errBillingOnlyLeaseNotEnded)
	mixed, err := control.mayEnd(billingTestLease(testBillingOnlySKU, "sku-deployable"))
	require.NoError(t, err, "a lease with a deployable SKU is Fred's to end")
	assert.Equal(t, testBillingOnlyLease, mixed.uuid)
	_, err = control.mayEnd(billingTestLease())
	require.NoError(t, err, "a lease without items is ended as before")
	_, err = control.mayEnd(nil)
	require.Error(t, err)
	_, _, err = control.rejectLease(t.Context(), endableLease{}, "reason")
	require.Error(t, err, "the zero value ends nothing")
	_, _, err = control.closeLease(t.Context(), endableLease{}, "reason")
	require.Error(t, err, "the zero value ends nothing")
}

// requireInventoryRecoveryPending leaves the store with inventory evidence
// that no sweep has projected yet, the state that holds back every action
// gated on backend inventory.
func requireInventoryRecoveryPending(t *testing.T, store *Store) {
	t.Helper()
	store.mu.Lock()
	store.setInventoryRecoveryRequiredLocked(true)
	store.mu.Unlock()
	t.Cleanup(func() {
		store.mu.Lock()
		store.setInventoryRecoveryRequiredLocked(false)
		store.mu.Unlock()
	})
	require.ErrorIs(t, store.leaseSideEffectError(testBillingOnlyLease), ErrUnprojectedInventoryPositive,
		"an inventory-gated action would be held back")
}

// TestAcknowledgeBillingOnly pins the reconciler's billing-only acknowledgment:
// it acts on an authoritative chain read alone, never on backend inventory,
// and only for a lease that read shows PENDING and billing-only.
func TestAcknowledgeBillingOnly(t *testing.T) {
	pending := func(t *testing.T, skus ...string) *billingOnlyTestControl {
		return &billingOnlyTestControl{t: t, state: billingtypes.LEASE_STATE_PENDING, skus: skus}
	}
	bind := func(t *testing.T, control *billingOnlyTestControl) (*ReconciliationCoordinator, *Store) {
		t.Helper()
		execution, store, calls := newBillingOnlyTestExecution(t, control)
		t.Cleanup(func() { assert.Zero(t, *calls, "a billing-only lease is never provisioned") })
		authority, err := execution.ReconciliationCoordinator(nil, nil)
		require.NoError(t, err)
		return authority, store
	}
	requireReleased := func(t *testing.T, authority *ReconciliationCoordinator) {
		t.Helper()
		claim := authority.coordinator.operations.TryClaimLeaseNow(testBillingOnlyLease)
		require.True(t, claim.Acquired(), "the lease claim is released")
		require.True(t, authority.coordinator.operations.ReleaseLease(claim.Claim()))
	}

	t.Run("a pending billing-only lease is acknowledged despite unprojected inventory", func(t *testing.T) {
		control := pending(t, testBillingOnlySKU)
		authority, store := bind(t, control)
		requireInventoryRecoveryPending(t, store)

		outcome, err := authority.AcknowledgeBillingOnly(t.Context(), testBillingOnlyLease)

		require.NoError(t, err)
		assert.Equal(t, BillingOnlyAcknowledged, outcome)
		assert.Equal(t, []string{testBillingOnlyLease}, control.acknowledged())
		requireReleased(t, authority)
	})

	t.Run("a deployable lease is never acknowledged ahead of its workload", func(t *testing.T) {
		control := pending(t, testBillingOnlySKU, "sku-test")
		authority, _ := bind(t, control)

		outcome, err := authority.AcknowledgeBillingOnly(t.Context(), testBillingOnlyLease)

		require.ErrorContains(t, err, "not billing-only")
		assert.Equal(t, BillingOnlyAckFailed, outcome)
		assert.Empty(t, control.acknowledged())
		requireReleased(t, authority)
	})

	t.Run("a lease no longer pending is settled", func(t *testing.T) {
		control := &billingOnlyTestControl{t: t, state: billingtypes.LEASE_STATE_ACTIVE, skus: []string{testBillingOnlySKU}}
		authority, _ := bind(t, control)

		outcome, err := authority.AcknowledgeBillingOnly(t.Context(), testBillingOnlyLease)

		require.NoError(t, err)
		assert.Equal(t, BillingOnlyAckSettled, outcome)
		assert.Empty(t, control.acknowledged())
	})

	t.Run("a held lifecycle claim defers it", func(t *testing.T) {
		control := pending(t, testBillingOnlySKU)
		authority, _ := bind(t, control)
		held := authority.coordinator.operations.TryClaimLeaseNow(testBillingOnlyLease)
		require.True(t, held.Acquired())

		outcome, err := authority.AcknowledgeBillingOnly(t.Context(), testBillingOnlyLease)

		require.NoError(t, err)
		assert.Equal(t, BillingOnlyAckBusy, outcome)
		assert.Empty(t, control.acknowledged())
		require.True(t, authority.coordinator.operations.ReleaseLease(held.Claim()))
	})

	t.Run("a committed acknowledgment is confirmed by a fresh read", func(t *testing.T) {
		control := pending(t, testBillingOnlySKU)
		control.ackErr, control.activateOnError = errors.New("rpc timeout after broadcast"), true
		authority, _ := bind(t, control)

		outcome, err := authority.AcknowledgeBillingOnly(t.Context(), testBillingOnlyLease)

		require.NoError(t, err)
		assert.Equal(t, BillingOnlyAcknowledged, outcome)
	})

	t.Run("an acknowledgment that did not commit fails for a retry", func(t *testing.T) {
		control := pending(t, testBillingOnlySKU)
		control.ackErr = errors.New("sequence mismatch")
		authority, _ := bind(t, control)

		outcome, err := authority.AcknowledgeBillingOnly(t.Context(), testBillingOnlyLease)

		require.ErrorContains(t, err, "sequence mismatch")
		assert.Equal(t, BillingOnlyAckFailed, outcome)
		requireReleased(t, authority)
	})
}

// TestExecuteCurrentLease_BillingOnlyIgnoresUnprojectedInventory pins the event
// path to the same rule: no inventory evidence holds a billing-only
// acknowledgment back.
func TestExecuteCurrentLease_BillingOnlyIgnoresUnprojectedInventory(t *testing.T) {
	control := &billingOnlyTestControl{
		t: t, state: billingtypes.LEASE_STATE_PENDING, skus: []string{testBillingOnlySKU},
	}
	authority, store, calls := newBillingOnlyTestCoordinator(t, control)
	requireInventoryRecoveryPending(t, store)
	request, err := NewProvisionEventRequest(testBillingOnlyLease, "tenant-test")
	require.NoError(t, err)

	result := authority.ExecuteCurrentLease(t.Context(), request)

	require.Equal(t, ProvisionEventBillingOnlyAcknowledged, result.Disposition(), result.Err())
	assert.Equal(t, []string{testBillingOnlyLease}, control.acknowledged())
	assert.Zero(t, *calls)
	requireLeaseClaimReleased(t, authority, testBillingOnlyLease)
}
