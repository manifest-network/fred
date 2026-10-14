package provisioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// testBillingOnlyPolicy builds the policy under which testBillingOnlySKU is
// billing-only.
func testBillingOnlyPolicy(t testing.TB) *BillingPolicy {
	t.Helper()
	policy, err := NewBillingPolicy([]string{testBillingOnlySKU})
	require.NoError(t, err)
	return policy
}

// mustDeployable classifies a lease under the explicit empty policy, the way
// every deployment path must before it can provision or restore.
func mustDeployable(t testing.TB, lease *billingtypes.Lease) DeployableLease {
	t.Helper()
	deployable, ok := NoBillingOnlySKUs().Classify(lease).Deployable()
	require.True(t, ok, "lease %s is not deployable", lease.GetUuid())
	return deployable
}

func testLease(skus ...string) *billingtypes.Lease {
	lease := &billingtypes.Lease{Uuid: "lease-1", Tenant: "tenant-a", State: billingtypes.LEASE_STATE_PENDING}
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
	_, deployable := empty.Classify(testLease(testBillingOnlySKU)).Deployable()
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

	billingOnly := policy.Classify(testLease(testBillingOnlySKU, testBillingOnlySKU))
	assert.True(t, billingOnly.BillingOnly(), "every item is billing-only")
	_, ok := billingOnly.Deployable()
	assert.False(t, ok, "a billing-only lease is never deployable")

	for name, lease := range map[string]*billingtypes.Lease{
		"deployable": testLease(deployableSKU),
		"mixed":      testLease(testBillingOnlySKU, deployableSKU),
	} {
		t.Run(name+" lease is deployable", func(t *testing.T) {
			class := policy.Classify(lease)
			assert.False(t, class.BillingOnly())
			deployable, ok := class.Deployable()
			require.True(t, ok)
			assert.Equal(t, lease.Uuid, deployable.Lease().Uuid)
		})
	}

	itemless, ok := policy.Classify(testLease()).Deployable()
	assert.True(t, ok, "a lease without items keeps the normal path, where the backend refuses it")
	assert.Equal(t, "lease-1", itemless.Lease().Uuid)

	var none *BillingPolicy
	for name, class := range map[string]LeaseClass{
		"nil policy":         none.Classify(testLease(deployableSKU)),
		"zero policy":        (&BillingPolicy{}).Classify(testLease(deployableSKU)),
		"nil lease":          policy.Classify(nil),
		"lease without uuid": policy.Classify(&billingtypes.Lease{Items: testLease(deployableSKU).Items}),
	} {
		t.Run(name+" is neither", func(t *testing.T) {
			assert.False(t, class.BillingOnly(), "nothing unclassified is acknowledged")
			_, ok := class.Deployable()
			assert.False(t, ok, "nothing unclassified is deployed")
		})
	}
}

func TestDeployableLease_IsDetachedFromTheCallersLease(t *testing.T) {
	lease := testLease("sku-1")
	deployable := mustDeployable(t, lease)

	lease.Items[0].SkuUuid = testBillingOnlySKU
	lease.Uuid = "lease-2"

	got := deployable.Lease()
	assert.Equal(t, "lease-1", got.Uuid)
	assert.Equal(t, "sku-1", got.Items[0].SkuUuid, "editing the caller's lease cannot change what was classified")
	got.Items[0].SkuUuid = "edited"
	assert.Equal(t, "sku-1", deployable.Lease().Items[0].SkuUuid, "the accessor returns a copy")
}

func TestDeploymentRequestsRequireAClassifiedLease(t *testing.T) {
	_, err := provisionRequestFor(DeployableLease{}, "provider-1", "http://cb", nil, "")
	require.ErrorIs(t, err, errNotDeployable)
	_, err = RestoreRequestFor(DeployableLease{}, "provider-1", "source-lease", "http://cb")
	require.ErrorIs(t, err, errNotDeployable)

	deployable := mustDeployable(t, testLease("sku-1"))
	provision, err := provisionRequestFor(deployable, "provider-1", "http://cb", []byte("payload"), "abc")
	require.NoError(t, err)
	assert.Equal(t, "lease-1", provision.LeaseUUID)
	assert.Equal(t, "tenant-a", provision.Tenant)
	assert.Equal(t, "abc", provision.PayloadHash)
	noPayload, err := provisionRequestFor(deployable, "provider-1", "http://cb", nil, "abc")
	require.NoError(t, err)
	assert.Empty(t, noPayload.PayloadHash, "a hash is never sent without its payload")

	restore, err := RestoreRequestFor(deployable, "provider-1", "source-lease", "http://cb")
	require.NoError(t, err)
	assert.Equal(t, "lease-1", restore.LeaseUUID)
	assert.Equal(t, "source-lease", restore.FromLeaseUUID)
	assert.Equal(t, "tenant-a", restore.Tenant)
	require.Len(t, restore.Items, 1)
	assert.Equal(t, "sku-1", restore.Items[0].SKU)
}
