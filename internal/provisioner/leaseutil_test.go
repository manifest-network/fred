package provisioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

func TestExtractLeaseItems_ServiceName(t *testing.T) {
	t.Run("items with service names", func(t *testing.T) {
		lease := &billingtypes.Lease{
			Items: []billingtypes.LeaseItem{
				{SkuUuid: "sku-1", Quantity: 1, ServiceName: "web"},
				{SkuUuid: "sku-2", Quantity: 2, ServiceName: "db"},
			},
		}
		items := ExtractLeaseItems(lease)
		require.Len(t, items, 2)
		assert.Equal(t, "web", items[0].ServiceName)
		assert.Equal(t, "sku-1", items[0].SKU)
		assert.Equal(t, 1, items[0].Quantity)
		assert.Equal(t, "db", items[1].ServiceName)
		assert.Equal(t, "sku-2", items[1].SKU)
		assert.Equal(t, 2, items[1].Quantity)
	})

	t.Run("items without service names", func(t *testing.T) {
		lease := &billingtypes.Lease{
			Items: []billingtypes.LeaseItem{
				{SkuUuid: "sku-1", Quantity: 3},
			},
		}
		items := ExtractLeaseItems(lease)
		require.Len(t, items, 1)
		assert.Empty(t, items[0].ServiceName)
	})

	t.Run("nil lease", func(t *testing.T) {
		assert.Nil(t, ExtractLeaseItems(nil))
	})

	t.Run("empty items", func(t *testing.T) {
		lease := &billingtypes.Lease{}
		assert.Nil(t, ExtractLeaseItems(lease))
	})
}

func TestBillingOnlySKUs_Covers(t *testing.T) {
	const billingOnly = "01a126cc-7edd-700d-9ac6-02ed34fa4bff"
	const deployable = "01a08b4f-fe71-7008-a853-c34efec3dc74"
	set := NewBillingOnlySKUs([]string{billingOnly})

	lease := func(skus ...string) *billingtypes.Lease {
		l := &billingtypes.Lease{}
		for _, sku := range skus {
			l.Items = append(l.Items, billingtypes.LeaseItem{SkuUuid: sku, Quantity: 1})
		}
		return l
	}

	assert.True(t, set.Covers(lease(billingOnly)), "a lease of billing-only items is billing-only")
	assert.True(t, set.Covers(lease(billingOnly, billingOnly)), "every item is billing-only")
	assert.False(t, set.Covers(lease(deployable)), "a deployable lease is not billing-only")
	assert.False(t, set.Covers(lease(billingOnly, deployable)),
		"a mixed lease takes the normal path, where the backend refuses the SKU it cannot serve")
	assert.False(t, set.Covers(lease()), "a lease with no items is not billing-only")
	assert.False(t, set.Covers(nil), "a missing lease is not billing-only")

	var none BillingOnlySKUs
	assert.False(t, none.Covers(lease(billingOnly)), "an unconfigured set covers nothing")
	assert.Nil(t, NewBillingOnlySKUs(nil), "an empty config list builds the empty set")
}
