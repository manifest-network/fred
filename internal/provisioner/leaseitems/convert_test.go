package leaseitems

import (
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
)

func TestFromLeaseCopiesCompleteBackendShape(t *testing.T) {
	assert.Nil(t, FromLease(nil))
	assert.Nil(t, FromLease(&billingtypes.Lease{}))

	lease := &billingtypes.Lease{Items: []billingtypes.LeaseItem{
		{
			SkuUuid:      "sku-a",
			Quantity:     2,
			ServiceName:  "api",
			CustomDomain: "api.example.test",
		},
	}}
	items := FromLease(lease)
	if assert.Len(t, items, 1) {
		assert.Equal(t, "sku-a", items[0].SKU)
		assert.Equal(t, 2, items[0].Quantity)
		assert.Equal(t, "api", items[0].ServiceName)
		assert.Equal(t, "api.example.test", items[0].CustomDomain)
	}
}
