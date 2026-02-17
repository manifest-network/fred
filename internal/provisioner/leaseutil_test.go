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
