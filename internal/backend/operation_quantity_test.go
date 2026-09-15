package backend

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateOperationQuantitiesBoundsEachItemAndAggregate(t *testing.T) {
	tests := []struct {
		name  string
		items []LeaseItem
	}{
		{name: "empty", items: nil},
		{name: "zero", items: []LeaseItem{{SKU: "small", ServiceName: "app", Quantity: 0}}},
		{name: "negative", items: []LeaseItem{{SKU: "small", ServiceName: "app", Quantity: -1}}},
		{name: "max int", items: []LeaseItem{{SKU: "small", ServiceName: "app", Quantity: math.MaxInt}}},
		{name: "aggregate", items: []LeaseItem{
			{SKU: "small", ServiceName: "app", Quantity: MaxOperationQuantity},
			{SKU: "small", ServiceName: "worker", Quantity: 1},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ValidateOperationQuantities(test.items)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrValidation)
		})
	}

	total, err := ValidateOperationQuantities([]LeaseItem{
		{SKU: "small", ServiceName: "app", Quantity: MaxOperationQuantity - 1},
		{SKU: "small", ServiceName: "worker", Quantity: 1},
	})
	require.NoError(t, err)
	assert.Equal(t, MaxOperationQuantity, total)
}
