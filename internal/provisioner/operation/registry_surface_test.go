package operation

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistryExportsOnlyOneShotSettlementBinding(t *testing.T) {
	typeOfRegistry := reflect.TypeFor[*Registry]()
	methods := make([]string, 0, typeOfRegistry.NumMethod())
	for index := range typeOfRegistry.NumMethod() {
		methods = append(methods, typeOfRegistry.Method(index).Name)
	}

	assert.Equal(t, []string{"BindSettlementAuthority"}, methods,
		"raw claim, initiation, lookup, settlement, and drain mechanics must remain package-private")
}
