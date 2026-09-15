package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteFactoryMintsOnlyItsValidatedLifecycleOrigin(t *testing.T) {
	factory, err := NewRouteFactory("https://fred.example/api?trace=a%2fb")
	require.NoError(t, err)
	id := mustTestID(t, canonicalTestID)

	route, err := factory.For(id)
	require.NoError(t, err)
	assert.True(t, route.Valid())
	assert.Equal(t, id, route.ID())
	assert.Equal(t,
		"https://fred.example/api/callbacks/provision?trace=a%2fb&lifecycle_id="+id.String(),
		route.URL(),
	)
}

func TestRouteFactoryAndRouteZeroValuesGrantNoDestination(t *testing.T) {
	var factory *RouteFactory
	assert.False(t, factory.Valid())
	_, err := factory.For(mustTestID(t, canonicalTestID))
	require.Error(t, err)

	var route Route
	assert.False(t, route.Valid())
	assert.False(t, route.ID().Valid())
	assert.Empty(t, route.URL())

	factory, err = NewRouteFactory("https://fred.example?lifecycle_id=shadow")
	assert.Nil(t, factory)
	require.Error(t, err)
}

func TestRouteFactoryMintsExplicitLegacyRoute(t *testing.T) {
	factory, err := NewRouteFactory("https://fred.example/proxy?trace=a%2Fb&&z=last")
	require.NoError(t, err)

	route, err := factory.Legacy()
	require.NoError(t, err)
	require.True(t, route.Valid())
	assert.True(t, route.IsLegacy())
	assert.False(t, route.ID().Valid())
	assert.Equal(t,
		"https://fred.example/proxy/callbacks/provision?trace=a%2Fb&&z=last",
		route.URL(),
	)
}
