package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestCallbackRouteFactoryDerivesOneTypedPair(t *testing.T) {
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174000")
	require.NoError(t, err)
	factory, err := NewCallbackRouteFactory(
		"http://localhost:8080/root///?trace=a%2Fb&&z=last",
	)
	require.NoError(t, err)

	pair, err := factory.ForOperation(operationID)
	require.NoError(t, err)
	require.True(t, pair.ValidFor(operationID))
	assert.Equal(t,
		"http://localhost:8080/root/callbacks/provision?trace=a%2Fb&&z=last&operation_id=123e4567-e89b-42d3-a456-426614174000",
		pair.OperationURL(),
	)
	assert.Equal(t,
		"http://localhost:8080/root/callbacks/provision?trace=a%2Fb&&z=last&lifecycle_id=123e4567-e89b-42d3-a456-426614174000",
		pair.LifecycleURL(),
	)
}

func TestCallbackRouteFactoryRejectsAmbientOrMalformedAuthority(t *testing.T) {
	for _, rawBase := range []string{
		"http://localhost:8080?trace=%ZZ",
		"http://localhost:8080?trace=x;y",
		"/relative/callback",
		"ftp://localhost/callback",
		"http://operator@localhost/callback",
		"http://localhost/callback#not-sent",
		"http://localhost/root/../callback",
		"http://localhost/root//callback",
		"http://localhost/root%2Fcallback",
		"http://localhost/root%00callback",
		`http://localhost/root\callback`,
		"http://localhost:8080?operation_id=123e4567-e89b-42d3-a456-426614174000",
		"http://localhost:8080?lifecycle%5Fid=123e4567-e89b-42d3-a456-426614174000",
	} {
		t.Run(rawBase, func(t *testing.T) {
			factory, err := NewCallbackRouteFactory(rawBase)
			require.Error(t, err)
			assert.Nil(t, factory)
		})
	}
}

func TestCallbackRouteFactoryRejectsInvalidOperation(t *testing.T) {
	factory, err := NewCallbackRouteFactory("http://localhost:8080")
	require.NoError(t, err)

	pair, err := factory.ForOperation(operation.OperationID{})
	require.Error(t, err)
	assert.False(t, pair.ValidFor(operation.OperationID{}))
}
