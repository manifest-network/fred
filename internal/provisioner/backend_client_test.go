package provisioner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	backendclienttest "github.com/manifest-network/fred/internal/testsupport/backendclient"
)

func newBackendHTTPClientForTest(
	t testing.TB,
	config backend.HTTPClientConfig,
) *backend.HTTPClient {
	t.Helper()
	client, cleanup, err := backendclienttest.New(
		config,
		testBackendStorageID(config.Name),
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return client
}
