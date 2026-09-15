package provisioner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	backendclienttest "github.com/manifest-network/fred/internal/testsupport/backendclient"
)

type backendHTTPClientConfig = backendclienttest.Config

func newBackendHTTPClientForTest(
	t testing.TB,
	config backendHTTPClientConfig,
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
