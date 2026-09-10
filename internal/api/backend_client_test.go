package api

import (
	"context"
	"net/http"
	"net/http/httptest"
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
		testAPIBackendStorageID(config.Name),
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return client
}

func TestBackendHTTPClientFixturePreservesTypedRestoreRefusal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"too large","code":"demote_exceeds_tier"}`))
	}))
	t.Cleanup(server.Close)

	client := newBackendHTTPClientForTest(t, backendHTTPClientConfig{
		Name: "typed-restore-refusal", BaseURL: server.URL,
	})
	outcome := backend.InvokeRestore(context.Background(), client, backend.RestoreRequest{})
	require.True(t, outcome.Refused(), "identity-bound HTTP observation must retain refusal authority: %v", outcome.Err())
	require.Equal(t, backend.RestoreRefusalDemoteDataExceedsTier, outcome.Refusal())
}
