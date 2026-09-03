package backendclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func TestNew_TranslatesIdentityBoundaryWithoutWeakeningFixtureContract(t *testing.T) {
	t.Parallel()

	const secret = "fixture-secret-at-least-32-bytes-long"
	type observation struct {
		method       string
		path         string
		identitySeen string
		signatureErr error
	}
	observed := make(chan observation, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		observed <- observation{
			method:       request.Method,
			path:         request.URL.Path,
			identitySeen: request.URL.Query().Get(backendidentity.QueryParameter),
			signatureErr: hmacauth.VerifyRequest(
				secret,
				request,
				body,
				request.Header.Get(hmacauth.SignatureHeader),
				5*time.Minute,
			),
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	identity := fixtureIdentity(t)
	client, cleanup, err := New(backend.HTTPClientConfig{
		Name:    "backend-a",
		BaseURL: server.URL,
		Secret:  secret,
	}, identity)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	err = client.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID: "21b69b2b-947c-43e2-8dda-9db3ac86cd4a",
	})
	require.NoError(t, err)

	got := <-observed
	assert.Equal(t, http.MethodPost, got.method)
	assert.Equal(t, "/provision", got.path)
	assert.Empty(t, got.identitySeen,
		"the compatibility boundary must not expose upgraded-only query state to the legacy fixture")
	assert.NoError(t, got.signatureErr,
		"the compatibility boundary must re-sign the exact translated request target")
}

func TestNew_RejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	identity := fixtureIdentity(t)
	_, cleanup, err := New(backend.HTTPClientConfig{
		Name:    "backend-a",
		BaseURL: "backend.invalid",
	}, identity)
	assert.Nil(t, cleanup)
	assert.ErrorContains(t, err, "must be an absolute origin")

	_, cleanup, err = New(backend.HTTPClientConfig{
		Name:    "backend-a",
		BaseURL: "http://backend.invalid",
	}, backendidentity.ID{})
	assert.Nil(t, cleanup)
	assert.ErrorIs(t, err, backendidentity.ErrInvalidID)
}

func fixtureIdentity(t testing.TB) backendidentity.ID {
	t.Helper()
	identity, err := backendidentity.Parse("123e4567-e89b-42d3-a456-426614174000")
	require.NoError(t, err)
	return identity
}
