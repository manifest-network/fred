package backend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

const (
	testBackendStorageIDA = "550e8400-e29b-41d4-a716-446655440000"
	testBackendStorageIDB = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	testIdentityClientKey = "identity-client-test-key-at-least-32-bytes"
)

type testStorageIdentityResolver struct {
	id    backendidentity.ID
	bound bool
}

func (resolver *testStorageIdentityResolver) ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool) {
	if resolver == nil {
		panic("typed-nil storage identity resolver must be rejected")
	}
	return resolver.id, resolver.bound
}

func mustBackendStorageID(t *testing.T, value string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return id
}

func TestNewIdentityBoundHTTPClientRejectsTypedNilResolverAndNonOriginBaseURL(t *testing.T) {
	t.Parallel()

	var typedNil *testStorageIdentityResolver
	client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
		Name: "backend-a", BaseURL: "https://backend.example", Secret: testIdentityClientKey,
	}, typedNil)
	assert.Nil(t, client)
	assert.Error(t, err)

	resolver := &testStorageIdentityResolver{
		id: mustBackendStorageID(t, testBackendStorageIDA), bound: true,
	}
	for _, rawURL := range []string{
		"", "backend.example", "ftp://backend.example", "https://user@backend.example",
		"https://backend.example/path", "https://backend.example?query=value",
		"https://backend.example?", "https://backend.example#fragment", "https://backend.example#",
		"https://:443", "https://.",
		"https://backend.example:", "https://backend.example:0", "https://backend.example:65536",
	} {
		client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
			Name: "backend-a", BaseURL: rawURL, Secret: testIdentityClientKey,
		}, resolver)
		assert.Nil(t, client, rawURL)
		assert.Error(t, err, rawURL)
	}
	client, err = NewIdentityBoundHTTPClient(HTTPClientConfig{
		Name: "backend-a", BaseURL: "https://backend.example", Timeout: -time.Second,
		Secret: testIdentityClientKey,
	}, resolver)
	assert.Nil(t, client)
	assert.ErrorContains(t, err, "timeout must not be negative")
}

func TestNewIdentityBoundHTTPClientRejectsMissingOrWeakHMACSecret(t *testing.T) {
	t.Parallel()

	resolver := &testStorageIdentityResolver{
		id: mustBackendStorageID(t, testBackendStorageIDA), bound: true,
	}
	for _, secret := range []string{"", "short"} {
		client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
			Name: "backend-a", BaseURL: "https://backend.example", Secret: secret,
		}, resolver)
		assert.Nil(t, client)
		assert.ErrorContains(t, err, "HMAC secret must be at least")
	}
}

func TestIdentityBoundHTTPClientValidatesEveryResponseIdentity(t *testing.T) {
	t.Parallel()

	expected := mustBackendStorageID(t, testBackendStorageIDA)
	other := mustBackendStorageID(t, testBackendStorageIDB)
	tests := []struct {
		name    string
		headers []string
		want    error
	}{
		{name: "matching", headers: []string{expected.String()}},
		{name: "missing", want: ErrBackendStorageIdentityMissing},
		{name: "empty", headers: []string{""}, want: ErrBackendStorageIdentityMissing},
		{name: "duplicate", headers: []string{expected.String(), expected.String()}, want: ErrBackendStorageIdentityMissing},
		{name: "malformed", headers: []string{"not-a-uuid"}, want: ErrBackendStorageIdentityMismatch},
		{name: "noncanonical", headers: []string{"550E8400-E29B-41D4-A716-446655440000"}, want: ErrBackendStorageIdentityMismatch},
		{name: "different", headers: []string{other.String()}, want: ErrBackendStorageIdentityMismatch},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assert.Equal(t, "/info/lease-a", r.URL.Path)
				assert.Equal(t, []string{expected.String()}, r.URL.Query()[backendidentity.QueryParameter])
				for _, value := range test.headers {
					w.Header().Add(backendidentity.ResponseHeader, value)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"host":"backend.example"}`))
			}))
			defer server.Close()

			client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
				Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
			}, &testStorageIdentityResolver{id: expected, bound: true})
			require.NoError(t, err)
			result, err := client.GetInfo(t.Context(), "lease-a")
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "backend.example", result.Host)
			}
			assert.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestIdentityBoundHTTPClientClassifiesOnlyHeaderlessServerErrorsAsUnavailable(t *testing.T) {
	t.Parallel()

	expected := mustBackendStorageID(t, testBackendStorageIDA)
	other := mustBackendStorageID(t, testBackendStorageIDB)
	tests := []struct {
		name            string
		status          int
		headers         []string
		wantErr         error
		wantUnavailable bool
	}{
		{
			name: "headerless server error", status: http.StatusServiceUnavailable,
			wantErr: ErrBackendStorageIdentityMissing, wantUnavailable: true,
		},
		{
			name: "headerless success", status: http.StatusOK,
			wantErr: ErrBackendStorageIdentityMissing,
		},
		{
			name: "headerless client error", status: http.StatusBadRequest,
			wantErr: ErrBackendStorageIdentityMissing,
		},
		{
			name: "empty identity on server error", status: http.StatusServiceUnavailable,
			headers: []string{""}, wantErr: ErrBackendStorageIdentityMissing,
		},
		{
			name: "duplicate identity on server error", status: http.StatusServiceUnavailable,
			headers: []string{expected.String(), expected.String()},
			wantErr: ErrBackendStorageIdentityMissing,
		},
		{
			name: "malformed identity on server error", status: http.StatusServiceUnavailable,
			headers: []string{"not-a-uuid"}, wantErr: ErrBackendStorageIdentityMismatch,
		},
		{
			name: "mismatched identity on server error", status: http.StatusServiceUnavailable,
			headers: []string{other.String()}, wantErr: ErrBackendStorageIdentityMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for _, header := range test.headers {
					w.Header().Add(backendidentity.ResponseHeader, header)
				}
				w.WriteHeader(test.status)
			}))
			defer server.Close()

			client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
				Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
			}, &testStorageIdentityResolver{id: expected, bound: true})
			require.NoError(t, err)

			err = client.Health(t.Context())
			assert.ErrorIs(t, err, test.wantErr)
			assert.Equal(t, test.wantUnavailable,
				IsBackendStorageIdentityMissingServerError(err))
		})
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{id: expected, bound: true})
	require.NoError(t, err)
	err = client.Provision(t.Context(), ProvisionRequest{})
	assert.ErrorIs(t, err, ErrBackendStorageIdentityMissing)
	assert.False(t, IsBackendStorageIdentityMissingServerError(err),
		"a headerless 5xx cannot prove that a side effect was refused")
}

func TestIdentityBoundHTTPClientUsesExactBoundPathForEverySideEffect(t *testing.T) {
	t.Parallel()

	id := mustBackendStorageID(t, testBackendStorageIDA)
	seen := make(chan string, 6)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, []string{id.String()}, r.URL.Query()[backendidentity.QueryParameter])
		seen <- r.URL.Path
		w.Header().Set(backendidentity.ResponseHeader, id.String())
		if r.URL.Path == backendidentity.BoundPathPrefix+id.String()+"/deprovision" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == backendidentity.BoundPathPrefix+id.String()+"/reconcile_custom_domain" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{id: id, bound: true})
	require.NoError(t, err)
	ctx := t.Context()
	invocations := []struct {
		path string
		call func() error
	}{
		{path: "/provision", call: func() error { return client.Provision(ctx, ProvisionRequest{}) }},
		{path: "/deprovision", call: func() error { return client.Deprovision(ctx, "lease-a") }},
		{path: "/restart", call: func() error { return client.Restart(ctx, RestartRequest{}) }},
		{path: "/update", call: func() error { return client.Update(ctx, UpdateRequest{}) }},
		{path: "/restore", call: func() error { return client.Restore(ctx, RestoreRequest{}) }},
		{path: "/reconcile_custom_domain", call: func() error {
			return client.ReconcileCustomDomain(ctx, "lease-a", nil)
		}},
	}
	for _, invocation := range invocations {
		require.NoError(t, invocation.call(), invocation.path)
		assert.Equal(t, backendidentity.BoundPathPrefix+id.String()+invocation.path, <-seen)
	}
}

func TestIdentityBoundHTTPClientRefusesUnboundSideEffectWithoutNetwork(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()

	client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{})
	require.NoError(t, err)

	err = client.Provision(t.Context(), ProvisionRequest{})
	assert.ErrorIs(t, err, ErrBackendStorageIdentityUnbound)
	assert.Zero(t, calls.Load())
}

func TestIdentityBoundHTTPClientClassifiesOldBackendBeforeLegacySideEffect(t *testing.T) {
	t.Parallel()

	id := mustBackendStorageID(t, testBackendStorageIDA)
	var boundCalls atomic.Int32
	var legacyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/provision":
			legacyCalls.Add(1)
			w.WriteHeader(http.StatusAccepted)
		default:
			boundCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
	}, &testStorageIdentityResolver{id: id, bound: true})
	require.NoError(t, err)

	err = client.Provision(t.Context(), ProvisionRequest{})
	assert.ErrorIs(t, err, ErrBackendUpgradeRequired)
	assert.Equal(t, int32(1), boundCalls.Load())
	assert.Zero(t, legacyCalls.Load(), "the upgraded client must never fall back to a legacy write path")
}

func TestIdentityBoundHTTPClientNeverFollowsBackendRedirects(t *testing.T) {
	t.Parallel()

	id := mustBackendStorageID(t, testBackendStorageIDA)
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer target.Close()

	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(backendidentity.ResponseHeader, id.String())
				http.Redirect(w, r, target.URL+"/legacy-side-effect", status)
			}))
			defer origin.Close()

			client, err := NewIdentityBoundHTTPClient(HTTPClientConfig{
				Name: "backend-a", BaseURL: origin.URL, Secret: testIdentityClientKey,
			}, &testStorageIdentityResolver{id: id, bound: true})
			require.NoError(t, err)
			err = client.Provision(t.Context(), ProvisionRequest{})
			assert.Error(t, err)
			assert.Zero(t, targetCalls.Load())
		})
	}
}

func TestInventoryPaginationRejectsStorageIdentityChange(t *testing.T) {
	t.Parallel()

	idA := mustBackendStorageID(t, testBackendStorageIDA)
	idB := mustBackendStorageID(t, testBackendStorageIDB)
	const continuation = "018f47a2-8b1c-7def-8123-456789abcdef"

	for _, endpoint := range []string{"provisions", "retentions"} {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				if call == 1 {
					w.Header().Set(backendidentity.ResponseHeader, idA.String())
				} else {
					w.Header().Set(backendidentity.ResponseHeader, idB.String())
				}
				w.Header().Set("Content-Type", "application/json")
				response := map[string]any{endpoint: []any{}}
				if r.URL.Query().Get("continue") == "" {
					response["continue"] = continuation
				}
				assert.NoError(t, json.NewEncoder(w).Encode(response))
			}))
			defer server.Close()

			client := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "backend-a", BaseURL: server.URL})
			var err error
			if endpoint == "provisions" {
				_, _, err = client.ListProvisionsWithIdentity(t.Context())
			} else {
				_, _, err = client.ListRetentionsWithIdentity(t.Context())
			}
			assert.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
			assert.Equal(t, int32(2), calls.Load())
		})
	}
}
