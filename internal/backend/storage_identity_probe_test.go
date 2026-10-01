package backend

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestProbeStorageIdentityReadsTheHeaderWhateverTheStatus(t *testing.T) {
	storage, err := backendidentity.New()
	require.NoError(t, err)
	for name, test := range map[string]struct {
		status   int
		identity string
		wantErr  bool
	}{
		"inventory answers":       {status: http.StatusOK, identity: storage.String()},
		"inventory fails":         {status: http.StatusInternalServerError, identity: storage.String()},
		"identity did not verify": {status: http.StatusServiceUnavailable, wantErr: true},
		"malformed identity":      {status: http.StatusOK, identity: "not-a-uuid", wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			var requested string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requested = r.URL.RequestURI()
				if test.identity != "" {
					w.Header().Set(backendidentity.ResponseHeader, test.identity)
				}
				w.WriteHeader(test.status)
			}))
			t.Cleanup(server.Close)
			policy, err := NewConnectionPolicy(ConnectionConfig{
				Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
			})
			require.NoError(t, err)

			observed, err := ProbeStorageIdentity(t.Context(), policy)
			assert.Equal(t, "/provisions?limit=1", requested, "one single-row page")
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, storage, observed)
		})
	}
}

func TestProbeStorageIdentityReportsAnUnreachableBackend(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	policy, err := NewConnectionPolicy(ConnectionConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: testIdentityClientKey,
	})
	require.NoError(t, err)
	_, err = ProbeStorageIdentity(t.Context(), policy)
	require.Error(t, err)
}
