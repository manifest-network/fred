package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/placementprobe"
)

const backendConnectionTestSecret = "backend-tls-parity-test-secret-at-least-32-bytes"

// TestBackendConnectionTLSParity checks the real config composition used by
// providerd, fresh-placement inventory, and offline repair. A successful probe
// must never approve a TLS version that the runtime cannot use.
func TestBackendConnectionTLSParity(t *testing.T) {
	t.Parallel()
	for _, version := range []struct {
		name     string
		value    uint16
		accepted bool
	}{
		{"TLS12", tls.VersionTLS12, false},
		{"TLS13", tls.VersionTLS13, true},
	} {
		t.Run(version.name, func(t *testing.T) {
			server := newBackendConnectionTLSServer(t, &tls.Config{MinVersion: version.value, MaxVersion: version.value})
			caFile := filepath.Join(t.TempDir(), "ca.pem")
			require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{
				Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
			}), 0o600))

			t.Run("private CA", func(t *testing.T) {
				checkBackendConnections(t, server.URL, caFile, version.accepted)
			})
			t.Run("system roots", func(t *testing.T) {
				if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
					t.Skip("SSL_CERT_FILE does not override native certificate stores")
				}
				// Go caches system roots process-wide. Run the actual nil-RootCAs
				// branch in a fresh test process, with no TLSCAFile or skip-verify
				// setting, instead of substituting an explicit test root pool.
				executable, err := os.Executable()
				require.NoError(t, err)
				child := exec.CommandContext(t.Context(), executable,
					"-test.run=^TestBackendConnectionSystemRootsHelper$", "-test.v")
				accepted := "false"
				if version.accepted {
					accepted = "true"
				}
				child.Env = append(os.Environ(),
					"FRED_TEST_BACKEND_TLS_URL="+server.URL,
					"FRED_TEST_BACKEND_TLS_ACCEPTED="+accepted,
					"SSL_CERT_FILE="+caFile,
					"SSL_CERT_DIR="+t.TempDir(),
				)
				output, err := child.CombinedOutput()
				require.NoError(t, err, "%s", output)
			})
		})
	}
}

func TestBackendConnectionSystemRootsHelper(t *testing.T) {
	url := os.Getenv("FRED_TEST_BACKEND_TLS_URL")
	if url == "" {
		t.Skip("only run as the isolated system-root subprocess")
	}
	checkBackendConnections(t, url, "", os.Getenv("FRED_TEST_BACKEND_TLS_ACCEPTED") == "true")
}

func newBackendConnectionTLSServer(t *testing.T, serverTLS *tls.Config) *httptest.Server {
	t.Helper()
	id := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, serverTLS.MaxVersion, r.TLS.Version)
		if serverTLS.ClientAuth == tls.RequireAndVerifyClientCert {
			assert.Len(t, r.TLS.VerifiedChains, 1)
		}
		signature := r.Header.Get(hmacauth.SignatureHeader)
		if err := hmacauth.VerifyRequest(backendConnectionTestSecret, r, nil, signature, 5*time.Minute); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if expected := r.URL.Query().Get(backendidentity.QueryParameter); expected != "" {
			assert.Equal(t, id.String(), expected)
		}
		w.Header().Set(backendidentity.ResponseHeader, id.String())
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/health":
			assert.Equal(t, id.String(), r.URL.Query().Get(backendidentity.QueryParameter))
			w.WriteHeader(http.StatusOK)
		case "/provisions":
			assert.NoError(t, json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
				Provisions: []backend.ProvisionInfo{},
			}))
		case "/retentions":
			assert.NoError(t, json.NewEncoder(w).Encode(backend.ListRetentionsResponse{
				Retentions: []backend.RetainedLease{},
			}))
		default:
			http.NotFound(w, r)
		}
	}))
	server.TLS = serverTLS
	server.Config.ErrorLog = log.New(io.Discard, "", 0) // Expected TLS 1.2 handshake refusals.
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func checkBackendConnections(t *testing.T, url, caFile string, accepted bool) {
	t.Helper()
	configured := config.BackendConfig{
		Name: "backend-a", URL: url, Timeout: 5 * time.Second,
		HMACSecret: backendConnectionTestSecret, TLSCAFile: caFile,
	}
	checkConfiguredBackendConnections(t, configured, func(t *testing.T, err error) {
		t.Helper()
		if accepted {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "protocol version not supported")
		}
	})
}

func checkConfiguredBackendConnections(t *testing.T, configured config.BackendConfig, checkResult func(*testing.T, error)) {
	t.Helper()
	id := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	cfg := &config.Config{Backends: []config.BackendConfig{configured}}
	resolver := startupIdentityResolver{"backend-a": id}
	t.Run("runtime", func(t *testing.T) {
		client, err := newProductionBackendClient(configured.Name, cfg, resolver)
		require.NoError(t, err)
		checkResult(t, client.Health(t.Context()))
	})
	t.Run("bootstrap inventory", func(t *testing.T) {
		clients, err := placementprobe.NewClients(cfg)
		require.NoError(t, err)
		_, mutable := clients[0].(backend.Backend)
		require.False(t, mutable, "bootstrap must keep its inventory-only capability")
		inventories, err := placementprobe.Collect(t.Context(), clients)
		checkResult(t, err)
		if err == nil {
			assert.Equal(t, id, inventories["backend-a"].StorageIdentity)
		} else {
			assert.Nil(t, inventories)
		}
	})
	t.Run("identity-bound repair inventory", func(t *testing.T) {
		clients, err := placementprobe.NewIdentityBoundClients(cfg, resolver)
		require.NoError(t, err)
		inventories, err := placementprobe.Collect(t.Context(), clients)
		checkResult(t, err)
		if err == nil {
			assert.Equal(t, id, inventories["backend-a"].StorageIdentity)
		} else {
			assert.Nil(t, inventories)
		}
	})
}

func TestBackendConnectionDevelopmentSkipVerifyPreserved(t *testing.T) {
	t.Parallel()
	server := newBackendConnectionTLSServer(t, &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
	checkConfiguredBackendConnections(t, config.BackendConfig{
		Name: "backend-a", URL: server.URL, HMACSecret: backendConnectionTestSecret,
		Timeout: 5 * time.Second, TLSSkipVerify: true,
	}, func(t *testing.T, err error) {
		t.Helper()
		require.NoError(t, err)
	})
}

func TestBackendConnectionMutualTLSPreserved(t *testing.T) {
	t.Parallel()
	// This client identity is trusted only by the test server. It is separate
	// from the server's own certificate, so both directions must authenticate.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "providerd-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true, IsCA: true,
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	directory := t.TempDir()
	clientCert := filepath.Join(directory, "client.pem")
	clientKey := filepath.Join(directory, "client.key")
	require.NoError(t, os.WriteFile(clientCert, certPEM, 0o600))
	require.NoError(t, os.WriteFile(clientKey, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
	clientRoots := x509.NewCertPool()
	require.True(t, clientRoots.AppendCertsFromPEM(certPEM))
	server := newBackendConnectionTLSServer(t, &tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		ClientCAs: clientRoots, ClientAuth: tls.RequireAndVerifyClientCert,
	})
	caFile := filepath.Join(directory, "server-ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	configured := config.BackendConfig{
		Name: "backend-a", URL: server.URL, Timeout: 5 * time.Second,
		HMACSecret: backendConnectionTestSecret, TLSCAFile: caFile,
	}
	t.Run("missing client identity", func(t *testing.T) {
		checkConfiguredBackendConnections(t, configured, func(t *testing.T, err error) {
			t.Helper()
			require.ErrorContains(t, err, "certificate required")
		})
	})
	t.Run("verified client identity", func(t *testing.T) {
		configured.TLSClientCertFile = clientCert
		configured.TLSClientKeyFile = clientKey
		checkConfiguredBackendConnections(t, configured, func(t *testing.T, err error) {
			t.Helper()
			require.NoError(t, err)
		})
	})
}
