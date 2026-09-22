package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestMockServerTLSRequiresCompletePair(t *testing.T) {
	for _, paths := range [][2]string{{"cert.pem", ""}, {"", "key.pem"}} {
		_, err := mockServerTLS(paths[0], paths[1])
		require.ErrorContains(t, err, "must both be set")
	}
	config, err := mockServerTLS("", "")
	require.NoError(t, err)
	require.Nil(t, config, "standalone development HTTP remains supported")
}

func TestMockServerTLSProvidesAuthenticatedOfflineInventory(t *testing.T) {
	fixture := httptest.NewTLSServer(nil)
	defer fixture.Close()
	certFile, keyFile := filepath.Join(t.TempDir(), "cert.pem"), filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.Certificate().Raw}), 0o600))
	key, err := x509.MarshalPKCS8PrivateKey(fixture.TLS.Certificates[0].PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0o600))
	config, err := mockServerTLS(certFile, keyFile)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), config.MinVersion)
	server := httptest.NewUnstartedServer(newTestServer(t, nil).Handler())
	server.TLS = config
	server.StartTLS()
	defer server.Close()
	connection, err := backend.NewConnectionPolicy(backend.ConnectionConfig{Name: "mock", BaseURL: server.URL, Secret: testBackendSecret, TLSCAFile: certFile})
	require.NoError(t, err)
	policy, err := backend.NewAuthenticatedEvidencePolicy(connection)
	require.NoError(t, err)
	client, err := policy.NewInventoryClient()
	require.NoError(t, err)
	provisions, id, err := client.ListProvisionsWithIdentity(t.Context())
	require.NoError(t, err)
	require.Empty(t, provisions)
	require.Equal(t, testStorageID, id.String())
}
