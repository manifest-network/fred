package tlsconfig_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/tlsconfig"
)

type testCertPaths struct {
	caFile         string
	serverCertFile string
	serverKeyFile  string
	clientCertFile string // CN=client, DNS SAN=providerd.internal
	clientKeyFile  string
	// untrusted* is a self-signed server leaf NOT signed by caFile.
	untrustedCertFile string
	untrustedKeyFile  string
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path,
		pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600))
}

func writeKey(t *testing.T, path string, key *ecdsa.PrivateKey) {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	writePEM(t, path, "EC PRIVATE KEY", der)
}

func writeTestCerts(t *testing.T) testCertPaths {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	p := testCertPaths{caFile: filepath.Join(dir, "ca.pem")}
	writePEM(t, p.caFile, "CERTIFICATE", caDER)

	writeLeaf := func(name string, eku []x509.ExtKeyUsage, dns []string, ips []net.IP) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  eku,
			DNSNames:     dns,
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		require.NoError(t, err)
		certFile := filepath.Join(dir, name+".pem")
		keyFile := filepath.Join(dir, name+"-key.pem")
		writePEM(t, certFile, "CERTIFICATE", der)
		writeKey(t, keyFile, key)
		return certFile, keyFile
	}

	loopback := []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback}
	p.serverCertFile, p.serverKeyFile = writeLeaf("server",
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, []string{"localhost"}, loopback)
	// Client leaf: CN=client, plus a DNS SAN so we can test SAN-based pinning.
	p.clientCertFile, p.clientKeyFile = writeLeaf("client",
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, []string{"providerd.internal"}, nil)

	// Untrusted self-signed server leaf for skip-verify tests.
	uKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	uTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "untrusted"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  loopback,
	}
	uDER, err := x509.CreateCertificate(rand.Reader, uTmpl, uTmpl, &uKey.PublicKey, uKey)
	require.NoError(t, err)
	p.untrustedCertFile = filepath.Join(dir, "untrusted.pem")
	p.untrustedKeyFile = filepath.Join(dir, "untrusted-key.pem")
	writePEM(t, p.untrustedCertFile, "CERTIFICATE", uDER)
	writeKey(t, p.untrustedKeyFile, uKey)

	return p
}

func TestServerConfig_MinVersionAndDefaults(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, "", nil)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
	require.Equal(t, tls.NoClientCert, cfg.ClientAuth)
	require.Nil(t, cfg.VerifyPeerCertificate)
	require.Len(t, cfg.Certificates, 1)
}

func TestServerConfig_MTLSEnablesClientAuth(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, certs.caFile, nil)
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	require.NotNil(t, cfg.ClientCAs)
	require.Nil(t, cfg.VerifyPeerCertificate) // no allowlist => no pinning
}

func TestServerConfig_AllowedNamesWithoutClientCAErrors(t *testing.T) {
	certs := writeTestCerts(t)
	_, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, "", []string{"providerd"})
	require.Error(t, err)
}

func TestServerConfig_BadCert(t *testing.T) {
	_, err := tlsconfig.ServerConfig("/nope/cert.pem", "/nope/key.pem", "", nil)
	require.Error(t, err)
}

func TestServerConfig_BadClientCA(t *testing.T) {
	certs := writeTestCerts(t)
	_, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, "/nope/ca.pem", nil)
	require.Error(t, err)
}

func TestClientConfig_MinVersionAndRootCAs(t *testing.T) {
	certs := writeTestCerts(t)
	cfg, err := tlsconfig.ClientConfig(certs.caFile, false, "", "")
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
	require.NotNil(t, cfg.RootCAs)
	require.False(t, cfg.InsecureSkipVerify)
	require.Empty(t, cfg.Certificates)
}

func TestClientConfig_BadCAFile(t *testing.T) {
	_, err := tlsconfig.ClientConfig("/nope/ca.pem", false, "", "")
	require.Error(t, err)
}

func TestClientConfig_BadClientCert(t *testing.T) {
	_, err := tlsconfig.ClientConfig("", false, "/nope/c.pem", "/nope/k.pem")
	require.Error(t, err)
}

// startMTLSServer starts an httptest TLS server using the given server config
// and returns its URL. The handler always replies 200.
func startMTLSServer(t *testing.T, srvCfg *tls.Config) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = srvCfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestEndToEnd_MTLS_RoundTrip(t *testing.T) {
	certs := writeTestCerts(t)
	srvCfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, certs.caFile, nil)
	require.NoError(t, err)
	url := startMTLSServer(t, srvCfg)

	cliCfg, err := tlsconfig.ClientConfig(certs.caFile, false, certs.clientCertFile, certs.clientKeyFile)
	require.NoError(t, err)
	resp, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: cliCfg}}).Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEndToEnd_MTLS_RejectsMissingClientCert(t *testing.T) {
	certs := writeTestCerts(t)
	srvCfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, certs.caFile, nil)
	require.NoError(t, err)
	url := startMTLSServer(t, srvCfg)

	cliCfg, err := tlsconfig.ClientConfig(certs.caFile, false, "", "") // trusts CA, presents no client cert
	require.NoError(t, err)
	_, err = (&http.Client{Transport: &http.Transport{TLSClientConfig: cliCfg}}).Get(url)
	require.Error(t, err)
}

func TestEndToEnd_MTLS_AllowlistAcceptsCN(t *testing.T) {
	certs := writeTestCerts(t)
	srvCfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, certs.caFile, []string{"client"})
	require.NoError(t, err)
	require.NotNil(t, srvCfg.VerifyPeerCertificate)
	url := startMTLSServer(t, srvCfg)

	cliCfg, err := tlsconfig.ClientConfig(certs.caFile, false, certs.clientCertFile, certs.clientKeyFile)
	require.NoError(t, err)
	resp, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: cliCfg}}).Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEndToEnd_MTLS_AllowlistAcceptsSAN(t *testing.T) {
	certs := writeTestCerts(t)
	srvCfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, certs.caFile, []string{"providerd.internal"})
	require.NoError(t, err)
	url := startMTLSServer(t, srvCfg)

	cliCfg, err := tlsconfig.ClientConfig(certs.caFile, false, certs.clientCertFile, certs.clientKeyFile)
	require.NoError(t, err)
	resp, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: cliCfg}}).Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestEndToEnd_MTLS_AllowlistRejectsUnknownName(t *testing.T) {
	certs := writeTestCerts(t)
	srvCfg, err := tlsconfig.ServerConfig(certs.serverCertFile, certs.serverKeyFile, certs.caFile, []string{"someone-else"})
	require.NoError(t, err)
	url := startMTLSServer(t, srvCfg)

	// Cert is signed by the trusted CA but its CN/SAN are not in the allowlist.
	cliCfg, err := tlsconfig.ClientConfig(certs.caFile, false, certs.clientCertFile, certs.clientKeyFile)
	require.NoError(t, err)
	_, err = (&http.Client{Transport: &http.Transport{TLSClientConfig: cliCfg}}).Get(url)
	require.Error(t, err)
}

func TestEndToEnd_SkipVerify(t *testing.T) {
	certs := writeTestCerts(t)
	srvCfg, err := tlsconfig.ServerConfig(certs.untrustedCertFile, certs.untrustedKeyFile, "", nil)
	require.NoError(t, err)
	url := startMTLSServer(t, srvCfg)

	strict, err := tlsconfig.ClientConfig("", false, "", "") // no matching CA -> fail
	require.NoError(t, err)
	_, err = (&http.Client{Transport: &http.Transport{TLSClientConfig: strict}}).Get(url)
	require.Error(t, err)

	loose, err := tlsconfig.ClientConfig("", true, "", "") // skip-verify -> success
	require.NoError(t, err)
	resp, err := (&http.Client{Transport: &http.Transport{TLSClientConfig: loose}}).Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
