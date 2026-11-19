//go:build integration

package e2e

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// certAuthority is a throwaway CA, generated per fixture, that signs both ends
// of the providerd-to-backend hop.
type certAuthority struct {
	cert     *x509.Certificate
	key      *ecdsa.PrivateKey
	certFile string
}

func newCertAuthority(t *testing.T) *certAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fred e2e CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	ca := &certAuthority{cert: cert, key: key, certFile: filepath.Join(t.TempDir(), "ca.pem")}
	writePEM(t, ca.certFile, "CERTIFICATE", der, 0o644)
	return ca
}

// issue writes a leaf certificate and its key into dir, owned by the service
// account, and returns their paths. A server leaf is valid for 127.0.0.1; a
// client leaf is identified by its common name.
func (ca *certAuthority) issue(t *testing.T, dir, file, commonName string, server bool) (certFile, keyFile string) {
	t.Helper()
	certDER, keyDER := ca.sign(t, commonName, server)
	certFile = filepath.Join(dir, file+"-cert.pem")
	keyFile = filepath.Join(dir, file+"-key.pem")
	writePEM(t, certFile, "CERTIFICATE", certDER, 0o600)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER, 0o600)
	GiveToServiceAccount(t, certFile, keyFile)
	return certFile, keyFile
}

// clientConfig is the TLS configuration of a client holding the certificate
// the backend pins.
func (ca *certAuthority) clientConfig(t *testing.T) *tls.Config {
	t.Helper()
	certDER, keyDER := ca.sign(t, clientCertName, false)
	key, err := x509.ParseECPrivateKey(keyDER)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key}},
	}
}

func (ca *certAuthority) sign(t *testing.T, commonName string, server bool) (certDER, keyDER []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
	}
	certDER, err = x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err = x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return certDER, keyDER
}

func writePEM(t *testing.T, path, blockType string, der []byte, mode os.FileMode) {
	t.Helper()
	encoded := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	require.NoError(t, os.WriteFile(path, encoded, mode))
}
