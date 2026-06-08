# ENG-103: TLS/mTLS for providerd ↔ docker-backend transport — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the providerd → docker-backend HTTP hop run over TLS (optionally mutually-authenticated, with client-identity pinning), configurable on both sides, with plaintext HTTP remaining the default so existing deployments keep working.

**Architecture:** A new `internal/tlsconfig` package builds `*tls.Config` for the server side (cert/key + optional `ClientCAs`/`RequireAndVerifyClientCert` + optional client-name pinning via `VerifyPeerCertificate`) and the client side (optional `RootCAs` + client cert + skip-verify), both pinned to `MinVersion: tls.VersionTLS13`. docker-backend's listener branches to `ListenAndServeTLS` when a cert is configured. providerd builds a per-backend `*tls.Config` at startup (composition root, where file-I/O errors are handled) and hands it to `NewHTTPClient`, which applies it to the backend transport. The hop stays **HTTP/1.1 over TLS** (it is plaintext HTTP/1.1 today) — we deliberately do not force HTTP/2. The existing gRPC TLS helper (`chain.buildTLSConfig`) is intentionally left untouched.

**Tech Stack:** Go stdlib `crypto/tls`, `crypto/x509`, `net/http`; `github.com/stretchr/testify` for assertions; `net/http/httptest` for end-to-end TLS tests.

**Decisions locked (from design + best-practice review):**
- New `internal/tlsconfig` package for the new hop **only**; `chain.buildTLSConfig` stays as-is.
- `MinVersion: tls.VersionTLS13` on the new hop (Mozilla "Modern" profile; we control both ends).
- **Client-identity pinning:** `RequireAndVerifyClientCert` only proves the client cert chains to our CA — *not* who the client is. docker-backend gets an optional `tls_client_allowed_names` allowlist enforced by a server-side `VerifyPeerCertificate` (matches the client cert's CN or a DNS SAN). Empty list = accept any cert from the CA (back-compat).
- **No HTTP/2 forcing:** keep HTTP/1.1 over TLS (behavior-preserving; avoids the custom-TLS-config + `ForceAttemptHTTP2` footgun, golang/go#20645).
- Certs are **pre-provisioned files loaded once at startup**; hot-reload is out of scope (ENG-294). Cert provisioning is out of scope (manifest-deploy / ENG-104).
- Config-error handling for client TLS lives in `cmd/providerd/main.go`; `NewHTTPClient` keeps its no-error signature by accepting a pre-built `*tls.Config`.

**Conventions:** All commands run from the worktree root `/home/fmorency/dev/fred/.claude/worktrees/eng-103-tls-transport`. Branch: `worktree-eng-103-tls-transport`.

---

## File Structure

**Create:**
- `internal/tlsconfig/tlsconfig.go` — `ServerConfig` + `ClientConfig` builders, plus a `VerifyPeerCertificate` name-pinner. Single responsibility: turn file paths + an allowlist into a validated `*tls.Config`.
- `internal/tlsconfig/tlsconfig_test.go` — unit + end-to-end handshake tests, with an in-package cert-generation helper (keeps cert-gen out of production code and avoids a shared test-support package).

**Modify:**
- `internal/backend/docker/config.go` — add `TLSCertFile`/`TLSKeyFile`/`TLSClientCAFile`/`TLSClientAllowedNames` fields + validation in `Validate()`.
- `internal/backend/docker/config_test.go` — validation tests (uses existing `validConfig()` helper at line 16).
- `cmd/docker-backend/main.go` — branch the listener to `ListenAndServeTLS` when TLS is configured.
- `internal/config/config.go` — extend `BackendConfig` with TLS fields; add pairing validation + production-mode skip-verify rejection.
- `internal/config/config_test.go` — add a `validConfig()` helper (factored from `TestConfig_Validate_Valid`) and validation tests.
- `internal/backend/client.go` — add `TLSClientConfig *tls.Config` to `HTTPClientConfig`; apply it in `NewHTTPClient`.
- `internal/backend/client_test.go` — white-box wiring test (package `backend`, asserts the unexported transport).
- `cmd/providerd/main.go` — build the per-backend `*tls.Config` and pass it into `HTTPClientConfig`.
- `config.example.yaml` — document the new per-backend TLS fields.
- `docker-backend.example.yaml` — document the new server-side TLS fields.

**Test coverage rationale:** the *real* TLS behavior (mTLS success, missing-client-cert rejection, name-allowlist accept/reject, private-CA trust, skip-verify, MinVersion) is proven end-to-end in `internal/tlsconfig` against an `httptest` TLS server. `NewHTTPClient`'s job is only to copy a `*tls.Config` onto the transport — proven by a focused white-box test. The composition (providerd dialing a TLS docker-backend) follows by transitivity; a manual smoke is included in the final verification.

---

## Task 1: `internal/tlsconfig` package

**Files:**
- Create: `internal/tlsconfig/tlsconfig.go`
- Test: `internal/tlsconfig/tlsconfig_test.go`

- [ ] **Step 1: Write the cert-generation test helper + failing tests**

Create `internal/tlsconfig/tlsconfig_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail (no package yet)**

Run: `go test ./internal/tlsconfig/...`
Expected: FAIL — build error, `package .../internal/tlsconfig` not found / `undefined: tlsconfig.ServerConfig`.

- [ ] **Step 3: Implement the package**

Create `internal/tlsconfig/tlsconfig.go`:

```go
// Package tlsconfig builds *tls.Config values for Fred's internal
// providerd <-> backend HTTP hop (ENG-103). Server configs optionally
// require a client certificate (mTLS) and optionally pin the client's
// identity (CN / DNS SAN) to an allowlist; client configs optionally trust a
// private CA and present a client certificate. Both pin MinVersion to TLS 1.3.
//
// Certificates are read from files once, at call time; rotation requires a
// restart (hot-reload is tracked in ENG-294).
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerConfig builds a server-side *tls.Config from a certificate/key pair.
//
// When clientCAFile is non-empty, mutual TLS is enabled: the server requires
// and verifies a client certificate signed by that CA.
//
// When allowedClientNames is non-empty (which requires clientCAFile), the
// server additionally pins the client's identity: the presented certificate's
// CommonName or one of its DNS SANs must appear in the list. This closes the
// gap that RequireAndVerifyClientCert only checks the chain, not *who* the
// client is — without it, any certificate signed by the CA would be accepted.
func ServerConfig(certFile, keyFile, clientCAFile string, allowedClientNames []string) (*tls.Config, error) {
	if len(allowedClientNames) > 0 && clientCAFile == "" {
		return nil, fmt.Errorf("allowed client names require a client CA (mutual TLS)")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if clientCAFile != "" {
		pool, err := loadCertPool(clientCAFile)
		if err != nil {
			return nil, fmt.Errorf("load client CA: %w", err)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		if len(allowedClientNames) > 0 {
			cfg.VerifyPeerCertificate = clientNamePinner(allowedClientNames)
		}
	}
	return cfg, nil
}

// ClientConfig builds a client-side *tls.Config. When caFile is non-empty it
// is used as the root CA set (otherwise the system roots apply). When both
// clientCertFile and clientKeyFile are non-empty, the pair is presented for
// mutual TLS. skipVerify disables server certificate verification (dev only).
func ClientConfig(caFile string, skipVerify bool, clientCertFile, clientKeyFile string) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: skipVerify, //nolint:gosec // opt-in, rejected in production_mode (see config.Validate)
	}
	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("load CA: %w", err)
		}
		cfg.RootCAs = pool
	}
	if clientCertFile != "" || clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// clientNamePinner returns a VerifyPeerCertificate callback that passes only
// when the verified client leaf's CommonName or a DNS SAN is in allowed.
// It runs after RequireAndVerifyClientCert has already verified the chain, so
// verifiedChains is non-empty.
func clientNamePinner(allowed []string) func([][]byte, [][]*x509.Certificate) error {
	set := make(map[string]struct{}, len(allowed))
	for _, n := range allowed {
		set[n] = struct{}{}
	}
	return func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
		for _, chain := range verifiedChains {
			leaf := chain[0]
			if _, ok := set[leaf.Subject.CommonName]; ok {
				return nil
			}
			for _, dns := range leaf.DNSNames {
				if _, ok := set[dns]; ok {
					return nil
				}
			}
		}
		leaf := verifiedChains[0][0]
		return fmt.Errorf("client certificate identity not allowed (CN=%q SANs=%v)",
			leaf.Subject.CommonName, leaf.DNSNames)
	}
}

// loadCertPool reads a PEM bundle and returns a pool containing its certs.
func loadCertPool(file string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", file)
	}
	return pool, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/tlsconfig/...`
Expected: PASS (all tests in Step 1).

- [ ] **Step 5: Commit**

```bash
git add internal/tlsconfig/
git commit -m "feat(tlsconfig): add server/client TLS builders with client-name pinning (ENG-103)"
```

---

## Task 2: docker-backend config fields + validation

**Files:**
- Modify: `internal/backend/docker/config.go` (Config struct ~line 42; `Validate()` ~line 293)
- Test: `internal/backend/docker/config_test.go`

- [ ] **Step 1: Write failing validation tests**

Append to `internal/backend/docker/config_test.go` (uses the existing `validConfig()` helper at line 16):

```go
func TestConfig_Validate_TLS(t *testing.T) {
	t.Run("cert without key is rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.TLSCertFile = "/etc/fred/docker-backend/tls/cert.pem"
		require.ErrorContains(t, cfg.Validate(), "tls_cert_file and tls_key_file")
	})

	t.Run("key without cert is rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.TLSKeyFile = "/etc/fred/docker-backend/tls/key.pem"
		require.ErrorContains(t, cfg.Validate(), "tls_cert_file and tls_key_file")
	})

	t.Run("client CA without server cert is rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.TLSClientCAFile = "/etc/fred/docker-backend/tls/client-ca.pem"
		require.ErrorContains(t, cfg.Validate(), "tls_client_ca_file requires")
	})

	t.Run("allowed names without client CA is rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.TLSCertFile = "/etc/fred/docker-backend/tls/cert.pem"
		cfg.TLSKeyFile = "/etc/fred/docker-backend/tls/key.pem"
		cfg.TLSClientAllowedNames = []string{"providerd"}
		require.ErrorContains(t, cfg.Validate(), "tls_client_allowed_names requires")
	})

	t.Run("cert and key together is accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.TLSCertFile = "/etc/fred/docker-backend/tls/cert.pem"
		cfg.TLSKeyFile = "/etc/fred/docker-backend/tls/key.pem"
		require.NoError(t, cfg.Validate())
	})

	t.Run("full mTLS config with allowlist is accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.TLSCertFile = "/etc/fred/docker-backend/tls/cert.pem"
		cfg.TLSKeyFile = "/etc/fred/docker-backend/tls/key.pem"
		cfg.TLSClientCAFile = "/etc/fred/docker-backend/tls/client-ca.pem"
		cfg.TLSClientAllowedNames = []string{"providerd"}
		require.NoError(t, cfg.Validate())
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/backend/docker/ -run TestConfig_Validate_TLS`
Expected: FAIL — `cfg.TLSCertFile undefined` (fields not yet added).

- [ ] **Step 3: Add the fields**

In `internal/backend/docker/config.go`, immediately after the `ListenAddr` field (currently lines 41-42):

```go
	// ListenAddr is the address the HTTP server listens on.
	ListenAddr string `yaml:"listen_addr"`

	// TLSCertFile and TLSKeyFile enable HTTPS on the listener when both are
	// set; otherwise it serves plaintext HTTP (the default). Loaded once at
	// startup — rotation requires a restart (see ENG-294).
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`

	// TLSClientCAFile turns on mutual TLS when set: the listener requires and
	// verifies a client certificate signed by this CA. Requires TLSCertFile and
	// TLSKeyFile (the listener must be on TLS first).
	TLSClientCAFile string `yaml:"tls_client_ca_file"`

	// TLSClientAllowedNames optionally pins the mTLS client's identity: the
	// presented certificate's CommonName or a DNS SAN must be in this list.
	// Empty accepts any certificate signed by TLSClientCAFile. Requires
	// TLSClientCAFile. Use this whenever the client CA is not dedicated solely
	// to providerd.
	TLSClientAllowedNames []string `yaml:"tls_client_allowed_names"`
```

- [ ] **Step 4: Add the validation**

In `internal/backend/docker/config.go` `Validate()`, immediately after the `listen_addr` required check (currently lines 293-295):

```go
	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
	}

	// TLS: cert and key are set together; client-CA (mTLS) needs the listener
	// on TLS first; client-name pinning needs mTLS.
	if (c.TLSCertFile != "") != (c.TLSKeyFile != "") {
		return fmt.Errorf("both tls_cert_file and tls_key_file must be set together")
	}
	if c.TLSClientCAFile != "" && c.TLSCertFile == "" {
		return fmt.Errorf("tls_client_ca_file requires tls_cert_file and tls_key_file (mTLS needs the listener on TLS)")
	}
	if len(c.TLSClientAllowedNames) > 0 && c.TLSClientCAFile == "" {
		return fmt.Errorf("tls_client_allowed_names requires tls_client_ca_file (mTLS must be enabled to pin client identity)")
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/backend/docker/ -run TestConfig_Validate`
Expected: PASS (new TLS subtests + existing validation tests).

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/config.go internal/backend/docker/config_test.go
git commit -m "feat(docker-backend): add TLS/mTLS config fields + validation (ENG-103)"
```

---

## Task 3: docker-backend TLS listener wiring

**Files:**
- Modify: `cmd/docker-backend/main.go` (imports; server setup lines ~104-119)

- [ ] **Step 1: Add imports**

In `cmd/docker-backend/main.go`, add `"crypto/tls"` to the stdlib import group and `"github.com/manifest-network/fred/internal/tlsconfig"` to the project import group.

- [ ] **Step 2: Build the server TLS config (fail-fast) before the server**

In `cmd/docker-backend/main.go`, replace the "Setup HTTP server" block (currently lines 103-110):

```go
	// Setup HTTP server
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
```

with:

```go
	// Build the listener TLS config up front so a bad cert fails fast before we
	// announce readiness. Config.Validate (run in docker.New) already enforces
	// field pairing; ServerConfig loads and parses the actual files.
	var tlsServerConfig *tls.Config
	if cfg.TLSCertFile != "" {
		tlsServerConfig, err = tlsconfig.ServerConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile, cfg.TLSClientAllowedNames)
		if err != nil {
			logger.Error("failed to build TLS config", "error", err)
			os.Exit(1)
		}
	}

	// Setup HTTP server
	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      server.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		TLSConfig:    tlsServerConfig, // nil => plaintext HTTP
	}
```

- [ ] **Step 3: Branch the serve goroutine to TLS**

In `cmd/docker-backend/main.go`, replace the "Start HTTP server" goroutine (currently lines 112-119):

```go
	// Start HTTP server
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
```

with:

```go
	// Start HTTP server
	serverErr := make(chan error, 1)
	go func() {
		var serveErr error
		if tlsServerConfig != nil {
			logger.Info("starting HTTPS server", "addr", cfg.ListenAddr,
				"mtls", cfg.TLSClientCAFile != "", "pinned_names", len(cfg.TLSClientAllowedNames))
			// The cert/key live in tlsServerConfig.Certificates (loaded by
			// tlsconfig.ServerConfig), so the file arguments are empty.
			serveErr = httpServer.ListenAndServeTLS("", "")
		} else {
			logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
			serveErr = httpServer.ListenAndServe()
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErr <- serveErr
		}
	}()
```

- [ ] **Step 4: Build and vet**

Run: `go build ./cmd/docker-backend/... && go vet ./cmd/docker-backend/...`
Expected: no output, exit 0.

- [ ] **Step 5: Run existing docker-backend tests (no regression)**

Run: `go test ./cmd/docker-backend/...`
Expected: PASS. TLS serve behavior is covered end-to-end by Task 1's `tlsconfig` tests and the manual smoke in the final verification.

- [ ] **Step 6: Commit**

```bash
git add cmd/docker-backend/main.go
git commit -m "feat(docker-backend): serve HTTPS/mTLS when tls_cert_file is configured (ENG-103)"
```

---

## Task 4: providerd BackendConfig fields + validation

**Files:**
- Modify: `internal/config/config.go` (`BackendConfig` lines 128-135; basic backends loop lines 416-438; production-mode block lines 470-490)
- Test: `internal/config/config_test.go`

- [ ] **Step 1: Add a `validConfig()` test helper + failing tests**

Append to `internal/config/config_test.go` (the helper is factored from the inline config in `TestConfig_Validate_Valid`, lines 151-184, so new tests don't repeat the full struct):

```go
// validConfig returns a Config that passes Validate(), for mutation in tests.
func validConfig() Config {
	return Config{
		ProviderUUID:              "01234567-89ab-cdef-0123-456789abcdef",
		ProviderAddress:           "manifest1abc",
		KeyName:                   "provider",
		KeyringDir:                "/home/provider/.manifest",
		Bech32Prefix:              "manifest",
		WithdrawInterval:          time.Hour,
		RateLimitRPS:              10,
		RateLimitBurst:            20,
		GRPCEndpoint:              "localhost:9090",
		WebSocketURL:              "ws://localhost:26657/websocket",
		GasLimit:                  500000,
		GasPrice:                  25,
		GasAdjustment:             1.2,
		FeeDenom:                  "umfx",
		HTTPReadTimeout:           15 * time.Second,
		HTTPWriteTimeout:          15 * time.Second,
		HTTPIdleTimeout:           60 * time.Second,
		WebSocketPingInterval:     30 * time.Second,
		TxPollInterval:            500 * time.Millisecond,
		TxTimeout:                 30 * time.Second,
		QueryPageLimit:            100,
		MaxWithdrawIterations:     100,
		WebSocketReconnectInitial: time.Second,
		WebSocketReconnectMax:     60 * time.Second,
		MaxRequestBodySize:        1 << 20,
		CreditCheckErrorThreshold: 3,
		CreditCheckRetryInterval:  30 * time.Second,
		ReconciliationInterval:    5 * time.Minute,
		ShutdownTimeout:           30 * time.Second,
		Backends:                  []BackendConfig{{Name: "mock", URL: "http://localhost:9000", IsDefault: true}},
		CallbackBaseURL:           "http://localhost:8080",
		CallbackSecret:            "a]Gy4/r^SfN?b{Ye9t#L@F8z&V+mWkPq",
	}
}

func TestConfig_Validate_BackendTLS(t *testing.T) {
	t.Run("client cert without key is rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.Backends = []BackendConfig{{
			Name: "b1", URL: "https://10.0.0.1:9001", IsDefault: true,
			TLSClientCertFile: "/etc/fred/providerd/tls/client.pem",
		}}
		require.ErrorContains(t, cfg.Validate(), "tls_client_cert_file and tls_client_key_file")
	})

	t.Run("client key without cert is rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.Backends = []BackendConfig{{
			Name: "b1", URL: "https://10.0.0.1:9001", IsDefault: true,
			TLSClientKeyFile: "/etc/fred/providerd/tls/client-key.pem",
		}}
		require.ErrorContains(t, cfg.Validate(), "tls_client_cert_file and tls_client_key_file")
	})

	t.Run("full mTLS backend is accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.Backends = []BackendConfig{{
			Name: "b1", URL: "https://10.0.0.1:9001", IsDefault: true,
			TLSCAFile:         "/etc/fred/providerd/tls/backend-ca.pem",
			TLSClientCertFile: "/etc/fred/providerd/tls/client.pem",
			TLSClientKeyFile:  "/etc/fred/providerd/tls/client-key.pem",
		}}
		require.NoError(t, cfg.Validate())
	})

	t.Run("production_mode rejects backend tls_skip_verify", func(t *testing.T) {
		cfg := validConfig()
		cfg.ProductionMode = true
		cfg.Backends = []BackendConfig{{
			Name: "b1", URL: "https://10.0.0.1:9001", IsDefault: true,
			TLSSkipVerify: true,
		}}
		require.ErrorContains(t, cfg.Validate(), "tls_skip_verify")
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run TestConfig_Validate_BackendTLS`
Expected: FAIL — `unknown field TLSClientCertFile in struct literal`.

- [ ] **Step 3: Extend `BackendConfig`**

In `internal/config/config.go`, replace the `BackendConfig` struct (lines 128-135):

```go
// BackendConfig configures a single provisioning backend.
type BackendConfig struct {
	Name      string        `mapstructure:"name"`
	URL       string        `mapstructure:"url"`
	Timeout   time.Duration `mapstructure:"timeout"`
	SKUs      []string      `mapstructure:"skus"`
	IsDefault bool          `mapstructure:"default"`

	// TLS for the providerd -> backend hop (ENG-103). Empty fields fall back to
	// Go defaults (system root CAs, no client certificate).
	TLSCAFile         string `mapstructure:"tls_ca_file"`          // private CA that signed the backend's server cert
	TLSSkipVerify     bool   `mapstructure:"tls_skip_verify"`      // DEV ONLY; rejected when production_mode is true
	TLSClientCertFile string `mapstructure:"tls_client_cert_file"` // client cert for mTLS (set with key)
	TLSClientKeyFile  string `mapstructure:"tls_client_key_file"`  // client key for mTLS (set with cert)
}
```

- [ ] **Step 4: Add client cert/key pairing validation**

In `internal/config/config.go`, inside the basic backends loop, after the URL validation (currently after line 430, before the `IsDefault` check at line 432):

```go
		if err := validateHTTPURL(b.URL); err != nil {
			return fmt.Errorf("backends[%d].url: %w", i, err)
		}

		// mTLS client cert and key are set together.
		if (b.TLSClientCertFile != "") != (b.TLSClientKeyFile != "") {
			return fmt.Errorf("backends[%d]: both tls_client_cert_file and tls_client_key_file must be set together", i)
		}

		if b.IsDefault {
```

- [ ] **Step 5: Add production-mode skip-verify rejection**

In `internal/config/config.go`, in the production-mode block, immediately after the gRPC skip-verify check (currently lines 474-476):

```go
		if c.GRPCTLSEnabled && c.GRPCTLSSkipVerify {
			return fmt.Errorf("production_mode: grpc_tls_skip_verify cannot be enabled with grpc_tls_enabled")
		}
		for i, b := range c.Backends {
			if b.TLSSkipVerify {
				return fmt.Errorf("production_mode: backends[%d].tls_skip_verify cannot be enabled", i)
			}
		}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/config/ -run TestConfig_Validate`
Expected: PASS (new `TestConfig_Validate_BackendTLS` subtests + all existing validation tests).

- [ ] **Step 7: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(providerd): add per-backend TLS config + validation (ENG-103)"
```

---

## Task 5: NewHTTPClient applies the TLS config

**Files:**
- Modify: `internal/backend/client.go` (imports; `HTTPClientConfig` lines 433-462; `NewHTTPClient` transport block lines 486-490)
- Test: `internal/backend/client_test.go` (package `backend`, white-box)

- [ ] **Step 1: Write failing white-box wiring tests**

Append to `internal/backend/client_test.go` (already `package backend`; add `"crypto/tls"` and `"net/http"` to its imports if not present):

```go
func TestNewHTTPClient_AppliesTLSClientConfig(t *testing.T) {
	sentinel := &tls.Config{MinVersion: tls.VersionTLS13}
	c := NewHTTPClient(HTTPClientConfig{
		Name:            "tls-backend",
		BaseURL:         "https://backend.example:9001",
		TLSClientConfig: sentinel,
	})
	tr, ok := c.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Same(t, sentinel, tr.TLSClientConfig)
}

func TestNewHTTPClient_NoTLSConfig_LeavesTransportDefault(t *testing.T) {
	c := NewHTTPClient(HTTPClientConfig{Name: "plain", BaseURL: "http://backend:9001"})
	tr, ok := c.httpClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, tr.TLSClientConfig)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/backend/ -run TestNewHTTPClient_`
Expected: FAIL — `unknown field TLSClientConfig in struct literal`.

- [ ] **Step 3: Add the import**

In `internal/backend/client.go`, add `"crypto/tls"` to the stdlib import group.

- [ ] **Step 4: Add the `HTTPClientConfig` field**

In `internal/backend/client.go`, in the `HTTPClientConfig` struct, after the `Secret` field (line 439):

```go
	Secret              string

	// TLSClientConfig, when non-nil, is applied to the backend HTTP transport
	// (private-CA trust and/or a client certificate for mTLS). Built by the
	// caller from per-backend config so this package performs no file I/O.
	TLSClientConfig *tls.Config
```

- [ ] **Step 5: Apply it in `NewHTTPClient`**

In `internal/backend/client.go`, replace the transport construction (lines 486-490):

```go
	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
	}
```

with:

```go
	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
	}
	if cfg.TLSClientConfig != nil {
		transport.TLSClientConfig = cfg.TLSClientConfig
		// The hop stays HTTP/1.1 over TLS (it is plaintext HTTP/1.1 today).
		// A custom TLSClientConfig disables Go's automatic HTTP/2; we
		// deliberately do NOT set ForceAttemptHTTP2 — these are low-volume
		// JSON request/response calls and h2 with a custom TLS config carries
		// a known footgun (golang/go#20645).
	}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/backend/ -run TestNewHTTPClient_`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/backend/client.go internal/backend/client_test.go
git commit -m "feat(backend): apply optional TLSClientConfig to the HTTP backend transport (ENG-103)"
```

---

## Task 6: providerd wires per-backend TLS into NewHTTPClient

**Files:**
- Modify: `cmd/providerd/main.go` (imports; backend init loop lines 242-251)

- [ ] **Step 1: Add imports**

In `cmd/providerd/main.go`, add `"crypto/tls"` to the stdlib import group and `"github.com/manifest-network/fred/internal/tlsconfig"` to the project import group.

- [ ] **Step 2: Build and pass the per-backend TLS config**

In `cmd/providerd/main.go`, replace the backend init loop body (lines 242-251):

```go
	for _, bcfg := range cfg.Backends {
		client := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:                bcfg.Name,
			BaseURL:             bcfg.URL,
			Timeout:             bcfg.Timeout,
			Secret:              string(cfg.CallbackSecret),
			RequestDuration:     metrics.BackendRequestDuration,
			RequestsTotal:       metrics.BackendRequestsTotal,
			CircuitBreakerState: metrics.BackendCircuitBreakerState,
		})
```

with:

```go
	for _, bcfg := range cfg.Backends {
		// Build the per-backend TLS config at the composition root so file-I/O
		// errors fail startup here (config.Validate has already checked field
		// pairing and the production_mode skip-verify rule). Left nil when no
		// TLS fields are set, so plaintext http:// backends are unaffected.
		var tlsClientConfig *tls.Config
		if bcfg.TLSCAFile != "" || bcfg.TLSClientCertFile != "" || bcfg.TLSClientKeyFile != "" || bcfg.TLSSkipVerify {
			tlsClientConfig, err = tlsconfig.ClientConfig(bcfg.TLSCAFile, bcfg.TLSSkipVerify, bcfg.TLSClientCertFile, bcfg.TLSClientKeyFile)
			if err != nil {
				return fmt.Errorf("backend %q: build TLS client config: %w", bcfg.Name, err)
			}
		}

		client := backend.NewHTTPClient(backend.HTTPClientConfig{
			Name:                bcfg.Name,
			BaseURL:             bcfg.URL,
			Timeout:             bcfg.Timeout,
			Secret:              string(cfg.CallbackSecret),
			TLSClientConfig:     tlsClientConfig,
			RequestDuration:     metrics.BackendRequestDuration,
			RequestsTotal:       metrics.BackendRequestsTotal,
			CircuitBreakerState: metrics.BackendCircuitBreakerState,
		})
```

Note: `err` is already in scope in this function (declared earlier, e.g. the `eventSub, err := ...` block), so `tlsClientConfig, err =` reuses it.

- [ ] **Step 3: Build and vet**

Run: `go build ./cmd/providerd/... && go vet ./cmd/providerd/...`
Expected: no output, exit 0. (If vet reports shadowing, confirm the assignment uses `=` not `:=`.)

- [ ] **Step 4: Run the broader test set (no regression)**

Run: `go test ./cmd/providerd/... ./internal/backend/... ./internal/config/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/providerd/main.go
git commit -m "feat(providerd): build per-backend TLS client config from config (ENG-103)"
```

---

## Task 7: Document the new fields in example configs

**Files:**
- Modify: `config.example.yaml` (backends section, before the CALLBACK banner ~line 298)
- Modify: `docker-backend.example.yaml` (after `listen_addr`, ~line 16)

- [ ] **Step 1: Document providerd per-backend TLS**

In `config.example.yaml`, insert this commented block at the end of the `backends:` examples, immediately before the `# ====` CALLBACK CONFIGURATION banner (currently line 298):

```yaml
  # ---------------------------------------------------------------------------
  # TLS / mTLS to a backend (ENG-103)
  # ---------------------------------------------------------------------------
  # When a backend listens on HTTPS, use https:// in url and point tls_ca_file
  # at the private CA that signed the backend's server certificate (omit it to
  # use the system root CAs). For mutual TLS, also supply a client certificate
  # and key that the backend's tls_client_ca_file trusts.
  #
  # - name: docker-1
  #   url: "https://10.0.0.1:9001"
  #   tls_ca_file: "/etc/fred/providerd/tls/backend-ca.pem"
  #   # mTLS (optional): present a client certificate to the backend
  #   tls_client_cert_file: "/etc/fred/providerd/tls/client.pem"
  #   tls_client_key_file:  "/etc/fred/providerd/tls/client-key.pem"
  #   # tls_skip_verify: true   # DEV ONLY — rejected when production_mode: true
  #   skus:
  #     - "a1b2c3d4-e5f6-7890-abcd-1234567890ab"
```

- [ ] **Step 2: Document docker-backend listener TLS**

In `docker-backend.example.yaml`, insert immediately after the `listen_addr: ":9001"` line (line 16):

```yaml

# ---------------------------------------------------------------------------
# TLS / mTLS (ENG-103)
# ---------------------------------------------------------------------------
# When tls_cert_file and tls_key_file are both set, the listener serves HTTPS
# instead of plaintext HTTP. The server certificate MUST carry a SAN matching
# the address providerd dials (Go ignores the legacy CN). Point providerd's
# per-backend url at https:// and its tls_ca_file at the CA that signed this
# certificate.
# tls_cert_file: "/etc/fred/docker-backend/tls/cert.pem"
# tls_key_file:  "/etc/fred/docker-backend/tls/key.pem"
#
# For mutual TLS, also set tls_client_ca_file: the listener then requires and
# verifies a client certificate signed by this CA (providerd presents one via
# its per-backend tls_client_cert_file / tls_client_key_file). Requires
# tls_cert_file / tls_key_file above.
# tls_client_ca_file: "/etc/fred/docker-backend/tls/client-ca.pem"
#
# Client-identity pinning (recommended unless the client CA is dedicated solely
# to providerd): mTLS only proves the client cert was signed by the CA, not who
# the client is. List the allowed client certificate CommonNames / DNS SANs to
# reject any other cert the CA may have signed. Empty = accept any such cert.
# tls_client_allowed_names:
#   - providerd
#
# NOTE: certificates are loaded once at startup; rotation requires a restart
# (hot-reload is tracked in ENG-294).
```

- [ ] **Step 3: Sanity-check YAML validity**

Run: `go test ./cmd/docker-backend/... -run TestLoadConfig` (the loadconfig tests parse the example shape).
Expected: PASS. If no test exercises the example file, instead run `python3 -c "import yaml; yaml.safe_load(open('config.example.yaml')); yaml.safe_load(open('docker-backend.example.yaml')); print('ok')"`.
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add config.example.yaml docker-backend.example.yaml
git commit -m "docs(config): document TLS/mTLS fields for the backend hop (ENG-103)"
```

---

## Task 8: Full-suite verification + acceptance-criteria check

- [ ] **Step 1: Full build, vet, race-enabled test run**

Run:
```bash
go build ./...
go vet ./...
go test ./internal/tlsconfig/... ./internal/backend/... ./internal/config/... ./cmd/docker-backend/... ./cmd/providerd/... -race
```
Expected: all PASS, no race reports.

- [ ] **Step 2: Lint (project linter)**

Run: `golangci-lint run ./internal/tlsconfig/... ./internal/backend/... ./internal/config/... ./cmd/...`
Expected: clean. (If the `//nolint:gosec` on `InsecureSkipVerify` is rejected by config, adjust to the repo's accepted suppression style.)

- [ ] **Step 3: Manual smoke (optional, requires a Docker host)**

Generate throwaway certs, start docker-backend with TLS, and curl it:
```bash
# CA + server cert (SAN: localhost,127.0.0.1)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout /tmp/key.pem -out /tmp/cert.pem -days 1 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
# set tls_cert_file/tls_key_file in docker-backend.yaml, start it, then:
curl --cacert /tmp/cert.pem https://localhost:9001/health
```
Expected: `{"status":"healthy"}` over TLS. (Skip if no Docker host is available; behavior is already covered by Task 1's end-to-end tests.)

- [ ] **Step 4: Confirm acceptance criteria**

Tick each against the work:
- [ ] docker-backend listens on HTTPS when configured — Task 3 + Task 1 e2e.
- [ ] docker-backend requires client certs (mTLS) when configured — Task 3 (`tls_client_ca_file`) + `TestEndToEnd_MTLS_RejectsMissingClientCert`.
- [ ] docker-backend pins client identity when configured — Task 2/3 (`tls_client_allowed_names`) + `TestEndToEnd_MTLS_Allowlist*`.
- [ ] providerd dials backends over HTTPS with a private CA — Task 5/6 + `TestEndToEnd_MTLS_RoundTrip`.
- [ ] providerd presents a client cert (mTLS) when configured — Task 4/5/6 + `TestEndToEnd_MTLS_RoundTrip`.
- [ ] Plaintext HTTP remains the default; existing configs keep working — nil-config paths in Tasks 3/5/6 + unchanged existing tests.
- [ ] `production_mode` rejects `tls_skip_verify: true` for backends — Task 4 (`TestConfig_Validate_BackendTLS`).
- [ ] `config.example.yaml` + `docker-backend.example.yaml` document the new fields — Task 7.

- [ ] **Step 5: Final commit (if any lint/verification fixups were needed)**

```bash
git add -A
git commit -m "chore(eng-103): verification fixups"
```

---

## Self-Review

**Spec coverage:** every ENG-103 acceptance criterion maps to a task (see Task 8 Step 4). Server-side HTTPS+mTLS → Tasks 2/3; client-identity pinning → Tasks 2/3; client-side CA+mTLS → Tasks 4/5/6; plaintext default → nil-paths throughout; production_mode skip-verify → Task 4; docs → Task 7. The shared builder is Task 1.

**Placeholder scan:** no TBD/“add error handling”/“similar to” — every code step is complete, including the cert-generation helper and the name-pinner.

**Type consistency:** `tlsconfig.ServerConfig(certFile, keyFile, clientCAFile string, allowedClientNames []string)` and `tlsconfig.ClientConfig(caFile string, skipVerify bool, clientCertFile, clientKeyFile string)` are used with identical signatures in Tasks 1, 3, and 6. `HTTPClientConfig.TLSClientConfig *tls.Config` is defined in Task 5 and consumed in Task 6. Config field names (`TLSCertFile`/`TLSKeyFile`/`TLSClientCAFile`/`TLSClientAllowedNames` on docker.Config; `TLSCAFile`/`TLSSkipVerify`/`TLSClientCertFile`/`TLSClientKeyFile` on BackendConfig) are consistent across struct, validation, wiring, and docs.

**Best-practice review (web-verified):** TLS 1.3-only matches Mozilla's "Modern" profile for known/internal clients. Client-identity pinning via `VerifyPeerCertificate` addresses the documented `RequireAndVerifyClientCert` gap (chain-only, no identity). HTTP/2 deliberately not forced (behavior-preserving; avoids golang/go#20645). Server cert SAN requirement documented (Go ≥1.15).

**Out of scope (intentional):** `chain.buildTLSConfig` untouched; hot-reload (ENG-294); cert provisioning / manifest-deploy (ENG-104).
