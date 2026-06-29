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
			cfg.VerifyConnection = clientNamePinner(allowedClientNames)
		}
	}
	return cfg, nil
}

// ClientConfig builds a client-side *tls.Config. When caFile is non-empty it
// is used as the root CA set (otherwise the system roots apply). When both
// clientCertFile and clientKeyFile are non-empty, the pair is presented for
// mutual TLS; supplying only one of the two is an error. skipVerify disables
// server certificate verification (dev only).
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
	if (clientCertFile != "") != (clientKeyFile != "") {
		return nil, fmt.Errorf("client cert and key must both be set or both empty")
	}
	if clientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// clientNamePinner returns a VerifyConnection callback that passes only when
// the verified client leaf's CommonName or a DNS SAN is in allowed. It uses
// VerifyConnection (not VerifyPeerCertificate) so the check also runs on
// resumed TLS connections; RequireAndVerifyClientCert has already verified
// the chain, so VerifiedChains is normally populated.
func clientNamePinner(allowed []string) func(tls.ConnectionState) error {
	set := make(map[string]struct{}, len(allowed))
	for _, n := range allowed {
		set[n] = struct{}{}
	}
	return func(cs tls.ConnectionState) error {
		for _, chain := range cs.VerifiedChains {
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
		if len(cs.VerifiedChains) == 0 {
			return fmt.Errorf("client certificate identity not allowed: no verified chains")
		}
		leaf := cs.VerifiedChains[0][0]
		return fmt.Errorf("client certificate identity not allowed (CN=%q SANs=%v)",
			leaf.Subject.CommonName, leaf.DNSNames)
	}
}

// loadCertPool reads a PEM bundle and returns a pool containing its certs.
func loadCertPool(file string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file) //nolint:gosec // G304: file is an operator-configured TLS cert/CA path, read at startup — not tenant-reachable
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", file)
	}
	return pool, nil
}
