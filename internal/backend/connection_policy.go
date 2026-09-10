package backend

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/httpurl"
	"github.com/manifest-network/fred/internal/tlsconfig"
)

// ConnectionConfig is the configuration-time input for a backend connection.
// Configured CA/client credentials are loaded once; callers never supply a
// mutable tls.Config. With no CA file, Go's standard system-root lookup applies.
// Skip verification and plaintext are development options rejected by the
// provider's production configuration validation.
type ConnectionConfig struct {
	Name              string
	BaseURL           string
	Secret            string
	Timeout           time.Duration
	TLSCAFile         string
	TLSSkipVerify     bool
	TLSClientCertFile string
	TLSClientKeyFile  string
}

// ConnectionPolicy is an immutable, validated backend connection. Both the
// inventory-only and identity-bound factories require this same policy, so
// system-root trust cannot accidentally select a different protocol floor.
// Its zero value is invalid and cannot construct a usable client.
type ConnectionPolicy struct {
	state *connectionPolicy
}

type connectionPolicy struct {
	name      string
	baseURL   string
	secret    string
	timeout   time.Duration
	tlsConfig *tls.Config
}

// NewConnectionPolicy validates connection settings and captures privately
// owned configured TLS material. HTTPS always requires TLS 1.3, with either
// system roots or the configured private CA; clients cannot override it after
// construction.
func NewConnectionPolicy(cfg ConnectionConfig) (ConnectionPolicy, error) {
	if cfg.Timeout < 0 {
		return ConnectionPolicy{}, errors.New("backend timeout must not be negative")
	}
	if len(cfg.Secret) < hmacauth.MinSecretLength {
		return ConnectionPolicy{}, fmt.Errorf("backend HMAC secret must be at least %d bytes, got %d", hmacauth.MinSecretLength, len(cfg.Secret))
	}
	origin, err := httpurl.NormalizeOrigin(cfg.BaseURL)
	if err != nil {
		return ConnectionPolicy{}, fmt.Errorf("backend base URL: %w", err)
	}
	trust, err := tlsconfig.ClientConfig(cfg.TLSCAFile, cfg.TLSSkipVerify, cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
	if err != nil {
		return ConnectionPolicy{}, fmt.Errorf("backend TLS policy: %w", err)
	}
	return ConnectionPolicy{state: &connectionPolicy{
		name: cfg.Name, baseURL: origin, secret: cfg.Secret,
		timeout: cfg.Timeout, tlsConfig: trust,
	}}, nil
}

func (policy ConnectionPolicy) valid() bool { return policy.state != nil }

// Format and LogValue prevent reflective formatting from disclosing credentials.
func (policy ConnectionPolicy) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("backend.ConnectionPolicy{redacted}"))
}

func (policy ConnectionPolicy) LogValue() slog.Value {
	return slog.StringValue("backend.ConnectionPolicy{redacted}")
}
