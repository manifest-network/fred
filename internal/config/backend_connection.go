package config

import (
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
)

// BackendConnectionPolicy composes the same validated connection policy for
// runtime, offline proof, and topology-change clients. Config validation owns
// production/development eligibility; backend policy construction owns the
// protocol floor and captures credentials without exposing a mutable TLS object.
func (c *Config) BackendConnectionPolicy(name string) (backend.ConnectionPolicy, error) {
	secret, err := c.ResolveBackendHMACSecret(name)
	if err != nil {
		return backend.ConnectionPolicy{}, err
	}
	for _, configured := range c.Backends {
		if configured.Name == name {
			return backend.NewConnectionPolicy(backend.ConnectionConfig{
				Name: configured.Name, BaseURL: configured.URL,
				Secret: string(secret), Timeout: configured.Timeout,
				TLSCAFile: configured.TLSCAFile, TLSSkipVerify: configured.TLSSkipVerify,
				TLSClientCertFile: configured.TLSClientCertFile,
				TLSClientKeyFile:  configured.TLSClientKeyFile,
			})
		}
	}
	return backend.ConnectionPolicy{}, fmt.Errorf("backend %q is not configured", name)
}
