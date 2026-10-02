package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_Validate_BackendFence(t *testing.T) {
	t.Run("one fenced backend, including the default", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[0].Fenced = true
		require.True(t, cfg.Backends[0].IsDefault)
		require.NoError(t, cfg.Validate())
	})

	t.Run("refused when every backend is fenced", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[0].Fenced = true
		cfg.Backends[1].Fenced = true
		require.ErrorContains(t, cfg.Validate(), "every backend is fenced")
	})

	t.Run("refused under the shared legacy callback_secret", func(t *testing.T) {
		cfg := validConfig()
		cfg.Backends = append(cfg.Backends, BackendConfig{Name: "other", URL: "http://localhost:9001"})
		cfg.Backends[1].Fenced = true
		require.ErrorContains(t, cfg.Validate(),
			"backends[1].fenced requires a per-backend hmac_secret on every backend")
	})
}

func TestConfig_BackendConnectionPolicyLoadsNothingForAFencedBackend(t *testing.T) {
	cfg := perBackendRotationConfig()
	cfg.Backends[0].URL = "https://backend-a:9000"
	cfg.Backends[0].TLSCAFile = filepath.Join(t.TempDir(), "revoked-ca.pem") // never written
	require.NoError(t, cfg.Validate())

	_, err := cfg.BackendConnectionPolicy("backend-a")
	require.Error(t, err, "an unfenced backend loads its CA file")

	cfg.Backends[0].Fenced = true
	policy, err := cfg.BackendConnectionPolicy("backend-a")
	require.NoError(t, err, "a fenced backend's TLS files may already be gone")
	require.True(t, policy.Fenced())

	live, err := cfg.BackendConnectionPolicy("backend-b")
	require.NoError(t, err)
	require.False(t, live.Fenced())
}

func TestLoad_BackendFencedDecodes(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`
provider_uuid: "01234567-89ab-cdef-0123-456789abcdef"
provider_address: "manifest1abc"
key_name: "provider"
keyring_dir: "/home/provider/.manifest"
callback_base_url: "http://localhost:8080"
placement_store_db_path: "/var/lib/fred/placements.db"
backends:
  - name: "docker-1"
    url: "http://10.0.0.1:9000"
    hmac_secret: "docker-1-secret-0123456789abcdef"
    default: true
  - name: "docker-2"
    url: "http://10.0.0.2:9000"
    hmac_secret: "docker-2-secret-0123456789abcdef"
    fenced: true
`), 0o600))

	cfg, err := Load(configPath)
	require.NoError(t, err)
	require.False(t, cfg.Backends[0].Fenced)
	require.True(t, cfg.Backends[1].Fenced)
}
