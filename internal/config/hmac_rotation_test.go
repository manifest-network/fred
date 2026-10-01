package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	rotationSecretA = "backend-a-secret-0123456789abcdef"
	rotationSecretB = "backend-b-secret-0123456789abcdef"
	rotationOldA    = "backend-a-old-secret-0123456789ab"
)

func perBackendRotationConfig() Config {
	cfg := validConfig()
	cfg.CallbackSecret = ""
	cfg.Backends = []BackendConfig{
		{Name: "backend-a", URL: "http://backend-a:9000", IsDefault: true, HMACSecret: rotationSecretA},
		{Name: "backend-b", URL: "http://backend-b:9000", HMACSecret: rotationSecretB},
	}
	return cfg
}

func TestConfig_Validate_HMACSecretPrevious(t *testing.T) {
	t.Run("a previous key during a rotation", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[0].HMACSecretPrevious = rotationOldA
		require.NoError(t, cfg.Validate())
	})

	t.Run("refused without the backend's own hmac_secret", func(t *testing.T) {
		cfg := validConfig() // legacy shared callback_secret mode
		cfg.Backends[0].HMACSecretPrevious = rotationOldA
		require.ErrorContains(t, cfg.Validate(), "backends[0].hmac_secret_previous requires hmac_secret on the same backend")
	})

	t.Run("refused when short", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[0].HMACSecretPrevious = "short"
		require.ErrorContains(t, cfg.Validate(), "backends[0].hmac_secret_previous must be at least")
	})

	t.Run("refused when it is the backend's own key", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[0].HMACSecretPrevious = RotationSecret(rotationSecretA)
		require.ErrorContains(t, cfg.Validate(),
			"backends[0].hmac_secret_previous duplicates backends[0].hmac_secret")
	})

	t.Run("refused when another backend accepts an equivalent key", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[1].HMACSecretPrevious = RotationSecret(rotationSecretA + "\x00")
		require.ErrorContains(t, cfg.Validate(),
			"backends[1].hmac_secret_previous duplicates backends[0].hmac_secret",
			"a zero-padded copy is the same HMAC key")
	})

	t.Run("current keys are compared by equivalence too", func(t *testing.T) {
		cfg := perBackendRotationConfig()
		cfg.Backends[1].HMACSecret = Secret(rotationSecretA + "\x00")
		require.ErrorContains(t, cfg.Validate(), "backends[1].hmac_secret duplicates backends[0].hmac_secret")
	})
}

func TestConfig_BackendCallbackKeys(t *testing.T) {
	cfg := perBackendRotationConfig()
	cfg.Backends[0].HMACSecretPrevious = rotationOldA

	keys, err := cfg.BackendCallbackKeys("backend-a")
	require.NoError(t, err)
	assert.True(t, keys.HasRotation())
	keys, err = cfg.BackendCallbackKeys("backend-b")
	require.NoError(t, err)
	assert.True(t, keys.Valid())
	assert.False(t, keys.HasRotation())

	_, err = cfg.BackendCallbackKeys("missing")
	assert.ErrorContains(t, err, "not configured")
	legacy := validConfig()
	_, err = legacy.BackendCallbackKeys(legacy.Backends[0].Name)
	assert.ErrorContains(t, err, "no per-backend HMAC secret", "the legacy shared secret is not a keyring")
}

func TestLoad_HMACSecretPreviousDecodesAsARedactedRotationSecret(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`
chain_id: "test-chain"
grpc_endpoint: "localhost:9090"
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
    hmac_secret_previous: "docker-1-old-secret-0123456789ab"
    default: true
`), 0o600))

	cfg, err := Load(configPath)
	require.NoError(t, err)
	assert.Equal(t, RotationSecret("docker-1-old-secret-0123456789ab"), cfg.Backends[0].HMACSecretPrevious)
	assert.Equal(t, "[REDACTED]", cfg.Backends[0].HMACSecretPrevious.String())
}
