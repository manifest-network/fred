package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDevInitProviderConfigTemplateIsValid(t *testing.T) {
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PROVIDER_") {
			t.Setenv(name, "")
		}
	}

	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "dev-init.sh"))
	require.NoError(t, err)

	const heredocStart = `cat > "$CONFIG_DOCKER_FILE" <<YAML`
	start := strings.Index(string(script), heredocStart)
	require.NotEqual(t, -1, start, "provider config template not found")

	template := string(script)[start+len(heredocStart):]
	end := strings.Index(template, "\nYAML\n")
	require.NotEqual(t, -1, end, "provider config template terminator not found")
	template = template[:end]

	values := map[string]string{
		"CALLBACK_SECRET":  strings.Repeat("a", 32),
		"CERT_FILE":        "/tmp/fred-cert.pem",
		"CHAIN_HOME":       "/tmp/manifest",
		"CHAIN_ID":         "manifest-dev",
		"KEYRING_BACKEND":  "test",
		"KEY_FILE":         "/tmp/fred-key.pem",
		"KEY_NAME":         "provider",
		"PROVIDER_ADDRESS": "manifest1provider",
		"PROVIDER_UUID":    "01234567-89ab-cdef-0123-456789abcdef",
		"REPO_ROOT":        "/tmp/fred",
	}
	var unknown []string
	generated := os.Expand(template, func(name string) string {
		value, ok := values[name]
		if !ok {
			unknown = append(unknown, name)
		}
		return value
	})
	require.Empty(t, unknown, "test fixture must expand every template variable")

	configPath := filepath.Join(t.TempDir(), "config.docker.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(generated), 0o600))

	cfg, err := Load(configPath)
	require.NoError(t, err, "dev-init must generate a config accepted by providerd")
	require.Equal(t, "/tmp/fred/placements.db", cfg.PlacementStoreDBPath)
}
