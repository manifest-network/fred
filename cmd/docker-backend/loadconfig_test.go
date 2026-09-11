package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevInitDockerConfigTemplateIsValid(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "dev-init.sh"))
	require.NoError(t, err)

	const heredocStart = `cat >| "$DOCKER_BACKEND_TMP" <<YAML`
	start := strings.Index(string(script), heredocStart)
	require.NotEqual(t, -1, start, "docker config template not found")
	template := string(script)[start+len(heredocStart):]
	end := strings.Index(template, "\nYAML\n")
	require.NotEqual(t, -1, end, "docker config template terminator not found")
	template = template[:end]

	callbackSecret := strings.Repeat("a", 32) + `"\suffix`
	callbackSecretJSON, err := json.Marshal(callbackSecret)
	require.NoError(t, err)
	values := map[string]string{
		"CALLBACK_DB_PATH_JSON":           `"/work/fred/callbacks.db"`,
		"CALLBACK_SECRET_JSON":            string(callbackSecretJSON),
		"DIAGNOSTICS_DB_PATH_JSON":        `"/work/fred/diagnostics.db"`,
		"DOCKER_BACKEND_LISTEN_ADDR_JSON": `":9001"`,
		"RELEASES_DB_PATH_JSON":           `"/work/fred/releases.db"`,
		"RETENTION_DB_PATH_JSON":          `"/work/fred/retention.db"`,
		"SKU_DOCKER_LARGE_JSON":           `"550e8400-e29b-41d4-a716-446655440004"`,
		"SKU_DOCKER_MEDIUM_JSON":          `"550e8400-e29b-41d4-a716-446655440003"`,
		"SKU_DOCKER_MICRO_JSON":           `"550e8400-e29b-41d4-a716-446655440001"`,
		"SKU_DOCKER_SMALL_JSON":           `"550e8400-e29b-41d4-a716-446655440002"`,
		"VOLUME_DATA_PATH_JSON":           `"/home/fred/fred-volumes"`,
		"VOLUME_MOUNT_PATH_JSON":          `"/"`,
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

	configPath := filepath.Join(t.TempDir(), "docker-backend.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(generated), 0o600))
	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.NoError(t, cfg.Validate(), "dev-init must generate a config accepted by docker-backend")
	require.Equal(t, "/", cfg.VolumeMountPath)
	require.Equal(t, "/work/fred/callbacks.db", cfg.CallbackDBPath)
	require.Equal(t, "/work/fred/releases.db", cfg.ReleasesDBPath)
	require.Equal(t, "/work/fred/retention.db", cfg.RetentionDBPath)
	require.Equal(t, "/work/fred/diagnostics.db", cfg.DiagnosticsDBPath)
	require.Equal(t, callbackSecret, string(cfg.CallbackSecret),
		"JSON-compatible YAML quoting must preserve arbitrary HMAC secret bytes")
}

// TestLoadConfig_PartialSKUProfiles_DoesNotMergeDefaults is the ENG-238
// regression test. A YAML config that declares a single sku_profiles entry
// must produce a Config whose SKUProfiles map contains exactly that one
// entry — no defaults bleeding through from DefaultConfig() via yaml.v3's
// map-key merge.
func TestLoadConfig_PartialSKUProfiles_DoesNotMergeDefaults(t *testing.T) {
	const yamlInput = `
name: docker
listen_addr: ":9001"
docker_host: "unix:///var/run/docker.sock"
host_address: "192.168.1.100"
callback_secret: "this-is-a-32-character-secret!!x"
total_cpu_cores: 8.0
total_memory_mb: 16384
total_disk_mb: 102400
sku_mapping:
  "019c1ee8-9d9c-7003-8d01-4188dfe9e204": "docker-large"
sku_profiles:
  docker-large:
    cpu_cores: 2.0
    memory_mb: 2048
    disk_mb: 4096
volume_data_path: "/var/lib/fred/volumes"
volume_mount_path: "/var/lib/fred/volumes"
volume_filesystem: "btrfs"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "docker-backend.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlInput), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err)

	// The bug: yaml.v3 merges into a pre-populated map, so SKUProfiles
	// would end up with docker-micro, docker-small, docker-medium AND
	// docker-large. After the ENG-238 fix, only docker-large is present.
	assert.Len(t, cfg.SKUProfiles, 1, "SKUProfiles must contain only the operator-declared tier; got %v", cfg.SKUProfiles)
	assert.Contains(t, cfg.SKUProfiles, "docker-large")
	assert.NotContains(t, cfg.SKUProfiles, "docker-micro")
	assert.NotContains(t, cfg.SKUProfiles, "docker-small")
	assert.NotContains(t, cfg.SKUProfiles, "docker-medium")

	// And the loaded config must pass Validate — the whole point of the
	// fix is that a config with one declared tier and one mapping is now
	// accepted instead of rejected as "unreachable from on-chain SKUs".
	require.NoError(t, cfg.Validate())
}

// TestLoadConfig_MissingSKUProfiles_FailsValidation locks in the new
// contract: without an operator-supplied sku_profiles block, Validate
// produces a clear, immediate error instead of silently using defaults.
func TestLoadConfig_MissingSKUProfiles_FailsValidation(t *testing.T) {
	const yamlInput = `
name: docker
listen_addr: ":9001"
docker_host: "unix:///var/run/docker.sock"
host_address: "192.168.1.100"
callback_secret: "this-is-a-32-character-secret!!x"
total_cpu_cores: 8.0
total_memory_mb: 16384
total_disk_mb: 102400
`
	dir := t.TempDir()
	path := filepath.Join(dir, "docker-backend.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlInput), 0o600))

	cfg, err := loadConfig(path)
	require.NoError(t, err) // loadConfig itself does not validate

	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one SKU profile is required")
}
