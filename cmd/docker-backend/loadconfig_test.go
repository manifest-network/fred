package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
