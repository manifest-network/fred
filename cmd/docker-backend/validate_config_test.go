package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validateConfigYAML = `
name: docker
listen_addr: ":9001"
docker_host: "unix:///var/run/docker.sock"
host_address: "192.168.1.100"
callback_secret: "this-is-a-32-character-secret!!x"
total_cpu_cores: 8.0
total_memory_mb: 16384
total_disk_mb: 102400
sku_profiles:
  "550e8400-e29b-41d4-a716-446655440001":
    cpu_cores: 0.5
    memory_mb: 512
    disk_mb: 0
`

func writeValidateConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "docker-backend.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestParseStartupFlagsValidateConfig(t *testing.T) {
	startup, err := parseStartupFlags([]string{"-config", "/etc/fred/docker-backend.yaml", "--validate-config"}, nil)
	require.NoError(t, err)
	assert.True(t, startup.validateConfig)
	assert.Equal(t, "/etc/fred/docker-backend.yaml", startup.configPath)
}

func TestValidateStartupConfigRunsEveryPreSideEffectCheck(t *testing.T) {
	cfg, err := validateStartupConfig(writeValidateConfig(t, validateConfigYAML))
	require.NoError(t, err)
	assert.Equal(t, "docker", cfg.Name)

	for name, test := range map[string]struct {
		contents string
		env      map[string]string
		want     string
	}{
		"unknown key": {
			contents: validateConfigYAML + "no_such_key: true\n",
			want:     "failed to load config",
		},
		"log level": {
			contents: validateConfigYAML + "log_level: loud\n",
			want:     "invalid log_level in config",
		},
		"semantic validation": {
			contents: validateConfigYAML[:strings.Index(validateConfigYAML, "sku_profiles:")],
			want:     "at least one SKU profile is required",
		},
		"environment override": {
			contents: validateConfigYAML,
			env:      map[string]string{"DOCKER_BACKEND_HOST_ADDRESS": "not an address"},
			want:     "invalid config",
		},
	} {
		t.Run(name, func(t *testing.T) {
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			_, err := validateStartupConfig(writeValidateConfig(t, test.contents))
			require.ErrorContains(t, err, test.want)
		})
	}
}
