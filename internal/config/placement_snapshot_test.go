package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotConfig() Config {
	cfg := validConfig()
	cfg.PayloadStoreDBPath = "/var/lib/fred/payloads.db"
	cfg.PlacementSnapshotDir = "/var/backups/fred"
	cfg.PlacementSnapshotInterval = time.Hour
	cfg.PlacementSnapshotRetain = 24
	return cfg
}

func TestValidate_PlacementSnapshots(t *testing.T) {
	t.Run("disabled snapshots ignore their other settings", func(t *testing.T) {
		cfg := validConfig()
		require.Empty(t, cfg.PlacementSnapshotDir)
		require.Zero(t, cfg.PlacementSnapshotInterval)
		require.NoError(t, cfg.Validate())
	})

	t.Run("a complete configuration is accepted", func(t *testing.T) {
		cfg := snapshotConfig()
		require.NoError(t, cfg.Validate())
		cfg.PlacementSnapshotInterval = MinPlacementSnapshotInterval
		cfg.PlacementSnapshotRetain = MaxPlacementSnapshotRetain
		require.NoError(t, cfg.Validate())
	})

	for name, tc := range map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"relative directory": {
			mutate: func(c *Config) { c.PlacementSnapshotDir = "backups/fred" },
			want:   "placement_snapshot_dir must be an absolute, clean path",
		},
		"unclean directory": {
			mutate: func(c *Config) { c.PlacementSnapshotDir = "/var/backups/../backups/fred" },
			want:   "placement_snapshot_dir must be an absolute, clean path",
		},
		"no payload store": {
			mutate: func(c *Config) { c.PayloadStoreDBPath = "" },
			want:   "placement_snapshot_dir requires payload_store_db_path",
		},
		"the placement database's directory": {
			mutate: func(c *Config) { c.PlacementSnapshotDir = "/var/lib/fred" },
			want:   "must not be the directory of a live database",
		},
		"the payload database's directory": {
			mutate: func(c *Config) {
				c.PayloadStoreDBPath = "/srv/payloads/payloads.db"
				c.PlacementSnapshotDir = "/srv/payloads"
			},
			want: "must not be the directory of a live database",
		},
		"interval below the minimum": {
			mutate: func(c *Config) { c.PlacementSnapshotInterval = MinPlacementSnapshotInterval - time.Second },
			want:   "placement_snapshot_interval must be at least 5m0s",
		},
		"no sets retained": {
			mutate: func(c *Config) { c.PlacementSnapshotRetain = 0 },
			want:   "placement_snapshot_retain must be between 1 and 1000",
		},
		"too many sets retained": {
			mutate: func(c *Config) { c.PlacementSnapshotRetain = MaxPlacementSnapshotRetain + 1 },
			want:   "placement_snapshot_retain must be between 1 and 1000",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := snapshotConfig()
			tc.mutate(&cfg)
			require.ErrorContains(t, cfg.Validate(), tc.want)
		})
	}
}

func TestLoad_PlacementSnapshotDefaultsAndEnvironment(t *testing.T) {
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
payload_store_db_path: "/var/lib/fred/payloads.db"
backends:
  - name: "docker-1"
    url: "http://10.0.0.1:9000"
    hmac_secret: "docker-1-secret-0123456789abcdef"
    default: true
`), 0o600))

	cfg, err := Load(configPath)
	require.NoError(t, err)
	assert.Empty(t, cfg.PlacementSnapshotDir, "snapshots are off unless a directory is configured")
	assert.Equal(t, time.Hour, cfg.PlacementSnapshotInterval)
	assert.Equal(t, 24, cfg.PlacementSnapshotRetain)

	t.Setenv("PROVIDER_PLACEMENT_SNAPSHOT_DIR", "/var/backups/fred")
	t.Setenv("PROVIDER_PLACEMENT_SNAPSHOT_RETAIN", "48")
	cfg, err = Load(configPath)
	require.NoError(t, err)
	assert.Equal(t, "/var/backups/fred", cfg.PlacementSnapshotDir)
	assert.Equal(t, 48, cfg.PlacementSnapshotRetain)
}
