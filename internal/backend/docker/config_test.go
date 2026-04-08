package docker

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
)

// validConfig returns a Config that passes Validate().
// Tests mutate individual fields to trigger specific errors.
func validConfig() Config {
	return Config{
		Name:                   "docker",
		ListenAddr:             ":9001",
		DockerHost:             "unix:///var/run/docker.sock",
		TotalCPUCores:          8.0,
		TotalMemoryMB:          16384,
		TotalDiskMB:            102400,
		ImagePullTimeout:       300_000_000_000, // 5m
		ContainerCreateTimeout: 30_000_000_000,  // 30s
		ContainerStartTimeout:  30_000_000_000,  // 30s
		ReconcileInterval:      300_000_000_000, // 5m
		ProvisionTimeout:       600_000_000_000, // 10m
		HostAddress:            "192.168.1.100",
		CallbackSecret:         config.Secret(strings.Repeat("x", 32)),
		AllowedRegistries:      []string{"docker.io"},
		SKUMapping: map[string]string{
			"sku-uuid-1": "small",
		},
		SKUProfiles: map[string]SKUProfile{
			"small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
		},
		VolumeDataPath: "/data/volumes",
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		cfg := validConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("valid config without SKU mapping passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUMapping = nil
		require.NoError(t, cfg.Validate())
	})

	t.Run("valid config with stateless SKUs and no volume path", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles = map[string]SKUProfile{
			"stateless": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0},
		}
		cfg.SKUMapping = map[string]string{"sku-uuid-1": "stateless"}
		cfg.VolumeDataPath = ""
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_RequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "empty name",
			mutate:  func(c *Config) { c.Name = "" },
			wantErr: "name is required",
		},
		{
			name:    "empty listen_addr",
			mutate:  func(c *Config) { c.ListenAddr = "" },
			wantErr: "listen_addr is required",
		},
		{
			name:    "empty docker_host",
			mutate:  func(c *Config) { c.DockerHost = "" },
			wantErr: "docker_host is required",
		},
		{
			name:    "empty host_address",
			mutate:  func(c *Config) { c.HostAddress = "" },
			wantErr: "host_address is required",
		},
		{
			name:    "empty callback_secret",
			mutate:  func(c *Config) { c.CallbackSecret = "" },
			wantErr: "callback_secret is required",
		},
		{
			name:    "no allowed registries",
			mutate:  func(c *Config) { c.AllowedRegistries = nil },
			wantErr: "at least one allowed registry is required",
		},
		{
			name:    "no SKU profiles",
			mutate:  func(c *Config) { c.SKUProfiles = nil },
			wantErr: "at least one SKU profile is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfig_Validate_PositiveValues(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "zero total_cpu_cores",
			mutate:  func(c *Config) { c.TotalCPUCores = 0 },
			wantErr: "total_cpu_cores must be positive",
		},
		{
			name:    "negative total_cpu_cores",
			mutate:  func(c *Config) { c.TotalCPUCores = -1 },
			wantErr: "total_cpu_cores must be positive",
		},
		{
			name:    "zero total_memory_mb",
			mutate:  func(c *Config) { c.TotalMemoryMB = 0 },
			wantErr: "total_memory_mb must be positive",
		},
		{
			name:    "negative total_memory_mb",
			mutate:  func(c *Config) { c.TotalMemoryMB = -1 },
			wantErr: "total_memory_mb must be positive",
		},
		{
			name:    "zero total_disk_mb",
			mutate:  func(c *Config) { c.TotalDiskMB = 0 },
			wantErr: "total_disk_mb must be positive",
		},
		{
			name:    "negative total_disk_mb",
			mutate:  func(c *Config) { c.TotalDiskMB = -1 },
			wantErr: "total_disk_mb must be positive",
		},
		{
			name:    "zero image_pull_timeout",
			mutate:  func(c *Config) { c.ImagePullTimeout = 0 },
			wantErr: "image_pull_timeout must be positive",
		},
		{
			name:    "negative image_pull_timeout",
			mutate:  func(c *Config) { c.ImagePullTimeout = -1 },
			wantErr: "image_pull_timeout must be positive",
		},
		{
			name:    "zero container_create_timeout",
			mutate:  func(c *Config) { c.ContainerCreateTimeout = 0 },
			wantErr: "container_create_timeout must be positive",
		},
		{
			name:    "zero container_start_timeout",
			mutate:  func(c *Config) { c.ContainerStartTimeout = 0 },
			wantErr: "container_start_timeout must be positive",
		},
		{
			name:    "zero reconcile_interval",
			mutate:  func(c *Config) { c.ReconcileInterval = 0 },
			wantErr: "reconcile_interval must be positive",
		},
		{
			name:    "zero provision_timeout",
			mutate:  func(c *Config) { c.ProvisionTimeout = 0 },
			wantErr: "provision_timeout must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfig_Validate_CallbackSecret(t *testing.T) {
	t.Run("too short", func(t *testing.T) {
		cfg := validConfig()
		cfg.CallbackSecret = config.Secret("short")
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "callback_secret must be at least 32 characters")
	})

	t.Run("exactly 32 chars passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.CallbackSecret = config.Secret(strings.Repeat("a", 32))
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_HostAddress(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr string
	}{
		{name: "valid IPv4", addr: "192.168.1.1"},
		{name: "valid IPv6", addr: "::1"},
		{name: "valid hostname", addr: "example.com"},
		{name: "valid localhost", addr: "localhost"},
		{name: "valid IP with port", addr: "192.168.1.1:8080"},
		{name: "valid subdomain", addr: "host.example.com"},
		{name: "url with scheme", addr: "http://example.com", wantErr: "must be a hostname or IP, not a URL"},
		{name: "url with slash", addr: "example.com/path", wantErr: "must be a hostname or IP, not a URL"},
		{name: "contains space", addr: "example .com", wantErr: "must be a hostname or IP, not a URL"},
		{name: "bare word no dot", addr: "notahostname", wantErr: "not a valid hostname or IP"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.HostAddress = tt.addr
			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_HostBindIP(t *testing.T) {
	tests := []struct {
		name    string
		ip      string
		wantErr string
	}{
		{name: "empty is valid", ip: ""},
		{name: "all interfaces", ip: "0.0.0.0"},
		{name: "loopback", ip: "127.0.0.1"},
		{name: "IPv6 loopback", ip: "::1"},
		{name: "invalid IP", ip: "not-an-ip", wantErr: "host_bind_ip is not a valid IP address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.HostBindIP = tt.ip
			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_SKUProfiles(t *testing.T) {
	t.Run("invalid profile rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles["bad"] = SKUProfile{CPUCores: 0, MemoryMB: 512}
		cfg.SKUMapping["sku-uuid-bad"] = "bad"
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "cpu_cores")
	})
}

func TestConfig_Validate_SKUMapping(t *testing.T) {
	t.Run("mapping references unknown profile", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUMapping["sku-uuid-2"] = "nonexistent"
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "references unknown profile")
		assert.ErrorContains(t, err, "nonexistent")
	})

	t.Run("unreachable profile", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles["orphan"] = SKUProfile{CPUCores: 1, MemoryMB: 1024}
		// SKUMapping only maps to "small", not "orphan"
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "has no sku_mapping entry and is unreachable")
		assert.ErrorContains(t, err, "orphan")
	})

	t.Run("no mapping skips unreachability check", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUMapping = nil
		// With no mapping at all, the unreachability check is skipped
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_ContainerLimits(t *testing.T) {
	t.Run("pids_limit zero rejected", func(t *testing.T) {
		cfg := validConfig()
		v := int64(0)
		cfg.ContainerPidsLimit = &v
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "container_pids_limit must be >= 1")
	})

	t.Run("pids_limit negative rejected", func(t *testing.T) {
		cfg := validConfig()
		v := int64(-1)
		cfg.ContainerPidsLimit = &v
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "container_pids_limit must be >= 1")
	})

	t.Run("pids_limit nil is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.ContainerPidsLimit = nil
		require.NoError(t, cfg.Validate())
	})

	t.Run("pids_limit 1 is valid", func(t *testing.T) {
		cfg := validConfig()
		v := int64(1)
		cfg.ContainerPidsLimit = &v
		require.NoError(t, cfg.Validate())
	})

	t.Run("negative tmpfs_size_mb rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.ContainerTmpfsSizeMB = -1
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "container_tmpfs_size_mb must be >= 0")
	})

	t.Run("zero tmpfs_size_mb is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.ContainerTmpfsSizeMB = 0
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_MaxAge(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "negative callback_max_age",
			mutate:  func(c *Config) { c.CallbackMaxAge = -1 },
			wantErr: "callback_max_age must be non-negative",
		},
		{
			name:    "negative diagnostics_max_age",
			mutate:  func(c *Config) { c.DiagnosticsMaxAge = -1 },
			wantErr: "diagnostics_max_age must be non-negative",
		},
		{
			name:    "negative releases_max_age",
			mutate:  func(c *Config) { c.ReleasesMaxAge = -1 },
			wantErr: "releases_max_age must be non-negative",
		},
		{
			name:   "zero max ages are valid",
			mutate: func(c *Config) { c.CallbackMaxAge = 0; c.DiagnosticsMaxAge = 0; c.ReleasesMaxAge = 0 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestConfig_Validate_Volume(t *testing.T) {
	t.Run("stateful SKU without volume_data_path", func(t *testing.T) {
		cfg := validConfig()
		cfg.VolumeDataPath = ""
		// Default SKU has DiskMB > 0
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "volume_data_path is required when any SKU profile has disk_mb > 0")
	})

	t.Run("volume_data_path with whitespace", func(t *testing.T) {
		cfg := validConfig()
		cfg.VolumeDataPath = "/data/my volumes"
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "volume_data_path must not contain whitespace")
	})

	t.Run("volume_data_path with tab", func(t *testing.T) {
		cfg := validConfig()
		cfg.VolumeDataPath = "/data/\tvolumes"
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "volume_data_path must not contain whitespace")
	})

	t.Run("valid volume_filesystem values", func(t *testing.T) {
		for _, fs := range []string{"btrfs", "xfs", "zfs"} {
			cfg := validConfig()
			cfg.VolumeFilesystem = fs
			assert.NoError(t, cfg.Validate(), "filesystem %q should be valid", fs)
		}
	})

	t.Run("invalid volume_filesystem", func(t *testing.T) {
		cfg := validConfig()
		cfg.VolumeFilesystem = "ext4"
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "volume_filesystem must be btrfs, xfs, or zfs")
	})

	t.Run("empty volume_filesystem is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.VolumeFilesystem = ""
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_TenantQuota(t *testing.T) {
	t.Run("valid quota", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: 4.0,
			MaxMemoryMB: 8192,
			MaxDiskMB:   51200,
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("nil quota is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = nil
		require.NoError(t, cfg.Validate())
	})

	t.Run("invalid quota propagates error", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: 0, // invalid
			MaxMemoryMB: 8192,
			MaxDiskMB:   51200,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "tenant_quota.")
		assert.ErrorContains(t, err, "max_cpu_cores")
	})

	t.Run("quota cpu exceeds total", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: 100.0, // exceeds TotalCPUCores (8.0)
			MaxMemoryMB: 8192,
			MaxDiskMB:   51200,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "tenant_quota.max_cpu_cores")
		assert.ErrorContains(t, err, "exceeds total_cpu_cores")
	})

	t.Run("quota memory exceeds total", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: 4.0,
			MaxMemoryMB: 999999, // exceeds TotalMemoryMB
			MaxDiskMB:   51200,
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "tenant_quota.max_memory_mb")
		assert.ErrorContains(t, err, "exceeds total_memory_mb")
	})

	t.Run("quota disk exceeds total", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: 4.0,
			MaxMemoryMB: 8192,
			MaxDiskMB:   999999, // exceeds TotalDiskMB
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "tenant_quota.max_disk_mb")
		assert.ErrorContains(t, err, "exceeds total_disk_mb")
	})

	t.Run("quota exactly at total is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: cfg.TotalCPUCores,
			MaxMemoryMB: cfg.TotalMemoryMB,
			MaxDiskMB:   cfg.TotalDiskMB,
		}
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_IngressRequiresNetworkIsolation(t *testing.T) {
	ingress := IngressConfig{
		Enabled:        true,
		WildcardDomain: "example.com",
		Entrypoint:     "websecure",
	}

	t.Run("ingress with network_isolation disabled", func(t *testing.T) {
		cfg := validConfig()
		cfg.NetworkIsolation = ptrBool(false)
		cfg.Ingress = ingress
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "ingress requires network_isolation to be enabled")
	})

	t.Run("ingress with network_isolation enabled", func(t *testing.T) {
		cfg := validConfig()
		cfg.NetworkIsolation = ptrBool(true)
		cfg.Ingress = ingress
		require.NoError(t, cfg.Validate())
	})

	t.Run("ingress with network_isolation default", func(t *testing.T) {
		cfg := validConfig()
		cfg.NetworkIsolation = nil // defaults to true
		cfg.Ingress = ingress
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_DefaultConfig_Validates(t *testing.T) {
	cfg := DefaultConfig()
	// DefaultConfig is missing required secrets/addresses — set them
	cfg.CallbackSecret = config.Secret(strings.Repeat("s", 32))
	cfg.HostAddress = "192.168.1.1"
	cfg.SKUMapping = map[string]string{
		"uuid-1": "docker-micro",
		"uuid-2": "docker-small",
		"uuid-3": "docker-medium",
		"uuid-4": "docker-large",
	}
	cfg.VolumeDataPath = "/data/volumes"
	require.NoError(t, cfg.Validate())
}

func TestConfig_GetSKUProfile(t *testing.T) {
	cfg := validConfig()

	t.Run("direct lookup", func(t *testing.T) {
		// When no mapping exists, lookup by profile name
		profile, err := cfg.GetSKUProfile("small")
		require.NoError(t, err)
		assert.Equal(t, 0.5, profile.CPUCores)
	})

	t.Run("mapped lookup", func(t *testing.T) {
		profile, err := cfg.GetSKUProfile("sku-uuid-1")
		require.NoError(t, err)
		assert.Equal(t, 0.5, profile.CPUCores)
	})

	t.Run("unknown SKU", func(t *testing.T) {
		_, err := cfg.GetSKUProfile("nonexistent")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "nonexistent")
	})
}

func TestConfig_Helpers(t *testing.T) {
	t.Run("GetHostBindIP default", func(t *testing.T) {
		cfg := Config{}
		assert.Equal(t, "0.0.0.0", cfg.GetHostBindIP())
	})

	t.Run("GetHostBindIP custom", func(t *testing.T) {
		cfg := Config{HostBindIP: "127.0.0.1"}
		assert.Equal(t, "127.0.0.1", cfg.GetHostBindIP())
	})

	t.Run("IsNetworkIsolation default", func(t *testing.T) {
		cfg := Config{}
		assert.True(t, cfg.IsNetworkIsolation())
	})

	t.Run("IsNetworkIsolation disabled", func(t *testing.T) {
		cfg := Config{NetworkIsolation: ptrBool(false)}
		assert.False(t, cfg.IsNetworkIsolation())
	})

	t.Run("IsReadonlyRootfs default", func(t *testing.T) {
		cfg := Config{}
		assert.True(t, cfg.IsReadonlyRootfs())
	})

	t.Run("IsReadonlyRootfs disabled", func(t *testing.T) {
		cfg := Config{ContainerReadonlyRootfs: ptrBool(false)}
		assert.False(t, cfg.IsReadonlyRootfs())
	})

	t.Run("GetPidsLimit default", func(t *testing.T) {
		cfg := Config{}
		limit := cfg.GetPidsLimit()
		require.NotNil(t, limit)
		assert.Equal(t, int64(256), *limit)
	})

	t.Run("GetPidsLimit custom", func(t *testing.T) {
		cfg := Config{ContainerPidsLimit: ptrInt64(512)}
		limit := cfg.GetPidsLimit()
		require.NotNil(t, limit)
		assert.Equal(t, int64(512), *limit)
	})

	t.Run("GetTmpfsSizeMB default", func(t *testing.T) {
		cfg := Config{}
		assert.Equal(t, 64, cfg.GetTmpfsSizeMB())
	})

	t.Run("GetTmpfsSizeMB custom", func(t *testing.T) {
		cfg := Config{ContainerTmpfsSizeMB: 128}
		assert.Equal(t, 128, cfg.GetTmpfsSizeMB())
	})

	t.Run("HasStatefulSKUs false", func(t *testing.T) {
		cfg := Config{SKUProfiles: map[string]SKUProfile{
			"stateless": {CPUCores: 1, MemoryMB: 512, DiskMB: 0},
		}}
		assert.False(t, cfg.HasStatefulSKUs())
	})

	t.Run("HasStatefulSKUs true", func(t *testing.T) {
		cfg := Config{SKUProfiles: map[string]SKUProfile{
			"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
		}}
		assert.True(t, cfg.HasStatefulSKUs())
	})
}
