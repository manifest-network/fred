package k3s

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
)

// validConfig returns a Config that passes Validate().
// Tests mutate a copy to trigger specific errors.
func validConfig() Config {
	return Config{
		Name:              "k3s",
		ListenAddr:        ":9002",
		TotalCPUCores:     8.0,
		TotalMemoryMB:     16384,
		TotalDiskMB:       102400,
		ReconcileInterval: 5 * time.Minute,
		HostAddress:       "192.168.1.100",
		CallbackSecret:    config.Secret(strings.Repeat("x", 32)),
		AllowedRegistries: []string{"docker.io"},
		SKUMapping: map[string]string{
			"sku-uuid-1": "k3s-small",
		},
		SKUProfiles: map[string]SKUProfile{
			"k3s-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
		},
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

	t.Run("valid config with stateless SKUs passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles = map[string]SKUProfile{
			"stateless": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0},
		}
		cfg.SKUMapping = map[string]string{"sku-uuid-stateless": "stateless"}
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
			name:    "zero reconcile_interval",
			mutate:  func(c *Config) { c.ReconcileInterval = 0 },
			wantErr: "reconcile_interval must be positive",
		},
		{
			name:    "negative reconcile_interval",
			mutate:  func(c *Config) { c.ReconcileInterval = -1 },
			wantErr: "reconcile_interval must be positive",
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
		// "k3s-small" remains mapped via "sku-uuid-1"; "orphan" has no mapping
		// entry, so the bidirectional unreachability check at config.go:248-260
		// rejects it.
		cfg.SKUProfiles["orphan"] = SKUProfile{CPUCores: 1, MemoryMB: 1024, DiskMB: 1024}
		err := cfg.Validate()
		require.Error(t, err)
		assert.ErrorContains(t, err, "has no sku_mapping entry and is unreachable")
		assert.ErrorContains(t, err, "orphan")
	})

	t.Run("no mapping skips unreachability check", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUMapping = nil
		// With no mapping at all, the reverse check is gated off.
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
		// Validate uses strict ">" comparison, so equality is accepted.
		cfg.TenantQuota = &shared.TenantQuotaConfig{
			MaxCPUCores: cfg.TotalCPUCores,
			MaxMemoryMB: cfg.TotalMemoryMB,
			MaxDiskMB:   cfg.TotalDiskMB,
		}
		require.NoError(t, cfg.Validate())
	})
}

func TestConfig_Validate_IngressPlaceholder(t *testing.T) {
	t.Run("disabled passes", func(t *testing.T) {
		cfg := validConfig()
		cfg.Ingress = IngressConfig{Enabled: false}
		require.NoError(t, cfg.Validate())
	})

	t.Run("enabled rejected with exact message", func(t *testing.T) {
		cfg := validConfig()
		cfg.Ingress.Enabled = true
		err := cfg.Validate()
		require.Error(t, err)
		// Exact match — the design pins this string so callers can grep for
		// it operationally. Any drift here is a must-fix.
		assert.EqualError(t, err, "ingress not supported in k3s-backend scaffold")
	})
}

func TestConfig_DefaultConfig_Validates(t *testing.T) {
	cfg := DefaultConfig()
	// DefaultConfig leaves required secrets / addresses empty so the user is
	// forced to fill them. Fill the minimum to make Validate pass — and fill
	// SKUMapping for every default profile so the bidirectional unreachability
	// check is satisfied.
	cfg.CallbackSecret = config.Secret(strings.Repeat("s", 32))
	cfg.HostAddress = "192.168.1.1"
	cfg.SKUMapping = map[string]string{
		"uuid-1": "k3s-micro",
		"uuid-2": "k3s-small",
		"uuid-3": "k3s-medium",
		"uuid-4": "k3s-large",
	}
	require.NoError(t, cfg.Validate())
}

func TestConfig_GetSKUProfile(t *testing.T) {
	cfg := validConfig()

	t.Run("direct lookup", func(t *testing.T) {
		// When the input is a profile name (not a UUID), the function falls
		// through the mapping check and looks up the profile directly.
		profile, err := cfg.GetSKUProfile("k3s-small")
		require.NoError(t, err)
		assert.Equal(t, 0.5, profile.CPUCores)
	})

	t.Run("mapped lookup", func(t *testing.T) {
		// "sku-uuid-1" resolves to "k3s-small" via SKUMapping.
		profile, err := cfg.GetSKUProfile("sku-uuid-1")
		require.NoError(t, err)
		assert.Equal(t, 0.5, profile.CPUCores)
	})

	t.Run("unknown SKU wraps ErrUnknownSKU", func(t *testing.T) {
		_, err := cfg.GetSKUProfile("nonexistent")
		require.Error(t, err)
		assert.ErrorIs(t, err, backend.ErrUnknownSKU,
			"GetSKUProfile must wrap backend.ErrUnknownSKU so errors.Is works downstream")
		assert.ErrorContains(t, err, "nonexistent")
	})
}
