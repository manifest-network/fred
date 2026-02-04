package docker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRegistry(t *testing.T) {
	tests := []struct {
		image    string
		expected string
	}{
		// Docker Hub official images
		{"nginx", "docker.io"},
		{"nginx:latest", "docker.io"},
		{"nginx:1.25-alpine", "docker.io"},
		{"library/nginx", "docker.io"},
		{"library/nginx:latest", "docker.io"},

		// Docker Hub user images
		{"myorg/myapp", "docker.io"},
		{"myorg/myapp:v1", "docker.io"},
		{"myorg/myapp:v1.2.3", "docker.io"},

		// Other registries
		{"ghcr.io/org/app", "ghcr.io"},
		{"ghcr.io/org/app:latest", "ghcr.io"},
		{"gcr.io/project/image", "gcr.io"},
		{"gcr.io/project/image:tag", "gcr.io"},
		{"registry.example.com/image", "registry.example.com"},
		{"registry.example.com/org/image:tag", "registry.example.com"},
		{"registry.example.com:5000/image", "registry.example.com"},

		// Localhost
		{"localhost/image", "localhost"},
		{"localhost:5000/image", "localhost"},

		// With digests
		{"nginx@sha256:abc123", "docker.io"},
		{"ghcr.io/org/app@sha256:abc123", "ghcr.io"},
	}

	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			result := ParseRegistry(tt.image)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsImageAllowed(t *testing.T) {
	allowed := []string{"docker.io", "ghcr.io"}

	tests := []struct {
		image    string
		expected bool
	}{
		{"nginx", true},
		{"nginx:latest", true},
		{"ghcr.io/org/app", true},
		{"gcr.io/project/image", false},
		{"registry.example.com/image", false},
	}

	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			result := IsImageAllowed(tt.image, allowed)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateImage(t *testing.T) {
	allowed := []string{"docker.io", "ghcr.io"}

	tests := []struct {
		name      string
		image     string
		expectErr bool
	}{
		{"valid docker hub", "nginx:latest", false},
		{"valid ghcr", "ghcr.io/org/app", false},
		{"invalid registry", "gcr.io/project/image", true},
		{"empty image", "", true},
		{"invalid format colon prefix", ":latest", true},
		{"invalid format slash prefix", "/image", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateImage(tt.image, allowed)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestResourcePool(t *testing.T) {
	profiles := map[string]SKUProfile{
		"small": {CPUCores: 1.0, MemoryMB: 512, DiskMB: 1024},
		"large": {CPUCores: 4.0, MemoryMB: 4096, DiskMB: 8192},
	}

	// Helper to create a resolver from profiles map
	makeResolver := func(profiles map[string]SKUProfile) SKUResolver {
		return func(sku string) (SKUProfile, error) {
			if p, ok := profiles[sku]; ok {
				return p, nil
			}
			return SKUProfile{}, fmt.Errorf("unknown SKU: %s", sku)
		}
	}

	t.Run("allocate and release", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		// Allocate
		err := pool.TryAllocate("lease-1", "small")
		require.NoError(t, err)

		stats := pool.Stats()
		assert.Equal(t, 1.0, stats.AllocatedCPU)
		assert.Equal(t, int64(512), stats.AllocatedMemory)

		// Release
		pool.Release("lease-1")

		stats = pool.Stats()
		assert.Equal(t, 0.0, stats.AllocatedCPU)
	})

	t.Run("insufficient resources", func(t *testing.T) {
		// Small pool
		pool := NewResourcePool(2.0, 1024, 2048, makeResolver(profiles))

		// First allocation succeeds
		err := pool.TryAllocate("lease-1", "small")
		require.NoError(t, err)

		// Second allocation should fail (not enough for large)
		err = pool.TryAllocate("lease-2", "large")
		assert.Error(t, err)
	})

	t.Run("unknown SKU", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		err := pool.TryAllocate("lease-1", "nonexistent")
		assert.Error(t, err)
	})

	t.Run("duplicate allocation", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		err := pool.TryAllocate("lease-1", "small")
		require.NoError(t, err)

		err = pool.TryAllocate("lease-1", "small")
		assert.Error(t, err)
	})

	t.Run("release nonexistent", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		// Should not panic
		pool.Release("nonexistent")
	})

	t.Run("reset", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		// Allocate something
		pool.TryAllocate("lease-1", "small")

		// Reset with different allocations
		allocations := []ResourceAllocation{
			{LeaseUUID: "lease-2", SKU: "large", CPUCores: 4.0, MemoryMB: 4096, DiskMB: 8192},
		}
		pool.Reset(allocations)

		stats := pool.Stats()
		assert.Equal(t, 1, stats.AllocationCount)
		assert.Equal(t, 4.0, stats.AllocatedCPU)

		// Original allocation should be gone
		alloc := pool.GetAllocation("lease-1")
		assert.Nil(t, alloc)
	})
}

func TestParseManifest(t *testing.T) {
	t.Run("valid manifest", func(t *testing.T) {
		data := `{
			"image": "nginx:1.25-alpine",
			"ports": {
				"80/tcp": {},
				"443/tcp": {"host_port": 8443}
			},
			"env": {
				"NGINX_HOST": "example.com"
			},
			"command": ["/bin/sh", "-c", "echo hello"],
			"args": ["--config", "/etc/app.yaml"],
			"labels": {"app": "webserver"},
			"health_check": {
				"test": ["CMD", "curl", "-f", "http://localhost/health"],
				"interval": "30s",
				"timeout": "5s",
				"retries": 3
			}
		}`

		m, err := ParseManifest([]byte(data))
		require.NoError(t, err)

		assert.Equal(t, "nginx:1.25-alpine", m.Image)
		assert.Len(t, m.Ports, 2)
		assert.Equal(t, 8443, m.Ports["443/tcp"].HostPort)
		assert.Equal(t, "example.com", m.Env["NGINX_HOST"])
		assert.Equal(t, 30*time.Second, m.HealthCheck.Interval.Duration())
	})

	t.Run("minimal manifest", func(t *testing.T) {
		data := `{"image": "nginx"}`

		m, err := ParseManifest([]byte(data))
		require.NoError(t, err)

		assert.Equal(t, "nginx", m.Image)
	})

	t.Run("empty manifest", func(t *testing.T) {
		_, err := ParseManifest([]byte{})
		assert.Error(t, err)
	})

	t.Run("missing image", func(t *testing.T) {
		data := `{"ports": {"80/tcp": {}}}`

		_, err := ParseManifest([]byte(data))
		assert.Error(t, err)
	})

	t.Run("invalid port spec", func(t *testing.T) {
		data := `{"image": "nginx", "ports": {"invalid": {}}}`

		_, err := ParseManifest([]byte(data))
		assert.Error(t, err)
	})

	t.Run("reserved label prefix", func(t *testing.T) {
		data := `{"image": "nginx", "labels": {"fred.custom": "value"}}`

		_, err := ParseManifest([]byte(data))
		assert.Error(t, err)
	})

	t.Run("invalid host_port", func(t *testing.T) {
		data := `{"image": "nginx", "ports": {"80/tcp": {"host_port": 99999}}}`

		_, err := ParseManifest([]byte(data))
		assert.Error(t, err)
	})
}

func TestConfigValidation(t *testing.T) {
	validConfig := func() Config {
		cfg := DefaultConfig()
		cfg.CallbackSecret = "this-is-a-32-character-secret!!x" // 33 chars
		cfg.HostAddress = "192.168.1.100"
		return cfg
	}

	t.Run("valid config", func(t *testing.T) {
		cfg := validConfig()
		assert.NoError(t, cfg.Validate())
	})

	t.Run("missing name", func(t *testing.T) {
		cfg := validConfig()
		cfg.Name = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing listen addr", func(t *testing.T) {
		cfg := validConfig()
		cfg.ListenAddr = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("no SKU profiles", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("invalid SKU profile", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles["invalid"] = SKUProfile{CPUCores: 0} // Invalid: zero CPU
		assert.Error(t, cfg.Validate())
	})

	t.Run("no allowed registries", func(t *testing.T) {
		cfg := validConfig()
		cfg.AllowedRegistries = nil
		assert.Error(t, cfg.Validate())
	})

	t.Run("short callback secret", func(t *testing.T) {
		cfg := validConfig()
		cfg.CallbackSecret = "short"
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing host address", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = ""
		assert.Error(t, cfg.Validate())
	})

	t.Run("host address with URL rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "http://example.com"
		assert.Error(t, cfg.Validate())
	})

	t.Run("bare hostname rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "myhostname"
		assert.Error(t, cfg.Validate())
	})

	t.Run("localhost accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "localhost"
		assert.NoError(t, cfg.Validate())
	})

	t.Run("FQDN accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "backend.example.com"
		assert.NoError(t, cfg.Validate())
	})
}

func TestDuration(t *testing.T) {
	t.Run("unmarshal string", func(t *testing.T) {
		var d Duration
		err := json.Unmarshal([]byte(`"30s"`), &d)
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, d.Duration())
	})

	t.Run("unmarshal number", func(t *testing.T) {
		var d Duration
		err := json.Unmarshal([]byte(`1000000000`), &d) // 1 second in nanoseconds
		require.NoError(t, err)
		assert.Equal(t, time.Second, d.Duration())
	})

	t.Run("marshal", func(t *testing.T) {
		d := Duration(30 * time.Second)
		data, err := json.Marshal(d)
		require.NoError(t, err)
		assert.Equal(t, `"30s"`, string(data))
	})
}

func TestHealthCheckValidation(t *testing.T) {
	t.Run("valid CMD", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"CMD", "curl", "-f", "http://localhost/health"},
		}
		assert.NoError(t, hc.Validate())
	})

	t.Run("valid CMD-SHELL", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"CMD-SHELL", "curl -f http://localhost/health"},
		}
		assert.NoError(t, hc.Validate())
	})

	t.Run("valid NONE", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"NONE"},
		}
		assert.NoError(t, hc.Validate())
	})

	t.Run("empty test", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{},
		}
		assert.Error(t, hc.Validate())
	})

	t.Run("CMD without command", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"CMD"},
		}
		assert.Error(t, hc.Validate())
	})

	t.Run("invalid test type", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"INVALID", "command"},
		}
		assert.Error(t, hc.Validate())
	})

	t.Run("negative retries", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test:    []string{"CMD", "curl", "-f", "http://localhost/health"},
			Retries: -1,
		}
		assert.Error(t, hc.Validate())
	})
}

func TestPortSpecValidation(t *testing.T) {
	tests := []struct {
		spec      string
		expectErr bool
	}{
		{"80/tcp", false},
		{"443/tcp", false},
		{"53/udp", false},
		{"8080/tcp", false},
		{"65535/tcp", false},
		{"1/tcp", false},

		// Invalid
		{"80", true},           // Missing protocol
		{"tcp/80", true},       // Wrong order
		{"80/http", true},      // Invalid protocol
		{"0/tcp", true},        // Port too low
		{"65536/tcp", true},    // Port too high
		{"-1/tcp", true},       // Negative port
		{"abc/tcp", true},      // Non-numeric port
		{"80/tcp/extra", true}, // Extra component
	}

	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			err := validatePortSpec(tt.spec)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSKUMapping(t *testing.T) {
	cfg := Config{
		SKUMapping: map[string]string{
			"019c1ee7-1aaf-7000-802c-ad775c72cc27": "docker-small",
			"019c1ee7-1aaf-7000-802c-ad775c72cc28": "docker-large",
		},
		SKUProfiles: map[string]SKUProfile{
			"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
			"docker-large": {CPUCores: 2.0, MemoryMB: 2048, DiskMB: 4096},
		},
	}

	t.Run("UUID maps to profile", func(t *testing.T) {
		profile, err := cfg.GetSKUProfile("019c1ee7-1aaf-7000-802c-ad775c72cc27")
		require.NoError(t, err)
		assert.Equal(t, 0.5, profile.CPUCores)
		assert.Equal(t, int64(512), profile.MemoryMB)
	})

	t.Run("direct profile name still works", func(t *testing.T) {
		profile, err := cfg.GetSKUProfile("docker-large")
		require.NoError(t, err)
		assert.Equal(t, 2.0, profile.CPUCores)
	})

	t.Run("unknown UUID fails", func(t *testing.T) {
		_, err := cfg.GetSKUProfile("unknown-uuid")
		assert.Error(t, err)
	})

	t.Run("mapped to nonexistent profile fails validation", func(t *testing.T) {
		badCfg := Config{
			Name:          "test",
			ListenAddr:    ":9001",
			DockerHost:    "unix:///var/run/docker.sock",
			TotalCPUCores: 8.0,
			TotalMemoryMB: 16384,
			TotalDiskMB:   102400,
			SKUMapping: map[string]string{
				"some-uuid": "nonexistent-profile",
			},
			SKUProfiles: map[string]SKUProfile{
				"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024},
			},
			AllowedRegistries:     []string{"docker.io"},
			CallbackSecret:        "this-is-a-32-character-secret!!",
			HostAddress:           "192.168.1.100",
			ImagePullTimeout:      5 * time.Minute,
			ContainerStartTimeout: 30 * time.Second,
			ReconcileInterval:     5 * time.Minute,
		}
		err := badCfg.Validate()
		assert.Error(t, err)
	})
}

func TestConfigHardeningDefaults(t *testing.T) {
	cfg := DefaultConfig()

	assert.True(t, cfg.IsNetworkIsolation())
	assert.True(t, cfg.IsReadonlyRootfs())
	assert.Equal(t, int64(256), *cfg.GetPidsLimit())
	assert.Equal(t, 64, cfg.GetTmpfsSizeMB())
	assert.Equal(t, "0.0.0.0", cfg.GetHostBindIP())
}

func TestConfigHardeningOverrides(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NetworkIsolation = ptrBool(false)
	cfg.ContainerReadonlyRootfs = ptrBool(false)
	cfg.ContainerPidsLimit = ptrInt64(512)
	cfg.ContainerTmpfsSizeMB = 128
	cfg.HostBindIP = "127.0.0.1"

	assert.False(t, cfg.IsNetworkIsolation())
	assert.False(t, cfg.IsReadonlyRootfs())
	assert.Equal(t, int64(512), *cfg.GetPidsLimit())
	assert.Equal(t, 128, cfg.GetTmpfsSizeMB())
	assert.Equal(t, "127.0.0.1", cfg.GetHostBindIP())
}

func TestConfigHardeningValidation(t *testing.T) {
	validConfig := func() Config {
		cfg := DefaultConfig()
		cfg.CallbackSecret = "this-is-a-32-character-secret!!x"
		cfg.HostAddress = "192.168.1.100"
		return cfg
	}

	t.Run("invalid host_bind_ip", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostBindIP = "not-an-ip"
		assert.Error(t, cfg.Validate())
	})

	t.Run("valid host_bind_ip", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostBindIP = "127.0.0.1"
		assert.NoError(t, cfg.Validate())
	})

	t.Run("empty host_bind_ip is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostBindIP = ""
		assert.NoError(t, cfg.Validate())
	})

	t.Run("pids_limit too low", func(t *testing.T) {
		cfg := validConfig()
		cfg.ContainerPidsLimit = ptrInt64(0)
		assert.Error(t, cfg.Validate())
	})
}

func TestTenantNetworkName(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		name1 := TenantNetworkName("manifest1abc")
		name2 := TenantNetworkName("manifest1abc")
		assert.Equal(t, name1, name2)
	})

	t.Run("different tenants get different names", func(t *testing.T) {
		name1 := TenantNetworkName("tenant-a")
		name2 := TenantNetworkName("tenant-b")
		assert.NotEqual(t, name1, name2)
	})

	t.Run("starts with prefix", func(t *testing.T) {
		name := TenantNetworkName("any-tenant")
		assert.True(t, strings.HasPrefix(name, "fred-tenant-"))
	})
}
