package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

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

func TestParseManifest_Env(t *testing.T) {
	t.Run("safe env allowed", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"APP_PORT": "8080", "DATABASE_URL": "postgres://localhost/db"}}`

		m, err := ParseManifest([]byte(data))
		require.NoError(t, err)
		assert.Equal(t, "8080", m.Env["APP_PORT"])
	})

	t.Run("PATH blocked", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"PATH": "/usr/local/bin:/usr/bin"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "not allowed")
	})

	t.Run("PATH case insensitive", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"Path": "/usr/local/bin"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "not allowed")
	})

	t.Run("LD_PRELOAD blocked", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"LD_PRELOAD": "/tmp/malicious.so"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "not allowed")
	})

	t.Run("LD_LIBRARY_PATH blocked", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"LD_LIBRARY_PATH": "/tmp"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "prefix")
	})

	t.Run("LD_AUDIT blocked", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"LD_AUDIT": "/tmp/audit.so"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "prefix")
	})

	t.Run("ld_preload case insensitive", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"ld_preload": "/tmp/lib.so"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "not allowed")
	})

	t.Run("DOCKER_ prefix blocked", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"DOCKER_HOST": "tcp://attacker:2375"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "prefix")
	})

	t.Run("FRED_ prefix blocked", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"FRED_INTERNAL": "value"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "prefix")
	})

	t.Run("empty name rejected", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"": "value"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "empty")
	})

	t.Run("name with equals rejected", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"FOO=BAR": "value"}}`

		_, err := ParseManifest([]byte(data))
		assert.ErrorContains(t, err, "invalid character")
	})

	t.Run("mixed safe and blocked rejects all", func(t *testing.T) {
		data := `{"image": "nginx", "env": {"APP_PORT": "8080", "LD_PRELOAD": "/tmp/lib.so"}}`

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
	assert.True(t, cfg.IsDiskQuota())
	assert.Equal(t, "callbacks.db", cfg.CallbackDBPath)
	assert.Equal(t, "diagnostics.db", cfg.DiagnosticsDBPath)
	assert.Equal(t, 7*24*time.Hour, cfg.DiagnosticsMaxAge)
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

	cfg.ContainerDiskQuota = ptrBool(false)
	assert.False(t, cfg.IsDiskQuota())
}

func TestIsDiskQuota_NilDefaultsTrue(t *testing.T) {
	cfg := Config{}
	assert.True(t, cfg.IsDiskQuota())
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

	t.Run("valid tenant quota", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: 4096,
			MaxDiskMB:   50000,
		}
		assert.NoError(t, cfg.Validate())
	})

	t.Run("tenant quota zero cpu", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: 0,
			MaxMemoryMB: 4096,
			MaxDiskMB:   50000,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max_cpu_cores")
	})

	t.Run("tenant quota zero memory", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: 0,
			MaxDiskMB:   50000,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max_memory_mb")
	})

	t.Run("tenant quota zero disk", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: 4096,
			MaxDiskMB:   0,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max_disk_mb")
	})

	t.Run("tenant quota exceeds total cpu", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: cfg.TotalCPUCores + 1,
			MaxMemoryMB: 4096,
			MaxDiskMB:   50000,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds total_cpu_cores")
	})

	t.Run("tenant quota exceeds total memory", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: cfg.TotalMemoryMB + 1,
			MaxDiskMB:   50000,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds total_memory_mb")
	})

	t.Run("tenant quota exceeds total disk", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = &TenantQuotaConfig{
			MaxCPUCores: 2.0,
			MaxMemoryMB: 4096,
			MaxDiskMB:   cfg.TotalDiskMB + 1,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds total_disk_mb")
	})

	t.Run("nil tenant quota is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.TenantQuota = nil
		assert.NoError(t, cfg.Validate())
	})

	t.Run("negative diagnostics_max_age rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.DiagnosticsMaxAge = -1
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "diagnostics_max_age")
	})

	t.Run("zero diagnostics_max_age is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DiagnosticsMaxAge = 0
		assert.NoError(t, cfg.Validate())
	})
}

func TestParseManifest_Tmpfs(t *testing.T) {
	t.Run("valid tmpfs paths", func(t *testing.T) {
		data := `{
			"image": "nginx:latest",
			"ports": {"80/tcp": {}},
			"tmpfs": ["/var/cache/nginx", "/var/log/nginx"]
		}`
		m, err := ParseManifest([]byte(data))
		require.NoError(t, err)
		assert.Equal(t, []string{"/var/cache/nginx", "/var/log/nginx"}, m.Tmpfs)
	})

	t.Run("empty tmpfs is valid", func(t *testing.T) {
		data := `{"image": "nginx:latest", "tmpfs": []}`
		m, err := ParseManifest([]byte(data))
		require.NoError(t, err)
		assert.Empty(t, m.Tmpfs)
	})

	t.Run("no tmpfs field is valid", func(t *testing.T) {
		data := `{"image": "nginx:latest"}`
		m, err := ParseManifest([]byte(data))
		require.NoError(t, err)
		assert.Nil(t, m.Tmpfs)
	})

	t.Run("relative path rejected", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["var/cache"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be absolute")
	})

	t.Run("root path rejected", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "root filesystem")
	})

	t.Run("/tmp rejected (managed by backend)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/tmp"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "managed by the backend")
	})

	t.Run("/run rejected (managed by backend)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/run"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "managed by the backend")
	})

	t.Run("/proc rejected (sensitive)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/proc"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sensitive path")
	})

	t.Run("/sys rejected (sensitive)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/sys"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sensitive path")
	})

	t.Run("/dev rejected (sensitive)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/dev"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sensitive path")
	})

	t.Run("duplicate paths rejected", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/var/log", "/var/log"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})

	t.Run("paths are cleaned before duplicate check", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/var/log/", "/var/log"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
	})

	t.Run("too many mounts rejected", func(t *testing.T) {
		paths := make([]string, maxTmpfsMounts+1)
		for i := range paths {
			paths[i] = fmt.Sprintf("/mnt/vol%d", i)
		}
		data, _ := json.Marshal(DockerManifest{Image: "nginx", Tmpfs: paths})
		_, err := ParseManifest(data)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many mounts")
	})

	t.Run("path with .. rejected after cleaning to blocked path", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/tmp/../proc"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		// After path.Clean, "/tmp/../proc" becomes "/proc" which is blocked
		assert.Contains(t, err.Error(), "sensitive path")
	})

	t.Run("/proc/self rejected (subdirectory of sensitive path)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/proc/self"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "under sensitive path")
	})

	t.Run("/sys/fs/cgroup rejected (subdirectory of sensitive path)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/sys/fs/cgroup"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "under sensitive path")
	})

	t.Run("/dev/shm rejected (subdirectory of sensitive path)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/dev/shm"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "under sensitive path")
	})

	t.Run("/run/secrets rejected (subdirectory of managed path)", func(t *testing.T) {
		data := `{"image": "nginx", "tmpfs": ["/run/secrets"]}`
		_, err := ParseManifest([]byte(data))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "under sensitive path")
	})
}

func TestHasActiveHealthCheck(t *testing.T) {
	tests := []struct {
		name     string
		manifest DockerManifest
		expected bool
	}{
		{
			name:     "nil health check",
			manifest: DockerManifest{Image: "busybox:latest"},
			expected: false,
		},
		{
			name:     "empty test slice",
			manifest: DockerManifest{Image: "busybox:latest", HealthCheck: &HealthCheckConfig{Test: []string{}}},
			expected: false,
		},
		{
			name:     "NONE disables health check",
			manifest: DockerManifest{Image: "busybox:latest", HealthCheck: &HealthCheckConfig{Test: []string{"NONE"}}},
			expected: false,
		},
		{
			name:     "CMD is active",
			manifest: DockerManifest{Image: "busybox:latest", HealthCheck: &HealthCheckConfig{Test: []string{"CMD", "true"}}},
			expected: true,
		},
		{
			name:     "CMD-SHELL is active",
			manifest: DockerManifest{Image: "busybox:latest", HealthCheck: &HealthCheckConfig{Test: []string{"CMD-SHELL", "true"}}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.manifest.HasActiveHealthCheck())
		})
	}
}

func TestIsPortBindingError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"port already allocated", fmt.Errorf("failed to create container: port is already allocated"), true},
		{"address already in use", fmt.Errorf("Bind for 0.0.0.0:8080: address already in use"), true},
		{"unrelated error", fmt.Errorf("disk full"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isPortBindingError(tt.err))
		})
	}
}

func TestHasEphemeralPorts(t *testing.T) {
	tests := []struct {
		name     string
		ports    map[string]PortConfig
		expected bool
	}{
		{"nil ports", nil, false},
		{"empty ports", map[string]PortConfig{}, false},
		{"ephemeral port", map[string]PortConfig{"80/tcp": {HostPort: 0}}, true},
		{"explicit port only", map[string]PortConfig{"80/tcp": {HostPort: 8080}}, false},
		{"mixed ports", map[string]PortConfig{"80/tcp": {HostPort: 0}, "443/tcp": {HostPort: 8443}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hasEphemeralPorts(tt.ports))
		})
	}
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

func TestCheckDaemonCapabilities(t *testing.T) {
	t.Run("disk quota warning on non-xfs", func(t *testing.T) {
		mock := &mockDockerClient{
			DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
				return DaemonSecurityInfo{
					StorageDriver:     "overlay2",
					BackingFilesystem: "ext4",
					SecurityOptions:   []string{"name=seccomp,profile=default"},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		// disk quota is true by default — should warn about ext4
		b.checkDaemonCapabilities(context.Background())
		// Verify no panic; warning is logged (not assertable without a test logger).
	})

	t.Run("disk quota warning on non-overlay2", func(t *testing.T) {
		mock := &mockDockerClient{
			DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
				return DaemonSecurityInfo{
					StorageDriver:     "devicemapper",
					BackingFilesystem: "xfs",
					SecurityOptions:   []string{"name=seccomp,profile=default"},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.checkDaemonCapabilities(context.Background())
	})

	t.Run("no disk quota warning when disabled", func(t *testing.T) {
		mock := &mockDockerClient{
			DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
				return DaemonSecurityInfo{
					StorageDriver:     "devicemapper",
					BackingFilesystem: "ext4",
					SecurityOptions:   []string{"name=seccomp,profile=default"},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.cfg.ContainerDiskQuota = ptrBool(false)
		b.checkDaemonCapabilities(context.Background())
	})

	t.Run("seccomp warning when missing", func(t *testing.T) {
		mock := &mockDockerClient{
			DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
				return DaemonSecurityInfo{
					StorageDriver:     "overlay2",
					BackingFilesystem: "xfs",
					SecurityOptions:   []string{"name=apparmor"},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.checkDaemonCapabilities(context.Background())
	})

	t.Run("no warnings when everything is correct", func(t *testing.T) {
		mock := &mockDockerClient{
			DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
				return DaemonSecurityInfo{
					StorageDriver:     "overlay2",
					BackingFilesystem: "xfs",
					SecurityOptions:   []string{"name=seccomp,profile=default"},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.checkDaemonCapabilities(context.Background())
	})

	t.Run("daemon info error is non-fatal", func(t *testing.T) {
		mock := &mockDockerClient{
			DaemonInfoFn: func(ctx context.Context) (DaemonSecurityInfo, error) {
				return DaemonSecurityInfo{}, fmt.Errorf("connection refused")
			},
		}
		b := newBackendForTest(mock, nil)
		b.checkDaemonCapabilities(context.Background())
		// Should not panic; warning is logged and startup continues.
	})
}

func TestUpdateResourceMetrics(t *testing.T) {
	t.Run("sets ratios correctly", func(t *testing.T) {
		updateResourceMetrics(shared.ResourceStats{
			TotalCPU:          8.0,
			AllocatedCPU:      2.0,
			TotalMemoryMB:     16384,
			AllocatedMemoryMB: 4096,
			TotalDiskMB:       102400,
			AllocatedDiskMB:   51200,
		})

		assert.InDelta(t, 0.25, testutil.ToFloat64(resourceCPUAllocatedRatio), 0.001)
		assert.InDelta(t, 0.25, testutil.ToFloat64(resourceMemoryAllocatedRatio), 0.001)
		assert.InDelta(t, 0.5, testutil.ToFloat64(resourceDiskAllocatedRatio), 0.001)
	})

	t.Run("zero totals skips update", func(t *testing.T) {
		// Reset to known values first
		resourceCPUAllocatedRatio.Set(0.99)
		resourceMemoryAllocatedRatio.Set(0.99)
		resourceDiskAllocatedRatio.Set(0.99)

		updateResourceMetrics(shared.ResourceStats{
			TotalCPU:      0,
			TotalMemoryMB: 0,
			TotalDiskMB:   0,
		})

		// Values should remain at 0.99 since zero totals are skipped
		assert.InDelta(t, 0.99, testutil.ToFloat64(resourceCPUAllocatedRatio), 0.001)
		assert.InDelta(t, 0.99, testutil.ToFloat64(resourceMemoryAllocatedRatio), 0.001)
		assert.InDelta(t, 0.99, testutil.ToFloat64(resourceDiskAllocatedRatio), 0.001)
	})
}
