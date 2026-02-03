package docker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
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
			if result != tt.expected {
				t.Errorf("ParseRegistry(%q) = %q, want %q", tt.image, result, tt.expected)
			}
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
			if result != tt.expected {
				t.Errorf("IsImageAllowed(%q, %v) = %v, want %v", tt.image, allowed, result, tt.expected)
			}
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
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateImage(%q) error = %v, expectErr %v", tt.image, err, tt.expectErr)
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
		if err != nil {
			t.Fatalf("TryAllocate failed: %v", err)
		}

		stats := pool.Stats()
		if stats.AllocatedCPU != 1.0 {
			t.Errorf("AllocatedCPU = %v, want 1.0", stats.AllocatedCPU)
		}
		if stats.AllocatedMemory != 512 {
			t.Errorf("AllocatedMemory = %v, want 512", stats.AllocatedMemory)
		}

		// Release
		pool.Release("lease-1")

		stats = pool.Stats()
		if stats.AllocatedCPU != 0 {
			t.Errorf("AllocatedCPU after release = %v, want 0", stats.AllocatedCPU)
		}
	})

	t.Run("insufficient resources", func(t *testing.T) {
		// Small pool
		pool := NewResourcePool(2.0, 1024, 2048, makeResolver(profiles))

		// First allocation succeeds
		err := pool.TryAllocate("lease-1", "small")
		if err != nil {
			t.Fatalf("first allocation failed: %v", err)
		}

		// Second allocation should fail (not enough for large)
		err = pool.TryAllocate("lease-2", "large")
		if err == nil {
			t.Error("expected error for insufficient resources")
		}
	})

	t.Run("unknown SKU", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		err := pool.TryAllocate("lease-1", "nonexistent")
		if err == nil {
			t.Error("expected error for unknown SKU")
		}
	})

	t.Run("duplicate allocation", func(t *testing.T) {
		pool := NewResourcePool(8.0, 16384, 102400, makeResolver(profiles))

		err := pool.TryAllocate("lease-1", "small")
		if err != nil {
			t.Fatalf("first allocation failed: %v", err)
		}

		err = pool.TryAllocate("lease-1", "small")
		if err == nil {
			t.Error("expected error for duplicate allocation")
		}
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
		if stats.AllocationCount != 1 {
			t.Errorf("AllocationCount = %v, want 1", stats.AllocationCount)
		}
		if stats.AllocatedCPU != 4.0 {
			t.Errorf("AllocatedCPU = %v, want 4.0", stats.AllocatedCPU)
		}

		// Original allocation should be gone
		alloc := pool.GetAllocation("lease-1")
		if alloc != nil {
			t.Error("expected lease-1 to be gone after reset")
		}
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
		if err != nil {
			t.Fatalf("ParseManifest failed: %v", err)
		}

		if m.Image != "nginx:1.25-alpine" {
			t.Errorf("Image = %q, want nginx:1.25-alpine", m.Image)
		}
		if len(m.Ports) != 2 {
			t.Errorf("Ports count = %d, want 2", len(m.Ports))
		}
		if m.Ports["443/tcp"].HostPort != 8443 {
			t.Errorf("HostPort = %d, want 8443", m.Ports["443/tcp"].HostPort)
		}
		if m.Env["NGINX_HOST"] != "example.com" {
			t.Errorf("Env NGINX_HOST = %q, want example.com", m.Env["NGINX_HOST"])
		}
		if m.HealthCheck.Interval.Duration() != 30*time.Second {
			t.Errorf("HealthCheck.Interval = %v, want 30s", m.HealthCheck.Interval.Duration())
		}
	})

	t.Run("minimal manifest", func(t *testing.T) {
		data := `{"image": "nginx"}`

		m, err := ParseManifest([]byte(data))
		if err != nil {
			t.Fatalf("ParseManifest failed: %v", err)
		}

		if m.Image != "nginx" {
			t.Errorf("Image = %q, want nginx", m.Image)
		}
	})

	t.Run("empty manifest", func(t *testing.T) {
		_, err := ParseManifest([]byte{})
		if err == nil {
			t.Error("expected error for empty manifest")
		}
	})

	t.Run("missing image", func(t *testing.T) {
		data := `{"ports": {"80/tcp": {}}}`

		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("expected error for missing image")
		}
	})

	t.Run("invalid port spec", func(t *testing.T) {
		data := `{"image": "nginx", "ports": {"invalid": {}}}`

		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("expected error for invalid port spec")
		}
	})

	t.Run("reserved label prefix", func(t *testing.T) {
		data := `{"image": "nginx", "labels": {"fred.custom": "value"}}`

		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("expected error for reserved label prefix")
		}
	})

	t.Run("invalid host_port", func(t *testing.T) {
		data := `{"image": "nginx", "ports": {"80/tcp": {"host_port": 99999}}}`

		_, err := ParseManifest([]byte(data))
		if err == nil {
			t.Error("expected error for host_port > 65535")
		}
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
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate failed: %v", err)
		}
	})

	t.Run("missing name", func(t *testing.T) {
		cfg := validConfig()
		cfg.Name = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing name")
		}
	})

	t.Run("missing listen addr", func(t *testing.T) {
		cfg := validConfig()
		cfg.ListenAddr = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing listen_addr")
		}
	})

	t.Run("no SKU profiles", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles = nil
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for no SKU profiles")
		}
	})

	t.Run("invalid SKU profile", func(t *testing.T) {
		cfg := validConfig()
		cfg.SKUProfiles["invalid"] = SKUProfile{CPUCores: 0} // Invalid: zero CPU
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for invalid SKU profile")
		}
	})

	t.Run("no allowed registries", func(t *testing.T) {
		cfg := validConfig()
		cfg.AllowedRegistries = nil
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for no allowed registries")
		}
	})

	t.Run("short callback secret", func(t *testing.T) {
		cfg := validConfig()
		cfg.CallbackSecret = "short"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for short callback secret")
		}
	})

	t.Run("missing host address", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = ""
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing host address")
		}
	})

	t.Run("host address with URL rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "http://example.com"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for URL in host address")
		}
	})

	t.Run("bare hostname rejected", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "myhostname"
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for bare hostname without dot")
		}
	})

	t.Run("localhost accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "localhost"
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("FQDN accepted", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostAddress = "backend.example.com"
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestDuration(t *testing.T) {
	t.Run("unmarshal string", func(t *testing.T) {
		var d Duration
		err := json.Unmarshal([]byte(`"30s"`), &d)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if d.Duration() != 30*time.Second {
			t.Errorf("Duration = %v, want 30s", d.Duration())
		}
	})

	t.Run("unmarshal number", func(t *testing.T) {
		var d Duration
		err := json.Unmarshal([]byte(`1000000000`), &d) // 1 second in nanoseconds
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		if d.Duration() != time.Second {
			t.Errorf("Duration = %v, want 1s", d.Duration())
		}
	})

	t.Run("marshal", func(t *testing.T) {
		d := Duration(30 * time.Second)
		data, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}
		if string(data) != `"30s"` {
			t.Errorf("Marshal = %s, want \"30s\"", data)
		}
	})
}

func TestHealthCheckValidation(t *testing.T) {
	t.Run("valid CMD", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"CMD", "curl", "-f", "http://localhost/health"},
		}
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate failed: %v", err)
		}
	})

	t.Run("valid CMD-SHELL", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"CMD-SHELL", "curl -f http://localhost/health"},
		}
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate failed: %v", err)
		}
	})

	t.Run("valid NONE", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"NONE"},
		}
		if err := hc.Validate(); err != nil {
			t.Errorf("Validate failed: %v", err)
		}
	})

	t.Run("empty test", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{},
		}
		if err := hc.Validate(); err == nil {
			t.Error("expected error for empty test")
		}
	})

	t.Run("CMD without command", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"CMD"},
		}
		if err := hc.Validate(); err == nil {
			t.Error("expected error for CMD without command")
		}
	})

	t.Run("invalid test type", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test: []string{"INVALID", "command"},
		}
		if err := hc.Validate(); err == nil {
			t.Error("expected error for invalid test type")
		}
	})

	t.Run("negative retries", func(t *testing.T) {
		hc := &HealthCheckConfig{
			Test:    []string{"CMD", "curl", "-f", "http://localhost/health"},
			Retries: -1,
		}
		if err := hc.Validate(); err == nil {
			t.Error("expected error for negative retries")
		}
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
			if (err != nil) != tt.expectErr {
				t.Errorf("validatePortSpec(%q) error = %v, expectErr %v", tt.spec, err, tt.expectErr)
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
		if err != nil {
			t.Fatalf("GetSKUProfile() error = %v", err)
		}
		if profile.CPUCores != 0.5 {
			t.Errorf("expected CPUCores 0.5, got %v", profile.CPUCores)
		}
		if profile.MemoryMB != 512 {
			t.Errorf("expected MemoryMB 512, got %v", profile.MemoryMB)
		}
	})

	t.Run("direct profile name still works", func(t *testing.T) {
		profile, err := cfg.GetSKUProfile("docker-large")
		if err != nil {
			t.Fatalf("GetSKUProfile() error = %v", err)
		}
		if profile.CPUCores != 2.0 {
			t.Errorf("expected CPUCores 2.0, got %v", profile.CPUCores)
		}
	})

	t.Run("unknown UUID fails", func(t *testing.T) {
		_, err := cfg.GetSKUProfile("unknown-uuid")
		if err == nil {
			t.Error("expected error for unknown SKU")
		}
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
		if err == nil {
			t.Error("expected validation error for mapping to nonexistent profile")
		}
	})
}

func TestConfigHardeningDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.IsNetworkIsolation() {
		t.Error("expected IsNetworkIsolation() = true by default")
	}
	if !cfg.IsReadonlyRootfs() {
		t.Error("expected IsReadonlyRootfs() = true by default")
	}
	if *cfg.GetPidsLimit() != 256 {
		t.Errorf("expected GetPidsLimit() = 256, got %d", *cfg.GetPidsLimit())
	}
	if cfg.GetTmpfsSizeMB() != 64 {
		t.Errorf("expected GetTmpfsSizeMB() = 64, got %d", cfg.GetTmpfsSizeMB())
	}
	if cfg.GetHostBindIP() != "0.0.0.0" {
		t.Errorf("expected GetHostBindIP() = 0.0.0.0, got %s", cfg.GetHostBindIP())
	}
}

func TestConfigHardeningOverrides(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NetworkIsolation = ptrBool(false)
	cfg.ContainerReadonlyRootfs = ptrBool(false)
	cfg.ContainerPidsLimit = ptrInt64(512)
	cfg.ContainerTmpfsSizeMB = 128
	cfg.HostBindIP = "127.0.0.1"

	if cfg.IsNetworkIsolation() {
		t.Error("expected IsNetworkIsolation() = false after override")
	}
	if cfg.IsReadonlyRootfs() {
		t.Error("expected IsReadonlyRootfs() = false after override")
	}
	if *cfg.GetPidsLimit() != 512 {
		t.Errorf("expected GetPidsLimit() = 512, got %d", *cfg.GetPidsLimit())
	}
	if cfg.GetTmpfsSizeMB() != 128 {
		t.Errorf("expected GetTmpfsSizeMB() = 128, got %d", cfg.GetTmpfsSizeMB())
	}
	if cfg.GetHostBindIP() != "127.0.0.1" {
		t.Errorf("expected GetHostBindIP() = 127.0.0.1, got %s", cfg.GetHostBindIP())
	}
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
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for invalid host_bind_ip")
		}
	})

	t.Run("valid host_bind_ip", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostBindIP = "127.0.0.1"
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("empty host_bind_ip is valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.HostBindIP = ""
		if err := cfg.Validate(); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("pids_limit too low", func(t *testing.T) {
		cfg := validConfig()
		cfg.ContainerPidsLimit = ptrInt64(0)
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for pids_limit = 0")
		}
	})
}

func TestTenantNetworkName(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		name1 := TenantNetworkName("manifest1abc")
		name2 := TenantNetworkName("manifest1abc")
		if name1 != name2 {
			t.Errorf("expected deterministic names, got %s != %s", name1, name2)
		}
	})

	t.Run("different tenants get different names", func(t *testing.T) {
		name1 := TenantNetworkName("tenant-a")
		name2 := TenantNetworkName("tenant-b")
		if name1 == name2 {
			t.Error("expected different names for different tenants")
		}
	})

	t.Run("starts with prefix", func(t *testing.T) {
		name := TenantNetworkName("any-tenant")
		if name[:len("fred-tenant-")] != "fred-tenant-" {
			t.Errorf("expected fred-tenant- prefix, got %s", name)
		}
	})
}
