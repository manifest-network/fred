// Package docker implements a Docker backend for Fred that provisions ephemeral
// containers with SKU-based resource profiles, registry allowlisting, and port
// mapping for tenant connectivity.
package docker

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// Type aliases for readability within the docker package.
type SKUProfile = shared.SKUProfile
type TenantQuotaConfig = shared.TenantQuotaConfig

// Config holds the configuration for the Docker backend.
type Config struct {
	// Name is the backend identifier.
	Name string `yaml:"name"`

	// ListenAddr is the address the HTTP server listens on.
	ListenAddr string `yaml:"listen_addr"`

	// DockerHost is the Docker daemon socket path or URL.
	DockerHost string `yaml:"docker_host"`

	// TotalCPUCores is the total CPU cores available in the resource pool.
	TotalCPUCores float64 `yaml:"total_cpu_cores"`

	// TotalMemoryMB is the total memory available in MB.
	TotalMemoryMB int64 `yaml:"total_memory_mb"`

	// TotalDiskMB is the total disk space available in MB.
	TotalDiskMB int64 `yaml:"total_disk_mb"`

	// SKUMapping maps on-chain SKU UUIDs to profile names.
	// This allows the backend to translate chain SKU UUIDs to local resource profiles.
	// Example: {"019c1ee7-1aaf-7000-802c-ad775c72cc27": "docker-small"}
	SKUMapping map[string]string `yaml:"sku_mapping"`

	// SKUProfiles maps SKU names to resource profiles.
	SKUProfiles map[string]SKUProfile `yaml:"sku_profiles"`

	// AllowedRegistries is the list of allowed container registries.
	AllowedRegistries []string `yaml:"allowed_registries"`

	// CallbackSecret is the HMAC secret for signing callbacks.
	CallbackSecret string `yaml:"callback_secret"`

	// HostAddress is the external address for port mappings.
	HostAddress string `yaml:"host_address"`

	// ImagePullTimeout is the timeout for pulling images.
	ImagePullTimeout time.Duration `yaml:"image_pull_timeout"`

	// ContainerCreateTimeout is the timeout for creating containers.
	ContainerCreateTimeout time.Duration `yaml:"container_create_timeout"`

	// ContainerStartTimeout is the timeout for starting containers.
	ContainerStartTimeout time.Duration `yaml:"container_start_timeout"`

	// ReconcileInterval is how often to reconcile state with Docker.
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`

	// CallbackInsecureSkipVerify skips TLS certificate verification for callbacks.
	// WARNING: Only use for development with self-signed certificates.
	CallbackInsecureSkipVerify bool `yaml:"callback_insecure_skip_verify"`

	// CallbackDBPath is the path to the bbolt database for persisting pending callbacks.
	// Defaults to "callbacks.db".
	CallbackDBPath string `yaml:"callback_db_path"`

	// ProvisionTimeout is the maximum time allowed for the entire provisioning
	// operation (image pull + container creation + start). If exceeded, the
	// provisioning is canceled and a failure callback is sent.
	ProvisionTimeout time.Duration `yaml:"provision_timeout"`

	// HostBindIP is the IP address to bind container ports to.
	// Defaults to "0.0.0.0" (all interfaces).
	HostBindIP string `yaml:"host_bind_ip"`

	// NetworkIsolation enables per-tenant Docker network isolation.
	// When true, each tenant's containers are placed in a separate internal bridge network.
	// Defaults to true.
	NetworkIsolation *bool `yaml:"network_isolation"`

	// ContainerReadonlyRootfs sets the container's root filesystem to read-only.
	// When true, /tmp and /run are mounted as tmpfs. Defaults to true.
	ContainerReadonlyRootfs *bool `yaml:"container_readonly_rootfs"`

	// ContainerPidsLimit limits the number of PIDs in each container.
	// Defaults to 256.
	ContainerPidsLimit *int64 `yaml:"container_pids_limit"`

	// ContainerTmpfsSizeMB sets the tmpfs size in MB for /tmp and /run when
	// readonly rootfs is enabled. Defaults to 64.
	ContainerTmpfsSizeMB int `yaml:"container_tmpfs_size_mb"`

	// StartupVerifyDuration is how long to wait after starting containers before
	// verifying they're still running. This catches containers that crash immediately
	// on startup (e.g., bad config, read-only filesystem errors, missing dependencies).
	// The success callback is only sent after verification passes.
	// Defaults to 5 seconds. Setting to 0 uses the default (verification cannot be disabled).
	StartupVerifyDuration time.Duration `yaml:"startup_verify_duration"`

	// TenantQuota configures per-tenant resource limits.
	// When set, prevents any single tenant from consuming the entire pool.
	TenantQuota *TenantQuotaConfig `yaml:"tenant_quota"`

	// ContainerDiskQuota enables Docker overlay2 storage driver disk quotas
	// on containers. Requires overlay2 with xfs + pquota mount option.
	// When enabled, StorageOpt is set on the container's HostConfig.
	// Defaults to true. Set to false on unsupported filesystems.
	ContainerDiskQuota *bool `yaml:"container_disk_quota"`

	// CallbackMaxAge is the maximum age of a persisted callback entry.
	// Entries older than this are removed by the callback store's background cleanup.
	// Defaults to 24h.
	CallbackMaxAge time.Duration `yaml:"callback_max_age"`

	// DiagnosticsDBPath is the path to the bbolt database for persisting failure diagnostics.
	// Defaults to "diagnostics.db".
	DiagnosticsDBPath string `yaml:"diagnostics_db_path"`

	// DiagnosticsMaxAge is the maximum age of a persisted diagnostic entry.
	// Entries older than this are removed by the diagnostics store's background cleanup.
	// Defaults to 7 days.
	DiagnosticsMaxAge time.Duration `yaml:"diagnostics_max_age"`
}

func ptrBool(b bool) *bool    { return &b }
func ptrInt64(i int64) *int64 { return &i }

// GetHostBindIP returns the configured bind IP, defaulting to "0.0.0.0".
func (c *Config) GetHostBindIP() string {
	if c.HostBindIP != "" {
		return c.HostBindIP
	}
	return "0.0.0.0"
}

// IsNetworkIsolation returns whether per-tenant network isolation is enabled.
// Defaults to true (secure by default).
func (c *Config) IsNetworkIsolation() bool {
	if c.NetworkIsolation != nil {
		return *c.NetworkIsolation
	}
	return true
}

// IsReadonlyRootfs returns whether containers should have a read-only root filesystem.
// Defaults to true (secure by default).
func (c *Config) IsReadonlyRootfs() bool {
	if c.ContainerReadonlyRootfs != nil {
		return *c.ContainerReadonlyRootfs
	}
	return true
}

// GetPidsLimit returns the PID limit for containers. Defaults to 256.
func (c *Config) GetPidsLimit() *int64 {
	if c.ContainerPidsLimit != nil {
		return c.ContainerPidsLimit
	}
	v := int64(256)
	return &v
}

// GetTmpfsSizeMB returns the tmpfs size in MB. Defaults to 64.
func (c *Config) GetTmpfsSizeMB() int {
	if c.ContainerTmpfsSizeMB > 0 {
		return c.ContainerTmpfsSizeMB
	}
	return 64
}

// IsDiskQuota returns whether container disk quotas are enabled.
// Defaults to true. Requires overlay2 with xfs + pquota.
func (c *Config) IsDiskQuota() bool {
	if c.ContainerDiskQuota != nil {
		return *c.ContainerDiskQuota
	}
	return true
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Name:                    "docker",
		ListenAddr:              ":9001",
		DockerHost:              "unix:///var/run/docker.sock",
		TotalCPUCores:           8.0,
		TotalMemoryMB:           16384,
		TotalDiskMB:             102400,
		ImagePullTimeout:        5 * time.Minute,
		ContainerCreateTimeout:  30 * time.Second,
		ContainerStartTimeout:   30 * time.Second,
		ReconcileInterval:       5 * time.Minute,
		ProvisionTimeout:        10 * time.Minute,
		CallbackDBPath:          "callbacks.db",
		NetworkIsolation:        ptrBool(true),
		ContainerReadonlyRootfs: ptrBool(true),
		ContainerPidsLimit:      ptrInt64(256),
		ContainerTmpfsSizeMB:    64,
		ContainerDiskQuota:      ptrBool(true),
		CallbackMaxAge:          24 * time.Hour,
		DiagnosticsDBPath:       "diagnostics.db",
		DiagnosticsMaxAge:       7 * 24 * time.Hour,
		SKUProfiles: map[string]SKUProfile{
			"docker-micro": {
				CPUCores: 0.25,
				MemoryMB: 256,
				DiskMB:   512,
			},
			"docker-small": {
				CPUCores: 0.5,
				MemoryMB: 512,
				DiskMB:   1024,
			},
			"docker-medium": {
				CPUCores: 1.0,
				MemoryMB: 1024,
				DiskMB:   2048,
			},
			"docker-large": {
				CPUCores: 2.0,
				MemoryMB: 2048,
				DiskMB:   4096,
			},
		},
		AllowedRegistries: []string{
			"docker.io",
			"ghcr.io",
		},
	}
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}

	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
	}

	if c.DockerHost == "" {
		return fmt.Errorf("docker_host is required")
	}

	if c.TotalCPUCores <= 0 {
		return fmt.Errorf("total_cpu_cores must be positive")
	}

	if c.TotalMemoryMB <= 0 {
		return fmt.Errorf("total_memory_mb must be positive")
	}

	if c.TotalDiskMB <= 0 {
		return fmt.Errorf("total_disk_mb must be positive")
	}

	if len(c.SKUProfiles) == 0 {
		return fmt.Errorf("at least one SKU profile is required")
	}

	for name, profile := range c.SKUProfiles {
		if err := validateSKUProfile(name, profile); err != nil {
			return err
		}
	}

	// Validate that all SKU mappings point to existing profiles
	for skuUUID, profileName := range c.SKUMapping {
		if _, ok := c.SKUProfiles[profileName]; !ok {
			return fmt.Errorf("sku_mapping[%s] references unknown profile %q", skuUUID, profileName)
		}
	}

	if len(c.AllowedRegistries) == 0 {
		return fmt.Errorf("at least one allowed registry is required")
	}

	if c.CallbackSecret == "" {
		return fmt.Errorf("callback_secret is required")
	}

	if len(c.CallbackSecret) < 32 {
		return fmt.Errorf("callback_secret must be at least 32 characters")
	}

	if c.HostAddress == "" {
		return fmt.Errorf("host_address is required")
	}

	// Validate host_address is a valid IP or hostname.
	// Reject values that look like URLs or contain slashes/schemes.
	if strings.ContainsAny(c.HostAddress, "/ ") {
		return fmt.Errorf("host_address must be a hostname or IP, not a URL: %s", c.HostAddress)
	}
	// Strip port if present (e.g., "192.168.1.1:8080") for IP validation.
	hostOnly := c.HostAddress
	if h, _, err := net.SplitHostPort(c.HostAddress); err == nil {
		hostOnly = h
	}
	if net.ParseIP(hostOnly) == nil {
		// Not an IP — validate as hostname (must contain at least one dot or be "localhost").
		if hostOnly != "localhost" && !strings.Contains(hostOnly, ".") {
			return fmt.Errorf("host_address is not a valid hostname or IP: %s", c.HostAddress)
		}
	}

	if c.ImagePullTimeout <= 0 {
		return fmt.Errorf("image_pull_timeout must be positive")
	}

	if c.ContainerCreateTimeout <= 0 {
		return fmt.Errorf("container_create_timeout must be positive")
	}

	if c.ContainerStartTimeout <= 0 {
		return fmt.Errorf("container_start_timeout must be positive")
	}

	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("reconcile_interval must be positive")
	}

	if c.ProvisionTimeout <= 0 {
		return fmt.Errorf("provision_timeout must be positive")
	}

	if c.HostBindIP != "" && net.ParseIP(c.HostBindIP) == nil {
		return fmt.Errorf("host_bind_ip is not a valid IP address: %s", c.HostBindIP)
	}

	if c.ContainerPidsLimit != nil && *c.ContainerPidsLimit < 1 {
		return fmt.Errorf("container_pids_limit must be >= 1")
	}

	if c.ContainerTmpfsSizeMB < 0 {
		return fmt.Errorf("container_tmpfs_size_mb must be >= 0")
	}

	if c.CallbackMaxAge < 0 {
		return fmt.Errorf("callback_max_age must be non-negative")
	}

	if c.DiagnosticsMaxAge < 0 {
		return fmt.Errorf("diagnostics_max_age must be non-negative")
	}

	if c.TenantQuota != nil {
		tq := c.TenantQuota
		if err := tq.Validate(); err != nil {
			return fmt.Errorf("tenant_quota.%w", err)
		}
		if tq.MaxCPUCores > c.TotalCPUCores {
			return fmt.Errorf("tenant_quota.max_cpu_cores (%.2f) exceeds total_cpu_cores (%.2f)", tq.MaxCPUCores, c.TotalCPUCores)
		}
		if tq.MaxMemoryMB > c.TotalMemoryMB {
			return fmt.Errorf("tenant_quota.max_memory_mb (%d) exceeds total_memory_mb (%d)", tq.MaxMemoryMB, c.TotalMemoryMB)
		}
		if tq.MaxDiskMB > c.TotalDiskMB {
			return fmt.Errorf("tenant_quota.max_disk_mb (%d) exceeds total_disk_mb (%d)", tq.MaxDiskMB, c.TotalDiskMB)
		}
	}

	return nil
}

func validateSKUProfile(name string, profile SKUProfile) error {
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("SKU %q: %w", name, err)
	}
	return nil
}

// GetSKUProfile returns the profile for a SKU, or an error if not found.
// It first checks if the SKU is a UUID that maps to a profile name via SKUMapping,
// then falls back to direct profile lookup.
func (c *Config) GetSKUProfile(sku string) (SKUProfile, error) {
	// First, check if there's a mapping from SKU UUID to profile name
	profileName := sku
	if c.SKUMapping != nil {
		if mapped, ok := c.SKUMapping[sku]; ok {
			profileName = mapped
		}
	}

	// Look up the profile by name
	profile, ok := c.SKUProfiles[profileName]
	if !ok {
		return SKUProfile{}, fmt.Errorf("unknown SKU: %s (profile: %s)", sku, profileName)
	}
	return profile, nil
}
