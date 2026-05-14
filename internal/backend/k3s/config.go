package k3s

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
)

// Type aliases for readability within the k3s package. The underlying
// types live in internal/backend/shared so every backend (Docker, K3s,
// future Nomad, etc.) describes its SKU profiles and tenant quotas
// with the same shape.
type SKUProfile = shared.SKUProfile
type TenantQuotaConfig = shared.TenantQuotaConfig

// IngressConfig is a scaffold placeholder for the K3s ingress story.
// In ENG-133 the field exists only so the YAML shape is fixed; setting
// Enabled=true is rejected by Validate() because the full Ingress /
// Gateway API mapping has not landed yet.
//
// K8s anchor: an Ingress (or the newer Gateway in the Gateway API)
// is roughly the analog of Docker labels for Traefik — a
// declarative "route external traffic to this Service" object.
// ClassName picks which controller (Traefik, nginx, etc.) handles
// the route.
type IngressConfig struct {
	Enabled   bool   `yaml:"enabled"`
	ClassName string `yaml:"class_name"`
}

// Validate rejects Enabled=true for the ENG-133 scaffold. Full Ingress
// support lands in a later child of ENG-132; until then the field
// shape is reserved so the YAML doesn't have to change schema again.
func (ic *IngressConfig) Validate() error {
	if ic.Enabled {
		return fmt.Errorf("ingress not supported in k3s-backend scaffold")
	}
	return nil
}

// Config holds the configuration for the K3s backend.
//
// The shape mirrors docker.Config for every field where the meaning is
// substrate-agnostic (totals, SKU mapping/profiles, callbacks,
// diagnostics, releases, tenant quota, reconcile interval, host
// address). Docker-private hardening / volume / network-isolation
// knobs are dropped because they have no K8s analog in scope for
// ENG-133. K8s adds one field, KubeconfigPath — the analog of
// DOCKER_HOST that tells client-go which API server to talk to. The
// canonical kept / dropped / added list is the design plan
// (§2 of the ENG-133 architecture doc); the comment here only
// describes the shape direction, not an enumeration that could
// drift out of sync.
type Config struct {
	// LogLevel controls the log verbosity (debug, info, warn, error).
	// When empty, defaults to "info" at startup (see cmd/k3s-backend/main.go).
	LogLevel string `yaml:"log_level"`

	// Name is the backend identifier.
	Name string `yaml:"name"`

	// ListenAddr is the address the HTTP server listens on.
	ListenAddr string `yaml:"listen_addr"`

	// KubeconfigPath is the explicit path to a kubeconfig file used by
	// client-go to talk to the K3s API server. When empty, the resolver
	// (kubeclient.go, T3) falls back in order: KubeconfigPathList → in-cluster
	// config → ~/.kube/config (via the default loader). K8s anchor: this is
	// the rough analog of Docker's DOCKER_HOST — a single configuration
	// pointer that tells the client which cluster to address.
	//
	// Prefer an absolute path. Go's stdlib doesn't expand "~", and
	// neither does the T3 resolver. Relative paths work (client-go
	// resolves them against the process's working directory) but are
	// fragile — callers should either set this explicitly to (for
	// example) "/etc/rancher/k3s/k3s.yaml" or leave it empty and rely
	// on the resolution chain above.
	KubeconfigPath string `yaml:"kubeconfig_path"`

	// KubeconfigPathList is the merged-precedence list of kubeconfig file
	// paths used when KUBECONFIG is set to a multi-path value
	// (e.g. KUBECONFIG=/path/a:/path/b on Linux). It is NOT a YAML field:
	// applyEnvOverrides in cmd/k3s-backend/main.go populates it from the
	// env var when it sees os.PathListSeparator, and resolveRESTConfig
	// hands the list to client-go's clientcmd.ClientConfigLoadingRules
	// (Precedence:) for the canonical merge semantics.
	//
	// Resolution precedence: KubeconfigPath (single explicit path) wins
	// when set; otherwise a non-empty KubeconfigPathList wins over
	// in-cluster config (so an operator explicitly setting a multi-path
	// KUBECONFIG isn't silently overridden when the binary happens to
	// run inside a Pod). When both are empty the resolver falls through
	// to in-cluster and then the default loader (~/.kube/config).
	KubeconfigPathList []string `yaml:"-"`

	// TotalCPUCores is the total CPU cores available in the resource pool.
	TotalCPUCores float64 `yaml:"total_cpu_cores"`

	// TotalMemoryMB is the total memory available in MB.
	TotalMemoryMB int64 `yaml:"total_memory_mb"`

	// TotalDiskMB is the total disk space available in MB.
	TotalDiskMB int64 `yaml:"total_disk_mb"`

	// SKUMapping maps on-chain SKU UUIDs to profile names.
	// This allows the backend to translate chain SKU UUIDs to local resource profiles.
	// Example: {"019c1ee7-1aaf-7000-802c-ad775c72cc27": "k3s-small"}
	SKUMapping map[string]string `yaml:"sku_mapping"`

	// SKUProfiles maps SKU names to resource profiles.
	SKUProfiles map[string]SKUProfile `yaml:"sku_profiles"`

	// AllowedRegistries is the list of allowed container registries.
	AllowedRegistries []string `yaml:"allowed_registries"`

	// CallbackSecret is the HMAC secret for signing callbacks.
	CallbackSecret config.Secret `yaml:"callback_secret"`

	// HostAddress is the external address for connection info returned to tenants.
	HostAddress string `yaml:"host_address"`

	// CallbackInsecureSkipVerify skips TLS certificate verification for callbacks.
	// WARNING: This disables TLS certificate validation, enabling MITM attacks.
	// NEVER enable in production. Only use for local development with self-signed certificates.
	CallbackInsecureSkipVerify bool `yaml:"callback_insecure_skip_verify"`

	// CallbackDBPath is the path to the bbolt database for persisting pending callbacks.
	// Defaults to "k3s-callbacks.db" (k3s-prefixed so the file doesn't
	// collide with docker-backend's "callbacks.db" when both backends run
	// from the same working directory — bbolt takes an exclusive file lock
	// and would refuse to open a name already held by the sibling process).
	CallbackDBPath string `yaml:"callback_db_path"`

	// CallbackMaxAge is the maximum age of a persisted callback entry.
	// Entries older than this are removed by the callback store's background cleanup.
	// Defaults to 24h.
	CallbackMaxAge time.Duration `yaml:"callback_max_age"`

	// DiagnosticsDBPath is the path to the bbolt database for persisting failure diagnostics.
	// Defaults to "k3s-diagnostics.db" (k3s-prefixed; see CallbackDBPath
	// for the side-by-side rationale).
	DiagnosticsDBPath string `yaml:"diagnostics_db_path"`

	// DiagnosticsMaxAge is the maximum age of a persisted diagnostic entry.
	// Entries older than this are removed by the diagnostics store's background cleanup.
	// Defaults to 7 days.
	DiagnosticsMaxAge time.Duration `yaml:"diagnostics_max_age"`

	// ReleasesDBPath is the path to the bbolt database for persisting release history.
	// Defaults to "k3s-releases.db" (k3s-prefixed; see CallbackDBPath for
	// the side-by-side rationale).
	ReleasesDBPath string `yaml:"releases_db_path"`

	// ReleasesMaxAge is the maximum age of a persisted release entry.
	// Entries older than this are removed by the release store's background cleanup.
	// Defaults to 90 days.
	ReleasesMaxAge time.Duration `yaml:"releases_max_age"`

	// ReconcileInterval is how often to reconcile state with the cluster.
	// In the ENG-133 scaffold no reconcile loop is started; the field is
	// kept so later children plug in without changing the schema.
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`

	// TenantQuota configures per-tenant resource limits.
	// When set, prevents any single tenant from consuming the entire pool.
	TenantQuota *TenantQuotaConfig `yaml:"tenant_quota"`

	// Ingress is the placeholder for the K3s ingress story. Validate
	// rejects Enabled=true for the ENG-133 scaffold.
	Ingress IngressConfig `yaml:"ingress"`
}

// DefaultConfig returns a Config with sensible defaults.
//
// Listen address is :9002 (one above docker-backend's :9001) so the two
// backends can run side-by-side on the same host without colliding. The
// default SKU profile names are k3s-* to make it obvious in logs which
// backend a request was routed to.
func DefaultConfig() Config {
	return Config{
		Name:              "k3s",
		ListenAddr:        ":9002",
		TotalCPUCores:     8.0,
		TotalMemoryMB:     16384,
		TotalDiskMB:       102400,
		CallbackDBPath:    "k3s-callbacks.db",
		CallbackMaxAge:    24 * time.Hour,
		DiagnosticsDBPath: "k3s-diagnostics.db",
		DiagnosticsMaxAge: 7 * 24 * time.Hour,
		ReleasesDBPath:    "k3s-releases.db",
		ReleasesMaxAge:    90 * 24 * time.Hour,
		ReconcileInterval: 5 * time.Minute,
		SKUProfiles: map[string]SKUProfile{
			"k3s-micro": {
				CPUCores: 0.25,
				MemoryMB: 256,
				DiskMB:   512,
			},
			"k3s-small": {
				CPUCores: 0.5,
				MemoryMB: 512,
				DiskMB:   1024,
			},
			"k3s-medium": {
				CPUCores: 1.0,
				MemoryMB: 1024,
				DiskMB:   2048,
			},
			"k3s-large": {
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
//
// The check set is the docker-backend's check set minus the validations
// for fields that don't exist in this Config (the dropped Docker-private
// hardening / volume / network-isolation knobs). The Ingress placeholder
// is still validated and rejects Enabled=true.
func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}

	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
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

	// Validate that all SKU mappings point to existing profiles.
	for skuUUID, profileName := range c.SKUMapping {
		if _, ok := c.SKUProfiles[profileName]; !ok {
			return fmt.Errorf("sku_mapping[%s] references unknown profile %q", skuUUID, profileName)
		}
	}

	// Validate that all SKU profiles are reachable via sku_mapping.
	// On-chain SKUs are UUIDs — profiles without a mapping are unreachable.
	if len(c.SKUMapping) > 0 {
		mapped := make(map[string]bool, len(c.SKUMapping))
		for _, profileName := range c.SKUMapping {
			mapped[profileName] = true
		}
		for name := range c.SKUProfiles {
			if !mapped[name] {
				return fmt.Errorf("sku_profiles[%s] has no sku_mapping entry and is unreachable from on-chain SKUs", name)
			}
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

	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("reconcile_interval must be positive")
	}

	if c.CallbackMaxAge < 0 {
		return fmt.Errorf("callback_max_age must be non-negative")
	}

	if c.DiagnosticsMaxAge < 0 {
		return fmt.Errorf("diagnostics_max_age must be non-negative")
	}

	if c.ReleasesMaxAge < 0 {
		return fmt.Errorf("releases_max_age must be non-negative")
	}

	if err := c.Ingress.Validate(); err != nil {
		return err
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
// It first checks if the SKU is a UUID that maps to a profile name via
// SKUMapping, then falls back to direct profile lookup.
func (c *Config) GetSKUProfile(sku string) (SKUProfile, error) {
	// First, check if there's a mapping from SKU UUID to profile name.
	profileName := sku
	if c.SKUMapping != nil {
		if mapped, ok := c.SKUMapping[sku]; ok {
			profileName = mapped
		}
	}

	// Look up the profile by name.
	profile, ok := c.SKUProfiles[profileName]
	if !ok {
		return SKUProfile{}, fmt.Errorf("%w: %s (profile: %s)", backend.ErrUnknownSKU, sku, profileName)
	}
	return profile, nil
}
