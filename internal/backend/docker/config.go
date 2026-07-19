// Package docker implements a Docker backend for Fred that provisions ephemeral
// containers with SKU-based resource profiles, registry allowlisting, and port
// mapping for tenant connectivity.
package docker

import (
	"fmt"
	"net"
	"strings"
	"time"
	"unicode"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
)

// Type aliases for readability within the docker package.
type SKUProfile = shared.SKUProfile
type TenantQuotaConfig = shared.TenantQuotaConfig

// Recover-time migration defaults. Shared by [DefaultConfig] and the
// `cmp.Or` guards in [Backend.executeLegacyMigration] so a deployment
// that constructs a [Config] without going through [DefaultConfig] (or
// supplies an explicit 0 via YAML) still gets a sane safety window
// instead of an immediately-canceled context or instant `-prev`
// removal.
const (
	defaultMigrationReadyTimeout = 90 * time.Second
	defaultMigrationGracePeriod  = time.Minute
)

// DefaultMaxRequestBodySize caps inbound HTTP request bodies for the docker
// backend. It is deliberately larger than providerd's
// config.DefaultMaxRequestBodySize (1 MiB): providerd caps the raw tenant body,
// then re-serializes and wraps it (JSON envelope + base64) before forwarding, so
// a manifest that just cleared providerd can exceed 1 MiB on the backend hop and
// would otherwise be rejected with an opaque 400. Configurable via
// max_request_body_size / DOCKER_BACKEND_MAX_REQUEST_BODY_SIZE. (ENG-448 / F42)
const DefaultMaxRequestBodySize int64 = 2 << 20 // 2 MiB

// defaultMinAvgFileBytes is the assumed minimum average file size (bytes) for a
// volume's workload when MinAvgFileBytes is unset (<= 0). Shared by
// [Config.GetMinAvgFileBytes] and inodeHardLimit so the two can't drift. See
// ENG-548.
const defaultMinAvgFileBytes = 1024

// Config holds the configuration for the Docker backend.
type Config struct {
	// LogLevel controls the log verbosity (debug, info, warn, error).
	// When empty, defaults to "info" at startup (see cmd/docker-backend/main.go).
	LogLevel string `yaml:"log_level"`

	// Name is the backend identifier.
	Name string `yaml:"name"`

	// ListenAddr is the address the HTTP server listens on.
	ListenAddr string `yaml:"listen_addr"`

	// MaxRequestBodySize caps inbound HTTP request bodies (bytes). It must
	// exceed providerd's request cap plus forward-wrapping overhead; defaults to
	// DefaultMaxRequestBodySize when unset or non-positive. (ENG-448 / F42)
	MaxRequestBodySize int64 `yaml:"max_request_body_size"`

	// ProductionMode tightens startup checks beyond basic validation. When true,
	// Validate rejects dev-only insecure toggles — currently
	// callback_insecure_skip_verify, which disables TLS verification on the
	// backend → Fred callback hop. Mirrors providerd's production_mode (which
	// gates the reverse providerd → backend tls_skip_verify). Defaults to false.
	ProductionMode bool `yaml:"production_mode"`

	// TLSCertFile and TLSKeyFile enable HTTPS on the listener when both are
	// set; otherwise it serves plaintext HTTP (the default). Loaded once at
	// startup — rotation requires a restart (see ENG-294).
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`

	// TLSClientCAFile turns on mutual TLS when set: the listener requires and
	// verifies a client certificate signed by this CA. Requires TLSCertFile and
	// TLSKeyFile (the listener must be on TLS first).
	TLSClientCAFile string `yaml:"tls_client_ca_file"`

	// TLSClientAllowedNames optionally pins the mTLS client's identity: the
	// presented certificate's CommonName or a DNS SAN must be in this list.
	// Empty accepts any certificate signed by TLSClientCAFile. Requires
	// TLSClientCAFile. Use this whenever the client CA is not dedicated solely
	// to providerd.
	TLSClientAllowedNames []string `yaml:"tls_client_allowed_names"`

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
	CallbackSecret config.Secret `yaml:"callback_secret"`

	// HostAddress is the external address for port mappings.
	HostAddress string `yaml:"host_address"`

	// ImagePullTimeout is the timeout for pulling images.
	ImagePullTimeout time.Duration `yaml:"image_pull_timeout"`

	// ContainerCreateTimeout is the timeout for creating containers.
	ContainerCreateTimeout time.Duration `yaml:"container_create_timeout"`

	// ContainerStartTimeout is the timeout for starting containers.
	ContainerStartTimeout time.Duration `yaml:"container_start_timeout"`

	// ContainerStopTimeout is the grace period for stopping containers.
	// Containers receive SIGTERM and have this long to shut down gracefully
	// before being force-killed (SIGKILL). Defaults to 30 seconds.
	ContainerStopTimeout time.Duration `yaml:"container_stop_timeout"`

	// ReconcileInterval is how often to reconcile state with Docker.
	ReconcileInterval time.Duration `yaml:"reconcile_interval"`

	// CallbackInsecureSkipVerify skips TLS certificate verification for callbacks.
	// WARNING: This disables TLS certificate validation, enabling MITM attacks.
	// NEVER enable in production. Only use for local development with self-signed certificates.
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
	// When true, each tenant's containers are placed in a separate bridge network.
	// Provides inter-tenant isolation. The network is created with Internal:false
	// — required for port publishing (moby#36174) — so containers retain outbound
	// internet access as a side effect. Defaults to true.
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

	// VolumeDataPath is the host directory for managed volumes.
	// Required when any SKU profile has DiskMB > 0.
	// Each container gets a quota-enforced subdirectory under this path.
	VolumeDataPath string `yaml:"volume_data_path"`

	// VolumeFilesystem specifies the filesystem type for volume quota enforcement.
	// Supported values: "btrfs", "xfs", "zfs". If empty, auto-detected from VolumeDataPath.
	VolumeFilesystem string `yaml:"volume_filesystem"`

	// MinAvgFileBytes is the smallest average file size (bytes) a volume's workload
	// may have before the per-volume XFS inode quota (ihard) — rather than the block
	// quota (bhard) — binds. It is the mkfs "-i bytes-per-inode" idiom applied as an
	// inode-ceiling ratio: ihard = DiskMB*MiB / MinAvgFileBytes (floored at 262144
	// inodes). Smaller => MORE inodes allowed => looser cap. 0 uses the default
	// (1024 => 1024 inodes/MiB).
	// Values below 512 are rejected (they would uncap the fix). See ENG-548.
	MinAvgFileBytes int64 `yaml:"min_avg_file_bytes"`

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

	// ReleasesDBPath is the path to the bbolt database for persisting release history.
	// Defaults to "releases.db".
	ReleasesDBPath string `yaml:"releases_db_path"`

	// ReleasesMaxAge is the maximum age of a persisted release entry.
	// Entries older than this are removed by the release store's background cleanup.
	// Defaults to 90 days.
	ReleasesMaxAge time.Duration `yaml:"releases_max_age"`

	// RetainOnClose controls whether a lease's managed VOLUMES are soft-deleted
	// (renamed into the fred-retained- namespace and recorded in the retention
	// store) instead of immediately destroyed when the lease is closed. The
	// lease's containers are still stopped and removed either way; only the
	// volumes are retained. When false (default), the volumes are destroyed
	// immediately on close.
	RetainOnClose bool `yaml:"retain_on_close"`

	// RetentionDBPath is the path to the bbolt database for persisting
	// soft-deleted leases awaiting restore or reaping.
	// Defaults to "retention.db".
	RetentionDBPath string `yaml:"retention_db_path"`

	// RetentionMaxAge is the grace window after which a soft-deleted lease
	// becomes eligible for reaping. 0 disables reaping entirely.
	// Defaults to 90 days.
	RetentionMaxAge time.Duration `yaml:"retention_max_age"`

	// RetentionReapInterval is how often the background reaper sweeps for
	// expired soft-deleted leases. Decoupled from RetentionMaxAge so the
	// sweep cadence can be tuned independently of the grace window.
	// Defaults to 1h.
	RetentionReapInterval time.Duration `yaml:"retention_reap_interval"`

	// MaxRetainedLeasesPerTenant caps how many soft-deleted leases a single
	// tenant may have in the retention store at once. 0 means unlimited.
	MaxRetainedLeasesPerTenant int `yaml:"max_retained_leases_per_tenant"`

	// RetentionOrphanConfirmations is the number of consecutive retention sweeps a
	// soft-deleted record must be observed with ALL its retained volumes missing
	// before the record is pruned (ENG-370). It is a SWEEP COUNT, not a duration:
	// the effective confirmation window is N × RetentionReapInterval (≈3h at the
	// default 1h interval), so shortening RetentionReapInterval proportionally
	// shrinks the window — re-tune N to keep a fixed grace. 0 is valid and
	// disables orphan pruning entirely (kill-switch); negative values are rejected
	// by Validate. Defaults to 3.
	RetentionOrphanConfirmations int `yaml:"retention_orphan_confirmations"`

	// MaxRetainedDiskMB caps the aggregate disk (MB) the provider will hold in
	// the retained (soft-deleted) tier across ALL tenants. When retaining a
	// closing lease would push the total over this cap, the lease is destroyed
	// immediately instead of retained (refuse-to-retain) — never evicting
	// another tenant's in-grace data. 0 means unlimited (default; retained
	// volumes still count against total_disk_mb via the admission gate, but are
	// not separately bounded). Must be <= total_disk_mb and, when set, >= the
	// largest single stateful SKU's disk_mb (else a SKU-legal lease could never
	// be retained). Independent of tenant_quota.max_disk_mb: it may be smaller,
	// in which case a tenant's max-sized lease is SKU-legal yet refused
	// retention. Value is a plain integer in MB (mebibytes, 2^20 bytes —
	// consistent with total_disk_mb and the SKU disk_mb fields).
	MaxRetainedDiskMB int64 `yaml:"max_retained_disk_mb"`

	// RetentionPartitionSource selects where the OPTIONAL retention-partition
	// key is read from at lease close. "" (default) disables partitioning —
	// behavior is byte-identical to a build without the feature. Formats:
	//   "manifest.label:<key>"  read from each service's labels map
	//   "manifest.env:<key>"    read from each service's env map
	// The extracted value only ever SUB-DIVIDES a tenant's retention budget
	// (never raises any cap); non-allowlisted tenants' declarations collapse
	// to the whole-tenant bucket. Invalid/unsatisfiable sources fail startup.
	RetentionPartitionSource string `yaml:"retention_partition_source"`

	// MaxRetainedDiskMBPerTenant caps one tenant's aggregate retained disk
	// (MB). Same refuse-to-retain semantics as max_retained_disk_mb but
	// per-tenant. 0 = unlimited (default; today's behavior). Overridden per
	// tenant by a retention_tenant_budgets entry.
	MaxRetainedDiskMBPerTenant int64 `yaml:"max_retained_disk_mb_per_tenant"`

	// RetentionTenantBudgets is the aggregator allowlist: per-tenant retention
	// budget overrides keyed by the on-chain tenant address (an opaque string —
	// deliberately not bech32-validated; a typo'd key silently never matches,
	// which the startup budget-sanity pass surfaces). Only tenants listed here
	// with max_partitions > 0 ever persist a non-empty partition.
	RetentionTenantBudgets map[string]RetentionTenantBudget `yaml:"retention_tenant_budgets"`

	// MigrationGracePeriod is how long the renamed `-prev` legacy container
	// lingers after a successful recover-time migration before forced
	// removal. Preserves rollback potential if the operator interrupts fred
	// in the migration window to inspect. Defaults to 1m.
	MigrationGracePeriod time.Duration `yaml:"migration_grace_period"`

	// MigrationReadyTimeout caps how long the recover-time migration waits
	// for the new stack-form container to reach `healthy` (or `running`
	// when no health check is declared) before declaring the migration
	// failed for that lease. Defaults to 90s.
	MigrationReadyTimeout time.Duration `yaml:"migration_ready_timeout"`

	// Ingress configures optional reverse proxy integration.
	// When enabled, containers with routable TCP ports get proxy labels
	// pointing Traefik at the per-tenant network for HTTPS auto-discovery.
	// Requires network_isolation to be enabled.
	Ingress IngressConfig `yaml:"ingress"`
}

// RetentionTenantBudget overrides the default per-tenant retention caps for one
// allowlisted (aggregator) tenant and optionally enables partition sub-caps.
// Aggregates are REQUIRED positive: "unlimited" is unrepresentable inside a
// budget (load-bearing for the bounded-scan argument).
type RetentionTenantBudget struct {
	MaxRetainedLeases     int   `yaml:"max_retained_leases"`
	MaxRetainedDiskMB     int64 `yaml:"max_retained_disk_mb"`
	MaxPartitions         int   `yaml:"max_partitions"`            // 0 = elevation-only: labels still collapse
	PerPartitionMaxLeases int   `yaml:"per_partition_max_leases"`  // 0 = no L2 count sub-cap
	PerPartitionMaxDiskMB int64 `yaml:"per_partition_max_disk_mb"` // 0 = no L2 disk sub-cap
}

const (
	// maxRetentionPartitionsPerTenant is the compile-time ceiling on a budget's
	// max_partitions — an order of magnitude above any plausible per-backend
	// sub-tenant fleet while keeping worst-case per-close scans trivial.
	maxRetentionPartitionsPerTenant = 1024
	// maxRetentionBudgetLeases bounds per-close decode work for a budgeted
	// tenant's ListByTenant snapshot (~tens of MB worst-case).
	maxRetentionBudgetLeases = 65536
	// maxRetentionBudgetKeyLen bounds a budget map key (the opaque tenant
	// address); a longer key is almost certainly a paste error, never matched.
	maxRetentionBudgetKeyLen = 128
)

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

// GetMinAvgFileBytes returns the configured minimum average file size in bytes,
// used to derive per-volume XFS inode limits. Defaults to 1024. See ENG-548.
func (c *Config) GetMinAvgFileBytes() int64 {
	if c.MinAvgFileBytes > 0 {
		return c.MinAvgFileBytes
	}
	return defaultMinAvgFileBytes
}

// HasStatefulSKUs returns true if any SKU profile has DiskMB > 0,
// indicating that volume management is needed.
func (c *Config) HasStatefulSKUs() bool {
	for _, p := range c.SKUProfiles {
		if p.DiskMB > 0 {
			return true
		}
	}
	return false
}

// largestSKUDiskMB returns the maximum disk_mb across all configured SKU
// profiles (0 if none are stateful). Used to validate that a retained-disk cap
// can hold at least a one-unit lease of every SKU.
func (c *Config) largestSKUDiskMB() int64 {
	var largest int64
	for _, p := range c.SKUProfiles {
		if p.DiskMB > largest {
			largest = p.DiskMB
		}
	}
	return largest
}

// DefaultConfig returns a Config with sensible defaults.
//
// SKUProfiles is intentionally left empty: tier sizing is operator policy,
// not a code default. yaml.v3 merges map keys, so seeding defaults here
// would silently leak into any partial sku_profiles: block in YAML and
// trip the bidirectional sku_mapping/sku_profiles reachability check in
// Validate (see ENG-238). Operators must declare sku_profiles in their
// config; Validate enforces non-empty.
func DefaultConfig() Config {
	return Config{
		Name:                         "docker",
		ListenAddr:                   ":9001",
		MaxRequestBodySize:           DefaultMaxRequestBodySize,
		DockerHost:                   "unix:///var/run/docker.sock",
		TotalCPUCores:                8.0,
		TotalMemoryMB:                16384,
		TotalDiskMB:                  102400,
		ImagePullTimeout:             5 * time.Minute,
		ContainerCreateTimeout:       30 * time.Second,
		ContainerStartTimeout:        30 * time.Second,
		ContainerStopTimeout:         30 * time.Second,
		ReconcileInterval:            5 * time.Minute,
		ProvisionTimeout:             10 * time.Minute,
		CallbackDBPath:               "callbacks.db",
		NetworkIsolation:             ptrBool(true),
		ContainerReadonlyRootfs:      ptrBool(true),
		ContainerPidsLimit:           ptrInt64(256),
		ContainerTmpfsSizeMB:         64,
		CallbackMaxAge:               24 * time.Hour,
		DiagnosticsDBPath:            "diagnostics.db",
		DiagnosticsMaxAge:            7 * 24 * time.Hour,
		ReleasesDBPath:               "releases.db",
		ReleasesMaxAge:               90 * 24 * time.Hour,
		RetentionDBPath:              "retention.db",
		RetentionMaxAge:              90 * 24 * time.Hour,
		RetentionReapInterval:        time.Hour,
		RetentionOrphanConfirmations: 3,
		MigrationGracePeriod:         defaultMigrationGracePeriod,
		MigrationReadyTimeout:        defaultMigrationReadyTimeout,
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

	// A non-positive cap (unset in YAML, or 0) falls back to the default rather
	// than disabling the body limit entirely. (ENG-448 / F42)
	if c.MaxRequestBodySize <= 0 {
		c.MaxRequestBodySize = DefaultMaxRequestBodySize
	}

	if c.ListenAddr == "" {
		return fmt.Errorf("listen_addr is required")
	}

	// TLS: cert and key are set together; client-CA (mTLS) needs the listener
	// on TLS first; client-name pinning needs mTLS.
	if (c.TLSCertFile != "") != (c.TLSKeyFile != "") {
		return fmt.Errorf("both tls_cert_file and tls_key_file must be set together")
	}
	if c.TLSClientCAFile != "" && c.TLSCertFile == "" {
		return fmt.Errorf("tls_client_ca_file requires tls_cert_file and tls_key_file (mTLS needs the listener on TLS)")
	}
	if len(c.TLSClientAllowedNames) > 0 && c.TLSClientCAFile == "" {
		return fmt.Errorf("tls_client_allowed_names requires tls_client_ca_file (mTLS must be enabled to pin client identity)")
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

	if c.ReleasesMaxAge < 0 {
		return fmt.Errorf("releases_max_age must be non-negative")
	}

	if c.RetentionMaxAge < 0 {
		return fmt.Errorf("retention_max_age must be non-negative")
	}

	if c.RetentionReapInterval < 0 {
		return fmt.Errorf("retention_reap_interval must be non-negative")
	}

	if c.MaxRetainedLeasesPerTenant < 0 {
		return fmt.Errorf("max_retained_leases_per_tenant must be non-negative")
	}

	if c.RetentionOrphanConfirmations < 0 {
		return fmt.Errorf("retention_orphan_confirmations must be non-negative")
	}
	if c.MaxRetainedDiskMB < 0 {
		return fmt.Errorf("max_retained_disk_mb must be non-negative")
	}
	if c.MaxRetainedDiskMB > 0 {
		if c.MaxRetainedDiskMB > c.TotalDiskMB {
			return fmt.Errorf("max_retained_disk_mb (%d) must not exceed total_disk_mb (%d)", c.MaxRetainedDiskMB, c.TotalDiskMB)
		}
		if largest := c.largestSKUDiskMB(); largest > 0 && c.MaxRetainedDiskMB < largest {
			return fmt.Errorf("max_retained_disk_mb (%d) must be >= the largest stateful SKU disk_mb (%d), else a SKU-legal lease could never be retained", c.MaxRetainedDiskMB, largest)
		}
	}

	// max_retained_disk_mb_per_tenant: the L1-default per-tenant disk cap. Same
	// refuse-to-retain (destroy) semantics as the global cap, so the below-largest
	// SKU check is likewise a hard error (a cap under the largest stateful SKU
	// means a SKU-legal lease is always refused ⇒ destroyed).
	if c.MaxRetainedDiskMBPerTenant < 0 {
		return fmt.Errorf("max_retained_disk_mb_per_tenant must be non-negative")
	}
	if c.MaxRetainedDiskMBPerTenant > 0 {
		if c.MaxRetainedDiskMBPerTenant > c.TotalDiskMB {
			return fmt.Errorf("max_retained_disk_mb_per_tenant (%d) must not exceed total_disk_mb (%d)", c.MaxRetainedDiskMBPerTenant, c.TotalDiskMB)
		}
		if c.MaxRetainedDiskMB > 0 && c.MaxRetainedDiskMBPerTenant > c.MaxRetainedDiskMB {
			return fmt.Errorf("max_retained_disk_mb_per_tenant (%d) must not exceed max_retained_disk_mb (%d)", c.MaxRetainedDiskMBPerTenant, c.MaxRetainedDiskMB)
		}
		if largest := c.largestSKUDiskMB(); largest > 0 && c.MaxRetainedDiskMBPerTenant < largest {
			return fmt.Errorf("max_retained_disk_mb_per_tenant (%d) must be >= the largest stateful SKU disk_mb (%d), else a SKU-legal lease could never be retained", c.MaxRetainedDiskMBPerTenant, largest)
		}
	}

	// retention_partition_source: a malformed or unsatisfiable source is a startup
	// failure, never a close-time surprise. ParsePartitionSource also rejects
	// reserved label prefixes / blocked env keys via the shared manifest helpers,
	// so an unsatisfiable key (one no tenant manifest could ever declare) is caught
	// here rather than silently never matching.
	if _, err := shared.ParsePartitionSource(c.RetentionPartitionSource); err != nil {
		return fmt.Errorf("retention_partition_source: %w", err)
	}

	// retention_tenant_budgets: per-entry validation, name-prefixed errors
	// (mirrors the sku_profiles validate loop, config.go:468-472).
	for tenant, budget := range c.RetentionTenantBudgets {
		if err := c.validateRetentionTenantBudget(tenant, budget); err != nil {
			return err
		}
	}

	// Volume management validation
	if c.HasStatefulSKUs() && c.VolumeDataPath == "" {
		return fmt.Errorf("volume_data_path is required when any SKU profile has disk_mb > 0")
	}
	if strings.ContainsAny(c.VolumeDataPath, " \t\n") {
		return fmt.Errorf("volume_data_path must not contain whitespace (got %q)", c.VolumeDataPath)
	}
	if c.VolumeFilesystem != "" {
		switch c.VolumeFilesystem {
		case "btrfs", "xfs", "zfs":
			// valid
		default:
			return fmt.Errorf("volume_filesystem must be btrfs, xfs, or zfs (got %q)", c.VolumeFilesystem)
		}
	}

	// min_avg_file_bytes has inverted semantics (smaller => looser inode cap), so a
	// too-small value silently uncaps the ENG-548 inode limit. Reject negative and
	// nonzero values below 512 (the most generous ratio the design admits); 0 = default.
	if c.MinAvgFileBytes < 0 || (c.MinAvgFileBytes > 0 && c.MinAvgFileBytes < 512) {
		return fmt.Errorf("min_avg_file_bytes must be 0 (default 1024) or >= 512 (got %d)", c.MinAvgFileBytes)
	}

	if err := c.Ingress.Validate(); err != nil {
		return err
	}

	if c.Ingress.Enabled && !c.IsNetworkIsolation() {
		return fmt.Errorf("ingress requires network_isolation to be enabled")
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

	// Production mode security enforcement (runs after all basic validation).
	// callback_insecure_skip_verify disables TLS verification on the backend →
	// Fred callback hop; it is a dev-only escape hatch and must never reach a
	// production deployment.
	if c.ProductionMode && c.CallbackInsecureSkipVerify {
		return fmt.Errorf("production_mode: callback_insecure_skip_verify cannot be enabled")
	}

	return nil
}

func validateSKUProfile(name string, profile SKUProfile) error {
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("SKU %q: %w", name, err)
	}
	return nil
}

// validateRetentionTenantBudget validates one aggregator-allowlist budget entry.
// The tenant key is opaque (no bech32 parse — the backend treats tenant as an
// opaque string everywhere). Aggregates are REQUIRED positive: "unlimited" is
// unrepresentable inside a budget by construction (closes the 0=unlimited hole
// and bounds every per-close tenant scan). Disk caps use the same
// below-largest-SKU hard error as the global cap: a disk cap under the largest
// stateful SKU means a SKU-legal lease is always refused ⇒ destroyed.
func (c *Config) validateRetentionTenantBudget(tenant string, b RetentionTenantBudget) error {
	if strings.TrimSpace(tenant) == "" {
		return fmt.Errorf("retention_tenant_budgets[%q]: tenant key must not be empty", tenant)
	}
	if strings.IndexFunc(tenant, unicode.IsSpace) >= 0 {
		return fmt.Errorf("retention_tenant_budgets[%q]: tenant key must not contain whitespace", tenant)
	}
	if len(tenant) > maxRetentionBudgetKeyLen {
		return fmt.Errorf("retention_tenant_budgets[%q]: tenant key too long (%d > %d)", tenant, len(tenant), maxRetentionBudgetKeyLen)
	}

	// L1 aggregate count — REQUIRED > 0, bounded by the compile-time ceiling.
	if b.MaxRetainedLeases <= 0 {
		return fmt.Errorf("retention_tenant_budgets[%s].max_retained_leases must be > 0 (a budget cannot express unlimited)", tenant)
	}
	if b.MaxRetainedLeases > maxRetentionBudgetLeases {
		return fmt.Errorf("retention_tenant_budgets[%s].max_retained_leases (%d) must be <= %d", tenant, b.MaxRetainedLeases, maxRetentionBudgetLeases)
	}

	// L1 aggregate disk — REQUIRED > 0, bounded by the pool / global cap, and at
	// least the largest stateful SKU.
	largest := c.largestSKUDiskMB()
	if b.MaxRetainedDiskMB <= 0 {
		return fmt.Errorf("retention_tenant_budgets[%s].max_retained_disk_mb must be > 0 (a budget cannot express unlimited)", tenant)
	}
	if b.MaxRetainedDiskMB > c.TotalDiskMB {
		return fmt.Errorf("retention_tenant_budgets[%s].max_retained_disk_mb (%d) must not exceed total_disk_mb (%d)", tenant, b.MaxRetainedDiskMB, c.TotalDiskMB)
	}
	if c.MaxRetainedDiskMB > 0 && b.MaxRetainedDiskMB > c.MaxRetainedDiskMB {
		return fmt.Errorf("retention_tenant_budgets[%s].max_retained_disk_mb (%d) must not exceed max_retained_disk_mb (%d)", tenant, b.MaxRetainedDiskMB, c.MaxRetainedDiskMB)
	}
	if largest > 0 && b.MaxRetainedDiskMB < largest {
		return fmt.Errorf("retention_tenant_budgets[%s].max_retained_disk_mb (%d) must be >= the largest stateful SKU disk_mb (%d), else a SKU-legal lease could never be retained", tenant, b.MaxRetainedDiskMB, largest)
	}

	// max_partitions in [0, ceiling]; 0 = elevation-only (labels still collapse).
	if b.MaxPartitions < 0 {
		return fmt.Errorf("retention_tenant_budgets[%s].max_partitions must be non-negative", tenant)
	}
	if b.MaxPartitions > maxRetentionPartitionsPerTenant {
		return fmt.Errorf("retention_tenant_budgets[%s].max_partitions (%d) must be <= %d", tenant, b.MaxPartitions, maxRetentionPartitionsPerTenant)
	}

	// L2 count sub-cap — optional; when set, bounded by the aggregate and requires
	// partitions to be enabled.
	if b.PerPartitionMaxLeases < 0 {
		return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_leases must be non-negative", tenant)
	}
	if b.PerPartitionMaxLeases > 0 {
		if b.PerPartitionMaxLeases > b.MaxRetainedLeases {
			return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_leases (%d) must not exceed max_retained_leases (%d)", tenant, b.PerPartitionMaxLeases, b.MaxRetainedLeases)
		}
		if b.MaxPartitions <= 0 {
			return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_leases (%d) requires max_partitions > 0", tenant, b.PerPartitionMaxLeases)
		}
	}

	// L2 disk sub-cap — optional; requires a count sub-cap (jam guard), partitions
	// enabled, and the same pool / SKU bounds as the aggregate disk.
	if b.PerPartitionMaxDiskMB < 0 {
		return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_disk_mb must be non-negative", tenant)
	}
	if b.PerPartitionMaxDiskMB > 0 {
		if b.PerPartitionMaxLeases <= 0 {
			return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_disk_mb requires per_partition_max_leases > 0: a disk-only sub-cap has no rolling mechanism — once full, every close in that partition is refused (destroyed) forever", tenant)
		}
		if b.MaxPartitions <= 0 {
			return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_disk_mb requires max_partitions > 0", tenant)
		}
		if b.PerPartitionMaxDiskMB > b.MaxRetainedDiskMB {
			return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_disk_mb (%d) must not exceed max_retained_disk_mb (%d)", tenant, b.PerPartitionMaxDiskMB, b.MaxRetainedDiskMB)
		}
		if largest > 0 && b.PerPartitionMaxDiskMB < largest {
			return fmt.Errorf("retention_tenant_budgets[%s].per_partition_max_disk_mb (%d) must be >= the largest stateful SKU disk_mb (%d), else a SKU-legal lease could never be retained", tenant, b.PerPartitionMaxDiskMB, largest)
		}
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
		return SKUProfile{}, fmt.Errorf("%w: %s (profile: %s)", backend.ErrUnknownSKU, sku, profileName)
	}
	return profile, nil
}
