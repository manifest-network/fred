package docker

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// maxTmpfsMounts is the maximum number of additional tmpfs mounts a tenant can request.
// Combined with the automatic /tmp and /run mounts (2), and the default 64MB per mount,
// this limits total tmpfs memory to 6 × 64MB = 384MB. This prevents tenants from using
// excessive RAM via tmpfs mounts that bypass the container's cgroup memory limit.
const maxTmpfsMounts = 4

// DockerManifest represents the tenant's container specification.
// Resource limits are determined by the SKU, not the manifest.
type DockerManifest struct {
	// Image is the container image to run (required).
	Image string `json:"image"`

	// Ports specifies port mappings. Key format: "port/protocol" (e.g., "80/tcp").
	// Value can specify optional host_port.
	Ports map[string]PortConfig `json:"ports,omitempty"`

	// Env specifies environment variables.
	Env map[string]string `json:"env,omitempty"`

	// Command overrides the container entrypoint.
	Command []string `json:"command,omitempty"`

	// Args provides arguments to the command.
	Args []string `json:"args,omitempty"`

	// Labels adds custom labels to the container.
	Labels map[string]string `json:"labels,omitempty"`

	// HealthCheck configures container health checking.
	HealthCheck *HealthCheckConfig `json:"health_check,omitempty"`

	// Tmpfs specifies additional writable tmpfs mount paths for containers
	// running with a read-only root filesystem. Paths like /tmp and /run are
	// always mounted automatically. Use this for application-specific writable
	// directories (e.g., "/var/cache/nginx", "/var/log/nginx").
	// Each mount uses the operator-configured tmpfs size limit.
	Tmpfs []string `json:"tmpfs,omitempty"`

	// User overrides the container's runtime user. Required for images like
	// postgres whose entrypoint starts as root and tries to chown data
	// directories — since CapDrop ALL removes CAP_CHOWN, the container must
	// run directly as the target user instead.
	// Accepts the same formats as Docker's USER directive: "uid", "uid:gid",
	// "username", or "username:group". When set, volumes are pre-chowned to
	// the resolved UID/GID and container.Config.User is set accordingly.
	User string `json:"user,omitempty"`
}

// PortConfig specifies port mapping configuration.
type PortConfig struct {
	// HostPort optionally specifies a fixed host port.
	// If not set, Docker assigns a random available port.
	HostPort int `json:"host_port,omitempty"`
}

// HealthCheckConfig specifies container health check parameters.
type HealthCheckConfig struct {
	// Test is the health check command. First element can be "CMD", "CMD-SHELL", or "NONE".
	Test []string `json:"test"`

	// Interval between health checks.
	Interval Duration `json:"interval,omitempty"`

	// Timeout for each health check.
	Timeout Duration `json:"timeout,omitempty"`

	// Retries before marking unhealthy.
	Retries int `json:"retries,omitempty"`

	// StartPeriod is the initial grace period.
	StartPeriod Duration `json:"start_period,omitempty"`
}

// Duration wraps time.Duration for JSON unmarshaling from strings like "30s".
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		// Try as number (nanoseconds)
		var n int64
		if err := json.Unmarshal(b, &n); err != nil {
			return fmt.Errorf("duration must be a string or number: %w", err)
		}
		*d = Duration(n)
		return nil
	}

	dur, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(dur)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// ParseManifest parses and validates a DockerManifest from JSON.
func ParseManifest(data []byte) (*DockerManifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("manifest is empty")
	}

	var m DockerManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("invalid manifest JSON: %w", err)
	}

	if err := m.Validate(); err != nil {
		return nil, err
	}

	return &m, nil
}

// Validate checks that the manifest is valid.
func (m *DockerManifest) Validate() error {
	if m.Image == "" {
		return fmt.Errorf("image is required")
	}

	// Validate port specifications
	for portSpec, portCfg := range m.Ports {
		if err := validatePortSpec(portSpec); err != nil {
			return fmt.Errorf("invalid port %q: %w", portSpec, err)
		}
		if portCfg.HostPort < 0 || portCfg.HostPort > 65535 {
			return fmt.Errorf("invalid port %q: host_port must be between 0 and 65535", portSpec)
		}
	}

	// Validate health check if present
	if m.HealthCheck != nil {
		if err := m.HealthCheck.Validate(); err != nil {
			return fmt.Errorf("invalid health_check: %w", err)
		}
	}

	// Validate environment variables
	if err := validateEnvVars(m.Env); err != nil {
		return err
	}

	// Validate labels don't conflict with fred.* namespace
	for key := range m.Labels {
		if strings.HasPrefix(key, "fred.") {
			return fmt.Errorf("labels cannot use reserved prefix 'fred.': %s", key)
		}
	}

	// Validate tmpfs mounts
	if err := validateTmpfsPaths(m.Tmpfs); err != nil {
		return err
	}

	// Validate user field if present
	if m.User != "" {
		if err := validateUserSpec(m.User); err != nil {
			return err
		}
	}

	return nil
}

// validateUserSpec validates a Docker USER specification.
// Valid formats: "uid", "uid:gid", "username", "username:group".
func validateUserSpec(user string) error {
	if strings.ContainsAny(user, " \t\n\r") {
		return fmt.Errorf("user: must not contain whitespace: %q", user)
	}
	parts := strings.SplitN(user, ":", 2)
	if parts[0] == "" {
		return fmt.Errorf("user: user part cannot be empty: %q", user)
	}
	if len(parts) == 2 && parts[1] == "" {
		return fmt.Errorf("user: group part cannot be empty after colon: %q", user)
	}
	return nil
}

// validatePortSpec validates a port specification like "80/tcp".
func validatePortSpec(spec string) error {
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return fmt.Errorf("must be in format 'port/protocol'")
	}

	port := parts[0]
	protocol := parts[1]

	// Validate port is a number
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("port must be a number")
	}
	if portNum < 1 || portNum > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	// Validate protocol
	protocol = strings.ToLower(protocol)
	if protocol != "tcp" && protocol != "udp" {
		return fmt.Errorf("protocol must be 'tcp' or 'udp'")
	}

	return nil
}

// HealthCheckTestType represents the type prefix of a Docker health check test command.
type HealthCheckTestType string

const (
	HealthCheckTestCMD      HealthCheckTestType = "CMD"
	HealthCheckTestCMDShell HealthCheckTestType = "CMD-SHELL"
	HealthCheckTestNone     HealthCheckTestType = "NONE"
)

// HasActiveHealthCheck returns true when the manifest declares a health check
// that is not disabled (i.e., Test[0] is not "NONE").
func (m *DockerManifest) HasActiveHealthCheck() bool {
	return m.HealthCheck != nil && len(m.HealthCheck.Test) > 0 && HealthCheckTestType(m.HealthCheck.Test[0]) != HealthCheckTestNone
}

// blockedEnvNames are environment variable names that tenants cannot set.
// These variables control process execution in ways that could compromise
// container security or interfere with the runtime.
var blockedEnvNames = map[string]bool{
	"PATH": true, // prevents command hijacking
}

// blockedEnvPrefixes are prefixes for environment variable names that tenants
// cannot set. Any variable whose uppercase name starts with one of these
// prefixes is rejected.
var blockedEnvPrefixes = []string{
	"LD_",     // dynamic linker (LD_PRELOAD, LD_LIBRARY_PATH, LD_AUDIT, etc.)
	"FRED_",   // reserved for backend-internal use
	"DOCKER_", // Docker runtime variables
}

// validateEnvVars checks that environment variable names are safe.
func validateEnvVars(env map[string]string) error {
	for k := range env {
		if k == "" {
			return fmt.Errorf("env: variable name cannot be empty")
		}
		if strings.ContainsAny(k, "=\x00") {
			return fmt.Errorf("env: variable name contains invalid character: %q", k)
		}

		upper := strings.ToUpper(k)
		if blockedEnvNames[upper] {
			return fmt.Errorf("env: variable %q is not allowed", k)
		}
		for _, prefix := range blockedEnvPrefixes {
			if strings.HasPrefix(upper, prefix) {
				return fmt.Errorf("env: variable %q is not allowed (prefix %q is reserved)", k, prefix)
			}
		}
	}
	return nil
}

// blockedTmpfsPaths cannot be used as tmpfs mounts. Backend-managed paths
// (/tmp, /run) are blocked at the root but allow subdirectories (e.g.,
// /run/mysqld) since those are nested tmpfs within an existing tmpfs.
// Kernel filesystems block both the root and all subdirectories.
var blockedTmpfsPaths = map[string]bool{
	"/tmp":  true,
	"/run":  true,
	"/proc": true,
	"/sys":  true,
	"/dev":  true,
}

// sensitiveKernelPaths block all subdirectories — arbitrary tmpfs mounts
// under these could mask kernel state.
var sensitiveKernelPaths = map[string]bool{
	"/proc": true,
	"/sys":  true,
	"/dev":  true,
}

// validateTmpfsPaths checks that tmpfs mount paths are safe and reasonable.
func validateTmpfsPaths(paths []string) error {
	if len(paths) > maxTmpfsMounts {
		return fmt.Errorf("tmpfs: too many mounts (%d), maximum is %d", len(paths), maxTmpfsMounts)
	}

	seen := make(map[string]bool, len(paths))
	for _, p := range paths {
		// Must be absolute
		if !path.IsAbs(p) {
			return fmt.Errorf("tmpfs: path must be absolute: %q", p)
		}

		// Clean the path to normalize (resolve .., trailing slashes, etc.)
		// Note: path.Clean resolves all ".." components, so paths like "/var/../etc"
		// become "/etc". The blockedTmpfsPaths check below handles sensitive paths.
		cleaned := path.Clean(p)

		// Must not be the root filesystem
		if cleaned == "/" {
			return fmt.Errorf("tmpfs: cannot mount root filesystem")
		}

		// Exact blocked paths (both backend-managed and kernel) are rejected.
		if blockedTmpfsPaths[cleaned] {
			return fmt.Errorf("tmpfs: path %q is managed by the backend or is a sensitive path", cleaned)
		}
		// Sub-paths are only blocked under sensitive kernel filesystems.
		// Sub-paths under backend-managed tmpfs (/tmp, /run) are allowed
		// because they create harmless nested tmpfs mounts (e.g.,
		// /run/mysqld for databases that need a socket directory).
		for sensitive := range sensitiveKernelPaths {
			if strings.HasPrefix(cleaned, sensitive+"/") {
				return fmt.Errorf("tmpfs: path %q is under sensitive path %q", cleaned, sensitive)
			}
		}

		// Check for duplicates
		if seen[cleaned] {
			return fmt.Errorf("tmpfs: duplicate path: %q", cleaned)
		}
		seen[cleaned] = true
	}

	return nil
}

// StackManifest represents a multi-service deployment where each service
// has its own DockerManifest. The services map is keyed by service name
// (matching LeaseItem.ServiceName from the chain).
type StackManifest struct {
	Services map[string]*DockerManifest `json:"services"`
}

// Validate checks that the stack manifest is valid.
func (s *StackManifest) Validate() error {
	if len(s.Services) == 0 {
		return fmt.Errorf("stack manifest must have at least one service")
	}
	for name, svc := range s.Services {
		if name == "" {
			return fmt.Errorf("service name cannot be empty")
		}
		if svc == nil {
			return fmt.Errorf("service %q has nil manifest", name)
		}
		if err := svc.Validate(); err != nil {
			return fmt.Errorf("service %q: %w", name, err)
		}
	}
	return nil
}

// ParsePayload auto-detects whether data is a single DockerManifest or a
// StackManifest (contains a "services" key). Returns exactly one non-nil result.
func ParsePayload(data []byte) (*DockerManifest, *StackManifest, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("payload is empty")
	}

	// Probe for the "services" key to detect stack format.
	var probe struct {
		Services json.RawMessage `json:"services"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, nil, fmt.Errorf("invalid payload JSON: %w", err)
	}

	if probe.Services != nil {
		var stack StackManifest
		if err := json.Unmarshal(data, &stack); err != nil {
			return nil, nil, fmt.Errorf("invalid stack manifest JSON: %w", err)
		}
		if err := stack.Validate(); err != nil {
			return nil, nil, err
		}
		return nil, &stack, nil
	}

	manifest, err := ParseManifest(data)
	if err != nil {
		return nil, nil, err
	}
	return manifest, nil, nil
}

// ValidateStackAgainstItems ensures a 1:1 mapping between manifest service
// names and lease item service names. Every lease item must have a matching
// service in the manifest, and vice versa.
func ValidateStackAgainstItems(stack *StackManifest, items []backend.LeaseItem) error {
	manifestNames := make(map[string]bool, len(stack.Services))
	for name := range stack.Services {
		manifestNames[name] = true
	}

	itemNames := make(map[string]bool, len(items))
	for _, item := range items {
		itemNames[item.ServiceName] = true
	}

	// Check for services in manifest but not in lease items.
	for name := range manifestNames {
		if !itemNames[name] {
			return fmt.Errorf("manifest service %q has no matching lease item", name)
		}
	}

	// Check for lease items not in manifest.
	for name := range itemNames {
		if !manifestNames[name] {
			return fmt.Errorf("lease item service %q has no matching manifest service", name)
		}
	}

	return nil
}

// Validate checks that the health check configuration is valid.
func (h *HealthCheckConfig) Validate() error {
	if len(h.Test) == 0 {
		return fmt.Errorf("test is required")
	}

	// First element must be CMD, CMD-SHELL, or NONE
	switch HealthCheckTestType(h.Test[0]) {
	case HealthCheckTestCMD, HealthCheckTestCMDShell:
		if len(h.Test) < 2 {
			return fmt.Errorf("test requires at least one command after %s", h.Test[0])
		}
	case HealthCheckTestNone:
		// Valid, disables health check
	default:
		return fmt.Errorf("test must start with CMD, CMD-SHELL, or NONE")
	}

	if h.Retries < 0 {
		return fmt.Errorf("retries cannot be negative")
	}

	return nil
}
