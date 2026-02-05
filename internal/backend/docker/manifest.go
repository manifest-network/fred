package docker

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
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
	"LD_",    // dynamic linker (LD_PRELOAD, LD_LIBRARY_PATH, LD_AUDIT, etc.)
	"FRED_",  // reserved for backend-internal use
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

// blockedTmpfsPaths are paths that cannot be used as tmpfs mounts because they
// are managed by the backend (/tmp, /run) or are sensitive kernel filesystems.
var blockedTmpfsPaths = map[string]bool{
	"/tmp":  true,
	"/run":  true,
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

		// Must not overlap with blocked paths or their subdirectories.
		// For example, /proc/self and /sys/fs/cgroup must be rejected.
		if blockedTmpfsPaths[cleaned] {
			return fmt.Errorf("tmpfs: path %q is managed by the backend or is a sensitive path", cleaned)
		}
		for blocked := range blockedTmpfsPaths {
			if strings.HasPrefix(cleaned, blocked+"/") {
				return fmt.Errorf("tmpfs: path %q is under sensitive path %q", cleaned, blocked)
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
