package docker

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

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

	// Validate labels don't conflict with fred.* namespace
	for key := range m.Labels {
		if strings.HasPrefix(key, "fred.") {
			return fmt.Errorf("labels cannot use reserved prefix 'fred.': %s", key)
		}
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

// Validate checks that the health check configuration is valid.
func (h *HealthCheckConfig) Validate() error {
	if len(h.Test) == 0 {
		return fmt.Errorf("test is required")
	}

	// First element must be CMD, CMD-SHELL, or NONE
	switch h.Test[0] {
	case "CMD", "CMD-SHELL":
		if len(h.Test) < 2 {
			return fmt.Errorf("test requires at least one command after %s", h.Test[0])
		}
	case "NONE":
		// Valid, disables health check
	default:
		return fmt.Errorf("test must start with CMD, CMD-SHELL, or NONE")
	}

	if h.Retries < 0 {
		return fmt.Errorf("retries cannot be negative")
	}

	return nil
}
