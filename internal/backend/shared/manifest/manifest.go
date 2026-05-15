// Package manifest defines the tenant-facing container manifest schema
// (single-container flatManifest and multi-service StackManifest), JSON
// parsing, and validation. It is consumed by every backend that
// provisions tenant workloads (Docker, K3s, future substrates) so the
// on-the-wire schema and validation stay identical across substrates.
//
// # Substrate boundaries
//
// Some fields here carry Docker-flavored conventions because that is
// the lingua franca of the schema. K8s/K3s translation is the job of
// each backend, not of this package:
//
//   - HealthCheckConfig.Test uses the Docker exec format
//     (["CMD", cmd, args...], ["CMD-SHELL", "shell-string"], ["NONE"]).
//     A K3s/K8s backend must translate this to exec / httpGet /
//     tcpSocket probes substrate-side; that translation is out of
//     scope for this package.
//   - validateUserSpec accepts Docker's USER-directive format
//     (uid, uid:gid, username, username:group). Generic enough to
//     remain here as the wire format; backends translate to
//     securityContext.runAsUser / runAsGroup as needed.
//   - Tmpfs and StopGracePeriod are Docker-flavored field names but
//     translate cleanly to K8s emptyDir.medium=Memory tmpfs mounts and
//     terminationGracePeriodSeconds; backends do that translation
//     substrate-side.
package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// DefaultServiceName is the synthetic service name applied when fred wraps a
// legacy flat manifest into a 1-service stack. Centralized so callers stay
// consistent.
const DefaultServiceName = "app"

// Manifest is the exported alias for the per-service manifest type within
// a StackManifest. Function signatures that operate on an individual
// service inside a stack (compose project builders, container-create
// params, readiness waiters) take *Manifest to receive a service's spec
// by reference; the underlying type is the unexported flatManifest, kept
// unexported so the JSON wire format stays single-rooted under "services"
// (no external code can construct a top-level flat manifest by
// value-literal).
//
// This alias is permanent — it is NOT transitional. The original Task 2
// commit framed it as a "transitional shim" pending removal in Task 14/15,
// but the Task 15 analysis surfaced that external packages legitimately
// hold per-service references via this name and the alias serves an
// ongoing role. The exported manifest API is StackManifest as the
// top-level wire shape plus Manifest as the per-service handle for
// in-process traversal.
type Manifest = flatManifest

// serviceNameRe restricts service names to DNS-label-safe characters.
// This prevents path traversal, invalid Docker container names, and
// other injection via service names interpolated into container names,
// volume IDs, filesystem paths, and network aliases.
var serviceNameRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// MaxTmpfsMounts is the maximum number of additional tmpfs mounts a tenant can request.
// Combined with the automatic /tmp and /run mounts (2), and the default 64MB per mount,
// this limits total tmpfs memory to 6 × 64MB = 384MB. This prevents tenants from using
// excessive RAM via tmpfs mounts that bypass the container's cgroup memory limit.
const MaxTmpfsMounts = 4

// flatManifest represents the tenant's container specification.
// Resource limits are determined by the SKU, not the manifest.
type flatManifest struct {
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
	//
	// Field name follows Docker convention. K8s/K3s backends translate this
	// substrate-side to emptyDir mounts with medium=Memory.
	Tmpfs []string `json:"tmpfs,omitempty"`

	// User overrides the container's runtime user. Required for images like
	// postgres whose entrypoint starts as root and tries to chown data
	// directories — since CapDrop ALL removes CAP_CHOWN, the container must
	// run directly as the target user instead.
	// Accepts the same formats as Docker's USER directive: "uid", "uid:gid",
	// "username", or "username:group". When set, volumes are pre-chowned to
	// the resolved UID/GID and container.Config.User is set accordingly.
	User string `json:"user,omitempty"`

	// DependsOn declares startup dependencies on other services (stack-only).
	// Keys are service names; values specify the condition to wait for.
	DependsOn map[string]DependsOnCondition `json:"depends_on,omitempty"`

	// StopGracePeriod is the time to wait after SIGTERM before sending SIGKILL.
	// Bounded to 1s–120s.
	//
	// Field name follows Docker convention. K8s/K3s backends translate this
	// substrate-side to terminationGracePeriodSeconds.
	StopGracePeriod *Duration `json:"stop_grace_period,omitempty"`

	// Init runs an init process (tini) as PID 1 inside the container for
	// proper zombie reaping and signal forwarding.
	Init *bool `json:"init,omitempty"`

	// Expose documents inter-service ports without creating host bindings.
	// Values are port numbers as strings (e.g., "3000", "8080").
	Expose []string `json:"expose,omitempty"`
}

// DependsOnCondition specifies the condition a dependency must meet.
type DependsOnCondition struct {
	Condition string `json:"condition"`
}

// PortConfig specifies port mapping configuration.
type PortConfig struct {
	// HostPort optionally specifies a fixed host port.
	// If not set, Docker assigns a random available port.
	HostPort int `json:"host_port,omitempty"`

	// Ingress marks this port as the preferred ingress port for routing.
	// Only valid on TCP ports; at most one port in a manifest may set
	// this to true. When set, SelectIngressPort returns this port
	// regardless of the default preference order (80 > 8080 > lowest TCP).
	Ingress bool `json:"ingress,omitempty"`
}

// HealthCheckConfig specifies container health check parameters.
type HealthCheckConfig struct {
	// Test is the health check command. First element can be "CMD", "CMD-SHELL", or "NONE".
	//
	// The slice format follows Docker's exec convention:
	//   - ["CMD", "binary", "arg1", "arg2", ...] — exec the binary with args
	//   - ["CMD-SHELL", "shell-string"]          — exec /bin/sh -c shell-string
	//   - ["NONE"]                                — disable inherited health check
	//
	// K8s/K3s backends MUST translate this substrate-side to the appropriate
	// probe shape (exec / httpGet / tcpSocket). Translation is out of scope
	// for this package; this is the wire format the chain stores.
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

// ParseManifest parses and validates a flatManifest from JSON.
// Unknown fields are rejected to catch accidental misuse (e.g., passing
// a stack manifest where a single-container manifest is expected).
func ParseManifest(data []byte) (*flatManifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("manifest is empty")
	}

	var m flatManifest
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("invalid manifest JSON: %w", err)
	}

	if err := m.Validate(); err != nil {
		return nil, err
	}

	return &m, nil
}

// Validate checks that the manifest is valid for single-container use.
// It rejects stack-only fields like depends_on.
func (m *flatManifest) Validate() error {
	return m.validate(false)
}

// ValidateInStack checks that the manifest is valid for use within a stack.
// It allows stack-only fields like depends_on (cross-service validation is
// done separately by StackManifest.Validate).
func (m *flatManifest) ValidateInStack() error {
	return m.validate(true)
}

// validate is the shared validation logic. When inStack is false, stack-only
// fields (depends_on) are rejected.
func (m *flatManifest) validate(inStack bool) error {
	if m.Image == "" {
		return fmt.Errorf("image is required")
	}

	// depends_on is stack-only.
	if !inStack && len(m.DependsOn) > 0 {
		return fmt.Errorf("depends_on is only allowed in stack manifests")
	}

	// Validate port specifications
	var ingressPorts []string
	for portSpec, portCfg := range m.Ports {
		if err := validatePortSpec(portSpec); err != nil {
			return fmt.Errorf("invalid port %q: %w", portSpec, err)
		}
		if portCfg.HostPort < 0 || portCfg.HostPort > 65535 {
			return fmt.Errorf("invalid port %q: host_port must be between 0 and 65535", portSpec)
		}
		if portCfg.Ingress {
			ingressPorts = append(ingressPorts, portSpec)
			if _, ok := ParseTCPPort(portSpec); !ok {
				return fmt.Errorf("invalid port %q: ingress hint requires TCP protocol", portSpec)
			}
		}
	}
	if len(ingressPorts) > 1 {
		return fmt.Errorf("at most one port may have ingress set to true, got %v", ingressPorts)
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

	// Validate stop_grace_period bounds (1s–120s).
	if m.StopGracePeriod != nil {
		d := m.StopGracePeriod.Duration()
		if d < time.Second {
			return fmt.Errorf("stop_grace_period must be at least 1s, got %s", d)
		}
		if d > 120*time.Second {
			return fmt.Errorf("stop_grace_period must be at most 120s, got %s", d)
		}
	}

	// Validate expose ports.
	if err := validateExposePorts(m.Expose); err != nil {
		return err
	}

	return nil
}

// validateUserSpec validates a Docker USER specification.
// Valid formats: "uid", "uid:gid", "username", "username:group".
//
// This format is generic enough to remain the wire format across
// substrates. K8s/K3s backends translate uid/gid substrate-side
// (securityContext.runAsUser / runAsGroup); username strings require
// resolution against the container image's /etc/passwd, which is also
// substrate-side.
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

// ParseTCPPort extracts the port number from a "port/tcp" spec string
// (case-insensitive protocol). Returns (port, true) for valid TCP specs,
// (0, false) otherwise.
func ParseTCPPort(spec string) (int, bool) {
	parts := strings.SplitN(spec, "/", 2)
	if len(parts) != 2 || strings.ToLower(parts[1]) != "tcp" {
		return 0, false
	}
	port, err := strconv.Atoi(parts[0])
	if err != nil || port < 1 || port > 65535 {
		return 0, false
	}
	return port, true
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
func (m *flatManifest) HasActiveHealthCheck() bool {
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
	if len(paths) > MaxTmpfsMounts {
		return fmt.Errorf("tmpfs: too many mounts (%d), maximum is %d", len(paths), MaxTmpfsMounts)
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

// validateExposePorts checks that expose port values are valid port numbers
// with no duplicates.
func validateExposePorts(ports []string) error {
	seen := make(map[string]bool, len(ports))
	for _, p := range ports {
		port, err := strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("expose: %q is not a valid port number", p)
		}
		if port < 1 || port > 65535 {
			return fmt.Errorf("expose: port %d must be between 1 and 65535", port)
		}
		if seen[p] {
			return fmt.Errorf("expose: duplicate port %q", p)
		}
		seen[p] = true
	}
	return nil
}

// StackManifest represents a multi-service deployment where each service
// has its own flatManifest. The services map is keyed by service name
// (matching LeaseItem.ServiceName from the chain).
type StackManifest struct {
	Services map[string]*flatManifest `json:"services"`
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
		if len(name) > 63 {
			return fmt.Errorf("service name %q exceeds 63 characters", name)
		}
		if !serviceNameRe.MatchString(name) {
			return fmt.Errorf("service name %q must match [a-z0-9]([a-z0-9-]*[a-z0-9])?", name)
		}
		if svc == nil {
			return fmt.Errorf("service %q has nil manifest", name)
		}
		if err := svc.ValidateInStack(); err != nil {
			return fmt.Errorf("service %q: %w", name, err)
		}
	}

	// Cross-service depends_on validation.
	if err := s.validateDependsOn(); err != nil {
		return err
	}

	return nil
}

// dependsOnMaxDepth is the maximum dependency chain depth for cycle detection.
const dependsOnMaxDepth = 10

// validateDependsOn checks cross-service dependency constraints:
// - Referenced services must exist in the stack.
// - No self-dependencies.
// - Only "service_started" and "service_healthy" conditions allowed.
// - "service_healthy" requires the referenced service to have an active health check.
// - No cycles (DFS with max depth 10).
func (s *StackManifest) validateDependsOn() error {
	for name, svc := range s.Services {
		for dep, cond := range svc.DependsOn {
			// Self-dependency.
			if dep == name {
				return fmt.Errorf("service %q: depends_on cannot reference itself", name)
			}

			// Referenced service must exist.
			depSvc, ok := s.Services[dep]
			if !ok {
				return fmt.Errorf("service %q: depends_on references unknown service %q", name, dep)
			}

			// Validate condition.
			switch cond.Condition {
			case "service_started":
				// Always valid.
			case "service_healthy":
				if !depSvc.HasActiveHealthCheck() {
					return fmt.Errorf("service %q: depends_on %q with condition %q requires %q to have a health_check",
						name, dep, cond.Condition, dep)
				}
			case "":
				return fmt.Errorf("service %q: depends_on %q has empty condition", name, dep)
			default:
				return fmt.Errorf("service %q: depends_on %q has invalid condition %q (must be service_started or service_healthy)",
					name, dep, cond.Condition)
			}
		}
	}

	// Cycle detection via DFS.
	return s.detectDependsOnCycles()
}

// detectDependsOnCycles performs DFS-based cycle detection across the
// depends_on graph. Returns an error if any cycle is found or if the
// dependency chain exceeds dependsOnMaxDepth.
func (s *StackManifest) detectDependsOnCycles() error {
	const (
		white = 0 // unvisited
		gray  = 1 // in current path
		black = 2 // fully explored
	)

	color := make(map[string]int, len(s.Services))

	var dfs func(name string, depth int) error
	dfs = func(name string, depth int) error {
		if depth > dependsOnMaxDepth {
			return fmt.Errorf("depends_on: dependency chain exceeds maximum depth of %d", dependsOnMaxDepth)
		}
		color[name] = gray
		svc := s.Services[name]
		if svc == nil {
			return nil
		}
		for dep := range svc.DependsOn {
			switch color[dep] {
			case gray:
				return fmt.Errorf("depends_on: cycle detected involving service %q", dep)
			case white:
				if err := dfs(dep, depth+1); err != nil {
					return err
				}
			}
		}
		color[name] = black
		return nil
	}

	for name := range s.Services {
		if color[name] == white {
			if err := dfs(name, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParsePayload parses a deployment manifest payload and always returns the
// stack-shaped representation regardless of the input format.
//
// Stack-format input (`{"services": {...}}`) is parsed directly and
// validated. Flat-format input is parsed via the internal flatManifest type
// and wrapped as `{"services": {DefaultServiceName: <flat>}}`, then
// validated as a 1-service stack. Empty payloads and unparseable JSON
// return an error.
//
// Boundary-normalization contract: every downstream component consumes a
// *StackManifest. The legacy single-service shape is preserved on the wire
// for tenant compatibility but disappears at this seam. Deprecation
// logging for flat payloads happens at the call site, which has access to
// the logger and lease UUID; the parser stays pure.
func ParsePayload(data []byte) (*StackManifest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty payload")
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, isStack := probe["services"]; isStack {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		var stack StackManifest
		if err := dec.Decode(&stack); err != nil {
			return nil, fmt.Errorf("parse stack manifest: %w", err)
		}
		if err := stack.Validate(); err != nil {
			return nil, fmt.Errorf("validate stack manifest: %w", err)
		}
		return &stack, nil
	}

	// Flat (legacy) form — parse, validate, then auto-wrap.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var flat flatManifest
	if err := dec.Decode(&flat); err != nil {
		return nil, fmt.Errorf("parse flat manifest: %w", err)
	}
	if err := flat.Validate(); err != nil {
		return nil, fmt.Errorf("validate flat manifest: %w", err)
	}
	// depends_on is structurally impossible on a flat payload (no peers).
	// flat.Validate already rejects it via the non-inStack path, but the
	// guard documents the invariant at the wrap seam.
	if len(flat.DependsOn) > 0 {
		return nil, fmt.Errorf("depends_on is not valid in a flat manifest")
	}
	stack := &StackManifest{Services: map[string]*flatManifest{DefaultServiceName: &flat}}
	if err := stack.Validate(); err != nil {
		return nil, fmt.Errorf("validate auto-wrapped stack: %w", err)
	}
	return stack, nil
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
		if itemNames[item.ServiceName] {
			return fmt.Errorf("duplicate lease item service name %q", item.ServiceName)
		}
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
