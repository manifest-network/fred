package docker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// Labels used for tracking managed containers.
const (
	LabelManaged       = "fred.managed"
	LabelLeaseUUID     = "fred.lease_uuid"
	LabelTenant        = "fred.tenant"
	LabelProviderUUID  = "fred.provider_uuid"
	LabelSKU           = "fred.sku"
	LabelCreatedAt     = "fred.created_at"
	LabelInstanceIndex = "fred.instance_index"
	LabelFailCount     = "fred.fail_count"
)

// ContainerInfo holds information about a managed container.
type ContainerInfo struct {
	ContainerID   string
	LeaseUUID     string
	Tenant        string
	ProviderUUID  string
	SKU           string
	InstanceIndex int
	FailCount     int
	Image         string
	Status        string
	CreatedAt     time.Time
	Ports         map[string]PortBinding
}

// PortBinding represents a port mapping.
type PortBinding struct {
	HostIP   string `json:"host_ip"`
	HostPort string `json:"host_port"`
}

// DockerClient wraps the Docker client for container lifecycle operations.
type DockerClient struct {
	client *client.Client
}

// NewDockerClient creates a new Docker client.
func NewDockerClient(host string) (*DockerClient, error) {
	opts := []client.Opt{
		client.WithAPIVersionNegotiation(),
	}

	if host != "" {
		opts = append(opts, client.WithHost(host))
	}

	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &DockerClient{client: cli}, nil
}

// Close closes the Docker client.
func (d *DockerClient) Close() error {
	return d.client.Close()
}

// Ping checks connectivity to the Docker daemon.
func (d *DockerClient) Ping(ctx context.Context) error {
	_, err := d.client.Ping(ctx)
	return err
}

// PullImage pulls a container image with timeout.
func (d *DockerClient) PullImage(ctx context.Context, imageName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	reader, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Consume the output to complete the pull
	_, err = io.Copy(io.Discard, reader)
	if err != nil {
		return fmt.Errorf("failed to complete image pull: %w", err)
	}

	return nil
}

// CreateContainerParams holds parameters for creating a container.
type CreateContainerParams struct {
	LeaseUUID     string
	Tenant        string
	ProviderUUID  string
	SKU           string
	Manifest      *DockerManifest
	Profile       SKUProfile
	InstanceIndex int // For multi-unit leases (0-based index)

	// Retry tracking
	FailCount int

	// Hardening parameters
	HostBindIP     string
	ReadonlyRootfs bool
	PidsLimit      *int64
	TmpfsSizeMB    int
	NetworkConfig  *networktypes.NetworkingConfig
}

// CreateContainer creates a new container with the specified configuration.
func (d *DockerClient) CreateContainer(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Build labels
	labels := map[string]string{
		LabelManaged:       "true",
		LabelLeaseUUID:     params.LeaseUUID,
		LabelTenant:        params.Tenant,
		LabelProviderUUID:  params.ProviderUUID,
		LabelSKU:           params.SKU,
		LabelCreatedAt:     time.Now().Format(time.RFC3339),
		LabelInstanceIndex: strconv.Itoa(params.InstanceIndex),
		LabelFailCount:     strconv.Itoa(params.FailCount),
	}

	// Add user labels (already validated to not conflict with fred.*)
	for k, v := range params.Manifest.Labels {
		labels[k] = v
	}

	// Build environment variables
	var env []string
	for k, v := range params.Manifest.Env {
		env = append(env, k+"="+v)
	}

	// Build port bindings
	exposedPorts := nat.PortSet{}
	portBindings := nat.PortMap{}

	for portSpec, portCfg := range params.Manifest.Ports {
		port := nat.Port(portSpec)
		exposedPorts[port] = struct{}{}

		hostIP := params.HostBindIP
		if hostIP == "" {
			hostIP = "0.0.0.0"
		}
		binding := nat.PortBinding{
			HostIP: hostIP,
		}
		if portCfg.HostPort > 0 {
			binding.HostPort = strconv.Itoa(portCfg.HostPort)
		}
		portBindings[port] = []nat.PortBinding{binding}
	}

	// Build container config
	config := &container.Config{
		Image:        params.Manifest.Image,
		Env:          env,
		Labels:       labels,
		ExposedPorts: exposedPorts,
	}

	// Set command and args if provided
	if len(params.Manifest.Command) > 0 {
		config.Entrypoint = params.Manifest.Command
	}
	if len(params.Manifest.Args) > 0 {
		config.Cmd = params.Manifest.Args
	}

	// Set health check if provided
	if params.Manifest.HealthCheck != nil {
		config.Healthcheck = &container.HealthConfig{
			Test:        params.Manifest.HealthCheck.Test,
			Interval:    params.Manifest.HealthCheck.Interval.Duration(),
			Timeout:     params.Manifest.HealthCheck.Timeout.Duration(),
			Retries:     params.Manifest.HealthCheck.Retries,
			StartPeriod: params.Manifest.HealthCheck.StartPeriod.Duration(),
		}
	}

	// Build host config with resource limits and hardening
	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		Resources: container.Resources{
			NanoCPUs:   int64(params.Profile.CPUCores * 1e9),
			Memory:     params.Profile.MemoryMB * 1024 * 1024,
			MemorySwap: params.Profile.MemoryMB * 1024 * 1024, // equal to Memory = no swap
			PidsLimit:  params.PidsLimit,
		},
		RestartPolicy: container.RestartPolicy{
			Name: container.RestartPolicyDisabled,
		},
		CapDrop:        []string{"ALL"},
		SecurityOpt:    []string{"no-new-privileges:true"},
		ReadonlyRootfs: params.ReadonlyRootfs,
	}
	if params.ReadonlyRootfs {
		tmpfsSize := fmt.Sprintf("size=%dM", params.TmpfsSizeMB)
		hostConfig.Tmpfs = map[string]string{
			"/tmp": tmpfsSize,
			"/run": tmpfsSize,
		}
	}

	// Generate container name from lease UUID and instance index
	containerName := fmt.Sprintf("fred-%s-%d", params.LeaseUUID, params.InstanceIndex)

	networkConfig := params.NetworkConfig
	if networkConfig == nil {
		networkConfig = &networktypes.NetworkingConfig{}
	}

	resp, err := d.client.ContainerCreate(ctx, config, hostConfig, networkConfig, nil, containerName)
	if err != nil {
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	return resp.ID, nil
}

// StartContainer starts a container.
func (d *DockerClient) StartContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := d.client.ContainerStart(ctx, containerID, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	return nil
}

// RemoveContainer removes a container. It is idempotent — removing an
// already-removed container returns nil.
func (d *DockerClient) RemoveContainer(ctx context.Context, containerID string) error {
	err := d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{
		Force: true,
	})
	if err != nil {
		if client.IsErrNotFound(err) {
			return nil // Already removed
		}
		return fmt.Errorf("failed to remove container: %w", err)
	}

	return nil
}

// labelMeta holds parsed metadata from container labels.
type labelMeta struct {
	InstanceIndex int
	FailCount     int
	CreatedAt     time.Time
}

// parseLabelMeta extracts metadata from container labels. Returns an error if
// a label is present but malformed, which prevents silent defaulting to zero
// values that could cause resource allocation ID collisions.
func parseLabelMeta(labels map[string]string) (labelMeta, error) {
	var m labelMeta
	if idxStr, ok := labels[LabelInstanceIndex]; ok {
		idx, parseErr := strconv.Atoi(idxStr)
		if parseErr != nil {
			return m, fmt.Errorf("invalid %s label %q: %w", LabelInstanceIndex, idxStr, parseErr)
		}
		m.InstanceIndex = idx
	}
	if fcStr, ok := labels[LabelFailCount]; ok {
		fc, parseErr := strconv.Atoi(fcStr)
		if parseErr != nil {
			return m, fmt.Errorf("invalid %s label %q: %w", LabelFailCount, fcStr, parseErr)
		}
		m.FailCount = fc
	}
	if createdStr, ok := labels[LabelCreatedAt]; ok {
		t, parseErr := time.Parse(time.RFC3339, createdStr)
		if parseErr != nil {
			return m, fmt.Errorf("invalid %s label %q: %w", LabelCreatedAt, createdStr, parseErr)
		}
		m.CreatedAt = t
	}
	return m, nil
}

// InspectContainer returns detailed information about a container.
func (d *DockerClient) InspectContainer(ctx context.Context, containerID string) (*ContainerInfo, error) {
	resp, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect container: %w", err)
	}

	meta, err := parseLabelMeta(resp.Config.Labels)
	if err != nil {
		return nil, fmt.Errorf("container %s: %w", resp.ID, err)
	}

	info := &ContainerInfo{
		ContainerID:   resp.ID,
		LeaseUUID:     resp.Config.Labels[LabelLeaseUUID],
		Tenant:        resp.Config.Labels[LabelTenant],
		ProviderUUID:  resp.Config.Labels[LabelProviderUUID],
		SKU:           resp.Config.Labels[LabelSKU],
		Image:         resp.Config.Image,
		Status:        resp.State.Status,
		InstanceIndex: meta.InstanceIndex,
		FailCount:     meta.FailCount,
		CreatedAt:     meta.CreatedAt,
		Ports:         make(map[string]PortBinding),
	}

	// Extract port bindings
	for port, bindings := range resp.NetworkSettings.Ports {
		if len(bindings) > 0 {
			info.Ports[string(port)] = PortBinding{
				HostIP:   bindings[0].HostIP,
				HostPort: bindings[0].HostPort,
			}
		}
	}

	return info, nil
}

// ListManagedContainers returns all containers managed by Fred.
func (d *DockerClient) ListManagedContainers(ctx context.Context) ([]ContainerInfo, error) {
	containers, err := d.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
		),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var result []ContainerInfo
	for _, c := range containers {
		meta, err := parseLabelMeta(c.Labels)
		if err != nil {
			// Skip containers with malformed Fred labels — these can't be
			// reliably tracked for resource accounting.
			continue
		}

		info := ContainerInfo{
			ContainerID:   c.ID,
			LeaseUUID:     c.Labels[LabelLeaseUUID],
			Tenant:        c.Labels[LabelTenant],
			ProviderUUID:  c.Labels[LabelProviderUUID],
			SKU:           c.Labels[LabelSKU],
			Image:         c.Image,
			Status:        c.State,
			InstanceIndex: meta.InstanceIndex,
			FailCount:     meta.FailCount,
			CreatedAt:     meta.CreatedAt,
			Ports:         make(map[string]PortBinding),
		}

		// Extract port bindings from container ports
		for _, p := range c.Ports {
			portSpec := fmt.Sprintf("%d/%s", p.PrivatePort, p.Type)
			info.Ports[portSpec] = PortBinding{
				HostIP:   p.IP,
				HostPort: strconv.Itoa(int(p.PublicPort)),
			}
		}

		result = append(result, info)
	}

	return result, nil
}

// TenantNetworkName returns a deterministic network name for a tenant address.
func TenantNetworkName(tenant string) string {
	h := sha256.Sum256([]byte(tenant))
	return "fred-tenant-" + hex.EncodeToString(h[:8])
}

// EnsureTenantNetwork creates or returns the existing network for a tenant.
// The network is an internal bridge with no external connectivity.
func (d *DockerClient) EnsureTenantNetwork(ctx context.Context, tenant string) (string, error) {
	name := TenantNetworkName(tenant)

	// Check if network already exists
	networks, err := d.client.NetworkList(ctx, networktypes.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("name", name),
			filters.Arg("label", LabelManaged+"=true"),
		),
	})
	if err != nil {
		return "", fmt.Errorf("failed to list networks: %w", err)
	}
	if len(networks) > 0 {
		return networks[0].ID, nil
	}

	// Create the network
	resp, err := d.client.NetworkCreate(ctx, name, networktypes.CreateOptions{
		Driver:   "bridge",
		Internal: true,
		Labels: map[string]string{
			LabelManaged: "true",
			LabelTenant:  tenant,
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create tenant network: %w", err)
	}
	return resp.ID, nil
}

// RemoveTenantNetworkIfEmpty removes the tenant's network if no containers are connected.
func (d *DockerClient) RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error {
	name := TenantNetworkName(tenant)

	// Inspect to check connected containers
	resp, err := d.client.NetworkInspect(ctx, name, networktypes.InspectOptions{})
	if err != nil {
		if client.IsErrNotFound(err) {
			return nil // Already removed
		}
		return fmt.Errorf("failed to inspect network: %w", err)
	}

	if len(resp.Containers) > 0 {
		return nil // Still in use
	}

	if err := d.client.NetworkRemove(ctx, resp.ID); err != nil {
		if client.IsErrNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to remove network: %w", err)
	}
	return nil
}

// ListManagedNetworks returns all networks created by Fred, with full details.
func (d *DockerClient) ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error) {
	summaries, err := d.client.NetworkList(ctx, networktypes.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
		),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list managed networks: %w", err)
	}

	var result []networktypes.Inspect
	for _, s := range summaries {
		inspected, err := d.client.NetworkInspect(ctx, s.ID, networktypes.InspectOptions{})
		if err != nil {
			continue // Network may have been removed between list and inspect
		}
		result = append(result, inspected)
	}
	return result, nil
}

// buildNetworkConfig returns a NetworkingConfig that attaches to the given network,
// or an empty config if networkID is empty.
func buildNetworkConfig(networkID string) *networktypes.NetworkingConfig {
	if networkID == "" {
		return &networktypes.NetworkingConfig{}
	}
	return &networktypes.NetworkingConfig{
		EndpointsConfig: map[string]*networktypes.EndpointSettings{
			networkID: {},
		},
	}
}

// containerStatusToProvisionStatus converts Docker status to provision status.
func containerStatusToProvisionStatus(status string) string {
	status = strings.ToLower(status)
	switch status {
	case "created", "restarting":
		return "provisioning"
	case "running", "paused":
		return "ready"
	case "removing", "exited", "dead":
		return "failed"
	default:
		return "unknown"
	}
}
