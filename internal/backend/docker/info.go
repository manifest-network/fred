package docker

import (
	"context"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// GetReleases returns the release history for a lease.
func (b *Backend) GetReleases(_ context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	b.provisionsMu.RLock()
	_, exists := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()

	if !exists {
		return nil, backend.ErrNotProvisioned
	}

	if b.releaseStore == nil {
		return nil, nil
	}

	releases, err := b.releaseStore.List(leaseUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to list releases: %w", err)
	}

	result := make([]backend.ReleaseInfo, len(releases))
	for i, r := range releases {
		result[i] = backend.ReleaseInfo{
			Version:   r.Version,
			Image:     r.Image,
			Status:    r.Status,
			CreatedAt: r.CreatedAt,
			Error:     r.Error,
			Manifest:  r.Manifest,
		}
	}
	return result, nil
}

// GetInfo returns lease information including connection details.
// For multi-unit leases, returns an "instances" array with each container's info.
// For stack leases, returns a "services" map grouping instances by service name.
func (b *Backend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.RUnlock()
		return nil, backend.ErrNotProvisioned
	}
	status := prov.Status
	isStack := prov.IsStack()
	containerIDs := append([]string(nil), prov.ContainerIDs...)
	var serviceContainers map[string][]string
	if isStack {
		serviceContainers = make(map[string][]string, len(prov.ServiceContainers))
		for k, v := range prov.ServiceContainers {
			serviceContainers[k] = append([]string(nil), v...)
		}
	}
	b.provisionsMu.RUnlock()

	if status != backend.ProvisionStatusReady {
		return nil, backend.ErrNotProvisioned
	}

	// Stack response: group instances by service name.
	if isStack {
		services := make(map[string]any)
		for svcName, svcContainerIDs := range serviceContainers {
			var instances []map[string]any
			for _, containerID := range svcContainerIDs {
				info, err := b.docker.InspectContainer(ctx, containerID)
				if err != nil {
					return nil, fmt.Errorf("failed to inspect container: %w", err)
				}
				ports := make(map[string]map[string]string)
				for portSpec, binding := range info.Ports {
					ports[portSpec] = map[string]string{
						"host_ip":   binding.HostIP,
						"host_port": binding.HostPort,
					}
				}
				instance := map[string]any{
					"instance_index": info.InstanceIndex,
					"container_id":   shortID(info.ContainerID),
					"image":          info.Image,
					"status":         info.Status,
					"ports":          ports,
				}
				if info.FQDN != "" {
					instance["fqdn"] = info.FQDN
				}
				instances = append(instances, instance)
			}
			services[svcName] = map[string]any{
				"instances": instances,
			}
		}
		leaseInfo := backend.LeaseInfo{
			"host":     b.cfg.HostAddress,
			"services": services,
		}
		return &leaseInfo, nil
	}

	// Legacy response: flat instances array.
	var instances []map[string]any
	for _, containerID := range containerIDs {
		info, err := b.docker.InspectContainer(ctx, containerID)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect container: %w", err)
		}

		ports := make(map[string]map[string]string)
		for portSpec, binding := range info.Ports {
			ports[portSpec] = map[string]string{
				"host_ip":   binding.HostIP,
				"host_port": binding.HostPort,
			}
		}

		instance := map[string]any{
			"instance_index": info.InstanceIndex,
			"container_id":   shortID(info.ContainerID),
			"image":          info.Image,
			"status":         info.Status,
			"ports":          ports,
		}
		if info.FQDN != "" {
			instance["fqdn"] = info.FQDN
		}
		instances = append(instances, instance)
	}

	leaseInfo := backend.LeaseInfo{
		"host":      b.cfg.HostAddress,
		"instances": instances,
	}

	return &leaseInfo, nil
}

// RefreshState synchronizes in-memory provision state with Docker.
func (b *Backend) RefreshState(ctx context.Context) error {
	return b.recoverState(ctx)
}

// GetProvision returns a single provision by lease UUID.
// Falls back to the diagnostics store when the provision is not in memory
// (e.g., after deprovision). Returns ErrNotProvisioned only if both miss.
func (b *Backend) GetProvision(_ context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	b.provisionsMu.RLock()
	prov, ok := b.provisions[leaseUUID]
	var info *backend.ProvisionInfo
	if ok {
		info = &backend.ProvisionInfo{
			LeaseUUID:    prov.LeaseUUID,
			ProviderUUID: prov.ProviderUUID,
			Status:       prov.Status,
			CreatedAt:    prov.CreatedAt,
			BackendName:  b.cfg.Name,
			FailCount:    prov.FailCount,
			LastError:    prov.LastError,
		}
	}
	b.provisionsMu.RUnlock()

	if info != nil {
		return info, nil
	}

	// Fall back to persisted diagnostics.
	if b.diagnosticsStore != nil {
		entry, err := b.diagnosticsStore.Get(leaseUUID)
		if err != nil {
			b.logger.Warn("diagnostics store lookup failed", "lease_uuid", leaseUUID, "error", err)
		}
		if entry != nil {
			return &backend.ProvisionInfo{
				LeaseUUID:    entry.LeaseUUID,
				ProviderUUID: entry.ProviderUUID,
				Status:       backend.ProvisionStatusFailed,
				CreatedAt:    entry.CreatedAt,
				BackendName:  b.cfg.Name,
				FailCount:    entry.FailCount,
				LastError:    entry.Error,
			}, nil
		}
	}

	return nil, backend.ErrNotProvisioned
}

// ListProvisions returns all currently provisioned resources.
func (b *Backend) ListProvisions(_ context.Context) ([]backend.ProvisionInfo, error) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	result := make([]backend.ProvisionInfo, 0, len(b.provisions))
	for _, prov := range b.provisions {
		result = append(result, backend.ProvisionInfo{
			LeaseUUID:    prov.LeaseUUID,
			ProviderUUID: prov.ProviderUUID,
			Status:       prov.Status,
			CreatedAt:    prov.CreatedAt,
			BackendName:  b.cfg.Name,
			FailCount:    prov.FailCount,
			LastError:    prov.LastError,
		})
	}

	return result, nil
}

// GetLogs returns the last N lines of stdout/stderr for each container in
// a lease, keyed by instance index (e.g., "0", "1").
// Falls back to the diagnostics store when the provision is not in memory
// (e.g., after deprovision). Returns ErrNotProvisioned only if both miss.
// On partial failure (some containers succeed, some fail), the successful logs
// are returned along with error placeholders, and the errors are logged.
func (b *Backend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	if exists {
		isStack := prov.IsStack()
		containerIDs := append([]string(nil), prov.ContainerIDs...)
		var serviceContainers map[string][]string
		if isStack {
			serviceContainers = make(map[string][]string, len(prov.ServiceContainers))
			for k, v := range prov.ServiceContainers {
				serviceContainers[k] = append([]string(nil), v...)
			}
		}
		b.provisionsMu.RUnlock()

		// Stack logs: key by "serviceName/instanceIndex" (e.g., "web/0", "db/0").
		if isStack {
			result := make(map[string]string, len(containerIDs))
			for svcName, svcContainerIDs := range serviceContainers {
				for i, containerID := range svcContainerIDs {
					key := fmt.Sprintf("%s/%d", svcName, i)
					logs, err := b.docker.ContainerLogs(ctx, containerID, tail)
					if err != nil {
						b.logger.Warn("failed to retrieve container logs",
							"lease_uuid", leaseUUID,
							"service", svcName,
							"instance", i,
							"container_id", shortID(containerID),
							"error", err,
						)
						result[key] = fmt.Sprintf("<error: %s>", err)
						continue
					}
					result[key] = logs
				}
			}
			return result, nil
		}

		// Legacy logs: key by instance index ("0", "1", ...).
		result := make(map[string]string, len(containerIDs))
		for i, containerID := range containerIDs {
			logs, err := b.docker.ContainerLogs(ctx, containerID, tail)
			if err != nil {
				b.logger.Warn("failed to retrieve container logs",
					"lease_uuid", leaseUUID,
					"instance", i,
					"container_id", shortID(containerID),
					"error", err,
				)
				result[fmt.Sprintf("%d", i)] = fmt.Sprintf("<error: %s>", err)
				continue
			}
			result[fmt.Sprintf("%d", i)] = logs
		}
		return result, nil
	}
	b.provisionsMu.RUnlock()

	// Fall back to persisted diagnostics.
	if b.diagnosticsStore != nil {
		entry, err := b.diagnosticsStore.Get(leaseUUID)
		if err != nil {
			b.logger.Warn("diagnostics store lookup failed", "lease_uuid", leaseUUID, "error", err)
		}
		if entry != nil && len(entry.Logs) > 0 {
			return entry.Logs, nil
		}
	}

	return nil, backend.ErrNotProvisioned
}

// Stats returns current resource usage statistics.
func (b *Backend) Stats() shared.ResourceStats {
	return b.pool.Stats()
}
