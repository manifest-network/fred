package docker

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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
// Always populates `Services` keyed by service name (the primary
// post-Tasks-3-9 source of truth). `Instances` is a flattened
// convenience view computed by concatenating service instances in
// deterministic service-name order so older tooling that consumes the
// flat array keeps working.
func (b *Backend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.RUnlock()
		return nil, backend.ErrNotProvisioned
	}
	status := prov.Status
	serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
	for k, v := range prov.ServiceContainers {
		serviceContainers[k] = append([]string(nil), v...)
	}
	b.provisionsMu.RUnlock()

	if status != backend.ProvisionStatusReady {
		return nil, backend.ErrNotProvisioned
	}

	// Build `Services` first; inspect every service-container exactly once.
	services := make(map[string]backend.LeaseService, len(serviceContainers))
	for svcName, svcContainerIDs := range serviceContainers {
		svcInstances := make([]backend.LeaseInstance, 0, len(svcContainerIDs))
		for _, containerID := range svcContainerIDs {
			info, err := b.docker.InspectContainer(ctx, containerID)
			if err != nil {
				return nil, fmt.Errorf("failed to inspect container: %w", err)
			}
			svcInstances = append(svcInstances, inspectToInstance(info))
		}
		services[svcName] = backend.LeaseService{Instances: svcInstances}
	}

	// Derive `Instances` by flattening in deterministic service-name
	// order — preserves backwards-compatibility for callers that still
	// consume the flat slice (e.g., older tooling that pre-dates the
	// Services map). Total length equals sum of per-service quantities.
	var instances []backend.LeaseInstance
	for _, name := range slices.Sorted(maps.Keys(services)) {
		instances = append(instances, services[name].Instances...)
	}

	return &backend.LeaseInfo{
		Host:      b.cfg.HostAddress,
		Services:  services,
		Instances: instances,
	}, nil
}

// inspectToInstance converts a ContainerInfo from Docker inspect into a backend LeaseInstance.
func inspectToInstance(info *ContainerInfo) backend.LeaseInstance {
	inst := backend.LeaseInstance{
		InstanceIndex: info.InstanceIndex,
		ContainerID:   leasesm.ShortID(info.ContainerID),
		Image:         info.Image,
		Status:        info.Status,
		FQDN:          info.FQDN,
	}
	if len(info.Ports) > 0 {
		inst.Ports = make(map[string]backend.PortBinding, len(info.Ports))
		for portSpec, binding := range info.Ports {
			inst.Ports[portSpec] = backend.PortBinding{
				HostIP:   binding.HostIP,
				HostPort: binding.HostPort,
			}
		}
	}
	return inst
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
		pi := provisionToInfo(prov, b.cfg.Name)
		info = &pi
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
		result = append(result, provisionToInfo(prov, b.cfg.Name))
	}

	return result, nil
}

// LookupProvisions returns provision info for the requested lease UUIDs.
// Missing leases are absent from the returned slice (not an error). O(k) lookups
// against the in-memory provisions map, where k = len(uuids).
func (b *Backend) LookupProvisions(_ context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	result := make([]backend.ProvisionInfo, 0, len(uuids))
	for _, uuid := range uuids {
		if prov, ok := b.provisions[uuid]; ok {
			result = append(result, provisionToInfo(prov, b.cfg.Name))
		}
	}

	return result, nil
}

// GetLogs returns the last N lines of stdout/stderr for each container in
// a lease, keyed by "serviceName/instanceIndex" (e.g., "web/0", "db/0").
// Falls back to the diagnostics store when the provision is not in memory
// (e.g., after deprovision). Returns ErrNotProvisioned only if both miss.
// On partial failure (some containers succeed, some fail), the successful logs
// are returned along with error placeholders, and the errors are logged.
//
// Post-Tasks-3-9 the live path always populates `prov.ServiceContainers`, so
// the legacy "key by instance index only" branch is gone — every lease is
// stack-shaped from the in-memory state's perspective.
func (b *Backend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	b.provisionsMu.RLock()
	prov, exists := b.provisions[leaseUUID]
	if exists {
		containerIDs := append([]string(nil), prov.ContainerIDs...)
		serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
		for k, v := range prov.ServiceContainers {
			serviceContainers[k] = append([]string(nil), v...)
		}
		b.provisionsMu.RUnlock()

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
						"container_id", leasesm.ShortID(containerID),
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

// provisionToInfo converts a provision to a backend.ProvisionInfo.
// Items are defensively copied (each provision now carries them post-
// Task-3 normalization) and ServiceImages are extracted from
// prov.StackManifest. SKU is propagated unchanged. The legacy single-
// service `Image` field was deleted in Task 15 — callers that need a
// representative image consult ServiceImages or iterate Items.
func provisionToInfo(prov *provision, backendName string) backend.ProvisionInfo {
	info := backend.ProvisionInfo{
		LeaseUUID:    prov.LeaseUUID,
		ProviderUUID: prov.ProviderUUID,
		Status:       prov.Status,
		CreatedAt:    prov.CreatedAt,
		BackendName:  backendName,
		FailCount:    prov.FailCount,
		LastError:    prov.LastError,
		Quantity:     prov.Quantity,
	}
	// Post-Task-15 every provision carries `prov.Items` (populated at
	// Provision time by NormalizeProvisionRequest and rehydrated from
	// container labels by recover.go). The Items + ServiceImages
	// representation is the canonical workload-metadata shape for the
	// ProvisionInfo response. The legacy single-image fields
	// (prov.Image/prov.SKU) are gone.
	info.Items = append([]backend.LeaseItem(nil), prov.Items...)
	info.ServiceImages = serviceImages(prov.StackManifest)
	return info
}

// serviceImages extracts a service name → image map from a StackManifest.
// Returns nil if the manifest is nil (e.g., after cold restart with no release store).
func serviceImages(sm *manifest.StackManifest) map[string]string {
	if sm == nil {
		return nil
	}
	images := make(map[string]string, len(sm.Services))
	for name, m := range sm.Services {
		images[name] = m.Image
	}
	return images
}

// Stats returns current resource usage statistics.
func (b *Backend) Stats() shared.ResourceStats {
	return b.pool.Stats()
}
