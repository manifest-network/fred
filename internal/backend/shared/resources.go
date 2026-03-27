package shared

import (
	"fmt"
	"sync"
)

// SKUResolver resolves a SKU identifier to its resource profile.
// This abstracts the SKU mapping logic so it can be shared between components.
type SKUResolver func(sku string) (SKUProfile, error)

// ResourceAllocation tracks resources allocated to a single lease.
type ResourceAllocation struct {
	LeaseUUID     string
	Tenant        string
	SKU           string
	CPUCores      float64
	MemoryMB      int64
	DiskMB        int64
	BandwidthMbps int64
}

// ResourcePool manages the backend's resource capacity.
// It provides atomic allocation and release of resources based on SKU profiles.
type ResourcePool struct {
	mu sync.Mutex

	// Total capacity
	totalCPU       float64
	totalMemory    int64
	totalDisk      int64
	totalBandwidth int64

	// Current allocations
	allocatedCPU       float64
	allocatedMemory    int64
	allocatedDisk      int64
	allocatedBandwidth int64

	// Per-lease tracking
	allocations map[string]ResourceAllocation

	// Per-tenant aggregate tracking
	tenantUsage map[string]ResourceAllocation
	tenantQuota *TenantQuotaConfig

	// skuResolver resolves SKU identifiers to profiles
	skuResolver SKUResolver
}

// NewResourcePool creates a new resource pool with the given capacity.
// Panics if resolver is nil (programming error).
func NewResourcePool(totalCPU float64, totalMemoryMB, totalDiskMB, totalBandwidthMbps int64, resolver SKUResolver, tenantQuota *TenantQuotaConfig) *ResourcePool {
	if resolver == nil {
		panic("shared.NewResourcePool: resolver must not be nil")
	}
	return &ResourcePool{
		totalCPU:       totalCPU,
		totalMemory:    totalMemoryMB,
		totalDisk:      totalDiskMB,
		totalBandwidth: totalBandwidthMbps,
		allocations:    make(map[string]ResourceAllocation),
		tenantUsage:    make(map[string]ResourceAllocation),
		tenantQuota:    tenantQuota,
		skuResolver:    resolver,
	}
}

// TryAllocate attempts to reserve resources for a lease based on SKU profile.
// The tenant parameter is used for per-tenant quota enforcement.
// Returns nil if allocation succeeds, error if SKU is unknown or insufficient resources.
func (p *ResourcePool) TryAllocate(leaseUUID, sku, tenant string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if already allocated
	if _, exists := p.allocations[leaseUUID]; exists {
		return fmt.Errorf("lease %s already has allocated resources", leaseUUID)
	}

	// Resolve SKU to profile
	profile, err := p.skuResolver(sku)
	if err != nil {
		return err
	}

	// Check global capacity
	if p.allocatedCPU+profile.CPUCores > p.totalCPU {
		return fmt.Errorf("insufficient CPU: need %.2f cores, have %.2f available",
			profile.CPUCores, p.totalCPU-p.allocatedCPU)
	}
	if p.allocatedMemory+profile.MemoryMB > p.totalMemory {
		return fmt.Errorf("insufficient memory: need %d MB, have %d MB available",
			profile.MemoryMB, p.totalMemory-p.allocatedMemory)
	}
	if p.allocatedDisk+profile.DiskMB > p.totalDisk {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			profile.DiskMB, p.totalDisk-p.allocatedDisk)
	}
	if profile.BandwidthMbps > 0 && p.allocatedBandwidth+profile.BandwidthMbps > p.totalBandwidth {
		return fmt.Errorf("insufficient bandwidth: need %d Mbps, have %d Mbps available",
			profile.BandwidthMbps, p.totalBandwidth-p.allocatedBandwidth)
	}

	// Check per-tenant quota if configured
	if p.tenantQuota != nil && tenant != "" {
		usage := p.tenantUsage[tenant]
		if usage.CPUCores+profile.CPUCores > p.tenantQuota.MaxCPUCores {
			return fmt.Errorf("tenant %s CPU quota exceeded: need %.2f cores, have %.2f available (quota: %.2f)",
				tenant, profile.CPUCores, p.tenantQuota.MaxCPUCores-usage.CPUCores, p.tenantQuota.MaxCPUCores)
		}
		if usage.MemoryMB+profile.MemoryMB > p.tenantQuota.MaxMemoryMB {
			return fmt.Errorf("tenant %s memory quota exceeded: need %d MB, have %d MB available (quota: %d)",
				tenant, profile.MemoryMB, p.tenantQuota.MaxMemoryMB-usage.MemoryMB, p.tenantQuota.MaxMemoryMB)
		}
		if usage.DiskMB+profile.DiskMB > p.tenantQuota.MaxDiskMB {
			return fmt.Errorf("tenant %s disk quota exceeded: need %d MB, have %d MB available (quota: %d)",
				tenant, profile.DiskMB, p.tenantQuota.MaxDiskMB-usage.DiskMB, p.tenantQuota.MaxDiskMB)
		}
		if p.tenantQuota.MaxBandwidthMbps > 0 && usage.BandwidthMbps+profile.BandwidthMbps > p.tenantQuota.MaxBandwidthMbps {
			return fmt.Errorf("tenant %s bandwidth quota exceeded: need %d Mbps, have %d Mbps available (quota: %d)",
				tenant, profile.BandwidthMbps, p.tenantQuota.MaxBandwidthMbps-usage.BandwidthMbps, p.tenantQuota.MaxBandwidthMbps)
		}
	}

	// Reserve resources
	p.allocatedCPU += profile.CPUCores
	p.allocatedMemory += profile.MemoryMB
	p.allocatedDisk += profile.DiskMB
	p.allocatedBandwidth += profile.BandwidthMbps

	p.allocations[leaseUUID] = ResourceAllocation{
		LeaseUUID:     leaseUUID,
		Tenant:        tenant,
		SKU:           sku,
		CPUCores:      profile.CPUCores,
		MemoryMB:      profile.MemoryMB,
		DiskMB:        profile.DiskMB,
		BandwidthMbps: profile.BandwidthMbps,
	}

	// Update tenant aggregate
	if tenant != "" {
		usage := p.tenantUsage[tenant]
		usage.CPUCores += profile.CPUCores
		usage.MemoryMB += profile.MemoryMB
		usage.DiskMB += profile.DiskMB
		usage.BandwidthMbps += profile.BandwidthMbps
		p.tenantUsage[tenant] = usage
	}

	return nil
}

// Release returns resources for a lease back to the pool.
// It is safe to call Release for a lease that has no allocation.
func (p *ResourcePool) Release(leaseUUID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	alloc, exists := p.allocations[leaseUUID]
	if !exists {
		return
	}

	p.allocatedCPU -= alloc.CPUCores
	p.allocatedMemory -= alloc.MemoryMB
	p.allocatedDisk -= alloc.DiskMB
	p.allocatedBandwidth -= alloc.BandwidthMbps

	// Update tenant aggregate
	if alloc.Tenant != "" {
		usage := p.tenantUsage[alloc.Tenant]
		usage.CPUCores -= alloc.CPUCores
		usage.MemoryMB -= alloc.MemoryMB
		usage.DiskMB -= alloc.DiskMB
		usage.BandwidthMbps -= alloc.BandwidthMbps
		if usage.CPUCores <= 0 && usage.MemoryMB <= 0 && usage.DiskMB <= 0 && usage.BandwidthMbps <= 0 {
			delete(p.tenantUsage, alloc.Tenant)
		} else {
			p.tenantUsage[alloc.Tenant] = usage
		}
	}

	delete(p.allocations, leaseUUID)
}

// GetAllocation returns the allocation for a lease, or nil if not allocated.
func (p *ResourcePool) GetAllocation(leaseUUID string) *ResourceAllocation {
	p.mu.Lock()
	defer p.mu.Unlock()

	alloc, exists := p.allocations[leaseUUID]
	if !exists {
		return nil
	}
	return &alloc
}

// Stats returns current resource usage statistics.
func (p *ResourcePool) Stats() ResourceStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return ResourceStats{
		TotalCPU:               p.totalCPU,
		TotalMemoryMB:          p.totalMemory,
		TotalDiskMB:            p.totalDisk,
		TotalBandwidthMbps:     p.totalBandwidth,
		AllocatedCPU:           p.allocatedCPU,
		AllocatedMemoryMB:      p.allocatedMemory,
		AllocatedDiskMB:        p.allocatedDisk,
		AllocatedBandwidthMbps: p.allocatedBandwidth,
		AllocationCount:        len(p.allocations),
	}
}

// TenantStats returns resource usage statistics for a specific tenant.
func (p *ResourcePool) TenantStats(tenant string) ResourceStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	usage := p.tenantUsage[tenant]
	stats := ResourceStats{
		AllocatedCPU:           usage.CPUCores,
		AllocatedMemoryMB:      usage.MemoryMB,
		AllocatedDiskMB:        usage.DiskMB,
		AllocatedBandwidthMbps: usage.BandwidthMbps,
	}
	if p.tenantQuota != nil {
		stats.TotalCPU = p.tenantQuota.MaxCPUCores
		stats.TotalMemoryMB = p.tenantQuota.MaxMemoryMB
		stats.TotalDiskMB = p.tenantQuota.MaxDiskMB
		stats.TotalBandwidthMbps = p.tenantQuota.MaxBandwidthMbps
	}
	return stats
}

// ResourceStats contains resource usage statistics.
type ResourceStats struct {
	TotalCPU               float64
	TotalMemoryMB          int64
	TotalDiskMB            int64
	TotalBandwidthMbps     int64
	AllocatedCPU           float64
	AllocatedMemoryMB      int64
	AllocatedDiskMB        int64
	AllocatedBandwidthMbps int64
	AllocationCount        int
}

// AvailableCPU returns available CPU cores.
func (s ResourceStats) AvailableCPU() float64 {
	return s.TotalCPU - s.AllocatedCPU
}

// AvailableMemoryMB returns available memory in MB.
func (s ResourceStats) AvailableMemoryMB() int64 {
	return s.TotalMemoryMB - s.AllocatedMemoryMB
}

// AvailableDiskMB returns available disk in MB.
func (s ResourceStats) AvailableDiskMB() int64 {
	return s.TotalDiskMB - s.AllocatedDiskMB
}

// AvailableBandwidthMbps returns available bandwidth in Mbps.
func (s ResourceStats) AvailableBandwidthMbps() int64 {
	return s.TotalBandwidthMbps - s.AllocatedBandwidthMbps
}

// Reset clears all allocations and rebuilds from a list of allocations.
func (p *ResourcePool) Reset(allocations []ResourceAllocation) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear existing
	p.allocations = make(map[string]ResourceAllocation)
	p.tenantUsage = make(map[string]ResourceAllocation)
	p.allocatedCPU = 0
	p.allocatedMemory = 0
	p.allocatedDisk = 0
	p.allocatedBandwidth = 0

	// Rebuild from provided allocations
	for _, alloc := range allocations {
		p.allocations[alloc.LeaseUUID] = alloc
		p.allocatedCPU += alloc.CPUCores
		p.allocatedMemory += alloc.MemoryMB
		p.allocatedDisk += alloc.DiskMB
		p.allocatedBandwidth += alloc.BandwidthMbps

		// Rebuild tenant aggregates
		if alloc.Tenant != "" {
			usage := p.tenantUsage[alloc.Tenant]
			usage.CPUCores += alloc.CPUCores
			usage.MemoryMB += alloc.MemoryMB
			usage.DiskMB += alloc.DiskMB
			usage.BandwidthMbps += alloc.BandwidthMbps
			p.tenantUsage[alloc.Tenant] = usage
		}
	}
}

// ListAllocations returns a copy of all current allocations.
func (p *ResourcePool) ListAllocations() []ResourceAllocation {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]ResourceAllocation, 0, len(p.allocations))
	for _, alloc := range p.allocations {
		result = append(result, alloc)
	}
	return result
}
