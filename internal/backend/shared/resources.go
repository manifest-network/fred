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
	LeaseUUID string
	Tenant    string
	SKU       string
	CPUCores  float64
	MemoryMB  int64
	DiskMB    int64
}

// ResourcePool manages the backend's resource capacity.
// It provides atomic allocation and release of resources based on SKU profiles.
type ResourcePool struct {
	mu sync.Mutex

	// Total capacity
	totalCPU    float64
	totalMemory int64
	totalDisk   int64

	// Current allocations
	allocatedCPU    float64
	allocatedMemory int64
	allocatedDisk   int64

	// retainedDisk is the aggregate disk (MB) reserved by soft-deleted
	// (retained) volumes. It is a projection pushed by the owner via
	// SetRetainedDisk (derived from the retention store), subtracted from
	// available disk in TryAllocate so retained volumes keep counting against
	// the pool until they are actually reaped. Not touched by Reset (which owns
	// only live allocations) — the owner re-pushes it after recover.
	retainedDisk int64

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
func NewResourcePool(totalCPU float64, totalMemoryMB, totalDiskMB int64, resolver SKUResolver, tenantQuota *TenantQuotaConfig) *ResourcePool {
	if resolver == nil {
		panic("shared.NewResourcePool: resolver must not be nil")
	}
	return &ResourcePool{
		totalCPU:    totalCPU,
		totalMemory: totalMemoryMB,
		totalDisk:   totalDiskMB,
		allocations: make(map[string]ResourceAllocation),
		tenantUsage: make(map[string]ResourceAllocation),
		tenantQuota: tenantQuota,
		skuResolver: resolver,
	}
}

// TryAllocate attempts to reserve resources for a new provision (gates all of
// CPU, memory, disk, and tenant quota).
func (p *ResourcePool) TryAllocate(leaseUUID, sku, tenant string) error {
	return p.tryAllocate(leaseUUID, sku, tenant, true, 0)
}

// TryAllocateAdopt reserves resources for a lease being RESTORED by adopting an
// existing retained volume (rename, not fresh disk). It gates CPU, memory, and
// tenant quota normally. For the global disk gate it does NOT re-check the whole
// footprint — the adopted bytes are already committed on disk and counted in the
// retained projection, so re-gating would double-count and spuriously deny a
// restore that physically fits. Instead it gates only the PROMOTE DELTA: the
// growth of the new SKU's DiskMB above adoptOldDiskMB (the disk the retained
// record already contributed to the retained projection). Same-tier and demote
// (delta <= 0) skip the disk gate exactly as before; a promote into a larger
// tier is capacity-checked so restore cannot over-commit the pool (ENG-545). The
// full new DiskMB is still added to allocatedDisk for correct live accounting;
// the retained projection drops adoptOldDiskMB when the record flips to restoring.
func (p *ResourcePool) TryAllocateAdopt(leaseUUID, sku, tenant string, adoptOldDiskMB int64) error {
	return p.tryAllocate(leaseUUID, sku, tenant, false, adoptOldDiskMB)
}

// tryAllocate is the shared implementation for TryAllocate and TryAllocateAdopt.
// When gateDisk is true the full DiskMB is gated against global capacity (fresh
// provision). When false (adopt/restore) only the promote delta above
// adoptOldDiskMB is gated; adoptOldDiskMB is ignored when gateDisk is true.
func (p *ResourcePool) tryAllocate(leaseUUID, sku, tenant string, gateDisk bool, adoptOldDiskMB int64) error {
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
	// Global disk gate. A fresh provision gates its full DiskMB. An adopt
	// (restore) gates only the PROMOTE DELTA above the volume's already-committed
	// retained footprint: those bytes are already counted in retainedDisk, so only
	// growth (DiskMB - adoptOldDiskMB) is new disk pressure. Same-tier and demote
	// (delta <= 0) add no pressure and skip the gate exactly as the prior
	// unconditional adopt skip did; only a promote into a larger tier is
	// capacity-checked, so a restore can no longer over-commit the pool (ENG-545).
	diskPressure := profile.DiskMB
	if !gateDisk {
		if diskPressure = profile.DiskMB - adoptOldDiskMB; diskPressure < 0 {
			diskPressure = 0
		}
	}
	if (gateDisk || diskPressure > 0) && p.allocatedDisk+p.retainedDisk+diskPressure > p.totalDisk {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			diskPressure, max(int64(0), p.totalDisk-p.allocatedDisk-p.retainedDisk))
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
	}

	// Reserve resources
	p.allocatedCPU += profile.CPUCores
	p.allocatedMemory += profile.MemoryMB
	p.allocatedDisk += profile.DiskMB

	p.allocations[leaseUUID] = ResourceAllocation{
		LeaseUUID: leaseUUID,
		Tenant:    tenant,
		SKU:       sku,
		CPUCores:  profile.CPUCores,
		MemoryMB:  profile.MemoryMB,
		DiskMB:    profile.DiskMB,
	}

	// Update tenant aggregate
	if tenant != "" {
		usage := p.tenantUsage[tenant]
		usage.CPUCores += profile.CPUCores
		usage.MemoryMB += profile.MemoryMB
		usage.DiskMB += profile.DiskMB
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

	// Update tenant aggregate
	if alloc.Tenant != "" {
		usage := p.tenantUsage[alloc.Tenant]
		usage.CPUCores -= alloc.CPUCores
		usage.MemoryMB -= alloc.MemoryMB
		usage.DiskMB -= alloc.DiskMB
		if usage.CPUCores <= 0 && usage.MemoryMB <= 0 && usage.DiskMB <= 0 {
			delete(p.tenantUsage, alloc.Tenant)
		} else {
			p.tenantUsage[alloc.Tenant] = usage
		}
	}

	delete(p.allocations, leaseUUID)
}

// SetRetainedDisk records the aggregate disk (MB) reserved by retained
// (soft-deleted) volumes. The owner derives this from the retention store and
// pushes it here; TryAllocate subtracts it from available disk so retained
// volumes keep counting against the pool until reaped. Idempotent.
func (p *ResourcePool) SetRetainedDisk(mb int64) {
	if mb < 0 {
		mb = 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retainedDisk = mb
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
		TotalCPU:          p.totalCPU,
		TotalMemoryMB:     p.totalMemory,
		TotalDiskMB:       p.totalDisk,
		AllocatedCPU:      p.allocatedCPU,
		AllocatedMemoryMB: p.allocatedMemory,
		AllocatedDiskMB:   p.allocatedDisk,
		RetainedDiskMB:    p.retainedDisk,
		AllocationCount:   len(p.allocations),
	}
}

// TenantStats returns resource usage statistics for a specific tenant.
// RetainedDiskMB is intentionally left 0: retained disk is a provider-level
// term (not attributed per tenant), so AvailableDiskMB() on a tenant snapshot
// intentionally excludes it.
func (p *ResourcePool) TenantStats(tenant string) ResourceStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	usage := p.tenantUsage[tenant]
	stats := ResourceStats{
		AllocatedCPU:      usage.CPUCores,
		AllocatedMemoryMB: usage.MemoryMB,
		AllocatedDiskMB:   usage.DiskMB,
	}
	if p.tenantQuota != nil {
		stats.TotalCPU = p.tenantQuota.MaxCPUCores
		stats.TotalMemoryMB = p.tenantQuota.MaxMemoryMB
		stats.TotalDiskMB = p.tenantQuota.MaxDiskMB
	}
	return stats
}

// ResourceStats contains resource usage statistics.
type ResourceStats struct {
	TotalCPU          float64
	TotalMemoryMB     int64
	TotalDiskMB       int64
	AllocatedCPU      float64
	AllocatedMemoryMB int64
	AllocatedDiskMB   int64
	RetainedDiskMB    int64
	AllocationCount   int
}

// AvailableCPU returns available CPU cores.
func (s ResourceStats) AvailableCPU() float64 {
	return s.TotalCPU - s.AllocatedCPU
}

// AvailableMemoryMB returns available memory in MB.
func (s ResourceStats) AvailableMemoryMB() int64 {
	return s.TotalMemoryMB - s.AllocatedMemoryMB
}

// AvailableDiskMB returns disk available for new allocations: total minus live
// allocations minus retained (soft-deleted) reservations, clamped to >= 0 (a
// total_disk_mb shrink or stale retained projection must not surface a negative
// "available" via the /stats endpoints).
func (s ResourceStats) AvailableDiskMB() int64 {
	return max(int64(0), s.TotalDiskMB-s.AllocatedDiskMB-s.RetainedDiskMB)
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

	// Rebuild from provided allocations
	for _, alloc := range allocations {
		p.allocations[alloc.LeaseUUID] = alloc
		p.allocatedCPU += alloc.CPUCores
		p.allocatedMemory += alloc.MemoryMB
		p.allocatedDisk += alloc.DiskMB

		// Rebuild tenant aggregates
		if alloc.Tenant != "" {
			usage := p.tenantUsage[alloc.Tenant]
			usage.CPUCores += alloc.CPUCores
			usage.MemoryMB += alloc.MemoryMB
			usage.DiskMB += alloc.DiskMB
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
