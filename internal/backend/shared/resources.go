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
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tryAllocateLocked(leaseUUID, sku, tenant, true)
}

// AdoptInstance identifies one container instance to reserve on the restore/adopt
// path: its pool allocation id and the SKU of the tier it is restored onto.
type AdoptInstance struct {
	ID  string
	SKU string
}

// TryAllocateAdoptAll atomically reserves every instance of a restore under a
// single lock acquisition. Restore adopts existing volumes (rename, not fresh
// disk), so the per-instance reservations SKIP the global disk gate — the adopted
// bytes are already committed on disk and counted in the retained projection.
// Disk CAPACITY is gated once, here, on the AGGREGATE promote delta. The pool
// computes the new total from its OWN resolver — the exact DiskMB the per-instance
// reservations below will add — and subtracts oldRetainedDiskMB (the lease's
// already-committed retained footprint, which the caller derives from the
// retention record and which is already counted in retainedDisk). A same-tier or
// demote restore (delta <= 0) adds no disk pressure.
//
// Gating the delta and committing all reservations under ONE lock is what makes
// admission correct on three axes: (1) EXACT — a per-volume disk gate would
// double-count the retained bytes still in the projection until ClaimForRestore
// and reject a fitting multi-volume promote; (2) ATOMIC — no concurrent
// TryAllocate/restore can consume disk between the delta check and the
// reservations, so the pool cannot be over-committed; and (3) CONSISTENT — the
// gated new total is computed from the same resolver that sizes the reservations,
// so a caller whose SKU resolver diverges from the pool's cannot under-gate disk
// (ENG-545, PR #184 review). Only oldRetainedDiskMB is a caller input, and it
// enters the gate in the safe direction (undercounting it enlarges the delta).
//
// CPU, memory, and tenant quota are gated per instance (new containers). On any
// failure nothing is reserved: the instances committed so far in this call are
// rolled back before returning.
func (p *ResourcePool) TryAllocateAdoptAll(instances []AdoptInstance, tenant string, oldRetainedDiskMB int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var newDiskMB int64
	for _, in := range instances {
		profile, err := p.skuResolver(in.SKU)
		if err != nil {
			return err
		}
		newDiskMB += profile.DiskMB
	}
	if delta := newDiskMB - oldRetainedDiskMB; delta > 0 && p.allocatedDisk+p.retainedDisk+delta > p.totalDisk {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			delta, max(int64(0), p.totalDisk-p.allocatedDisk-p.retainedDisk))
	}

	reserved := make([]string, 0, len(instances))
	for _, in := range instances {
		if err := p.tryAllocateLocked(in.ID, in.SKU, tenant, false); err != nil {
			for _, id := range reserved {
				p.releaseLocked(id)
			}
			return err
		}
		reserved = append(reserved, in.ID)
	}
	return nil
}

// tryAllocateLocked reserves one instance. The caller MUST hold p.mu. When
// gateDisk is true the global disk capacity check is enforced (fresh provision);
// when false (adopt) it is skipped because the disk is already committed and its
// capacity is gated by the caller (TryAllocateAdoptAll's aggregate delta). CPU,
// memory, and tenant quota are always gated; the full DiskMB is always added to
// allocatedDisk for correct live accounting.
func (p *ResourcePool) tryAllocateLocked(leaseUUID, sku, tenant string, gateDisk bool) error {
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
	if gateDisk && p.allocatedDisk+p.retainedDisk+profile.DiskMB > p.totalDisk {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			profile.DiskMB, max(int64(0), p.totalDisk-p.allocatedDisk-p.retainedDisk))
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
	p.releaseLocked(leaseUUID)
}

// releaseLocked is Release's body; the caller MUST hold p.mu.
func (p *ResourcePool) releaseLocked(leaseUUID string) {
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
	p.ResetPreserving(allocations, nil)
}

// ResetPreserving is the recovery-safe variant of Reset. It rebuilds the pool's
// live allocations from allocations, but first RETAINS every current allocation
// for which keep returns true. Retained entries take precedence over any
// same-key entry in allocations (deduped by allocation key), so an allocation
// present in both is counted exactly once.
//
// recoverState rebuilds the pool from a container-derived snapshot each tick,
// but a lease that is mid-operation (Provisioning/Restarting/Updating) may have
// no containers yet (still pulling its image) or containers that do not yet
// reflect its authoritative TryAllocate reservation, so such leases are excluded
// from that snapshot. Dropping their reservation on the rebuild would let
// TryAllocate momentarily see phantom free capacity and over-admit past physical
// capacity, leaving the pool over-committed once the lease re-registers
// (ENG-546). The caller marks those in-flight leases via keep so their existing
// reservations survive the rebuild, keyed identically — read from the live pool
// rather than reconstructed, so the reservation is preserved even in the window
// before its Items are populated.
//
// keep receives each current allocation's key and is invoked while the pool lock
// is held, so it must not call back into ResourcePool or perform expensive or
// blocking work. A nil keep preserves nothing, making ResetPreserving identical
// to Reset.
func (p *ResourcePool) ResetPreserving(allocations []ResourceAllocation, keep func(key string) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Snapshot the entries to retain from the CURRENT allocations before
	// clearing, keyed by allocation key so a same-key entry in allocations
	// cannot double-count them in the rebuild below.
	var preserved map[string]ResourceAllocation
	if keep != nil {
		for key, alloc := range p.allocations {
			if keep(key) {
				if preserved == nil {
					preserved = make(map[string]ResourceAllocation)
				}
				preserved[key] = alloc
			}
		}
	}

	// Clear existing.
	p.allocations = make(map[string]ResourceAllocation)
	p.tenantUsage = make(map[string]ResourceAllocation)
	p.allocatedCPU = 0
	p.allocatedMemory = 0
	p.allocatedDisk = 0

	// Rebuild from the provided allocations, skipping any key being preserved
	// (the retained entry is authoritative and re-added below).
	for _, alloc := range allocations {
		if _, isPreserved := preserved[alloc.LeaseUUID]; isPreserved {
			continue
		}
		p.addLocked(alloc)
	}
	// Re-add the retained in-flight reservations.
	for _, alloc := range preserved {
		p.addLocked(alloc)
	}
}

// addLocked folds a single allocation into the live totals and the per-lease and
// per-tenant maps. The caller must hold p.mu.
func (p *ResourcePool) addLocked(alloc ResourceAllocation) {
	p.allocations[alloc.LeaseUUID] = alloc
	p.allocatedCPU += alloc.CPUCores
	p.allocatedMemory += alloc.MemoryMB
	p.allocatedDisk += alloc.DiskMB

	if alloc.Tenant != "" {
		usage := p.tenantUsage[alloc.Tenant]
		usage.CPUCores += alloc.CPUCores
		usage.MemoryMB += alloc.MemoryMB
		usage.DiskMB += alloc.DiskMB
		p.tenantUsage[alloc.Tenant] = usage
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
