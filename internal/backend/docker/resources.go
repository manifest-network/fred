package docker

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

	// Per-lease tracking
	allocations map[string]ResourceAllocation

	// skuResolver resolves SKU identifiers to profiles
	skuResolver SKUResolver
}

// NewResourcePool creates a new resource pool with the given capacity.
func NewResourcePool(totalCPU float64, totalMemoryMB, totalDiskMB int64, resolver SKUResolver) *ResourcePool {
	return &ResourcePool{
		totalCPU:    totalCPU,
		totalMemory: totalMemoryMB,
		totalDisk:   totalDiskMB,
		allocations: make(map[string]ResourceAllocation),
		skuResolver: resolver,
	}
}

// TryAllocate attempts to reserve resources for a lease based on SKU profile.
// Returns nil if allocation succeeds, error if SKU is unknown or insufficient resources.
func (p *ResourcePool) TryAllocate(leaseUUID, sku string) error {
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

	// Check capacity
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

	// Reserve resources
	p.allocatedCPU += profile.CPUCores
	p.allocatedMemory += profile.MemoryMB
	p.allocatedDisk += profile.DiskMB

	p.allocations[leaseUUID] = ResourceAllocation{
		LeaseUUID: leaseUUID,
		SKU:       sku,
		CPUCores:  profile.CPUCores,
		MemoryMB:  profile.MemoryMB,
		DiskMB:    profile.DiskMB,
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
		TotalCPU:        p.totalCPU,
		TotalMemoryMB:   p.totalMemory,
		TotalDiskMB:     p.totalDisk,
		AllocatedCPU:    p.allocatedCPU,
		AllocatedMemory: p.allocatedMemory,
		AllocatedDisk:   p.allocatedDisk,
		AllocationCount: len(p.allocations),
	}
}

// ResourceStats contains resource usage statistics.
type ResourceStats struct {
	TotalCPU        float64
	TotalMemoryMB   int64
	TotalDiskMB     int64
	AllocatedCPU    float64
	AllocatedMemory int64
	AllocatedDisk   int64
	AllocationCount int
}

// AvailableCPU returns available CPU cores.
func (s ResourceStats) AvailableCPU() float64 {
	return s.TotalCPU - s.AllocatedCPU
}

// AvailableMemoryMB returns available memory in MB.
func (s ResourceStats) AvailableMemoryMB() int64 {
	return s.TotalMemoryMB - s.AllocatedMemory
}

// AvailableDiskMB returns available disk in MB.
func (s ResourceStats) AvailableDiskMB() int64 {
	return s.TotalDiskMB - s.AllocatedDisk
}

// Reset clears all allocations and rebuilds from a list of allocations.
// This is used during state recovery from Docker.
func (p *ResourcePool) Reset(allocations []ResourceAllocation) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear existing
	p.allocations = make(map[string]ResourceAllocation)
	p.allocatedCPU = 0
	p.allocatedMemory = 0
	p.allocatedDisk = 0

	// Rebuild from provided allocations
	for _, alloc := range allocations {
		p.allocations[alloc.LeaseUUID] = alloc
		p.allocatedCPU += alloc.CPUCores
		p.allocatedMemory += alloc.MemoryMB
		p.allocatedDisk += alloc.DiskMB
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
