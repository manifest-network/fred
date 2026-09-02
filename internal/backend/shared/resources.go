package shared

import (
	"fmt"
	"maps"
	"math"
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

// ResourcePool manages the backend's resource capacity. It provides atomic
// allocation and release from immutable resource rows; effective disk may
// include substrate-specific scratch in addition to the durable SKU profile.
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
// It panics when a capacity, quota, or resolver is invalid: callers construct a
// pool once at startup, so accepting a malformed boundary would permanently
// corrupt or disable its admission accounting. The quota is copied so later
// caller mutation cannot invalidate a live pool.
func NewResourcePool(totalCPU float64, totalMemoryMB, totalDiskMB int64, resolver SKUResolver, tenantQuota *TenantQuotaConfig) *ResourcePool {
	if math.IsNaN(totalCPU) || math.IsInf(totalCPU, 0) {
		panic("shared.NewResourcePool: total CPU must be finite")
	}
	if totalCPU <= 0 {
		panic("shared.NewResourcePool: total CPU must be positive")
	}
	if totalMemoryMB <= 0 {
		panic("shared.NewResourcePool: total memory must be positive")
	}
	if totalDiskMB <= 0 {
		panic("shared.NewResourcePool: total disk must be positive")
	}
	if resolver == nil {
		panic("shared.NewResourcePool: resolver must not be nil")
	}
	var quotaCopy *TenantQuotaConfig
	if tenantQuota != nil {
		if err := tenantQuota.Validate(); err != nil {
			panic(fmt.Sprintf("shared.NewResourcePool: invalid tenant quota: %v", err))
		}
		quota := *tenantQuota
		quotaCopy = &quota
	}
	return &ResourcePool{
		totalCPU:    totalCPU,
		totalMemory: totalMemoryMB,
		totalDisk:   totalDiskMB,
		allocations: make(map[string]ResourceAllocation),
		tenantUsage: make(map[string]ResourceAllocation),
		tenantQuota: quotaCopy,
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

// TryAllocateResolved reserves resources using an already-resolved immutable
// profile. Durable workflow owners use this path so admission cannot resolve a
// different value from the profile persisted for recovery.
func (p *ResourcePool) TryAllocateResolved(
	leaseUUID, tenant string,
	resources SKUResourceSnapshot,
) error {
	profile, err := effectiveAllocationProfile(resources)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tryAllocateProfileLocked(leaseUUID, resources.SKU, tenant, profile, true)
}

// ReplaceResolvedAll atomically replaces an existing operation's exact
// allocation keys with a new immutable snapshot. It is the re-provision
// boundary: predecessor containers are torn down first, then the pool and the
// provision projection move generations together without exposing phantom
// free capacity to another tenant. Any validation/capacity failure restores the
// complete predecessor accounting byte-for-byte.
func (p *ResourcePool) ReplaceResolvedAll(
	oldIDs []string,
	instances []ResolvedAdoptInstance,
	tenant string,
) error {
	profiles := make([]SKUProfile, len(instances))
	for i, instance := range instances {
		profile, err := effectiveAllocationProfile(instance.Resources)
		if err != nil {
			return fmt.Errorf("replacement allocation %q: %w", instance.ID, err)
		}
		profiles[i] = profile
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	allocationsBefore := maps.Clone(p.allocations)
	tenantUsageBefore := maps.Clone(p.tenantUsage)
	allocatedCPUBefore := p.allocatedCPU
	allocatedMemoryBefore := p.allocatedMemory
	allocatedDiskBefore := p.allocatedDisk
	rollback := func() {
		p.allocations = allocationsBefore
		p.tenantUsage = tenantUsageBefore
		p.allocatedCPU = allocatedCPUBefore
		p.allocatedMemory = allocatedMemoryBefore
		p.allocatedDisk = allocatedDiskBefore
	}

	for _, id := range oldIDs {
		p.releaseLocked(id)
	}
	for i, instance := range instances {
		if err := p.tryAllocateProfileLocked(
			instance.ID,
			instance.Resources.SKU,
			tenant,
			profiles[i],
			true,
		); err != nil {
			rollback()
			return fmt.Errorf("replacement allocation %q: %w", instance.ID, err)
		}
	}
	return nil
}

// AdoptInstance identifies one container instance to reserve on the restore/adopt
// path: its pool allocation id and the SKU of the tier it is restored onto.
type AdoptInstance struct {
	ID  string
	SKU string
}

// ResolvedAdoptInstance pairs an adopted allocation ID with the exact profile
// already frozen in its durable operation intent.
type ResolvedAdoptInstance struct {
	ID        string
	Resources SKUResourceSnapshot
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
// double-count the retained bytes still in the projection until ClaimForRestoreWithAuthority
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

	if oldRetainedDiskMB < 0 {
		return fmt.Errorf("old retained disk must not be negative: %d MB", oldRetainedDiskMB)
	}
	resolved := make([]ResolvedAdoptInstance, 0, len(instances))
	for _, in := range instances {
		profile, err := p.resolveProfile(in.SKU)
		if err != nil {
			return err
		}
		resolved = append(resolved, ResolvedAdoptInstance{
			ID: in.ID,
			Resources: SKUResourceSnapshot{
				SKU: in.SKU, CPUCores: profile.CPUCores,
				MemoryMB: profile.MemoryMB, DiskMB: profile.DiskMB,
			},
		})
	}
	return p.tryAllocateAdoptAllResolvedLocked(resolved, tenant, oldRetainedDiskMB)
}

// TryAllocateAdoptAllResolved is the immutable-profile counterpart of
// TryAllocateAdoptAll. The complete batch is validated and committed under one
// pool lock, preserving the aggregate promote-delta gate while guaranteeing
// that accounting uses the same values as the durable operation intent.
func (p *ResourcePool) TryAllocateAdoptAllResolved(
	instances []ResolvedAdoptInstance,
	tenant string,
	oldRetainedDiskMB int64,
) error {
	for _, in := range instances {
		if _, err := effectiveAllocationProfile(in.Resources); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tryAllocateAdoptAllResolvedLocked(instances, tenant, oldRetainedDiskMB)
}

func (p *ResourcePool) tryAllocateAdoptAllResolvedLocked(
	resolved []ResolvedAdoptInstance,
	tenant string,
	oldRetainedDiskMB int64,
) error {
	if oldRetainedDiskMB < 0 {
		return fmt.Errorf("old retained disk must not be negative: %d MB", oldRetainedDiskMB)
	}
	var newDiskMB int64
	for _, in := range resolved {
		effectiveDiskMB, err := in.Resources.EffectiveDiskMB()
		if err != nil {
			return err
		}
		var ok bool
		newDiskMB, ok = addNonNegativeInt64(newDiskMB, effectiveDiskMB)
		if !ok {
			return fmt.Errorf("restore disk requirement exceeds supported range")
		}
	}
	if delta := newDiskMB - min(newDiskMB, oldRetainedDiskMB); delta > p.availableDiskLocked() {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			delta, p.availableDiskLocked())
	}

	reserved := make([]string, 0, len(resolved))
	for _, in := range resolved {
		profile, err := effectiveAllocationProfile(in.Resources)
		if err != nil {
			return err
		}
		if err := p.tryAllocateProfileLocked(in.ID, in.Resources.SKU, tenant, profile, false); err != nil {
			for _, id := range reserved {
				p.releaseLocked(id)
			}
			return err
		}
		reserved = append(reserved, in.ID)
	}
	return nil
}

func effectiveAllocationProfile(resources SKUResourceSnapshot) (SKUProfile, error) {
	effectiveDiskMB, err := resources.EffectiveDiskMB()
	if err != nil {
		return SKUProfile{}, err
	}
	profile := resources.Profile()
	profile.DiskMB = effectiveDiskMB
	return profile, nil
}

// tryAllocateLocked reserves one instance. The caller MUST hold p.mu. When
// gateDisk is true the global disk capacity check is enforced (fresh provision);
// when false (adopt) it is skipped because the disk is already committed and its
// capacity is gated by the caller (TryAllocateAdoptAll's aggregate delta). CPU,
// memory, and tenant quota are always gated; the full DiskMB is always added to
// allocatedDisk for correct live accounting.
func (p *ResourcePool) tryAllocateLocked(leaseUUID, sku, tenant string, gateDisk bool) error {
	// Reject duplicates before invoking a caller-supplied resolver. The helper
	// repeats this check for batch adoption, which enters with a pre-resolved
	// profile and may encounter duplicate instance IDs within the same batch.
	if _, exists := p.allocations[leaseUUID]; exists {
		return fmt.Errorf("lease %s already has allocated resources", leaseUUID)
	}

	// Resolve SKU to profile
	profile, err := p.resolveProfile(sku)
	if err != nil {
		return err
	}
	return p.tryAllocateProfileLocked(leaseUUID, sku, tenant, profile, gateDisk)
}

// tryAllocateProfileLocked reserves a profile already validated by
// resolveProfile. TryAllocateAdoptAll resolves each instance exactly once, so a
// stateful library resolver cannot make the aggregate disk gate and committed
// reservation disagree.
func (p *ResourcePool) tryAllocateProfileLocked(leaseUUID, sku, tenant string, profile SKUProfile, gateDisk bool) error {
	if _, exists := p.allocations[leaseUUID]; exists {
		return fmt.Errorf("lease %s already has allocated resources", leaseUUID)
	}

	// Check global capacity
	if profile.CPUCores > p.availableCPULocked() {
		return fmt.Errorf("insufficient CPU: need %.2f cores, have %.2f available",
			profile.CPUCores, p.availableCPULocked())
	}
	if profile.MemoryMB > p.availableMemoryLocked() {
		return fmt.Errorf("insufficient memory: need %d MB, have %d MB available",
			profile.MemoryMB, p.availableMemoryLocked())
	}
	if gateDisk && profile.DiskMB > p.availableDiskLocked() {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			profile.DiskMB, p.availableDiskLocked())
	}

	// Check per-tenant quota if configured
	if p.tenantQuota != nil && tenant != "" {
		usage := p.tenantUsage[tenant]
		if profile.CPUCores > availableCPU(p.tenantQuota.MaxCPUCores, usage.CPUCores) {
			return fmt.Errorf("tenant %s CPU quota exceeded: need %.2f cores, have %.2f available (quota: %.2f)",
				tenant, profile.CPUCores, availableCPU(p.tenantQuota.MaxCPUCores, usage.CPUCores), p.tenantQuota.MaxCPUCores)
		}
		if profile.MemoryMB > availableInt64(p.tenantQuota.MaxMemoryMB, usage.MemoryMB) {
			return fmt.Errorf("tenant %s memory quota exceeded: need %d MB, have %d MB available (quota: %d)",
				tenant, profile.MemoryMB, availableInt64(p.tenantQuota.MaxMemoryMB, usage.MemoryMB), p.tenantQuota.MaxMemoryMB)
		}
		if profile.DiskMB > availableInt64(p.tenantQuota.MaxDiskMB, usage.DiskMB) {
			return fmt.Errorf("tenant %s disk quota exceeded: need %d MB, have %d MB available (quota: %d)",
				tenant, profile.DiskMB, availableInt64(p.tenantQuota.MaxDiskMB, usage.DiskMB), p.tenantQuota.MaxDiskMB)
		}
	}

	// Calculate every new aggregate before publishing any part of the
	// reservation. The disk sum needs an explicit check on adopt: its capacity
	// gate deliberately accounts only the promote delta while allocatedDisk still
	// records the full restored footprint.
	nextCPU := p.allocatedCPU + profile.CPUCores
	if math.IsInf(nextCPU, 0) {
		return fmt.Errorf("CPU accounting exceeds supported range")
	}
	nextMemory, ok := addNonNegativeInt64(p.allocatedMemory, profile.MemoryMB)
	if !ok {
		return fmt.Errorf("memory accounting exceeds supported range")
	}
	nextDisk, ok := addNonNegativeInt64(p.allocatedDisk, profile.DiskMB)
	if !ok {
		return fmt.Errorf("disk accounting exceeds supported range")
	}

	usage := p.tenantUsage[tenant]
	if tenant != "" {
		usage.CPUCores += profile.CPUCores
		if math.IsInf(usage.CPUCores, 0) {
			return fmt.Errorf("tenant %s CPU accounting exceeds supported range", tenant)
		}
		usage.MemoryMB, ok = addNonNegativeInt64(usage.MemoryMB, profile.MemoryMB)
		if !ok {
			return fmt.Errorf("tenant %s memory accounting exceeds supported range", tenant)
		}
		usage.DiskMB, ok = addNonNegativeInt64(usage.DiskMB, profile.DiskMB)
		if !ok {
			return fmt.Errorf("tenant %s disk accounting exceeds supported range", tenant)
		}
	}

	// Reserve resources only after every aggregate is known representable.
	p.allocatedCPU = nextCPU
	p.allocatedMemory = nextMemory
	p.allocatedDisk = nextDisk

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
		p.tenantUsage[tenant] = usage
	}

	return nil
}

func (p *ResourcePool) availableCPULocked() float64 {
	return availableCPU(p.totalCPU, p.allocatedCPU)
}

func (p *ResourcePool) availableMemoryLocked() int64 {
	return availableInt64(p.totalMemory, p.allocatedMemory)
}

func (p *ResourcePool) availableDiskLocked() int64 {
	return availableInt64(p.totalDisk, p.allocatedDisk, p.retainedDisk)
}

// availableCPU and availableInt64 calculate headroom without adding usage to a
// request. Addition-based gates can wrap int64 or overflow float64 to +Inf and
// accidentally admit work; subtraction from a validated positive limit fails
// closed when accounting is already at or above capacity.
func availableCPU(total, used float64) float64 {
	if math.IsNaN(total) || math.IsInf(total, 0) || math.IsNaN(used) || math.IsInf(used, 0) || total <= 0 || used < 0 || used >= total {
		return 0
	}
	return total - used
}

func availableInt64(total int64, used ...int64) int64 {
	if total <= 0 {
		return 0
	}
	remaining := total
	for _, value := range used {
		if value < 0 || value >= remaining {
			return 0
		}
		remaining -= value
	}
	return remaining
}

func addNonNegativeInt64(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || b > math.MaxInt64-a {
		return 0, false
	}
	return a + b, true
}

// resolveProfile validates library-supplied resolver output at the accounting
// boundary. Backend configuration validation normally catches invalid profiles,
// but ResourcePool is also reusable directly; accepting NaN here would poison
// allocatedCPU and make every subsequent capacity comparison false.
func (p *ResourcePool) resolveProfile(sku string) (SKUProfile, error) {
	profile, err := p.skuResolver(sku)
	if err != nil {
		return SKUProfile{}, err
	}
	if err := validateResolvedProfile(sku, profile); err != nil {
		return SKUProfile{}, err
	}
	return profile, nil
}

func validateResolvedProfile(sku string, profile SKUProfile) error {
	if sku == "" {
		return fmt.Errorf("resource profile requires a SKU")
	}
	if err := profile.Validate(); err != nil {
		return fmt.Errorf("invalid resource profile for SKU %q: %w", sku, err)
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
	delete(p.allocations, leaseUUID)
	// The allocation map is the exact ownership authority. Floating-point
	// subtraction is not its own inverse when allocations are released in a
	// different order from admission, so an otherwise-empty pool can end at a
	// tiny negative allocatedCPU. availableCPU deliberately treats negative
	// accounting as corrupt and fails closed; publish the exact empty state from
	// the map instead of letting harmless round-off wedge all later admission.
	if len(p.allocations) == 0 {
		p.allocatedCPU = 0
		p.allocatedMemory = 0
		p.allocatedDisk = 0
	}

	// Update tenant aggregate
	if alloc.Tenant != "" {
		usage := p.tenantUsage[alloc.Tenant]
		usage.CPUCores -= alloc.CPUCores
		usage.MemoryMB -= alloc.MemoryMB
		usage.DiskMB -= alloc.DiskMB
		// Every valid allocation has positive integral memory, so zero memory is
		// an exact proof that this was the tenant's last allocation. Use that
		// structural fact instead of requiring an inexact CPU sum to reach <= 0.
		if usage.MemoryMB == 0 {
			delete(p.tenantUsage, alloc.Tenant)
		} else {
			p.tenantUsage[alloc.Tenant] = usage
		}
	}
}

// SetRetainedDisk records the aggregate disk (MB) reserved by retained
// (soft-deleted) volumes. The owner derives this from the retention store and
// pushes it here; TryAllocate subtracts it from available disk so retained
// volumes keep counting against the pool until reaped. It rejects a negative
// projection without changing the last valid value: silently converting broken
// accounting to zero would erase durable capacity authority and over-admit.
// Idempotent.
func (p *ResourcePool) SetRetainedDisk(mb int64) error {
	if mb < 0 {
		return fmt.Errorf("retained disk must be non-negative: %d MB", mb)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retainedDisk = mb
	return nil
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
	return availableCPU(s.TotalCPU, s.AllocatedCPU)
}

// AvailableMemoryMB returns available memory in MB, clamped to zero when the
// recovered allocation projection is already at or above configured capacity.
func (s ResourceStats) AvailableMemoryMB() int64 {
	return availableInt64(s.TotalMemoryMB, s.AllocatedMemoryMB)
}

// AvailableDiskMB returns disk available for new allocations: total minus live
// allocations minus retained (soft-deleted) reservations, clamped to >= 0 (a
// total_disk_mb shrink or stale retained projection must not surface a negative
// "available" via the /stats endpoints).
func (s ResourceStats) AvailableDiskMB() int64 {
	return availableInt64(s.TotalDiskMB, s.AllocatedDiskMB, s.RetainedDiskMB)
}

// Reset clears all allocations and rebuilds from a list of allocations. It
// returns an error without changing the existing snapshot if an allocation is
// malformed or the rebuilt accounting cannot be represented.
func (p *ResourcePool) Reset(allocations []ResourceAllocation) error {
	return p.ResetPreserving(allocations, nil)
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
// to Reset. Like Reset, it returns an error without publishing a partial
// snapshot when the rebuilt accounting is invalid or overflows.
func (p *ResourcePool) ResetPreserving(allocations []ResourceAllocation, keep func(key string) bool) error {
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

	// Build and validate a replacement snapshot before publishing any of it. This
	// keeps Reset atomic even if a library caller supplies a non-finite allocation
	// or enough entries to overflow an aggregate. Duplicate input keys are folded
	// last-wins, matching the allocation map, while preserved entries still win.
	rebuilt := make(map[string]ResourceAllocation, len(allocations)+len(preserved))
	for _, alloc := range allocations {
		if _, isPreserved := preserved[alloc.LeaseUUID]; isPreserved {
			continue
		}
		rebuilt[alloc.LeaseUUID] = alloc
	}
	for key, alloc := range preserved {
		rebuilt[key] = alloc
	}

	allocatedCPU, allocatedMemory, allocatedDisk, tenantUsage, err := aggregateAllocations(rebuilt)
	if err != nil {
		return fmt.Errorf("rebuild resource allocations: %w", err)
	}
	p.allocations = rebuilt
	p.tenantUsage = tenantUsage
	p.allocatedCPU = allocatedCPU
	p.allocatedMemory = allocatedMemory
	p.allocatedDisk = allocatedDisk
	return nil
}

// aggregateAllocations validates and folds a complete recovery snapshot without
// mutating the live pool. Recovery may legitimately report usage above a newly
// reduced configured capacity; that remains represented and makes future
// admission fail closed. Values that cannot be represented are rejected.
func aggregateAllocations(allocations map[string]ResourceAllocation) (float64, int64, int64, map[string]ResourceAllocation, error) {
	var allocatedCPU float64
	var allocatedMemory, allocatedDisk int64
	tenantUsage := make(map[string]ResourceAllocation)
	for key, alloc := range allocations {
		if math.IsNaN(alloc.CPUCores) || math.IsInf(alloc.CPUCores, 0) {
			return 0, 0, 0, nil, fmt.Errorf("allocation %q CPU must be finite", key)
		}
		if alloc.CPUCores <= 0 {
			return 0, 0, 0, nil, fmt.Errorf("allocation %q CPU must be positive", key)
		}
		if alloc.MemoryMB <= 0 {
			return 0, 0, 0, nil, fmt.Errorf("allocation %q memory must be positive", key)
		}
		if alloc.DiskMB < 0 {
			return 0, 0, 0, nil, fmt.Errorf("allocation %q disk must be non-negative", key)
		}
		allocatedCPU += alloc.CPUCores
		if math.IsInf(allocatedCPU, 0) {
			return 0, 0, 0, nil, fmt.Errorf("aggregate CPU exceeds supported range")
		}
		var ok bool
		allocatedMemory, ok = addNonNegativeInt64(allocatedMemory, alloc.MemoryMB)
		if !ok {
			return 0, 0, 0, nil, fmt.Errorf("aggregate memory exceeds supported range")
		}
		allocatedDisk, ok = addNonNegativeInt64(allocatedDisk, alloc.DiskMB)
		if !ok {
			return 0, 0, 0, nil, fmt.Errorf("aggregate disk exceeds supported range")
		}

		if alloc.Tenant != "" {
			usage := tenantUsage[alloc.Tenant]
			usage.CPUCores += alloc.CPUCores
			if math.IsInf(usage.CPUCores, 0) {
				return 0, 0, 0, nil, fmt.Errorf("tenant %q aggregate CPU exceeds supported range", alloc.Tenant)
			}
			usage.MemoryMB, ok = addNonNegativeInt64(usage.MemoryMB, alloc.MemoryMB)
			if !ok {
				return 0, 0, 0, nil, fmt.Errorf("tenant %q aggregate memory exceeds supported range", alloc.Tenant)
			}
			usage.DiskMB, ok = addNonNegativeInt64(usage.DiskMB, alloc.DiskMB)
			if !ok {
				return 0, 0, 0, nil, fmt.Errorf("tenant %q aggregate disk exceeds supported range", alloc.Tenant)
			}
			tenantUsage[alloc.Tenant] = usage
		}
	}
	return allocatedCPU, allocatedMemory, allocatedDisk, tenantUsage, nil
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
