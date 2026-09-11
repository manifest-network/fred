package shared

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
)

// ProvisionAdmission binds a pending operation to pool-owned capacity before
// actor admission. Copies share one state: only an unstarted admission may be
// aborted, and only matching terminal evidence may release executing capacity.
// Ambiguity preserves the conservative ledger for ordinary durable recovery.
type ProvisionAdmission struct{ state *provisionReservation }

// ProvisionResourceExecution is the consumed reservation carried by the live
// worker. Only Begin can mint it; an unaccepted command cannot execute work or
// consume terminal evidence through its admission token.
// The distinct private field name prevents Go conversions between capabilities.
type ProvisionResourceExecution struct{ execution *provisionReservation }

func (e ProvisionResourceExecution) Operation() OperationIntentClaim {
	return (ProvisionAdmission{state: e.execution}).Operation()
}

func (e ProvisionResourceExecution) Valid() bool {
	if e.execution == nil {
		return false
	}
	e.execution.pool.mu.Lock()
	defer e.execution.pool.mu.Unlock()
	return e.execution.begun && !e.execution.closed
}

type provisionReservation struct {
	pool        *ResourcePool
	operation   OperationIntentClaim
	before      []ResourceAllocation
	predecessor []ResourceAllocation
	target      []ResourceAllocation
	begun       bool
	closed      bool
}

func (a ProvisionAdmission) Operation() OperationIntentClaim {
	if a.state == nil {
		return OperationIntentClaim{}
	}
	return a.state.operation
}

func (a ProvisionAdmission) Valid() bool {
	if a.state == nil || !a.state.operation.Valid() {
		return false
	}
	a.state.pool.mu.Lock()
	defer a.state.pool.mu.Unlock()
	return !a.state.closed
}

// Begin transfers admission ownership to its one worker before Started can
// issue a mutation capability. A rejected command can no longer abort a copy.
func (a ProvisionAdmission) Begin() (ProvisionResourceExecution, error) {
	if a.state == nil {
		return ProvisionResourceExecution{}, errors.New("provision capacity admission is required")
	}
	p := a.state.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if a.state.begun || a.state.closed {
		return ProvisionResourceExecution{}, errors.New("provision capacity admission was already consumed")
	}
	a.state.begun = true
	return ProvisionResourceExecution{execution: a.state}, nil
}

// Abort restores the exact pre-admission ledger only while no worker owns it.
func (a ProvisionAdmission) Abort() error {
	if a.state == nil {
		return errors.New("provision capacity admission is required")
	}
	p := a.state.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if a.state.begun {
		return errors.New("provision capacity admission is no longer abortable")
	}
	if a.state.closed {
		return nil
	}
	return a.finishLocked(a.state.before)
}

func (a ProvisionResourceExecution) CompleteSuccess(proof OperationReleaseCommitted) error {
	if a.execution == nil || !proof.MatchesIntent(a.Operation()) {
		return errors.New("provision capacity success requires the exact committed release")
	}
	return a.complete(a.execution.target)
}

func (a ProvisionResourceExecution) CompleteFailure(proof OperationReleaseUncommitted) error {
	if a.execution == nil || !proof.MatchesIntent(a.Operation()) {
		return errors.New("provision capacity failure requires exact uncommitted evidence")
	}
	// A predecessor can still own bytes even when this attempt never entered a
	// mutation. Keep its durable sizing, not merely the possibly absent old pool
	// projection. This is conservative even if exact cleanup already removed it.
	return a.complete(a.execution.predecessor)
}

func (a ProvisionResourceExecution) complete(allocations []ResourceAllocation) error {
	p := a.execution.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if !a.execution.begun || a.execution.closed {
		return errors.New("provision capacity execution is not current")
	}
	return (ProvisionAdmission{state: a.execution}).finishLocked(allocations)
}

// DeferRecovery ends live ownership without releasing any reservation. Durable
// intent/release evidence, not this process-local capability, owns recovery.
func (a ProvisionResourceExecution) DeferRecovery() {
	if a.execution == nil {
		return
	}
	p := a.execution.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if !a.execution.begun || a.execution.closed {
		return
	}
	for key, allocation := range p.allocations {
		if allocation.reservation == a.execution {
			allocation.reservation = nil
			p.allocations[key] = allocation
		}
	}
	a.execution.closed = true
}

func (a ProvisionAdmission) finishLocked(allocations []ResourceAllocation) error {
	p := a.state.pool
	next := maps.Clone(p.allocations)
	for key, allocation := range next {
		if allocation.reservation == a.state {
			delete(next, key)
		}
	}
	for _, allocation := range allocations {
		allocation.reservation = nil
		next[allocation.LeaseUUID] = allocation
	}
	if err := p.publishResourceSnapshotLocked(next); err != nil {
		return err
	}
	a.state.closed = true
	return nil
}

// ReserveProvisionResources derives both generations from their journal
// authority, then atomically checks and reserves their conservative envelope.
// Neither an absent predecessor Release nor an existing Failed projection
// bypasses this admission. No substrate work or actor command is performed.
func (s *OperationSettlement) ReserveProvisionResources(
	p *ResourcePool,
	claim OperationIntentClaim,
) (ProvisionAdmission, error) {
	if p == nil {
		return ProvisionAdmission{}, errors.New("provision resource pool is required")
	}
	durable, err := requireOperationSettlementClaim(s, claim)
	if err != nil {
		return ProvisionAdmission{}, err
	}
	if durable.Kind() != OperationIntentProvision || durable.ExecutionPhase() != OperationExecutionBeforeEffects {
		return ProvisionAdmission{}, errors.New("provision capacity requires an unstarted provision operation")
	}
	target, err := provisionResourceAllocations(durable.LeaseUUID(), durable.Tenant(), durable.EffectiveItems(), durable.ResourceProfiles())
	if err != nil {
		return ProvisionAdmission{}, err
	}
	var predecessor []ResourceAllocation
	active, err := s.releases.LatestActive(durable.LeaseUUID())
	if err != nil {
		return ProvisionAdmission{}, err
	}
	if active != nil {
		authorityTenant := ""
		if active.RuntimeAuthority != nil {
			authorityTenant = active.RuntimeAuthority.Tenant()
		} else if active.LegacyRuntimeAuthority != nil {
			authorityTenant = active.LegacyRuntimeAuthority.Tenant()
		}
		if authorityTenant != durable.Tenant() {
			return ProvisionAdmission{}, errors.New("provision predecessor resource authority differs from tenant")
		}
		predecessor, err = provisionResourceAllocations(durable.LeaseUUID(), durable.Tenant(), active.Items, active.ResourceProfiles)
		if err != nil {
			return ProvisionAdmission{}, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.accountingHolds) != 0 {
		return ProvisionAdmission{}, ErrResourceAccountingIncomplete
	}
	var before []ResourceAllocation
	for key, allocation := range p.allocations {
		owner, err := allocationLeaseUUID(key)
		if err != nil || owner != durable.LeaseUUID() {
			continue
		}
		if allocation.reservation != nil {
			return ProvisionAdmission{}, errors.New("provision capacity already has a live owner")
		}
		before = append(before, allocation)
	}
	predecessor, err = ConservativeResourceEnvelope(before, predecessor)
	if err != nil {
		return ProvisionAdmission{}, err
	}
	envelope, err := ConservativeResourceEnvelope(predecessor, target)
	if err != nil {
		return ProvisionAdmission{}, err
	}
	state := &provisionReservation{pool: p, operation: durable, before: before, predecessor: predecessor, target: target}
	next := maps.Clone(p.allocations)
	for _, allocation := range envelope {
		allocation.reservation = state
		next[allocation.LeaseUUID] = allocation
	}
	cpu, memory, disk, tenants, err := aggregateAllocations(next)
	if err != nil {
		return ProvisionAdmission{}, err
	}
	if cpu-p.allocatedCPU > p.availableCPULocked() ||
		memory-p.allocatedMemory > p.availableMemoryLocked() ||
		disk-p.allocatedDisk > p.availableDiskLocked() {
		return ProvisionAdmission{}, errors.New("insufficient resources for provision reservation")
	}
	if quota := p.tenantQuota; quota != nil {
		old, next := p.tenantUsage[durable.Tenant()], tenants[durable.Tenant()]
		if next.CPUCores-old.CPUCores > availableCPU(quota.MaxCPUCores, old.CPUCores) ||
			next.MemoryMB-old.MemoryMB > availableInt64(quota.MaxMemoryMB, old.MemoryMB) ||
			next.DiskMB-old.DiskMB > availableInt64(quota.MaxDiskMB, old.DiskMB) {
			return ProvisionAdmission{}, errors.New("tenant quota exceeded by provision reservation")
		}
	}
	p.allocations, p.allocatedCPU, p.allocatedMemory, p.allocatedDisk, p.tenantUsage = next, cpu, memory, disk, tenants
	return ProvisionAdmission{state: state}, nil
}

func provisionResourceAllocations(leaseUUID, tenant string, items []backend.LeaseItem, profiles []SKUResourceSnapshot) ([]ResourceAllocation, error) {
	bySKU := make(map[string]SKUResourceSnapshot, len(profiles))
	for _, profile := range profiles {
		bySKU[profile.SKU] = profile
	}
	var allocations []ResourceAllocation
	for _, item := range items {
		profile, ok := bySKU[item.SKU]
		if !ok || item.ServiceName == "" || item.Quantity <= 0 {
			return nil, errors.New("provision resource authority is incomplete")
		}
		disk, err := profile.EffectiveDiskMB()
		if err != nil {
			return nil, err
		}
		for index := range item.Quantity {
			allocations = append(allocations, ResourceAllocation{
				LeaseUUID: fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, index),
				Tenant:    tenant, SKU: item.SKU, CPUCores: profile.CPUCores, MemoryMB: profile.MemoryMB, DiskMB: disk,
			})
		}
	}
	return allocations, nil
}

// ConservativeResourceEnvelope retains every immutable cohort's keys and the
// maximum sizing of overlapping keys. Admission and Pending-intent recovery use
// the same rule: replacing a generation cannot expose capacity before teardown.
func ConservativeResourceEnvelope(cohorts ...[]ResourceAllocation) ([]ResourceAllocation, error) {
	byID := make(map[string]ResourceAllocation)
	for _, cohort := range cohorts {
		for _, allocation := range cohort {
			if prior, exists := byID[allocation.LeaseUUID]; exists {
				if prior.Tenant != allocation.Tenant {
					return nil, errors.New("resource envelope crosses tenant ownership")
				}
				allocation.CPUCores = max(prior.CPUCores, allocation.CPUCores)
				allocation.MemoryMB = max(prior.MemoryMB, allocation.MemoryMB)
				allocation.DiskMB = max(prior.DiskMB, allocation.DiskMB)
			}
			allocation.reservation = nil
			byID[allocation.LeaseUUID] = allocation
		}
	}
	if _, _, _, _, err := aggregateAllocations(byID); err != nil {
		return nil, err
	}
	result := make([]ResourceAllocation, 0, len(byID))
	for _, key := range slices.Sorted(maps.Keys(byID)) {
		result = append(result, byID[key])
	}
	return result, nil
}

func (p *ResourcePool) publishResourceSnapshotLocked(allocations map[string]ResourceAllocation) error {
	cpu, memory, disk, tenants, err := aggregateAllocations(allocations)
	if err != nil {
		return err
	}
	p.allocations, p.allocatedCPU, p.allocatedMemory, p.allocatedDisk, p.tenantUsage = allocations, cpu, memory, disk, tenants
	return nil
}
