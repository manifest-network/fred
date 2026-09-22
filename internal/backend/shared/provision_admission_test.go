package shared

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvisionAdmissionAndExecutionCannotBeConverted(t *testing.T) {
	admission := reflect.TypeFor[ProvisionAdmission]()
	execution := reflect.TypeFor[ProvisionResourceExecution]()
	assert.False(t, admission.ConvertibleTo(execution), "only Begin may issue worker execution authority")
	assert.False(t, execution.ConvertibleTo(admission), "executing work cannot regain abort authority by conversion")
}

func TestProvisionAdmissionRefusalIsAtomic(t *testing.T) {
	for _, reason := range []string{"accounting hold", "CPU", "memory", "disk", "tenant quota"} {
		t.Run(reason, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			spec := testOperationIntentSpec(t, "resource-admission")
			spec.Items[0].Quantity = 2
			claim := beginHandoffOperation(t, stores.settlement, spec)
			cpu, memory, disk := 8.0, int64(8192), int64(16384)
			var quota *TenantQuotaConfig
			switch reason {
			case "CPU":
				cpu = 1.5
			case "memory":
				memory = 768
			case "disk":
				disk = 1536
			case "tenant quota":
				quota = &TenantQuotaConfig{MaxCPUCores: 1.5, MaxMemoryMB: 8192, MaxDiskMB: 16384}
			}
			pool := NewResourcePool(cpu, memory, disk, func(string) (SKUProfile, error) {
				t.Fatal("immutable provision admission must not consult the mutable resolver")
				return SKUProfile{}, nil
			}, quota)
			require.NoError(t, pool.TryAllocateResolved(spec.LeaseUUID+"-app-0", spec.Tenant, spec.ResourceProfiles[0]))
			before := pool.ListAllocations()
			if reason == "accounting hold" {
				hold := pool.HoldUnaccountedFootprint()
				defer hold.Release()
			}
			admission, err := stores.settlement.ReserveProvisionResources(pool, claim)
			require.Error(t, err)
			if reason == "accounting hold" {
				require.ErrorIs(t, err, ErrResourceAccountingIncomplete)
			}
			require.False(t, admission.Valid())
			assert.Equal(t, before, pool.ListAllocations(), "refusal must preserve the whole predecessor ledger")
		})
	}
}

func TestProvisionAdmissionOwnsConservativeEnvelopeUntilExactSettlement(t *testing.T) {
	for _, outcome := range []string{"abort", "failure", "success", "ambiguous"} {
		t.Run(outcome, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			spec := testOperationIntentSpec(t, "resource-envelope")
			claim := beginHandoffOperation(t, stores.settlement, spec)
			pool := NewResourcePool(8, 8192, 16384, func(string) (SKUProfile, error) {
				t.Fatal("admission must use immutable resources")
				return SKUProfile{}, nil
			}, nil)
			old := spec.ResourceProfiles[0]
			old.CPUCores, old.MemoryMB, old.DiskMB = 2, 1024, 2048
			key := spec.LeaseUUID + "-app-0"
			require.NoError(t, pool.TryAllocateResolved(key, spec.Tenant, old))
			before := pool.ListAllocations()
			admission, err := stores.settlement.ReserveProvisionResources(pool, claim)
			require.NoError(t, err)
			require.True(t, admission.Valid())
			assert.Equal(t, before, pool.ListAllocations(), "a downgrade cannot expose predecessor capacity before teardown")
			// The input slice and public read snapshots are not authority aliases.
			spec.ResourceProfiles[0].CPUCores = 7
			copyOfLedger := pool.ListAllocations()
			copyOfLedger[0].CPUCores = 7
			assert.Equal(t, 2.0, pool.Stats().AllocatedCPU)
			if outcome == "abort" {
				require.NoError(t, admission.Abort())
				require.NoError(t, admission.Abort(), "actor rejection and caller rollback are idempotent")
				assert.Equal(t, before, pool.ListAllocations())
				_, err := admission.Begin()
				require.Error(t, err)
				return
			}
			execution, err := admission.Begin()
			require.NoError(t, err)
			require.Error(t, admission.Abort(), "a copied admission cannot abort its live worker")
			_, err = admission.Begin()
			require.Error(t, err, "copies cannot start two workers")
			pool.Release(key)
			require.NoError(t, pool.ResetConservatively([]ResourceAllocation{{
				LeaseUUID: key, Tenant: claim.Tenant(), SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
			}}))
			assert.Equal(t, before, pool.ListAllocations(), "ordinary release/rebuild cannot shrink a live envelope")
			switch outcome {
			case "failure":
				proof := commitHandoffRefusal(t, stores.settlement, claim)
				require.NoError(t, execution.CompleteFailure(proof))
				assert.Equal(t, before, pool.ListAllocations())
			case "success":
				candidate, err := stores.settlement.PrepareOperationRelease(claim)
				require.NoError(t, err)
				proof := commitHandoffOperation(t, stores.settlement, candidate)
				require.NoError(t, execution.CompleteSuccess(proof))
				assert.Equal(t, 1.0, pool.Stats().AllocatedCPU)
				assert.Equal(t, int64(512), pool.Stats().AllocatedMemoryMB)
				assert.Equal(t, int64(1024), pool.Stats().AllocatedDiskMB)
			case "ambiguous":
				execution.DeferRecovery()
				assert.Equal(t, before, pool.ListAllocations(), "uncertainty retains all capacity")
			}
			execution.DeferRecovery()
			require.False(t, execution.Valid())
			// Terminal or deferred ownership must not leave an immortal live pin.
			pool.Release(key)
			require.Empty(t, pool.ListAllocations())
		})
	}
}

func TestConservativeResourceEnvelopePreservesBothTopologies(t *testing.T) {
	old := []ResourceAllocation{
		{LeaseUUID: "old-only", Tenant: "tenant", SKU: "old", CPUCores: 1, MemoryMB: 1024, DiskMB: 2048},
		{LeaseUUID: "shared", Tenant: "tenant", SKU: "old", CPUCores: 2, MemoryMB: 512, DiskMB: 0},
	}
	next := []ResourceAllocation{
		{LeaseUUID: "new-only", Tenant: "tenant", SKU: "new", CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
		{LeaseUUID: "shared", Tenant: "tenant", SKU: "new", CPUCores: 1, MemoryMB: 1024, DiskMB: 1024},
	}
	envelope, err := ConservativeResourceEnvelope(old, next)
	require.NoError(t, err)
	require.Len(t, envelope, 3)
	assert.Equal(t, ResourceAllocation{LeaseUUID: "shared", Tenant: "tenant", SKU: "new", CPUCores: 2, MemoryMB: 1024, DiskMB: 1024}, envelope[2])
	next[1].Tenant = "someone-else"
	_, err = ConservativeResourceEnvelope(old, next)
	require.Error(t, err)
}
