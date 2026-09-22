package shared

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionOrphanPrunerExposesNoCallerSelectedTarget(t *testing.T) {
	_, rawDeleteExposed := reflect.TypeFor[*RetentionStore]().MethodByName("PruneOrphanedActive")
	assert.False(t, rawDeleteExposed, "the store must not expose orphan deletion")

	sweep, ok := reflect.TypeFor[*RetentionOrphanPruner]().MethodByName("Sweep")
	require.True(t, ok)
	require.Equal(t, 2, sweep.Type.NumIn(), "Sweep accepts only receiver and context")
	assert.Equal(t, reflect.TypeFor[context.Context](), sweep.Type.In(1))
	for index := 0; index < reflect.TypeFor[RetentionOrphanSweepResult]().NumField(); index++ {
		field := reflect.TypeFor[RetentionOrphanSweepResult]().Field(index)
		assert.NotEqual(t, reflect.TypeFor[ActiveRetentionCandidate](), field.Type)
		assert.NotEqual(t, reflect.TypeFor[ActiveRetentionProof](), field.Type)
	}
}

func TestRetentionOrphanPrunerDoesNotCarryStreakAcrossRowRevision(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, restoreClaim, source := restoreHandoffFixture(t, stores, "orphan-row-revision")
	inventory, err := BindRetentionOrphanVolumeInventory(func(context.Context) ([]string, error) {
		return nil, nil
	})
	require.NoError(t, err)
	pruner, err := NewRetentionOrphanPruner(stores.retentions, 2, true, inventory)
	require.NoError(t, err)

	first, err := pruner.Sweep(context.Background())
	require.NoError(t, err)
	require.Zero(t, first.Pruned)

	// Active -> Restoring -> Active rewrites the exact row generation under the
	// same lease UUID. The replacement has earned no absence observation yet.
	candidate, err := stores.restore.PrepareRestoreClaim(restoreClaim)
	require.NoError(t, err)
	restoring, err := stores.restore.ClaimForRestore(candidate, 0)
	require.NoError(t, err)
	_, err = stores.retentions.RollbackRestoring(restoring, restoring.Entry().ResourceProfiles)
	require.NoError(t, err)

	second, err := pruner.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, second.Pruned, "a new exact row revision starts at observation one")
	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, current)
	third, err := pruner.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, third.Pruned)
}

func TestRetentionOrphanPrunerRejectsZeroInventoryCapability(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, err := NewRetentionOrphanPruner(
		stores.retentions, 2, true, RetentionOrphanVolumeInventory{},
	)
	require.ErrorContains(t, err, "complete inventory")

	_, err = BindRetentionOrphanVolumeInventory(nil)
	require.ErrorContains(t, err, "complete listing operation")
}

func TestRetentionOrphanPrunerRejectsClosedStoreBeforeInventory(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	inventoryCalls := 0
	inventory, err := BindRetentionOrphanVolumeInventory(func(context.Context) ([]string, error) {
		inventoryCalls++
		return nil, nil
	})
	require.NoError(t, err)
	pruner, err := NewRetentionOrphanPruner(stores.retentions, 1, true, inventory)
	require.NoError(t, err)
	require.NoError(t, stores.retentions.Close())

	_, err = NewRetentionOrphanPruner(stores.retentions, 1, true, inventory)
	require.ErrorContains(t, err, "open identity-bound retention store")
	_, err = pruner.Sweep(context.Background())
	require.ErrorContains(t, err, "store is closed")
	assert.Zero(t, inventoryCalls, "a closed store must fail before physical inventory I/O")
}

type nilRetentionOrphanInventory struct{}

func (i *nilRetentionOrphanInventory) ListForProof(context.Context) ([]string, error) {
	if i == nil {
		panic("nil inventory receiver")
	}
	return nil, nil
}

func TestRetentionOrphanPrunerFailsClosedOnBoundTypedNilReceiver(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, _, source := restoreHandoffFixture(t, stores, "typed-nil-inventory")
	var sourceInventory *nilRetentionOrphanInventory
	inventory, err := BindRetentionOrphanVolumeInventory(sourceInventory.ListForProof)
	require.NoError(t, err, "a Go method value can be non-nil even when its receiver is nil")
	pruner, err := NewRetentionOrphanPruner(stores.retentions, 1, true, inventory)
	require.NoError(t, err)

	result, err := pruner.Sweep(context.Background())
	require.ErrorContains(t, err, "inventory panicked")
	assert.Equal(t, RetentionOrphanSkipInventoryError, result.SkipReason)
	current, getErr := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, getErr)
	require.NotNil(t, current, "an inventory panic must not consume retention authority")
}
