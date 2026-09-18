package operation

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bindRuntimeForTest(t *testing.T, registry *Registry) RuntimeController {
	t.Helper()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	runtime := authority.RuntimeController()
	require.True(t, runtime.valid())
	return runtime
}

func TestRuntimeControllerZeroAndForeignValuesGrantNoObservationOrControl(t *testing.T) {
	registry := newTestRegistry()
	foreign := newTestRegistry()
	authority, err := registry.BindSettlementAuthority()
	require.NoError(t, err)
	foreignAuthority, err := foreign.BindSettlementAuthority()
	require.NoError(t, err)
	requireStarted(t, registry, testTrackSpec("lease-1"))

	for _, runtime := range []RuntimeController{
		{},
		{registry: registry, marker: foreignAuthority.marker},
	} {
		assert.False(t, runtime.Contains("lease-1"))
		assert.Zero(t, runtime.Count())
		assert.Zero(t, runtime.PendingWorkCount())
		assert.Zero(t, runtime.WaitForDrain(context.Background(), time.Nanosecond))
		assert.Nil(t, runtime.PendingLeaseUUIDs())
		assert.NotPanics(t, runtime.BeginDrain)
	}

	assert.True(t, authority.RuntimeController().Contains("lease-1"))
}

func TestRuntimeControllerObservesDetachedOperationAndClaimUnion(t *testing.T) {
	registry := newTestRegistry()
	runtime := bindRuntimeForTest(t, registry)
	requireStarted(t, registry, testTrackSpec("lease-operation"))
	claim := registry.tryClaimLeaseNow("lease-action")
	require.True(t, claim.Acquired())
	t.Cleanup(func() { registry.releaseLease(claim.Claim()) })

	assert.True(t, runtime.Contains("lease-operation"))
	assert.False(t, runtime.Contains("lease-action"),
		"Contains deliberately reports operations, not transient action claims")
	assert.Equal(t, 1, runtime.Count())
	assert.Equal(t, 2, runtime.PendingWorkCount())

	leases := runtime.PendingLeaseUUIDs()
	sort.Strings(leases)
	assert.Equal(t, []string{"lease-action", "lease-operation"}, leases)
	leases[0] = "mutated"
	fresh := runtime.PendingLeaseUUIDs()
	sort.Strings(fresh)
	assert.Equal(t, []string{"lease-action", "lease-operation"}, fresh)
}
