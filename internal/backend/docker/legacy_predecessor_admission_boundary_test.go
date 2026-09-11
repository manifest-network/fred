package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// The adversarial recovery fixture can deliberately issue Started directly.
// Normal provision admission cannot: the pool must first mint the consumed
// resource capability required by the actor/physical worker. A raw adopted
// predecessor has no tenant/sizing authority from which to mint that capability.
func TestItemslessLegacyPredecessorCannotAdmitProvisionExecution(t *testing.T) {
	spec := dockerOperationIntentSpec(t, backendidentity.ID{})
	b, stores := newItemslessV013PredecessorRecoveryFixture(t, spec, &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	})
	claim := createdDockerOperationClaim(t, beginOperationIntentForSettlementTest(t, stores.operations, spec))
	require.Equal(t, shared.OperationExecutionBeforeEffects, claim.ExecutionPhase())
	before, err := stores.releases.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.Empty(t, before.Items)
	require.Empty(t, before.ResourceProfiles)
	require.Nil(t, before.LegacyRuntimeAuthority)

	admission, err := stores.operations.ReserveProvisionResources(b.pool, claim)
	require.ErrorContains(t, err, "provision predecessor resource authority differs from tenant")
	assert.False(t, admission.Valid())
	execution, err := admission.Begin()
	require.Error(t, err)
	assert.False(t, execution.Valid(), "ignoring the refusal cannot yield physical provision authority")
	assert.Empty(t, b.pool.ListAllocations())
	assert.Empty(t, b.provisions)
	claims, err := stores.operations.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, shared.OperationExecutionBeforeEffects, claims[0].ExecutionPhase())

	// A capacity hold cannot provide the missing operator cleanup authority.
	// Neither public close shape may substitute the candidate's smaller items
	// for a predecessor whose original footprint is no longer reconstructible.
	hold := b.pool.HoldUnaccountedFootprint()
	defer hold.Release()
	closeRequest, err := stores.close.NewCloseRequest(spec.LeaseUUID, false)
	require.NoError(t, err)
	_, err = stores.close.BeginClose(closeRequest)
	require.ErrorContains(t, err, "has no complete runtime authority")
	cleanupRequest, err := stores.close.NewCleanupCloseRequest(spec.LeaseUUID)
	require.NoError(t, err)
	_, err = stores.close.BeginCleanupClose(cleanupRequest)
	require.ErrorContains(t, err, "has no complete runtime authority")
	after, err := stores.releases.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}
