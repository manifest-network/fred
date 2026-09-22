package shared

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestFailedOperationPredecessorRejectsCrossStoreAndStaleHeads(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	other := openOperationHandoffStores(t, "docker-b")
	spec, active, failed := settleActivePredecessorThenFailedSuccessorForClose(
		t, stores, "failed-predecessor-stale",
	)
	head := operationLeaseMutationHead{claim: failed}

	unlock := stores.callbacks.lockDeliveryLease(spec.LeaseUUID)
	relation, err := bindFailedOperationOverRelease(
		stores.callbacks, stores.releases, head, active,
	)
	unlock()
	require.NoError(t, err)
	require.Equal(t, operationFailurePredecessorActive, relation.record().Kind)

	_, err = bindFailedOperationOverRelease(
		other.callbacks, other.releases, head, active,
	)
	require.ErrorContains(t, err, "another journal")

	closeSettlement := newCloseSettlementForTest(t, stores)
	request, err := closeSettlement.NewCloseRequest(spec.LeaseUUID, false)
	require.NoError(t, err)
	_, err = closeSettlement.BeginClose(request)
	require.NoError(t, err)

	unlock = stores.callbacks.lockDeliveryLease(spec.LeaseUUID)
	_, err = bindFailedOperationOverRelease(
		stores.callbacks, stores.releases, head, active,
	)
	unlock()
	require.ErrorContains(t, err, "successor")
}

func TestFailedOperationPredecessorMustBeRemintedAfterReleaseJournalReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec, oldActive, failed := settleActivePredecessorThenFailedSuccessorForClose(
		t, stores, "failed-predecessor-reopen",
	)
	head := operationLeaseMutationHead{claim: failed}

	require.NoError(t, stores.releases.Close())
	stores.releases = nil
	reopened, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.releases = reopened
	_, freshActive, err := reopened.claimLatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	assert.Equal(t, oldActive.Version(), freshActive.Version())
	assert.Equal(t, oldActive.Digest(), freshActive.Digest())

	unlock := stores.callbacks.lockDeliveryLease(spec.LeaseUUID)
	_, err = bindFailedOperationOverRelease(
		stores.callbacks, reopened, head, oldActive,
	)
	require.ErrorContains(t, err, "another journal")
	relation, freshErr := bindFailedOperationOverRelease(
		stores.callbacks, reopened, head, freshActive,
	)
	unlock()
	require.NoError(t, freshErr)
	assert.Equal(t, freshActive.Version(), relation.predecessor.Version())
}

func TestOperationFailureCannotSealPredecessorFromAnotherPrincipal(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	predecessor := seedCloseSettlementRelease(t, stores, "failed-predecessor-principal")
	successor := predecessor
	successor.Tenant = "tenant-b"
	successor.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	var err error
	successor.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(
		successor.CallbackURL, "",
	)
	require.NoError(t, err)
	claim := beginHandoffOperation(t, stores.settlement, successor)
	uncommitted := commitHandoffRefusal(t, stores.settlement, claim)

	_, err = stores.settlement.resolveOperationFailure(uncommitted, "definitive refusal")
	require.ErrorContains(t, err, "different principal authority")
	states, listErr := stores.settlement.ListOperationIntents()
	require.NoError(t, listErr)
	require.Len(t, states, 1)
	assert.Equal(t, operationIntentPending, states[0].entry.State)
}
