package placement

import (
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"
)

func TestRestoreSourceEligibilityClassifiesEveryChainState(t *testing.T) {
	t.Parallel()
	want := map[billingtypes.LeaseState]bool{
		billingtypes.LEASE_STATE_UNSPECIFIED: false,
		billingtypes.LEASE_STATE_PENDING:     false,
		billingtypes.LEASE_STATE_ACTIVE:      false,
		billingtypes.LEASE_STATE_CLOSED:      true,
		billingtypes.LEASE_STATE_EXPIRED:     true,
		billingtypes.LEASE_STATE_REJECTED:    true,
	}
	// A ledger upgrade must explicitly classify each new state before admission
	// or recovery policy can change; unknown wire values still fail closed.
	require.Len(t, want, len(billingtypes.LeaseState_name))
	for number, name := range billingtypes.LeaseState_name {
		t.Run(name, func(t *testing.T) {
			state := billingtypes.LeaseState(number)
			eligible, classified := want[state]
			require.True(t, classified, "classify new chain state %s", name)
			observation := observedExactLease{lease: billingtypes.Lease{State: state}}
			require.Equal(t, eligible, observation.mayHoldRetainedData())
		})
	}
	require.False(t, (observedExactLease{}).mayHoldRetainedData())
	for _, state := range []billingtypes.LeaseState{-1, 99} {
		require.False(t, (observedExactLease{lease: billingtypes.Lease{State: state}}).mayHoldRetainedData())
	}
}
