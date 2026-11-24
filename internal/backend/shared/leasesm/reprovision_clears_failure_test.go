package leasesm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// A re-provision must replace the failed attempt's verdict the moment it is
// admitted, before its worker has done anything. A lease that kept its old
// error while provisioning would show a tenant the previous failure as the
// state of the new attempt.
func TestReprovisionClearsThePreviousFailureOnEnteringProvisioning(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusFailed,
		LastError: "container exited with status 1",
		Reason:    backend.ReasonContainerExited,
		Message:   "container exited during startup",
	})
	actor := newTestActor(t, "lease-1", testActorOpts{ProvisionStore: store})

	require.NoError(t, actor.sm.requestProvision(context.Background(), shared.OperationIntentClaim{}))

	state, ok := store.Get("lease-1")
	require.True(t, ok)
	require.Equal(t, backend.ProvisionStatusProvisioning, state.Status)
	require.Empty(t, state.LastError)
	require.Empty(t, state.Reason)
	require.Empty(t, state.Message)
}
