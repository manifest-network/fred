//go:build integration

package chain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// TestProviderd_Lifecycle takes a lease from creation to close through a whole
// provider. A tenant creates the lease on the chain and uploads its manifest;
// providerd must deploy it on the backend and acknowledge it on the chain.
// The tenant then closes the lease on the chain, and providerd must remove
// the workload.
func TestProviderd_Lifecycle(t *testing.T) {
	p := StartProvider(t)
	tenant := p.NewTenant(t)

	lease := tenant.Deploy(t, p.SKUs.Stateless, map[string]any{
		"image":   e2e.BusyboxImage,
		"command": []string{"sleep", "3600"},
	})
	// Only the provider's acknowledgement makes a lease active, and providerd
	// sends it only after the backend reported the workload ready. The budget
	// covers a first image admission.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		onChain := p.Chain.Lease(t, lease)
		if onChain.State == LeaseActive {
			break
		}
		// Pending is the only state on the way to active.
		require.Equal(t, LeasePending, onChain.State, "the lease left pending without becoming active: %+v", onChain)
		require.True(t, time.Now().Before(deadline), "the lease is still pending after 3 minutes")
		time.Sleep(500 * time.Millisecond)
	}
	onBackend, err := p.Backend.Client.Get(t.Context(), lease)
	require.NoError(t, err)
	require.Equal(t, "ready", onBackend.Status)
	containers := e2e.LeaseContainers(t, lease)
	require.Len(t, containers, 1)
	require.True(t, containers[0].State.Running, "container state: %s", containers[0].State.Status)

	tenant.Close(t, lease)
	require.Equal(t, LeaseClosed, p.Chain.Lease(t, lease).State)
	// A lease without a volume leaves nothing behind once providerd has acted
	// on the close.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, err := p.Backend.Client.Get(t.Context(), lease)
		assert.ErrorIs(collect, err, e2e.ErrNotFound)
	}, time.Minute, 500*time.Millisecond)
	require.Empty(t, e2e.LeaseContainers(t, lease))
}
