//go:build integration

package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProviderd_Lifecycle takes a lease from creation to close through a whole
// provider. A tenant creates the lease on the chain and uploads its manifest;
// providerd must deploy it on the backend and acknowledge it on the chain.
// The tenant then closes the lease on the chain, and providerd must remove
// the workload.
func TestProviderd_Lifecycle(t *testing.T) {
	p := StartProvider(t, 1)
	backend := p.Backends[0]
	tenant := p.NewTenant(t)

	lease := tenant.Deploy(t, p.SKUs.Stateless, sleeper("3600"))
	// Only the provider's acknowledgement makes a lease active, and providerd
	// sends it only after the backend reported the workload ready.
	requireActive(t, p.Chain, lease)
	requireStatus(t, backend, lease, "ready")
	running(t, backend, lease)
	deployed, err := backend.Client.Get(t.Context(), lease)
	require.NoError(t, err)
	require.Equal(t, tenant.Address(), deployed.Tenant, "the backend must hold the lease for its tenant")

	tenant.Close(t, lease)
	require.Equal(t, LeaseClosed, p.Chain.Lease(t, lease).State)
	// A lease without a volume leaves nothing behind once providerd has acted
	// on the close.
	requireGone(t, backend, lease)
}
