package docker

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestMaintenanceCommandsWaitForPriorCompletionDelivery(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	require.NoError(t, h.releases.ActivateMaintenance(h.target))
	completion, err := h.callbacks.ResolveMaintenanceIntent(
		h.intent, backend.CallbackStatusSuccess, "",
	)
	require.NoError(t, err)

	active, err := h.releases.LatestActive(h.leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	var stack manifest.StackManifest
	require.NoError(t, json.Unmarshal(active.Manifest, &stack))
	h.b.provisions[h.leaseUUID] = &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            h.leaseUUID,
			Tenant:               active.RuntimeAuthority.Tenant(),
			ProviderUUID:         active.RuntimeAuthority.ProviderUUID(),
			Status:               backend.ProvisionStatusReady,
			StackManifest:        &stack,
			CallbackURL:          active.RuntimeAuthority.CallbackURL(),
			LifecycleCallbackURL: active.RuntimeAuthority.LifecycleCallbackURL(),
			Items:                append([]backend.LeaseItem(nil), active.Items...),
			ContainerIDs:         []string{"source-container"},
			ServiceContainers:    map[string][]string{"web": {"source-container"}},
		},
		ResourceProfiles: shared.CloneSKUResourceSnapshot(active.ResourceProfiles),
	}
	h.b.cfg.Ingress = IngressConfig{
		Enabled:        true,
		WildcardDomain: "backend.example.net",
		Entrypoint:     "websecure",
	}
	h.b.customDomainDNSReady = func(context.Context, string) bool { return true }

	var composeMutations atomic.Int32
	upStarted := make(chan struct{})
	h.b.compose = &mockComposeExecutor{
		UpFn: func(ctx context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			if composeMutations.Add(1) == 1 {
				close(upStarted)
			}
			<-ctx.Done()
			return ctx.Err()
		},
		DownFn: func(context.Context, string, time.Duration) error {
			composeMutations.Add(1)
			return nil
		},
	}

	historyBefore, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	assertBlocked := func(err error) {
		t.Helper()
		require.ErrorIs(t, err, backend.ErrInvalidState)
		assert.ErrorContains(t, err, "previous maintenance completion")
		history, listErr := h.releases.List(h.leaseUUID)
		require.NoError(t, listErr)
		assert.Len(t, history, len(historyBefore),
			"refused admission must not append a target release")
		assert.Zero(t, composeMutations.Load(),
			"refused admission must not mutate the Compose substrate")
		assert.Empty(t, h.b.actors,
			"refused admission must not enqueue work on the lease actor")
		assert.Equal(t, backend.ProvisionStatusReady, h.b.provisions[h.leaseUUID].Status)
	}

	assertBlocked(h.b.Restart(t.Context(), backend.RestartRequest{
		LeaseUUID:   h.leaseUUID,
		CallbackURL: active.RuntimeAuthority.LifecycleCallbackURL(),
	}))
	assertBlocked(h.b.Update(t.Context(), backend.UpdateRequest{
		LeaseUUID:   h.leaseUUID,
		CallbackURL: active.RuntimeAuthority.LifecycleCallbackURL(),
		Payload:     active.Manifest,
	}))
	desired := append([]backend.LeaseItem(nil), active.Items...)
	desired[0].CustomDomain = "tenant.example.org"
	assertBlocked(h.b.ReconcileCustomDomain(t.Context(), h.leaseUUID, desired))
	assert.Empty(t, h.b.provisions[h.leaseUUID].Items[0].CustomDomain,
		"refused custom-domain admission must not commit desired state")

	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, completion.DeliveryID, pending[0].DeliveryID)
	require.NoError(t, h.callbacks.RemoveEntry(completion))

	require.NoError(t, h.b.Restart(t.Context(), backend.RestartRequest{
		LeaseUUID:   h.leaseUUID,
		CallbackURL: active.RuntimeAuthority.LifecycleCallbackURL(),
	}))
	select {
	case <-upStarted:
	case <-time.After(time.Second):
		t.Fatal("restart did not reach the substrate after precise completion removal")
	}
	historyAfter, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	assert.Len(t, historyAfter, len(historyBefore)+1,
		"precise completion removal must release maintenance admission")

	h.b.stopCancel()
	h.b.wg.Wait()
}
