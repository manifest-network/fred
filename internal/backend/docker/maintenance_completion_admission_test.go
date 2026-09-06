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
	activeProof, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
	require.NoError(t, err)
	require.NoError(t, h.b.callbackPublisher.PublishMaintenanceSuccessContext(
		context.Background(), activeProof,
	))
	pending, err := h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	completion := pending[0]

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
			ActiveOperationID:    active.OperationID,
			Items:                append([]backend.LeaseItem(nil), active.Items...),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(active.ResourceProfiles),
			ContainerIDs:         []string{"source-container"},
			ServiceContainers:    map[string][]string{"web": {"source-container"}},
		},
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

	assertBlocked(h.b.Restart(t.Context(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   h.leaseUUID,
		CallbackURL: active.RuntimeAuthority.LifecycleCallbackURL(),
	}))
	assertBlocked(h.b.Update(t.Context(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   h.leaseUUID,
		CallbackURL: active.RuntimeAuthority.LifecycleCallbackURL(),
		Payload:     active.Manifest,
	}))
	desired := append([]backend.LeaseItem(nil), active.Items...)
	desired[0].CustomDomain = "tenant.example.org"
	assertBlocked(h.b.ReconcileCustomDomain(t.Context(), h.leaseUUID, desired))
	assert.Empty(t, h.b.provisions[h.leaseUUID].Items[0].CustomDomain,
		"refused custom-domain admission must not commit desired state")

	pending, err = h.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, completion.DeliveryID, pending[0].DeliveryID)
	acknowledgePendingCallbacksForTest(t, h.callbacks)

	require.NoError(t, h.b.Restart(t.Context(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
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

func TestExactMaintenanceReplayNeverRepeatsBackendMutation(t *testing.T) {
	for name, kind := range map[string]shared.MaintenanceIntentKind{
		"restart": shared.MaintenanceIntentRestart,
		"update":  shared.MaintenanceIntentUpdate,
	} {
		t.Run(name, func(t *testing.T) {
			h := newMaintenanceRecoveryHarnessForKind(t, kind)
			requestID := h.intent.MaintenanceID()
			callbackURL := h.intent.LifecycleCallbackURL()
			payload := []byte(nil)
			if kind == shared.MaintenanceIntentUpdate {
				payload = h.intent.TargetRelease().Manifest
			}
			invoke := func(callback string, body []byte) error {
				if kind == shared.MaintenanceIntentRestart {
					return h.b.Restart(t.Context(), backend.RestartRequest{
						MaintenanceID: requestID, LeaseUUID: h.leaseUUID, CallbackURL: callback,
					})
				}
				return h.b.Update(t.Context(), backend.UpdateRequest{
					MaintenanceID: requestID, LeaseUUID: h.leaseUUID,
					CallbackURL: callback, Payload: body,
				})
			}

			// A retry while the backend WAL is Pending joins the durable command;
			// it does not need mutable provision state and cannot append or route.
			releasesBefore, err := h.releases.List(h.leaseUUID)
			require.NoError(t, err)
			require.NoError(t, invoke(callbackURL, payload))
			releasesAfter, err := h.releases.List(h.leaseUUID)
			require.NoError(t, err)
			assert.Equal(t, releasesBefore, releasesAfter)
			assert.Empty(t, h.b.actors)

			// Model backend completion followed by provider loss of its response.
			// The permanent live-lease receipt must acknowledge the provider's
			// redispatch without touching Release history, actors, or Compose.
			h.appendTarget(true)
			activeProof, activateErr := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
			require.NoError(t, activateErr)
			require.NoError(t, h.b.callbackPublisher.PublishMaintenanceSuccessContext(
				context.Background(), activeProof,
			))
			releasesBefore, err = h.releases.List(h.leaseUUID)
			require.NoError(t, err)
			require.NoError(t, invoke(callbackURL, payload))
			releasesAfter, err = h.releases.List(h.leaseUUID)
			require.NoError(t, err)
			assert.Equal(t, releasesBefore, releasesAfter)
			assert.Empty(t, h.b.actors)

			// Reusing the same opaque ID with changed wire authority is a
			// conflict even after completion and before mutable lease reads.
			assert.ErrorIs(t,
				invoke(callbackURL+"&divergent=1", append(payload, 'x')),
				backend.ErrInvalidState,
			)
		})
	}
}

func TestLateCompletedUpdateReplayCannotReinstallSupersededPayload(t *testing.T) {
	h := newMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	firstID := h.intent.MaintenanceID()
	firstCallback := h.intent.LifecycleCallbackURL()
	firstPayload := h.intent.TargetRelease().Manifest

	h.appendTarget(true)
	firstProof, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
	require.NoError(t, err)
	require.NoError(t, h.b.callbackPublisher.PublishMaintenanceSuccessContext(
		context.Background(), firstProof,
	))
	acknowledgePendingCallbacksForTest(t, h.callbacks)

	active, source, err := h.b.maintenanceSettlement.ClaimLatestActive(h.leaseUUID)
	require.NoError(t, err)
	secondPayload := append(append([]byte(nil), active.Manifest...), ' ')
	target := active
	target.Version = 0
	target.Status = "deploying"
	target.MaintenanceID = shared.MaintenanceID{}
	target.Manifest = secondPayload
	target.CreatedAt = time.Now()
	secondRequest, err := h.b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), shared.MaintenanceIntentUpdate, h.leaseUUID,
		firstCallback, secondPayload,
	)
	require.NoError(t, err)
	secondCandidate, err := h.b.maintenanceSettlement.NewMaintenanceIntentCandidate(
		secondRequest, source, target,
	)
	require.NoError(t, err)
	secondAdmission, err := h.b.maintenanceSettlement.BeginMaintenanceIntent(secondCandidate)
	require.NoError(t, err)
	secondAppend, err := h.b.maintenanceSettlement.StartMaintenanceAppend(
		createdTestMaintenanceDispatch(t, secondAdmission),
	)
	require.NoError(t, err)
	secondTarget, err := h.b.maintenanceSettlement.AppendMaintenance(secondAppend)
	require.NoError(t, err)
	secondTarget, err = h.b.maintenanceSettlement.BindMaintenanceIntentTarget(secondTarget)
	require.NoError(t, err)
	secondProof, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, secondTarget)
	require.NoError(t, err)
	require.NoError(t, h.b.callbackPublisher.PublishMaintenanceSuccessContext(
		context.Background(), secondProof,
	))
	acknowledgePendingCallbacksForTest(t, h.callbacks)

	historyBefore, err := h.releases.List(h.leaseUUID)
	require.NoError(t, err)
	err = h.b.Update(t.Context(), backend.UpdateRequest{
		MaintenanceID: firstID,
		LeaseUUID:     h.leaseUUID,
		CallbackURL:   firstCallback,
		Payload:       firstPayload,
	})
	require.ErrorIs(t, err, backend.ErrInvalidState)
	historyAfter, listErr := h.releases.List(h.leaseUUID)
	require.NoError(t, listErr)
	assert.Equal(t, historyBefore, historyAfter)
	assert.Empty(t, h.b.actors,
		"superseded replay must be refused before actor or Compose mutation")
}
