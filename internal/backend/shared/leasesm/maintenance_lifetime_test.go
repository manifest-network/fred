package leasesm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func testMaintenanceHandoff(t *testing.T, shutdown context.Context) shared.MaintenanceWorkerHandoff {
	t.Helper()
	handoff, cancel := shared.NewMaintenanceWorkerHandoff(shutdown, 10*time.Minute)
	t.Cleanup(cancel)
	return handoff
}

func testMaintenanceLifetime(t *testing.T, shutdown context.Context) shared.MaintenanceWorkerLifetime {
	t.Helper()
	lifetime, err := testMaintenanceHandoff(t, shutdown).ClaimWorker()
	require.NoError(t, err)
	t.Cleanup(lifetime.Cancel)
	return lifetime
}

func maintenanceMessageForLifetimeTest(op string, h shared.MaintenanceWorkerHandoff, claim shared.MaintenanceIntentClaim, target shared.MaintenanceReleaseClaim, ack chan error) leaseMessage {
	if op == "update" {
		return updateRequestedMsg{Lifetime: h, CallbackURL: claim.CallbackURL(), LifecycleCallbackURL: claim.LifecycleCallbackURL(), Maintenance: claim, Target: target, Ack: ack}
	}
	return restartRequestedMsg{Lifetime: h, CallbackURL: claim.CallbackURL(), LifecycleCallbackURL: claim.LifecycleCallbackURL(), Maintenance: claim, Target: target, Ack: ack}
}

// A claimed-then-rejected (409) handoff, a pre-claim rejection on a
// terminated actor, and a completed worker all release the backend-owned lifetime.
func TestMaintenanceLifetimeIsReleasedOnEveryExit(t *testing.T) {
	for _, op := range []string{"restart", "update"} {
		t.Run(op, func(t *testing.T) {
			kind := shared.MaintenanceIntentRestart
			if op == "update" {
				kind = shared.MaintenanceIntentUpdate
			}
			firstClaim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
			secondClaim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
			store := newMockProvisionStore()
			store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			finish := sync.OnceFunc(func() { close(release) })
			defer finish()
			firstSuccess := testMaintenanceSuccess(t, firstClaim, ReplaceSuccessProjection{})
			actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
				StopCtx: ctx, ProvisionStore: store,
				MaintenanceWorkFn: func(shared.MaintenanceWorkerLifetime, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
					<-release
					return replaceWorkTerminal{result: firstSuccess}
				},
			})
			h1, discard1 := shared.NewMaintenanceWorkerHandoff(context.Background(), time.Hour)
			defer discard1()
			ack1 := make(chan error, 1)
			require.True(t, actor.tryEnqueue(maintenanceMessageForLifetimeTest(op, h1, firstClaim, testMaintenanceTarget(t, firstClaim), ack1)))
			require.NoError(t, <-ack1)
			discard1() // a lost HTTP reply must not revoke the claimed worker
			require.NoError(t, h1.TargetContext().Err(), "accepted worker lifetime must survive caller discard")

			h2, discard2 := shared.NewMaintenanceWorkerHandoff(context.Background(), time.Hour)
			defer discard2()
			ack2 := make(chan error, 1)
			require.True(t, actor.tryEnqueue(maintenanceMessageForLifetimeTest(op, h2, secondClaim, testMaintenanceTarget(t, secondClaim), ack2)))
			require.ErrorIs(t, <-ack2, backend.ErrInvalidState)
			require.Eventually(t, func() bool { return h2.TargetContext().Err() == context.Canceled }, time.Second, time.Millisecond, "claimed-then-rejected lifetime must be released by the actor")

			finish()
			require.Eventually(t, func() bool { return h1.TargetContext().Err() != nil }, 3*time.Second, 5*time.Millisecond,
				"completed worker must release its lifetime")

			terminated := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: store})
			terminated.terminated = true
			h3, discard3 := shared.NewMaintenanceWorkerHandoff(context.Background(), time.Hour)
			defer discard3()
			ack3 := make(chan error, 1)
			msg := maintenanceMessageForLifetimeTest(op, h3, secondClaim, testMaintenanceTarget(t, secondClaim), ack3)
			switch m := msg.(type) {
			case restartRequestedMsg:
				terminated.handleRestartRequested(m)
			case updateRequestedMsg:
				terminated.handleUpdateRequested(m)
			}
			require.Error(t, <-ack3)
			require.ErrorIs(t, h3.TargetContext().Err(), context.Canceled, "pre-claim actor rejection must discard the handoff itself")
		})
	}
}

func TestMaintenanceLifetimeIsReleasedBeforeActorConsumesCompletion(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	success := testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		MaintenanceWorkFn: func(shared.MaintenanceWorkerLifetime, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			return replaceWorkTerminal{result: success}
		},
	})
	actor.pendingMaintenance = claim
	handoff, discard := shared.NewMaintenanceWorkerHandoff(t.Context(), time.Hour)
	defer discard()
	worker, err := handoff.ClaimWorker()
	require.NoError(t, err)
	defer worker.Cancel()
	actor.spawnMaintenanceWorker(worker, testMaintenanceTarget(t, claim))
	actor.cfg.WG.Wait()
	require.ErrorIs(t, handoff.TargetContext().Err(), context.Canceled,
		"worker completion must release its lifetime even before the actor handles the queued terminal event")
}
