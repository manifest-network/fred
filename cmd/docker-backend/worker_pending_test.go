package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// Embed the consumer interfaces to make an unexpected fixture call fail. Only
// the actor's admitted provision/drain path is supplied by this fixture.
type pendingWorkerStore struct {
	leasesm.LeaseProvisionStore
	mu    sync.Mutex
	state leasesm.ProvisionState
}

func (store *pendingWorkerStore) LookupStatus(string) (backend.ProvisionStatus, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.Status, true
}
func (store *pendingWorkerStore) Exists(string) bool { return true }
func (store *pendingWorkerStore) UpdateFn(_ string, update func(*leasesm.ProvisionState)) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	update(&store.state)
	return true
}

type pendingWorkerCapabilities struct {
	leasesm.InstanceInspector
	leasesm.DiagnosticsGatherer
	leasesm.SMMetrics
}

func (pendingWorkerCapabilities) SMTransition(string, string, string) {}
func (pendingWorkerCapabilities) ActorCreated()                       {}

func actorLifecyclePendingError(t *testing.T) error {
	t.Helper()
	return journalPendingError(t, func(operations *shared.OperationSettlement, _ *shared.MaintenanceSettlement, candidate shared.OperationReleaseCandidate) error {
		pool := shared.NewResourcePool(4, 4096, 8192, func(string) (shared.SKUProfile, error) {
			return shared.SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
		}, nil)
		admission, err := operations.ReserveProvisionResources(pool, candidate.Intent())
		require.NoError(t, err)
		recovery, err := shared.NewRecoveryCoordinator(shared.RecoveryCoordinatorConfig{
			ExcludeLease: func(context.Context, string, func() error) (bool, error) {
				return false, errors.New("unexpected recovery")
			},
		})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		var workers sync.WaitGroup
		started, complete := make(chan struct{}), make(chan struct{})
		capabilities := pendingWorkerCapabilities{}
		actor, err := leasesm.NewLeaseActor(leasesm.LeaseActorConfig{
			LeaseUUID: handlerTestLeaseUUID, StopCtx: ctx, WG: &workers, Logger: slog.Default(),
			Inspector: capabilities, Diag: capabilities, Metrics: capabilities,
			ProvisionStore:  &pendingWorkerStore{state: leasesm.ProvisionState{LeaseUUID: handlerTestLeaseUUID, Status: backend.ProvisionStatusProvisioning}},
			RecoveryLineage: recovery.Lineage(),
			OnTerminated:    func(string, *leasesm.LeaseActor) {},
			ProvisionWorkFn: func(_ context.Context, execution shared.ProvisionResourceExecution) leasesm.ProvisionWorkOutcome {
				close(started)
				<-complete
				outcome, err := leasesm.NewProvisionWorkAmbiguous(errors.New("fixture preserves recovery"), execution.Operation())
				require.NoError(t, err)
				return outcome
			},
			RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) leasesm.ReplaceWorkOutcome {
				panic("unexpected restore")
			},
			MaintenanceWorkFn: func(shared.MaintenanceWorkerLifetime, shared.MaintenanceReleaseClaim) leasesm.ReplaceWorkOutcome {
				panic("unexpected maintenance")
			},
			PersistDiagnosticsFn:     func(shared.DiagnosticEntry, []string, map[string]string) {},
			SendOperationSuccessFn:   func(shared.OperationReleaseCommitted) {},
			SendOperationFailureFn:   func(shared.OperationReleaseUncommitted, string) {},
			SendLifecycleFailureFn:   func(shared.RuntimeGenerationProof, string) {},
			SendMaintenanceSuccessFn: func(shared.MaintenanceReleaseActive) {},
			SendMaintenanceFailureFn: func(shared.MaintenanceReleaseFailure, string) {},
			DoDeprovisionFn:          func(context.Context, leasesm.ActorCloseScope) error { panic("teardown before worker drain") },
		}, func(*leasesm.LeaseActor) {})
		require.NoError(t, err)
		defer func() { close(complete); cancel(); workers.Wait() }()
		command, reply, err := leasesm.NewProvisionCommand(ctx, admission)
		require.NoError(t, err)
		require.True(t, actor.TryEnqueueCommand(command))
		require.NoError(t, <-reply.Result())
		<-started
		command, reply, err = leasesm.NewDeprovisionCommand(t.Context())
		require.NoError(t, err)
		require.True(t, actor.TryEnqueueCommand(command))
		pending := <-reply.Result()
		require.True(t, leasesm.IsLifecyclePending(pending), pending)
		return pending
	})
}
