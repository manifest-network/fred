package docker

import (
	"context"
	"fmt"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// newLeaseActor constructs a docker-substrate lease actor by building the
// LeaseActorConfig closures around `b` and delegating to
// leasesm.NewLeaseActor. The factory keeps callsites
// (`actorForLocked`, tests) at the same shape they had pre-lift:
// `newLeaseActor(b, leaseUUID)` returns a *leasesm.LeaseActor whose
// run goroutine has already been spawned.
//
// Pattern matches PR4's seam-and-closure approach for PersistDiagnosticsFn /
// SendOperationCallbackFn and PR5b-1's Metrics
// adapter — each substrate-private operation is reached via a closure
// captured at construction, so the actor never holds a *Backend pointer
// of its own.
//
// DoDeprovisionFn was added at checkpoint A of PR5b-2 specifically so
// the handleDeprovision body, post-lift, can dispatch substrate-specific
// teardown without reaching into *Backend. The ctx threaded through
// MUST be the actor-owned ctx delivered with the inbound DeprovisionMsg
// (which carries the caller's ctx); the docker substrate honors this
// per the LeaseActorConfig.DoDeprovisionFn contract.
func newLeaseActor(b *Backend, leaseUUID string) *leasesm.LeaseActor {
	if b.recoveryCoordinator == nil {
		// Lightweight unit fixtures may construct actors before installing durable
		// settlements. Production always binds the complete coordinator in New.
		coordinator, err := shared.NewRecoveryCoordinator(shared.RecoveryCoordinatorConfig{
			ExcludeLease: b.withRecoveryLeaseExclusion,
		})
		if err != nil {
			panic(fmt.Sprintf("construct recovery coordinator for lease actor %q: %v", leaseUUID, err))
		}
		b.recoveryCoordinator = coordinator
	}
	actor, err := leasesm.NewLeaseActor(leasesm.LeaseActorConfig{
		LeaseUUID:         leaseUUID,
		Logger:            b.logger,
		StopCtx:           b.stopCtx,
		WG:                &b.wg,
		Inspector:         b.inspector,
		Diag:              b.gatherer,
		ProvisionStore:    b.provisionStore,
		Metrics:           dockerSMMetrics{},
		ProvisionWorkFn:   b.executeProvisionWork,
		RestoreWorkFn:     b.executeRestoreWork,
		MaintenanceWorkFn: b.executeMaintenanceWork,
		// OnTerminated closes over `a` so the registry-delete check
		// preserves its "only delete if I'm still the registered
		// actor for this UUID" semantics — equivalent to the
		// removeFromRegistry `reg == a` guard in the original
		// docker-side implementation. Without this check, a fresh
		// actor stored for the same UUID after our exit started
		// could be clobbered.
		OnTerminated: func(uuid string, terminated *leasesm.LeaseActor) {
			b.actorsMu.Lock()
			defer b.actorsMu.Unlock()
			if reg, ok := b.actors[uuid]; ok && reg == terminated {
				delete(b.actors, uuid)
			}
		},
		PersistDiagnosticsFn: func(entry shared.DiagnosticEntry, containerIDs []string, keys map[string]string) {
			b.persistDiagnostics(entry, containerIDs, keys)
		},
		SendOperationSuccessFn: func(
			committed shared.OperationReleaseCommitted,
		) {
			b.sendOperationSuccessWithURL(committed)
		},
		SendOperationFailureFn: func(
			proof shared.OperationReleaseUncommitted,
			errMsg string,
		) {
			b.sendOperationFailure(proof, errMsg)
		},
		SendLifecycleFailureFn: func(runtime shared.RuntimeGenerationProof, errMsg string) {
			b.sendLifecycleFailure(runtime, errMsg)
		},
		SendMaintenanceSuccessFn: func(
			active shared.MaintenanceReleaseActive,
		) {
			if err := b.resolveMaintenanceSuccess(active); err != nil {
				b.logger.Error("failed to settle maintenance success; durable intent retained for recovery",
					"lease_uuid", active.LeaseUUID(), "maintenance_id", active.MaintenanceID(), "error", err)
			}
		},
		SendMaintenanceFailureFn: func(
			failed shared.MaintenanceReleaseFailure,
			errMsg string,
		) {
			if err := b.resolveMaintenanceFailure(failed, errMsg); err != nil {
				b.logger.Error("failed to settle maintenance failure; durable intent retained for recovery",
					"lease_uuid", failed.LeaseUUID(), "maintenance_id", failed.MaintenanceID(), "error", err)
			}
		},
		RecoveryLineage: b.recoveryCoordinator.Lineage(),
		DoDeprovisionFn: func(ctx context.Context, scope leasesm.ActorCloseScope) error {
			return b.doDeprovision(ctx, scope)
		},
	}, func(actor *leasesm.LeaseActor) {
		b.actors[leaseUUID] = actor
	})
	if err != nil {
		panic(fmt.Sprintf("construct lease actor %q: %v", leaseUUID, err))
	}
	return actor
}
