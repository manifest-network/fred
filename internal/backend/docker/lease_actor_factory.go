package docker

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
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
// SendOperationCallbackFn / PersistDiagnosticsWithLogsFn and PR5b-1's Metrics
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
	return leasesm.NewLeaseActor(func(a *leasesm.LeaseActor) leasesm.LeaseActorConfig {
		return leasesm.LeaseActorConfig{
			LeaseUUID:      leaseUUID,
			Logger:         b.logger,
			StopCtx:        b.stopCtx,
			WG:             &b.wg,
			Inspector:      b.inspector,
			Diag:           b.gatherer,
			CallbackSender: b.callbackSender,
			ProvisionStore: b.provisionStore,
			Metrics:        dockerSMMetrics{},
			// OnTerminated closes over `a` so the registry-delete check
			// preserves its "only delete if I'm still the registered
			// actor for this UUID" semantics — equivalent to the
			// removeFromRegistry `reg == a` guard in the original
			// docker-side implementation. Without this check, a fresh
			// actor stored for the same UUID after our exit started
			// could be clobbered.
			OnTerminated: func(uuid string) {
				b.actorsMu.Lock()
				defer b.actorsMu.Unlock()
				if reg, ok := b.actors[uuid]; ok && reg == a {
					delete(b.actors, uuid)
				}
			},
			PersistDiagnosticsFn: func(entry shared.DiagnosticEntry, containerIDs []string, keys map[string]string) {
				b.persistDiagnostics(entry, containerIDs, keys)
			},
			PersistDiagnosticsWithLogsFn: func(entry shared.DiagnosticEntry, logs map[string]string) {
				b.persistDiagnosticsWithLogs(entry, logs)
			},
			SendOperationCallbackFn: func(uuid, url string, status backend.CallbackStatus, errMsg string) {
				// Provision/restore operation completions never carry the
				// deprovision retain-success flag.
				b.sendOperationCallbackWithURL(uuid, url, status, errMsg)
			},
			SendLifecycleCallbackFn: func(uuid, url string, status backend.CallbackStatus, errMsg string) {
				if url == "" {
					b.provisionsMu.RLock()
					var completionURL string
					if provision, exists := b.provisions[uuid]; exists {
						completionURL = provision.CallbackURL
					}
					b.provisionsMu.RUnlock()
					resolved, err := backend.ResolveLifecycleCallbackURL(completionURL, "")
					if err != nil {
						b.logger.Error("cannot derive lifecycle callback URL; suppressing observational callback",
							"lease_uuid", uuid, "error", err)
						return
					}
					url = resolved
				}
				b.sendLifecycleCallbackWithURL(uuid, url, status, errMsg, false)
			},
			SendMaintenanceCallbackFn: func(claim shared.MaintenanceIntentClaim, status backend.CallbackStatus, errMsg string) {
				if b.callbackSender == nil {
					b.logger.Error("cannot settle maintenance callback without durable sender",
						"lease_uuid", claim.LeaseUUID())
					return
				}
				if err := b.callbackSender.SendMaintenanceCallback(claim, status, errMsg); err != nil {
					b.logger.Error("failed to settle maintenance callback; durable intent retained for recovery",
						"lease_uuid", claim.LeaseUUID(), "maintenance_id", claim.MaintenanceID(), "error", err)
				}
			},
			DoDeprovisionFn: func(ctx context.Context, leaseUUID string) error {
				return b.doDeprovision(ctx, leaseUUID)
			},
		}
	})
}
