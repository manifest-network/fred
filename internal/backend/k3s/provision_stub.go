package k3s

import (
	"context"
	"fmt"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// stubProvisionerErrMsg is the canonical "not implemented" string sent
// in every failure callback (and persisted as LastError in diagnostics)
// for ENG-133. ENG-134+ replaces this with real K8s provisioning errors.
const stubProvisionerErrMsg = "not implemented"

// Provision validates the request, records a "provisioning" in-memory
// entry, and spawns a goroutine that flips the entry to "failed" and
// posts a signed callback with status=failed, error="not implemented"
// per ENG-133 acceptance criterion AC3. The HTTP handler writes 202
// when this method returns nil.
//
// Stub semantics: every accepted provision is reported as failed.
// ENG-134+ replaces runStubProvisioner with the real Pod/Deployment
// creation flow.
func (b *Backend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	// Short-circuit when the caller has already abandoned the request.
	// Mirrors docker-backend's ctx-aware pattern (internal/backend/docker/
	// lease_actor_routing.go uses the same `ctx.Err() != nil → return err`
	// guard at the top of routing entry points). Without this check, a
	// canceled/timeout HTTP request can still produce a recorded provision
	// + spawned goroutine + signed callback after the caller has hung up.
	if err := ctx.Err(); err != nil {
		provisionsTotal.WithLabelValues("rejected").Inc()
		return err
	}

	if req.LeaseUUID == "" {
		provisionsTotal.WithLabelValues("rejected").Inc()
		return fmt.Errorf("%w: lease_uuid is required", backend.ErrValidation)
	}
	if req.CallbackURL == "" {
		provisionsTotal.WithLabelValues("rejected").Inc()
		return fmt.Errorf("%w: callback_url is required", backend.ErrValidation)
	}
	if len(req.Items) == 0 {
		provisionsTotal.WithLabelValues("rejected").Inc()
		return fmt.Errorf("%w: items is required", backend.ErrValidation)
	}

	b.provisionsMu.Lock()
	// Mirror docker-backend's status-aware check: only entries in
	// ProvisionStatusFailed are eligible for replacement. Fred's reconciler
	// relies on this — it retries failed-active leases by calling Provision
	// again until FailCount reaches the configured retry ceiling. Rejecting
	// all existing entries (regardless of status) would loop the reconciler
	// at 409 forever, never incrementing FailCount, never garbage-collecting
	// the lease.
	var prevFailCount int
	if existing, exists := b.provisions[req.LeaseUUID]; exists {
		if existing.Status != backend.ProvisionStatusFailed {
			b.provisionsMu.Unlock()
			provisionsTotal.WithLabelValues("rejected").Inc()
			return backend.ErrAlreadyProvisioned
		}
		// Carry forward FailCount across the replacement so the runStubProvisioner
		// goroutine's increment lands on the cumulative count.
		prevFailCount = existing.FailCount
		// Cancel the prior entry's lifecycle so any zombie worker holding
		// its pointer observes cancellation. Pointer-equality in the worker
		// also catches this, but explicit cancel makes the invariant
		// "delete from map ⇒ cancel ctx" structural rather than emergent.
		existing.cancel()
		delete(b.provisions, req.LeaseUUID)
	}
	// Per-lease cancellable context (ENG-189). Parent is b.stopCtx (the
	// backend lifecycle context) so shutdown also cancels in-flight
	// workers. Deprovision calls cancel inside provisionsMu before
	// deleting the map entry; the worker captures p.ctx under the same
	// lock and consults ctx.Err() before every post-unlock external
	// write.
	ctx, cancel := context.WithCancel(b.stopCtx)
	p := &provision{
		LeaseUUID:    req.LeaseUUID,
		Tenant:       req.Tenant,
		ProviderUUID: req.ProviderUUID,
		Status:       backend.ProvisionStatusProvisioning,
		CallbackURL:  req.CallbackURL,
		FailCount:    prevFailCount,
		CreatedAt:    time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}
	b.provisions[req.LeaseUUID] = p
	// b.wg.Add(1) is called inside the lock-held region (before Unlock)
	// so any future Stop() implementation that takes provisionsMu before
	// wg.Wait() sees the counter incremented atomically with the map
	// insertion. Today's Stop() doesn't take the lock, so this is purely
	// defensive — callers must still sequence Stop() after the HTTP
	// server's graceful shutdown drains in-flight Provision calls.
	b.wg.Add(1)
	b.provisionsMu.Unlock()

	provisionsTotal.WithLabelValues("accepted").Inc()
	go b.runStubProvisioner(p)
	return nil
}

// runStubProvisioner is the canonical ENG-133 "not implemented"
// responder. It flips the in-memory provision record to "failed",
// persists a diagnostics entry (so post-deprovision GetProvision can
// still surface the failure cause), and sends a signed callback. Runs
// in its own goroutine tracked by b.wg so Stop() waits for it.
//
// ENG-134+ replaces this with the real K8s Pod / Deployment / Service
// creation flow; the canonical-message contract for ENG-133 acceptance
// (AC3) is status=failed, error="not implemented".
func (b *Backend) runStubProvisioner(p *provision) {
	defer b.wg.Done()

	b.provisionsMu.Lock()
	// Suppress stale Failed callbacks when a concurrent Deprovision has
	// already removed (or replaced) the lease entry. Without this check,
	// a sequence of Provision -> Deprovision -> (worker scheduled later)
	// would persist a failure diagnostic AND fire a signed status=failed
	// callback for a lease Fred has already torn down — Fred would treat
	// it as a real failure and trip its circuit breaker / fail-count
	// metrics. Mirrors docker-backend's leasesm OnExit-cancel pattern
	// (PR #79 / commit cc62f3b).
	//
	// The pointer-equality check also handles the Provision-replaces-
	// failed-entry path: if a new Provision call replaced the entry we
	// were spawned for, only the new worker should mutate the new entry.
	current, exists := b.provisions[p.LeaseUUID]
	if !exists || current != p {
		b.provisionsMu.Unlock()
		return
	}
	p.Status = backend.ProvisionStatusFailed
	p.LastError = stubProvisionerErrMsg
	// Increment FailCount (not set to 1) so retry cycles accumulate
	// correctly across Provision -> failed -> Provision-again -> failed
	// chains. The Provision method carries forward prevFailCount when
	// replacing a failed entry; runStubProvisioner adds 1 on top.
	// Docker parity: docker's in-memory provision wraps
	// leasesm.ProvisionState which carries FailCount through retries.
	p.FailCount++
	currentFailCount := p.FailCount
	callbackURL := p.CallbackURL
	leaseUUID := p.LeaseUUID
	tenant := p.Tenant
	providerUUID := p.ProviderUUID
	// Capture the lease ctx while still under provisionsMu: Deprovision
	// will call cancel() inside the same lock before deleting the map
	// entry, so this read is race-free and the worker sees the latest
	// cancellation state on its next ctx.Err() check.
	leaseCtx := p.ctx
	b.provisionsMu.Unlock()

	// Checkpoint 1: pre-diagnostic-write. ENG-189 case (b) — a
	// Deprovision that wins the lock between unlock and Store would
	// otherwise persist a failure diagnostic for a torn-down lease.
	if hook := b.beforeDiagnosticStore; hook != nil {
		hook()
	}
	if err := leaseCtx.Err(); err != nil {
		b.logger.Debug("suppressing diagnostic persist for canceled provision",
			"lease_uuid", leaseUUID,
			"err", err,
		)
		return
	}

	if err := b.diagnosticsStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    leaseUUID,
		ProviderUUID: providerUUID,
		Tenant:       tenant,
		Error:        stubProvisionerErrMsg,
		FailCount:    currentFailCount,
		CreatedAt:    time.Now(),
	}); err != nil {
		b.logger.Error("failed to persist failure diagnostic",
			"lease_uuid", leaseUUID,
			"error", err,
		)
	}

	// Checkpoint 2: pre-callback-send. ENG-189 case (c) —
	// shared.CallbackSender.SendCallback persists to bbolt BEFORE
	// delivery, so without this guard a torn-down lease can still
	// have a stale status=failed callback queued for replay.
	if hook := b.beforeCallbackSend; hook != nil {
		hook()
	}
	if err := leaseCtx.Err(); err != nil {
		b.logger.Debug("suppressing callback send for canceled provision",
			"lease_uuid", leaseUUID,
			"err", err,
		)
		return
	}

	b.callbackSender.SendCallback(
		leaseUUID,
		callbackURL,
		b.cfg.Name,
		backend.CallbackStatusFailed,
		stubProvisionerErrMsg,
	)
}

// Deprovision is idempotent: removes the in-memory record if present
// and returns nil whether or not the lease existed.
//
// Diagnostics are intentionally NOT deleted here. They survive
// deprovision so GetProvision's diagnostics-store fallback can surface
// failure diagnostics for tenants and operators after the lease ends.
// cfg.DiagnosticsMaxAge handles eventual cleanup.
//
// ENG-189 lifecycle: cancel the per-lease ctx INSIDE provisionsMu
// before deleting the map entry. runStubProvisioner captures p.ctx
// under the same lock and ctx.Err()-checks before each post-unlock
// external write (diagnostic Store, callback send), so an in-flight
// worker that already released the lock observes cancellation at its
// next checkpoint and aborts.
//
// Residual TOCTOU (accepted scope, ENG-189 plan §7 / R1): cancellation
// cannot stop an in-flight SendCallback once the worker's checkpoint-2
// ctx.Err() check has passed. Between that check and SendCallback's
// bbolt Store + outbound HTTP POST, nothing here can abort the
// callback — shared.CallbackSender has no per-lease ctx today. The
// callbackStore.Remove call below clears any pre-existing pending
// entry (e.g., from a prior failed delivery on the same lease's replay
// queue); it does NOT cancel an HTTP POST already in flight, and it
// does NOT prevent a racing worker that passes its ctx.Err() check
// from re-persisting its own status=failed entry after this Remove
// runs as a no-op. The window is ns-scale by construction (the
// worker's last instruction before SendCallback is the ctx.Err()
// check). Closing it requires reshaping the seam — e.g., a
// Deprovision-side barrier that waits for any in-flight SendCallback
// to drain, or pairing a ctx-threaded SendCallback with
// Store-under-provisionsMu discipline so the persist becomes part of
// the cancellable critical section. Either approach is deferred to
// ENG-134+, where the real K8s worker replaces runStubProvisioner and
// reshapes this seam anyway.
//
// TestDeprovision_RemovesPendingCallback pins the replay-queue cleanup
// regression: a failed delivery persists an entry; Deprovision must
// clear it so a subsequent restart's ReplayPendingCallbacks does not
// fire a stale status=failed callback for a torn-down lease.
func (b *Backend) Deprovision(ctx context.Context, leaseUUID string) error {
	_ = ctx
	b.provisionsMu.Lock()
	if existing, ok := b.provisions[leaseUUID]; ok {
		// Cancel before delete + before unlock so any in-flight worker
		// observes ctx.Err() != nil at its next external-write checkpoint.
		existing.cancel()
		delete(b.provisions, leaseUUID)
	}
	if err := b.callbackStore.Remove(leaseUUID); err != nil {
		b.logger.Error("failed to remove pending callback on deprovision",
			"lease_uuid", leaseUUID,
			"error", err,
		)
	}
	b.provisionsMu.Unlock()
	return nil
}

// GetInfo always returns ErrNotProvisioned in the ENG-133 scaffold —
// the stub provisioner never transitions a lease to "ready", so there
// is no connection info to surface. The HTTP handler maps to 404
// (BACKEND_GUIDE.md: "404 Not Found — Lease not provisioned or not
// ready yet").
func (b *Backend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	_ = ctx
	_ = leaseUUID
	return nil, backend.ErrNotProvisioned
}

// GetLogs returns ErrNotProvisioned in the ENG-133 scaffold — no
// container/pod has ever run, so no logs are available. ENG-134+ wires
// real log retrieval via kubectl-style Pod log streaming.
func (b *Backend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	_ = ctx
	_ = leaseUUID
	_ = tail
	return nil, backend.ErrNotProvisioned
}

// GetProvision returns the provision record for a lease. It reads from
// the in-memory map first; on miss, it falls back to the diagnostics
// store (which persists failures across deprovision, so tenants can
// still query why a lease failed after teardown).
//
// On the fallback path: shared.DiagnosticEntry has no Status field, so
// this method synthesizes backend.ProvisionStatusFailed. The
// diagnostics store is failure-only by construction (only failure
// paths in runStubProvisioner / future ENG-134+ workers call
// diagnosticsStore.Store), so this synthesis is safe — there is no
// other status that could plausibly be associated with a stored
// diagnostic entry. (Architect-approved revision, 2026-05-12.)
func (b *Backend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	_ = ctx

	// Map-path read: construct the ProvisionInfo under RLock so the
	// reads of p.Status / p.FailCount / p.LastError (mutated by
	// runStubProvisioner under the write lock) are race-free. The
	// diagnostics fallback below runs OUTSIDE the lock because
	// diagnosticsStore.Get is unbounded bbolt I/O — holding the lock
	// across it would serialize concurrent GetProvision callers
	// pointlessly.
	b.provisionsMu.RLock()
	if p, exists := b.provisions[leaseUUID]; exists {
		info := &backend.ProvisionInfo{
			LeaseUUID:    p.LeaseUUID,
			ProviderUUID: p.ProviderUUID,
			Status:       p.Status,
			FailCount:    p.FailCount,
			LastError:    p.LastError,
			CreatedAt:    p.CreatedAt,
		}
		b.provisionsMu.RUnlock()
		return info, nil
	}
	b.provisionsMu.RUnlock()

	diag, err := b.diagnosticsStore.Get(leaseUUID)
	if err != nil {
		return nil, fmt.Errorf("diagnostics fallback: %w", err)
	}
	if diag == nil {
		return nil, backend.ErrNotProvisioned
	}
	return &backend.ProvisionInfo{
		LeaseUUID:    diag.LeaseUUID,
		ProviderUUID: diag.ProviderUUID,
		Status:       backend.ProvisionStatusFailed,
		FailCount:    diag.FailCount,
		LastError:    diag.Error,
		CreatedAt:    diag.CreatedAt,
	}, nil
}

// ListProvisions returns a snapshot of every in-memory provision
// record. Empty slice (not nil) is returned when the map is empty so
// JSON serialization produces `[]` not `null`.
func (b *Backend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	_ = ctx
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	out := make([]backend.ProvisionInfo, 0, len(b.provisions))
	for _, p := range b.provisions {
		out = append(out, backend.ProvisionInfo{
			LeaseUUID:    p.LeaseUUID,
			ProviderUUID: p.ProviderUUID,
			Status:       p.Status,
			FailCount:    p.FailCount,
			LastError:    p.LastError,
			CreatedAt:    p.CreatedAt,
		})
	}
	return out, nil
}

// LookupProvisions returns the subset of in-memory records whose UUIDs
// appear in the input slice. Missing UUIDs are silently omitted (the
// HTTP handler distinguishes "no matches" from "endpoint missing" by
// the 200 with empty slice vs 404).
func (b *Backend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	_ = ctx
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	out := make([]backend.ProvisionInfo, 0, len(uuids))
	for _, u := range uuids {
		if p, ok := b.provisions[u]; ok {
			out = append(out, backend.ProvisionInfo{
				LeaseUUID:    p.LeaseUUID,
				ProviderUUID: p.ProviderUUID,
				Status:       p.Status,
				FailCount:    p.FailCount,
				LastError:    p.LastError,
				CreatedAt:    p.CreatedAt,
			})
		}
	}
	return out, nil
}

// Restart is a stub for ENG-133. Returns ErrNotProvisioned (mapped to
// 404 by the HTTP handler) because the stub provisioner never reaches
// a restartable state. ENG-134+ wires real Pod restart flows.
func (b *Backend) Restart(ctx context.Context, req backend.RestartRequest) error {
	_ = ctx
	_ = req
	return backend.ErrNotProvisioned
}

// Update is a stub for ENG-133. Returns ErrNotProvisioned. ENG-134+
// wires real Deployment update / image-pull / rollback flows.
func (b *Backend) Update(ctx context.Context, req backend.UpdateRequest) error {
	_ = ctx
	_ = req
	return backend.ErrNotProvisioned
}

// ReconcileCustomDomain is a no-op in the ENG-133 scaffold and returns
// nil. The backendService contract documents this method as idempotent /
// no-op for leases the backend doesn't manage (or when ingress is
// disabled), and docker-backend follows the same pattern
// (internal/backend/docker/reconcile_custom_domain.go early-returns nil
// for missing / non-ready provisions and when ingress.Enabled=false).
//
// Returning ErrNotProvisioned here would 404 every reconciler tick on
// every active lease, polluting Fred's reconciler error metrics and
// log output for a path that has nothing to do — k3s-backend's Ingress
// config rejects Enabled=true (config.go:IngressConfig.Validate), so
// there are no Ingress / Gateway routes to reconcile.
//
// ENG-134+ wires real custom-domain reconciliation via Ingress /
// Gateway API objects; the no-op default carries over for the
// missing/non-ready/disabled cases.
func (b *Backend) ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error {
	_ = ctx
	_ = leaseUUID
	_ = items
	return nil
}

// GetReleases is a stub for ENG-133. Returns ErrNotProvisioned. The
// release store is opened by New (for future use), but no entries are
// ever written until ENG-134+ adds the real provision/update flows.
func (b *Backend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	_ = ctx
	_ = leaseUUID
	return nil, backend.ErrNotProvisioned
}

// Stats returns the resource pool's current snapshot. The k3s scaffold
// never allocates against the pool (the stub provisioner doesn't call
// pool.TryAllocate), so AllocationCount is always 0 and Available* ==
// Total* in ENG-133. ENG-134+ wires real allocation.
func (b *Backend) Stats() shared.ResourceStats {
	return b.pool.Stats()
}

// Compile-time assertion that *Backend implements the interface the
// HTTP server in cmd/k3s-backend/server.go calls into (backendService).
// The interface itself lives in a `package main` we can't import here,
// so this guard re-states the contract inline. ENG-134+ will replace
// these stubs but the method set must remain stable for the HTTP
// handlers — drift here is a compile-time error in this package, not a
// link-time error in cmd/k3s-backend.
type _backendServiceGuard interface {
	Provision(ctx context.Context, req backend.ProvisionRequest) error
	Deprovision(ctx context.Context, leaseUUID string) error
	GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error)
	GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error)
	GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error)
	ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error)
	LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error)
	Restart(ctx context.Context, req backend.RestartRequest) error
	Update(ctx context.Context, req backend.UpdateRequest) error
	ReconcileCustomDomain(ctx context.Context, leaseUUID string, items []backend.LeaseItem) error
	GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error)
	Health(ctx context.Context) error
	Stats() shared.ResourceStats
}

var _ _backendServiceGuard = (*Backend)(nil)

// Compile-time check: *Backend satisfies the full backend.Backend contract,
// not just the narrower cmd/k3s-backend HTTP-handler interface above. The
// local _backendServiceGuard only covers what the HTTP server consumes; the
// reconciler / router consume the wider backend.Backend interface, and
// drift between them (such as round-13's missing RefreshState) goes
// undetected when only the narrow guard is in place. Any future addition
// to backend.Backend that k3s does not implement will fail the build here.
var _ backend.Backend = (*Backend)(nil)
