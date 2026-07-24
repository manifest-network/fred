package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// replaceOpReason maps a replace op to its curated failure-category Reason
// (ENG-508). doReplaceContainers runs for restart, update, AND restore, so each
// is mapped explicitly; an unrecognized op defaults to ReasonInternal (never
// misclassified as one of the named operations). The paired human message is
// always `op + " failed"`, matching the CallbackErr base built in
// doReplaceContainers, so the two cannot diverge.
func replaceOpReason(op string) backend.Reason {
	switch op {
	case "update":
		return backend.ReasonUpdateFailed
	case "restore":
		return backend.ReasonRestoreFailed
	case "restart":
		return backend.ReasonRestartFailed
	default:
		return backend.ReasonInternal
	}
}

// applyCustomDomainOverrides applies per-ServiceName custom_domain values to the
// given items slice, keyed by ServiceName so it is robust to a recoverState
// rebuild that reorders Items. No-op when overrides is empty.
//
// Two call sites (ENG-231), with opposite intent about WHICH slice to pass:
//   - routeReplaceRestart passes the off-actor worker-snapshot COPY. It must NOT
//     pass prov.Items here — that would be an off-actor mutation of live state.
//   - customDomainOnSuccess passes prov.Items itself, to COMMIT the values. That
//     is safe (and intended) because it runs on the serial actor goroutine inside
//     onEnterReadyFromReplaceCompleted's UpdateFn, the sole writer of prov.Items.
func applyCustomDomainOverrides(items []backend.LeaseItem, overrides map[string]string) {
	if len(overrides) == 0 {
		return
	}
	for i := range items {
		if d, ok := overrides[items[i].ServiceName]; ok {
			items[i].CustomDomain = d
		}
	}
}

// customDomainOnSuccess returns an OnSuccess hook that commits the override
// values into prov.Items. It runs inside onEnterReadyFromReplaceCompleted on the
// serial actor goroutine, under the same UpdateFn critical section as the
// Status->Ready flip, and ONLY on a successful redeploy — so the actor commits
// nothing to prov.Items on a failed redeploy. Returns nil when there are no
// overrides, preserving the plain-restart behavior. (ENG-231)
func customDomainOnSuccess(overrides map[string]string) func(*leasesm.ProvisionState) {
	if len(overrides) == 0 {
		return nil
	}
	// Reuse the worker-snapshot match/assign so the committed prov.Items value
	// and the rendered container label cannot diverge from a one-sided edit.
	return func(p *leasesm.ProvisionState) {
		applyCustomDomainOverrides(p.Items, overrides)
	}
}

// Restart restarts containers for a lease without changing the manifest.
// State machine: Ready|Failed → Restarting → Ready|Failed
//
// SEAM CLOSED (ENG-230). This prelude is read-only: it fast-fails on
// ErrNotProvisioned / ErrInvalidState under provisionsMu, snapshots the
// fields the worker needs, then does pure work (manifest marshal +
// release-store Append). It performs NO write to prov.Status /
// prov.CallbackURL — the lease actor's onEnterRestarting entry action is
// the sole writer of those fields, firing inside handleRestartRequested
// BEFORE the ack. Because Restart() returns only after observing that
// ack, the "Restart() returns => prov.Status == Restarting" invariant
// the HTTP handler's event-broker publish depends on (api/handlers.go:
// RestartLease) is preserved without an off-actor write.
//
// The prelude's fast-fail is only a route-time precondition — it does NOT
// guarantee the lease is still Ready/Failed when the actor dequeues the
// message. The real serialization is the actor inbox (the only path that
// mutates prov.Status). So a same-lease concurrent restart that passes the
// route-time check but loses the race (the winner already ran
// onEnterRestarting) is REJECTED by the actor, not prevented here:
// handleRestartRequested's classifyReplaceReject returns ErrInvalidState
// for the busy SM, which this function forwards and api/handlers.go maps
// to a clean 409.
//
// Since no off-actor Status write remains, there is nothing to roll back
// on a marshal / Append / routing / ack failure: the error paths just
// return (the release-store Append is on a separate bbolt store; a
// "deploying" record left behind on routing/ack failure is cosmetic —
// recover.go skips non-active releases and deprovision deletes them).
func (b *Backend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return b.routeReplaceRestart(ctx, req.LeaseUUID, req.CallbackURL, nil)
}

// routeReplaceRestart is the shared restart routing used by the public Restart
// (overrides == nil) and by ReconcileCustomDomain (overrides carries the
// per-ServiceName custom_domain changes). The SEAM-CLOSED (ENG-230) prelude is
// unchanged: read-only fast-fail under provisionsMu, field snapshot, no
// prov.Status write — the actor's onEnterRestarting is the sole writer, firing
// BEFORE the ack, so "returns => Status==Restarting" holds for HTTP-handler
// publish (api/handlers.go:RestartLease). A concurrent caller that passes the
// route-time check but loses the actor race gets ErrInvalidState (409 for HTTP;
// silent retry-next-tick for the reconciler). The only addition over the plain
// Restart prelude is that custom-domain overrides are applied to the worker's
// item snapshot (a copy) and committed into prov.Items by the actor's success
// entry action via OnSuccess (ENG-231).
func (b *Backend) routeReplaceRestart(ctx context.Context, leaseUUID, callbackURL string, overrides map[string]string) error {
	logger := b.logger.With("lease_uuid", leaseUUID)

	b.provisionsMu.Lock()
	prov, exists := b.provisions[leaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return backend.ErrNotProvisioned
	}
	if prov.Status != backend.ProvisionStatusReady && prov.Status != backend.ProvisionStatusFailed {
		status := prov.Status
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: cannot restart from status %s", backend.ErrInvalidState, status)
	}
	if prov.StackManifest == nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: no stored manifest for restart (pre-migration legacy lease?)", backend.ErrInvalidState)
	}
	stackManifest := prov.StackManifest
	containerIDs := append([]string(nil), prov.ContainerIDs...)
	serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
	for k, v := range prov.ServiceContainers {
		serviceContainers[k] = append([]string(nil), v...)
	}
	items := append([]backend.LeaseItem(nil), prov.Items...)
	// Apply custom-domain overrides to the worker's snapshot COPY (never
	// prov.Items). Keyed by ServiceName, so even if recoverState swapped the
	// struct between the reconciler's diff and here, the desired domain is
	// re-applied onto the current items. (ENG-231/ENG-278)
	applyCustomDomainOverrides(items, overrides)
	b.provisionsMu.Unlock()

	// Record restart release as deploying. Abort if this fails — without a
	// release record, ActivateLatest after success is a no-op, and a cold
	// restart would recover the previous manifest (silently rolling back).
	if b.releaseStore != nil {
		manifestBytes, marshalErr := json.Marshal(stackManifest)
		if marshalErr != nil {
			return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)
		}
		if relErr := b.releaseStore.Append(leaseUUID, shared.Release{
			Manifest:  manifestBytes,
			Image:     "stack",
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	// Hand off to the lease actor. The actor's onEnterRestarting writes
	// Status=Restarting (+ CallbackURL) BEFORE acking, then spawns the replace
	// worker. On success, onEnterReadyFromReplaceCompleted runs onSuccess (the
	// prov.Items custom_domain commit) under UpdateFn, atomic with Status->Ready.
	opCtx, opCancel := b.shutdownAwareContext()
	onSuccess := customDomainOnSuccess(overrides)
	work := func() leasesm.ReplaceResult {
		return b.doRestart(opCtx, leaseUUID, stackManifest, containerIDs, serviceContainers, items, onSuccess, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, leaseUUID, leasesm.RestartRequestedMsg{Cancel: opCancel, Work: work, Ack: ack, CallbackURL: callbackURL}); routeErr != nil {
		opCancel()
		return routeErr
	}
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		return err
	}
	return nil
}

// doRestart performs an async stack restart: stops all service containers
// and recreates them from the stored StackManifest.
//
// The SKU-preflight failure branch sets RecoveredIfSourceActive: it touches
// no containers, so the lease is left exactly in its replace-start state —
// "recovered to Ready" is correct iff its containers were running at start
// (i.e. the lease was active). doRestart no longer knows that; the actor
// derives it from its serial, actor-observed replaceWasActive
// (spawnReplaceWorker), which is correct even in the death-then-restart
// ordering where the prelude's route-time snapshot was stale.
func (b *Backend) doRestart(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, onSuccess func(*leasesm.ProvisionState), logger *slog.Logger) leasesm.ReplaceResult {
	profiles := make(map[string]SKUProfile, len(items))
	for _, item := range items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, profErr := b.cfg.GetSKUProfile(item.SKU)
		if profErr != nil {
			err := fmt.Errorf("SKU profile lookup failed for %s: %w", item.SKU, profErr)
			b.recordPreflightFailure(leaseUUID, backend.ReasonRestartFailed, backend.MsgRestartFailed, err, logger)
			return leasesm.ReplaceResult{
				CallbackErr:             backend.MsgRestartFailed,
				Err:                     err,
				RecoveredIfSourceActive: true,
				Failure: leasesm.ReplaceFailureInfo{
					Operation:   "restart",
					Reason:      backend.ReasonRestartFailed,
					CallbackErr: backend.MsgRestartFailed,
					LastError:   err.Error(),
				},
			}
		}
		profiles[item.SKU] = profile
	}

	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "restart",
		Logger:            logger,
		OnSuccess:         onSuccess,
	})
}

// replaceContainersOp describes a stack container replacement operation.
type replaceContainersOp struct {
	LeaseUUID         string
	Stack             *manifest.StackManifest
	Items             []backend.LeaseItem
	Profiles          map[string]SKUProfile
	OldContainerIDs   []string
	ServiceContainers map[string][]string // old service → container IDs mapping
	Operation         string              // "restart", "update", or "restore"
	Logger            *slog.Logger

	// NoComposeRollback disables the failure-path rollbackViaCompose. The
	// restore op sets it: there are NO prior containers to "recover" to (the
	// new lease was reserved at Provisioning, never Ready), and the restore
	// caller (doRestore) owns its own compensating teardown — compose.Down +
	// re-quarantining the adopted volumes back to the retained namespace. With
	// this true, Restored stays false on failure, so spawnReplaceWorker
	// dispatches replaceFailedMsg (terminal Failed) rather than
	// replaceRecoveredMsg. Defaults false: restart/update are unaffected.
	NoComposeRollback bool

	// OnSuccess is called under provisionsMu lock after successful replacement.
	OnSuccess func(prov *leasesm.ProvisionState)
}

// doReplaceContainers performs the stack container replacement lifecycle
// using Docker Compose. Compose handles stopping old containers and starting
// new ones via a single Up call, with rollback via Up with the previous manifest.
//
// Returns leasesm.ReplaceResult — see doReplaceContainers for the protocol.
// Stack variant's OnSuccess typically sets StackManifest; this function
// populates the leasesm.ReplaceResult's fields for the SM entry action.
func (b *Backend) doReplaceContainers(ctx context.Context, op replaceContainersOp) (resultRet leasesm.ReplaceResult) {
	var err error
	var callbackErr string
	var newContainerIDs []string
	var imageSetups map[string]*imageSetup
	newServiceContainers := make(map[string][]string)
	projectName := composeProjectName(op.LeaseUUID)

	defer func() {
		if err != nil {
			op.Logger.Error(op.Operation+" failed (stack)", "error", err)

			if b.releaseStore != nil {
				// Message == the CallbackErr base (op + " failed") by construction,
				// so restart/update/restore never diverge or mislabel.
				rReason, rMsg := replaceOpReason(op.Operation), op.Operation+" failed"
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", rReason, rMsg); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
				}
			}

			// Capture logs from the FAILED new containers BEFORE the
			// rollback tears them down. Without this, the persisted
			// diagnostic entry would record empty logs because the
			// containers are gone by the time the SM entry action runs
			// persistDiagnostics.
			failureLogs := b.captureContainerLogs(newContainerIDs, stackContainerLogKeys(newServiceContainers))

			// Rollback: rebuild the Project from the previous StackManifest and
			// Compose Up to restore the old containers. Skipped for the restore
			// op (NoComposeRollback): a failed restore has no prior containers to
			// recover to — doRestore's terminal defer does the compensating
			// teardown — and leaving Restored=false makes spawnReplaceWorker fire
			// replaceFailedMsg (terminal Failed) instead of replaceRecoveredMsg.
			restored := false
			if !op.NoComposeRollback {
				restored = b.rollbackViaCompose(op)
			}
			if restored {
				op.Logger.Info("rolled back to previous containers via compose (stack)")
				callbackErr += "; rolled back to previous version"
			} else {
				callbackErr += "; rollback failed"
			}

			// Stack rollback: oldStopped is effectively true — compose.Up
			// with ForceRecreate replaces every container in the project, so
			// the LastError-clear-on-restart rule matches the single-manifest
			// doReplaceContainers semantics.
			resultRet = leasesm.ReplaceResult{
				CallbackErr: callbackErr,
				Err:         err,
				Restored:    restored,
				Failure: leasesm.ReplaceFailureInfo{
					Operation:   op.Operation,
					Reason:      replaceOpReason(op.Operation),
					OldStopped:  true,
					CallbackErr: callbackErr,
					LastError:   err.Error(),
					Logs:        failureLogs,
				},
			}
			return
		}

		if b.releaseStore != nil {
			if relErr := b.releaseStore.ActivateLatest(op.LeaseUUID); relErr != nil {
				op.Logger.Warn("failed to update release status", "error", relErr)
			}
		}

		resultRet = leasesm.ReplaceResult{
			Success: leasesm.ReplaceSuccessResult{
				ContainerIDs:      newContainerIDs,
				ServiceContainers: newServiceContainers,
				OnSuccess:         op.OnSuccess,
			},
		}
	}()

	// Per-service image setup.
	imgStart := time.Now()
	imageSetups = make(map[string]*imageSetup)
	for svcName, svc := range op.Stack.Services {
		imgSetup, setupErr := b.inspectImageForSetup(ctx, svc.Image, svc.User)
		if setupErr != nil {
			err = setupErr
			callbackErr = op.Operation + " failed"
			return
		}
		imageSetups[svcName] = imgSetup
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseImageSetup).Observe(time.Since(imgStart).Seconds())

	// Read provision metadata.
	b.provisionsMu.RLock()
	failCount := 0
	tenant := ""
	providerUUID := ""
	callbackURL := ""
	if prov, ok := b.provisions[op.LeaseUUID]; ok {
		failCount = prov.FailCount
		tenant = prov.Tenant
		providerUUID = prov.ProviderUUID
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.RUnlock()

	// Resolve tenant network name.
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		if _, netErr := b.ensureTenantNetwork(ctx, tenant); netErr != nil {
			err = netErr
			callbackErr = op.Operation + " failed"
			return
		}
		networkName = TenantNetworkName(tenant)
	}

	// Ensure volumes exist for all services/instances.
	volStart := time.Now()
	volBinds, _, volErr := b.setupVolBinds(ctx, op.LeaseUUID, op.Items, op.Profiles, imageSetups, op.Stack.Services, op.Logger)
	if volErr != nil {
		err = volErr
		callbackErr = op.Operation + " failed"
		return
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseVolumeSetup).Observe(time.Since(volStart).Seconds())

	// Build Compose project and bring it up.
	// ForceRecreate is used for restarts (config unchanged but containers need replacing).
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:    op.LeaseUUID,
		Tenant:       tenant,
		ProviderUUID: providerUUID,
		CallbackURL:  callbackURL,
		BackendName:  b.cfg.Name,
		FailCount:    failCount,
		Stack:        op.Stack,
		Items:        op.Items,
		Profiles:     op.Profiles,
		ImageSetups:  imageSetups,
		NetworkName:  networkName,
		VolBinds:     volBinds,
		Cfg:          &b.cfg,
		Ingress:      b.cfg.Ingress,
	})

	op.Logger.Info("compose up for "+op.Operation, "project", projectName, "services", len(project.Services))
	forceRecreate := op.Operation == "restart"
	upStart := time.Now()
	if upErr := b.compose.Up(ctx, project, composeUpOpts{ForceRecreate: forceRecreate}); upErr != nil {
		err = fmt.Errorf("compose up failed: %w", upErr)
		callbackErr = op.Operation + " failed"
		return
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseComposeUp).Observe(time.Since(upStart).Seconds())

	// Discover new container IDs via Compose PS.
	containers, psErr := b.compose.PS(ctx, projectName)
	if psErr != nil {
		err = fmt.Errorf("compose ps failed: %w", psErr)
		callbackErr = op.Operation + " failed"
		return
	}

	newContainerIDs, newServiceContainers = mapComposeContainers(containers, op.Items)

	// Verify startup per-service so each service uses its own health check config.
	verifyStart := time.Now()
	for svcName, svcCIDs := range newServiceContainers {
		svc := op.Stack.Services[svcName]
		if err = b.verifyStartup(ctx, svc, svcCIDs, op.Logger.With("service", svcName)); err != nil {
			callbackErr = startupErrorToCallbackMsg(err)
			return
		}
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseVerifyStartup).Observe(time.Since(verifyStart).Seconds())

	op.Logger.Info(op.Operation+" completed (stack)", "containers", len(newContainerIDs))
	return
}

// rollbackViaCompose restores the previous stack state by rebuilding a
// Compose project from the previous StackManifest (still in the provision,
// since OnSuccess hasn't run) and calling Compose Up. Returns true on success.
func (b *Backend) rollbackViaCompose(op replaceContainersOp) bool {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Read previous manifest from provision (OnSuccess hasn't run, so
	// prov.StackManifest is still the old manifest).
	b.provisionsMu.RLock()
	prov, ok := b.provisions[op.LeaseUUID]
	if !ok {
		b.provisionsMu.RUnlock()
		op.Logger.Error("rollback: provision not found")
		return false
	}
	prevStack := prov.StackManifest
	tenant := prov.Tenant
	providerUUID := prov.ProviderUUID
	callbackURL := prov.CallbackURL
	failCount := prov.FailCount
	b.provisionsMu.RUnlock()

	if prevStack == nil {
		op.Logger.Error("rollback: no previous stack manifest available")
		return false
	}

	// Inspect images for the previous manifest.
	prevImageSetups := make(map[string]*imageSetup)
	for svcName, svc := range prevStack.Services {
		imgSetup, setupErr := b.inspectImageForSetup(rollbackCtx, svc.Image, svc.User)
		if setupErr != nil {
			op.Logger.Error("rollback: image inspection failed", "service", svcName, "error", setupErr)
			return false
		}
		prevImageSetups[svcName] = imgSetup
	}

	// Resolve network name.
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		networkName = TenantNetworkName(tenant)
	}

	// Re-use existing volumes (already created during original provision).
	volBinds, _, volErr := b.setupVolBinds(rollbackCtx, op.LeaseUUID, op.Items, op.Profiles, prevImageSetups, prevStack.Services, op.Logger)
	if volErr != nil {
		op.Logger.Error("rollback: volume setup failed", "error", volErr)
		return false
	}

	// Build project from previous manifest.
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:    op.LeaseUUID,
		Tenant:       tenant,
		ProviderUUID: providerUUID,
		CallbackURL:  callbackURL,
		BackendName:  b.cfg.Name,
		FailCount:    failCount,
		Stack:        prevStack,
		Items:        op.Items,
		Profiles:     op.Profiles,
		ImageSetups:  prevImageSetups,
		NetworkName:  networkName,
		VolBinds:     volBinds,
		Cfg:          &b.cfg,
		Ingress:      b.cfg.Ingress,
	})

	// Compose Up with ForceRecreate to restore previous containers.
	if upErr := b.compose.Up(rollbackCtx, project, composeUpOpts{ForceRecreate: true}); upErr != nil {
		op.Logger.Error("rollback: compose up failed", "error", upErr)
		return false
	}

	// Discover restored container IDs and update provision.
	containers, psErr := b.compose.PS(rollbackCtx, composeProjectName(op.LeaseUUID))
	if psErr != nil {
		op.Logger.Error("rollback: compose ps failed", "error", psErr)
		return false
	}

	containerIDs, serviceContainers := mapComposeContainers(containers, op.Items)
	b.provisionsMu.Lock()
	if p, ok := b.provisions[op.LeaseUUID]; ok {
		p.ContainerIDs = containerIDs
		p.ServiceContainers = serviceContainers
	}
	b.provisionsMu.Unlock()

	return true
}

// recordPreflightFailure logs the preflight error (e.g., profile lookup,
// image pull) and marks the latest release as failed. Provision-state
// mutations (LastError, FailCount, Status, persistDiagnostics) are
// handled by the SM entry action that fires when the caller returns its
// leasesm.ReplaceResult — see the preflight branches of doRestart /
// doUpdate (post-Task-14, the unified stack-shaped versions).
func (b *Backend) recordPreflightFailure(leaseUUID string, reason backend.Reason, message string, err error, logger *slog.Logger) {
	logger.Error("preflight failed", "error", err)

	if b.releaseStore != nil {
		if relErr := b.releaseStore.UpdateLatestStatus(leaseUUID, "failed", reason, message); relErr != nil {
			logger.Warn("failed to update release status", "error", relErr)
		}
	}
}

// Update deploys a new manifest for a lease, replacing containers.
// State machine: Ready|Failed → Updating → Ready|Failed
//
// SEAM CLOSED (ENG-230) — see the extended comment on Backend.Restart.
// Like Restart, the prelude is read-only: it fast-fails / validates
// under provisionsMu, snapshots fields, then records the release. It
// performs NO write to prov.Status / prov.CallbackURL — the actor's
// onEnterUpdating entry action is the sole writer, firing inside
// handleUpdateRequested BEFORE the ack, so the "Update() returns =>
// Status is Updating" contract holds without an off-actor write. No
// rollback is needed on any failure path (nothing on prov was mutated).
func (b *Backend) Update(ctx context.Context, req backend.UpdateRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID)

	// Synchronous phase: read-only validation + field snapshot (no
	// prov.Status / prov.CallbackURL write — ENG-230).
	b.provisionsMu.Lock()
	prov, exists := b.provisions[req.LeaseUUID]
	if !exists {
		b.provisionsMu.Unlock()
		return backend.ErrNotProvisioned
	}
	if prov.Status != backend.ProvisionStatusReady && prov.Status != backend.ProvisionStatusFailed {
		status := prov.Status
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: cannot update from status %s", backend.ErrInvalidState, status)
	}

	// Boundary normalization: prov.Items must be populated (it is set at
	// Provision time and rehydrated from container labels by recover.go).
	// Task 3's `len(prov.Items) > 0` guard is removed here per the Task 3
	// review carry-over; after Task 8-9's recover-time migration every
	// recovered provision will have Items populated. A surviving empty
	// Items now surfaces immediately as ErrInvalidState rather than
	// silently routing into the (now-gone) legacy path.
	if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: prov.Items}); err != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidState, err)
	}

	// Parse new payload. ParsePayload always returns a *StackManifest;
	// legacy flat payloads are auto-wrapped under DefaultServiceName.
	stackManifest, parseErr := manifest.ParsePayload(req.Payload)
	if parseErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, parseErr)
	}
	if isFlatPayload(req.Payload) {
		logger.Warn("manifest deprecation: tenant submitted flat single-service manifest; auto-wrapped as 1-service stack",
			"lease_uuid", req.LeaseUUID)
	}

	// Validate stack against stored items. A flat payload submitted against
	// a multi-service stack lease auto-wraps to {"app": <flat>} and falls
	// through here as a service-name mismatch — preserving the pre-Task-2
	// error category via ErrInvalidManifest (mirrors provision.go).
	if valErr := manifest.ValidateStackAgainstItems(stackManifest, prov.Items); valErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, valErr)
	}
	// Reject tenant-pinned fixed host ports on update too (ENG-605); mirrors
	// provision.go so a tenant cannot introduce a squatted port via update.
	if hpErr := manifest.ValidateNoFixedHostPorts(stackManifest); hpErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, hpErr)
	}
	// Validate all images.
	for svcName, svc := range stackManifest.Services {
		if imgErr := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); imgErr != nil {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, imgErr)
		}
	}
	// Validate all SKU profiles.
	profiles := make(map[string]SKUProfile, len(prov.Items))
	for _, item := range prov.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, profErr := b.cfg.GetSKUProfile(item.SKU)
		if profErr != nil {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %w", backend.ErrValidation, profErr)
		}
		profiles[item.SKU] = profile
	}

	oldContainerIDs := append([]string(nil), prov.ContainerIDs...)
	serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
	for k, v := range prov.ServiceContainers {
		serviceContainers[k] = append([]string(nil), v...)
	}
	items := append([]backend.LeaseItem(nil), prov.Items...)
	// No pre-replace status snapshot: Status/CallbackURL writes and gauge
	// bookkeeping are the actor's, keyed on the actor-observed
	// replaceWasActive (onEnterUpdating). The update preflight is
	// unconditionally Failed regardless, so the worker needs no status hint.
	b.provisionsMu.Unlock()

	// Record release. Image:"stack" is the existing stack-path sentinel
	// (pre-Task-2 behavior for multi-service leases); after Task 6 it
	// also covers auto-wrapped 1-service leases. GetReleases tenants
	// already see this sentinel for stack-shaped leases.
	if b.releaseStore != nil {
		if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
			Manifest:  req.Payload,
			Image:     "stack",
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	// Hand off to the actor. The actor's onEnterUpdating writes
	// Status=Updating (+ CallbackURL) BEFORE acking. See
	// handleUpdateRequested / spawnReplaceWorker.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doUpdate(opCtx, req.LeaseUUID, stackManifest, profiles, oldContainerIDs, serviceContainers, items, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.UpdateRequestedMsg{Cancel: opCancel, Work: work, Ack: ack, CallbackURL: req.CallbackURL}); routeErr != nil {
		opCancel()
		return routeErr
	}
	// See ackOrAbort's comment for the ctx-vs-ack race rationale.
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		return err
	}
	return nil
}

// doUpdate performs the actual stack container update asynchronously.
//
// Unlike doRestart, doUpdate takes no wasActive flag: an update preflight
// failure (image pull) is unconditionally Failed — a missed image pull never
// achieved the desired new-image state, so the lease is Failed even from a
// Ready source. This asymmetry is intentional; do not key it on wasActive.
func (b *Backend) doUpdate(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, profiles map[string]SKUProfile, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, logger *slog.Logger) leasesm.ReplaceResult {
	// Pull each unique image (deduplicated).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image for update", "service", svcName, "image", svc.Image)
		if pullErr := b.docker.PullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); pullErr != nil {
			err := fmt.Errorf("image pull failed for service %s: %w", svcName, pullErr)
			b.recordPreflightFailure(leaseUUID, backend.ReasonImagePullFailed, backend.MsgImagePullFailed, err, logger)
			// Force Status=Failed unconditionally (Restored:false) since the
			// user's desired state (the new image set) was not achieved.
			return leasesm.ReplaceResult{
				CallbackErr: backend.MsgImagePullFailed,
				Err:         err,
				Restored:    false,
				Failure: leasesm.ReplaceFailureInfo{
					Operation:   "update",
					Reason:      backend.ReasonImagePullFailed,
					CallbackErr: backend.MsgImagePullFailed,
					LastError:   err.Error(),
				},
			}
		}
		pulledImages[svc.Image] = true
	}

	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "update",
		Logger:            logger,
		OnSuccess: func(prov *leasesm.ProvisionState) {
			prov.StackManifest = stack
		},
	})
}
