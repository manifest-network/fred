package docker

import (
	"cmp"
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

// restartRollback undoes the synchronous Status/CallbackURL mutation that
// Restart/Update made before handing off to the actor, and marks the
// just-Append'd release "failed" with the supplied cause so the release
// history records why the handoff did not complete. Called from every
// send-refusal / ack-error / ctx-cancel branch in the three replace-
// starting paths — the cause differentiates shutdown, caller ctx cancel,
// and SM rejection.
func (b *Backend) restartRollback(leaseUUID string, prevStatus backend.ProvisionStatus, prevCallbackURL string, cause error, logger *slog.Logger) {
	b.provisionsMu.Lock()
	if p, ok := b.provisions[leaseUUID]; ok {
		p.Status = prevStatus
		p.CallbackURL = prevCallbackURL
	}
	b.provisionsMu.Unlock()
	if b.releaseStore != nil {
		msg := "rollback"
		if cause != nil {
			msg = cause.Error()
		}
		if relErr := b.releaseStore.UpdateLatestStatus(leaseUUID, "failed", msg); relErr != nil {
			logger.Warn("failed to update release status during rollback", "error", relErr, "rollback_cause", msg)
		}
	}
}

// Restart restarts containers for a lease without changing the manifest.
// State machine: Ready|Failed → Restarting → Ready|Failed
//
// ARCHITECTURAL SEAM — intentional. This function writes prov.Status to
// Restarting synchronously under provisionsMu BEFORE routing to the
// lease actor. Most Status writes live inside the actor's goroutine
// (SM entry/exit actions and doDeprovision); Restart and Update are
// the two places where a Status write happens outside the actor.
//
// Why the seam exists:
//   - Fast-fail semantics: concurrent Restart calls (or a Restart racing
//     a Container-died transition to Failing) get ErrInvalidState
//     immediately under the mutex, not after an inbox round-trip.
//   - The invariant "Restart() returns => prov.Status == Restarting"
//     is depended on by the HTTP handler's event-broker publish
//     (api/handlers.go: RestartLease). A caller observing the lease
//     state after Restart() returns sees Restarting atomically.
//
// Compensation: the narrow race between the SM guard's RLock-release
// and onEnterFailing's Lock-acquire (a Restart can slip in during the
// guard's 10s InspectContainer) is handled by an explicit Status
// recheck in onEnterFailing (lease_sm.go) that bails on !Ready and
// bumps lease_failing_race_skipped_total.
//
// To close the seam: move the Status write + SM fire into the actor's
// handleRestartRequested, ensuring the actor writes Status BEFORE
// acking so the contract above is preserved. Expected to eliminate
// the recheck/metric but not the other edge-case rules (Failing.Permit
// retries, Deprovisioning.Ignore, restored:false on Update preflight)
// which exist for independent reasons. Don't refactor without an
// operational trigger — the metric climbing in production, a new bug,
// or a broader rewrite of the Restart/Update path.
func (b *Backend) Restart(ctx context.Context, req backend.RestartRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID)

	// Synchronous phase: validate state and transition to Restarting
	b.provisionsMu.Lock()
	prov, exists := b.provisions[req.LeaseUUID]
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
		// Post-Task-5 every restartable provision must carry a stack
		// manifest. Legacy-shape provisions recovered before Tasks 8-9's
		// startup migration runs would only have prov.Manifest populated
		// and end up here; surfacing it as InvalidState lets the operator
		// know a recover-time migration is owed. After Task 9 lands, no
		// in-tree path can produce a manifest-less restartable provision.
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: no stored manifest for restart (pre-migration legacy lease?)", backend.ErrInvalidState)
	}
	prevStatus := prov.Status
	prevCallbackURL := prov.CallbackURL
	prov.Status = backend.ProvisionStatusRestarting
	if req.CallbackURL != "" {
		prov.CallbackURL = req.CallbackURL
	}
	stackManifest := prov.StackManifest
	containerIDs := append([]string(nil), prov.ContainerIDs...)
	serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
	for k, v := range prov.ServiceContainers {
		serviceContainers[k] = append([]string(nil), v...)
	}
	items := append([]backend.LeaseItem(nil), prov.Items...)
	b.provisionsMu.Unlock()

	// Record restart release as deploying. Abort if this fails — without a
	// release record, ActivateLatest after success is a no-op, and a cold
	// restart would recover the previous manifest (silently rolling back).
	if b.releaseStore != nil {
		manifestBytes, marshalErr := json.Marshal(stackManifest)
		if marshalErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			prov.CallbackURL = prevCallbackURL
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)
		}
		if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
			Manifest:  manifestBytes,
			Image:     "stack",
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			prov.CallbackURL = prevCallbackURL
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	// Hand off to the lease actor. Actor fires the Restarting
	// transition, acks, and spawns the replace worker (tracked by
	// workers barrier). See handleRestartRequested / spawnReplaceWorker.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doRestartStack(opCtx, req.LeaseUUID, stackManifest, containerIDs, serviceContainers, items, prevStatus, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.RestartRequestedMsg{Cancel: opCancel, Work: work, Ack: ack}); routeErr != nil {
		opCancel()
		b.restartRollback(req.LeaseUUID, prevStatus, prevCallbackURL, routeErr, logger)
		return routeErr
	}
	// See ackOrAbort's comment for the ctx-vs-ack race rationale.
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		b.restartRollback(req.LeaseUUID, prevStatus, prevCallbackURL, err, logger)
		return err
	}
	return nil
}

// (Outcome dispatch lives in leaseActor.spawnReplaceWorker so the worker
// goroutine is owned by the actor via workers barrier — see lease_actor.go.)

// doRestart performs the actual container restart asynchronously.
func (b *Backend) doRestart(ctx context.Context, leaseUUID string, m *manifest.Manifest, oldContainerIDs []string, sku, customDomain string, prevStatus backend.ProvisionStatus, logger *slog.Logger) leasesm.ReplaceResult {
	profile, profErr := b.cfg.GetSKUProfile(sku)
	if profErr != nil {
		err := fmt.Errorf("SKU profile lookup failed: %w", profErr)
		b.recordPreflightFailure(leaseUUID, err, logger)
		// Preflight failure restored the lease to prevStatus. Treat this
		// as a recovery (restored=true): the lease is back to its
		// pre-operation state, so the SM goes to Ready or Failed based on
		// prevStatus. The fireReplaceOutcome dispatcher will pick the
		// right event.
		return leasesm.ReplaceResult{
			CallbackErr: "restart failed",
			Err:         err,
			Restored:    prevStatus == backend.ProvisionStatusReady,
			Failure: leasesm.ReplaceFailureInfo{
				PrevStatus:  prevStatus,
				Operation:   "restart",
				OldStopped:  false, // preflight never stopped containers
				CallbackErr: "restart failed",
				LastError:   err.Error(),
			},
		}
	}

	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:       leaseUUID,
		Manifest:        m,
		SKU:             sku,
		Profile:         profile,
		OldContainerIDs: oldContainerIDs,
		Quantity:        len(oldContainerIDs),
		Operation:       "restart",
		PrevStatus:      prevStatus,
		Logger:          logger,
		CustomDomain:    customDomain,
	})
}

// doRestartStack performs an async stack restart: stops all service containers
// and recreates them from the stored StackManifest.
func (b *Backend) doRestartStack(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, prevStatus backend.ProvisionStatus, logger *slog.Logger) leasesm.ReplaceResult {
	profiles := make(map[string]SKUProfile, len(items))
	for _, item := range items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, profErr := b.cfg.GetSKUProfile(item.SKU)
		if profErr != nil {
			err := fmt.Errorf("SKU profile lookup failed for %s: %w", item.SKU, profErr)
			b.recordPreflightFailure(leaseUUID, err, logger)
			return leasesm.ReplaceResult{
				CallbackErr: "restart failed",
				Err:         err,
				Restored:    prevStatus == backend.ProvisionStatusReady,
				Failure: leasesm.ReplaceFailureInfo{
					PrevStatus:  prevStatus,
					Operation:   "restart",
					CallbackErr: "restart failed",
					LastError:   err.Error(),
				},
			}
		}
		profiles[item.SKU] = profile
	}

	return b.doReplaceStackContainers(ctx, replaceStackContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "restart",
		PrevStatus:        prevStatus,
		Logger:            logger,
	})
}

// replaceStackContainersOp describes a stack container replacement operation.
type replaceStackContainersOp struct {
	LeaseUUID         string
	Stack             *manifest.StackManifest
	Items             []backend.LeaseItem
	Profiles          map[string]SKUProfile
	OldContainerIDs   []string
	ServiceContainers map[string][]string     // old service → container IDs mapping
	Operation         string                  // "restart" or "update"
	PrevStatus        backend.ProvisionStatus // status before the operation began, for gauge accuracy
	Logger            *slog.Logger

	// OnSuccess is called under provisionsMu lock after successful replacement.
	OnSuccess func(prov *leasesm.ProvisionState)
}

// doReplaceStackContainers performs the stack container replacement lifecycle
// using Docker Compose. Compose handles stopping old containers and starting
// new ones via a single Up call, with rollback via Up with the previous manifest.
//
// Returns leasesm.ReplaceResult — see doReplaceContainers for the protocol.
// Stack variant's OnSuccess typically sets StackManifest; this function
// populates the leasesm.ReplaceResult's fields for the SM entry action.
func (b *Backend) doReplaceStackContainers(ctx context.Context, op replaceStackContainersOp) (resultRet leasesm.ReplaceResult) {
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
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", err.Error()); relErr != nil {
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
			// Compose Up to restore the old containers.
			restored := b.rollbackStackViaCompose(op)
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
					PrevStatus:  op.PrevStatus,
					Operation:   op.Operation,
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
				PrevStatus:        op.PrevStatus,
				ContainerIDs:      newContainerIDs,
				ServiceContainers: newServiceContainers,
				OnSuccess:         op.OnSuccess,
			},
		}
	}()

	// Per-service image setup.
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
	volBinds, _, volErr := b.setupStackVolBinds(ctx, op.LeaseUUID, op.Items, op.Profiles, imageSetups, op.Stack.Services, op.Logger)
	if volErr != nil {
		err = volErr
		callbackErr = op.Operation + " failed"
		return
	}

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
	if upErr := b.compose.Up(ctx, project, composeUpOpts{ForceRecreate: forceRecreate}); upErr != nil {
		err = fmt.Errorf("compose up failed: %w", upErr)
		callbackErr = op.Operation + " failed"
		return
	}

	// Discover new container IDs via Compose PS.
	containers, psErr := b.compose.PS(ctx, projectName)
	if psErr != nil {
		err = fmt.Errorf("compose ps failed: %w", psErr)
		callbackErr = op.Operation + " failed"
		return
	}

	newContainerIDs, newServiceContainers = mapComposeContainers(containers, op.Items)

	// Verify startup per-service so each service uses its own health check config.
	for svcName, svcCIDs := range newServiceContainers {
		svc := op.Stack.Services[svcName]
		if err = b.verifyStartup(ctx, svc, svcCIDs, op.Logger.With("service", svcName)); err != nil {
			callbackErr = startupErrorToCallbackMsg(err)
			return
		}
	}

	op.Logger.Info(op.Operation+" completed (stack)", "containers", len(newContainerIDs))
	return
}

// rollbackStackViaCompose restores the previous stack state by rebuilding a
// Compose project from the previous StackManifest (still in the provision,
// since OnSuccess hasn't run) and calling Compose Up. Returns true on success.
func (b *Backend) rollbackStackViaCompose(op replaceStackContainersOp) bool {
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
	volBinds, _, volErr := b.setupStackVolBinds(rollbackCtx, op.LeaseUUID, op.Items, op.Profiles, prevImageSetups, prevStack.Services, op.Logger)
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

// replaceContainersOp describes a container replacement operation with rollback.
// Used by both restart and update to share the stop → create → verify lifecycle.
type replaceContainersOp struct {
	LeaseUUID       string
	Manifest        *manifest.Manifest
	SKU             string
	Profile         SKUProfile
	OldContainerIDs []string
	Quantity        int                     // Number of new containers to create
	Operation       string                  // "restart" or "update" — used in log and callback messages
	PrevStatus      backend.ProvisionStatus // status before the operation began, for gauge accuracy
	Logger          *slog.Logger
	CustomDomain    string // Legacy single-item lease's custom domain (empty when not set)

	// OnSuccess is called under provisionsMu lock after successful replacement.
	// Used by update to set Image/Manifest on the provision. May be nil.
	OnSuccess func(prov *leasesm.ProvisionState)
}

// recordPreflightFailure logs the preflight error (e.g., profile lookup,
// image pull) and marks the latest release as failed. Provision-state
// mutations (LastError, FailCount, Status, persistDiagnostics) are
// handled by the SM entry action that fires when the caller returns its
// leasesm.ReplaceResult — see the preflight branches of doRestart / doRestartStack
// / doUpdate / doUpdateStack.
func (b *Backend) recordPreflightFailure(leaseUUID string, err error, logger *slog.Logger) {
	logger.Error("preflight failed", "error", err)

	if b.releaseStore != nil {
		if relErr := b.releaseStore.UpdateLatestStatus(leaseUUID, "failed", err.Error()); relErr != nil {
			logger.Warn("failed to update release status", "error", relErr)
		}
	}
}

// doReplaceContainers performs the container replacement lifecycle:
// inspect image → read metadata → setup networking → stop and rename old →
// create and start new → verify startup.
// Old containers are kept stopped for rollback on failure.
//
// Returns a leasesm.ReplaceResult (see lease_sm.go) carrying:
//   - err: non-nil on failure
//   - callbackErr: hardcoded on-chain-safe error string (failure path)
//   - restored: true if rollback succeeded (only meaningful when err != nil)
//   - success: containerIDs + OnSuccess populated when err == nil; the SM's
//     Ready entry action applies them
//   - failure: prevStatus, operation, oldStopped, callbackErr, lastError
//     populated when err != nil
//
// The goroutine wrapper picks the SM event from (err == nil, restored):
//   - err == nil               → evReplaceCompleted (Status → Ready)
//   - err != nil && restored   → evReplaceRecovered (Status → Ready, Failed callback)
//   - err != nil && !restored  → evReplaceFailed    (Status → Failed)
//
// All provision-struct mutations (Status, FailCount, LastError,
// ContainerIDs, OnSuccess) and the persistDiagnostics call live in
// the SM entry actions now — this function only does I/O (container
// ops, rollback attempt, release-store updates).
func (b *Backend) doReplaceContainers(ctx context.Context, op replaceContainersOp) (resultRet leasesm.ReplaceResult) {
	var err error
	var callbackErr string
	var newContainerIDs []string
	var oldStopped bool

	defer func() {
		if err != nil {
			op.Logger.Error(op.Operation+" failed", "error", err)

			// Mark release as failed.
			if b.releaseStore != nil {
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", err.Error()); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
				}
			}

			// Capture logs from the failed new containers BEFORE the
			// cleanup loop removes them. Index-based keys (nil keys
			// map → "0", "1", ...) for the single-manifest case.
			failureLogs := b.captureContainerLogs(newContainerIDs, nil)

			// Clean up failed new containers.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, cid := range newContainerIDs {
				if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
					op.Logger.Warn("failed to cleanup container after "+op.Operation+" error", "container_id", leasesm.ShortID(cid), "error", rmErr)
				}
			}

			// Rollback: restart old containers to restore service.
			restored := !oldStopped || b.rollbackContainers(op.LeaseUUID, op.OldContainerIDs, op.Logger)
			if restored {
				op.Logger.Info("rolled back to previous containers", "containers", len(op.OldContainerIDs))
				callbackErr += "; rolled back to previous version"
			} else if oldStopped {
				callbackErr += "; rollback failed"
			}

			resultRet = leasesm.ReplaceResult{
				CallbackErr: callbackErr,
				Err:         err,
				Restored:    restored,
				Failure: leasesm.ReplaceFailureInfo{
					PrevStatus:  op.PrevStatus,
					Operation:   op.Operation,
					OldStopped:  oldStopped,
					CallbackErr: callbackErr,
					LastError:   err.Error(),
					Logs:        failureLogs,
				},
			}
			return
		}

		// Success: remove old containers. Provision-struct mutations happen
		// in onEnterReadyFromReplaceCompleted.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, cid := range op.OldContainerIDs {
			if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
				op.Logger.Warn("failed to remove old container after "+op.Operation, "container_id", leasesm.ShortID(cid), "error", rmErr)
			}
		}

		// Mark release as active, previous as superseded.
		if b.releaseStore != nil {
			if relErr := b.releaseStore.ActivateLatest(op.LeaseUUID); relErr != nil {
				op.Logger.Warn("failed to update release status", "error", relErr)
			}
		}

		resultRet = leasesm.ReplaceResult{
			Success: leasesm.ReplaceSuccessResult{
				PrevStatus:   op.PrevStatus,
				ContainerIDs: newContainerIDs,
				OnSuccess:    op.OnSuccess,
			},
		}
	}()

	// Inspect image and resolve user.
	imgSetup, setupErr := b.inspectImageForSetup(ctx, op.Manifest.Image, op.Manifest.User)
	if setupErr != nil {
		err = setupErr
		callbackErr = op.Operation + " failed"
		return
	}
	if len(imgSetup.WritablePaths) > 0 {
		op.Logger.Info("auto-detected writable paths", "paths", imgSetup.WritablePaths, "uid", imgSetup.VolumeUID)
	}

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

	// Set up networking.
	networkConfig, netErr := b.ensureNetworkConfig(ctx, tenant)
	if netErr != nil {
		err = netErr
		callbackErr = op.Operation + " failed"
		return
	}

	// Stop and rename old containers to free the canonical name for replacements.
	// Old containers are kept stopped for rollback on failure.
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	for i, cid := range op.OldContainerIDs {
		op.Logger.Info("stopping container for "+op.Operation, "container_id", leasesm.ShortID(cid))
		if stopErr := b.docker.StopContainer(ctx, cid, stopTimeout); stopErr != nil {
			err = fmt.Errorf("failed to stop container %s: %w", leasesm.ShortID(cid), stopErr)
			callbackErr = op.Operation + " failed"
			return
		}
		oldStopped = true
		if renameErr := b.docker.RenameContainer(ctx, cid, prevContainerName(op.LeaseUUID, i)); renameErr != nil {
			err = fmt.Errorf("failed to rename old container %s: %w", leasesm.ShortID(cid), renameErr)
			callbackErr = op.Operation + " failed"
			return
		}
	}

	// Create and start new containers.
	newContainerIDs = make([]string, 0, op.Quantity)
	for i := range op.Quantity {
		volumeBinds, volErr := b.setupVolumeBinds(ctx, op.LeaseUUID, i, op.Profile.DiskMB, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
		if volErr != nil {
			err = volErr
			callbackErr = op.Operation + " failed"
			return
		}

		var writablePathBinds map[string]string
		if len(imgSetup.WritablePaths) > 0 {
			volumeID := fmt.Sprintf("fred-%s-%d", op.LeaseUUID, i)
			sizeMB := op.Profile.DiskMB
			if sizeMB <= 0 {
				sizeMB = int64(b.cfg.GetTmpfsSizeMB())
			}
			hostPath, _, wpVolErr := b.volumes.Create(ctx, volumeID, sizeMB)
			if wpVolErr == nil {
				writablePathBinds = b.setupWritablePathBinds(ctx, op.Manifest.Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024)
			} else {
				op.Logger.Warn("writable path content seeding unavailable on "+op.Operation, "error", wpVolErr)
			}
		}

		containerID, createErr := b.docker.CreateContainer(ctx, CreateContainerParams{
			LeaseUUID:         op.LeaseUUID,
			Tenant:            tenant,
			ProviderUUID:      providerUUID,
			SKU:               op.SKU,
			Manifest:          op.Manifest,
			Profile:           op.Profile,
			InstanceIndex:     i,
			FailCount:         failCount,
			CallbackURL:       callbackURL,
			HostBindIP:        b.cfg.GetHostBindIP(),
			ReadonlyRootfs:    b.cfg.IsReadonlyRootfs(),
			PidsLimit:         b.cfg.GetPidsLimit(),
			TmpfsSizeMB:       b.cfg.GetTmpfsSizeMB(),
			NetworkConfig:     networkConfig,
			VolumeBinds:       volumeBinds,
			ImageVolumes:      imgSetup.Volumes,
			WritablePathBinds: writablePathBinds,
			User:              imgSetup.ContainerUser,
			BackendName:       b.cfg.Name,
			Ingress:           b.cfg.Ingress,
			NetworkName:       TenantNetworkName(tenant),
			Quantity:          op.Quantity,
			CustomDomain:      op.CustomDomain,
		}, b.cfg.ContainerCreateTimeout)
		if createErr != nil {
			err = fmt.Errorf("container creation failed (instance %d): %w", i, createErr)
			callbackErr = op.Operation + " failed"
			return
		}
		newContainerIDs = append(newContainerIDs, containerID)

		if startErr := b.docker.StartContainer(ctx, containerID, b.cfg.ContainerStartTimeout); startErr != nil {
			err = fmt.Errorf("container start failed (instance %d): %w", i, startErr)
			callbackErr = op.Operation + " failed"
			return
		}
	}

	// Startup verification.
	if err = b.verifyStartup(ctx, op.Manifest, newContainerIDs, op.Logger); err != nil {
		callbackErr = startupErrorToCallbackMsg(err)
		return
	}

	op.Logger.Info(op.Operation+" completed", "containers", len(newContainerIDs))
	return
}

// Update deploys a new manifest for a lease, replacing containers.
// State machine: Ready|Failed → Updating → Ready|Failed
//
// ARCHITECTURAL SEAM — see the extended comment on Backend.Restart for
// the rationale. Like Restart, Update writes prov.Status to Updating
// synchronously under provisionsMu BEFORE routing to the lease actor,
// for fast-fail semantics and the "Update() returns => Status is
// Updating" contract. Compensated by onEnterFailing's Status recheck
// (lease_sm.go) + lease_failing_race_skipped_total metric. Don't
// refactor without an operational trigger.
func (b *Backend) Update(ctx context.Context, req backend.UpdateRequest) error {
	logger := b.logger.With("lease_uuid", req.LeaseUUID)

	// Synchronous phase: validate state and new manifest
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

	isStack := prov.IsStack()

	// Boundary normalization (defensive): if prov.Items is populated, it
	// was normalized at provision time — re-check here so any malformed
	// in-memory state (e.g., a recovered provision whose stored items
	// predate Task 3) is surfaced before triggering an update. Empty
	// prov.Items is tolerated because pre-Task-3 legacy provisions and
	// recovered legacy state may not carry items; the downstream code
	// keeps working with the existing prov.Manifest in that case. Task 6
	// folds this defensive check into the unified update path.
	if len(prov.Items) > 0 {
		if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: prov.Items}); err != nil {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %w", backend.ErrInvalidState, err)
		}
	}

	// Parse new payload. ParsePayload always returns a *StackManifest now;
	// legacy flat payloads are auto-wrapped under DefaultServiceName.
	stackManifest, parseErr := manifest.ParsePayload(req.Payload)
	if parseErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, parseErr)
	}

	// For legacy provisions, derive the single per-service manifest from the
	// auto-wrapped stack. A multi-service payload submitted against a legacy
	// provision is a wire-level mismatch — surface it the same way the
	// pre-Task-2 validator did. Tasks 3-7 collapse this branch.
	var m *manifest.Manifest
	if !isStack {
		m = stackManifest.Services[manifest.DefaultServiceName]
		if m == nil || len(stackManifest.Services) != 1 {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: non-stack lease cannot be updated with a stack manifest", backend.ErrInvalidManifest)
		}
	}

	if isStack {
		// Validate stack against stored items. Now that ParsePayload
		// auto-wraps flat payloads under DefaultServiceName, a flat payload
		// submitted against a stack lease falls through to this check
		// (auto-wrapped service name "app" mismatches the lease's named
		// services), so we wrap with ErrInvalidManifest — matching
		// provision.go's pattern and preserving the pre-Task-2 error
		// category for the payload-type-mismatch case.
		if valErr := manifest.ValidateStackAgainstItems(stackManifest, prov.Items); valErr != nil {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, valErr)
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
		prevStatus := prov.Status
		prevCallbackURL := prov.CallbackURL
		prov.Status = backend.ProvisionStatusUpdating
		if req.CallbackURL != "" {
			prov.CallbackURL = req.CallbackURL
		}
		b.provisionsMu.Unlock()

		// Record release.
		releaseImage := "stack"
		if b.releaseStore != nil {
			if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
				Manifest:  req.Payload,
				Image:     releaseImage,
				Status:    "deploying",
				CreatedAt: time.Now(),
			}); relErr != nil {
				b.provisionsMu.Lock()
				prov.Status = prevStatus
				prov.CallbackURL = prevCallbackURL
				b.provisionsMu.Unlock()
				return fmt.Errorf("failed to record release: %w", relErr)
			}
		}

		// Hand off to the actor. See handleUpdateRequested /
		// spawnReplaceWorker.
		opCtx, opCancel := b.shutdownAwareContext()
		work := func() leasesm.ReplaceResult {
			return b.doUpdateStack(opCtx, req.LeaseUUID, stackManifest, profiles, oldContainerIDs, serviceContainers, items, prevStatus, logger)
		}
		ack := make(chan error, 1)
		if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.UpdateRequestedMsg{Cancel: opCancel, Work: work, Ack: ack}); routeErr != nil {
			opCancel()
			b.restartRollback(req.LeaseUUID, prevStatus, prevCallbackURL, routeErr, logger)
			return routeErr
		}
		// See ackOrAbort's comment for the ctx-vs-ack race rationale.
		if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
			opCancel()
			b.restartRollback(req.LeaseUUID, prevStatus, prevCallbackURL, err, logger)
			return err
		}
		return nil
	}

	// Single-manifest path.
	if imgErr := shared.ValidateImage(m.Image, b.cfg.AllowedRegistries); imgErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrValidation, imgErr)
	}

	profile, profErr := b.cfg.GetSKUProfile(prov.SKU)
	if profErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrValidation, profErr)
	}

	oldContainerIDs := append([]string(nil), prov.ContainerIDs...)
	prevStatus := prov.Status
	prevCallbackURL := prov.CallbackURL
	prov.Status = backend.ProvisionStatusUpdating
	if req.CallbackURL != "" {
		prov.CallbackURL = req.CallbackURL
	}
	b.provisionsMu.Unlock()

	if b.releaseStore != nil {
		if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
			Manifest:  req.Payload,
			Image:     m.Image,
			Status:    "deploying",
			CreatedAt: time.Now(),
		}); relErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			prov.CallbackURL = prevCallbackURL
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to record release: %w", relErr)
		}
	}

	// Hand off to the actor. See handleUpdateRequested /
	// spawnReplaceWorker.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doUpdate(opCtx, req.LeaseUUID, m, profile, oldContainerIDs, prevStatus, logger)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.UpdateRequestedMsg{Cancel: opCancel, Work: work, Ack: ack}); routeErr != nil {
		opCancel()
		b.restartRollback(req.LeaseUUID, prevStatus, prevCallbackURL, routeErr, logger)
		return routeErr
	}
	// See ackOrAbort's comment for the ctx-vs-ack race rationale.
	if accepted, err := b.ackOrAbort(ctx, ack); !accepted {
		opCancel()
		b.restartRollback(req.LeaseUUID, prevStatus, prevCallbackURL, err, logger)
		return err
	}
	return nil
}

// doUpdate performs the actual container update asynchronously.
func (b *Backend) doUpdate(ctx context.Context, leaseUUID string, m *manifest.Manifest, profile SKUProfile, oldContainerIDs []string, prevStatus backend.ProvisionStatus, logger *slog.Logger) leasesm.ReplaceResult {
	// Pull new image — this is the only update-specific pre-flight step.
	logger.Info("pulling image for update", "image", m.Image)
	if pullErr := b.docker.PullImage(ctx, m.Image, b.cfg.ImagePullTimeout); pullErr != nil {
		err := fmt.Errorf("image pull failed: %w", pullErr)
		b.recordPreflightFailure(leaseUUID, err, logger)
		// Update preflight failure: force Status=Failed unconditionally
		// even though the old containers are still running. The user's
		// desired state (the new image) was not achieved; the lease is
		// semantically Failed until the user retries with a good image.
		// This differs from Restart preflight, where Status stays at
		// prevStatus because the operation's goal (a fresh container
		// start with the SAME image) is independent of whether the
		// operator's intent-to-update was achievable.
		return leasesm.ReplaceResult{
			CallbackErr: "image pull failed",
			Err:         err,
			Restored:    false,
			Failure: leasesm.ReplaceFailureInfo{
				PrevStatus:  prevStatus,
				Operation:   "update",
				CallbackErr: "image pull failed",
				LastError:   err.Error(),
			},
		}
	}

	// Read SKU, quantity, and (legacy) custom domain from provision.
	b.provisionsMu.RLock()
	sku := ""
	quantity := len(oldContainerIDs)
	customDomain := ""
	if prov, ok := b.provisions[leaseUUID]; ok {
		sku = prov.SKU
		quantity = prov.Quantity
		if len(prov.Items) > 0 {
			customDomain = prov.Items[0].CustomDomain
		}
	}
	b.provisionsMu.RUnlock()

	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:       leaseUUID,
		Manifest:        m,
		SKU:             sku,
		Profile:         profile,
		OldContainerIDs: oldContainerIDs,
		Quantity:        quantity,
		Operation:       "update",
		PrevStatus:      prevStatus,
		Logger:          logger,
		CustomDomain:    customDomain,
		OnSuccess: func(prov *leasesm.ProvisionState) {
			prov.Image = m.Image
			prov.Manifest = m
		},
	})
}

// doUpdateStack performs the actual stack container update asynchronously.
func (b *Backend) doUpdateStack(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, profiles map[string]SKUProfile, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, prevStatus backend.ProvisionStatus, logger *slog.Logger) leasesm.ReplaceResult {
	// Pull each unique image (deduplicated).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image for update", "service", svcName, "image", svc.Image)
		if pullErr := b.docker.PullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); pullErr != nil {
			err := fmt.Errorf("image pull failed for service %s: %w", svcName, pullErr)
			b.recordPreflightFailure(leaseUUID, err, logger)
			// Same semantics as single-manifest update preflight: force
			// Status=Failed unconditionally since the user's desired
			// state (the new image set) was not achieved.
			return leasesm.ReplaceResult{
				CallbackErr: "image pull failed",
				Err:         err,
				Restored:    false,
				Failure: leasesm.ReplaceFailureInfo{
					PrevStatus:  prevStatus,
					Operation:   "update",
					CallbackErr: "image pull failed",
					LastError:   err.Error(),
				},
			}
		}
		pulledImages[svc.Image] = true
	}

	return b.doReplaceStackContainers(ctx, replaceStackContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "update",
		PrevStatus:        prevStatus,
		Logger:            logger,
		OnSuccess: func(prov *leasesm.ProvisionState) {
			prov.StackManifest = stack
		},
	})
}
