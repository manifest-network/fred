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
)

// Restart restarts containers for a lease without changing the manifest.
// State machine: Ready|Failed → Restarting → Ready|Failed
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
	if prov.Manifest == nil && prov.StackManifest == nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: no stored manifest for restart", backend.ErrInvalidState)
	}
	isStack := prov.IsStack()
	prevStatus := prov.Status
	prevCallbackURL := prov.CallbackURL
	prov.Status = backend.ProvisionStatusRestarting
	if req.CallbackURL != "" {
		prov.CallbackURL = req.CallbackURL
	}
	manifest := prov.Manifest
	stackManifest := prov.StackManifest
	containerIDs := append([]string(nil), prov.ContainerIDs...)
	serviceContainers := make(map[string][]string, len(prov.ServiceContainers))
	for k, v := range prov.ServiceContainers {
		serviceContainers[k] = append([]string(nil), v...)
	}
	items := append([]backend.LeaseItem(nil), prov.Items...)
	sku := prov.SKU
	b.provisionsMu.Unlock()

	// Record restart release as deploying. Abort if this fails — without a
	// release record, ActivateLatest after success is a no-op, and a cold
	// restart would recover the previous manifest (silently rolling back).
	if b.releaseStore != nil {
		var manifestBytes []byte
		var marshalErr error
		var releaseImage string
		if isStack {
			manifestBytes, marshalErr = json.Marshal(stackManifest)
			releaseImage = "stack"
		} else {
			manifestBytes, marshalErr = json.Marshal(manifest)
			releaseImage = manifest.Image
		}
		if marshalErr != nil {
			b.provisionsMu.Lock()
			prov.Status = prevStatus
			prov.CallbackURL = prevCallbackURL
			b.provisionsMu.Unlock()
			return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)
		}
		if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
			Manifest:  manifestBytes,
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

	// Async phase
	b.wg.Go(func() {
		ctx, cancel := b.shutdownAwareContext()
		defer cancel()

		if isStack {
			b.doRestartStack(ctx, req.LeaseUUID, stackManifest, containerIDs, serviceContainers, items, prevStatus, logger)
		} else {
			b.doRestart(ctx, req.LeaseUUID, manifest, containerIDs, sku, prevStatus, logger)
		}
	})

	return nil
}

// doRestart performs the actual container restart asynchronously.
func (b *Backend) doRestart(ctx context.Context, leaseUUID string, manifest *DockerManifest, oldContainerIDs []string, sku string, prevStatus backend.ProvisionStatus, logger *slog.Logger) {
	profile, profErr := b.cfg.GetSKUProfile(sku)
	if profErr != nil {
		b.recordPreflightFailure(leaseUUID, "restart failed",
			fmt.Errorf("SKU profile lookup failed: %w", profErr),
			prevStatus, logger)
		return
	}

	b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:       leaseUUID,
		Manifest:        manifest,
		SKU:             sku,
		Profile:         profile,
		OldContainerIDs: oldContainerIDs,
		Quantity:        len(oldContainerIDs),
		Operation:       "restart",
		PrevStatus:      prevStatus,
		Logger:          logger,
	})
}

// doRestartStack performs an async stack restart: stops all service containers
// and recreates them from the stored StackManifest.
func (b *Backend) doRestartStack(ctx context.Context, leaseUUID string, stack *StackManifest, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, prevStatus backend.ProvisionStatus, logger *slog.Logger) {
	profiles := make(map[string]SKUProfile, len(items))
	for _, item := range items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, profErr := b.cfg.GetSKUProfile(item.SKU)
		if profErr != nil {
			b.recordPreflightFailure(leaseUUID, "restart failed",
				fmt.Errorf("SKU profile lookup failed for %s: %w", item.SKU, profErr),
				prevStatus, logger)
			return
		}
		profiles[item.SKU] = profile
	}

	b.doReplaceStackContainers(ctx, replaceStackContainersOp{
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
	Stack             *StackManifest
	Items             []backend.LeaseItem
	Profiles          map[string]SKUProfile
	OldContainerIDs   []string
	ServiceContainers map[string][]string     // old service → container IDs mapping
	Operation         string                  // "restart" or "update"
	PrevStatus        backend.ProvisionStatus // status before the operation began, for gauge accuracy
	Logger            *slog.Logger

	// OnSuccess is called under provisionsMu lock after successful replacement.
	OnSuccess func(prov *provision)
}

// doReplaceStackContainers performs the stack container replacement lifecycle
// using Docker Compose. Compose handles stopping old containers and starting
// new ones via a single Up call, with rollback via Up with the previous manifest.
func (b *Backend) doReplaceStackContainers(ctx context.Context, op replaceStackContainersOp) {
	var err error
	var callbackErr string
	var newContainerIDs []string
	var imageSetups map[string]*imageSetup
	newServiceContainers := make(map[string][]string)
	projectName := composeProjectName(op.LeaseUUID)

	defer func() {
		if err != nil {
			op.Logger.Error(op.Operation+" failed (stack)", "error", err)

			var diagSnap shared.DiagnosticEntry
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				prov.LastError = err.Error()
				prov.FailCount++
				diagSnap = diagnosticSnapshot(prov)
			}
			b.provisionsMu.Unlock()
			b.persistDiagnostics(diagSnap, newContainerIDs, stackContainerLogKeys(newServiceContainers))

			if b.releaseStore != nil {
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", err.Error()); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
				}
			}

			// Rollback: rebuild the Project from the previous StackManifest and
			// Compose Up to restore the old containers.
			restored := b.rollbackStackViaCompose(op)
			var callbackURL string
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				if restored {
					prov.Status = backend.ProvisionStatusReady
					if op.Operation == "restart" {
						prov.LastError = ""
					}
					op.Logger.Info("rolled back to previous containers via compose (stack)")
				} else {
					prov.Status = backend.ProvisionStatusFailed
					if op.PrevStatus == backend.ProvisionStatusReady {
						activeProvisions.Dec()
					}
				}
				callbackURL = prov.CallbackURL
			}
			b.provisionsMu.Unlock()

			if restored {
				callbackErr += "; rolled back to previous version"
			} else {
				callbackErr += "; rollback failed"
			}

			b.sendCallbackWithURL(op.LeaseUUID, callbackURL, backend.CallbackStatusFailed, callbackErr)
			return
		}

		// Success: update provision (Compose already replaced old containers).
		var callbackURL string
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[op.LeaseUUID]; ok {
			prov.ContainerIDs = newContainerIDs
			prov.ServiceContainers = newServiceContainers
			prov.Status = backend.ProvisionStatusReady
			prov.LastError = ""
			if op.PrevStatus == backend.ProvisionStatusFailed {
				activeProvisions.Inc()
			}
			if op.OnSuccess != nil {
				op.OnSuccess(prov)
			}
			callbackURL = prov.CallbackURL
		}
		b.provisionsMu.Unlock()

		if b.releaseStore != nil {
			if relErr := b.releaseStore.ActivateLatest(op.LeaseUUID); relErr != nil {
				op.Logger.Warn("failed to update release status", "error", relErr)
			}
		}

		b.sendCallbackWithURL(op.LeaseUUID, callbackURL, backend.CallbackStatusSuccess, "")
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
		if _, netErr := b.docker.EnsureTenantNetwork(ctx, tenant); netErr != nil {
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
	Manifest        *DockerManifest
	SKU             string
	Profile         SKUProfile
	OldContainerIDs []string
	Quantity        int                     // Number of new containers to create
	Operation       string                  // "restart" or "update" — used in log and callback messages
	PrevStatus      backend.ProvisionStatus // status before the operation began, for gauge accuracy
	Logger          *slog.Logger

	// OnSuccess is called under provisionsMu lock after successful replacement.
	// Used by update to set Image/Manifest on the provision. May be nil.
	OnSuccess func(prov *provision)
}

// recordPreflightFailure handles errors that occur before any containers are modified
// (e.g., profile lookup, image pull). It records LastError, persists diagnostics,
// updates release status, and sends a failure callback with callbackMsg.
// Because no containers were modified, the provision's status is restored to
// prevStatus (the status before the operation began) so that the observable
// state and activeProvisions gauge remain accurate.
func (b *Backend) recordPreflightFailure(leaseUUID string, callbackMsg string, err error, prevStatus backend.ProvisionStatus, logger *slog.Logger) {
	logger.Error("preflight failed", "error", err)

	var diagSnap shared.DiagnosticEntry
	var callbackURL string
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[leaseUUID]; ok {
		prov.LastError = err.Error()
		prov.FailCount++
		prov.Status = prevStatus
		diagSnap = diagnosticSnapshot(prov)
		callbackURL = prov.CallbackURL
	}
	b.provisionsMu.Unlock()
	b.persistDiagnostics(diagSnap, nil)

	if b.releaseStore != nil {
		if relErr := b.releaseStore.UpdateLatestStatus(leaseUUID, "failed", err.Error()); relErr != nil {
			logger.Warn("failed to update release status", "error", relErr)
		}
	}

	b.sendCallbackWithURL(leaseUUID, callbackURL, backend.CallbackStatusFailed, callbackMsg)
}

// doReplaceContainers performs the container replacement lifecycle:
// inspect image → read metadata → setup networking → stop and rename old →
// create and start new → verify startup.
// Old containers are kept stopped for rollback on failure.
func (b *Backend) doReplaceContainers(ctx context.Context, op replaceContainersOp) {
	var err error
	var callbackErr string
	var newContainerIDs []string
	var oldStopped bool

	defer func() {
		if err != nil {
			op.Logger.Error(op.Operation+" failed", "error", err)

			// Snapshot diagnostics under lock, then persist outside (I/O).
			var diagSnap shared.DiagnosticEntry
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				prov.LastError = err.Error()
				prov.FailCount++
				diagSnap = diagnosticSnapshot(prov)
			}
			b.provisionsMu.Unlock()
			b.persistDiagnostics(diagSnap, newContainerIDs)

			// Mark release as failed.
			if b.releaseStore != nil {
				if relErr := b.releaseStore.UpdateLatestStatus(op.LeaseUUID, "failed", err.Error()); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
				}
			}

			// Clean up failed new containers.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, cid := range newContainerIDs {
				if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
					op.Logger.Warn("failed to cleanup container after "+op.Operation+" error", "container_id", shortID(cid), "error", rmErr)
				}
			}

			// Rollback: restart old containers to restore service.
			restored := !oldStopped || b.rollbackContainers(op.LeaseUUID, op.OldContainerIDs, op.Logger)
			var callbackURL string
			b.provisionsMu.Lock()
			if prov, ok := b.provisions[op.LeaseUUID]; ok {
				if restored {
					prov.Status = backend.ProvisionStatusReady
					// Restart: clear error — we're back to the exact same state.
					// Update: keep LastError so the UI shows why the update failed.
					if oldStopped && op.Operation == "restart" {
						prov.LastError = ""
					}
				} else {
					prov.Status = backend.ProvisionStatusFailed
					// Adjust gauge: the provision was Ready before the
					// operation and is now Failed with no rollback. If it
					// was already Failed (restart-from-failed), the gauge
					// was already decremented during the original failure.
					if op.PrevStatus == backend.ProvisionStatusReady {
						activeProvisions.Dec()
					}
				}
				if restored {
					op.Logger.Info("rolled back to previous containers", "containers", len(op.OldContainerIDs))
				}
				callbackURL = prov.CallbackURL
			}
			b.provisionsMu.Unlock()

			if restored {
				callbackErr += "; rolled back to previous version"
			} else if oldStopped {
				callbackErr += "; rollback failed"
			}

			b.sendCallbackWithURL(op.LeaseUUID, callbackURL, backend.CallbackStatusFailed, callbackErr)
			return
		}

		// Success: remove old containers and update provision.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, cid := range op.OldContainerIDs {
			if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
				op.Logger.Warn("failed to remove old container after "+op.Operation, "container_id", shortID(cid), "error", rmErr)
			}
		}

		var callbackURL string
		b.provisionsMu.Lock()
		if prov, ok := b.provisions[op.LeaseUUID]; ok {
			prov.ContainerIDs = newContainerIDs
			prov.Status = backend.ProvisionStatusReady
			prov.LastError = ""
			// Adjust gauge: the provision was Failed and is now back to
			// Ready after a successful restart/update.
			if op.PrevStatus == backend.ProvisionStatusFailed {
				activeProvisions.Inc()
			}
			if op.OnSuccess != nil {
				op.OnSuccess(prov)
			}
			callbackURL = prov.CallbackURL
		}
		b.provisionsMu.Unlock()

		// Mark release as active, previous as superseded.
		if b.releaseStore != nil {
			if relErr := b.releaseStore.ActivateLatest(op.LeaseUUID); relErr != nil {
				op.Logger.Warn("failed to update release status", "error", relErr)
			}
		}

		b.sendCallbackWithURL(op.LeaseUUID, callbackURL, backend.CallbackStatusSuccess, "")
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
		op.Logger.Info("stopping container for "+op.Operation, "container_id", shortID(cid))
		if stopErr := b.docker.StopContainer(ctx, cid, stopTimeout); stopErr != nil {
			err = fmt.Errorf("failed to stop container %s: %w", shortID(cid), stopErr)
			callbackErr = op.Operation + " failed"
			return
		}
		oldStopped = true
		if renameErr := b.docker.RenameContainer(ctx, cid, prevContainerName(op.LeaseUUID, i)); renameErr != nil {
			err = fmt.Errorf("failed to rename old container %s: %w", shortID(cid), renameErr)
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
}

// Update deploys a new manifest for a lease, replacing containers.
// State machine: Ready|Failed → Updating → Ready|Failed
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

	// Parse new payload (auto-detects single vs stack).
	manifest, stackManifest, parseErr := ParsePayload(req.Payload)
	if parseErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, parseErr)
	}

	// Ensure payload type matches existing provision type.
	if isStack && stackManifest == nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: stack lease requires a stack manifest (with services key)", backend.ErrInvalidManifest)
	}
	if !isStack && stackManifest != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: non-stack lease cannot be updated with a stack manifest", backend.ErrInvalidManifest)
	}

	if isStack {
		// Validate stack against stored items.
		if valErr := ValidateStackAgainstItems(stackManifest, prov.Items); valErr != nil {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %w", backend.ErrValidation, valErr)
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

		// Async phase
		b.wg.Go(func() {
			ctx, cancel := b.shutdownAwareContext()
			defer cancel()

			b.doUpdateStack(ctx, req.LeaseUUID, stackManifest, profiles, oldContainerIDs, serviceContainers, items, prevStatus, logger)
		})
		return nil
	}

	// Legacy single-manifest path.
	if imgErr := shared.ValidateImage(manifest.Image, b.cfg.AllowedRegistries); imgErr != nil {
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
			Image:     manifest.Image,
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

	b.wg.Go(func() {
		ctx, cancel := b.shutdownAwareContext()
		defer cancel()

		b.doUpdate(ctx, req.LeaseUUID, manifest, profile, oldContainerIDs, prevStatus, logger)
	})

	return nil
}

// doUpdate performs the actual container update asynchronously.
func (b *Backend) doUpdate(ctx context.Context, leaseUUID string, manifest *DockerManifest, profile SKUProfile, oldContainerIDs []string, prevStatus backend.ProvisionStatus, logger *slog.Logger) {
	// Pull new image — this is the only update-specific pre-flight step.
	logger.Info("pulling image for update", "image", manifest.Image)
	if pullErr := b.docker.PullImage(ctx, manifest.Image, b.cfg.ImagePullTimeout); pullErr != nil {
		b.recordPreflightFailure(leaseUUID, "image pull failed",
			fmt.Errorf("image pull failed: %w", pullErr),
			prevStatus, logger)
		return
	}

	// Read SKU and quantity from provision (may differ from old container count).
	b.provisionsMu.RLock()
	sku := ""
	quantity := len(oldContainerIDs)
	if prov, ok := b.provisions[leaseUUID]; ok {
		sku = prov.SKU
		quantity = prov.Quantity
	}
	b.provisionsMu.RUnlock()

	b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:       leaseUUID,
		Manifest:        manifest,
		SKU:             sku,
		Profile:         profile,
		OldContainerIDs: oldContainerIDs,
		Quantity:        quantity,
		Operation:       "update",
		PrevStatus:      prevStatus,
		Logger:          logger,
		OnSuccess: func(prov *provision) {
			prov.Image = manifest.Image
			prov.Manifest = manifest
		},
	})
}

// doUpdateStack performs the actual stack container update asynchronously.
func (b *Backend) doUpdateStack(ctx context.Context, leaseUUID string, stack *StackManifest, profiles map[string]SKUProfile, oldContainerIDs []string, serviceContainers map[string][]string, items []backend.LeaseItem, prevStatus backend.ProvisionStatus, logger *slog.Logger) {
	// Pull each unique image (deduplicated).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image for update", "service", svcName, "image", svc.Image)
		if pullErr := b.docker.PullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); pullErr != nil {
			b.recordPreflightFailure(leaseUUID, "image pull failed",
				fmt.Errorf("image pull failed for service %s: %w", svcName, pullErr),
				prevStatus, logger)
			return
		}
		pulledImages[svc.Image] = true
	}

	b.doReplaceStackContainers(ctx, replaceStackContainersOp{
		LeaseUUID:         leaseUUID,
		Stack:             stack,
		Items:             items,
		Profiles:          profiles,
		OldContainerIDs:   oldContainerIDs,
		ServiceContainers: serviceContainers,
		Operation:         "update",
		PrevStatus:        prevStatus,
		Logger:            logger,
		OnSuccess: func(prov *provision) {
			prov.StackManifest = stack
		},
	})
}
