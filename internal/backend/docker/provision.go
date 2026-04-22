package docker

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	networktypes "github.com/docker/docker/api/types/network"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Provision starts async provisioning of containers.
// For multi-unit leases (quantity > 1), multiple containers are created.
// For multi-SKU leases, containers are created with the appropriate profile for each SKU.
//
// Pre-flight validation errors (unknown SKU, invalid manifest, disallowed image,
// insufficient resources) are returned synchronously so the caller can respond
// with an appropriate HTTP status. Only truly asynchronous failures (image pull,
// container create/start) are communicated via callback.
func (b *Backend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	totalQuantity := req.TotalQuantity()

	logger := b.logger.With(
		"lease_uuid", req.LeaseUUID,
		"tenant", req.Tenant,
		"items", len(req.Items),
		"total_quantity", totalQuantity,
	)

	// Atomically check-and-reserve the provision slot (fixes TOCTOU race).
	// Allow re-provisioning if the existing provision has failed (e.g., container
	// crashed and reconciler is retrying). Deprovisioning, Provisioning,
	// Restarting, Updating, and Ready are all in-flight or live states that
	// must not be re-provisioned until they reach Failed or are removed.
	var prevFailCount int
	var oldContainerIDs []string
	var oldQuantity int
	var oldItems []backend.LeaseItem // non-nil for stacks, needed for service-aware release
	b.provisionsMu.Lock()
	if existing, exists := b.provisions[req.LeaseUUID]; exists {
		if existing.Status != backend.ProvisionStatusFailed {
			b.provisionsMu.Unlock()
			return fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID)
		}
		// Capture data needed for cleanup, then release lock before Docker API calls.
		prevFailCount = existing.FailCount
		oldContainerIDs = existing.ContainerIDs
		oldQuantity = existing.Quantity
		oldItems = existing.Items
		delete(b.provisions, req.LeaseUUID)
	}
	b.provisions[req.LeaseUUID] = &provision{
		LeaseUUID:    req.LeaseUUID,
		Tenant:       req.Tenant,
		ProviderUUID: req.ProviderUUID,
		Status:       backend.ProvisionStatusProvisioning,
		Quantity:     totalQuantity,
		ContainerIDs: make([]string, 0, totalQuantity),
		CreatedAt:    time.Now(),
		FailCount:    prevFailCount,
		CallbackURL:  req.CallbackURL,
	}
	b.provisionsMu.Unlock()

	// Clean up old failed provision resources outside the lock.
	if oldQuantity > 0 {
		if len(oldItems) > 0 {
			// Stack: release service-aware allocation IDs.
			for _, item := range oldItems {
				for i := range item.Quantity {
					b.pool.Release(fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i))
				}
			}
		} else {
			// Legacy: release index-based allocation IDs.
			for i := range oldQuantity {
				b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
			}
		}
		// Remove old containers but keep volumes — stateful data persists
		// across re-provisions. Volumes are reused via idempotent Create in
		// doProvision, and only destroyed on explicit deprovision.
		for _, cid := range oldContainerIDs {
			if err := b.docker.RemoveContainer(ctx, cid); err != nil {
				logger.Warn("failed to remove old container during re-provision",
					"container_id", shortID(cid), "error", err)
			}
		}
		logger.Info("replacing failed provision",
			"fail_count", prevFailCount,
		)
	}

	// Validate all SKUs upfront and build profile map.
	// On failure, remove the reservation and return error synchronously.
	profiles := make(map[string]SKUProfile)
	for _, item := range req.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue // Already validated
		}
		profile, err := b.cfg.GetSKUProfile(item.SKU)
		if err != nil {
			b.removeProvision(req.LeaseUUID)
			return fmt.Errorf("%w: %w", backend.ErrValidation, err)
		}
		profiles[item.SKU] = profile
	}

	// Parse payload — auto-detects single manifest vs stack manifest.
	isStack, err := backend.IsStack(req.Items)
	if err != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	manifest, stackManifest, parseErr := ParsePayload(req.Payload)
	if parseErr != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, parseErr)
	}

	// Validate payload type matches lease items.
	if isStack && stackManifest == nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: lease items have service names but payload is not a stack manifest", backend.ErrInvalidManifest)
	}
	if !isStack && stackManifest != nil {
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("%w: payload is a stack manifest but lease items have no service names", backend.ErrInvalidManifest)
	}

	// Validate images against registry allowlist.
	if isStack {
		if err := ValidateStackAgainstItems(stackManifest, req.Items); err != nil {
			b.removeProvision(req.LeaseUUID)
			return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
		}
		for svcName, svc := range stackManifest.Services {
			if err := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); err != nil {
				b.removeProvision(req.LeaseUUID)
				return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, err)
			}
		}
	} else {
		if err := shared.ValidateImage(manifest.Image, b.cfg.AllowedRegistries); err != nil {
			b.removeProvision(req.LeaseUUID)
			return fmt.Errorf("%w: %w", backend.ErrValidation, err)
		}
	}

	// Try to allocate resources for all instances.
	// Stack uses service-aware allocation IDs: {leaseUUID}-{serviceName}-{instanceIndex}
	var allocatedIDs []string
	if isStack {
		for _, item := range req.Items {
			for i := range item.Quantity {
				instanceID := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
				if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
					for _, id := range allocatedIDs {
						b.pool.Release(id)
					}
					b.removeProvision(req.LeaseUUID)
					return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err)
				}
				allocatedIDs = append(allocatedIDs, instanceID)
			}
		}
	} else {
		instanceIdx := 0
		for _, item := range req.Items {
			for range item.Quantity {
				instanceID := fmt.Sprintf("%s-%d", req.LeaseUUID, instanceIdx)
				if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
					for _, id := range allocatedIDs {
						b.pool.Release(id)
					}
					b.removeProvision(req.LeaseUUID)
					return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err)
				}
				allocatedIDs = append(allocatedIDs, instanceID)
				instanceIdx++
			}
		}
	}

	// Update the reservation with full details now that validation passed.
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		prov.SKU = req.RoutingSKU()
		if isStack {
			prov.StackManifest = stackManifest
			prov.Items = req.Items
		} else {
			prov.Image = manifest.Image
		}
	}
	b.provisionsMu.Unlock()

	// Start async provisioning with shutdown-aware context.
	// The cancel function is stored on the actor so Provisioning.OnExit can
	// cancel it on DeprovisionRequested preemption — the structural
	// cc62f3b-class suppression for the Provision flow. On completion, the
	// goroutine fires the matching event into the actor's inbox; the SM's
	// Ready/Failed entry actions emit the terminal callback.
	//
	// The provisionRequestedMsg is sent *before* the goroutine spawns so
	// the SM reaches Provisioning before any completion event can land.
	// Ordering is preserved by the inbox's FIFO delivery.
	actor := b.actorFor(req.LeaseUUID)
	provCtx, provCancel := b.shutdownAwareContext()
	// workDone is closed by the goroutine's outermost defer. The actor's
	// Provisioning.OnExit waits on it (bounded) so a preempting Deprovision
	// either sees pre-published ContainerIDs or post-cleanup state — never
	// a mid-flight window where containers exist on the host but the
	// provision struct reports none (bug_012). The actor's run loop also
	// waits on it during shutdown so the terminal SM event lands before
	// the actor exits (bug_004).
	workDone := make(chan struct{})
	if !actor.send(provisionRequestedMsg{cancel: provCancel, done: workDone}) {
		provCancel()
		close(workDone)
		b.removeProvision(req.LeaseUUID)
		return fmt.Errorf("backend shutting down")
	}
	b.wg.Go(func() {
		// LIFO: provCancel runs first (notify anything on provCtx), then
		// the done signal unblocks OnExit's wait.
		defer close(workDone)
		defer provCancel()

		var callbackErr string
		var err error
		var result provisionSuccessResult
		if isStack {
			callbackErr, err, result = b.doProvisionStack(provCtx, req, stackManifest, profiles, logger)
		} else {
			callbackErr, err, result = b.doProvision(provCtx, req, manifest, profiles, logger)
		}
		// On success, publish ContainerIDs to the provision struct *before*
		// sending provisionCompletedMsg. The SM's Ready entry action rewrites
		// the same field with the same value; the early publish exists so
		// a concurrent doDeprovision (waiting on workDone in OnExit)
		// observes the IDs and removes them cleanly. On error, doProvision's
		// own defer has already cleaned up the containers, so no pre-publish
		// is needed.
		if err == nil {
			b.provisionsMu.Lock()
			if p, ok := b.provisions[req.LeaseUUID]; ok {
				p.ContainerIDs = result.containerIDs
			}
			b.provisionsMu.Unlock()
		}
		// sendTerminal bypasses the stopCtx refusal so the SM records the
		// outcome even during shutdown — the actor's run loop drains the
		// inbox before exit. A refusal at this point means the actor has
		// fully exited OR the inbox is wedged past the send timeout; both
		// are pathological, counted for ops visibility.
		var event string
		var ok bool
		if err != nil {
			event = "provision_errored"
			ok = actor.sendTerminal(provisionErroredMsg{callbackErr: callbackErr, lastError: err.Error()})
		} else {
			event = "provision_completed"
			ok = actor.sendTerminal(provisionCompletedMsg{result: result})
		}
		if !ok {
			leaseTerminalEventDroppedTotal.WithLabelValues(event).Inc()
			b.logger.Warn("terminal provision event dropped (actor exited or inbox wedged)",
				"lease_uuid", req.LeaseUUID,
				"event", event,
			)
		}
	})

	return nil
}

// volumeOwnerEntry caches the detected UID/GID for an image's VOLUME directories.
type volumeOwnerEntry struct {
	UID int
	GID int
}

// detectVolumeOwnerCached returns the detected volume owner for an image,
// using the cache keyed by image ID. On error, logs a warning and returns
// (0, 0) without caching so the next call retries (transient errors
// self-heal). Successful results are cached permanently since image IDs
// are immutable content-addressable digests.
func (b *Backend) detectVolumeOwnerCached(ctx context.Context, imageID, imageName string, volumePaths []string) (uid, gid int) {
	if v, ok := b.volumeOwnerCache.Load(imageID); ok {
		if entry, ok := v.(volumeOwnerEntry); ok {
			return entry.UID, entry.GID
		}
	}

	detectedUID, detectedGID, err := b.docker.DetectVolumeOwner(ctx, imageName, volumePaths)
	if err != nil {
		b.logger.Warn("failed to detect volume owner, defaulting to root (not cached)",
			"image", imageName, "error", err)
		return 0, 0
	}

	b.volumeOwnerCache.Store(imageID, volumeOwnerEntry{UID: detectedUID, GID: detectedGID})
	return detectedUID, detectedGID
}

// detectWritablePathsCached returns auto-detected writable paths for an image,
// using the cache keyed by image ID. On error, logs a warning and returns nil
// without caching so the next call retries. Successful results (including
// empty slices) are cached permanently since image IDs are immutable.
func (b *Backend) detectWritablePathsCached(ctx context.Context, imageID, imageName string, uid int) []string {
	if v, ok := b.writablePathCache.Load(imageID); ok {
		if paths, ok := v.([]string); ok {
			return paths
		}
	}

	paths, err := b.docker.DetectWritablePaths(ctx, imageName, uid, candidateWritableParents)
	if err != nil {
		b.logger.Warn("failed to detect writable paths, skipping (not cached)",
			"image", imageName, "error", err)
		return nil
	}

	b.writablePathCache.Store(imageID, paths)
	return paths
}

// imageSetup holds the results of image inspection needed for container creation.
type imageSetup struct {
	Volumes       []string // sorted VOLUME paths declared by the image
	ContainerUser string   // numeric "uid:gid" or "" for root
	VolumeUID     int      // UID for volume ownership
	VolumeGID     int      // GID for volume ownership
	WritablePaths []string // auto-detected writable paths for non-root images
}

// inspectImageForSetup inspects an image and resolves its VOLUME declarations
// and container user. This combines the image inspect, volume discovery, and
// user resolution steps that are common to doProvision, doRestart, and doUpdate.
func (b *Backend) inspectImageForSetup(ctx context.Context, image string, manifestUser string) (*imageSetup, error) {
	imageInfo, err := b.docker.InspectImage(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("image inspect failed: %w", err)
	}

	volumes := slices.Sorted(maps.Keys(imageInfo.Volumes))

	result := &imageSetup{Volumes: volumes}

	if manifestUser != "" || imageInfo.User != "" {
		uid, gid, resolveErr := b.docker.ResolveImageUser(ctx, image, manifestUser)
		if resolveErr != nil {
			return nil, fmt.Errorf("image user resolution failed: %w", resolveErr)
		}
		result.VolumeUID = uid
		result.VolumeGID = gid
		if uid != 0 || gid != 0 {
			result.ContainerUser = fmt.Sprintf("%d:%d", uid, gid)
		}
	} else if len(volumes) > 0 {
		// No explicit user set — auto-detect from VOLUME directory ownership.
		// Images like mongo/postgres pre-chown their VOLUME dirs during build;
		// detecting the owner lets us pre-chown host volumes and run as that
		// user, bypassing the entrypoint's chown+gosu (which requires
		// CAP_CHOWN that we drop).
		uid, gid := b.detectVolumeOwnerCached(ctx, imageInfo.ID, image, volumes)
		if uid != 0 || gid != 0 {
			result.VolumeUID = uid
			result.VolumeGID = gid
			result.ContainerUser = fmt.Sprintf("%d:%d", uid, gid)
		}
	}

	// Scan for writable paths owned by the container user (or any non-root
	// user for root images). Images like grafana/grafana (non-root, no VOLUMEs)
	// chown /var/lib/grafana during build; images like neo4j (root, has VOLUMEs)
	// chown /var/lib/neo4j to a service user. These paths get bind mounts from
	// managed volumes so the container has image content on a read-only rootfs.
	// Skipped when ReadonlyRootfs is disabled since the detection creates a temp
	// container and the results are only used for writable path mounting.
	if b.cfg.IsReadonlyRootfs() {
		result.WritablePaths = b.detectWritablePathsCached(ctx, imageInfo.ID, image, result.VolumeUID)
		result.WritablePaths = filterSubpaths(result.WritablePaths, result.Volumes)
	}

	return result, nil
}

// filterSubpaths removes candidates that are equal to or children of any parent path.
// This prevents writable paths from overlapping with VOLUME bind mounts
// (e.g., /data/transactions is a subtree of /data).
func filterSubpaths(candidates, parents []string) []string {
	if len(candidates) == 0 || len(parents) == 0 {
		return candidates
	}
	var result []string
	for _, c := range candidates {
		covered := false
		for _, p := range parents {
			if c == p || strings.HasPrefix(c, p+"/") {
				covered = true
				break
			}
		}
		if !covered {
			result = append(result, c)
		}
	}
	return result
}

// ensureNetworkConfig sets up per-tenant network isolation if enabled.
// Returns nil config when isolation is disabled. Routes through
// Backend.ensureTenantNetwork so the network cannot be removed by a
// concurrent deprovision between here and ContainerCreate.
func (b *Backend) ensureNetworkConfig(ctx context.Context, tenant string) (*networktypes.NetworkingConfig, error) {
	if !b.cfg.IsNetworkIsolation() {
		return nil, nil
	}
	networkID, err := b.ensureTenantNetwork(ctx, tenant)
	if err != nil {
		return nil, fmt.Errorf("tenant network setup failed: %w", err)
	}
	return buildNetworkConfig(networkID), nil
}

// setupVolumeBinds creates volume bind mounts for a single container instance.
// Returns nil when no volumes are needed (diskMB <= 0 or no image volumes).
// Volumes are created idempotently — existing volumes are reused.
func (b *Backend) setupVolumeBinds(ctx context.Context, leaseUUID string, instanceIndex int, diskMB int64, imageVolumes []string, volumeUID, volumeGID int) (map[string]string, error) {
	if diskMB <= 0 || len(imageVolumes) == 0 {
		return nil, nil
	}

	volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, instanceIndex)
	hostPath, _, err := b.volumes.Create(ctx, volumeID, diskMB)
	if err != nil {
		return nil, fmt.Errorf("volume access failed (instance %d): %w", instanceIndex, err)
	}

	binds := make(map[string]string, len(imageVolumes))
	for _, volPath := range imageVolumes {
		sanitized := sanitizeVolumePath(volPath)
		if sanitized == "" {
			continue
		}
		subdir := filepath.Join(hostPath, sanitized)
		if mkErr := os.MkdirAll(subdir, 0o700); mkErr != nil {
			return nil, fmt.Errorf("volume subdir creation failed (instance %d): %w", instanceIndex, mkErr)
		}
		if volumeUID != 0 || volumeGID != 0 {
			if chownErr := os.Chown(subdir, volumeUID, volumeGID); chownErr != nil {
				return nil, fmt.Errorf("volume subdir chown failed (instance %d): %w", instanceIndex, chownErr)
			}
		}
		binds[subdir] = volPath
	}
	return binds, nil
}

// buildStatefulVolumeBinds creates subdirectories for each image VOLUME path
// under hostPath and returns bind mount mappings. Returns an error if any
// VOLUME path cannot be sanitized (unsupported path format).
func buildStatefulVolumeBinds(hostPath string, imageVolumes []string, uid, gid int) (map[string]string, error) {
	binds := make(map[string]string, len(imageVolumes))
	for _, volPath := range imageVolumes {
		sanitized := sanitizeVolumePath(volPath)
		if sanitized == "" {
			return nil, fmt.Errorf("image declares unsupported VOLUME path %q", volPath)
		}
		subdir := filepath.Join(hostPath, sanitized)
		if err := os.MkdirAll(subdir, 0o700); err != nil {
			return nil, fmt.Errorf("volume subdir %q: %w", subdir, err)
		}
		if uid != 0 || gid != 0 {
			if err := os.Chown(subdir, uid, gid); err != nil {
				return nil, fmt.Errorf("chown volume subdir %q: %w", subdir, err)
			}
		}
		binds[subdir] = volPath
	}
	return binds, nil
}

// setupWritablePathBinds extracts image content for writable paths into
// a managed volume subdirectory and returns a bind map for container creation.
// Extraction failures are logged but don't fail the overall operation;
// paths that fail are simply omitted from the bind map.
func (b *Backend) setupWritablePathBinds(ctx context.Context, image string, writablePaths []string, hostVolumePath string, maxBytes int64) map[string]string {
	if len(writablePaths) == 0 {
		return nil
	}

	wpDir := filepath.Join(hostVolumePath, "_wp")
	// Remove stale content from prior extractions so files deleted
	// in a newer image don't persist.
	if err := os.RemoveAll(wpDir); err != nil {
		b.logger.Warn("failed to clean up old writable path content, extraction may contain stale files",
			"path", wpDir, "error", err)
	}
	failures := b.docker.ExtractImageContent(ctx, image, writablePaths, wpDir, maxBytes)

	binds := make(map[string]string, len(writablePaths))
	for _, wp := range writablePaths {
		if failures != nil {
			if pathErr, ok := failures[wp]; ok {
				b.logger.Warn("failed to extract writable path content",
					"path", wp, "image", image, "error", pathErr)
				continue
			}
		}
		sanitized := sanitizeVolumePath(wp)
		if sanitized == "" {
			b.logger.Warn("writable path rejected by sanitization", "path", wp, "image", image)
			continue
		}
		binds[filepath.Join(wpDir, sanitized)] = wp
	}

	return binds
}

// setupStackVolBinds creates volume bind mounts for all services/instances of a stack.
// It returns the volume binds map, a list of newly created volume IDs, and any fatal error.
// Non-fatal failures (writable-path-only volume creation) are logged as warnings.
func (b *Backend) setupStackVolBinds(
	ctx context.Context,
	leaseUUID string,
	items []backend.LeaseItem,
	profiles map[string]SKUProfile,
	imageSetups map[string]*imageSetup,
	services map[string]*DockerManifest,
	logger *slog.Logger,
) (map[string]map[int]serviceVolBinds, []string, error) {
	volBinds := make(map[string]map[int]serviceVolBinds)
	var createdVolumeIDs []string

	for _, item := range items {
		svcName := item.ServiceName
		profile := profiles[item.SKU]
		imgSetup := imageSetups[svcName]

		for i := range item.Quantity {
			needsStatefulVolume := profile.DiskMB > 0 && len(imgSetup.Volumes) > 0
			needsWritableVolume := len(imgSetup.WritablePaths) > 0

			if needsStatefulVolume || needsWritableVolume {
				volumeID := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, svcName, i)
				sizeMB := profile.DiskMB
				if sizeMB <= 0 {
					sizeMB = int64(b.cfg.GetTmpfsSizeMB())
				}
				hostPath, volCreated, volErr := b.volumes.Create(ctx, volumeID, sizeMB)
				if volErr != nil {
					if needsStatefulVolume {
						return nil, createdVolumeIDs, fmt.Errorf("volume creation failed (service %s, instance %d): %w", svcName, i, volErr)
					}
					logger.Warn("writable path content seeding unavailable (volume creation failed)", "service", svcName, "error", volErr)
					continue
				}
				if volCreated {
					createdVolumeIDs = append(createdVolumeIDs, volumeID)
				}
				binds := serviceVolBinds{}
				if needsStatefulVolume {
					var buildErr error
					binds.StatefulBinds, buildErr = buildStatefulVolumeBinds(hostPath, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
					if buildErr != nil {
						return nil, createdVolumeIDs, fmt.Errorf("volume setup failed (service %s, instance %d): %w", svcName, i, buildErr)
					}
				}
				if needsWritableVolume {
					binds.WritableBinds = b.setupWritablePathBinds(ctx, services[svcName].Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024)
				}
				if volBinds[svcName] == nil {
					volBinds[svcName] = make(map[int]serviceVolBinds)
				}
				volBinds[svcName][i] = binds
			}
		}
	}
	return volBinds, createdVolumeIDs, nil
}

// verifyStartup checks that containers started successfully.
// Uses health-check-aware polling when the manifest declares an active health check,
// otherwise falls back to a fixed-wait + inspect check.
func (b *Backend) verifyStartup(ctx context.Context, manifest *DockerManifest, containerIDs []string, logger *slog.Logger) error {
	if manifest.HasActiveHealthCheck() {
		return b.waitForHealthy(ctx, containerIDs, logger)
	}

	startupVerify := cmp.Or(b.cfg.StartupVerifyDuration, 5*time.Second)
	select {
	case <-ctx.Done():
		return fmt.Errorf("canceled during startup verification: %w", ctx.Err())
	case <-time.After(startupVerify):
	}

	for i, containerID := range containerIDs {
		info, err := b.docker.InspectContainer(ctx, containerID)
		if err != nil {
			return fmt.Errorf("failed to verify container %d after startup: %w", i, err)
		}
		status := containerStatusToProvisionStatus(info.Status)
		if status != backend.ProvisionStatusReady {
			diag := b.containerFailureDiagnostics(ctx, containerID, info)
			return fmt.Errorf("container %d exited during startup (status: %s): %s", i, info.Status, diag)
		}
	}
	return nil
}

// doProvision performs the actual container creation asynchronously.
// For multi-unit leases, it creates multiple containers.
// For multi-SKU leases, each container gets the appropriate resource profile.
//
// Returns (callbackErr, err, result). On success, result carries the
// populated provisionSuccessResult for the SM's Ready entry action to
// write into the provision struct. On failure, result is zero; the SM's
// Failed entry action uses (callbackErr, err.Error()) to populate
// LastError and the Failed callback.
//
// The defer is responsible for side effects that don't belong to the SM:
// duration metrics, pool release on failure, container/volume cleanup on
// failure, release-store updates on success, and stale-diagnostic removal
// on success. Provision struct mutations (Status, FailCount, LastError,
// ContainerIDs, Manifest) are owned by the SM entry actions.
func (b *Backend) doProvision(ctx context.Context, req backend.ProvisionRequest, manifest *DockerManifest, profiles map[string]SKUProfile, logger *slog.Logger) (callbackErrRet string, errRet error, resultRet provisionSuccessResult) {
	totalQuantity := req.TotalQuantity()
	var containerIDs []string
	var createdVolumeIDs []string // tracks volumes actually created for accurate cleanup
	var err error
	var callbackErr string // hardcoded message for callbacks (safe for on-chain)
	provisionStart := time.Now()

	defer func() {
		provisionDurationSeconds.Observe(time.Since(provisionStart).Seconds())
		if err != nil {
			logger.Error("provision failed", "lease_uuid", req.LeaseUUID, "error", err)
			provisionsTotal.WithLabelValues("failure").Inc()
			// Clean up on failure - release all allocated resources
			for i := range totalQuantity {
				b.pool.Release(fmt.Sprintf("%s-%d", req.LeaseUUID, i))
			}

			// Clean up any containers that were created.
			// Use a fresh context since the original may be canceled.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, cid := range containerIDs {
				if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
					logger.Warn("failed to cleanup container after error", "container_id", shortID(cid), "error", rmErr)
				}
			}

			// Destroy only volumes that were actually created during this attempt.
			for _, volumeID := range createdVolumeIDs {
				if volErr := b.volumes.Destroy(cleanupCtx, volumeID); volErr != nil {
					logger.Warn("failed to cleanup volume after error", "volume_id", volumeID, "error", volErr)
				}
			}

			callbackErrRet = callbackErr
			errRet = err
			return
		}

		// Success: metrics + persistent stores (release history, clear stale
		// diagnostics). Provision-struct mutations are owned by the SM
		// entry action — see onEnterReadyFromProvision.
		provisionsTotal.WithLabelValues("success").Inc()

		if b.releaseStore != nil {
			if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
				Manifest:  req.Payload,
				Image:     manifest.Image,
				Status:    "active",
				CreatedAt: time.Now(),
			}); relErr != nil {
				b.logger.Warn("failed to record initial release", "lease", req.LeaseUUID, "error", relErr)
			}
		}

		if b.diagnosticsStore != nil {
			if delErr := b.diagnosticsStore.Delete(req.LeaseUUID); delErr != nil {
				b.logger.Warn("failed to remove stale diagnostic entry", "lease", req.LeaseUUID, "error", delErr)
			}
		}

		updateResourceMetrics(b.pool.Stats())

		resultRet = provisionSuccessResult{
			containerIDs: containerIDs,
			manifest:     manifest,
		}
	}()

	// Check for early cancellation (e.g., shutdown requested before we started)
	if ctx.Err() != nil {
		logger.Warn("provisioning canceled before start", "error", ctx.Err())
		err = fmt.Errorf("provisioning canceled: %w", ctx.Err())
		callbackErr = "provisioning canceled"
		return
	}

	// Pull image (only once, shared by all containers)
	logger.Info("pulling image", "image", manifest.Image)
	pullStart := time.Now()
	if err = b.docker.PullImage(ctx, manifest.Image, b.cfg.ImagePullTimeout); err != nil {
		logger.Error("failed to pull image", "error", err)
		err = fmt.Errorf("image pull failed: %w", err)
		callbackErr = "image pull failed"
		return
	}
	imagePullDurationSeconds.Observe(time.Since(pullStart).Seconds())

	// Inspect image and resolve user
	imgSetup, setupErr := b.inspectImageForSetup(ctx, manifest.Image, manifest.User)
	if setupErr != nil {
		logger.Error("image setup failed", "error", setupErr)
		err = setupErr
		callbackErr = "image inspect failed"
		return
	}
	if imgSetup.ContainerUser != "" {
		logger.Info("resolved container user", "uid", imgSetup.VolumeUID, "gid", imgSetup.VolumeGID)
	}
	if len(imgSetup.WritablePaths) > 0 {
		logger.Info("auto-detected writable paths", "paths", imgSetup.WritablePaths, "uid", imgSetup.VolumeUID)
	}

	// Set up per-tenant network isolation
	networkConfig, netErr := b.ensureNetworkConfig(ctx, req.Tenant)
	if netErr != nil {
		logger.Error("failed to create tenant network", "error", netErr)
		err = netErr
		callbackErr = "tenant network setup failed"
		return
	}
	if networkConfig != nil {
		logger.Info("tenant network ready")
	}

	// Create and start containers for each item/unit
	containerIDs = make([]string, 0, totalQuantity)
	instanceIndex := 0
	for _, item := range req.Items {
		profile := profiles[item.SKU]

		for range item.Quantity {
			instanceLogger := logger.With("instance", instanceIndex, "sku", item.SKU)

			// Create container with instance index for unique naming
			instanceLogger.Info("creating container")
			// Read the current fail count so it's persisted in the container label.
			b.provisionsMu.RLock()
			failCount := 0
			if prov, ok := b.provisions[req.LeaseUUID]; ok {
				failCount = prov.FailCount
			}
			b.provisionsMu.RUnlock()

			// Create managed volume for stateful SKUs (disk_mb > 0) or writable paths.
			// doProvision uses inline volume creation to track newly-created volumes
			// for selective cleanup (reused volumes from a previous provision must not
			// be destroyed). doRestart/doUpdate use setupVolumeBinds which treats
			// volumes as idempotent.
			var volumeBinds map[string]string
			var writablePathBinds map[string]string
			needsStatefulVolume := profile.DiskMB > 0 && len(imgSetup.Volumes) > 0
			needsWritableVolume := len(imgSetup.WritablePaths) > 0

			if needsStatefulVolume || needsWritableVolume {
				volumeID := fmt.Sprintf("fred-%s-%d", req.LeaseUUID, instanceIndex)
				sizeMB := profile.DiskMB
				if sizeMB <= 0 {
					sizeMB = int64(b.cfg.GetTmpfsSizeMB())
				}
				hostPath, volCreated, volErr := b.volumes.Create(ctx, volumeID, sizeMB)
				if volErr != nil {
					if needsStatefulVolume {
						instanceLogger.Error("failed to create volume", "error", volErr)
						err = fmt.Errorf("volume creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, volErr)
						callbackErr = "volume creation failed"
						return
					}
					// Writable paths only — degrade: skip mounts, container may fail at startup.
					instanceLogger.Warn("writable path content seeding unavailable (volume creation failed)", "error", volErr)
				} else {
					if volCreated {
						createdVolumeIDs = append(createdVolumeIDs, volumeID)
					}

					// Set up VOLUME path subdirs (if stateful)
					if needsStatefulVolume {
						var buildErr error
						volumeBinds, buildErr = buildStatefulVolumeBinds(hostPath, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
						if buildErr != nil {
							err = fmt.Errorf("volume setup failed (instance %d, sku %s): %w", instanceIndex, item.SKU, buildErr)
							callbackErr = "volume creation failed"
							return
						}
					}

					// Set up writable path binds
					if needsWritableVolume {
						writablePathBinds = b.setupWritablePathBinds(ctx, manifest.Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024)
					}
				}
			} else if profile.DiskMB > 0 && len(imgSetup.Volumes) == 0 {
				instanceLogger.Warn("stateful SKU has disk_mb > 0 but image declares no VOLUME paths; disk budget is allocated but unused",
					"disk_mb", profile.DiskMB)
			}

			createStart := time.Now()
			containerID, createErr := b.docker.CreateContainer(ctx, CreateContainerParams{
				LeaseUUID:         req.LeaseUUID,
				Tenant:            req.Tenant,
				ProviderUUID:      req.ProviderUUID,
				SKU:               item.SKU,
				Manifest:          manifest,
				Profile:           profile,
				InstanceIndex:     instanceIndex,
				FailCount:         failCount,
				CallbackURL:       req.CallbackURL,
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
				NetworkName:       TenantNetworkName(req.Tenant),
				Quantity:          totalQuantity,
			}, b.cfg.ContainerCreateTimeout)
			containerCreateDurationSeconds.Observe(time.Since(createStart).Seconds())
			if createErr != nil {
				instanceLogger.Error("failed to create container", "error", createErr)
				err = fmt.Errorf("container creation failed (instance %d, sku %s): %w", instanceIndex, item.SKU, createErr)
				callbackErr = "container creation failed"
				return
			}
			containerIDs = append(containerIDs, containerID)

			// Start container
			instanceLogger.Info("starting container", "container_id", shortID(containerID))
			if startErr := b.docker.StartContainer(ctx, containerID, b.cfg.ContainerStartTimeout); startErr != nil {
				instanceLogger.Error("failed to start container", "error", startErr)
				err = fmt.Errorf("container start failed (instance %d, sku %s): %w", instanceIndex, item.SKU, startErr)
				callbackErr = "container start failed"
				return
			}

			instanceLogger.Info("container provisioned successfully", "container_id", shortID(containerID))
			instanceIndex++
		}
	}

	// Startup verification
	if err = b.verifyStartup(ctx, manifest, containerIDs, logger); err != nil {
		callbackErr = startupErrorToCallbackMsg(err)
		return
	}

	logger.Info("all containers provisioned and verified", "count", len(containerIDs))
	return
}

// doProvisionStack performs container creation for a stack (multi-service) lease
// using Docker Compose. Compose handles container creation, start ordering, and
// network attachment atomically via a single Up call.
//
// See doProvision for the (callbackErr, err, result) return contract.
// Stack-specific result fields are stackManifest + serviceContainers.
func (b *Backend) doProvisionStack(ctx context.Context, req backend.ProvisionRequest, stack *StackManifest, profiles map[string]SKUProfile, logger *slog.Logger) (callbackErrRet string, errRet error, resultRet provisionSuccessResult) {
	var containerIDs []string
	var createdVolumeIDs []string
	var err error
	var callbackErr string
	provisionStart := time.Now()
	serviceContainers := make(map[string][]string)
	projectName := composeProjectName(req.LeaseUUID)

	defer func() {
		provisionDurationSeconds.Observe(time.Since(provisionStart).Seconds())
		if err != nil {
			logger.Error("stack provision failed", "lease_uuid", req.LeaseUUID, "error", err)
			provisionsTotal.WithLabelValues("failure").Inc()

			// Release all service-aware allocation IDs.
			for _, item := range req.Items {
				for i := range item.Quantity {
					b.pool.Release(fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i))
				}
			}

			// Clean up via Compose Down (removes all project containers).
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if downErr := b.compose.Down(cleanupCtx, projectName, 10*time.Second); downErr != nil {
				logger.Warn("compose down failed during cleanup, falling back to individual removal", "error", downErr)
				for _, cid := range containerIDs {
					if rmErr := b.docker.RemoveContainer(cleanupCtx, cid); rmErr != nil {
						logger.Warn("failed to cleanup container after error", "container_id", shortID(cid), "error", rmErr)
					}
				}
			}
			for _, volumeID := range createdVolumeIDs {
				if volErr := b.volumes.Destroy(cleanupCtx, volumeID); volErr != nil {
					logger.Warn("failed to cleanup volume after error", "volume_id", volumeID, "error", volErr)
				}
			}
			callbackErrRet = callbackErr
			errRet = err
			return
		}

		provisionsTotal.WithLabelValues("success").Inc()

		if b.releaseStore != nil {
			if relErr := b.releaseStore.Append(req.LeaseUUID, shared.Release{
				Manifest:  req.Payload,
				Image:     "stack",
				Status:    "active",
				CreatedAt: time.Now(),
			}); relErr != nil {
				b.logger.Warn("failed to record initial release", "lease", req.LeaseUUID, "error", relErr)
			}
		}

		if b.diagnosticsStore != nil {
			if delErr := b.diagnosticsStore.Delete(req.LeaseUUID); delErr != nil {
				b.logger.Warn("failed to remove stale diagnostic entry", "lease", req.LeaseUUID, "error", delErr)
			}
		}

		updateResourceMetrics(b.pool.Stats())

		resultRet = provisionSuccessResult{
			containerIDs:      containerIDs,
			stackManifest:     stack,
			serviceContainers: serviceContainers,
		}
	}()

	if ctx.Err() != nil {
		logger.Warn("provisioning canceled before start", "error", ctx.Err())
		err = fmt.Errorf("provisioning canceled: %w", ctx.Err())
		callbackErr = "provisioning canceled"
		return
	}

	// Pull each unique image (deduplicated across services).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image", "service", svcName, "image", svc.Image)
		pullStart := time.Now()
		if err = b.docker.PullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); err != nil {
			logger.Error("failed to pull image", "service", svcName, "error", err)
			err = fmt.Errorf("image pull failed for service %s: %w", svcName, err)
			callbackErr = "image pull failed"
			return
		}
		imagePullDurationSeconds.Observe(time.Since(pullStart).Seconds())
		pulledImages[svc.Image] = true
	}

	// Per-service image setup (inspect, user resolution, writable paths).
	imageSetups := make(map[string]*imageSetup)
	for svcName, svc := range stack.Services {
		imgSetup, setupErr := b.inspectImageForSetup(ctx, svc.Image, svc.User)
		if setupErr != nil {
			logger.Error("image setup failed", "service", svcName, "error", setupErr)
			err = setupErr
			callbackErr = "image inspect failed"
			return
		}
		imageSetups[svcName] = imgSetup
	}

	// Resolve tenant network name (not Docker network ID — Compose needs the name).
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		_, netErr := b.ensureTenantNetwork(ctx, req.Tenant)
		if netErr != nil {
			logger.Error("failed to create tenant network", "error", netErr)
			err = netErr
			callbackErr = "tenant network setup failed"
			return
		}
		networkName = TenantNetworkName(req.Tenant)
	}

	// Pre-create volumes and build volume binds per service/instance.
	b.provisionsMu.RLock()
	failCount := 0
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		failCount = prov.FailCount
	}
	b.provisionsMu.RUnlock()

	var volBinds map[string]map[int]serviceVolBinds
	volBinds, createdVolumeIDs, err = b.setupStackVolBinds(ctx, req.LeaseUUID, req.Items, profiles, imageSetups, stack.Services, logger)
	if err != nil {
		callbackErr = "volume creation failed"
		return
	}

	// Build Compose project and bring it up.
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:    req.LeaseUUID,
		Tenant:       req.Tenant,
		ProviderUUID: req.ProviderUUID,
		CallbackURL:  req.CallbackURL,
		BackendName:  b.cfg.Name,
		FailCount:    failCount,
		Stack:        stack,
		Items:        req.Items,
		Profiles:     profiles,
		ImageSetups:  imageSetups,
		NetworkName:  networkName,
		VolBinds:     volBinds,
		Cfg:          &b.cfg,
		Ingress:      b.cfg.Ingress,
	})

	logger.Info("compose up", "project", projectName, "services", len(project.Services))
	if upErr := b.compose.Up(ctx, project, composeUpOpts{}); upErr != nil {
		err = fmt.Errorf("compose up failed: %w", upErr)
		callbackErr = "container creation failed"
		return
	}

	// Discover container IDs via Compose PS.
	containers, psErr := b.compose.PS(ctx, projectName)
	if psErr != nil {
		err = fmt.Errorf("compose ps failed: %w", psErr)
		callbackErr = "container creation failed"
		return
	}

	containerIDs, serviceContainers = mapComposeContainers(containers, req.Items)

	// Verify startup per-service so each service uses its own health check config.
	for svcName, svcCIDs := range serviceContainers {
		svc := stack.Services[svcName]
		if err = b.verifyStartup(ctx, svc, svcCIDs, logger.With("service", svcName)); err != nil {
			callbackErr = startupErrorToCallbackMsg(err)
			return
		}
	}

	logger.Info("all stack containers provisioned and verified", "count", len(containerIDs), "services", len(stack.Services))
	return
}

// mapComposeContainers maps Compose PS output to containerIDs and serviceContainers.
// For fanned-out services (web-0, web-1), it strips the instance suffix to recover
// the original service name.
func mapComposeContainers(containers []composeContainerSummary, items []backend.LeaseItem) ([]string, map[string][]string) {
	// Build a set of base service names for fan-out detection.
	svcQuantities := make(map[string]int, len(items))
	for _, item := range items {
		svcQuantities[item.ServiceName] = item.Quantity
	}

	var containerIDs []string
	serviceContainers := make(map[string][]string)

	for _, c := range containers {
		containerIDs = append(containerIDs, c.ID)
		// Recover original service name from Compose service name.
		// Fan-out: "web-0" → "web", single: "web" → "web".
		// We verify the suffix is a valid integer to avoid prefix collisions
		// (e.g., "web-extra" must not match "web" with qty>1).
		baseName := c.Service
		for svcName, qty := range svcQuantities {
			if qty > 1 && strings.HasPrefix(c.Service, svcName+"-") {
				suffix := strings.TrimPrefix(c.Service, svcName+"-")
				if _, parseErr := strconv.Atoi(suffix); parseErr == nil {
					baseName = svcName
					break
				}
			}
		}
		serviceContainers[baseName] = append(serviceContainers[baseName], c.ID)
	}
	return containerIDs, serviceContainers
}

// healthPollInterval is the interval between health check polls during startup verification.
const healthPollInterval = 2 * time.Second

// startupErrorToCallbackMsg maps a verifyStartup or waitForHealthy error to a
// hardcoded callback message safe for on-chain surfacing.
func startupErrorToCallbackMsg(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "unhealthy"):
		return "container reported unhealthy"
	case strings.Contains(msg, "exited during startup"):
		return "container exited during startup"
	case strings.Contains(msg, "canceled during startup verification"):
		return "container startup verification canceled"
	case strings.Contains(msg, "exited"):
		return "container exited during health check"
	default:
		return "container exited during startup"
	}
}

// waitForHealthy polls container health status until all containers report
// "healthy". It fails immediately if any container becomes "unhealthy" or
// exits. The method is bounded by the caller's context (typically the
// ProvisionTimeout).
func (b *Backend) waitForHealthy(ctx context.Context, containerIDs []string, logger *slog.Logger) error {
	pending := make(map[int]struct{}, len(containerIDs))
	for i := range containerIDs {
		pending[i] = struct{}{}
	}

	ticker := time.NewTicker(healthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for containers to become healthy: %w", ctx.Err())
		case <-ticker.C:
			for i := range pending {
				info, err := b.docker.InspectContainer(ctx, containerIDs[i])
				if err != nil {
					return fmt.Errorf("failed to inspect container %d during health check: %w", i, err)
				}

				// Check if container has exited.
				status := containerStatusToProvisionStatus(info.Status)
				if status == backend.ProvisionStatusFailed {
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], info)
					return fmt.Errorf("container %d exited while waiting for healthy (status: %s): %s", i, info.Status, diag)
				}

				switch info.Health {
				case HealthStatusHealthy:
					logger.Info("container healthy", "instance", i, "container_id", shortID(containerIDs[i]))
					delete(pending, i)
				case HealthStatusUnhealthy:
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], info)
					return fmt.Errorf("container %d reported unhealthy: %s", i, diag)
				default:
					// "starting" or other — keep polling
				}
			}

			if len(pending) == 0 {
				return nil
			}
		}
	}
}
