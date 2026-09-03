package docker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// isFlatPayload reports whether the raw payload bytes lack a top-level
// "services" key — i.e., a legacy flat single-service manifest was
// submitted. Used at the Provision/Update entry to fire the one-time
// deprecation log per lease without re-deriving the format from the
// post-wrap StackManifest. Best-effort: malformed JSON returns false so
// the deprecation log silently skips (the upstream ParsePayload already
// surfaced the parse error).
func isFlatPayload(data []byte) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return false
	}
	_, hasServices := probe["services"]
	return !hasServices
}

// maxLeaseQuantity bounds the total container count a single lease may request.
// The chain's billing module caps per-item quantity at 1e9, so without this guard an
// honest max-quantity lease would drive the pre-admission ContainerIDs allocation
// (make([]string, 0, totalQuantity)) to ~16 GB — an OOM reachable before any admission
// control. Real leases are a handful of containers; 1024 is far above any single node's
// real capacity (such a lease would fail admission anyway). (ENG-503)
const maxLeaseQuantity = backend.MaxOperationQuantity

func resolvedProvisionAllocations(
	leaseUUID string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) ([]string, []shared.ResolvedAdoptInstance, error) {
	resourcesBySKU, err := resourceSnapshotMap(items, resourceProfiles)
	if err != nil {
		return nil, nil, err
	}
	total, err := backend.ValidateOperationQuantities(items)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]string, 0, total)
	allocations := make([]shared.ResolvedAdoptInstance, 0, total)
	for _, item := range items {
		if item.ServiceName == "" {
			return nil, nil, errors.New("resolved provision allocation requires a service name")
		}
		for index := range item.Quantity {
			id := fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, index)
			ids = append(ids, id)
			allocations = append(allocations, shared.ResolvedAdoptInstance{
				ID:        id,
				Resources: resourcesBySKU[item.SKU],
			})
		}
	}
	return ids, allocations, nil
}

// replacementProvisionFailureRecovery is an unforgeable-by-callers marker
// carried only by the Provision admission path after it has torn down a Failed
// predecessor and published the candidate generation. A candidate can reuse
// the predecessor's canonical volumes, so ordinary fresh-provision cleanup
// cannot infer that createdVolumeIDs==0 means no durable bytes remain.
//
// On failure the worker retains the candidate intent/reservation and stops the
// backend. Cold recovery can then use the candidate intent plus the older
// active Release to rebuild the predecessor without exposing free capacity in
// the interim.
type replacementProvisionFailureRecovery struct{}

// Provision starts async provisioning of containers.
// For multi-unit leases (quantity > 1), multiple containers are created.
// For multi-SKU leases, containers are created with the appropriate profile for each SKU.
//
// Pre-flight validation errors (unknown SKU, invalid manifest, disallowed image,
// insufficient resources) are returned synchronously so the caller can respond
// with an appropriate HTTP status. Only truly asynchronous failures (image pull,
// container create/start) are communicated via callback.
func (b *Backend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if err := b.requireMutationAdmission(ctx, "provision"); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	unlockCommand := b.commandFence.Lock(req.LeaseUUID)
	defer unlockCommand()
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(
		req.CallbackURL, req.LifecycleCallbackURL,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", backend.ErrValidation, err)
	}
	req.LifecycleCallbackURL = lifecycleCallbackURL
	if exactRetry, err := b.probeOperationIntent(req.LeaseUUID, req.CallbackURL); err != nil {
		return err
	} else if exactRetry {
		return nil
	}
	if err := b.ensureRestoreDestinationUnowned(req.LeaseUUID); err != nil {
		return err
	}
	// Bound the chain-supplied quantity BEFORE the reservation's ContainerIDs allocation
	// below. item.Quantity is a uint64→int cast at ingest, so a negative value signals an
	// overflowed cast; a total above maxLeaseQuantity would drive an unbounded pre-admission
	// allocation (~16 GB at the chain's 1e9 billing cap) or, if negative, panic the make().
	// Rejected synchronously as a validation error, before any state is reserved. (ENG-503)
	totalQuantity, err := backend.ValidateOperationQuantities(req.Items)
	if err != nil {
		return err
	}

	// Boundary normalization: auto-tag a single unnamed item with the default
	// service name so every downstream component (and the parser below) sees a
	// uniformly stack-shaped request. Rejects malformed mixed/multi-unnamed
	// inputs that were structurally invalid under the legacy contract too.
	//
	// Runs BEFORE the reservation because the reservation publishes this lease's
	// ownership claim, and a claim is a set of canonical volume names built from
	// item.ServiceName (canonicalVolumeName, restore.go). Reserving with
	// un-normalized items would claim fred-{lease}--0 while the provision goes on
	// to create fred-{lease}-app-0 — a claim that protects nothing (ENG-681).
	if err := backend.NormalizeProvisionRequest(&req); err != nil {
		return err
	}

	logger := b.logger.With(
		"lease_uuid", req.LeaseUUID,
		"tenant", req.Tenant,
		"items", len(req.Items),
		"total_quantity", totalQuantity,
	)

	// Complete every pure validation before creating the durable acceptance
	// record. The intent is still committed before reservation cleanup, pool
	// allocation, Docker, volume, or actor side effects.
	profiles := make(map[string]SKUProfile)
	for _, item := range req.Items {
		if _, ok := profiles[item.SKU]; ok {
			continue
		}
		profile, err := b.cfg.GetSKUProfile(item.SKU)
		if err != nil {
			return fmt.Errorf("%w: %w", backend.ErrValidation, err)
		}
		profiles[item.SKU] = profile
	}
	resourceProfiles, err := b.snapshotResourceProfiles(req.Items, profiles)
	if err != nil {
		return fmt.Errorf("%w: snapshot resource profiles: %w", backend.ErrValidation, err)
	}
	resourcesBySKU, err := resourceSnapshotMap(req.Items, resourceProfiles)
	if err != nil {
		return fmt.Errorf("%w: validate resource profiles: %w", backend.ErrValidation, err)
	}
	stackManifest, err := manifest.ParsePayload(req.Payload)
	if err != nil {
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
	}
	if isFlatPayload(req.Payload) {
		logger.Warn("manifest deprecation: tenant submitted flat single-service manifest; auto-wrapped as 1-service stack",
			"lease_uuid", req.LeaseUUID)
	}
	if err := manifest.ValidateStackAgainstItems(stackManifest, req.Items); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
	}
	if err := validateComposeServiceNames(req.Items); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
	}
	if err := manifest.ValidateNoFixedHostPorts(stackManifest); err != nil {
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
	}
	var healthCheckServices []string
	for svcName, svc := range stackManifest.Services {
		if err := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); err != nil {
			return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, err)
		}
		if svc.HasActiveHealthCheck() {
			healthCheckServices = append(healthCheckServices, svcName)
		}
	}
	slices.Sort(healthCheckServices)
	// Decide the exact label-level custom domains before the write-ahead
	// acceptance barrier. Desired items remain immutable operation input, while
	// effective items record DNS-deferred empty domains for exact crash recovery.
	desiredItems := slices.Clone(req.Items)
	b.deferUnreadyCustomDomains(ctx, req.Items, req.LeaseUUID, logger)
	effectiveItems := slices.Clone(req.Items)

	intent, proceed, err := b.beginOperationIntent(
		shared.OperationIntentProvision,
		req.LeaseUUID,
		req.CallbackURL,
		req.LifecycleCallbackURL,
		req.Tenant,
		req.ProviderUUID,
		desiredItems,
		resourceProfiles,
		effectiveItems,
		healthCheckServices,
		req.Payload,
		"",
		0,
	)
	if err != nil {
		return err
	}
	if !proceed {
		// An exact request retry was already durably accepted or its completion
		// is pending delivery. Returning success is idempotent; starting another
		// worker would duplicate the substrate mutation.
		return nil
	}
	if err := b.checkOperationReleaseCapacity(*intent); err != nil {
		return b.refuseOperationIntent(intent, fmt.Errorf(
			"%w: reserve provision success release: %w",
			backend.ErrInsufficientResources,
			err,
		))
	}

	// Build the complete candidate reservation before touching the predecessor.
	// The reservation is also this lease's ownership claim on its canonical
	// volumes, so every publish carries Items and immutable sizing together.
	var prevFailCount int
	var oldProvision *provision
	var oldSnapshot recoveredProvision
	b.provisionsMu.Lock()
	if existing, exists := b.provisions[req.LeaseUUID]; exists {
		if existing.Status != backend.ProvisionStatusFailed {
			b.provisionsMu.Unlock()
			return b.refuseOperationIntent(intent,
				fmt.Errorf("%w: %s", backend.ErrAlreadyProvisioned, req.LeaseUUID))
		}
		// Keep the pointer solely as a generation/CAS token. Every value used
		// after dropping provisionsMu comes from this deep snapshot: actors and
		// cleanup paths intentionally mutate published provisions in place.
		oldSnapshot = recoveredFromProvision(existing)
		prevFailCount = oldSnapshot.FailCount
		oldProvision = existing
	}
	candidate := recoveredProvision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:    req.LeaseUUID,
			Tenant:       req.Tenant,
			ProviderUUID: req.ProviderUUID,
			SKU:          "", // set by enrichReserved after validation
			Status:       backend.ProvisionStatusProvisioning,
			Quantity:     totalQuantity,
			CreatedAt:    time.Now(),
			FailCount:    prevFailCount,
			LastError:    "",
			Reason:       "", // fresh reservation, no failure
			Message:      "",
			// Both routes are stored at reservation time so any failure or
			// deprovision racing the validation window resolves the correct
			// exact or observational capability from the map.
			CallbackURL:          req.CallbackURL,
			LifecycleCallbackURL: req.LifecycleCallbackURL,
			Items:                slices.Clone(req.Items), // the ownership claim; see above
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
			ContainerIDs:         make([]string, 0, totalQuantity),
			StackManifest:        nil, // set by enrichReserved
			ServiceContainers:    nil,
		},
		// VolumeCleanupAttempts: 0 by struct-zero — structural reset of the
		// per-lease counter is the whole point of the wrapper.
		resourceProfiles:      shared.CloneSKUResourceSnapshot(resourceProfiles),
		volumeCleanupAttempts: 0,
	}.materialize()
	if oldProvision == nil {
		b.provisions[req.LeaseUUID] = candidate
	}
	b.provisionsMu.Unlock()

	// Allocation IDs are always service-aware now:
	// {leaseUUID}-{serviceName}-{instanceIndex}. The legacy {leaseUUID}-{idx}
	// scheme is gone from the live path; Task 9's recover-time migration
	// converts on-disk artifacts that still carry it.
	allocatedIDs := make([]string, 0, totalQuantity)
	replacementAllocations := make([]shared.ResolvedAdoptInstance, 0, totalQuantity)
	for _, item := range req.Items {
		for i := range item.Quantity {
			instanceID := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
			allocatedIDs = append(allocatedIDs, instanceID)
			replacementAllocations = append(replacementAllocations, shared.ResolvedAdoptInstance{
				ID:        instanceID,
				Resources: resourcesBySKU[item.SKU],
			})
		}
	}

	var predecessorAllocationIDs []string
	var predecessorAllocations []shared.ResolvedAdoptInstance
	if oldProvision != nil {
		predecessorUnchanged := func() bool {
			b.provisionsMu.RLock()
			defer b.provisionsMu.RUnlock()
			return b.provisions[req.LeaseUUID] == oldProvision &&
				provisionMatchesRecovered(oldProvision, oldSnapshot)
		}
		if oldSnapshot.Tenant != req.Tenant || oldSnapshot.ProviderUUID != req.ProviderUUID {
			return b.refuseOperationIntent(intent, fmt.Errorf(
				"failed provision predecessor belongs to a different tenant or provider",
			))
		}
		var predecessorErr error
		predecessorAllocationIDs, predecessorAllocations, predecessorErr = resolvedProvisionAllocations(
			req.LeaseUUID, oldSnapshot.Items, oldSnapshot.resourceProfiles,
		)
		if predecessorErr != nil {
			return b.refuseOperationIntent(intent, fmt.Errorf(
				"rebuild failed provision predecessor allocation: %w", predecessorErr,
			))
		}
		// The predecessor projection and pool reservation remain authoritative while
		// its exact cohort is torn down. The candidate intent is already durable, so
		// any incomplete teardown becomes an outcome ambiguity: stop this backend and
		// let cold recovery validate the surviving subset against the predecessor
		// active Release before retrying. Volumes are deliberately kept for reuse.
		inventoryCtx, cancelInventory := b.recoveryDockerReadContext(context.WithoutCancel(ctx))
		containers, inventoryErr := b.listManagedContainersStrictForRecovery(inventoryCtx)
		if inventoryErr != nil {
			cancelInventory()
			return b.refuseOperationIntent(intent, fmt.Errorf("inspect failed provision predecessor: %w", inventoryErr))
		}
		var active *shared.Release
		if b.releaseStore != nil {
			var releaseErr error
			active, releaseErr = b.releaseStore.LatestActive(req.LeaseUUID)
			if releaseErr != nil {
				cancelInventory()
				return b.refuseOperationIntent(intent, fmt.Errorf("read failed provision predecessor release: %w", releaseErr))
			}
		}
		if activeIdentity, hasActiveIdentity := runtimeIdentityForRelease(active); hasActiveIdentity {
			oldLifecycleURL := oldSnapshot.LifecycleCallbackURL
			if activeIdentity.Class() == shared.ReleaseAuthorityLegacy {
				oldLifecycleURL, err = backend.ResolveLifecycleCallbackURL(
					oldSnapshot.CallbackURL, oldSnapshot.LifecycleCallbackURL,
				)
				if err != nil {
					cancelInventory()
					return b.refuseOperationIntent(intent, fmt.Errorf(
						"validate failed provision predecessor callback pair: %w", err,
					))
				}
			}
			if activeIdentity.Tenant() != req.Tenant ||
				activeIdentity.ProviderUUID() != req.ProviderUUID ||
				oldSnapshot.Tenant != activeIdentity.Tenant() ||
				oldSnapshot.ProviderUUID != activeIdentity.ProviderUUID() ||
				oldSnapshot.CallbackURL != activeIdentity.CallbackURL() ||
				oldLifecycleURL != activeIdentity.LifecycleCallbackURL() {
				cancelInventory()
				return b.refuseOperationIntent(intent, fmt.Errorf(
					"active provision predecessor identity differs from the live projection",
				))
			}
		}
		classification, classifyErr := b.classifyProvisionIntentSubstrate(
			inventoryCtx, *intent, active, containers,
		)
		cancelInventory()
		if classifyErr != nil {
			return b.refuseOperationIntent(intent, fmt.Errorf("validate failed provision predecessor: %w", classifyErr))
		}
		if classification.hasCurrent {
			return b.latchAmbiguousOperationOutcome(
				"validate replacement provision predecessor",
				errors.New("candidate provision substrate exists before worker admission"),
			)
		}
		if !predecessorUnchanged() {
			return b.refuseOperationIntent(intent, errors.New(
				"failed provision predecessor changed during read-only validation",
			))
		}
		if classification.legacyAuthority != nil {
			if classification.legacyPredecessor == nil || b.releaseStore == nil {
				return b.latchAmbiguousOperationOutcome(
					"persist replacement provision predecessor authority",
					errors.New("legacy predecessor classification has no durable release store fence"),
				)
			}
			if persistErr := b.releaseStore.BackfillLegacyRuntimeAuthority(
				req.LeaseUUID,
				*classification.legacyPredecessor,
				*classification.legacyAuthority,
			); persistErr != nil {
				return b.refuseOperationIntent(intent, fmt.Errorf(
					"persist failed provision predecessor runtime authority: %w", persistErr,
				))
			}
		}
		if !predecessorUnchanged() {
			return b.refuseOperationIntent(intent, errors.New(
				"failed provision predecessor changed before teardown",
			))
		}

		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		remaining, teardownErr := b.teardownLeaseContainers(
			cleanupCtx, req.LeaseUUID, classification.currentIDs, 10*time.Second,
			teardownOpProvisionCleanup, logger,
		)
		cleanupCancel()
		if teardownErr != nil || len(remaining) != 0 {
			cleanupErr := teardownErr
			if cleanupErr == nil {
				cleanupErr = fmt.Errorf("predecessor teardown left %d container(s)", len(remaining))
			}
			return b.latchAmbiguousOperationOutcome("replace failed provision predecessor", cleanupErr)
		}

		b.provisionsMu.Lock()
		if b.provisions[req.LeaseUUID] != oldProvision ||
			!provisionMatchesRecovered(oldProvision, oldSnapshot) {
			b.provisionsMu.Unlock()
			return b.latchAmbiguousOperationOutcome(
				"publish replacement provision reservation",
				errors.New("failed predecessor projection changed during teardown"),
			)
		}
		if replaceErr := b.pool.ReplaceResolvedAll(
			predecessorAllocationIDs, replacementAllocations, req.Tenant,
		); replaceErr != nil {
			b.provisionsMu.Unlock()
			return b.refuseOperationIntent(intent,
				fmt.Errorf("%w: %w", backend.ErrInsufficientResources, replaceErr))
		}
		b.provisions[req.LeaseUUID] = candidate
		b.provisionsMu.Unlock()
		logger.Info("replacing failed provision", "fail_count", prevFailCount)
	} else {
		for i, allocation := range replacementAllocations {
			if err := b.pool.TryAllocateResolved(
				allocation.ID, req.Tenant, allocation.Resources,
			); err != nil {
				for _, id := range allocatedIDs[:i] {
					b.pool.Release(id)
				}
				b.removeProvision(req.LeaseUUID)
				return b.refuseOperationIntent(intent,
					fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err))
			}
		}
	}

	rollbackUnacceptedProvision := func(cause error) error {
		if oldProvision == nil {
			for _, id := range allocatedIDs {
				b.pool.Release(id)
			}
			b.removeProvision(req.LeaseUUID)
			return b.refuseOperationIntent(intent, cause)
		}

		// The predecessor cohort is already proven absent, but its active Release
		// still owns the reusable volumes and failed runtime projection. Move pool
		// accounting and the map back together before settling a candidate that was
		// explicitly rejected by the actor.
		predecessor := oldSnapshot
		predecessor.Status = backend.ProvisionStatusFailed
		predecessor.ContainerIDs = nil
		predecessor.ServiceContainers = nil
		b.provisionsMu.Lock()
		if b.provisions[req.LeaseUUID] != candidate {
			b.provisionsMu.Unlock()
			return b.latchAmbiguousOperationOutcome(
				"restore predecessor after rejected provision admission",
				errors.New("candidate provision projection changed before rollback"),
			)
		}
		if replaceErr := b.pool.ReplaceResolvedAll(
			allocatedIDs, predecessorAllocations, oldSnapshot.Tenant,
		); replaceErr != nil {
			b.provisionsMu.Unlock()
			return b.latchAmbiguousOperationOutcome(
				"restore predecessor resource reservation after rejected provision admission",
				replaceErr,
			)
		}
		b.provisions[req.LeaseUUID] = predecessor.materialize()
		b.provisionsMu.Unlock()
		return b.refuseOperationIntent(intent, cause)
	}

	// Update the reservation with full details now that validation passed. Items
	// are NOT set here — the reservation already published them as this lease's
	// ownership claim (ENG-681).
	b.provisionsMu.Lock()
	if prov, ok := b.provisions[req.LeaseUUID]; ok {
		prov.enrichReserved(req.RoutingSKU(), stackManifest)
	}
	b.provisionsMu.Unlock()

	// Hand off to the lease actor. The actor fires the SM transition,
	// acks accept/reject, and spawns the worker goroutine internally
	// (tracked by its workers barrier), so the actor's exit defers wait
	// on the worker's terminal sendTerminal before tearing the actor
	// down. The wait is bounded by workExitWaitTimeout; a truly wedged
	// worker is left as a zombie and recoverState reconciles on next
	// start.
	provCtx, provCancel := b.shutdownAwareContext()
	work := func() (string, backend.Reason, leasesm.ProvisionSuccessResult, map[string]string, error) {
		var replacementRecovery *replacementProvisionFailureRecovery
		if oldProvision != nil {
			replacementRecovery = &replacementProvisionFailureRecovery{}
		}
		return b.doProvisionWithOperationID(
			provCtx,
			req,
			stackManifest,
			resourceProfiles,
			intent.OperationID(),
			intent.CreatedAt(),
			replacementRecovery,
			logger,
		)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.ProvisionRequestedMsg{
		Cancel: provCancel,
		Work:   work,
		Ack:    ack,
	}); routeErr != nil {
		provCancel()
		return rollbackUnacceptedProvision(routeErr)
	}
	// Wait for the actor to fire evProvisionRequested on its SM. Only an
	// explicit rejection proves no worker exists and authorizes rollback. Once
	// enqueued, cancellation is an unknown outcome: preserve the intent,
	// reservation, and allocation for worker completion or startup recovery.
	//
	// The candidate allocation MUST be rolled back if the worker never starts.
	// A fresh provision drops it; a re-provision atomically restores the
	// predecessor allocation and Failed projection because its active Release
	// and reusable volumes remain authoritative after the old cohort teardown.
	acceptance, err := b.awaitAsyncAcceptance(ctx, ack)
	switch acceptance {
	case asyncAcceptanceAccepted:
		return nil
	case asyncAcceptanceUnknown:
		return fmt.Errorf("provision acceptance is unknown; durable recovery retained: %s", err.Error())
	case asyncAcceptanceRejected:
		provCancel()
		return rollbackUnacceptedProvision(err)
	default:
		return fmt.Errorf("invalid provision acceptance state %d", acceptance)
	}
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

	detectedUID, detectedGID, err := b.mutationAdapter().detectVolumeOwner(ctx, imageName, volumePaths)
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

	paths, err := b.mutationAdapter().detectWritablePaths(ctx, imageName, uid, candidateWritableParents)
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
		uid, gid, resolveErr := b.mutationAdapter().resolveImageUser(ctx, image, manifestUser)
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

// buildStatefulVolumeBinds creates subdirectories for each image VOLUME path
// under hostPath and returns bind mount mappings. Returns an error if any
// VOLUME path cannot be sanitized (unsupported path format).
func buildStatefulVolumeBindsContext(ctx context.Context, hostPath string, imageVolumes []string, uid, gid int) (map[string]string, error) {
	binds := make(map[string]string, len(imageVolumes))
	if len(imageVolumes) == 0 {
		return binds, nil
	}
	// Confine every subdir operation to the volume root. A tenant with a
	// read-write stateful volume can plant a symlink inside it on one deploy
	// (e.g. `data -> /`), then on a later deploy declare a VOLUME whose path
	// traverses that symlink. sanitizeVolumePath validates only the *string*, so
	// raw os.MkdirAll/os.Chown would follow the on-disk symlink and escape the
	// volume root — bind-mounting or chowning arbitrary host paths (host "/",
	// another tenant's volume) into the container. os.Root refuses to traverse
	// any symlinked component that escapes the root, so creation fails closed.
	// Mirrors the ENG-430 tar-extraction hardening. See ENG-539.
	//
	// That covers ESCAPING symlinks only. os.Root is escape-safe, not
	// symlink-free: it permits — and MkdirAll deliberately succeeds on — a leaf
	// symlink that resolves INSIDE the root, so the leaf-symlink check below is
	// a second, separate guard, not a restatement of this one. See ENG-795.
	root, err := os.OpenRoot(hostPath)
	if err != nil {
		return nil, fmt.Errorf("open volume root %q: %w", hostPath, err)
	}
	defer func() { _ = root.Close() }()

	for _, volPath := range imageVolumes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sanitized := sanitizeVolumePath(volPath)
		if sanitized == "" {
			return nil, fmt.Errorf("image declares unsupported VOLUME path %q", volPath)
		}
		// Reserve the _wp directory for writable-path scaffolding. A declared VOLUME
		// that sanitizes to _wp (e.g. VOLUME /_wp) or nests under it would place
		// STATEFUL data under the volume root's _wp subtree, which isWritablePathOnly
		// (retention_writable_path.go) would then misclassify as ephemeral scaffolding
		// and DESTROY at close — silent tenant data loss. Fail closed at provision.
		if sanitized == writablePathSubdir || strings.HasPrefix(sanitized, writablePathSubdir+"/") {
			return nil, fmt.Errorf("image declares VOLUME %q that collides with the reserved writable-path directory %q", volPath, writablePathSubdir)
		}
		if err := root.MkdirAll(sanitized, 0o700); err != nil {
			return nil, fmt.Errorf("volume subdir %q: %w", filepath.Join(hostPath, sanitized), err)
		}
		// MkdirAll succeeding does NOT mean the leaf is a directory. os.Root's
		// MkdirAll mirrors os.MkdirAll and returns nil when the leaf is a symlink
		// that resolves — within the root — to a directory, leaving the leaf a
		// symlink on disk. The bind Source emitted below is the raw joined string,
		// which Docker resolves host-side (runc trusts the mount source and lets the
		// kernel follow it), so a symlinked leaf silently redirects the mount to
		// whatever the link names. A tenant that planted `{root}/data/x -> ".."` on
		// an earlier deploy and then declares VOLUME /data/x gets its own volume
		// ROOT mounted read-write: that is where the .fred-project-id quota marker
		// lives (volume_xfs.go reads it back unvalidated on Destroy/EnsureQuota) and
		// whose unwritability isWritablePathOnly's close-time classification assumes.
		//
		// Fail closed, mirroring resolveMigratedBindSource (migrate.go). Unlike the
		// writable-path equivalent in setupWritablePathBinds — which SKIPS, because a
		// _wp path may legitimately go unseeded — a stateful VOLUME has no safe
		// fallback: omitting the bind would run the workload on the container's
		// ephemeral layer and lose its data at the next replace. ErrNotExist is an
		// error here too; MkdirAll just created this path. (ENG-795)
		//
		// Scope, so the next reader does not over-trust this: on PROVISION it is a hard
		// boundary — no tenant container exists yet, so nothing can race it. On
		// update/restart it is only a race narrowing, because doReplaceContainers calls
		// setupVolBinds while the tenant's OLD container is still running and lets the
		// later compose.Up stop it, so the leaf can be exchanged between this Lstat and
		// dockerd resolving the Source. Closing that needs the writer gone before the
		// check (what migrate.go relies on) — mounting by fd, the way kubelet does it,
		// is not available to us because dockerd performs the mount. See ENG-797.
		info, lerr := root.Lstat(sanitized)
		if lerr != nil {
			return nil, fmt.Errorf("resolve volume subdir %q: %w", filepath.Join(hostPath, sanitized), lerr)
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			volumeBindSymlinkRejectedTotal.Inc()
			return nil, fmt.Errorf("volume subdir %q resolves through a symlink", filepath.Join(hostPath, sanitized))
		}
		if uid != 0 || gid != 0 {
			// Lchown, never Chown: the guard above has established the leaf is a real
			// directory, and Lchown never follows a symlink even if one were raced in
			// (CVE-2026-32282).
			if err := root.Lchown(sanitized, uid, gid); err != nil {
				return nil, fmt.Errorf("chown volume subdir %q: %w", filepath.Join(hostPath, sanitized), err)
			}
		}
		binds[filepath.Join(hostPath, sanitized)] = volPath
	}
	return binds, nil
}

// writablePathSubdir is the single fixed subdirectory inside a managed volume
// that holds writable-path scaffolding (image content seeded for auto-detected
// writable paths under a read-only rootfs). It is the on-disk discriminator for
// a writable-path-only volume: such a volume contains only this subtree and no
// declared-VOLUME (stateful) subdirs. Its content is wiped+reseeded from the
// image on every deploy including restore (ENG-367 contract), so it is ephemeral
// by construction. See isWritablePathOnly (retention_writable_path.go) for the
// close-time reclaim decision (ENG-406).
const writablePathSubdir = "_wp"

// setupWritablePathBinds extracts image content for writable paths into
// a managed volume subdirectory and returns a bind map for container creation.
// Extraction failures are logged but don't fail the overall operation;
// paths that fail are simply omitted from the bind map.
func (b *Backend) setupWritablePathBinds(ctx context.Context, image string, writablePaths []string, hostVolumePath string, maxBytes, maxEntries int64) map[string]string {
	if len(writablePaths) == 0 {
		return nil
	}

	wpDir := filepath.Join(hostVolumePath, writablePathSubdir)
	// Remove stale content from prior extractions so files deleted
	// in a newer image don't persist.
	if err := b.mutationAdapter().removePath(ctx, wpDir); err != nil {
		b.logger.Warn("failed to clean up old writable path content, extraction may contain stale files",
			"path", wpDir, "error", err)
	}
	failures, authErr := b.mutationAdapter().extractImageContent(ctx, image, writablePaths, wpDir, maxBytes, maxEntries)
	if authErr != nil {
		b.logger.Warn("failed to authorize writable path extraction", "error", authErr)
		return nil
	}

	// The bind Source below is mounted read-write into the container and Docker
	// resolves it host-side. Docker's CopyFromContainer does NOT follow a
	// final-component symlink, so a symlink writable path would extract to a symlink
	// Source that redirects the mount outside the volume (host escape). Detection
	// only yields real-directory writable paths today, but confine the Source to
	// wpDir here so the bind path is self-defending regardless. See ENG-543.
	wpRoot, rootErr := os.OpenRoot(wpDir)
	if rootErr != nil && !errors.Is(rootErr, fs.ErrNotExist) {
		// An unexpected failure opening the _wp root (permission, I/O, not-a-dir)
		// means the confinement checks below cannot run. Fail closed: seed nothing
		// rather than mount an unvalidated (possibly symlinked) Source. ErrNotExist
		// is safe — nothing was extracted, so no Source exists to be a symlink.
		b.logger.Warn("cannot open writable-path root for confinement checks; skipping all writable-path binds",
			"path", wpDir, "image", image, "error", rootErr)
		return nil
	}
	if wpRoot != nil {
		defer func() { _ = wpRoot.Close() }()
	}

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
		if wpRoot != nil {
			// os.Root refuses to traverse a symlinked component that escapes wpDir
			// (surfaced as a non-ErrNotExist error), and Lstat flags a symlinked
			// leaf. Either way the Source is unsafe to bind: skip it (best-effort —
			// the writable path is simply not seeded, matching the extraction-failure
			// contract).
			switch info, lerr := wpRoot.Lstat(sanitized); {
			case lerr == nil && info.Mode()&fs.ModeSymlink != 0:
				b.logger.Warn("writable-path bind source is a symlink; skipping to prevent host escape",
					"path", wp, "image", image)
				continue
			case lerr != nil && !errors.Is(lerr, fs.ErrNotExist):
				b.logger.Warn("writable-path bind source failed confinement check; skipping",
					"path", wp, "image", image, "error", lerr)
				continue
			}
		}
		binds[filepath.Join(wpDir, sanitized)] = wp
	}

	return binds
}

// setupVolBinds creates volume bind mounts for all services/instances of a stack.
// It returns the volume binds map, a list of newly created volume IDs, and any fatal error.
// Non-fatal failures (writable-path-only volume creation) are logged as warnings.
func (b *Backend) setupVolBinds(
	ctx context.Context,
	leaseUUID string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
	imageSetups map[string]*imageSetup,
	services map[string]*manifest.Manifest,
	logger *slog.Logger,
) (map[string]map[int]serviceVolBinds, []string, error) {
	resourcesBySKU, err := resourceSnapshotMap(items, resourceProfiles)
	if err != nil {
		return nil, nil, fmt.Errorf("validate volume resource profiles: %w", err)
	}
	volBinds := make(map[string]map[int]serviceVolBinds)
	var createdVolumeIDs []string

	for _, item := range items {
		svcName := item.ServiceName
		resources := resourcesBySKU[item.SKU]
		profile := resources.Profile()
		imgSetup := imageSetups[svcName]

		for i := range item.Quantity {
			needsStatefulVolume := profile.DiskMB > 0 && len(imgSetup.Volumes) > 0
			needsWritableVolume := len(imgSetup.WritablePaths) > 0

			if needsStatefulVolume || needsWritableVolume {
				volumeID := canonicalVolumeName(leaseUUID, svcName, i)
				sizeMB := profile.DiskMB
				if sizeMB <= 0 {
					sizeMB = resources.ScratchDiskMB
				}
				hostPath, volCreated, volErr := b.createManagedVolume(ctx, volumeID, sizeMB)
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
					binds.StatefulBinds, buildErr = b.mutationAdapter().prepareStatefulVolumeBinds(ctx, hostPath, imgSetup.Volumes, imgSetup.VolumeUID, imgSetup.VolumeGID)
					if buildErr != nil {
						return nil, createdVolumeIDs, fmt.Errorf("volume setup failed (service %s, instance %d): %w", svcName, i, buildErr)
					}
				}
				if needsWritableVolume {
					binds.WritableBinds = b.setupWritablePathBinds(ctx, services[svcName].Image, imgSetup.WritablePaths, hostPath, sizeMB*1024*1024, inodeHardLimit(sizeMB, b.cfg.GetMinAvgFileBytes()))
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
func (b *Backend) verifyStartup(ctx context.Context, m *manifest.Manifest, containerIDs []string, logger *slog.Logger) error {
	if m.HasActiveHealthCheck() {
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
			diag := b.containerFailureDiagnostics(ctx, containerID, containerInfoToInstanceState(info))
			return fmt.Errorf("container %d exited during startup (status: %s): %s", i, info.Status, diag)
		}
	}
	return nil
}

// deferUnreadyCustomDomains zeroes the CustomDomain of any item whose domain
// does not yet resolve (ENG-266), so the provision emits no
// -custom Traefik router — and Traefik fires no HTTP-01 order — before DNS is
// live. The periodic reconcile (ReconcileCustomDomain) re-applies the domain on
// a later tick once it resolves. It zeroes the deferred domain on BOTH the
// caller's `items` slice (the label-emit path reads it via buildComposeProject)
// AND the stored prov.Items: enrichReserved deep-copies Items (ENG-193), so
// prov.Items no longer aliases the caller's slice and the in-memory state that
// recoverState / ReconcileCustomDomain read must be updated explicitly to stay
// consistent with the emitted container labels.
func (b *Backend) deferUnreadyCustomDomains(ctx context.Context, items []backend.LeaseItem, leaseUUID string, logger *slog.Logger) {
	// Phase 1: decide which items to defer — DNS I/O, no lock held.
	var toDefer []int
	for i := range items {
		d := items[i].CustomDomain
		if d == "" {
			continue
		}
		// Validate before any DNS I/O: a malformed/forbidden value is rejected
		// at label-emit time (applyIngressLabels), so resolving it is wasted
		// network work and would leak the bad value to the public resolvers.
		if err := validateCustomDomain(d, b.cfg.Ingress.WildcardDomain); err != nil {
			continue
		}
		if !b.dnsGateAllows(ctx, d) {
			logger.Info("custom_domain set but DNS does not resolve yet; deferring to reconcile",
				"lease_uuid", leaseUUID, "custom_domain", d)
			toDefer = append(toDefer, i)
		}
	}
	if len(toDefer) == 0 {
		return
	}
	// Phase 2: apply under provisionsMu. enrichReserved deep-copies Items, so
	// prov.Items no longer aliases the caller's slice — update both explicitly:
	// `items` for the label-emit path, prov.Items for the in-memory state that
	// recoverState / ReconcileCustomDomain read under provisionsMu (ENG-193).
	// Both slices are copies of the same normalized req.Items, so they
	// correspond index-for-index.
	b.provisionsMu.Lock()
	prov, ok := b.provisions[leaseUUID]
	for _, i := range toDefer {
		items[i].CustomDomain = ""
		if ok && i < len(prov.Items) {
			prov.Items[i].CustomDomain = ""
		}
	}
	b.provisionsMu.Unlock()
}

// doProvision performs container creation for a stack (multi-service) lease
// using Docker Compose. Compose handles container creation, start ordering, and
// network attachment atomically via a single Up call.
//
// Returns the (callbackErr, result, logs, err) contract; stack-specific result
// fields are stackManifest + serviceContainers.
func (b *Backend) doProvision(ctx context.Context, req backend.ProvisionRequest, stack *manifest.StackManifest, resourceProfiles []shared.SKUResourceSnapshot, logger *slog.Logger) (callbackErrRet string, reasonRet backend.Reason, resultRet leasesm.ProvisionSuccessResult, logsRet map[string]string, errRet error) {
	return b.doProvisionWithOperationID(ctx, req, stack, resourceProfiles, "", time.Now(), nil, logger)
}

func (b *Backend) doProvisionWithOperationID(
	ctx context.Context,
	req backend.ProvisionRequest,
	stack *manifest.StackManifest,
	resourceProfiles []shared.SKUResourceSnapshot,
	operationID shared.OperationID,
	releaseCreatedAt time.Time,
	replacementRecovery *replacementProvisionFailureRecovery,
	logger *slog.Logger,
) (callbackErrRet string, reasonRet backend.Reason, resultRet leasesm.ProvisionSuccessResult, logsRet map[string]string, errRet error) {
	runtimeAuthority, authorityErr := releaseRuntimeAuthorityForOperation(
		operationID,
		req.Tenant,
		req.ProviderUUID,
		req.CallbackURL,
		req.LifecycleCallbackURL,
	)
	if authorityErr != nil {
		return "validate runtime authority", backend.ReasonInternal, leasesm.ProvisionSuccessResult{}, nil,
			fmt.Errorf("validate provision runtime authority: %w", authorityErr)
	}
	profiles, profileErr := resourceProfileMap(req.Items, resourceProfiles)
	if profileErr != nil {
		return "validate resource profiles", backend.ReasonInternal, leasesm.ProvisionSuccessResult{}, nil,
			fmt.Errorf("validate provision resource profiles: %w", profileErr)
	}
	var containerIDs []string
	var createdVolumeIDs []string
	var err error
	var callbackErr string
	// failReason is the curated failure-category code, authored at the failure
	// site (ENG-508). Defaults to ReasonContainerExited (the common
	// startup-verify failure); specific sites override it (e.g. image pull).
	failReason := backend.ReasonContainerExited
	provisionStart := time.Now()
	serviceContainers := make(map[string][]string)
	projectName := composeProjectName(req.LeaseUUID)

	defer func() {
		provisionDurationSeconds.Observe(time.Since(provisionStart).Seconds())
		if err != nil {
			logger.Error("stack provision failed", "lease_uuid", req.LeaseUUID, "error", err)
			provisionsTotal.WithLabelValues("failure").Inc()

			// Capture logs from the failed containers BEFORE removal —
			// see doProvision's equivalent comment. For stacks we also
			// pass the service-name map so the persisted keys are
			// "web/0"-style rather than raw indices.
			logsRet = b.captureContainerLogs(containerIDs, stackContainerLogKeys(serviceContainers))

			// Clean up via Compose Down (removes all project containers), falling back to
			// per-container removal on failure. The fallback re-discovers by label rather
			// than walking containerIDs, which is still nil whenever Up itself failed —
			// it is only assigned from compose PS AFTER a successful Up, so the recorded
			// list is empty for exactly the failures that leave containers behind
			// (ENG-647). A failed teardown is not evidence that the containers stopped
			// using their bind mounts. Keep the exact operation intent, full resource
			// reservation, and volume claims in that case; destroying a mounted volume
			// or returning its capacity to the pool would turn a recoverable cleanup
			// failure into data loss or over-admission. The backend-lifetime latch also
			// prevents the actor's terminal event from settling the operation intent.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			remaining, tdErr := b.teardownLeaseContainers(cleanupCtx, req.LeaseUUID, containerIDs, 10*time.Second,
				teardownOpProvisionCleanup, logger)
			if tdErr != nil || len(remaining) != 0 {
				cleanupErr := tdErr
				if cleanupErr == nil {
					cleanupErr = fmt.Errorf("container teardown left %d container(s)", len(remaining))
				}
				ambiguousErr := b.latchAmbiguousOperationOutcome(
					"cleanup failed provision containers", cleanupErr,
				)
				logger.Error("failed to cleanup containers after provision error; preserving operation recovery authority",
					"remaining_containers", len(remaining), "error", ambiguousErr)
				callbackErrRet = leasesm.ErrMsgInternal
				reasonRet = backend.ReasonInternal
				errRet = errors.Join(err, ambiguousErr)
				updateResourceMetrics(b.pool.Stats())
				return
			}
			// createdVolumeIDs holds only the volumes THIS call brought into existence
			// (Create reports created=false for a pre-existing directory), so it cannot
			// name an adopted one — but the ownership check is not optional here either,
			// because "cannot name" is a property of a caller, and this site had no
			// check of its own at all (ENG-658). Cleanup must be complete before the
			// pool allocation can be returned. If even one created volume remains, retain
			// the intent and reservation so the next process can rebuild the full claim,
			// settle the failed attempt, then let the ordinary orphan pass reap only names
			// not protected by an older committed release.
			rep := b.volumeOp(req.LeaseUUID, logger).destroy(
				cleanupCtx, destroySiteProvisionCleanup, createdVolumeIDs...,
			)
			if rep.leftOnDisk() {
				cleanupErr := rep.err()
				if cleanupErr == nil {
					cleanupErr = fmt.Errorf(
						"volume cleanup left %d claimed or refused volume(s)",
						len(rep.Claimed)+len(rep.Unproven),
					)
				}
				ambiguousErr := b.latchAmbiguousOperationOutcome(
					"cleanup failed provision volumes", cleanupErr,
				)
				logger.Error("failed to cleanup volume(s) after provision error; preserving operation recovery authority",
					"destroyed", len(rep.Destroyed), "refused", rep.refused(), "error", ambiguousErr)
				callbackErrRet = leasesm.ErrMsgInternal
				reasonRet = backend.ReasonInternal
				errRet = errors.Join(err, ambiguousErr)
				updateResourceMetrics(b.pool.Stats())
				return
			}
			if replacementRecovery != nil {
				// The candidate may have reused canonical volumes owned by the
				// predecessor active Release. Absence of candidate-created volumes is
				// therefore not proof that returning the reservation is safe. Preserve
				// the exact candidate intent and its conservative full allocation; cold
				// recovery will settle the failed candidate and atomically rebuild the
				// predecessor projection/accounting from its durable Release.
				ambiguousErr := b.latchAmbiguousOperationOutcome(
					"recover failed replacement provision",
					errors.New("replacement provision failed after predecessor teardown"),
				)
				logger.Error("replacement provision failed; preserving predecessor volume accounting for restart recovery",
					"error", ambiguousErr)
				callbackErrRet = leasesm.ErrMsgInternal
				reasonRet = backend.ReasonInternal
				errRet = errors.Join(err, ambiguousErr)
				updateResourceMetrics(b.pool.Stats())
				return
			}

			// Containers and every volume created by this attempt are now proven
			// absent. Only this boundary authorizes returning the reservation; the actor
			// may then settle the exact failed operation normally.
			for _, item := range req.Items {
				for i := range item.Quantity {
					b.pool.Release(fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i))
				}
			}
			callbackErrRet = callbackErr
			// Reason is authored at the failure site (failReason); ENG-508.
			// Defaults to ReasonContainerExited (startup-verify), overridden by
			// specific sites (e.g. image pull → ImagePullFailed) so (reason,
			// message) stay consistent. The success path leaves reasonRet "".
			reasonRet = failReason
			errRet = err
			return
		}

		if b.releaseStore != nil {
			if relErr := b.releaseStore.AppendActive(req.LeaseUUID, shared.Release{
				Manifest:         req.Payload,
				Image:            "stack",
				OperationID:      operationID,
				Items:            slices.Clone(req.Items),
				ResourceProfiles: shared.CloneSKUResourceSnapshot(resourceProfiles),
				RuntimeAuthority: runtimeAuthority,
				Status:           "active",
				CreatedAt:        releaseCreatedAt,
			}); relErr != nil {
				// Compose succeeded, so ordinary failure cleanup would destroy a
				// possibly-complete cohort while the exact operation intent is the
				// only durable causal evidence. Latch the Backend and return an
				// ambiguous result without changing local err: the defer must retain
				// containers, volumes, and pool reservations for cold-start recovery.
				provisionsTotal.WithLabelValues("failure").Inc()
				callbackErrRet = leasesm.ErrMsgInternal
				reasonRet = backend.ReasonInternal
				errRet = b.latchAmbiguousOperationOutcome("record successful provision release", relErr)
				logger.Error("successful provision release could not be recorded; preserving substrate and operation intent for restart recovery",
					"lease_uuid", req.LeaseUUID, "error", errRet)
				updateResourceMetrics(b.pool.Stats())
				return
			}
		}

		provisionsTotal.WithLabelValues("success").Inc()

		if b.diagnosticsStore != nil {
			if delErr := b.diagnosticsStore.Delete(req.LeaseUUID); delErr != nil {
				b.logger.Warn("failed to remove stale diagnostic entry", "lease", req.LeaseUUID, "error", delErr)
			}
		}

		updateResourceMetrics(b.pool.Stats())

		resultRet = leasesm.ProvisionSuccessResult{
			ContainerIDs:      containerIDs,
			StackManifest:     stack,
			ServiceContainers: serviceContainers,
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
		if err = b.mutationAdapter().pullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); err != nil {
			logger.Error("failed to pull image", "service", svcName, "error", err)
			err = fmt.Errorf("image pull failed for service %s: %w", svcName, err)
			callbackErr = backend.MsgImagePullFailed
			failReason = backend.ReasonImagePullFailed
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
	volBinds, createdVolumeIDs, err = b.setupVolBinds(ctx, req.LeaseUUID, req.Items, resourceProfiles, imageSetups, stack.Services, logger)
	if err != nil {
		callbackErr = "volume creation failed"
		return
	}

	// DNS-readiness gate (ENG-266): defer not-yet-resolving custom domains so
	// provision doesn't fire a premature HTTP-01 order; the reconcile adds them
	// once DNS is live.
	b.deferUnreadyCustomDomains(ctx, req.Items, req.LeaseUUID, logger)

	// Build Compose project and bring it up.
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:            req.LeaseUUID,
		Tenant:               req.Tenant,
		ProviderUUID:         req.ProviderUUID,
		CallbackURL:          req.CallbackURL,
		LifecycleCallbackURL: req.LifecycleCallbackURL,
		BackendName:          b.cfg.Name,
		FailCount:            failCount,
		Stack:                stack,
		Items:                req.Items,
		Profiles:             profiles,
		ImageSetups:          imageSetups,
		NetworkName:          networkName,
		VolBinds:             volBinds,
		Cfg:                  &b.cfg,
		Ingress:              b.cfg.Ingress,
	})

	logger.Info("compose up", "project", projectName, "services", len(project.Services))
	if upErr := b.mutationAdapter().composeUp(ctx, project, composeUpOpts{}); upErr != nil {
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

	var mapErr error
	containerIDs, serviceContainers, mapErr = mapComposeContainers(containers, req.Items)
	if mapErr != nil {
		err = fmt.Errorf("map compose ps cohort: %w", mapErr)
		callbackErr = "container creation failed"
		return
	}
	if !exactServiceContainerCohort(req.Items, containerIDs, serviceContainers) {
		err = errors.New("compose ps returned an incomplete or duplicate provision cohort")
		callbackErr = "container creation failed"
		return
	}

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

// mapComposeContainers maps Compose PS output to containerIDs and logical
// service names using the exact keys produced by composeServiceName. Prefix
// parsing is deliberately forbidden: a valid service named "web-01" is not an
// instance of a scaled "web" service.
func mapComposeContainers(containers []composeContainerSummary, items []backend.LeaseItem) ([]string, map[string][]string, error) {
	logicalNames, err := composeServiceLogicalNames(items)
	if err != nil {
		return nil, nil, fmt.Errorf("derive Compose service keys: %w", err)
	}

	var containerIDs []string
	serviceContainers := make(map[string][]string)

	for _, c := range containers {
		containerIDs = append(containerIDs, c.ID)
		logicalName, exists := logicalNames[c.Service]
		if !exists {
			return containerIDs, serviceContainers, fmt.Errorf(
				"compose ps returned unknown service key %q",
				c.Service,
			)
		}
		serviceContainers[logicalName] = append(serviceContainers[logicalName], c.ID)
	}
	return containerIDs, serviceContainers, nil
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
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], containerInfoToInstanceState(info))
					return fmt.Errorf("container %d exited while waiting for healthy (status: %s): %s", i, info.Status, diag)
				}

				switch info.Health {
				case HealthStatusHealthy:
					logger.Info("container healthy", "instance", i, "container_id", leasesm.ShortID(containerIDs[i]))
					delete(pending, i)
				case HealthStatusUnhealthy:
					diag := b.containerFailureDiagnostics(ctx, containerIDs[i], containerInfoToInstanceState(info))
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
