package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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
	case "restart", "custom_domain":
		return backend.ReasonRestartFailed
	default:
		return backend.ReasonInternal
	}
}

// applyCustomDomainOverrides applies per-ServiceName custom_domain values to the
// given items slice, keyed by ServiceName so it is robust to a recoverState
// rebuild that reorders Items. No-op when overrides is empty.
//
// routeReplaceRestart applies this to the detached target-release copy. The
// actor later projects that exact durable release; no caller-authored mutation
// hook crosses the worker boundary.
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

// Restart restarts containers for a lease without changing the manifest.
// State machine: Ready|Failed → Restarting → Ready|Failed
//
// SEAM CLOSED (ENG-230). This prelude is read-only: it fast-fails on
// ErrNotProvisioned / ErrInvalidState under provisionsMu, snapshots the
// fields the worker needs, then does pure work (manifest marshal +
// release-store Append). It performs NO write to prov.Status or either
// callback URL — the lease actor's onEnterRestarting entry action is
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
	request, err := b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		req.MaintenanceID, shared.MaintenanceIntentRestart, req.LeaseUUID,
		req.CallbackURL, nil,
	)
	if err != nil {
		return fmt.Errorf("%w: invalid restart request authority: %w", backend.ErrValidation, err)
	}
	return b.routeReplaceRestart(ctx, request, nil)
}

// resolveMaintenanceCallbackURLs validates a trusted maintenance route against
// the authority already persisted with a lease. The callback base may move, but
// typed identity can never rotate or downgrade; legacy routes remain tokenless.
func resolveMaintenanceCallbackURLs(
	callbackURL, lifecycleCallbackURL, requestedLifecycleURL string,
) (string, string, error) {
	return backend.ResolveMaintenanceCallbackURLs(
		callbackURL, lifecycleCallbackURL, requestedLifecycleURL,
	)
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
func (b *Backend) routeReplaceRestart(
	ctx context.Context,
	request shared.MaintenanceRequestAuthority,
	overrides map[string]string,
) error {
	if !request.Valid() {
		return fmt.Errorf("%w: restart requires exact maintenance request authority", backend.ErrValidation)
	}
	expectedKind := shared.MaintenanceIntentRestart
	if len(overrides) != 0 {
		expectedKind = shared.MaintenanceIntentCustomDomain
	}
	if request.Kind() != expectedKind {
		return fmt.Errorf("%w: restart maintenance kind does not match route", backend.ErrValidation)
	}
	leaseUUID := request.LeaseUUID()
	callbackURL := request.CallbackURL()
	if err := b.requireMutationAdmission(ctx, "restart"); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	// Serialize the complete release prelude through actor acceptance. Release
	// history is keyed by lease and its settlement is intentionally
	// latest-generation based, so two callers must not both append a deploying
	// row before the actor chooses which worker owns the lease. Holding this
	// fence until the ack also publishes Restarting before a restore-finalizer
	// sweep can take its own snapshot under the same fence.
	unlockCommand := b.commandFence.Lock(leaseUUID)
	defer unlockCommand()
	if b.callbackStore != nil && b.releaseStore != nil {
		disposition, err := b.maintenanceSettlement.ProbeMaintenanceIntent(request)
		if err != nil {
			if errors.Is(err, shared.ErrMaintenanceIntentConflict) {
				return fmt.Errorf("%w: maintenance id conflicts with stored restart authority", backend.ErrInvalidState)
			}
			return fmt.Errorf("probe restart maintenance replay: %w", err)
		}
		if replayed, replayErr := maintenanceReplayResult(disposition); replayed {
			return replayErr
		}
	}
	if err := b.settleCommittedOperationBeforeMaintenance(leaseUUID); err != nil {
		return err
	}
	if len(overrides) == 0 {
		if err := b.ensureRestoreDestinationRestartAvailable(leaseUUID); err != nil {
			return err
		}
	} else if err := b.ensureRestoreDestinationUnowned(leaseUUID); err != nil {
		return err
	}

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
	callbackURL, lifecycleCallbackURL, callbackErr := resolveMaintenanceCallbackURLs(
		prov.CallbackURL, prov.LifecycleCallbackURL, callbackURL,
	)
	if callbackErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: maintenance lifecycle callback: %w", backend.ErrValidation, callbackErr)
	}
	stackManifest := prov.StackManifest
	items := append([]backend.LeaseItem(nil), prov.Items...)
	tenant := prov.Tenant
	providerUUID := prov.ProviderUUID
	authorityItems := slices.Clone(items)
	resourceProfiles := shared.CloneSKUResourceSnapshot(prov.ResourceProfiles)
	// Apply custom-domain overrides to the worker's snapshot COPY (never
	// prov.Items). Keyed by ServiceName, so even if recoverState swapped the
	// struct between the reconciler's diff and here, the desired domain is
	// re-applied onto the current items. (ENG-231/ENG-278)
	applyCustomDomainOverrides(items, overrides)
	b.provisionsMu.Unlock()
	if err := validateComposeServiceNames(items); err != nil {
		return fmt.Errorf("%w: stored topology cannot form an injective Compose project: %w", backend.ErrInvalidState, err)
	}
	if len(resourceProfiles) == 0 {
		var profileErr error
		resourceProfiles, profileErr = b.activeResourceProfiles(ctx, leaseUUID, authorityItems)
		if profileErr != nil {
			return fmt.Errorf("resolve restart resource profiles: %w", profileErr)
		}
	}
	if _, profileErr := resourceProfileMap(authorityItems, resourceProfiles); profileErr != nil {
		return fmt.Errorf("validate restart resource profiles: %w", profileErr)
	}
	if b.releaseStore == nil || b.callbackStore == nil {
		return errors.New("durable release and callback stores are required for restart")
	}

	active, sourceClaim, activeErr := b.maintenanceSettlement.ClaimLatestActive(leaseUUID)
	if activeErr != nil {
		return fmt.Errorf("claim active release lineage: %w", activeErr)
	}
	sourceErr := validateReplaceSourceRelease(active)
	if sourceErr != nil {
		return fmt.Errorf("construct restart source authority: %w", sourceErr)
	}
	runtimeAuthority, legacyRuntimeAuthority, authorityErr := releaseRuntimeAuthoritiesForMaintenance(
		active, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	if authorityErr != nil {
		return fmt.Errorf("construct restart release runtime authority: %w", authorityErr)
	}
	manifestBytes, marshalErr := json.Marshal(stackManifest)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal manifest for release: %w", marshalErr)
	}
	admission, admitErr := b.admitMaintenance(request, sourceClaim, shared.Release{
		Manifest:               manifestBytes,
		Image:                  "stack",
		OperationID:            active.OperationID,
		Items:                  slices.Clone(items),
		ResourceProfiles:       resourceProfiles,
		RuntimeAuthority:       runtimeAuthority,
		LegacyRuntimeAuthority: legacyRuntimeAuthority,
		Status:                 "deploying",
		CreatedAt:              time.Now(),
	})
	if admitErr != nil {
		return admitErr
	}
	if !admission.created() {
		return nil
	}
	maintenance, targetRelease := admission.intent, admission.target

	// Hand off to the lease actor. The actor's onEnterRestarting writes
	// Status=Restarting and, when requested, moves the callback pair to a new base
	// without changing its validated identity before acking. On success the
	// actor projects the exact durable target release atomically with Ready.
	opCtx, opCancel := b.shutdownAwareContext()
	var command leasesm.ActorCommand
	var ack leasesm.ActorReply
	var commandErr error
	if request.Kind() == shared.MaintenanceIntentCustomDomain {
		command, ack, commandErr = leasesm.NewCustomDomainCommand(opCtx, targetRelease)
	} else {
		command, ack, commandErr = leasesm.NewRestartCommand(opCtx, targetRelease)
	}
	if commandErr != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, commandErr)
	}
	if routeErr := b.routeToLeaseBlocking(ctx, leaseUUID, command); routeErr != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, routeErr)
	}
	// Once routing succeeds, wait for the actor's definitive decision even if
	// the caller cancels. Returning on cancellation would release commandFence
	// while this message remained queued: a retry could append a newer release,
	// then this worker could accept and settle that retry's row as its own. The
	// caller context already bounded routing; an enqueued command must reach its
	// actor linearization point before the release fence can open.
	if err := <-ack.Result(); err != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, err)
	}
	return nil
}

// validateReplaceSourceRelease proves that the exact active Release claimed
// before maintenance admission contains complete rollback authority. The
// physical executor consumes the claim itself, so retaining a second copied
// snapshot would only create a divergent representation of the same authority.
func validateReplaceSourceRelease(release shared.Release) error {
	if release.Status != "active" {
		return errors.New("source release lacks active runtime authority")
	}
	if _, ok := runtimeIdentityForRelease(&release); !ok {
		return errors.New("source release lacks active runtime authority")
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return fmt.Errorf("parse source manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, release.Items); err != nil {
		return fmt.Errorf("validate source topology: %w", err)
	}
	if _, err := resourceProfileMap(release.Items, release.ResourceProfiles); err != nil {
		return fmt.Errorf("validate source resource profiles: %w", err)
	}
	return nil
}

// releaseRuntimeAuthoritiesForMaintenance preserves the active release's
// authority class. Current generations retain their operation-scoped typed
// authority; v0.13 generations retain a separately typed tokenless authority.
// MaintenanceID remains the exact UUIDv4 identity of the replacement WAL in
// both cases, so supporting a legacy source does not manufacture a provision
// operation capability that never existed.
func releaseRuntimeAuthoritiesForMaintenance(
	active shared.Release,
	tenant, providerUUID, callbackURL, lifecycleCallbackURL string,
) (*shared.ReleaseRuntimeAuthority, *shared.LegacyRuntimeAuthority, error) {
	authority, ok := runtimeIdentityForRelease(&active)
	if !ok {
		return nil, nil, errors.New("active release has no durable runtime authority")
	}
	if authority.Class() == shared.ReleaseAuthorityLegacy {
		legacy, err := shared.NewLegacyRuntimeAuthority(
			tenant, providerUUID, callbackURL, lifecycleCallbackURL,
		)
		if err != nil {
			return nil, nil, err
		}
		return nil, &legacy, nil
	}
	typed, err := releaseRuntimeAuthorityForOperation(
		active.OperationID, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	if err != nil {
		return nil, nil, err
	}
	if typed == nil {
		return nil, nil, errors.New("typed active release has no operation lineage")
	}
	return typed, nil, nil
}

// replaceContainersOp describes a stack container replacement operation.
type replaceContainersOp struct {
	LeaseUUID           string
	Stack               *manifest.StackManifest
	Items               []backend.LeaseItem
	ResourceProfiles    []shared.SKUResourceSnapshot
	Operation           string // "restart", "update", or "restore"
	TargetMaintenanceID shared.MaintenanceID
	Logger              *slog.Logger
}

func exactServiceContainerCohort(
	items []backend.LeaseItem,
	containerIDs []string,
	serviceContainers map[string][]string,
) bool {
	expected, err := backend.ValidateOperationQuantities(items)
	if err != nil || len(containerIDs) != expected || len(serviceContainers) != len(items) {
		return false
	}
	seen := make(map[string]struct{}, len(containerIDs))
	for _, item := range items {
		ids, ok := serviceContainers[item.ServiceName]
		if !ok || len(ids) != item.Quantity {
			return false
		}
		for _, id := range ids {
			if id == "" {
				return false
			}
			if _, duplicate := seen[id]; duplicate {
				return false
			}
			seen[id] = struct{}{}
		}
	}
	return len(seen) == expected
}

// Update deploys a new manifest for a lease, replacing containers.
// State machine: Ready|Failed → Updating → Ready|Failed
//
// SEAM CLOSED (ENG-230) — see the extended comment on Backend.Restart.
// Like Restart, the prelude is read-only: it fast-fails / validates
// under provisionsMu, snapshots fields, then records the release. It
// performs NO write to prov.Status or either callback URL — the actor's
// onEnterUpdating entry action is the sole status writer, firing inside
// handleUpdateRequested BEFORE the ack, so the "Update() returns =>
// Status is Updating" contract holds without an off-actor write. No
// rollback is needed on any failure path (nothing on prov was mutated).
func (b *Backend) Update(ctx context.Context, req backend.UpdateRequest) error {
	request, err := b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		req.MaintenanceID, shared.MaintenanceIntentUpdate, req.LeaseUUID,
		req.CallbackURL, req.Payload,
	)
	if err != nil {
		return fmt.Errorf("%w: invalid update request authority: %w", backend.ErrValidation, err)
	}
	if err := b.requireMutationAdmission(ctx, "update"); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	// See routeReplaceRestart: the release Append and the actor's authoritative
	// Updating transition are one admission critical section. A losing caller
	// observes Updating before it can append a second, unowned release row.
	unlockCommand := b.commandFence.Lock(req.LeaseUUID)
	defer unlockCommand()
	if b.callbackStore != nil && b.releaseStore != nil {
		disposition, err := b.maintenanceSettlement.ProbeMaintenanceIntent(request)
		if err != nil {
			if errors.Is(err, shared.ErrMaintenanceIntentConflict) {
				return fmt.Errorf("%w: maintenance id conflicts with stored update authority", backend.ErrInvalidState)
			}
			return fmt.Errorf("probe update maintenance replay: %w", err)
		}
		if replayed, replayErr := maintenanceReplayResult(disposition); replayed {
			return replayErr
		}
	}
	logger := b.logger.With("lease_uuid", req.LeaseUUID)
	if err := b.settleCommittedOperationBeforeMaintenance(req.LeaseUUID); err != nil {
		return err
	}
	if err := b.ensureRestoreDestinationUnowned(req.LeaseUUID); err != nil {
		return err
	}

	// Synchronous phase: read-only validation + field snapshot (no
	// prov.Status or callback URL writes — ENG-230).
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
	callbackURL, lifecycleCallbackURL, callbackErr := resolveMaintenanceCallbackURLs(
		prov.CallbackURL, prov.LifecycleCallbackURL, req.CallbackURL,
	)
	if callbackErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: maintenance lifecycle callback: %w", backend.ErrValidation, callbackErr)
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
	if nameErr := validateComposeServiceNames(prov.Items); nameErr != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, nameErr)
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
	items := append([]backend.LeaseItem(nil), prov.Items...)
	tenant := prov.Tenant
	providerUUID := prov.ProviderUUID
	resourceProfiles := shared.CloneSKUResourceSnapshot(prov.ResourceProfiles)
	// Runtime projection belongs to the actor and exact physical evidence;
	// this admission snapshot cannot decide whether a failed update restored
	// a healthy source.
	b.provisionsMu.Unlock()
	if len(resourceProfiles) == 0 {
		var profileErr error
		resourceProfiles, profileErr = b.activeResourceProfiles(ctx, req.LeaseUUID, items)
		if profileErr != nil {
			return fmt.Errorf("%w: resolve update resource profiles: %w", backend.ErrInvalidState, profileErr)
		}
	}
	if _, profileErr := resourceProfileMap(items, resourceProfiles); profileErr != nil {
		return fmt.Errorf("%w: validate update resource profiles: %w", backend.ErrInvalidState, profileErr)
	}
	if b.releaseStore == nil || b.callbackStore == nil {
		return errors.New("durable release and callback stores are required for update")
	}

	active, sourceClaim, activeErr := b.maintenanceSettlement.ClaimLatestActive(req.LeaseUUID)
	if activeErr != nil {
		return fmt.Errorf("claim active release lineage: %w", activeErr)
	}
	sourceErr := validateReplaceSourceRelease(active)
	if sourceErr != nil {
		return fmt.Errorf("construct update source authority: %w", sourceErr)
	}
	runtimeAuthority, legacyRuntimeAuthority, authorityErr := releaseRuntimeAuthoritiesForMaintenance(
		active, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	if authorityErr != nil {
		return fmt.Errorf("construct update release runtime authority: %w", authorityErr)
	}
	admission, admitErr := b.admitMaintenance(
		request,
		sourceClaim,
		shared.Release{
			Manifest:               req.Payload,
			Image:                  "stack",
			OperationID:            active.OperationID,
			Items:                  slices.Clone(items),
			ResourceProfiles:       resourceProfiles,
			RuntimeAuthority:       runtimeAuthority,
			LegacyRuntimeAuthority: legacyRuntimeAuthority,
			Status:                 "deploying",
			CreatedAt:              time.Now(),
		},
	)
	if admitErr != nil {
		return admitErr
	}
	if !admission.created() {
		return nil
	}
	maintenance, targetRelease := admission.intent, admission.target

	// Hand off to the actor. The actor's onEnterUpdating writes Status=Updating
	// and the prevalidated same-authority callback pair BEFORE acking. See
	// handleUpdateRequested / spawnReplaceWorker.
	opCtx, opCancel := b.shutdownAwareContext()
	command, ack, commandErr := leasesm.NewUpdateCommand(opCtx, targetRelease)
	if commandErr != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, commandErr)
	}
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, command); routeErr != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, routeErr)
	}
	// See routeReplaceRestart: after enqueue, commandFence stays closed until
	// the actor has definitively accepted or rejected this exact release.
	if err := <-ack.Result(); err != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, err)
	}
	return nil
}

// doUpdate performs the actual stack container update asynchronously.
//
// Unlike doRestart, doUpdate takes no wasActive flag: an update preflight
// failure (image pull) is unconditionally Failed — a missed image pull never
// achieved the desired new-image state, so the lease is Failed even from a
// Ready source. This asymmetry is intentional; do not key it on wasActive.
