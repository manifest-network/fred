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
	return b.routeReplaceRestart(ctx, req.LeaseUUID, req.CallbackURL, nil)
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
func (b *Backend) routeReplaceRestart(ctx context.Context, leaseUUID, callbackURL string, overrides map[string]string) error {
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
	logger := b.logger.With("lease_uuid", leaseUUID)
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
		resourceProfiles, profileErr = b.activeResourceProfiles(leaseUUID, authorityItems)
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
	active, sourceClaim, activeErr := b.releaseStore.ClaimLatestActive(leaseUUID)
	if activeErr != nil {
		return fmt.Errorf("claim active release lineage: %w", activeErr)
	}
	source, sourceErr := newReplaceSourceSnapshot(active)
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
	kind := shared.MaintenanceIntentRestart
	if len(overrides) != 0 {
		kind = shared.MaintenanceIntentCustomDomain
	}
	maintenance, targetRelease, admitErr := b.admitMaintenance(kind, sourceClaim, shared.Release{
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

	// Hand off to the lease actor. The actor's onEnterRestarting writes
	// Status=Restarting and, when requested, moves the callback pair to a new base
	// without changing its validated identity before acking. On success,
	// onEnterReadyFromReplaceCompleted runs onSuccess (the prov.Items
	// custom_domain commit) under UpdateFn, atomic with Status->Ready.
	opCtx, opCancel := b.shutdownAwareContext()
	onSuccess := customDomainOnSuccess(overrides)
	work := func() leasesm.ReplaceResult {
		return b.doRestart(
			opCtx,
			leaseUUID,
			stackManifest,
			resourceProfiles,
			items,
			callbackURL,
			lifecycleCallbackURL,
			maintenance,
			targetRelease,
			source,
			onSuccess,
			logger,
		)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, leaseUUID, leasesm.RestartRequestedMsg{
		Cancel:               opCancel,
		Work:                 work,
		Ack:                  ack,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Maintenance:          maintenance,
	}); routeErr != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, routeErr)
	}
	// Once routing succeeds, wait for the actor's definitive decision even if
	// the caller cancels. Returning on cancellation would release commandFence
	// while this message remained queued: a retry could append a newer release,
	// then this worker could accept and settle that retry's row as its own. The
	// caller context already bounded routing; an enqueued command must reach its
	// actor linearization point before the release fence can open.
	if err := <-ack; err != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, err)
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
func (b *Backend) doRestart(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, resourceProfiles []shared.SKUResourceSnapshot, items []backend.LeaseItem, callbackURL, lifecycleCallbackURL string, maintenance shared.MaintenanceIntentClaim, targetRelease shared.MaintenanceReleaseClaim, source replaceSourceSnapshot, onSuccess func(*leasesm.ProvisionState), logger *slog.Logger) leasesm.ReplaceResult {
	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:            leaseUUID,
		Stack:                stack,
		Items:                items,
		ResourceProfiles:     resourceProfiles,
		Operation:            "restart",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Maintenance:          maintenance,
		TargetRelease:        targetRelease,
		Source:               source,
		TargetMaintenanceID:  maintenance.MaintenanceID(),
		Logger:               logger,
		OnSuccess:            onSuccess,
	})
}

// replaceSourceSnapshot is the immutable rollback authority copied from the
// exact active Release claimed before maintenance admission. Rollback never
// reconstructs this identity from the actor's pending target projection.
type replaceSourceSnapshot struct {
	release              shared.Release
	Stack                *manifest.StackManifest
	Items                []backend.LeaseItem
	ResourceProfiles     []shared.SKUResourceSnapshot
	Tenant               string
	ProviderUUID         string
	CallbackURL          string
	LifecycleCallbackURL string
	MaintenanceID        shared.MaintenanceID
}

func newReplaceSourceSnapshot(release shared.Release) (replaceSourceSnapshot, error) {
	if release.Status != "active" {
		return replaceSourceSnapshot{}, errors.New("source release lacks active runtime authority")
	}
	authority, ok := runtimeIdentityForRelease(&release)
	if !ok {
		return replaceSourceSnapshot{}, errors.New("source release lacks active runtime authority")
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return replaceSourceSnapshot{}, fmt.Errorf("parse source manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, release.Items); err != nil {
		return replaceSourceSnapshot{}, fmt.Errorf("validate source topology: %w", err)
	}
	if _, err := resourceProfileMap(release.Items, release.ResourceProfiles); err != nil {
		return replaceSourceSnapshot{}, fmt.Errorf("validate source resource profiles: %w", err)
	}
	return replaceSourceSnapshot{
		release:              release,
		Stack:                stack,
		Items:                slices.Clone(release.Items),
		ResourceProfiles:     shared.CloneSKUResourceSnapshot(release.ResourceProfiles),
		Tenant:               authority.Tenant(),
		ProviderUUID:         authority.ProviderUUID(),
		CallbackURL:          authority.CallbackURL(),
		LifecycleCallbackURL: authority.LifecycleCallbackURL(),
		MaintenanceID:        release.MaintenanceID,
	}, nil
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
	LeaseUUID        string
	Stack            *manifest.StackManifest
	Items            []backend.LeaseItem
	ResourceProfiles []shared.SKUResourceSnapshot
	Operation        string // "restart", "update", or "restore"
	// CallbackURL/LifecycleCallbackURL are the pending maintenance route
	// emitted into the replacement cohort. Rollback deliberately ignores them
	// and reads the prior committed pair from ProvisionState.
	CallbackURL          string
	LifecycleCallbackURL string
	Maintenance          shared.MaintenanceIntentClaim
	TargetRelease        shared.MaintenanceReleaseClaim
	Source               replaceSourceSnapshot
	TargetMaintenanceID  shared.MaintenanceID
	Logger               *slog.Logger

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
	var preserveMaintenance bool
	var readyToCommit bool
	var newContainerIDs []string
	var imageSetups map[string]*imageSetup
	newServiceContainers := make(map[string][]string)
	projectName := composeProjectName(op.LeaseUUID)
	var profiles map[string]SKUProfile

	defer func() {
		// A replacement is not complete until the exact release generation is
		// durably active. If that commit fails, route through the existing
		// rollback/failure path instead of reporting success from substrate state
		// alone. Restore owns a separate finalizer/release commit and deliberately
		// remains outside this restart/update boundary.
		if err == nil && readyToCommit && b.releaseStore != nil && op.Operation != "restore" {
			if relErr := b.releaseStore.ActivateMaintenance(op.TargetRelease); relErr != nil {
				err = fmt.Errorf("activate successful %s release: %w", op.Operation, relErr)
				callbackErr = op.Operation + " failed"
				preserveMaintenance = true
			}
		}
		if err != nil {
			op.Logger.Error(op.Operation+" failed (stack)", "error", err)

			if b.releaseStore != nil && op.Operation != "restore" && !preserveMaintenance {
				// Restart and Update append a deploying release before actor
				// admission, so their worker owns the latest row while the provision
				// remains busy. Restore deliberately has no deploying row: its
				// successful finalizer appends active directly, and a failure must not
				// rewrite residual history for a reused destination UUID.
				rReason, rMsg := replaceOpReason(op.Operation), op.Operation+" failed"
				if relErr := b.releaseStore.FailMaintenance(op.TargetRelease, rReason, rMsg); relErr != nil {
					op.Logger.Warn("failed to update release status", "error", relErr)
					preserveMaintenance = true
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
			if !op.NoComposeRollback && !preserveMaintenance {
				restored = b.rollbackViaCompose(op)
			}
			if restored {
				op.Logger.Info("rolled back to previous containers via compose (stack)")
				callbackErr += "; rolled back to previous version"
			} else {
				callbackErr += "; rollback failed"
				if op.Operation != "restore" {
					// A failed rollback may leave an exact target/source mixture. Keep
					// the MaintenanceIntent as the only authority allowed to inspect and
					// remove target-ID containers on this process or a later restart.
					preserveMaintenance = true
				}
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
					Operation:           op.Operation,
					Reason:              replaceOpReason(op.Operation),
					OldStopped:          true,
					CallbackErr:         callbackErr,
					LastError:           err.Error(),
					Logs:                failureLogs,
					PreserveMaintenance: preserveMaintenance,
				},
			}
			return
		}

		resultRet = leasesm.ReplaceResult{
			Success: leasesm.ReplaceSuccessResult{
				ContainerIDs:      newContainerIDs,
				ServiceContainers: newServiceContainers,
				OnSuccess:         op.OnSuccess,
			},
		}
	}()

	if op.Operation != "restore" &&
		(!op.Maintenance.Valid() || !op.TargetRelease.Valid() ||
			op.Maintenance.MaintenanceID() != op.TargetRelease.MaintenanceID()) {
		err = errors.New("restart/update requires exact maintenance authority")
		callbackErr = op.Operation + " failed"
		preserveMaintenance = true
		return
	}

	profiles, err = resourceProfileMap(op.Items, op.ResourceProfiles)
	if err != nil {
		err = fmt.Errorf("validate %s resource profiles: %w", op.Operation, err)
		callbackErr = op.Operation + " failed"
		return
	}

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
	lifecycleCallbackURL := ""
	if prov, ok := b.provisions[op.LeaseUUID]; ok {
		failCount = prov.FailCount
		tenant = prov.Tenant
		providerUUID = prov.ProviderUUID
		callbackURL = prov.CallbackURL
		lifecycleCallbackURL = prov.LifecycleCallbackURL
	}
	b.provisionsMu.RUnlock()
	if op.CallbackURL != "" {
		callbackURL = op.CallbackURL
	}
	if op.LifecycleCallbackURL != "" {
		lifecycleCallbackURL = op.LifecycleCallbackURL
	}

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
	volBinds, _, volErr := b.setupVolBinds(ctx, op.LeaseUUID, op.Items, op.ResourceProfiles, imageSetups, op.Stack.Services, op.Logger)
	if volErr != nil {
		err = volErr
		callbackErr = op.Operation + " failed"
		return
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseVolumeSetup).Observe(time.Since(volStart).Seconds())

	// Build Compose project and bring it up.
	// ForceRecreate is used for restarts (config unchanged but containers need replacing).
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:            op.LeaseUUID,
		Tenant:               tenant,
		ProviderUUID:         providerUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		MaintenanceID:        op.TargetMaintenanceID,
		BackendName:          b.cfg.Name,
		FailCount:            failCount,
		Stack:                op.Stack,
		Items:                op.Items,
		Profiles:             profiles,
		ImageSetups:          imageSetups,
		NetworkName:          networkName,
		VolBinds:             volBinds,
		Cfg:                  &b.cfg,
		Ingress:              b.cfg.Ingress,
	})

	op.Logger.Info("compose up for "+op.Operation, "project", projectName, "services", len(project.Services))
	forceRecreate := op.Operation == "restart"
	upStart := time.Now()
	if upErr := b.mutationAdapter().composeUp(ctx, project, composeUpOpts{ForceRecreate: forceRecreate}); upErr != nil {
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

	var mapErr error
	newContainerIDs, newServiceContainers, mapErr = mapComposeContainers(containers, op.Items)
	if mapErr != nil {
		err = fmt.Errorf("map compose ps %s cohort: %w", op.Operation, mapErr)
		callbackErr = op.Operation + " failed"
		return
	}
	if !exactServiceContainerCohort(op.Items, newContainerIDs, newServiceContainers) {
		err = fmt.Errorf("compose ps returned an incomplete or duplicate %s cohort", op.Operation)
		callbackErr = op.Operation + " failed"
		return
	}
	if op.Operation != "restore" {
		if _, strictErr := b.strictReleaseCohortForComposePS(
			ctx, op.LeaseUUID, op.Maintenance.TargetRelease(), newContainerIDs,
		); strictErr != nil {
			err = fmt.Errorf("strict %s target proof: %w", op.Operation, strictErr)
			callbackErr = op.Operation + " failed"
			return
		}
	}

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
	// This is the sole activation gate. In particular, a panic anywhere before
	// every startup check completes leaves it false, so the enclosing worker
	// recovery cannot accidentally supersede the source release while unwinding.
	readyToCommit = true
	return
}

// rollbackViaCompose restores the previous stack state by rebuilding a
// Compose project from the previous StackManifest (still in the provision,
// since OnSuccess hasn't run) and calling Compose Up. Returns true on success.
func (b *Backend) rollbackViaCompose(op replaceContainersOp) bool {
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// The exact source generation was copied from the active durable Release
	// before maintenance admission. The actor already projects the pending target
	// callback route by this point, so deriving rollback labels from ProvisionState
	// would silently relabel the source generation as the failed target.
	b.provisionsMu.RLock()
	prov, ok := b.provisions[op.LeaseUUID]
	if !ok {
		b.provisionsMu.RUnlock()
		op.Logger.Error("rollback: provision not found")
		return false
	}
	failCount := prov.FailCount
	b.provisionsMu.RUnlock()

	if op.Source.Stack == nil || len(op.Source.Items) == 0 {
		op.Logger.Error("rollback: no exact source release authority")
		return false
	}
	profiles, profileErr := resourceProfileMap(op.Source.Items, op.Source.ResourceProfiles)
	if profileErr != nil {
		op.Logger.Error("rollback: resource profiles invalid", "error", profileErr)
		return false
	}

	// Inspect images for the previous manifest.
	prevImageSetups := make(map[string]*imageSetup)
	for svcName, svc := range op.Source.Stack.Services {
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
		networkName = TenantNetworkName(op.Source.Tenant)
	}

	// Re-use existing volumes (already created during original provision).
	volBinds, _, volErr := b.setupVolBinds(rollbackCtx, op.LeaseUUID, op.Source.Items, op.Source.ResourceProfiles, prevImageSetups, op.Source.Stack.Services, op.Logger)
	if volErr != nil {
		op.Logger.Error("rollback: volume setup failed", "error", volErr)
		return false
	}

	// Build project from previous manifest.
	project := buildComposeProject(composeProjectParams{
		LeaseUUID:            op.LeaseUUID,
		Tenant:               op.Source.Tenant,
		ProviderUUID:         op.Source.ProviderUUID,
		CallbackURL:          op.Source.CallbackURL,
		LifecycleCallbackURL: op.Source.LifecycleCallbackURL,
		MaintenanceID:        op.Source.MaintenanceID,
		BackendName:          b.cfg.Name,
		FailCount:            failCount,
		Stack:                op.Source.Stack,
		Items:                op.Source.Items,
		Profiles:             profiles,
		ImageSetups:          prevImageSetups,
		NetworkName:          networkName,
		VolBinds:             volBinds,
		Cfg:                  &b.cfg,
		Ingress:              b.cfg.Ingress,
	})

	// Compose Up with ForceRecreate to restore previous containers.
	if upErr := b.mutationAdapter().composeUp(rollbackCtx, project, composeUpOpts{ForceRecreate: true}); upErr != nil {
		op.Logger.Error("rollback: compose up failed", "error", upErr)
		return false
	}

	// Discover restored container IDs and update provision.
	containers, psErr := b.compose.PS(rollbackCtx, composeProjectName(op.LeaseUUID))
	if psErr != nil {
		op.Logger.Error("rollback: compose ps failed", "error", psErr)
		return false
	}

	containerIDs, serviceContainers, mapErr := mapComposeContainers(containers, op.Source.Items)
	if mapErr != nil {
		op.Logger.Error("rollback: map compose ps source cohort failed", "error", mapErr)
		return false
	}
	if !exactServiceContainerCohort(op.Source.Items, containerIDs, serviceContainers) {
		op.Logger.Error("rollback: compose ps did not return the exact source cohort")
		return false
	}
	observed, strictErr := b.strictReleaseCohortForComposePS(
		rollbackCtx, op.LeaseUUID, op.Source.release, containerIDs,
	)
	if strictErr != nil {
		op.Logger.Error("rollback: strict source proof failed", "error", strictErr)
		return false
	}
	if readyErr := b.verifyRecoveredMaintenanceReadiness(
		rollbackCtx, op.Source.release, observed,
	); readyErr != nil {
		op.Logger.Error("rollback: source cohort is not ready", "error", readyErr)
		return false
	}
	b.provisionsMu.Lock()
	if p, ok := b.provisions[op.LeaseUUID]; ok {
		p.ContainerIDs = containerIDs
		p.ServiceContainers = serviceContainers
	}
	b.provisionsMu.Unlock()

	return true
}

func (b *Backend) strictReleaseCohortForComposePS(
	ctx context.Context,
	leaseUUID string,
	release shared.Release,
	composeIDs []string,
) ([]ContainerInfo, error) {
	strict, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return nil, fmt.Errorf("strict managed-container inventory: %w", err)
	}
	observed := make([]ContainerInfo, 0, len(composeIDs))
	for _, container := range strict {
		if container.LeaseUUID == leaseUUID && !isLegacyRollbackRemnant(container) {
			observed = append(observed, container)
		}
	}
	if err := validateRecoveredReleaseCohort(&release, observed); err != nil {
		return nil, fmt.Errorf("cohort differs from exact release: %w", err)
	}
	composeIDs = slices.Clone(composeIDs)
	observedIDs := make([]string, 0, len(observed))
	for _, container := range observed {
		observedIDs = append(observedIDs, container.ContainerID)
	}
	slices.Sort(composeIDs)
	slices.Sort(observedIDs)
	if !slices.Equal(composeIDs, observedIDs) {
		return nil, errors.New("compose and strict container inventories disagree")
	}
	return observed, nil
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
	if err := b.requireMutationAdmission(ctx, "update"); err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	// See routeReplaceRestart: the release Append and the actor's authoritative
	// Updating transition are one admission critical section. A losing caller
	// observes Updating before it can append a second, unowned release row.
	unlockCommand := b.commandFence.Lock(req.LeaseUUID)
	defer unlockCommand()
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
	// No pre-replace status snapshot: status/callback-pair writes and gauge
	// bookkeeping are the actor's, keyed on the actor-observed
	// replaceWasActive (onEnterUpdating). The update preflight is
	// unconditionally Failed regardless, so the worker needs no status hint.
	b.provisionsMu.Unlock()
	if len(resourceProfiles) == 0 {
		var profileErr error
		resourceProfiles, profileErr = b.activeResourceProfiles(req.LeaseUUID, items)
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
	active, sourceClaim, activeErr := b.releaseStore.ClaimLatestActive(req.LeaseUUID)
	if activeErr != nil {
		return fmt.Errorf("claim active release lineage: %w", activeErr)
	}
	source, sourceErr := newReplaceSourceSnapshot(active)
	if sourceErr != nil {
		return fmt.Errorf("construct update source authority: %w", sourceErr)
	}
	runtimeAuthority, legacyRuntimeAuthority, authorityErr := releaseRuntimeAuthoritiesForMaintenance(
		active, tenant, providerUUID, callbackURL, lifecycleCallbackURL,
	)
	if authorityErr != nil {
		return fmt.Errorf("construct update release runtime authority: %w", authorityErr)
	}
	maintenance, targetRelease, admitErr := b.admitMaintenance(
		shared.MaintenanceIntentUpdate,
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

	// Hand off to the actor. The actor's onEnterUpdating writes Status=Updating
	// and the prevalidated same-authority callback pair BEFORE acking. See
	// handleUpdateRequested / spawnReplaceWorker.
	opCtx, opCancel := b.shutdownAwareContext()
	work := func() leasesm.ReplaceResult {
		return b.doUpdate(
			opCtx,
			req.LeaseUUID,
			stackManifest,
			resourceProfiles,
			items,
			callbackURL,
			lifecycleCallbackURL,
			maintenance,
			targetRelease,
			source,
			logger,
		)
	}
	ack := make(chan error, 1)
	if routeErr := b.routeToLeaseBlocking(ctx, req.LeaseUUID, leasesm.UpdateRequestedMsg{
		Cancel:               opCancel,
		Work:                 work,
		Ack:                  ack,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Maintenance:          maintenance,
	}); routeErr != nil {
		opCancel()
		return b.failUnacceptedMaintenance(maintenance, targetRelease, routeErr)
	}
	// See routeReplaceRestart: after enqueue, commandFence stays closed until
	// the actor has definitively accepted or rejected this exact release.
	if err := <-ack; err != nil {
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
func (b *Backend) doUpdate(ctx context.Context, leaseUUID string, stack *manifest.StackManifest, resourceProfiles []shared.SKUResourceSnapshot, items []backend.LeaseItem, callbackURL, lifecycleCallbackURL string, maintenance shared.MaintenanceIntentClaim, targetRelease shared.MaintenanceReleaseClaim, source replaceSourceSnapshot, logger *slog.Logger) leasesm.ReplaceResult {
	// Pull each unique image (deduplicated).
	pulledImages := make(map[string]bool)
	for svcName, svc := range stack.Services {
		if pulledImages[svc.Image] {
			continue
		}
		logger.Info("pulling image for update", "service", svcName, "image", svc.Image)
		if pullErr := b.mutationAdapter().pullImage(ctx, svc.Image, b.cfg.ImagePullTimeout); pullErr != nil {
			err := fmt.Errorf("image pull failed for service %s: %w", svcName, pullErr)
			preserve := false
			if relErr := b.releaseStore.FailMaintenance(
				targetRelease, backend.ReasonImagePullFailed, backend.MsgImagePullFailed,
			); relErr != nil {
				logger.Warn("failed to settle exact update release after image pull failure", "error", relErr)
				preserve = true
			}
			// Force Status=Failed unconditionally (Restored:false) since the
			// user's desired state (the new image set) was not achieved.
			return leasesm.ReplaceResult{
				CallbackErr: backend.MsgImagePullFailed,
				Err:         err,
				Restored:    false,
				Failure: leasesm.ReplaceFailureInfo{
					Operation:           "update",
					Reason:              backend.ReasonImagePullFailed,
					CallbackErr:         backend.MsgImagePullFailed,
					LastError:           err.Error(),
					PreserveMaintenance: preserve,
				},
			}
		}
		pulledImages[svc.Image] = true
	}

	return b.doReplaceContainers(ctx, replaceContainersOp{
		LeaseUUID:            leaseUUID,
		Stack:                stack,
		Items:                items,
		ResourceProfiles:     resourceProfiles,
		Operation:            "update",
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Maintenance:          maintenance,
		TargetRelease:        targetRelease,
		Source:               source,
		TargetMaintenanceID:  maintenance.MaintenanceID(),
		Logger:               logger,
		OnSuccess: func(prov *leasesm.ProvisionState) {
			prov.StackManifest = stack
		},
	})
}
