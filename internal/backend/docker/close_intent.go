package docker

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// acquireCloseIntent returns the immutable cleanup authority already owned by
// this lease, or publishes it before the first destructive side effect. The
// callback store transaction also preempts any provision/restore intent, so a
// close can never erase the operation completion it won against.
func (b *Backend) acquireCloseIntent(
	ctx context.Context,
	leaseUUID string,
	projectionExists bool,
	tenant string,
	providerUUID string,
	items []backend.LeaseItem,
	stack *manifest.StackManifest,
	callbackURL string,
	lifecycleCallbackURL string,
) (shared.CloseIntentClaim, bool, error) {
	if b.callbackStore == nil {
		return shared.CloseIntentClaim{}, false, nil
	}
	b.closeSnapshotMu.RLock()
	defer b.closeSnapshotMu.RUnlock()
	if existing, found, err := b.callbackStore.GetCloseIntent(leaseUUID); err != nil {
		return shared.CloseIntentClaim{}, false, fmt.Errorf("read durable close intent: %w", err)
	} else if found {
		if existing.Backend() != b.Name() || existing.BackendStorageID() != b.storageIdentity {
			return shared.CloseIntentClaim{}, false, fmt.Errorf(
				"durable close intent belongs to backend %q storage %s",
				existing.Backend(), existing.BackendStorageID(),
			)
		}
		return existing, true, nil
	}

	releases, err := b.releaseHistoryForClose(leaseUUID)
	if err != nil {
		return shared.CloseIntentClaim{}, false, err
	}
	fencedRelease := latestActiveRelease(releases)
	if fencedRelease == nil && len(releases) > 0 {
		fencedRelease = &releases[len(releases)-1]
	}

	cleanupOnly := !projectionExists
	if cleanupOnly && fencedRelease == nil {
		// There is no projection, release, or substrate authority to clean. The
		// operation-intent preemption below is still needed if an accepted worker
		// was canceled before publishing a provision, so retain the old atomic
		// settlement path for this truly effect-free case.
		if _, err := b.callbackStore.FailOperationIntentIfPresent(
			leaseUUID, "operation preempted by deprovision",
		); err != nil {
			return shared.CloseIntentClaim{}, false, fmt.Errorf("persist preempted operation completion: %w", err)
		}
		return shared.CloseIntentClaim{}, false, nil
	}

	manifestBytes, err := closeManifestBytes(stack, fencedRelease)
	if err != nil {
		return shared.CloseIntentClaim{}, false, err
	}
	// The fenced Release and the topology retained for a future restore must be
	// one authority. A maintenance worker commits its target Release before its
	// terminal actor message is consumed, so the in-memory projection can still
	// carry the source manifest/items while Deprovision is already allowed to
	// proceed. Prefer every complete Release field over that stale projection;
	// otherwise close could fence/delete the target generation while retaining
	// the source generation for restore.
	closeItems := slices.Clone(items)
	if fencedRelease != nil && len(fencedRelease.Items) > 0 {
		closeItems = slices.Clone(fencedRelease.Items)
	}
	if !cleanupOnly {
		if authority, ok := runtimeIdentityForRelease(fencedRelease); ok {
			tenant = authority.Tenant()
			providerUUID = authority.ProviderUUID()
			callbackURL = authority.CallbackURL()
			lifecycleCallbackURL = authority.LifecycleCallbackURL()
		}
	}
	if !cleanupOnly && callbackURL != "" {
		resolved, resolveErr := backend.ResolveLifecycleCallbackURL(callbackURL, lifecycleCallbackURL)
		if resolveErr != nil {
			return shared.CloseIntentClaim{}, false, fmt.Errorf(
				"resolve lifecycle callback before close admission: %w", resolveErr,
			)
		}
		lifecycleCallbackURL = resolved
	}

	rollbackTargets, err := b.closeLegacyRollbackTargets(
		ctx,
		leaseUUID,
		releases,
		tenant,
		providerUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	if err != nil {
		return shared.CloseIntentClaim{}, false, err
	}
	if cleanupOnly {
		tenant = ""
		providerUUID = ""
		callbackURL = ""
		lifecycleCallbackURL = ""
	}
	version, digest, err := closeReleaseFence(fencedRelease)
	if err != nil {
		return shared.CloseIntentClaim{}, false, err
	}
	var resourceProfiles []shared.SKUResourceSnapshot
	if fencedRelease != nil && len(fencedRelease.ResourceProfiles) > 0 {
		if !slices.Equal(fencedRelease.Items, closeItems) {
			return shared.CloseIntentClaim{}, false, fmt.Errorf(
				"fenced release resource authority differs from close topology",
			)
		}
		resourceProfiles = shared.CloneSKUResourceSnapshot(fencedRelease.ResourceProfiles)
	}
	if len(resourceProfiles) == 0 && projectionExists {
		b.provisionsMu.RLock()
		if live := b.provisions[leaseUUID]; live != nil && slices.Equal(live.Items, closeItems) {
			resourceProfiles = shared.CloneSKUResourceSnapshot(live.ResourceProfiles)
		}
		b.provisionsMu.RUnlock()
	}
	if len(resourceProfiles) == 0 {
		// v0.13 releases had no immutable profile field. Resolve that compatibility
		// row exactly once at close admission; every new provision/restore and every
		// recovered live projection carries a pinned snapshot.
		resourceProfiles, err = b.resolveResourceProfiles(closeItems)
	}
	if err == nil {
		err = validateDockerResourceProfiles(closeItems, resourceProfiles)
	}
	if err != nil {
		return shared.CloseIntentClaim{}, false, fmt.Errorf(
			"snapshot close resource authority before mutation: %w", err,
		)
	}
	admission, err := b.callbackStore.BeginCloseIntent(shared.CloseIntentSpec{
		LeaseUUID:             leaseUUID,
		Backend:               b.Name(),
		BackendStorageID:      b.storageIdentity,
		Tenant:                tenant,
		ProviderUUID:          providerUUID,
		Items:                 closeItems,
		ResourceProfiles:      resourceProfiles,
		Manifest:              manifestBytes,
		CallbackURL:           callbackURL,
		LifecycleCallbackURL:  lifecycleCallbackURL,
		RetainOnClose:         !cleanupOnly && b.cfg.RetainOnClose,
		CleanupOnly:           cleanupOnly,
		ActiveReleaseVersion:  version,
		ActiveReleaseDigest:   digest,
		LegacyRollbackTargets: rollbackTargets,
	})
	if err != nil {
		return shared.CloseIntentClaim{}, false, fmt.Errorf("publish durable close intent: %w", err)
	}
	return admission.Claim, true, nil
}

// settleCommittedOperationBeforeClose preserves causal callback ordering at the
// actor drain boundary. A worker writes its exact active Release before queuing
// terminal Success; if that terminal callback write failed (or Deprovision was
// already queued behind it), BeginCloseIntent must not classify the committed
// operation as preempted Failed. A genuinely uncommitted intent is deliberately
// left for BeginCloseIntent's atomic failure-preemption transaction.
func (b *Backend) settleCommittedOperationBeforeClose(leaseUUID string) error {
	if b.callbackStore == nil || b.releaseStore == nil {
		return nil
	}
	pending, err := b.pendingOperationIntentForLease(leaseUUID)
	if err != nil {
		return fmt.Errorf("list operation intents before close admission: %w", err)
	}
	if pending == nil {
		return nil
	}
	if pending.Backend() != b.Name() || pending.BackendStorageID() != b.storageIdentity {
		return fmt.Errorf(
			"operation intent before close belongs to backend %q storage %s",
			pending.Backend(), pending.BackendStorageID(),
		)
	}
	committed, err := b.operationIntentHasCommittedRelease(*pending)
	if err != nil {
		return fmt.Errorf("validate committed operation before close: %w", err)
	}
	if !committed {
		return nil
	}
	if _, err := b.callbackStore.ResolveOperationIntent(
		*pending, backend.CallbackStatusSuccess, "",
	); err != nil {
		return fmt.Errorf("settle committed operation before close admission: %w", err)
	}
	return nil
}

// settleCommittedOperationBeforeMaintenance closes the operation-callback
// write-ahead window before a restart, update, or autonomous custom-domain
// replacement can append a new release in the same provision/restore lineage.
// A committed operation is settled Success first, preserving FIFO causality.
// An uncommitted operation remains the sole mutation authority and blocks
// maintenance; changing its manifest or callback route would otherwise turn
// the same operation token into divergent durable evidence on recovery.
//
// Callers hold commandFence for leaseUUID, which excludes every accepted
// provision/restore command and every other maintenance prelude. CallbackStore
// still verifies the opaque claim in its own transaction, so a concurrent
// terminal actor settlement can only make this call fail closed and retryable.
func (b *Backend) settleCommittedOperationBeforeMaintenance(leaseUUID string) error {
	if b.callbackStore == nil || b.releaseStore == nil {
		return nil
	}
	pending, err := b.pendingOperationIntentForLease(leaseUUID)
	if err != nil {
		return fmt.Errorf("list operation intents before maintenance admission: %w", err)
	}
	if pending == nil {
		return nil
	}
	if pending.Backend() != b.Name() || pending.BackendStorageID() != b.storageIdentity {
		return fmt.Errorf(
			"operation intent before maintenance belongs to backend %q storage %s",
			pending.Backend(), pending.BackendStorageID(),
		)
	}
	committed, err := b.operationIntentHasCommittedRelease(*pending)
	if err != nil {
		return fmt.Errorf("validate committed operation before maintenance: %w", err)
	}
	if !committed {
		return fmt.Errorf(
			"%w: lease %q has an unresolved provision or restore operation",
			backend.ErrInvalidState, leaseUUID,
		)
	}
	if _, err := b.callbackStore.ResolveOperationIntent(
		*pending, backend.CallbackStatusSuccess, "",
	); err != nil {
		return fmt.Errorf("settle committed operation before maintenance admission: %w", err)
	}
	return nil
}

func (b *Backend) pendingOperationIntentForLease(
	leaseUUID string,
) (*shared.OperationIntentClaim, error) {
	claims, err := b.callbackStore.ListOperationIntents()
	if err != nil {
		return nil, err
	}
	var pending *shared.OperationIntentClaim
	for i := range claims {
		if claims[i].LeaseUUID() != leaseUUID {
			continue
		}
		if pending != nil {
			return nil, fmt.Errorf("multiple operation intents exist for lease %q", leaseUUID)
		}
		claim := claims[i]
		pending = &claim
	}
	return pending, nil
}

func (b *Backend) releaseHistoryForClose(leaseUUID string) ([]shared.Release, error) {
	if b.releaseStore == nil {
		return nil, nil
	}
	releases, err := b.releaseStore.List(leaseUUID)
	if err != nil {
		return nil, fmt.Errorf("read release history before close admission: %w", err)
	}
	return releases, nil
}

func latestActiveRelease(releases []shared.Release) *shared.Release {
	for index := len(releases) - 1; index >= 0; index-- {
		if releases[index].Status == "active" {
			return &releases[index]
		}
	}
	return nil
}

func closeManifestBytes(stack *manifest.StackManifest, release *shared.Release) ([]byte, error) {
	if release != nil && len(release.Manifest) > 0 {
		return bytes.Clone(release.Manifest), nil
	}
	if stack != nil {
		data, err := json.Marshal(stack)
		if err != nil {
			return nil, fmt.Errorf("marshal manifest for close intent: %w", err)
		}
		return data, nil
	}
	return nil, fmt.Errorf("close intent requires a durable manifest")
}

func closeReleaseFence(release *shared.Release) (int, [sha256.Size]byte, error) {
	if release == nil {
		return 0, [sha256.Size]byte{}, nil
	}
	encoded, err := json.Marshal(release)
	if err != nil {
		return 0, [sha256.Size]byte{}, fmt.Errorf("marshal active release fence: %w", err)
	}
	return release.Version, sha256.Sum256(encoded), nil
}

// closeLegacyRollbackTargets turns exact release topology plus current Docker
// evidence into immutable IDs before admission. A durable migration row is the
// strongest topology source. The compatibility fallback deliberately does not
// mint migration provenance: when an already-Items-bearing selected active
// release has an exact app topology, it only lets Close consume same-lease
// rollback names whose SKU/domain/index fit that bounded topology. A delayed
// close therefore never resolves a reused name to a replacement container, and
// an ordinary surviving `-prev` cannot rewrite release history.
func (b *Backend) closeLegacyRollbackTargets(
	ctx context.Context,
	leaseUUID string,
	releases []shared.Release,
	expectedTenant string,
	expectedProviderUUID string,
	expectedCallbackURL string,
	expectedLifecycleCallbackURL string,
) ([]shared.CloseLegacyRollbackTarget, error) {
	if (expectedTenant == "") != (expectedProviderUUID == "") {
		return nil, errors.New("legacy rollback tenant/provider expectation must be wholly absent or wholly present")
	}
	if expectedProviderUUID != "" && !backend.IsCanonicalLeaseUUID(expectedProviderUUID) {
		return nil, errors.New("legacy rollback expected provider UUID is not canonical")
	}
	if (expectedCallbackURL == "") != (expectedLifecycleCallbackURL == "") {
		return nil, errors.New("legacy rollback callback expectation must be wholly absent or wholly present")
	}
	var migrationItems []backend.LeaseItem
	var topologyRelease *shared.Release
	for index := range releases {
		release := &releases[index]
		if !release.LegacyMigration {
			continue
		}
		if migrationItems != nil && !slices.Equal(migrationItems, release.Items) {
			return nil, fmt.Errorf("durable legacy migration has divergent rollback topologies")
		}
		if topologyRelease != nil && !bytes.Equal(topologyRelease.Manifest, release.Manifest) {
			return nil, fmt.Errorf("durable legacy migration has divergent rollback manifests")
		}
		migrationItems = release.Items
		topologyRelease = release
	}
	containers, err := b.listManagedContainersForRecovery(ctx)
	if err != nil {
		return nil, fmt.Errorf("list rollback containers before close admission: %w", err)
	}
	rollbackContainers := make([]ContainerInfo, 0)
	for _, container := range containers {
		if container.LeaseUUID != leaseUUID || !isLegacyContainer(container) {
			continue
		}
		if !isLegacyRollbackRemnant(container) {
			return nil, fmt.Errorf(
				"legacy container %q is not an exact rollback remnant",
				container.Name,
			)
		}
		rollbackContainers = append(rollbackContainers, container)
	}
	if len(rollbackContainers) == 0 {
		return nil, nil
	}
	unmarkedCompatibility := migrationItems == nil
	if migrationItems == nil {
		selected := latestActiveRelease(releases)
		if selected == nil || len(selected.Items) == 0 {
			return nil, fmt.Errorf(
				"exact rollback remnants exist without selected active release item authority",
			)
		}
		migrationItems = selected.Items
		topologyRelease = selected
		if expectedTenant == "" || expectedProviderUUID == "" || expectedCallbackURL == "" {
			return nil, errors.New(
				"unmarked rollback remnants require complete live close identity authority",
			)
		}
	}
	if len(migrationItems) != 1 || migrationItems[0].ServiceName != manifest.DefaultServiceName {
		return nil, fmt.Errorf("legacy rollback remnants require one exact app release item")
	}
	if topologyRelease == nil {
		return nil, errors.New("legacy rollback remnants have no release manifest authority")
	}
	// A migration row predating typed runtime authority cannot identify a
	// partially-cleaned rollback cohort by itself, so every surviving remnant
	// must at least agree on one non-empty tenant/canonical-provider identity.
	// If a later active typed release exists, its non-expiring authority is the
	// stronger fence and the rollback cohort must match it too. This validation is
	// unconditional: cleanup-only close and restart cleanup intentionally have no
	// volatile projection from which to supply expected identities.
	observedTenant := rollbackContainers[0].Tenant
	observedProviderUUID := rollbackContainers[0].ProviderUUID
	if observedTenant == "" {
		return nil, errors.New("legacy rollback cohort has empty tenant identity")
	}
	if !backend.IsCanonicalLeaseUUID(observedProviderUUID) {
		return nil, errors.New("legacy rollback cohort provider UUID is not canonical")
	}
	for _, container := range rollbackContainers[1:] {
		if container.Tenant != observedTenant || container.ProviderUUID != observedProviderUUID {
			return nil, errors.New("legacy rollback cohort has divergent tenant/provider identity")
		}
	}
	if expectedTenant != "" &&
		(observedTenant != expectedTenant || observedProviderUUID != expectedProviderUUID) {
		return nil, errors.New("legacy rollback identity differs from the closing lease")
	}
	if selected := latestActiveRelease(releases); selected != nil {
		if authority, ok := runtimeIdentityForRelease(selected); ok &&
			(observedTenant != authority.Tenant() || observedProviderUUID != authority.ProviderUUID()) {
			return nil, errors.New("legacy rollback identity differs from active release authority")
		}
	}
	stack, err := manifest.ParsePayload(topologyRelease.Manifest)
	if err != nil {
		return nil, fmt.Errorf("parse legacy rollback release manifest: %w", err)
	}
	app := stack.Services[manifest.DefaultServiceName]
	if len(stack.Services) != 1 || app == nil {
		return nil, errors.New("legacy rollback release manifest is not one exact app service")
	}
	quantity, err := backend.ValidateOperationQuantities(migrationItems)
	if err != nil {
		return nil, fmt.Errorf("validate legacy rollback release topology: %w", err)
	}
	if _, err := sortedImmutableContainerIDs(rollbackContainers); err != nil {
		return nil, fmt.Errorf("validate immutable legacy rollback IDs: %w", err)
	}
	observedCallbackURL, observedLifecycleCallbackURL, err := resolveLegacyContainerCallbackURLs(
		rollbackContainers,
	)
	if err != nil {
		return nil, fmt.Errorf("validate legacy rollback callback cohort: %w", err)
	}
	// An unmarked fallback derives all authority from the selected live
	// release, so its callback pair must be that exact close generation. A
	// durable migration marker may legitimately predate a later restart/update
	// callback rotation; its rollback cohort still has to be internally exact,
	// but must not be relabeled as the newer callback generation.
	if unmarkedCompatibility &&
		(observedCallbackURL != expectedCallbackURL ||
			observedLifecycleCallbackURL != expectedLifecycleCallbackURL) {
		return nil, errors.New("legacy rollback callback identity differs from the closing lease")
	}
	targets := make([]shared.CloseLegacyRollbackTarget, 0, len(rollbackContainers))
	seenIndexes := make(map[int]string, len(rollbackContainers))
	item := migrationItems[0]
	for _, container := range rollbackContainers {
		if !isDockerNonRunningStatus(container.Status) ||
			container.BackendName != b.Name() ||
			container.InstanceIndex < 0 || container.InstanceIndex >= quantity ||
			container.SKU != item.SKU || container.CustomDomain != item.CustomDomain ||
			container.Image != app.Image ||
			(expectedTenant != "" && container.Tenant != expectedTenant) ||
			(expectedProviderUUID != "" && container.ProviderUUID != expectedProviderUUID) {
			return nil, fmt.Errorf(
				"legacy rollback container %q is outside selected release topology",
				container.Name,
			)
		}
		if prior, duplicate := seenIndexes[container.InstanceIndex]; duplicate {
			return nil, fmt.Errorf(
				"legacy rollback containers %q and %q duplicate instance index %d",
				prior,
				container.Name,
				container.InstanceIndex,
			)
		}
		seenIndexes[container.InstanceIndex] = container.Name
		targets = append(targets, shared.CloseLegacyRollbackTarget{
			ContainerID: container.ContainerID,
			Name:        container.Name,
		})
	}
	slices.SortFunc(targets, func(left, right shared.CloseLegacyRollbackTarget) int {
		return cmp.Or(
			bytes.Compare([]byte(left.Name), []byte(right.Name)),
			bytes.Compare([]byte(left.ContainerID), []byte(right.ContainerID)),
		)
	})
	return targets, nil
}

func (b *Backend) removeCloseRollbackTargets(
	ctx context.Context,
	claim shared.CloseIntentClaim,
	logger *slog.Logger,
) error {
	var errs []error
	for _, target := range claim.LegacyRollbackTargets() {
		if err := b.mutationAdapter().removeContainer(ctx, target.ContainerID); err != nil {
			logger.Warn("failed to remove close-owned legacy rollback container",
				"name", target.Name, "container_id", target.ContainerID, "error", err)
			errs = append(errs, fmt.Errorf("remove %s (%s): %w", target.Name, target.ContainerID, err))
		}
	}
	return errors.Join(errs...)
}

// cleanupCloseWithoutProjection consumes a cleanup-only claim. Absence from
// the volatile provision map is not proof that Docker or the volume filesystem
// is empty: an event can race the next inventory refresh, and a crashed
// provision can leave volumes after its final container disappears. The
// durable release snapshot supplies the bounded topology, while Compose and
// the ownership table supply current substrate truth.
//
// Cleanup-only claims never retain data and never give up after an arbitrary
// retry count. They have no tenant projection with which to author a safe
// reaping tombstone, so the journal remains the non-expiring retry owner until
// cleanup is definitive.
func (b *Backend) cleanupCloseWithoutProjection(
	ctx context.Context,
	claim shared.CloseIntentClaim,
	logger *slog.Logger,
) (shared.CloseIntentClaim, error) {
	stopTimeout := cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second)
	if _, err := b.teardownLeaseContainers(
		ctx,
		claim.LeaseUUID(),
		nil,
		stopTimeout,
		teardownOpDeprovision,
		logger,
	); err != nil {
		return b.recordCleanupOnlyFailure(claim, fmt.Errorf("tear down unprojected lease containers: %w", err))
	}
	if err := b.removeCloseRollbackTargets(ctx, claim, logger); err != nil {
		return b.recordCleanupOnlyFailure(claim, fmt.Errorf("remove unprojected legacy rollback containers: %w", err))
	}

	names := make([]string, 0)
	for _, item := range claim.Items() {
		for index := range item.Quantity {
			names = append(names, canonicalVolumeName(claim.LeaseUUID(), item.ServiceName, index))
		}
	}
	report := b.volumeOp(claim.LeaseUUID(), logger).destroy(
		ctx,
		destroySiteDeprovisionDestroy,
		names...,
	)
	if err := report.err(); err != nil {
		return b.recordCleanupOnlyFailure(claim, fmt.Errorf("destroy unprojected lease volumes: %w", err))
	}
	return claim, nil
}

func (b *Backend) recordCleanupOnlyFailure(
	claim shared.CloseIntentClaim,
	cause error,
) (shared.CloseIntentClaim, error) {
	refreshed, err := b.callbackStore.IncrementCloseCleanupAttempts(claim)
	if err != nil {
		return shared.CloseIntentClaim{}, fmt.Errorf(
			"persist cleanup-only close attempt after %w: %w",
			cause,
			err,
		)
	}
	return refreshed, cause
}

func (b *Backend) resolveCloseIntent(
	claim shared.CloseIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
	retained bool,
	prepare func() error,
	finalize func(),
) error {
	if b.callbackStore == nil || claim.IntentID() == "" {
		if prepare != nil {
			if err := prepare(); err != nil {
				return err
			}
		}
		if finalize != nil {
			finalize()
		}
		return nil
	}
	if len(errMsg) > callbackMaxErrorLen {
		errMsg = errMsg[:callbackMaxErrorLen-3] + "..."
	}
	if err := func() error {
		// Settlement is one recovery-visible state change: a writer may see the
		// close before this block or the finalized projection/accounting after it,
		// never a resolved journal paired with stale live state.
		b.closeSnapshotMu.RLock()
		defer b.closeSnapshotMu.RUnlock()
		if prepare != nil {
			if err := prepare(); err != nil {
				return err
			}
		}
		if _, err := b.callbackStore.ResolveCloseIntent(claim, status, errMsg, retained); err != nil {
			return fmt.Errorf("resolve durable close intent: %w", err)
		}
		if finalize != nil {
			finalize()
		}
		return nil
	}(); err != nil {
		return err
	}
	// The transaction already owns enqueue. Wake the tracked outbox worker
	// without performing callback network I/O inside the lease actor (or startup
	// recovery); failure to observe this coalescing signal is harmless because
	// the periodic replay loop reads the durable row.
	if b.callbackSender != nil {
		b.callbackSender.NotifyPendingCallbacks()
	}
	return nil
}
