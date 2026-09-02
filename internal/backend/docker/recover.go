package docker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type recoveredCallbackPair struct {
	callbackURL          string
	lifecycleCallbackURL string
	containerID          string
}

type recoveredInstanceKey struct {
	service string
	sku     string
	index   int
}

func runtimeIdentityForRelease(release *shared.Release) (shared.ReleaseRuntimeIdentity, bool) {
	if release == nil {
		return shared.ReleaseRuntimeIdentity{}, false
	}
	return release.RuntimeIdentity()
}

func containerMatchesReleaseRuntimeIdentity(
	container ContainerInfo,
	authority shared.ReleaseRuntimeIdentity,
) bool {
	lifecycleCallbackURL := container.LifecycleCallbackURL
	if authority.Class() == shared.ReleaseAuthorityLegacy {
		resolved, err := backend.ResolveLifecycleCallbackURL(
			container.CallbackURL, container.LifecycleCallbackURL,
		)
		if err != nil {
			return false
		}
		lifecycleCallbackURL = resolved
	}
	return container.Tenant == authority.Tenant() &&
		container.ProviderUUID == authority.ProviderUUID() &&
		container.CallbackURL == authority.CallbackURL() &&
		lifecycleCallbackURL == authority.LifecycleCallbackURL()
}

// validateRecoveredReleaseCohort proves that the container snapshot contains
// exactly the immutable instance set recorded by the active release. A count
// check alone is insufficient: a duplicate service/index could otherwise hide
// a missing sibling. Releases written by older binaries have no Items and are
// deliberately outside this invariant until their next successful mutation.
func validateRecoveredReleaseCohort(release *shared.Release, containers []ContainerInfo) error {
	if release == nil || len(release.Items) == 0 {
		return nil
	}
	expectedCount, err := backend.ValidateOperationQuantities(release.Items)
	if err != nil {
		return fmt.Errorf("validate durable release quantities: %w", err)
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return fmt.Errorf("parse durable release manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, release.Items); err != nil {
		return fmt.Errorf("validate durable release topology: %w", err)
	}

	expected := make(map[recoveredInstanceKey]string, expectedCount)
	for _, item := range release.Items {
		for index := range item.Quantity {
			key := recoveredInstanceKey{service: item.ServiceName, sku: item.SKU, index: index}
			if _, duplicate := expected[key]; duplicate {
				return fmt.Errorf("durable release contains duplicate instance %+v", key)
			}
			expected[key] = item.CustomDomain
		}
	}
	if len(containers) != len(expected) {
		return fmt.Errorf("found %d containers, expected %d", len(containers), len(expected))
	}

	seen := make(map[recoveredInstanceKey]struct{}, len(containers))
	identity := containers[0]
	if identity.LeaseUUID == "" || strings.TrimSpace(identity.Tenant) == "" ||
		strings.TrimSpace(identity.ProviderUUID) == "" {
		return fmt.Errorf("container %q has incomplete lease, tenant, or provider identity", identity.ContainerID)
	}
	authority, hasRuntimeAuthority := runtimeIdentityForRelease(release)
	if hasRuntimeAuthority && !containerMatchesReleaseRuntimeIdentity(identity, authority) {
		return fmt.Errorf("container %q identity differs from durable runtime authority", identity.ContainerID)
	}
	for _, container := range containers {
		if container.LeaseUUID != identity.LeaseUUID ||
			container.Tenant != identity.Tenant ||
			container.ProviderUUID != identity.ProviderUUID {
			return fmt.Errorf("container %q has divergent lease, tenant, or provider identity", container.ContainerID)
		}
		if hasRuntimeAuthority && !containerMatchesReleaseRuntimeIdentity(container, authority) {
			return fmt.Errorf("container %q identity differs from durable runtime authority", container.ContainerID)
		}
		if container.MaintenanceID != release.MaintenanceID {
			return fmt.Errorf(
				"container %q maintenance generation %q differs from durable release %q",
				container.ContainerID, container.MaintenanceID, release.MaintenanceID,
			)
		}
		key := recoveredInstanceKey{
			service: container.ServiceName,
			sku:     container.SKU,
			index:   container.InstanceIndex,
		}
		domain, exists := expected[key]
		if !exists {
			return fmt.Errorf("container %q is not in the durable instance set", container.ContainerID)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate container for durable instance %+v", key)
		}
		seen[key] = struct{}{}
		if container.CustomDomain != domain {
			return fmt.Errorf("container %q custom domain differs from the durable release", container.ContainerID)
		}
		service, exists := stack.Services[container.ServiceName]
		if !exists || service == nil || container.Image != service.Image {
			return fmt.Errorf("container %q image differs from the durable release", container.ContainerID)
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("container cohort is not the exact durable instance set")
	}
	return nil
}

// recoveredReleaseAllocations reconstructs the complete conservative resource
// reservation from the durable desired topology. Container inventory is only
// evidence of what survived; using it as the accounting source for a diverged
// cohort would make a missing stateful sibling's disk appear free. The caller
// supplies the identity-bearing tenant observed for the recovered generation.
func (b *Backend) recoveredReleaseAllocations(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) ([]shared.ResourceAllocation, error) {
	if strings.TrimSpace(tenant) == "" {
		return nil, errors.New("durable release allocation requires an observed tenant")
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return nil, fmt.Errorf("validate durable release quantities: %w", err)
	}
	return recoveredSnapshotAllocations(leaseUUID, tenant, items, resourceProfiles)
}

// recoveredSnapshotAllocations reconstructs an unresolved close from the
// immutable sizing authority committed before teardown. It deliberately never
// consults the current SKU configuration: operators may resize or remove a
// profile while a non-expiring close finalizer is pending, but that cannot
// change how much already-admitted substrate remains reserved.
func recoveredSnapshotAllocations(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	resourceProfiles []shared.SKUResourceSnapshot,
) ([]shared.ResourceAllocation, error) {
	total, err := backend.ValidateOperationQuantities(items)
	if err != nil {
		return nil, fmt.Errorf("validate durable close quantities: %w", err)
	}
	if err := validateDockerResourceProfiles(items, resourceProfiles); err != nil {
		return nil, fmt.Errorf("validate durable close resource snapshot: %w", err)
	}
	allocations := make([]shared.ResourceAllocation, 0, total)
	for _, item := range items {
		resources, ok := shared.LookupSKUResourceSnapshotRow(resourceProfiles, item.SKU)
		if !ok {
			return nil, fmt.Errorf("durable close resource snapshot omits SKU %q", item.SKU)
		}
		effectiveDiskMB, diskErr := resources.EffectiveDiskMB()
		if diskErr != nil {
			return nil, fmt.Errorf("durable close resource snapshot for SKU %q: %w", item.SKU, diskErr)
		}
		for index := range item.Quantity {
			allocations = append(allocations, shared.ResourceAllocation{
				LeaseUUID: fmt.Sprintf("%s-%s-%d", leaseUUID, item.ServiceName, index),
				Tenant:    tenant,
				SKU:       item.SKU,
				CPUCores:  resources.CPUCores,
				MemoryMB:  resources.MemoryMB,
				DiskMB:    effectiveDiskMB,
			})
		}
	}
	return allocations, nil
}

// recoveredCallbackPairs validates callback labels as a complete lease cohort
// before recovery chooses any sibling as its in-memory representative. Docker
// list order is not an authority boundary: every sibling must carry the exact
// same operation/lifecycle generation, or the lease is quarantined by failing
// recovery deterministically. An all-empty legacy cohort remains callbackless;
// mixing an empty sibling with a routed sibling is still divergence.
func recoveredCallbackPairs(containers []ContainerInfo) (map[string]recoveredCallbackPair, error) {
	return recoveredCallbackPairsExcept(containers, nil)
}

func recoveredCallbackPairsExcept(
	containers []ContainerInfo,
	skippedLeases map[string]struct{},
) (map[string]recoveredCallbackPair, error) {
	grouped := make(map[string][]ContainerInfo)
	for _, container := range containers {
		if container.LeaseUUID == "" || container.SKU == "" || isLegacyRollbackRemnant(container) {
			continue
		}
		if _, skipped := skippedLeases[container.LeaseUUID]; skipped {
			continue
		}
		grouped[container.LeaseUUID] = append(grouped[container.LeaseUUID], container)
	}

	pairs := make(map[string]recoveredCallbackPair, len(grouped))
	for _, leaseUUID := range slices.Sorted(maps.Keys(grouped)) {
		cohort := grouped[leaseUUID]
		slices.SortFunc(cohort, func(left, right ContainerInfo) int {
			return cmp.Or(
				cmp.Compare(left.ContainerID, right.ContainerID),
				cmp.Compare(left.Name, right.Name),
				cmp.Compare(left.CallbackURL, right.CallbackURL),
				cmp.Compare(left.LifecycleCallbackURL, right.LifecycleCallbackURL),
			)
		})

		for index, container := range cohort {
			pair := recoveredCallbackPair{
				callbackURL:          container.CallbackURL,
				lifecycleCallbackURL: container.LifecycleCallbackURL,
				containerID:          container.ContainerID,
			}
			switch {
			case pair.callbackURL == "" && pair.lifecycleCallbackURL == "":
				// Callback labels predate the durable callback fields. Keep this
				// cohort callbackless rather than manufacturing authority.
			case pair.callbackURL == "":
				return nil, fmt.Errorf(
					"lease %q container %q has lifecycle callback authority without an operation callback URL",
					leaseUUID, container.ContainerID,
				)
			default:
				if err := backend.ValidateOperationCallbackURL(pair.callbackURL); err != nil {
					return nil, fmt.Errorf("lease %q container %q has invalid operation callback URL: %w",
						leaseUUID, container.ContainerID, err)
				}
				resolved, err := backend.ResolveLifecycleCallbackURL(
					pair.callbackURL, pair.lifecycleCallbackURL,
				)
				if err != nil {
					return nil, fmt.Errorf("lease %q container %q has invalid callback pair: %w",
						leaseUUID, container.ContainerID, err)
				}
				pair.lifecycleCallbackURL = resolved
			}

			if index == 0 {
				pairs[leaseUUID] = pair
				continue
			}
			expected := pairs[leaseUUID]
			if pair.callbackURL != expected.callbackURL ||
				pair.lifecycleCallbackURL != expected.lifecycleCallbackURL {
				return nil, fmt.Errorf(
					"lease %q callback labels diverge between containers %q and %q",
					leaseUUID, expected.containerID, pair.containerID,
				)
			}
		}
	}
	return pairs, nil
}

// resumeRecoveredClose preserves the lease-actor serialization contract.
// Routing the typed message lets an existing actor drain any mutation worker,
// invokes the substrate finalizer even for an intentionally absent cleanup-only
// projection, and retires the actor after doDeprovision completes.
func (b *Backend) resumeRecoveredClose(
	ctx context.Context,
	claim shared.CloseIntentClaim,
) error {
	// The snapshot claim may have settled after recovery published its map. Fence
	// the lease, then re-read durable authority before routing: otherwise a new
	// provision can win after that settlement and this stale worker can tear down
	// the replacement generation.
	unlockCommand := b.commandFence.Lock(claim.LeaseUUID())
	defer unlockCommand()
	if b.callbackStore == nil {
		return errors.New("resume recovered close requires a callback store")
	}
	current, found, err := b.callbackStore.GetCloseIntent(claim.LeaseUUID())
	if err != nil {
		return fmt.Errorf("re-read recovered close intent: %w", err)
	}
	if !found {
		return nil
	}
	if current.Backend() != b.Name() || current.BackendStorageID() != b.storageIdentity {
		return fmt.Errorf(
			"recovered close intent for lease %q belongs to backend %q storage %s, not backend %q storage %s",
			current.LeaseUUID(), current.Backend(), current.BackendStorageID(), b.Name(), b.storageIdentity,
		)
	}
	reply := make(chan error, 1)
	if err := b.routeToLeaseBlocking(ctx, current.LeaseUUID(), leasesm.DeprovisionMsg{
		Ctx: ctx, Reply: reply,
	}); err != nil {
		return fmt.Errorf("route recovered close through lease actor: %w", err)
	}
	if err := b.waitForReply(ctx, reply); err != nil {
		return fmt.Errorf("apply recovered close through lease actor: %w", err)
	}
	return nil
}

// recoverState rebuilds in-memory state from Docker containers.
// Handles multi-unit leases by grouping containers by lease UUID.
// Merges with existing state to preserve in-flight provisions.
//
// Serialized by recoverMu to prevent concurrent calls (from the
// reconcile loop and RefreshState) from duplicating transition
// detection and failure callbacks.
func (b *Backend) recoverState(ctx context.Context) error {
	identityCtx, cancelIdentity := b.recoveryDockerReadContext(ctx)
	err := b.requireStorageIdentity(identityCtx)
	cancelIdentity()
	if err != nil {
		return fmt.Errorf("backend storage identity verification failed: %w", err)
	}
	b.recoverMu.Lock()
	defer b.recoverMu.Unlock()
	// Resolve restart/update WALs before ordinary recovery interprets the
	// source/target snapshot. A live actor generation is skipped under the same
	// per-lease command fence and remains protected below as an in-flight lease.
	if err := b.recoverMaintenanceIntents(ctx); err != nil {
		return err
	}
	b.closeSnapshotMu.Lock()
	closeSnapshotLocked := true
	defer func() {
		if closeSnapshotLocked {
			b.closeSnapshotMu.Unlock()
		}
	}()

	containers, err := b.listManagedContainersForRecovery(ctx)
	if err != nil {
		return err
	}

	// A durable close intent is the sole recovery authority once teardown has
	// been admitted. Load it before any callback-label or exact-release cohort
	// validation: those sources are expected to become incomplete as close
	// progresses, and treating that intentional destruction as ordinary
	// provision corruption would wedge startup at precisely the crash boundary
	// the journal exists to bridge. closeSnapshotMu excludes only close admission
	// and terminal settlement while inventory, journal, provisions, and pool are
	// published as one snapshot; slow destructive cleanup holds neither side.
	closeIntents := make(map[string]shared.CloseIntentClaim)
	if b.callbackStore != nil {
		claims, listErr := b.callbackStore.ListCloseIntents()
		if listErr != nil {
			return fmt.Errorf("list close intents before state recovery: %w", listErr)
		}
		for _, claim := range claims {
			if claim.Backend() != b.Name() || claim.BackendStorageID() != b.storageIdentity {
				return fmt.Errorf(
					"close intent for lease %q belongs to backend %q storage %s, not backend %q storage %s",
					claim.LeaseUUID(), claim.Backend(), claim.BackendStorageID(), b.Name(), b.storageIdentity,
				)
			}
			closeIntents[claim.LeaseUUID()] = claim
		}
	}
	pendingIntentLeases := make(map[string]struct{})
	operationClaimsByLease := make(map[string]shared.OperationIntentClaim)
	if b.callbackStore != nil {
		operationClaims, listErr := b.callbackStore.ListOperationIntents()
		if listErr != nil {
			return fmt.Errorf("list operation intents before state recovery: %w", listErr)
		}
		for _, claim := range operationClaims {
			if claim.Backend() != b.Name() || claim.BackendStorageID() != b.storageIdentity {
				return fmt.Errorf(
					"operation intent for lease %q belongs to backend %q storage %s, not backend %q storage %s",
					claim.LeaseUUID(), claim.Backend(), claim.BackendStorageID(), b.Name(), b.storageIdentity,
				)
			}
			if profileErr := validateDockerResourceProfiles(claim.Items(), claim.ResourceProfiles()); profileErr != nil {
				return fmt.Errorf(
					"operation intent for lease %q has invalid resource authority: %w",
					claim.LeaseUUID(), profileErr,
				)
			}
			if _, closing := closeIntents[claim.LeaseUUID()]; closing {
				return fmt.Errorf(
					"lease %q has simultaneous durable close and operation intents",
					claim.LeaseUUID(),
				)
			}
			pendingIntentLeases[claim.LeaseUUID()] = struct{}{}
			operationClaimsByLease[claim.LeaseUUID()] = claim
		}
	}
	if b.callbackStore != nil {
		maintenanceClaims, listErr := b.callbackStore.ListMaintenanceIntents()
		if listErr != nil {
			return fmt.Errorf("list maintenance intents before state recovery: %w", listErr)
		}
		for _, claim := range maintenanceClaims {
			if _, closing := closeIntents[claim.LeaseUUID()]; closing {
				return fmt.Errorf(
					"lease %q has simultaneous durable close and maintenance intents",
					claim.LeaseUUID(),
				)
			}
			if _, operating := operationClaimsByLease[claim.LeaseUUID()]; operating {
				return fmt.Errorf(
					"lease %q has simultaneous durable operation and maintenance intents",
					claim.LeaseUUID(),
				)
			}
			pendingIntentLeases[claim.LeaseUUID()] = struct{}{}
		}
	}
	// A restoring source row owns the destination's adopted bytes until an exact
	// active Release is durable. Its destination snapshot is therefore the
	// recovery authority after the operation intent has been consumed (including
	// the crash window where the restore succeeded but Release.Append failed).
	// Load it before inspecting container SKUs so recovery never falls back to
	// mutable configuration for that generation.
	restoreAuthorityByDestination := make(map[string]shared.RetentionEntry)
	if b.retentionStore != nil {
		restoring, listErr := b.retentionStore.ListRestoring()
		if listErr != nil {
			return fmt.Errorf("list restore destination authorities before state recovery: %w", listErr)
		}
		for _, source := range restoring {
			if len(source.DestinationItems) == 0 {
				continue // Pre-authority row: no exact destination sizing proof.
			}
			if prior, duplicate := restoreAuthorityByDestination[source.NewLeaseUUID]; duplicate {
				return fmt.Errorf(
					"multiple restore sources %q and %q own destination %q",
					prior.OriginalLeaseUUID, source.OriginalLeaseUUID, source.NewLeaseUUID,
				)
			}
			restoreAuthorityByDestination[source.NewLeaseUUID] = source
		}
	}

	ordinaryContainers := make([]ContainerInfo, 0, len(containers))
	for _, container := range containers {
		if _, closing := closeIntents[container.LeaseUUID]; closing {
			continue
		}
		ordinaryContainers = append(ordinaryContainers, container)
	}

	// Recover-time migration pre-pass. Groups any legacy single-service
	// containers by lease and produces a per-lease migration plan. Runs
	// BEFORE the main recovery loop so all in-memory provision state
	// observed by Update / Restart paths is post-migration consistent
	// (per QA's Task 6 carry-over note: prov.Items mutation must not
	// race ahead of prov.ServiceContainers population).
	//
	// Each plan is then executed atomically per lease by
	// executeLegacyMigration in the loop below. Any per-lease
	// failure aborts startup with operator-actionable guidance —
	// fred refuses to run with half-migrated state because the
	// stack-only downstream code can't drive a mixed cohort.
	legacyPlans, committedPrevRemnants, planErr := b.planLegacyMigrations(ctx, ordinaryContainers)
	if planErr != nil {
		return fmt.Errorf("plan legacy migrations: %w", planErr)
	}
	for _, plan := range legacyPlans {
		b.logger.Info("legacy lease migration planned",
			"lease_uuid", plan.LeaseUUID,
			"tenant", plan.Tenant,
			"sku", plan.SKU,
			"instances", len(plan.Instances),
		)
	}
	if len(legacyPlans) > 0 {
		// Execute each plan atomically per lease. Any per-lease failure
		// aborts startup with operator-actionable guidance — fred refuses
		// to run with half-migrated state because the stack-only code
		// downstream can't drive a mixed cohort.
		for _, plan := range legacyPlans {
			// Start intentionally supplies the backend lifetime rather than its
			// shorter caller context, but that must not make a wedged Docker or
			// Compose mutation process-lifetime blocking. Bound each lease's whole
			// migration; periodic reconciliation may impose a tighter parent.
			migrationTimeout := max(
				cmp.Or(b.cfg.ProvisionTimeout, 10*time.Minute),
				cmp.Or(b.cfg.MigrationReadyTimeout, defaultMigrationReadyTimeout),
			)
			migrationCtx, cancelMigration := context.WithTimeout(ctx, migrationTimeout)
			migrationErr := b.executeLegacyMigration(migrationCtx, plan, b.logger)
			cancelMigration()
			if migrationErr != nil {
				b.logger.Error("legacy migration failed; fred refuses to start with unmigrated legacy containers — "+
					"investigate the failure cause and re-run fred (migration is idempotent), "+
					"or deprovision the lease manually if data loss is acceptable",
					"lease_uuid", plan.LeaseUUID, "error", migrationErr)
				return fmt.Errorf("legacy migration failed: lease %s: %w", plan.LeaseUUID, migrationErr)
			}
		}
		// Re-list managed containers: migration changed every container's
		// name + label set, so the slice captured at the top of this
		// function is stale and the main loop below would otherwise see
		// the old names.
		refreshed, err := b.listManagedContainersForRecovery(ctx)
		if err != nil {
			return fmt.Errorf("re-list managed containers after migration: %w", err)
		}
		containers = refreshed
		ordinaryContainers = ordinaryContainers[:0]
		for _, container := range containers {
			if _, closing := closeIntents[container.LeaseUUID]; closing {
				continue
			}
			ordinaryContainers = append(ordinaryContainers, container)
		}
	}
	callbackPairs, err := recoveredCallbackPairsExcept(ordinaryContainers, pendingIntentLeases)
	if err != nil {
		return fmt.Errorf("validate recovered callback cohorts: %w", err)
	}

	// Bind ordinary recovery to the durable desired topology, not merely to
	// whatever subset Docker happened to list. Pending write-ahead operations
	// are excluded because their own exact intent classifier owns the transition
	// between the previous active release and the candidate generation.
	containersByLease := make(map[string][]ContainerInfo)
	for _, container := range ordinaryContainers {
		if container.LeaseUUID == "" || isLegacyRollbackRemnant(container) {
			continue
		}
		containersByLease[container.LeaseUUID] = append(containersByLease[container.LeaseUUID], container)
	}
	releaseLeaseUUIDs := make(map[string]struct{}, len(containersByLease)+len(committedPrevRemnants))
	for leaseUUID := range containersByLease {
		releaseLeaseUUIDs[leaseUUID] = struct{}{}
	}
	for leaseUUID := range committedPrevRemnants {
		releaseLeaseUUIDs[leaseUUID] = struct{}{}
	}
	if b.releaseStore != nil {
		storedLeaseUUIDs, listErr := b.releaseStore.LeaseUUIDs()
		if listErr != nil {
			return fmt.Errorf("enumerate durable release leases: %w", listErr)
		}
		for _, leaseUUID := range storedLeaseUUIDs {
			if _, closing := closeIntents[leaseUUID]; closing {
				continue
			}
			releaseLeaseUUIDs[leaseUUID] = struct{}{}
		}
	}
	releasesByLease := make(map[string]*shared.Release, len(releaseLeaseUUIDs))
	// maintenancePolicyFailures preserves terminal policies that intentionally
	// differ from pure substrate liveness. In particular, Update reports a
	// pre-substrate image-pull refusal as Failed even though the untouched source
	// cohort is still running; the exact failed maintenance Release is the
	// durable evidence needed to reproduce that projection after a restart.
	maintenancePolicyFailures := make(map[string]shared.Release)
	cohortIssues := make(map[string]error)
	if b.releaseStore != nil {
		for _, leaseUUID := range slices.Sorted(maps.Keys(releaseLeaseUUIDs)) {
			history, historyErr := b.releaseStore.List(leaseUUID)
			if historyErr != nil {
				return fmt.Errorf("read release history for lease %q: %w", leaseUUID, historyErr)
			}
			if len(history) > 0 {
				terminal := history[len(history)-1]
				if terminal.Status == "failed" && terminal.MaintenanceID.Valid() &&
					terminal.Reason == backend.ReasonImagePullFailed {
					maintenancePolicyFailures[leaseUUID] = terminal
				}
			}
			release, releaseErr := b.releaseStore.LatestActive(leaseUUID)
			if releaseErr != nil {
				return fmt.Errorf("read active release for lease %q: %w", leaseUUID, releaseErr)
			}
			if release == nil {
				if _, committed := committedPrevRemnants[leaseUUID]; committed {
					return fmt.Errorf("durably migrated lease %q no longer has an active release", leaseUUID)
				}
				continue
			}
			if len(release.Items) == 0 {
				if _, pending := pendingIntentLeases[leaseUUID]; !pending {
					items, deriveErr := deriveLegacyActiveReleaseItems(
						release,
						containersByLease[leaseUUID],
					)
					if deriveErr != nil {
						return fmt.Errorf(
							"derive v0.13 active release authority for lease %q: %w",
							leaseUUID,
							deriveErr,
						)
					}
					resourceProfiles, profileErr := b.resolveResourceProfiles(items)
					if profileErr != nil {
						return fmt.Errorf(
							"freeze v0.13 active release resource profiles for lease %q: %w",
							leaseUUID,
							profileErr,
						)
					}
					migrationEvidence := committedPrevRemnants[leaseUUID]
					authorityClass := shared.LegacyActiveAuthorityWorkload
					if migrationEvidence.legacyAuthorityClass != 0 {
						authorityClass = migrationEvidence.legacyAuthorityClass
					}
					if backfillErr := b.releaseStore.BackfillLegacyActiveAuthority(
						leaseUUID,
						*release,
						items,
						resourceProfiles,
						authorityClass,
					); backfillErr != nil {
						return fmt.Errorf(
							"persist v0.13 active release authority for lease %q: %w",
							leaseUUID,
							backfillErr,
						)
					}
					release.Items = items
					release.ResourceProfiles = resourceProfiles
					release.LegacyMigration = authorityClass == shared.LegacyActiveAuthorityMigration
				}
			}
			releasesByLease[leaseUUID] = release
			if len(release.Items) > 0 && len(release.ResourceProfiles) == 0 {
				resourceProfiles, profileErr := b.resolveResourceProfiles(release.Items)
				if profileErr != nil {
					return fmt.Errorf("resolve v0.13 active release resource profiles for lease %q: %w", leaseUUID, profileErr)
				}
				if profileErr := b.releaseStore.BackfillActiveResourceProfiles(
					leaseUUID, release.Version, release.Items, resourceProfiles,
				); profileErr != nil {
					return fmt.Errorf("backfill v0.13 active release resource profiles for lease %q: %w", leaseUUID, profileErr)
				}
				release.ResourceProfiles = resourceProfiles
			}
			if len(release.ResourceProfiles) > 0 {
				if profileErr := validateDockerResourceProfiles(release.Items, release.ResourceProfiles); profileErr != nil {
					return fmt.Errorf("validate active release resource profiles for lease %q: %w", leaseUUID, profileErr)
				}
			}
			if _, pending := pendingIntentLeases[leaseUUID]; pending {
				continue
			}
			if cohortErr := validateRecoveredReleaseCohort(release, containersByLease[leaseUUID]); cohortErr != nil {
				cohortIssues[leaseUUID] = cohortErr
				continue
			}
			// A complete v0.13 cohort is the last place its tokenless principal
			// and callback pair can be proven without inference. Freeze that identity
			// proactively, before an ordinary runtime failure or later replacement can
			// remove the last container. Callbackless pre-label cohorts remain on the
			// older conservative path because no callback authority can be minted.
			if release.OperationID == "" && release.RuntimeAuthority == nil &&
				release.LegacyRuntimeAuthority == nil && len(containersByLease[leaseUUID]) > 0 {
				pair := callbackPairs[leaseUUID]
				if pair.callbackURL != "" {
					identity := containersByLease[leaseUUID][0]
					legacyAuthority, authorityErr := shared.NewLegacyRuntimeAuthority(
						identity.Tenant,
						identity.ProviderUUID,
						pair.callbackURL,
						pair.lifecycleCallbackURL,
					)
					if authorityErr != nil {
						return fmt.Errorf("freeze v0.13 runtime authority for lease %q: %w",
							leaseUUID, authorityErr)
					}
					if backfillErr := b.releaseStore.BackfillLegacyRuntimeAuthority(
						leaseUUID, *release, legacyAuthority,
					); backfillErr != nil {
						return fmt.Errorf("persist v0.13 runtime authority for lease %q: %w",
							leaseUUID, backfillErr)
					}
					release.LegacyRuntimeAuthority = &legacyAuthority
				}
			}
		}
	}

	allocsByLease := make(map[string][]shared.ResourceAllocation)
	building := make(map[string]*recoveredProvision)
	// firstExitedByLease[uuid] is the container ID of the first container we
	// observed in an exited state for that lease. Used to fire containerDiedMsg
	// into the actor for Ready→Failed transitions so the SM handles the
	// callback emission via its Failing→Failed flow.
	firstExitedByLease := make(map[string]string)
	skippedUnknownSKU := 0

	// Group containers by lease UUID
	for _, c := range ordinaryContainers {
		// Skip containers without required labels
		if c.LeaseUUID == "" || c.SKU == "" {
			b.logger.Warn("skipping container with missing labels", "container_id", leasesm.ShortID(c.ContainerID))
			continue
		}

		// Skip migration -prev remnants. These are legacy containers renamed
		// by executeLegacyMigration's rollback window — they still carry the
		// fred.lease_uuid + fred.managed labels but no fred.service_name, so
		// without this guard the legacy-single-item branch below would
		// process them as live leases (inflating prov.Quantity and appending
		// a spurious LeaseItem{ServiceName:""}). The migration pre-pass now
		// deliberately consumes both original legacy names and `-prev` rollback
		// remnants, re-converges the durable migration, and schedules tracked
		// cleanup. This guard keeps those remnants out of the live projection
		// during the inspection grace window.
		if isLegacyRollbackRemnant(c) {
			continue
		}

		// Size the recovered instance from durable authority whenever one exists.
		// Operation intent wins during its write-ahead window; otherwise the active
		// release snapshot owns the live generation, followed by an outstanding
		// restore finalizer's exact destination snapshot. Current config is only the
		// v0.13/container-only compatibility path.
		var resourceProfiles []shared.SKUResourceSnapshot
		if claim, pending := operationClaimsByLease[c.LeaseUUID]; pending {
			resourceProfiles = claim.ResourceProfiles()
			if claim.Kind() == shared.OperationIntentProvision &&
				(c.CallbackURL != claim.CallbackURL() ||
					c.LifecycleCallbackURL != claim.LifecycleCallbackURL()) {
				// A re-provision writes its candidate intent before tearing down the
				// predecessor. Size an exact old-generation survivor from the active
				// Release it still belongs to; the operation classifier later requires
				// both callback labels and every immutable field to match before it can
				// authorize teardown.
				if release := releasesByLease[c.LeaseUUID]; release != nil {
					if predecessorIdentity, ok := runtimeIdentityForRelease(release); ok &&
						containerMatchesReleaseRuntimeIdentity(c, predecessorIdentity) {
						resourceProfiles = release.ResourceProfiles
					} else if release.OperationID == "" && release.RuntimeAuthority == nil &&
						release.LegacyRuntimeAuthority == nil {
						// Transitional v0.13 rows can reach this pass before their
						// tokenless identity has been frozen. Sizing a visibly
						// non-candidate survivor from the immutable active Release is
						// conservative accounting only; the strict intent classifier
						// still proves principal, callback pair, image, and exact instance
						// membership before granting teardown authority.
						resourceProfiles = release.ResourceProfiles
					}
				}
			}
		} else if release := releasesByLease[c.LeaseUUID]; release != nil {
			resourceProfiles = release.ResourceProfiles
		} else if source, pendingFinalize := restoreAuthorityByDestination[c.LeaseUUID]; pendingFinalize {
			resourceProfiles = source.DestinationResourceProfiles
		}
		var resources shared.SKUResourceSnapshot
		if len(resourceProfiles) > 0 {
			var found bool
			resources, found = shared.LookupSKUResourceSnapshotRow(resourceProfiles, c.SKU)
			if !found {
				return fmt.Errorf("durable resource profiles for lease %q omit observed SKU %q", c.LeaseUUID, c.SKU)
			}
		} else {
			legacyItems := []backend.LeaseItem{{SKU: c.SKU, Quantity: 1, ServiceName: cmp.Or(c.ServiceName, manifest.DefaultServiceName)}}
			legacyProfiles, legacyErr := b.resolveResourceProfiles(legacyItems)
			err = legacyErr
			if err != nil {
				b.logger.Error("skipping container with unknown SKU — container is running but untracked",
					"container_id", leasesm.ShortID(c.ContainerID),
					"sku", c.SKU,
				)
				skippedUnknownSKU++
				continue
			}
			resources = legacyProfiles[0]
			resourceProfiles = legacyProfiles
		}
		profile := resources.Profile()
		effectiveDiskMB, diskErr := resources.EffectiveDiskMB()
		if diskErr != nil {
			return fmt.Errorf("durable resource profile for lease %q SKU %q: %w", c.LeaseUUID, c.SKU, diskErr)
		}

		// Check if we already have a provision record for this lease
		prov, exists := building[c.LeaseUUID]
		if !exists {
			callbackPair := callbackPairs[c.LeaseUUID]
			if claim, pending := operationClaimsByLease[c.LeaseUUID]; pending {
				// recoveredCallbackPairsExcept intentionally excludes a pending
				// operation generation: a mixed old/new container cohort belongs to
				// the strict intent classifier, not ordinary lifecycle recovery. The
				// durable intent is nevertheless the exact callback authority for a
				// candidate projection. Publishing that authority here lets a fully
				// materialized generation pass the later success preflight even when
				// an older byte-identical active Release still exists. The classifier
				// independently proves every container label before settlement.
				callbackPair = recoveredCallbackPair{
					callbackURL:          claim.CallbackURL(),
					lifecycleCallbackURL: claim.LifecycleCallbackURL(),
				}
			}
			prov = &recoveredProvision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:            c.LeaseUUID,
					Tenant:               c.Tenant,
					ProviderUUID:         c.ProviderUUID,
					SKU:                  c.SKU,
					Status:               containerStatusToProvisionStatus(c.Status),
					Quantity:             0, // set from ContainerIDs below
					CreatedAt:            c.CreatedAt,
					FailCount:            c.FailCount,
					LastError:            "", // populated by cold-start/transition logic below
					Reason:               "", // populated by cold-start/transition logic below
					Message:              "",
					CallbackURL:          callbackPair.callbackURL,
					LifecycleCallbackURL: callbackPair.lifecycleCallbackURL,
					Items:                nil, // rebuilt from labels below
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
					ContainerIDs:         make([]string, 0),
					StackManifest:        nil, // restored below
					ServiceContainers:    nil, // rebuilt from labels below
				},
				resourceProfiles:      shared.CloneSKUResourceSnapshot(resourceProfiles),
				volumeCleanupAttempts: 0,
			}

			// Restore manifest from the last successful (active) release so
			// restart/update work after a cold start (manifest is not stored
			// in labels). Using LatestActive avoids picking up a failed
			// release (e.g., a failed update to a newer image).
			//
			// ParsePayload always returns a *StackManifest (legacy flat
			// payloads are auto-wrapped under DefaultServiceName). After
			// Task 9's recover-time migration runs, every recovered
			// provision is stack-form on disk, so the populated field is
			// always prov.StackManifest.
			if rel := releasesByLease[c.LeaseUUID]; rel != nil && len(rel.Manifest) > 0 {
				stackM, payloadErr := manifest.ParsePayload(rel.Manifest)
				if payloadErr != nil {
					b.logger.Warn("failed to parse recovered manifest",
						"lease_uuid", c.LeaseUUID, "error", payloadErr)
				} else {
					prov.StackManifest = stackM
				}
			} else if source, pendingFinalize := restoreAuthorityByDestination[c.LeaseUUID]; pendingFinalize {
				prov.StackManifest = source.StackManifest
			}

			building[c.LeaseUUID] = prov
		}

		// Add container ID to the provision
		prov.ContainerIDs = append(prov.ContainerIDs, c.ContainerID)
		prov.Quantity = len(prov.ContainerIDs)

		// Build ServiceContainers map and Items for stack containers.
		if c.ServiceName != "" {
			if prov.ServiceContainers == nil {
				prov.ServiceContainers = make(map[string][]string)
			}
			prov.ServiceContainers[c.ServiceName] = append(prov.ServiceContainers[c.ServiceName], c.ContainerID)

			// Rebuild Items from container labels (SKU + ServiceName per container).
			// Use a dedup map keyed by service name since multiple containers
			// belong to the same item.
			found := false
			for idx := range prov.Items {
				if prov.Items[idx].ServiceName == c.ServiceName {
					prov.Items[idx].Quantity = len(prov.ServiceContainers[c.ServiceName])
					// CustomDomain is per-service: all instance containers
					// of a service carry byte-identical labels. Trust the
					// first one we recovered; later iterations are no-ops.
					if prov.Items[idx].CustomDomain == "" && c.CustomDomain != "" {
						prov.Items[idx].CustomDomain = c.CustomDomain
					}
					found = true
					break
				}
			}
			if !found {
				prov.Items = append(prov.Items, backend.LeaseItem{
					SKU:          c.SKU,
					Quantity:     1,
					ServiceName:  c.ServiceName,
					CustomDomain: c.CustomDomain,
				})
			}
		} else if len(prov.Items) == 0 {
			// Legacy single-item lease: rebuild prov.Items[0] from this
			// (only) container's labels so Restart/Update can re-emit the
			// secondary router. Idempotent across recovery iterations
			// because legacy provisions hold one container.
			prov.Items = append(prov.Items, backend.LeaseItem{
				SKU:          c.SKU,
				Quantity:     1,
				CustomDomain: c.CustomDomain,
			})
		}

		// Use the highest FailCount across containers. Labels are normally
		// identical, but can diverge after a partial re-provision.
		if c.FailCount > prov.FailCount {
			prov.FailCount = c.FailCount
		}

		// If any container is not ready, mark the whole provision as not ready.
		// Also track the first exited container — recoverState fires
		// containerDiedMsg with this ID for Ready→Failed transitions so the
		// SM's Failing state handles the callback emission.
		status := containerStatusToProvisionStatus(c.Status)
		if status != backend.ProvisionStatusReady && prov.Status == backend.ProvisionStatusReady {
			prov.Status = status
		}
		if status == backend.ProvisionStatusFailed {
			if _, already := firstExitedByLease[c.LeaseUUID]; !already {
				firstExitedByLease[c.LeaseUUID] = c.ContainerID
			}
		}

		// Use instance-specific allocation ID, grouped by lease for filtering.
		// Stack uses service-aware IDs: {leaseUUID}-{serviceName}-{instanceIndex}
		var instanceID string
		if c.ServiceName != "" {
			instanceID = fmt.Sprintf("%s-%s-%d", c.LeaseUUID, c.ServiceName, c.InstanceIndex)
		} else {
			instanceID = fmt.Sprintf("%s-%d", c.LeaseUUID, c.InstanceIndex)
		}
		allocsByLease[c.LeaseUUID] = append(allocsByLease[c.LeaseUUID], shared.ResourceAllocation{
			LeaseUUID: instanceID,
			Tenant:    c.Tenant,
			SKU:       c.SKU,
			CPUCores:  profile.CPUCores,
			MemoryMB:  profile.MemoryMB,
			DiskMB:    effectiveDiskMB,
		})
	}

	// An uncommitted provision intent is the complete cleanup authority for its
	// write-ahead generation. Container inventory may expose only a prefix of the
	// requested cohort (or none at all), but publishing that partial shape would
	// under-count reserved capacity and leave the not-yet-mounted canonical volume
	// names unclaimed while recovery tears the cohort down. Overlay the immutable
	// intent snapshot before any cleanup can run. A same-token committed Release is
	// excluded: it crossed the success boundary and the release-owned path below is
	// its stronger runtime authority.
	for _, leaseUUID := range slices.Sorted(maps.Keys(operationClaimsByLease)) {
		claim := operationClaimsByLease[leaseUUID]
		if claim.Kind() != shared.OperationIntentProvision {
			continue
		}
		committed, commitErr := operationReleaseMatchesIntent(releasesByLease[leaseUUID], claim)
		if commitErr != nil {
			return fmt.Errorf("validate pending provision release for lease %q: %w", leaseUUID, commitErr)
		}
		if committed {
			continue
		}
		items := claim.EffectiveItems()
		quantity, quantityErr := backend.ValidateOperationQuantities(items)
		if quantityErr != nil {
			return fmt.Errorf("validate pending provision quantities for lease %q: %w", leaseUUID, quantityErr)
		}
		stackManifest, parseErr := manifest.ParsePayload(claim.Manifest())
		if parseErr != nil {
			return fmt.Errorf("parse pending provision manifest for lease %q: %w", leaseUUID, parseErr)
		}
		resourceProfiles := claim.ResourceProfiles()
		recovered, exists := building[leaseUUID]
		if !exists {
			recovered = &recoveredProvision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:            leaseUUID,
					Tenant:               claim.Tenant(),
					ProviderUUID:         claim.ProviderUUID(),
					SKU:                  items[0].SKU,
					Status:               backend.ProvisionStatusProvisioning,
					Quantity:             quantity,
					CreatedAt:            claim.CreatedAt(),
					FailCount:            0,
					LastError:            "",
					Reason:               "",
					Message:              "",
					CallbackURL:          claim.CallbackURL(),
					LifecycleCallbackURL: claim.LifecycleCallbackURL(),
					Items:                items,
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
					ContainerIDs:         nil,
					StackManifest:        stackManifest,
					ServiceContainers:    nil,
				},
				resourceProfiles:      resourceProfiles,
				volumeCleanupAttempts: 0,
			}
			building[leaseUUID] = recovered
		} else {
			// Labels identify candidate substrate; the intent alone authorizes the
			// complete topology, callbacks, and sizing. The strict classifier later
			// verifies every observed label before any teardown is attempted.
			recovered.LeaseUUID = leaseUUID
			recovered.Tenant = claim.Tenant()
			recovered.ProviderUUID = claim.ProviderUUID()
			recovered.SKU = items[0].SKU
			recovered.Quantity = quantity
			recovered.CreatedAt = claim.CreatedAt()
			recovered.CallbackURL = claim.CallbackURL()
			recovered.LifecycleCallbackURL = claim.LifecycleCallbackURL()
			recovered.Items = items
			recovered.ResourceProfiles = shared.CloneSKUResourceSnapshot(resourceProfiles)
			recovered.StackManifest = stackManifest
			recovered.resourceProfiles = resourceProfiles
		}
		allocations, allocationErr := recoveredSnapshotAllocations(
			leaseUUID, claim.Tenant(), items, resourceProfiles,
		)
		if allocationErr != nil {
			return fmt.Errorf("rebuild pending provision allocations for lease %q: %w", leaseUUID, allocationErr)
		}
		allocsByLease[leaseUUID] = allocations
	}

	// A current Release carries an all-or-nothing runtime identity specifically
	// so recovery remains convergent after its operation intent has settled and a
	// later restart observes zero survivors. When an operation is pending, only an
	// exact same-token Release may contribute this authority; an older active
	// generation must remain outside the candidate operation's recovery boundary.
	// The legacy identity is separately typed and tokenless, but provides the same
	// zero-survivor reconstruction for an adopted v0.13 Release.
	for _, leaseUUID := range slices.Sorted(maps.Keys(releasesByLease)) {
		release := releasesByLease[leaseUUID]
		authority, hasRuntimeAuthority := runtimeIdentityForRelease(release)
		if !hasRuntimeAuthority {
			continue
		}
		if _, restoreOwned := restoreAuthorityByDestination[leaseUUID]; restoreOwned {
			continue
		}
		if claim, pending := operationClaimsByLease[leaseUUID]; pending {
			committed, commitErr := operationReleaseMatchesIntent(release, claim)
			if commitErr != nil {
				return fmt.Errorf("validate committed operation release for lease %q: %w", leaseUUID, commitErr)
			}
			if !committed {
				continue
			}
		}

		items := slices.Clone(release.Items)
		quantity, quantityErr := backend.ValidateOperationQuantities(items)
		if quantityErr != nil {
			return fmt.Errorf("validate durable runtime quantities for lease %q: %w", leaseUUID, quantityErr)
		}
		resourceProfiles := shared.CloneSKUResourceSnapshot(release.ResourceProfiles)
		stackManifest, parseErr := manifest.ParsePayload(release.Manifest)
		if parseErr != nil {
			return fmt.Errorf("parse durable runtime manifest for lease %q: %w", leaseUUID, parseErr)
		}
		recovered, exists := building[leaseUUID]
		if !exists {
			recovered = &recoveredProvision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:            leaseUUID,
					Tenant:               authority.Tenant(),
					ProviderUUID:         authority.ProviderUUID(),
					SKU:                  items[0].SKU,
					Status:               backend.ProvisionStatusFailed,
					Quantity:             quantity,
					CreatedAt:            release.CreatedAt,
					FailCount:            0,
					LastError:            "",
					Reason:               "",
					Message:              "",
					CallbackURL:          authority.CallbackURL(),
					LifecycleCallbackURL: authority.LifecycleCallbackURL(),
					Items:                items,
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
					ContainerIDs:         nil,
					StackManifest:        stackManifest,
					ServiceContainers:    nil,
				},
				resourceProfiles:      resourceProfiles,
				volumeCleanupAttempts: 0,
			}
			building[leaseUUID] = recovered
			if _, already := cohortIssues[leaseUUID]; !already {
				cohortIssues[leaseUUID] = errors.New("committed release has no surviving containers")
			}
		} else {
			// Durable authority wins over potentially divergent survivor labels. The
			// cohort validator above records any mismatch and the merge below exposes
			// it only as a terminal Failed projection.
			recovered.Tenant = authority.Tenant()
			recovered.ProviderUUID = authority.ProviderUUID()
			recovered.CallbackURL = authority.CallbackURL()
			recovered.LifecycleCallbackURL = authority.LifecycleCallbackURL()
			recovered.SKU = items[0].SKU
			recovered.Quantity = quantity
			recovered.Items = items
			recovered.StackManifest = stackManifest
			recovered.resourceProfiles = resourceProfiles
		}
		allocations, allocationErr := recoveredSnapshotAllocations(
			leaseUUID, authority.Tenant(), items, resourceProfiles,
		)
		if allocationErr != nil {
			return fmt.Errorf("rebuild durable runtime allocations for lease %q: %w", leaseUUID, allocationErr)
		}
		allocsByLease[leaseUUID] = allocations
	}

	// A full close needs a conservative in-memory retry owner even when Docker
	// reports no survivors. Rebuild it exclusively from the immutable close
	// claim; container inventory contributes only best-effort IDs for teardown.
	// Cleanup-only claims intentionally publish no provision projection.
	for _, leaseUUID := range slices.Sorted(maps.Keys(closeIntents)) {
		claim := closeIntents[leaseUUID]
		if claim.CleanupOnly() {
			allocations, allocationErr := recoveredSnapshotAllocations(
				leaseUUID, "", claim.Items(), claim.ResourceProfiles(),
			)
			if allocationErr != nil {
				return fmt.Errorf(
					"rebuild cleanup-only close allocations for lease %q: %w",
					leaseUUID,
					allocationErr,
				)
			}
			allocsByLease[leaseUUID] = allocations
			continue
		}
		stackManifest, parseErr := manifest.ParsePayload(claim.Manifest())
		if parseErr != nil {
			return fmt.Errorf("parse durable close manifest for lease %q: %w", leaseUUID, parseErr)
		}
		items := claim.Items()
		quantity, quantityErr := backend.ValidateOperationQuantities(items)
		if quantityErr != nil {
			return fmt.Errorf("validate durable close quantities for lease %q: %w", leaseUUID, quantityErr)
		}
		containerIDSet := make(map[string]struct{})
		for _, container := range containers {
			if container.LeaseUUID == leaseUUID &&
				container.ContainerID != "" &&
				!isLegacyRollbackRemnant(container) {
				containerIDSet[container.ContainerID] = struct{}{}
			}
		}
		containerIDs := slices.Sorted(maps.Keys(containerIDSet))
		building[leaseUUID] = &recoveredProvision{ //exhaustruct:enforce
			ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
				LeaseUUID:            leaseUUID,
				Tenant:               claim.Tenant(),
				ProviderUUID:         claim.ProviderUUID(),
				SKU:                  items[0].SKU,
				Status:               backend.ProvisionStatusDeprovisioning,
				Quantity:             quantity,
				CreatedAt:            claim.CreatedAt(),
				FailCount:            0,
				LastError:            "",
				Reason:               "",
				Message:              "",
				CallbackURL:          claim.CallbackURL(),
				LifecycleCallbackURL: claim.LifecycleCallbackURL(),
				Items:                items,
				ResourceProfiles:     claim.ResourceProfiles(),
				ContainerIDs:         containerIDs,
				StackManifest:        stackManifest,
				ServiceContainers:    nil,
			},
			resourceProfiles:      claim.ResourceProfiles(),
			volumeCleanupAttempts: claim.CleanupAttempts(),
		}
		allocations, allocationErr := recoveredSnapshotAllocations(
			leaseUUID, claim.Tenant(), items, claim.ResourceProfiles(),
		)
		if allocationErr != nil {
			return fmt.Errorf("rebuild durable close allocations for lease %q: %w", leaseUUID, allocationErr)
		}
		allocsByLease[leaseUUID] = allocations
	}
	// Rebuild every destination still owned by a restore source finalizer. The
	// finalizer remains load-bearing both before and after its active Release is
	// committed: before commit it owns rollback; after commit it carries the
	// tenant/provider identity needed to reconstruct a zero-survivor destination
	// safely across repeated restarts. A committed destination is materialized as
	// Failed (and therefore repairable); an uncommitted zero-survivor attempt keeps
	// allocation only until reconciliation hands the adopted bytes back.
	for leaseUUID, source := range restoreAuthorityByDestination {
		if _, closing := closeIntents[leaseUUID]; closing {
			// The close journal was durably published before the restore finalizer
			// handoff. Its full snapshot owns projection and allocation recovery; the
			// resumed close re-validates both authorities and consumes the finalizer
			// before any teardown.
			continue
		}
		items := slices.Clone(source.DestinationItems)
		resourceProfiles := shared.CloneSKUResourceSnapshot(source.DestinationResourceProfiles)
		quantity, quantityErr := backend.ValidateOperationQuantities(items)
		if quantityErr != nil {
			return fmt.Errorf("validate restore finalizer quantities for destination %q: %w", leaseUUID, quantityErr)
		}
		if profileErr := validateDockerResourceProfiles(items, resourceProfiles); profileErr != nil {
			return fmt.Errorf("validate restore finalizer resource profiles for destination %q: %w", leaseUUID, profileErr)
		}
		if source.StackManifest == nil {
			return fmt.Errorf("restore finalizer for destination %q has no manifest", leaseUUID)
		}
		if topologyErr := manifest.ValidateStackAgainstItems(source.StackManifest, items); topologyErr != nil {
			return fmt.Errorf("validate restore finalizer topology for destination %q: %w", leaseUUID, topologyErr)
		}
		manifestBytes, marshalErr := json.Marshal(source.StackManifest)
		if marshalErr != nil {
			return fmt.Errorf("marshal restore finalizer manifest for destination %q: %w", leaseUUID, marshalErr)
		}
		release := releasesByLease[leaseUUID]
		committed := false
		if release != nil {
			matches, matchErr := restoreReleaseMatchesAuthority(release, source)
			if matchErr != nil {
				return fmt.Errorf("validate active release for restore destination %q: %w", leaseUUID, matchErr)
			}
			if !matches {
				return fmt.Errorf(
					"active release for restore destination %q differs from source finalizer authority",
					leaseUUID,
				)
			}
			committed = true
		}

		recovered, exists := building[leaseUUID]
		switch {
		case exists:
			cohortAuthority := &shared.Release{
				Manifest: manifestBytes,
				Items:    items,
			}
			if release != nil {
				// A successful maintenance may add an exact MaintenanceID and move
				// the callback base while the restore source finalizer still awaits
				// cleanup. The matching active Release is the generation authority.
				cohortAuthority = release
			}
			if recovered.Tenant != source.Tenant || recovered.ProviderUUID != source.ProviderUUID {
				cohortIssues[leaseUUID] = fmt.Errorf(
					"restored container identity does not match source finalizer tenant/provider",
				)
			} else if cohortErr := validateRecoveredReleaseCohort(
				cohortAuthority, containersByLease[leaseUUID],
			); cohortErr != nil {
				cohortIssues[leaseUUID] = fmt.Errorf("restore finalizer cohort: %w", cohortErr)
			}
			// The source finalizer is the immutable authorization boundary. Keep the
			// divergent substrate visible as Failed, but never publish its labels as
			// command or quota authority.
			recovered.Tenant = source.Tenant
			recovered.ProviderUUID = source.ProviderUUID
			recovered.Items = items
			recovered.Quantity = quantity
			recovered.resourceProfiles = resourceProfiles
			recovered.StackManifest = source.StackManifest
		case committed:
			callbackURL := source.DestinationCallbackURL
			lifecycleCallbackURL := source.DestinationLifecycleCallbackURL
			if release.RuntimeAuthority != nil {
				// The lingering restore finalizer retains the original typed
				// identity, while a successful maintenance release owns the current
				// callback base. restoreReleaseMatchesAuthority above proved that
				// this is a same-token/class move before these URLs can be consumed.
				callbackURL = release.RuntimeAuthority.CallbackURL()
				lifecycleCallbackURL = release.RuntimeAuthority.LifecycleCallbackURL()
			}
			if claim, pending := operationClaimsByLease[leaseUUID]; pending {
				if claim.Kind() != shared.OperationIntentRestore ||
					claim.SourceLeaseUUID() != source.OriginalLeaseUUID ||
					claim.SourceGeneration() != source.Generation {
					return fmt.Errorf("operation intent for committed restore destination %q conflicts with source finalizer", leaseUUID)
				}
				callbackURL = claim.CallbackURL()
				lifecycleCallbackURL = claim.LifecycleCallbackURL()
			}
			createdAt := source.RestoringSince
			if createdAt.IsZero() {
				createdAt = source.CreatedAt
			}
			building[leaseUUID] = &recoveredProvision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:            leaseUUID,
					Tenant:               source.Tenant,
					ProviderUUID:         source.ProviderUUID,
					SKU:                  items[0].SKU,
					Status:               backend.ProvisionStatusFailed,
					Quantity:             quantity,
					CreatedAt:            createdAt,
					FailCount:            0,
					LastError:            "",
					Reason:               "",
					Message:              "",
					CallbackURL:          callbackURL,
					LifecycleCallbackURL: lifecycleCallbackURL,
					Items:                items,
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(resourceProfiles),
					ContainerIDs:         nil,
					StackManifest:        source.StackManifest,
					ServiceContainers:    nil,
				},
				resourceProfiles:      resourceProfiles,
				volumeCleanupAttempts: 0,
			}
			cohortIssues[leaseUUID] = errors.New("committed restore destination has no surviving containers")
		default:
			// No active Release means destination ownership never committed. Preserve
			// allocation without manufacturing Restart authority; reconciliation owns
			// teardown, exact intent failure settlement, and source handback.
			delete(cohortIssues, leaseUUID)
		}
		allocations, allocationErr := recoveredSnapshotAllocations(
			leaseUUID, source.Tenant, items, resourceProfiles,
		)
		if allocationErr != nil {
			return fmt.Errorf("rebuild restore finalizer allocations for destination %q: %w", leaseUUID, allocationErr)
		}
		allocsByLease[leaseUUID] = allocations
	}
	// Release Items are the immutable ordered operation input. Docker's list
	// order is explicitly unspecified, so even an exact set match must restore
	// this durable slice rather than publishing the order in which containers
	// happened to be returned.
	for leaseUUID, release := range releasesByLease {
		if release == nil || len(release.Items) == 0 {
			continue
		}
		if _, restoreOwned := restoreAuthorityByDestination[leaseUUID]; restoreOwned {
			// The restore-finalizer pass already rebuilt the exact complete
			// reservation under the source-authorized tenant. Never overwrite it
			// from potentially divergent survivor labels.
			continue
		}
		if _, pending := pendingIntentLeases[leaseUUID]; pending {
			continue
		}
		if recovered, exists := building[leaseUUID]; exists {
			desiredQuantity, quantityErr := backend.ValidateOperationQuantities(release.Items)
			if quantityErr != nil {
				return fmt.Errorf(
					"validate durable release quantities for lease %q: %w",
					leaseUUID,
					quantityErr,
				)
			}
			recovered.Items = slices.Clone(release.Items)
			recovered.Quantity = desiredQuantity
			recovered.resourceProfiles = shared.CloneSKUResourceSnapshot(release.ResourceProfiles)
			allocations, allocationErr := b.recoveredReleaseAllocations(
				leaseUUID, recovered.Tenant, release.Items, release.ResourceProfiles,
			)
			if allocationErr != nil {
				return fmt.Errorf(
					"rebuild durable release allocations for lease %q: %w",
					leaseUUID,
					allocationErr,
				)
			}
			// Replace the survivor-derived allocation set even when the cohort is
			// incomplete or duplicated. The durable desired topology is the safe
			// accounting authority until explicit teardown releases its volumes.
			allocsByLease[leaseUUID] = allocations
		}
	}
	for leaseUUID, cohortErr := range cohortIssues {
		recovered, exists := building[leaseUUID]
		if !exists {
			return fmt.Errorf("durable release cohort for lease %q cannot be materialized: %w", leaseUUID, cohortErr)
		}
		desiredItems := recovered.Items
		if release := releasesByLease[leaseUUID]; release != nil {
			desiredItems = release.Items
		}
		b.logger.Error("recovered container cohort differs from durable release",
			"lease_uuid", leaseUUID,
			"observed_containers", len(recovered.ContainerIDs),
			"desired_containers", expectedIntentQuantity(desiredItems),
			"error", cohortErr,
		)
	}
	for leaseUUID, terminal := range maintenancePolicyFailures {
		if _, divergent := cohortIssues[leaseUUID]; divergent {
			continue
		}
		recovered := building[leaseUUID]
		if recovered == nil {
			continue
		}
		recovered.Status = backend.ProvisionStatusFailed
		recovered.LastError = cmp.Or(terminal.Message, backend.MsgImagePullFailed)
		recovered.Reason = backend.ReasonImagePullFailed
		recovered.Message = cmp.Or(terminal.Message, backend.MsgImagePullFailed)
	}

	// Merge with existing state and detect status transitions.
	b.provisionsMu.Lock()

	const incompleteCohortMessage = leasesm.ErrMsgCohortDiverged
	cohortDirectFailures := make(map[string]struct{}, len(cohortIssues))
	var cohortFailed []string
	var cohortTransitionLeases []string
	for leaseUUID, cohortErr := range cohortIssues {
		recovered := building[leaseUUID]
		existing, existed := b.provisions[leaseUUID]
		if existed && existing.Status == backend.ProvisionStatusReady {
			// Preserve the actor-visible source state. A typed event after the map
			// swap performs the Ready→Failed transition serially with every other
			// command for this lease.
			recovered.Status = backend.ProvisionStatusReady
			recovered.FailCount = existing.FailCount
			recovered.LastError = existing.LastError
			recovered.Reason = existing.Reason
			recovered.Message = existing.Message
			cohortTransitionLeases = append(cohortTransitionLeases, leaseUUID)
			continue
		}
		// On cold start there is no actor to synchronize. Materialize Failed
		// directly; any subsequently created actor initializes from that state.
		recovered.Status = backend.ProvisionStatusFailed
		if existed {
			recovered.FailCount = max(recovered.FailCount, existing.FailCount)
		} else {
			recovered.FailCount++
		}
		recovered.LastError = fmt.Sprintf("%s: %v", incompleteCohortMessage, cohortErr)
		recovered.Reason = backend.ReasonInternal
		recovered.Message = incompleteCohortMessage
		cohortDirectFailures[leaseUUID] = struct{}{}
		cohortFailed = append(cohortFailed, leaseUUID)
	}

	// Detect ready→failed transitions: containers that were running but have
	// since crashed. We hand off to the SM by firing containerDiedMsg on the
	// actor *after* the merge. Status stays Ready in the building value so the
	// actor's guard sees the pre-transition state and permits evContainerDied;
	// FailCount and LastError are populated by the SM's Failing entry action.
	var failedLeases []string
	for uuid, existing := range b.provisions {
		if existing.Status == backend.ProvisionStatusReady {
			if _, diverged := cohortIssues[uuid]; diverged {
				continue
			}
			if rec, ok := building[uuid]; ok && rec.Status == backend.ProvisionStatusFailed {
				rec.Status = backend.ProvisionStatusReady
				rec.FailCount = existing.FailCount
				rec.LastError = existing.LastError
				rec.Reason = existing.Reason
				rec.Message = existing.Message
				failedLeases = append(failedLeases, uuid)
				b.logger.Warn("container crashed after provisioning",
					"lease_uuid", uuid,
					"tenant", existing.Tenant,
				)
			}
		}
	}

	// Cold-start correction: provisions recovered as failed with no prior
	// in-memory state carry a creation-time FailCount label. Increment it to
	// account for the failure evidenced by the dead container. The baseline
	// LastError rides the materialized value.
	var coldStartFailed []string
	for uuid, rec := range building {
		if rec.Status == backend.ProvisionStatusFailed {
			if _, hasExisting := b.provisions[uuid]; !hasExisting {
				if _, policyFailure := maintenancePolicyFailures[uuid]; policyFailure {
					continue
				}
				if _, cohortFailure := cohortDirectFailures[uuid]; cohortFailure {
					continue
				}
				rec.FailCount++
				rec.LastError = leasesm.ErrMsgContainerExited
				rec.Reason = backend.ReasonContainerExited
				rec.Message = leasesm.ErrMsgContainerExited
				coldStartFailed = append(coldStartFailed, uuid)
				b.logger.Info("cold-start: adjusted FailCount for already-failed provision",
					"lease_uuid", uuid,
					"fail_count", rec.FailCount,
				)
			}
		}
	}

	// FailCount anti-regression on rebuilt entries: a re-list after an in-memory
	// increment would otherwise regress FailCount to the stale label. Preserve
	// the higher in-memory value. Skipped for in-flight statuses (preserved
	// wholesale below).
	for uuid, rec := range building {
		existing, ok := b.provisions[uuid]
		if !ok {
			continue
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// preserved wholesale below
		default:
			if existing.FailCount > rec.FailCount {
				rec.FailCount = existing.FailCount
			}
		}
	}

	// Publish: materialize every rebuilt entry into a fresh *provision (the only
	// path a recoveredProvision reaches b.provisions). A fresh struct clears
	// stale fields (LastError, VolumeCleanupAttempts) exactly as the prior
	// fresh-&provision{}+swap did.
	final := make(map[string]*provision, len(building))
	for uuid, rec := range building {
		final[uuid] = rec.materialize()
	}

	// Overlay existing entries that must be preserved: the actor / deprovision
	// goroutine owns their live state, so reuse the live *provision pointer
	// (no off-actor field mutation).
	for uuid, existing := range b.provisions {
		if _, closing := closeIntents[uuid]; closing {
			// The immutable close claim supersedes every volatile projection.
			// Cleanup-only closes deliberately remove a stale projection; full
			// closes use the conservative value materialized above.
			continue
		}
		if _, hasContainers := building[uuid]; hasContainers {
			// By-design (ENG-414): only the in-flight statuses below (plus a Failed
			// lease mid volume-cleanup-retry, ENG-603) are preserved here. Ready and
			// Failing/Failed[VCA==0] deliberately fall through to the container-derived
			// (materialized) value, so a crashed-then-running lease recovers to Ready
			// (locked by TestRecoverState_FailCountAntiRegression).
			// recoverState cannot distinguish that legitimate recovery from the narrow
			// race where the actor set Failing/Failed (via an event-loop die) AFTER our
			// pre-merge ListManagedContainers snapshot still showed the container
			// running — so an actor-set Failing/Failed can be momentarily overwritten
			// with Ready. This is accepted: it self-heals (the in-flight diag goroutine
			// completes → evDiagGathered → Failed and rewrites Status; or, once the dead
			// container is GC'd, the no-containers branch below drops the phantom-Ready
			// entry) and emits NO duplicate failure callback — Failing and Failed both
			// Ignore(evContainerDied) (lease_sm.go) and the SM's internal state
			// (NewStateMachine, not external storage) is unaffected by this map swap.
			switch existing.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				// In-flight re-provision: the rebuilt containers belong to the
				// previous (failed) provision; keep the in-flight entry so the
				// next container creation picks up the right FailCount.
				final[uuid] = existing
			case backend.ProvisionStatusDeprovisioning:
				// The deprovision goroutine owns this lease; do not resurrect it
				// to a container-derived status (ENG-193 explicit case).
				final[uuid] = existing
			case backend.ProvisionStatusFailed:
				// ENG-603: a Failed lease mid volume-cleanup-retry
				// (VolumeCleanupAttempts>0) reached deprovision.go's VCA-increment
				// block ONLY after its containers were successfully torn down (the
				// partial-container-failure branch returns without incrementing), so
				// its ContainerIDs are already nil and any container this snapshot
				// still lists for it is necessarily STALE — captured before the
				// concurrent doDeprovision's compose Down removed it, then merged here
				// after doDeprovision set Status=Failed. Resurrecting it to a
				// container-derived Ready (the general fall-through) would drop the
				// VolumeCleanupAttempts count and, once the next reconcile GCs the
				// phantom-Ready no-container entry, abandon the volume-cleanup retry
				// AND leak the still-held pool reservation (released only on a terminal
				// success or give-up). Preserve by pointer — same rationale as the
				// Deprovisioning case above and the ENG-546/562/563 Failed[VCA>0] pool
				// preservation. VCA==0 Failed leases still fall through to the
				// container-derived value so a Ready→crash→running lease recovers to
				// Ready (TestRecoverState_FailCountAntiRegression).
				if existing.VolumeCleanupAttempts > 0 {
					final[uuid] = existing
				}
			}
			continue
		}
		switch existing.Status {
		case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
			// In-flight operation that hasn't produced containers yet.
			final[uuid] = existing
		case backend.ProvisionStatusFailing:
			// Failing is transient (container-death detected, diag goroutine not
			// yet fired DiagGathered). Normalize to Failed so retry paths (which
			// require Status == Failed) can proceed. Build the kept entry as a
			// value — no in-place mutation of the published struct.
			rec := recoveredFromProvision(existing)
			rec.Status = backend.ProvisionStatusFailed
			final[uuid] = rec.materialize()
		case backend.ProvisionStatusFailed:
			// Failed provision whose containers are gone — preserve so the
			// reconciler sees the failure and its FailCount.
			final[uuid] = existing
		case backend.ProvisionStatusDeprovisioning:
			// Owned by the in-flight deprovision goroutine; preserve untouched
			// (ENG-193 explicit case — previously dropped on recovery).
			final[uuid] = existing
		}
	}
	// Build the container-derived allocations list, excluding leases with an
	// in-flight re-provision op (provisioning/restarting/updating). Their
	// container-derived allocations are in flux (old containers being torn down /
	// new ones materializing) or absent, so counting them would race the
	// authoritative reservation — the original ENG-546 exclusion. Those leases'
	// reservations are instead carried forward from the live pool by the general
	// preserve rule below (they are in `final`), so the exclusion is behavior-
	// preserving rather than strictly required — but keep it: it avoids counting
	// in-flux container values and is a margin against a duplicate-container
	// double-count should a reservation key ever be lost. Do NOT drop it without
	// restoring that guard.
	var allocations []shared.ResourceAllocation
	for uuid, allocs := range allocsByLease {
		if claim, closing := closeIntents[uuid]; closing && claim.CleanupOnly() {
			// Cleanup-only deliberately has no provision projection. Its durable
			// topology nevertheless counts until finalization succeeds; a later
			// sweep sees the resolved claim absent and drops these allocations.
			allocations = append(allocations, allocs...)
			continue
		}
		if prov, ok := final[uuid]; ok {
			switch prov.Status {
			case backend.ProvisionStatusProvisioning, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
				continue
			}
		}
		allocations = append(allocations, allocs...)
	}
	// Pool-authoritative preservation: the ResourcePool is the source of truth for
	// a tracked lease's reserved footprint. Preserve the reservation of EVERY lease
	// still tracked in b.provisions (`final`). ResetPreserving can only retain a key
	// that physically exists in the live pool, so this keeps exactly the
	// reservations really held and drops orphan keys of untracked leases. This one
	// rule subsumes the former per-status allowlist (Provisioning/Restarting/
	// Updating/Deprovisioning + Failed[VolumeCleanupAttempts>0]; ENG-546/562/563)
	// AND additionally keeps the still-held key of a Ready→crash→GC'd Failed lease,
	// a restore-rollback re-quarantine failure, and a deprovision partial-removal
	// failure — all Failed/VCA==0 leases whose bytes are still on disk that the
	// allowlist dropped (ENG-567).
	//
	// INVARIANT this relies on (test-locked): every path that frees a lease's bytes
	// also Releases its pool key, or Deletes the lease from b.provisions (which
	// drops it from `final`). Release sites are small and greppable: deprovision.go
	// releaseLive, provision.go failure defer + re-provision cleanup, restore.go
	// releaseAll. A genuinely-failed provision releases its key on the doProvision
	// failure path, so it has nothing to preserve.
	//
	// Race-freedom (unchanged from ENG-546): this runs while provisionsMu is held;
	// every reservation site (Provision, Restore) registers its provision entry
	// under provisionsMu BEFORE it reserves in the pool, so any reservation present
	// here is already in `final`, and a not-yet-registered Provision is blocked on
	// provisionsMu until we release.
	trackedUUIDs := make([]string, 0, len(final))
	for uuid := range final {
		trackedUUIDs = append(trackedUUIDs, uuid)
	}
	// A pre-acceptance restore can deliberately remove its volatile Provisioning
	// marker while re-quarantine/finalizer cleanup remains pending. If durable
	// failure settlement itself failed, the operation intent still names the
	// exact destination whose live pool reservation must survive this refresh.
	// Once the intent settles, the retention row's DestinationItems/Profiles is
	// sufficient to reconstruct the exact allocation without manufacturing a
	// Restart-admissible Failed projection.
	for uuid := range pendingIntentLeases {
		if _, alreadyTracked := final[uuid]; !alreadyTracked {
			trackedUUIDs = append(trackedUUIDs, uuid)
		}
	}
	if err := b.pool.ResetPreserving(allocations, func(key string) bool {
		// Allocation keys are {leaseUUID}-{service}-{index}. leaseUUID is not
		// canonicalized at ingress (IsValidUUID = uuid.Parse, config.go; accepts
		// hyphenless / urn:uuid: / braced / uppercase forms and keeps the original
		// string verbatim into the key, provision.go). The match is still
		// collision-free: no distinct valid-UUID string is a proper prefix of
		// another, and the trailing "-" delimiter guards the token boundary.
		for _, uuid := range trackedUUIDs {
			if strings.HasPrefix(key, uuid+"-") {
				return true
			}
		}
		return false
	}); err != nil {
		b.provisionsMu.Unlock()
		return fmt.Errorf("rebuild recovered resource pool: %w", err)
	}
	// Publish the provision map only after the pool accepted its matching
	// accounting snapshot. On error both authorities retain their previous
	// values, so recovery cannot expose a half-published generation.
	b.provisions = final

	// Snapshot aggregate stats from the recovered map before releasing the lock.
	// After unlock, `final` aliases `b.provisions` and concurrent goroutines
	// may modify both the map and the pointed-to provision structs.
	var readyCount float64
	totalContainers := 0
	leaseCount := len(final)
	activeTenants := make(map[string]bool, len(final))
	for _, p := range final {
		if p.Status == backend.ProvisionStatusReady {
			readyCount++
		}
		totalContainers += len(p.ContainerIDs)
		if p.Tenant != "" {
			activeTenants[p.Tenant] = true
		}
	}
	// Publish the gauge while the recovered provision snapshot is still locked.
	// A live transition that waits on provisionsMu will update its own Inc/Dec
	// only after this baseline is visible, rather than being overwritten by a
	// delayed Set after the publication locks are released.
	activeProvisions.Set(readyCount)
	b.provisionsMu.Unlock()
	b.closeSnapshotMu.Unlock()
	closeSnapshotLocked = false

	updateResourceMetrics(b.pool.Stats())
	b.refreshRetentionAccounting()

	// Resume every admitted close only after the conservative map and resource
	// reservations are visible. The snapshot writer is released before this loop:
	// resumeRecoveredClose takes the per-lease command fence and re-reads the
	// journal, so unrelated live closes remain concurrent and a stale snapshot
	// claim cannot tear down a replacement generation. A substrate or callback
	// error is deliberately non-fatal to startup:
	// resumeRecoveredClose preserves the close journal, its durable attempt
	// count, and actor-owned retry state, so the next level-triggered sweep can
	// retry without reconstructing authority from survivors.
	for _, leaseUUID := range slices.Sorted(maps.Keys(closeIntents)) {
		closeErr := b.resumeRecoveredClose(ctx, closeIntents[leaseUUID])
		if closeErr != nil {
			cleanupAttempts := closeIntents[leaseUUID].CleanupAttempts()
			if current, found, readErr := b.callbackStore.GetCloseIntent(leaseUUID); readErr == nil && found {
				cleanupAttempts = current.CleanupAttempts()
			}
			b.logger.Error("durable close recovery remains pending",
				"lease_uuid", leaseUUID,
				"cleanup_attempts", cleanupAttempts,
				"error", closeErr,
			)
			continue
		}
		b.logger.Info("durable close recovery completed", "lease_uuid", leaseUUID)
	}

	// Existing actors retain their own serialized SM state across periodic
	// recovery. Drive a typed Ready→Failed transition and wait for its reply so
	// the in-memory map, state machine, and lifecycle callback cannot diverge.
	slices.Sort(cohortTransitionLeases)
	for _, leaseUUID := range cohortTransitionLeases {
		reply := make(chan error, 1)
		if err := b.routeToLeaseBlocking(ctx, leaseUUID, leasesm.CohortDivergedMsg{
			Ctx:   ctx,
			Reply: reply,
		}); err != nil {
			return fmt.Errorf("route durable cohort divergence for lease %q: %w", leaseUUID, err)
		}
		if err := b.waitForReply(ctx, reply); err != nil {
			return fmt.Errorf("apply durable cohort divergence for lease %q: %w", leaseUUID, err)
		}
	}

	// Gather diagnostics for cold-start failures only. Ready→Failed
	// transitions (failedLeases) are handled by the SM's Failing state,
	// whose OnEntry action spawns the async diag goroutine — same code
	// path as a live container-death event.
	allFailed := append(slices.Clone(coldStartFailed), cohortFailed...)
	// failedDiag carries the gathered diagnostic together with the instance
	// identity (CreatedAt) captured at snapshot time, so the write loop below
	// can verify it is still enriching the SAME failed instance the diag was
	// gathered from (ENG-193 code-review #1).
	type failedDiag struct {
		diag      string
		createdAt time.Time
	}
	failedDiagnostics := make(map[string]failedDiag, len(allFailed))
	diagnosticCtx, cancelDiagnostics := b.recoveryDockerReadContext(ctx)
	for _, uuid := range coldStartFailed {
		b.provisionsMu.RLock()
		prov, ok := b.provisions[uuid]
		if !ok {
			b.provisionsMu.RUnlock()
			continue
		}
		containerIDs := append([]string(nil), prov.ContainerIDs...)
		// Capture the instance identity under the SAME RLock that snapshots
		// containerIDs, so the (CreatedAt, containerIDs) pair is consistent.
		createdAt := prov.CreatedAt
		b.provisionsMu.RUnlock()

		for _, cid := range containerIDs {
			state, inspErr := b.inspector.InspectInstance(diagnosticCtx, cid)
			if inspErr != nil {
				b.logger.Warn("failed to inspect container during diagnostics gathering", "lease", uuid, "container_id", leasesm.ShortID(cid), "error", inspErr)
				continue
			}
			// Mirror the "terminally gone?" decision from the SM guard:
			// PhaseExited and PhaseFailed cover the Docker statuses that
			// previously mapped to ProvisionStatusFailed in
			// containerStatusToProvisionStatus.
			if state != nil && (state.Phase == leasesm.PhaseExited || state.Phase == leasesm.PhaseFailed) {
				failedDiagnostics[uuid] = failedDiag{
					diag:      b.gatherer.GatherDiagnostics(diagnosticCtx, cid, state),
					createdAt: createdAt,
				}
				break
			}
		}
	}
	// Route the enriched LastError through the store seam (UpdateFn) so recover
	// holds no raw b.provisions field access. The Status==Failed re-check stays
	// INSIDE the closure: a concurrent Deprovision/Provision-retry/Restart that
	// took ownership during the diag window must not get its fresh LastError
	// clobbered with this failure's data (ENG-193).
	for uuid, fd := range failedDiagnostics {
		enriched := leasesm.ErrMsgContainerExited + ": " + fd.diag
		createdAt := fd.createdAt
		b.provisionStore.UpdateFn(uuid, func(p *leasesm.ProvisionState) {
			// Only enrich the SAME failed instance the diag was gathered from: a
			// Provision-retry that replaced the lease during the diag I/O window
			// gets a fresh CreatedAt, so its LastError is not clobbered with this
			// failure's data (ENG-193 code-review #1).
			if p.Status == backend.ProvisionStatusFailed && p.CreatedAt.Equal(createdAt) {
				p.LastError = enriched
				// Reason/Message carry the CURATED (tenant-safe) signal; the
				// enriched diag blob stays operator-only in LastError.
				p.Reason = backend.ReasonContainerExited
				p.Message = leasesm.ErrMsgContainerExited
			}
		})
	}

	// Snapshot diagnostics under lock, then persist outside (I/O). Same
	// Status==Failed gate so we don't persist a snapshot that aliases the new
	// owner's ContainerIDs (which would cause persistDiagnostics to fetch logs
	// from containers unrelated to the original failure).
	type diagItem struct {
		entry        shared.DiagnosticEntry
		containerIDs []string
		keys         map[string]string
	}
	diagItems := make([]diagItem, 0, len(allFailed))
	b.provisionsMu.RLock()
	for _, uuid := range allFailed {
		if prov, ok := b.provisions[uuid]; ok && prov.Status == backend.ProvisionStatusFailed {
			diagItems = append(diagItems, diagItem{
				entry:        leasesm.DiagnosticSnapshot(&prov.ProvisionState),
				containerIDs: append([]string(nil), prov.ContainerIDs...),
				keys:         leasesm.ContainerLogKeys(&prov.ProvisionState),
			})
		}
	}
	b.provisionsMu.RUnlock()
	for _, item := range diagItems {
		b.persistDiagnosticsContext(diagnosticCtx, item.entry, item.containerIDs, item.keys)
	}
	cancelDiagnostics()

	// Hand off Ready→Failed transitions to each lease's actor. The SM's
	// Ready→Failing→Failed flow gathers diagnostics via the async goroutine
	// (same code path as a live container-death event) and emits the
	// terminal Failed callback from Failed.OnEntryFrom(evDiagGathered).
	// Callback suppression on concurrent Deprovision is handled
	// structurally by Failing.OnExit.
	for _, uuid := range failedLeases {
		containerID, ok := firstExitedByLease[uuid]
		if !ok {
			// Shouldn't happen: if we detected a Ready→Failed transition,
			// some container for this lease was observed as exited.
			b.logger.Warn("ready→failed transition detected but no exited container",
				"lease_uuid", uuid)
			continue
		}
		if !b.routeToLease(uuid, leasesm.ContainerDiedMsg{ContainerID: containerID}) {
			dieEventDroppedTotal.WithLabelValues("reconcile").Inc()
			b.logger.Warn("die event dropped during reconcile dispatch; reconciler will re-detect",
				"lease_uuid", uuid, "container_id", leasesm.ShortID(containerID))
		}
	}

	stats := b.pool.Stats()
	logAttrs := []any{
		"leases", leaseCount,
		"containers", totalContainers,
		"cpu_allocated", stats.AllocatedCPU,
		"memory_allocated_mb", stats.AllocatedMemoryMB,
	}
	if skippedUnknownSKU > 0 {
		logAttrs = append(logAttrs, "untracked_unknown_sku", skippedUnknownSKU)
	}
	b.logger.Info("state recovered", logAttrs...)

	// Clean up orphaned tenant networks if network isolation is enabled
	if b.cfg.IsNetworkIsolation() {
		b.cleanupOrphanedNetworks(ctx, activeTenants)
	}

	// A restart may interrupt the grace cleanup after removing only part of a
	// legacy cohort. Resume removal only after this sweep has validated the live
	// stack against the exact durable release, and never while a write-ahead
	// operation owns the generation transition.
	for _, leaseUUID := range slices.Sorted(maps.Keys(committedPrevRemnants)) {
		if _, pending := pendingIntentLeases[leaseUUID]; pending {
			b.logger.Info("retaining migration rollback remnants while an operation is pending",
				"lease_uuid", leaseUUID,
			)
			continue
		}
		if _, diverged := cohortIssues[leaseUUID]; diverged {
			continue
		}
		cohort := committedPrevRemnants[leaseUUID]
		targets := make([]legacyRollbackCleanupTarget, 0, len(cohort.remnants))
		for _, remnant := range cohort.remnants {
			targets = append(targets, legacyRollbackCleanupTarget{
				ContainerID: remnant.ContainerID,
				Name:        remnant.Name,
			})
		}
		b.scheduleLegacyPrevCleanup(targets, b.logger.With("lease_uuid", leaseUUID))
	}

	return nil
}

// cleanupOrphanedVolumes destroys volumes on disk that have no matching provision.
// This catches volumes leaked by crashes between volume creation and container creation,
// or between container removal and volume destruction. Called once at startup after
// recoverState populates the provision map.
// activeReleaseClaimsVolume reports whether the exact canonical name appears in
// its lease's active release. The name-level check matters after a failed
// re-provision: the predecessor Release must keep its reusable volumes, but it
// must not shield fresh candidate-only volumes merely because they share a lease
// UUID. A legacy active release without item authority remains fail-safe and
// protects the whole lease namespace until migration supplies exact items.
func (b *Backend) activeReleaseClaimsVolume(volumeID string) (bool, error) {
	if b.releaseStore == nil {
		return false, nil
	}
	leaseUUID, ok := leaseUUIDFromVolumeName(volumeID)
	if !ok {
		return false, nil
	}
	rel, err := b.releaseStore.LatestActive(leaseUUID)
	if err != nil {
		// Fail safe: a transient release-store read error must NOT let the reaper
		// destroy a volume that may have an active release. Surface the failure so
		// startup cannot report readiness with an unclassified quota footprint.
		b.logger.Warn("cleanupOrphanedVolumes: release-store read failed; keeping volume (fail-safe)", "volume_id", volumeID, "error", err)
		return false, fmt.Errorf("read active release for orphan candidate %q: %w", volumeID, err)
	}
	if rel == nil {
		return false, nil
	}
	if len(rel.Items) == 0 {
		return true, nil
	}
	for _, item := range rel.Items {
		for index := range item.Quantity {
			if canonicalVolumeName(leaseUUID, item.ServiceName, index) == volumeID {
				return true, nil
			}
		}
	}
	return false, nil
}

func (b *Backend) cleanupOrphanedVolumes(ctx context.Context) error {
	volumeIDs, err := b.volumes.List()
	if err != nil {
		return fmt.Errorf("list volumes: %w", err)
	}
	if len(volumeIDs) == 0 {
		return nil
	}

	// An orphan is a volume NOTHING claims — that is the whole definition, and the
	// owner table (volume_destroy.go) is what answers it. This function used to build
	// its own parallel version of that table: live-provision canonicals plus, for every
	// retention record, the canonicals its renames may have left behind. Both halves
	// are now in one place, which matters because the second half was written months
	// after the close path's copy and the two had to agree forever (ENG-658).
	//
	// The protection it encoded is unchanged and still load-bearing: reconcileRetentions
	// runs immediately before this in Start and re-quarantines crash-stranded canonical
	// volumes back into the fred-retained- namespace, but a rename that FAILED (a real
	// error, not a benign no-op) leaves the volume canonical-named and in no live
	// provision — so an unclaimed-means-destroy loop would reap it. The table claims it,
	// via the record, exactly as the expected set did.
	//
	// Resolve up front rather than per volume: a store read error must skip the whole
	// run (we cannot tell a leak from a retained canonical we failed to protect), and
	// this way the run costs one read no matter how many volumes are on disk.
	//
	// Unlike the retention sweep, this sweep is NOT exposed to a concurrent claim
	// (checked for ENG-681): cmd/docker-backend builds and binds the HTTP server that
	// serves /provision only after Start returns, and Start runs this before it launches
	// the retention reaper, the reconcile loop and the event loop — so no goroutine that
	// could publish a provision is running yet. It is covered by the choke point's
	// destroy-time re-check anyway, at the cost of an uncontended mutex per orphan;
	// depending on that ordering rather than on the lock would be depending on it
	// forever.
	op := b.volumeOp("", b.logger)
	table, err := op.claims()
	if err != nil {
		// Count it, and at the same per-volume granularity every other site uses: this
		// is the documented ticketing signal, and the sweep bails before reaching
		// op.destroy, so nothing else would report it. The count is the volumes whose
		// fate this run could not decide — every non-retained name on disk.
		undecided := 0
		for _, id := range volumeIDs {
			if !isRetainedVolume(id) {
				undecided++
			}
		}
		volumeDestroyRefusedTotal.WithLabelValues(destroySiteOrphanGC, destroyRefusedUnreadable).Add(float64(undecided))
		b.logger.Error("cleanupOrphanedVolumes: ownership unresolvable; skipping orphan destruction this run (fail-safe)",
			"undecided_volumes", undecided, "error", err)
		return fmt.Errorf("resolve orphan volume ownership: %w", err)
	}

	candidates := make([]string, 0, len(volumeIDs))
	for _, id := range volumeIDs {
		if isRetainedVolume(id) {
			// A name property, not a claim: the retained namespace is where a closed
			// lease's data deliberately lives, and the retention sweep owns reaping it.
			continue
		}
		if _, unclaimed := table.mayDestroy(id, ""); !unclaimed {
			// Claimed by a live provision or a retention record — i.e. every healthy
			// lease's volume, on every boot. Filtered QUIETLY and before the release
			// probe: this is the ordinary case, not a refusal, and neither a log line,
			// a counter, nor a release-store read per live volume is warranted. That is
			// why this site emits no reason="claimed" series; the docs say so rather
			// than implying otherwise. The destroy below re-checks against the same
			// table regardless, so the guard does not depend on this filter being
			// right — only the noise does.
			continue
		}
		hasActiveRelease, releaseErr := b.activeReleaseClaimsVolume(id)
		if releaseErr != nil {
			return releaseErr
		}
		if hasActiveRelease {
			// A successfully-provisioned lease keeps an active release until it is
			// cleanly deprovisioned, so a volume whose lease still has one is not a
			// create-crash leak — its containers were merely removed out-of-band
			// (e.g. an operator `docker prune`). Reaping it would silently destroy
			// retained tenant data (ENG-505). Over-keeping a stale volume is the safe
			// direction here; a genuine leak has no release.
			//
			// Deliberately NOT folded into the owner table: this is a per-name RELEASE
			// -store read, not a claim, and a record's absence there is meaningful. Fold
			// it in and a give-up tombstone whose purgeReleaseHistory failed would become
			// permanently unreapable instead of self-healing.
			b.logger.Warn("cleanupOrphanedVolumes: active release still claims volume; not reaping it", "volume_id", id)
			continue
		}
		candidates = append(candidates, id)
	}

	rep := op.destroy(ctx, destroySiteOrphanGC, candidates...)
	for _, id := range rep.Destroyed {
		b.logger.Info("destroyed orphaned volume", "volume_id", id)
	}
	if len(rep.Destroyed) > 0 || len(rep.Errs) > 0 || rep.refused() > 0 {
		b.logger.Info("orphaned volume cleanup complete",
			"destroyed", len(rep.Destroyed), "failed", len(rep.Errs), "claimed", len(rep.Claimed))
	}
	if err := rep.err(); err != nil {
		return fmt.Errorf("destroy orphaned volumes: %w", err)
	}
	return nil
}

// cleanupOrphanedNetworks removes managed networks whose tenant has no active provisions.
// activeTenants is a cheap precheck to skip tenants obviously still in use;
// releaseTenantNetwork re-validates against a live b.provisions scan under
// the per-tenant mutex, so this path is safe against a Provision() arriving
// concurrently with reconcile.
func (b *Backend) cleanupOrphanedNetworks(ctx context.Context, activeTenants map[string]bool) {
	phaseCtx, cancelPhase := b.recoveryDockerReadContext(ctx)
	defer cancelPhase()
	networks, err := b.docker.ListManagedNetworks(phaseCtx)
	if err != nil {
		b.logger.Warn("failed to list managed networks for cleanup", "error", err)
		return
	}

	for _, n := range networks {
		if phaseCtx.Err() != nil {
			b.logger.Warn("managed network cleanup budget exhausted", "error", phaseCtx.Err())
			return
		}
		tenant := n.Labels[LabelTenant]
		if tenant != "" && !activeTenants[tenant] && len(n.Containers) == 0 {
			err := b.releaseTenantNetwork(phaseCtx, tenant)
			if err != nil {
				b.logger.Warn("failed to remove orphaned network", "network", n.Name, "error", err)
			} else {
				b.logger.Info("removed orphaned tenant network", "network", n.Name, "tenant", tenant)
			}
		}
	}
}

// reconcileLoop periodically reconciles the in-memory state with Docker.
// Note: WaitGroup.Done is handled by the caller via wg.Go() (Go 1.25+).
func (b *Backend) reconcileLoop() {
	ticker := time.NewTicker(b.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := b.recoverState(ctx); err != nil {
					b.logger.Error("reconciliation failed", "error", err)
					reconciliationTotal.WithLabelValues("error").Inc()
				} else {
					reconciliationTotal.WithLabelValues("success").Inc()
					reconcilerLastSuccessTimestamp.SetToCurrentTime()
				}
			}()
		}
	}
}

// containerEventLoop subscribes to Docker container "die" events and triggers
// immediate failure handling. This provides near-instant detection of container
// crashes, complementing the 5-minute reconcileLoop safety net.
func (b *Backend) containerEventLoop() {
	for {
		select {
		case <-b.stopCtx.Done():
			return
		default:
		}
		if err := b.requireStorageIdentity(b.stopCtx); err != nil {
			b.logger.Error("container event listener stopped by backend identity verification", "error", err)
			return
		}

		eventCh, errCh := b.docker.ContainerEvents(b.stopCtx)

	consume:
		for {
			select {
			case <-b.stopCtx.Done():
				return
			case event, ok := <-eventCh:
				if !ok {
					break consume
				}
				if event.Action == "die" {
					if err := b.requireStorageIdentity(b.stopCtx); err != nil {
						b.logger.Error("container event ignored after backend identity verification failed", "error", err)
						return
					}
					if leaseUUID, found := b.findLeaseByContainerID(event.ContainerID); found {
						if !b.routeToLease(leaseUUID, leasesm.ContainerDiedMsg{ContainerID: event.ContainerID}) {
							dieEventDroppedTotal.WithLabelValues("event_loop").Inc()
							b.logger.Warn("die event dropped at event loop dispatch; reconciler will re-detect",
								"lease_uuid", leaseUUID, "container_id", leasesm.ShortID(event.ContainerID))
						}
					}
				}
			case err, ok := <-errCh:
				if !ok {
					break consume
				}
				b.logger.Warn("container event stream error, reconnecting", "error", err)
				break consume
			}
		}

		// Backoff before reconnecting to avoid tight loop on persistent errors.
		select {
		case <-b.stopCtx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// findLeaseByContainerID returns the lease UUID and true if a provision
// containing the given container ID is found. Returns ("", false) otherwise.
// Called under no lock; acquires read lock internally.
//
// O(N*M) linear scan over all leases and their containers. A reverse index
// would be O(1) but adds sync overhead across provision/deprovision/restart/
// update/recover. Fine at expected scale (hundreds of leases, 1-10 containers).
func (b *Backend) findLeaseByContainerID(containerID string) (string, bool) {
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()

	for uuid, prov := range b.provisions {
		for _, cid := range prov.ContainerIDs {
			if cid == containerID {
				return uuid, true
			}
		}
	}
	return "", false
}
