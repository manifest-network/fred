package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// operationSubstrate is the complete physical workflow for one exact opaque
// operation subject. The subject and its storageMutations never escape the
// builder: the generic executor can invoke this capability, but cannot select
// a lease, operation, project, or mutation after construction.
type operationSubstrate func(context.Context) error

func buildOperationSubstrate(
	b *Backend,
	ops storageMutationOperations,
) func(substratemutation.Runner, shared.OperationPhysicalSubject) operationSubstrate {
	return func(runner substratemutation.Runner, subject shared.OperationPhysicalSubject) operationSubstrate {
		mutations := newOperationStorageMutations(runner, subject, ops)
		return func(ctx context.Context) error {
			if receipt, historical := subject.FailedReceiptCleanup(); historical {
				return b.doFailedOperationReceiptCleanup(mutations, ctx, subject, receipt)
			}
			intent := subject.Intent()
			if subject.RecoveryCleanup() {
				return b.doOperationRecoveryCleanup(mutations, ctx, subject)
			}
			switch intent.Kind() {
			case shared.OperationIntentProvision:
				stack, err := manifest.ParsePayload(intent.Manifest())
				if err != nil {
					return fmt.Errorf("parse Started provision manifest: %w", err)
				}
				req := backend.ProvisionRequest{
					LeaseUUID: intent.LeaseUUID(), Tenant: intent.Tenant(),
					ProviderUUID: intent.ProviderUUID(), Items: intent.EffectiveItems(),
					CallbackURL: intent.CallbackURL(), LifecycleCallbackURL: intent.LifecycleCallbackURL(),
					Payload: intent.Manifest(),
				}
				return b.doProvisionPhysical(
					mutations, ctx, req, stack, intent.ResourceProfiles(),
					b.logger.With(
						"lease_uuid", intent.LeaseUUID(),
						"operation_fingerprint", intent.OperationID().Fingerprint(),
					),
				)
			case shared.OperationIntentRestore:
				return b.doRestorePhysical(mutations, ctx, subject)
			default:
				return fmt.Errorf("unsupported operation kind %q", intent.Kind())
			}
		}
	}
}

func runOperationSubstrate(
	ctx context.Context,
	capability operationSubstrate,
	_ shared.OperationPhysicalSubject,
) error {
	if capability == nil {
		return errors.New("operation substrate capability is unavailable")
	}
	return capability(ctx)
}

// maintenanceSubstrate is a target-free invocation capability. All authority
// and every concrete target are fixed by buildMaintenanceSubstrate.
type maintenanceSubstrate func(context.Context) error

func buildMaintenanceSubstrate(
	b *Backend,
	ops storageMutationOperations,
) func(substratemutation.Runner, shared.MaintenancePhysicalSubject) maintenanceSubstrate {
	return func(runner substratemutation.Runner, subject shared.MaintenancePhysicalSubject) maintenanceSubstrate {
		mutations := newMaintenanceStorageMutations(runner, subject, ops)
		return func(ctx context.Context) error {
			if receipt, historical := subject.FailedReceiptCleanup(); historical {
				return b.doFailedMaintenanceReceiptCleanup(mutations, ctx, subject, receipt)
			}
			if subject.RecoveryCleanup() {
				return b.doMaintenanceRecoveryCleanup(mutations, ctx, subject)
			}
			return b.doMaintenancePhysical(mutations, ctx, subject)
		}
	}
}

func runMaintenanceSubstrate(
	ctx context.Context,
	capability maintenanceSubstrate,
	_ shared.MaintenancePhysicalSubject,
) error {
	if capability == nil {
		return errors.New("maintenance substrate capability is unavailable")
	}
	return capability(ctx)
}

func (b *Backend) classifyOperationPhysical(
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
) (shared.OperationPhysicalEvidence, error) {
	if !subject.Valid() {
		return shared.OperationPhysicalEvidence{}, errors.New("operation physical subject is invalid")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return shared.OperationPhysicalEvidence{}, err
	}
	if receipt, historical := subject.FailedReceiptCleanup(); historical {
		fence, err := newFailedOperationRecoveryFence(nil, []shared.FailedOperationReceipt{receipt})
		if err != nil {
			return shared.OperationPhysicalEvidence{}, err
		}
		targets, err := fence.targets(all)
		if err != nil {
			return shared.OperationPhysicalEvidence{}, err
		}
		if len(targets) != 0 {
			return shared.OperationPhysicalEvidence{}, errors.New("failed-operation receipt substrate remains")
		}
		return shared.NewOperationFailedReceiptAbsent(subject)
	}
	classification, err := b.classifyOperationIntentSubstrate(ctx, subject.Intent(), all)
	if err != nil {
		return shared.OperationPhysicalEvidence{}, err
	}
	switch {
	case classification.status == backend.CallbackStatusSuccess:
		return shared.NewOperationTargetReady(
			subject, classification.currentIDs, classification.serviceContainers,
		)
	case len(classification.currentIDs) == 0 &&
		(subject.RecoveryCleanup() || classification.waitEvidence == nil ||
			b.operationAbsenceObservationIsTerminal(subject)):
		absent, absentErr := b.operationStorageExactlyAbsent(ctx, subject)
		if absentErr != nil {
			return shared.OperationPhysicalEvidence{}, absentErr
		}
		if !absent {
			return shared.OperationPhysicalEvidence{}, errors.New("operation containers are absent but operation-owned volume state remains")
		}
		return shared.NewOperationExactAbsent(subject)
	case len(classification.currentIDs) == 0:
		return shared.OperationPhysicalEvidence{}, errors.New("operation substrate absence is not yet causally stable")
	default:
		return shared.OperationPhysicalEvidence{}, errors.New("operation substrate is not an exact Ready or Absent cohort")
	}
}

// operationAbsenceObservationIsTerminal determines whether this strict empty
// inventory is the final causal observation rather than the first view after a
// possibly outstanding Docker request. Only the durable operation's full
// visibility window supplies that bound: a Failed projection is observational
// state, not proof that the exact operation's worker is quiescent. Live execution
// cannot exploit that horizon to launder an uncertain mutation: Guard returns
// Ambiguous whenever its workflow or any Step failed, irrespective of classifier
// evidence.
func (b *Backend) operationAbsenceObservationIsTerminal(
	subject shared.OperationPhysicalSubject,
) bool {
	claim := subject.Intent()
	if !claim.Valid() {
		return false
	}
	timeout := b.cfg.ProvisionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	now := time.Now()
	return !now.Before(provisionIntentRecoveryDeadline(claim.CreatedAt(), now, timeout))
}

// operationStorageExactlyAbsent extends container absence to the managed
// volume namespace. Provision may fail terminally only when no canonical or
// retained name for its exact lease remains. Restore additionally requires
// every source volume to remain in the retained namespace; a crash after even
// one retained->canonical rename therefore stays ambiguous until recovery
// rolls the adoption back or completes it.
//
// Tenant networks are intentionally outside this operation-owned absence
// proof. They are shared, idempotent infrastructure for every lease of the
// tenant, carry no operation generation, and are removed only by the separate
// remove-if-empty lifecycle path. Creating one is nevertheless a Step: an
// uncertain Docker result still makes the current execution ambiguous.
func (b *Backend) operationStorageExactlyAbsent(
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
) (bool, error) {
	volumes, err := b.volumes.ListForProof(ctx)
	if err != nil {
		return false, fmt.Errorf("list managed volumes for operation absence: %w", err)
	}
	seen := make(map[string]struct{}, len(volumes))
	canonicalPrefix := leaseVolumePrefix(subject.LeaseUUID())
	retainedPrefix := retainedVolumePrefix + subject.LeaseUUID() + "-"
	preservedPredecessor := make(map[string]struct{})
	if predecessor, ok := subject.PredecessorRelease(); ok {
		preservedPredecessor = releaseCanonicalVolumeNames(subject.LeaseUUID(), predecessor)
	}
	for _, name := range volumes {
		seen[name] = struct{}{}
		if strings.HasPrefix(name, retainedPrefix) {
			return false, nil
		}
		if strings.HasPrefix(name, canonicalPrefix) {
			if _, preserved := preservedPredecessor[name]; !preserved {
				return false, nil
			}
		}
	}
	intent := subject.Intent()
	if !intent.Valid() || intent.Kind() != shared.OperationIntentRestore {
		return true, nil
	}
	record, err := b.retentionStore.Get(intent.SourceLeaseUUID())
	if err != nil {
		return false, fmt.Errorf("read restore source for absence proof: %w", err)
	}
	if record == nil || record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != subject.LeaseUUID() || record.Generation != intent.SourceGeneration() {
		return false, nil
	}
	for _, retained := range record.RetainedVolumeNames {
		if _, ok := seen[retained]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func releaseCanonicalVolumeNames(leaseUUID string, release shared.Release) map[string]struct{} {
	names := make(map[string]struct{})
	for _, item := range release.Items {
		for index := range item.Quantity {
			names[canonicalVolumeName(leaseUUID, item.ServiceName, index)] = struct{}{}
		}
	}
	return names
}

func physicalProjection(containers []ContainerInfo) ([]string, map[string][]string) {
	ids := make([]string, 0, len(containers))
	services := make(map[string][]string)
	for _, container := range containers {
		ids = append(ids, container.ContainerID)
		services[container.ServiceName] = append(services[container.ServiceName], container.ContainerID)
	}
	slices.Sort(ids)
	for service := range services {
		slices.Sort(services[service])
	}
	return ids, services
}

// classifyMaintenancePhysical is the one construction-bound exhaustive
// classifier shared by live execution and cold recovery. A Ready result is
// minted only from the complete lease cohort: every container must carry the
// backend, tenant/provider, callback, topology and maintenance-generation
// identity of the exact target (or source), and every instance must be running
// and pass its release's startup contract. An empty cohort, a complete source,
// and a non-empty divergent cohort are represented by disjoint evidence types.
func (b *Backend) classifyMaintenancePhysical(
	ctx context.Context,
	subject shared.MaintenancePhysicalSubject,
) (shared.MaintenancePhysicalEvidence, error) {
	if !subject.Valid() {
		return shared.MaintenancePhysicalEvidence{}, errors.New("maintenance physical subject is invalid")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return shared.MaintenancePhysicalEvidence{}, err
	}
	if receipt, historical := subject.FailedReceiptCleanup(); historical {
		targets, err := b.failedMaintenanceReceiptTargets(receipt, all)
		if err != nil {
			return shared.MaintenancePhysicalEvidence{}, err
		}
		if len(targets) != 0 {
			return shared.MaintenancePhysicalEvidence{}, errors.New(
				"failed-maintenance receipt substrate remains",
			)
		}
		return shared.NewMaintenanceFailedReceiptAbsent(subject)
	}
	leaseContainers := make([]ContainerInfo, 0)
	for _, container := range all {
		if container.LeaseUUID != subject.LeaseUUID() {
			continue
		}
		if container.BackendName != b.cfg.Name {
			return shared.MaintenancePhysicalEvidence{}, fmt.Errorf("container %q backend identity differs from this backend", container.ContainerID)
		}
		leaseContainers = append(leaseContainers, container)
	}
	if len(leaseContainers) == 0 {
		return shared.NewMaintenanceTargetAbsent(subject)
	}

	target, targetOK := subject.TargetRelease()
	source, sourceOK := subject.SourceRelease()
	if !targetOK || !sourceOK {
		return shared.MaintenancePhysicalEvidence{}, errors.New("maintenance physical subject has incomplete release authority")
	}
	allGeneration := func(id shared.MaintenanceID) bool {
		for _, container := range leaseContainers {
			if container.MaintenanceID != id {
				return false
			}
		}
		return true
	}
	ids, services := physicalProjection(leaseContainers)
	if allGeneration(subject.MaintenanceID()) &&
		validateRecoveredReleaseCohort(&target, leaseContainers) == nil {
		readiness, readyErr := b.classifyRecoveredMaintenanceReadiness(ctx, target, leaseContainers)
		if readyErr != nil {
			return shared.MaintenancePhysicalEvidence{}, readyErr
		}
		if readiness == maintenanceReadinessReady {
			return shared.NewMaintenanceTargetReady(subject, ids, services)
		}
		// Exact target topology which is observably unready remains ambiguous:
		// it is neither a complete source nor a structurally divergent target.
		return shared.MaintenancePhysicalEvidence{}, errors.New("maintenance target cohort is not ready")
	}
	if allGeneration(source.MaintenanceID) &&
		validateRecoveredReleaseCohort(&source, leaseContainers) == nil {
		readiness, readyErr := b.classifyRecoveredMaintenanceReadiness(ctx, source, leaseContainers)
		if readyErr != nil {
			return shared.MaintenancePhysicalEvidence{}, readyErr
		}
		if readiness == maintenanceReadinessReady {
			return shared.NewMaintenanceSourceReady(subject, ids, services)
		}
		return shared.MaintenancePhysicalEvidence{}, errors.New("maintenance source cohort is not ready")
	}
	return shared.NewMaintenanceTargetDivergent(subject, ids, services)
}

func operationFailureDetails(err error) (string, backend.Reason) {
	var physical *physicalOperationError
	if errors.As(err, &physical) {
		return physical.callback, physical.reason
	}
	return leasesm.ErrMsgInternal, backend.ReasonInternal
}

func (b *Backend) executeProvisionWork(
	ctx context.Context,
	admission shared.ProvisionResourceExecution,
) leasesm.ProvisionWorkOutcome {
	claim := admission.Operation()
	if !admission.Valid() {
		return mustProvisionAmbiguous(errors.New("provision execution requires consumed resource admission"), claim)
	}
	candidate, err := b.operationSettlement.PrepareOperationRelease(claim)
	if err != nil {
		return mustProvisionAmbiguous(err, claim)
	}
	execution, err := b.operationSettlement.StartOperationExecution(candidate)
	if err != nil {
		return mustProvisionAmbiguous(err, claim)
	}
	switch outcome := b.operationSettlement.ExecuteOperation(ctx, execution).(type) {
	case shared.OperationExecutionSuccess:
		committed, err := b.operationSettlement.CommitOperationSuccess(outcome)
		if err != nil {
			return mustProvisionAmbiguous(err, claim)
		}
		if err := admission.CompleteSuccess(committed); err != nil {
			return mustProvisionAmbiguous(err, claim)
		}
		result, err := leasesm.NewProvisionWorkSuccess(committed)
		if err != nil {
			return mustProvisionAmbiguous(err, claim)
		}
		return result
	case shared.OperationExecutionFailure:
		proof, err := b.operationSettlement.CommitOperationFailure(outcome)
		if err != nil {
			return mustProvisionAmbiguous(err, claim)
		}
		if err := admission.CompleteFailure(proof); err != nil {
			return mustProvisionAmbiguous(err, claim)
		}
		cause := outcome.Cause()
		if cause == nil {
			cause = errors.New("provision substrate is exactly absent")
		}
		callbackErr, reason := operationFailureDetails(cause)
		result, err := leasesm.NewProvisionWorkFailure(cause, callbackErr, reason, nil, proof)
		if err != nil {
			return mustProvisionAmbiguous(err, claim)
		}
		return result
	case shared.OperationExecutionAmbiguous:
		return mustProvisionAmbiguous(outcome.Cause(), claim)
	default:
		return mustProvisionAmbiguous(fmt.Errorf("unknown operation outcome %T", outcome), claim)
	}
}

func mustProvisionAmbiguous(err error, claim shared.OperationIntentClaim) leasesm.ProvisionWorkOutcome {
	if err == nil {
		err = errors.New("operation outcome is ambiguous")
	}
	outcome, buildErr := leasesm.NewProvisionWorkAmbiguous(err, claim)
	if buildErr != nil {
		panic(buildErr)
	}
	return outcome
}

func (b *Backend) executeRestoreWork(
	ctx context.Context,
	claim shared.OperationIntentClaim,
) leasesm.ReplaceWorkOutcome {
	return b.executeRestorePhysicalOutcome(ctx, claim)
}

func (b *Backend) executeMaintenanceWork(
	ctx context.Context,
	target shared.MaintenanceReleaseClaim,
) leasesm.ReplaceWorkOutcome {
	return b.executeMaintenancePhysicalOutcome(ctx, target)
}

// doOperationRecoveryCleanup is selected only by the construction-bound
// executor when OperationSettlement mints a recovery-cleanup subject from an
// exact current Started intent. It derives every destructive target from that
// subject and fresh strict inventory; callers cannot provide IDs or names.
func (b *Backend) doOperationRecoveryCleanup(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
) error {
	intent := subject.Intent()
	if mutations == nil || !intent.Valid() || !subject.RecoveryCleanup() {
		return errors.New("started operation recovery-cleanup authority is invalid")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return fmt.Errorf("inventory recovered operation cleanup: %w", err)
	}
	ids, err := b.exactRecoveredOperationCleanupIDs(ctx, intent, all)
	if err != nil {
		return fmt.Errorf("derive exact recovered operation cleanup: %w", err)
	}
	teardownOp := teardownOpProvisionCleanup
	if intent.Kind() == shared.OperationIntentRestore {
		teardownOp = teardownOpRestoreReconcile
	}
	remaining, err := b.teardownLeaseContainersWith(
		mutations, ctx, subject.LeaseUUID(), ids, 10*time.Second,
		teardownOp, physicalLogger(b, subject.LeaseUUID()),
	)
	if err != nil || len(remaining) != 0 {
		return fmt.Errorf("cleanup recovered operation containers: %w",
			errors.Join(err, fmt.Errorf("%d container(s) may remain", len(remaining))))
	}
	// Compose Down snapshots a project inside the daemon. A Create accepted by
	// the previous process may become visible after that snapshot, so its nil
	// result is not exhaustive absence. Re-list under the same storage lineage
	// and remove only identities derived from this exact opaque subject. Repeat
	// until an empty observation follows the last removal; a continuously
	// appearing cohort remains ambiguous and keeps the durable intent/receipt.
	for observation := range 3 {
		fresh, inventoryErr := b.strictIdentityBoundOperationInventory(ctx)
		if inventoryErr != nil {
			return fmt.Errorf("confirm recovered operation cleanup: %w", inventoryErr)
		}
		lateIDs, deriveErr := b.exactRecoveredOperationCleanupIDs(ctx, intent, fresh)
		if deriveErr != nil {
			return fmt.Errorf("derive late recovered operation cleanup: %w", deriveErr)
		}
		if len(lateIDs) == 0 {
			break
		}
		if observation == 2 {
			return fmt.Errorf("recovered operation substrate remained after two exact cleanup passes")
		}
		for _, containerID := range lateIDs {
			if err := mutations.removeContainer(ctx, containerID); err != nil {
				return fmt.Errorf("remove late recovered operation container %q: %w", containerID, err)
			}
		}
	}

	switch intent.Kind() {
	case shared.OperationIntentRestore:
		return b.rollbackRecoveredRestoreVolumes(mutations, ctx, subject)
	case shared.OperationIntentProvision:
		return b.cleanupRecoveredProvisionVolumes(mutations, ctx, subject)
	default:
		return fmt.Errorf("unsupported recovered operation kind %q", intent.Kind())
	}
}

func (b *Backend) doFailedOperationReceiptCleanup(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
	receipt shared.FailedOperationReceipt,
) error {
	bound, ok := subject.FailedReceiptCleanup()
	if mutations == nil || !ok || bound.OperationID() != receipt.OperationID() ||
		bound.LeaseUUID() != receipt.LeaseUUID() {
		return errors.New("failed-operation receipt cleanup authority is invalid")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return fmt.Errorf("inventory failed-operation late substrate: %w", err)
	}
	fence, err := newFailedOperationRecoveryFence(nil, []shared.FailedOperationReceipt{receipt})
	if err != nil {
		return err
	}
	targets, err := fence.targets(all)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := mutations.removeContainer(ctx, target.containerID); err != nil {
			return fmt.Errorf("remove late failed-operation container %q: %w", target.containerID, err)
		}
	}
	return nil
}

func (b *Backend) cleanupRecoveredProvisionVolumes(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
) error {
	volumes, err := b.volumes.ListForProof(ctx)
	if err != nil {
		return fmt.Errorf("list recovered provision volumes: %w", err)
	}
	preserve := make(map[string]struct{})
	if predecessor, ok := subject.PredecessorRelease(); ok {
		preserve = releaseCanonicalVolumeNames(subject.LeaseUUID(), predecessor)
	}
	prefix := leaseVolumePrefix(subject.LeaseUUID())
	for _, name := range volumes {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if _, keep := preserve[name]; keep {
			continue
		}
		if err := mutations.destroyVolume(ctx, name); err != nil {
			return fmt.Errorf("destroy recovered provision volume %q: %w", name, err)
		}
	}
	return nil
}

func (b *Backend) rollbackRecoveredRestoreVolumes(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
) error {
	intent := subject.Intent()
	record, err := b.retentionStore.Get(intent.SourceLeaseUUID())
	if err != nil {
		return fmt.Errorf("read recovered restore source: %w", err)
	}
	if record == nil || record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != subject.LeaseUUID() ||
		record.Generation != intent.SourceGeneration() {
		return errors.New("recovered restore source authority changed before cleanup")
	}
	volumes, err := b.volumes.ListForProof(ctx)
	if err != nil {
		return fmt.Errorf("list recovered restore volumes: %w", err)
	}
	present := make(map[string]struct{}, len(volumes))
	for _, name := range volumes {
		present[name] = struct{}{}
	}
	for _, retained := range record.RetainedVolumeNames {
		canonical := retainedToNewCanonical(retained, record.OriginalLeaseUUID, subject.LeaseUUID())
		_, hasRetained := present[retained]
		_, hasCanonical := present[canonical]
		switch {
		case hasRetained && hasCanonical:
			return fmt.Errorf("restore volume exists in both retained and canonical namespaces: %q", retained)
		case hasCanonical:
			if err := mutations.renameVolume(ctx, canonical, retained); err != nil {
				return fmt.Errorf("re-quarantine recovered restore volume %q: %w", canonical, err)
			}
		case !hasRetained:
			return fmt.Errorf("restore volume is absent from both namespaces: %q", retained)
		}
	}
	return nil
}

// doMaintenanceRecoveryCleanup removes only the exact target generation
// selected by a durable Started maintenance subject. Source-generation and
// foreign-lease containers cannot pass storageMutations.requireContainer.
func (b *Backend) doMaintenanceRecoveryCleanup(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.MaintenancePhysicalSubject,
) error {
	intent := subject.Intent()
	target, ok := subject.TargetRelease()
	if mutations == nil || !subject.RecoveryCleanup() || !intent.Valid() || !ok {
		return errors.New("started maintenance recovery-cleanup authority is invalid")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return fmt.Errorf("inventory maintenance cleanup: %w", err)
	}
	targets, _, err := maintenanceTargetContainers(intent, all)
	if err != nil {
		return err
	}
	for _, candidate := range targets {
		if err := b.validateMaintenanceTargetContainer(intent, target, candidate); err != nil {
			return fmt.Errorf("refuse ambiguous maintenance cleanup: %w", err)
		}
		if err := mutations.removeContainer(ctx, candidate.ContainerID); err != nil {
			return fmt.Errorf("remove exact maintenance target %q: %w", candidate.ContainerID, err)
		}
	}
	return nil
}

func (b *Backend) doFailedMaintenanceReceiptCleanup(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.MaintenancePhysicalSubject,
	receipt shared.FailedMaintenanceReceipt,
) error {
	bound, ok := subject.FailedReceiptCleanup()
	if mutations == nil || !ok || !bound.Valid() ||
		bound.MaintenanceID() != receipt.MaintenanceID() ||
		bound.LeaseUUID() != receipt.LeaseUUID() {
		return errors.New("failed-maintenance receipt cleanup authority is invalid")
	}
	for observation := range 3 {
		all, err := b.strictIdentityBoundOperationInventory(ctx)
		if err != nil {
			return fmt.Errorf("inventory failed-maintenance cleanup: %w", err)
		}
		targets, err := b.failedMaintenanceReceiptTargets(receipt, all)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			return nil
		}
		if observation == 2 {
			return errors.New("failed-maintenance substrate remained after two exact cleanup passes")
		}
		for _, target := range targets {
			if err := mutations.removeContainer(ctx, target.ContainerID); err != nil {
				return fmt.Errorf("remove late failed-maintenance container %q: %w", target.ContainerID, err)
			}
		}
	}
	return nil
}

func (b *Backend) failedMaintenanceReceiptTargets(
	receipt shared.FailedMaintenanceReceipt,
	containers []ContainerInfo,
) ([]ContainerInfo, error) {
	if !receipt.Valid() || receipt.Backend() != b.Name() ||
		receipt.BackendStorageID() != b.storageIdentity {
		return nil, errors.New("failed-maintenance receipt belongs to another backend storage lineage")
	}
	target, ok := receipt.TargetRelease()
	if !ok {
		return nil, errors.New("failed-maintenance receipt has no exact target release")
	}
	targets := make([]ContainerInfo, 0)
	for _, container := range containers {
		if container.MaintenanceID == receipt.MaintenanceID() &&
			container.LeaseUUID != receipt.LeaseUUID() {
			return nil, fmt.Errorf("failed maintenance ID %s is attached to foreign lease %q",
				receipt.MaintenanceID(), container.LeaseUUID)
		}
		if container.LeaseUUID != receipt.LeaseUUID() ||
			container.MaintenanceID != receipt.MaintenanceID() {
			continue
		}
		if err := validateMaintenanceGenerationContainer(
			receipt.LeaseUUID(), receipt.MaintenanceID(), receipt.Backend(), target, container,
		); err != nil {
			return nil, fmt.Errorf("refuse ambiguous failed-maintenance cleanup: %w", err)
		}
		targets = append(targets, container)
	}
	slices.SortFunc(targets, compareContainerIdentity)
	return targets, nil
}

func physicalLogger(b *Backend, leaseUUID string) *slog.Logger {
	return b.logger.With("lease_uuid", leaseUUID)
}

func (b *Backend) doRestorePhysical(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
) error {
	intent := subject.Intent()
	if mutations == nil || !intent.Valid() || intent.Kind() != shared.OperationIntentRestore {
		return errors.New("started restore authority is invalid")
	}
	record, err := b.retentionStore.Get(intent.SourceLeaseUUID())
	if err != nil {
		return fmt.Errorf("read Started restore source: %w", err)
	}
	if record == nil || record.Status != shared.RetentionStatusRestoring ||
		record.NewLeaseUUID != subject.LeaseUUID() ||
		record.Generation != intent.SourceGeneration() ||
		record.Tenant != intent.Tenant() || record.ProviderUUID != intent.ProviderUUID() {
		return errors.New("started restore source authority changed before physical execution")
	}
	adoptStart := time.Now()
	for _, retained := range record.RetainedVolumeNames {
		canonical := retainedToNewCanonical(retained, record.OriginalLeaseUUID, subject.LeaseUUID())
		if err := mutations.renameVolume(ctx, retained, canonical); err != nil {
			return fmt.Errorf("adopt retained volume %q: %w", retained, err)
		}
	}
	replacePhaseDurationSeconds.WithLabelValues("restore", phaseAdopt).Observe(time.Since(adoptStart).Seconds())
	return b.doReplacePhysical(ctx, mutations, replaceContainersOp{
		LeaseUUID: subject.LeaseUUID(), Stack: record.StackManifest,
		Items: intent.EffectiveItems(), ResourceProfiles: intent.ResourceProfiles(),
		Operation: "restore", CallbackURL: intent.CallbackURL(),
		LifecycleCallbackURL: intent.LifecycleCallbackURL(), NoComposeRollback: true,
		Logger: physicalLogger(b, subject.LeaseUUID()),
	})
}

func (b *Backend) doMaintenancePhysical(
	mutations *storageMutations,
	ctx context.Context,
	subject shared.MaintenancePhysicalSubject,
) error {
	intent := subject.Intent()
	target, ok := subject.TargetRelease()
	if mutations == nil || !intent.Valid() || !ok {
		return errors.New("started maintenance authority is invalid")
	}
	stack, err := manifest.ParsePayload(target.Manifest)
	if err != nil {
		return fmt.Errorf("parse Started maintenance manifest: %w", err)
	}
	operation := string(intent.Kind())
	if intent.Kind() == shared.MaintenanceIntentUpdate {
		pulled := make(map[string]struct{}, len(stack.Services))
		for service, spec := range stack.Services {
			if _, exists := pulled[spec.Image]; exists {
				continue
			}
			if err := mutations.pullImage(ctx, spec.Image, b.cfg.ImagePullTimeout); err != nil {
				return &physicalOperationError{
					callback: backend.MsgImagePullFailed,
					reason:   backend.ReasonImagePullFailed,
					cause:    fmt.Errorf("pull update image for service %q: %w", service, err),
				}
			}
			pulled[spec.Image] = struct{}{}
		}
	}
	return b.doReplacePhysical(ctx, mutations, replaceContainersOp{
		LeaseUUID: subject.LeaseUUID(), Stack: stack, Items: target.Items,
		ResourceProfiles: target.ResourceProfiles, Operation: operation,
		CallbackURL: intent.CallbackURL(), LifecycleCallbackURL: intent.LifecycleCallbackURL(),
		Maintenance: intent, TargetMaintenanceID: subject.MaintenanceID(),
		Logger: physicalLogger(b, subject.LeaseUUID()),
	})
}

// doReplacePhysical contains only substrate work. It cannot activate/fail a
// release or construct an actor result; the fixed strict classifier and the
// settlement consume those responsibilities after this function returns.
func (b *Backend) doReplacePhysical(
	ctx context.Context,
	mutations *storageMutations,
	op replaceContainersOp,
) error {
	if mutations == nil || op.Stack == nil || op.LeaseUUID != mutations.leaseUUID {
		return errors.New("replacement target differs from Started subject")
	}
	profiles, err := resourceProfileMap(op.Items, op.ResourceProfiles)
	if err != nil {
		return fmt.Errorf("validate %s resource profiles: %w", op.Operation, err)
	}
	imageSetupStartedAt := time.Now()
	imageSetups := make(map[string]*imageSetup, len(op.Stack.Services))
	for service, spec := range op.Stack.Services {
		setup, err := b.inspectImageForSetup(mutations, ctx, spec.Image, spec.User)
		if err != nil {
			return fmt.Errorf("inspect %s image for service %q: %w", op.Operation, service, err)
		}
		imageSetups[service] = setup
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseImageSetup).
		Observe(time.Since(imageSetupStartedAt).Seconds())
	var networkName string
	if b.cfg.IsNetworkIsolation() {
		if err := b.ensureTenantNetworkWith(mutations, ctx, mutations.tenant); err != nil {
			return fmt.Errorf("ensure %s tenant network: %w", op.Operation, err)
		}
		networkName = TenantNetworkName(mutations.tenant)
	}
	volumeSetupStartedAt := time.Now()
	volBinds, _, err := b.setupVolBinds(
		mutations, ctx, op.LeaseUUID, op.Items, op.ResourceProfiles,
		imageSetups, op.Stack.Services, op.Logger,
	)
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseVolumeSetup).
		Observe(time.Since(volumeSetupStartedAt).Seconds())
	if err != nil {
		return fmt.Errorf("prepare %s volumes: %w", op.Operation, err)
	}
	b.provisionsMu.RLock()
	failCount := 0
	if provision := b.provisions[op.LeaseUUID]; provision != nil {
		failCount = provision.FailCount
	}
	b.provisionsMu.RUnlock()
	project := buildComposeProject(composeProjectParams{
		LeaseUUID: op.LeaseUUID, Tenant: mutations.tenant, ProviderUUID: mutations.providerUUID,
		CallbackURL: mutations.callbackURL, LifecycleCallbackURL: mutations.lifecycleURL,
		MaintenanceID: op.TargetMaintenanceID, BackendName: b.cfg.Name, FailCount: failCount,
		Stack: op.Stack, Items: op.Items, Profiles: profiles, ImageSetups: imageSetups,
		NetworkName: networkName, VolBinds: volBinds, Cfg: &b.cfg, Ingress: b.cfg.Ingress,
	})
	composeUpStartedAt := time.Now()
	composeUpErr := mutations.composeUp(ctx, project, composeUpOpts{ForceRecreate: op.Operation == "restart"})
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseComposeUp).
		Observe(time.Since(composeUpStartedAt).Seconds())
	if composeUpErr != nil {
		return fmt.Errorf("compose up for %s: %w", op.Operation, composeUpErr)
	}
	containers, err := b.compose.PS(ctx, composeProjectName(op.LeaseUUID))
	if err != nil {
		return fmt.Errorf("compose ps after %s: %w", op.Operation, err)
	}
	ids, byService, err := mapComposeContainers(containers, op.Items)
	if err != nil {
		return fmt.Errorf("compose returned a non-exact %s cohort: %w", op.Operation, err)
	}
	if !exactServiceContainerCohort(op.Items, ids, byService) {
		return fmt.Errorf("compose returned a non-exact %s cohort", op.Operation)
	}
	verifyStartedAt := time.Now()
	for service, serviceIDs := range byService {
		if err := b.verifyStartup(ctx, op.Stack.Services[service], serviceIDs, op.Logger.With("service", service)); err != nil {
			return err
		}
	}
	replacePhaseDurationSeconds.WithLabelValues(op.Operation, phaseVerifyStartup).
		Observe(time.Since(verifyStartedAt).Seconds())
	return nil
}

func (b *Backend) executeRestorePhysicalOutcome(
	ctx context.Context,
	claim shared.OperationIntentClaim,
) leasesm.ReplaceWorkOutcome {
	startedAt := time.Now()
	candidate, err := b.operationSettlement.PrepareOperationRelease(claim)
	if err != nil {
		return mustRestoreAmbiguous(err, claim)
	}
	execution, err := b.operationSettlement.StartOperationExecution(candidate)
	if err != nil {
		return mustRestoreAmbiguous(err, claim)
	}
	switch outcome := b.operationSettlement.ExecuteOperation(ctx, execution).(type) {
	case shared.OperationExecutionSuccess:
		committed, err := b.operationSettlement.CommitOperationSuccess(outcome)
		if err != nil {
			return mustRestoreAmbiguous(err, claim)
		}
		if err := b.finalizeCommittedRestoreSource(claim); err != nil {
			return mustRestoreAmbiguous(err, claim)
		}
		result, err := leasesm.NewRestoreWorkSuccess(committed)
		if err != nil {
			return mustRestoreAmbiguous(err, claim)
		}
		restoresTotal.WithLabelValues("success").Inc()
		restoreDurationSeconds.Observe(time.Since(startedAt).Seconds())
		return result
	case shared.OperationExecutionFailure:
		proof, err := b.operationSettlement.CommitOperationFailure(outcome)
		if err != nil {
			return mustRestoreAmbiguous(err, claim)
		}
		cause := outcome.Cause()
		if cause == nil {
			cause = errors.New("restore substrate is exactly absent")
		}
		result, err := leasesm.NewRestoreReplaceFailure(cause, leasesm.ReplaceFailureDetails{
			Reason: backend.ReasonRestoreFailed, CallbackErr: "restore failed", LastError: cause.Error(),
		}, proof)
		if err != nil {
			return mustRestoreAmbiguous(err, claim)
		}
		wrapped, err := leasesm.NewRestoreWorkResult(result)
		if err != nil {
			return mustRestoreAmbiguous(err, claim)
		}
		restoresTotal.WithLabelValues("failure").Inc()
		return wrapped
	case shared.OperationExecutionAmbiguous:
		return mustRestoreAmbiguous(outcome.Cause(), claim)
	default:
		return mustRestoreAmbiguous(fmt.Errorf("unknown restore outcome %T", outcome), claim)
	}
}

func (b *Backend) finalizeCommittedRestoreSource(claim shared.OperationIntentClaim) error {
	record, err := b.retentionStore.Get(claim.SourceLeaseUUID())
	if err != nil {
		return err
	}
	if record == nil {
		return nil
	}
	if record.Status != shared.RetentionStatusRestoring || record.NewLeaseUUID != claim.LeaseUUID() ||
		record.Generation != claim.SourceGeneration() {
		return errors.New("restore finalizer changed before committed source release")
	}
	return b.deleteRestoreFinalizerStrict(claim.LeaseUUID(), record)
}

func mustRestoreAmbiguous(err error, claim shared.OperationIntentClaim) leasesm.ReplaceWorkOutcome {
	if err == nil {
		err = errors.New("restore outcome is ambiguous")
	}
	outcome, buildErr := leasesm.NewAmbiguousRestoreWork(err, claim)
	if buildErr != nil {
		panic(buildErr)
	}
	return outcome
}

func (b *Backend) executeMaintenancePhysicalOutcome(
	ctx context.Context,
	target shared.MaintenanceReleaseClaim,
) leasesm.ReplaceWorkOutcome {
	intent := target.Intent()
	execution, err := b.maintenanceSettlement.StartMaintenanceExecution(target)
	if err != nil {
		return mustMaintenanceAmbiguous(err, intent)
	}
	switch outcome := b.maintenanceSettlement.ExecuteMaintenance(ctx, execution).(type) {
	case shared.MaintenanceExecutionSuccess:
		active, err := b.maintenanceSettlement.ActivateMaintenance(outcome)
		if err != nil {
			return mustMaintenanceAmbiguous(err, intent)
		}
		result, err := leasesm.NewMaintenanceWorkSuccess(active)
		if err != nil {
			return mustMaintenanceAmbiguous(err, intent)
		}
		return result
	case shared.MaintenanceExecutionFailure:
		cause := outcome.Cause()
		if cause == nil {
			cause = fmt.Errorf("%s substrate did not commit the target", intent.Kind())
		}
		reason := maintenanceFailureReason(intent.Kind())
		callbackErr := string(intent.Kind()) + " failed"
		var physical *physicalOperationError
		if errors.As(cause, &physical) {
			reason = physical.reason
			callbackErr = physical.callback
		}
		failed, err := b.maintenanceSettlement.FailMaintenance(
			outcome, reason, callbackErr,
		)
		if err != nil {
			return mustMaintenanceAmbiguous(err, intent)
		}
		result, err := leasesm.NewMaintenanceReplaceFailure(cause, outcome.SourceRecovered(), false,
			leasesm.ReplaceFailureDetails{Reason: reason,
				CallbackErr: callbackErr, LastError: cause.Error()}, failed)
		if err != nil {
			return mustMaintenanceAmbiguous(err, intent)
		}
		wrapped, err := leasesm.NewMaintenanceWorkResult(result)
		if err != nil {
			return mustMaintenanceAmbiguous(err, intent)
		}
		return wrapped
	case shared.MaintenanceExecutionAmbiguous:
		return mustMaintenanceAmbiguous(outcome.Cause(), intent)
	default:
		return mustMaintenanceAmbiguous(fmt.Errorf("unknown maintenance outcome %T", outcome), intent)
	}
}

func mustMaintenanceAmbiguous(err error, intent shared.MaintenanceIntentClaim) leasesm.ReplaceWorkOutcome {
	if err == nil {
		err = errors.New("maintenance outcome is ambiguous")
	}
	outcome, buildErr := leasesm.NewAmbiguousMaintenanceWork(err, intent)
	if buildErr != nil {
		panic(buildErr)
	}
	return outcome
}
