package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// startupFailure is the provision workflow's typed finding (ENG-1125): a
// container of a settled launch's exact cohort positively failed startup
// verification. It is the Finding the substratemutation Guard hands to the
// classifier from a wholly successful session, and only after the workflow
// rolled the attempt back exactly; the classifier alone decides whether it
// becomes evidence.
//
// Only newStartupFailure mints one, and internal/testutil confines its calls
// to observeStartup. The zero value means "no finding" and is never valid.
type startupFailure struct{ state *startupFailureState }

type startupFailureState struct {
	mutations *storageMutations
	launch    settledLaunch
	container startupContainer
	surface   *physicalOperationError
	sealed    shared.OperationStartupFailure
}

// newStartupFailure proves a failed startup watch into a finding. Every input
// must be positive: a receipt that this exact execution's launch settled, a
// failed container that belongs to the launch's exact PS cohort, and that
// container's own inspection carrying the Started operation's identity. Its
// death's live provenance is taken from the event ledger, waiting briefly for
// the die event; a missing event only leaves the death unattributed.
func (b *Backend) newStartupFailure(
	ctx context.Context,
	mutations *storageMutations,
	launch settledLaunch,
	cohort startupCohort,
	watch startupWatch,
) (startupFailure, error) {
	if mutations == nil || !launch.boundTo(mutations) {
		return startupFailure{}, errors.New("startup failure requires this execution's settled launch")
	}
	subject := mutations.operationSubject
	intent := subject.Intent()
	if _, historical := subject.FailedReceiptCleanup(); historical || subject.RecoveryCleanup() ||
		!intent.Valid() || intent.Kind() != shared.OperationIntentProvision {
		return startupFailure{}, errors.New("startup failure requires a live provision execution")
	}
	if !watch.failed() || watch.surface == nil || watch.info == nil {
		return startupFailure{}, errors.New("startup failure requires a failed startup observation")
	}
	if !cohort.contains(watch.container) {
		return startupFailure{}, errors.New("failed startup container is not in the launch's exact cohort")
	}
	info := watch.info
	if info.ContainerID != watch.container.id || info.LeaseUUID != intent.LeaseUUID() ||
		info.BackendName != b.cfg.Name || info.Tenant != intent.Tenant() ||
		info.ProviderUUID != intent.ProviderUUID() || info.CallbackURL != intent.CallbackURL() ||
		info.LifecycleCallbackURL != intent.LifecycleCallbackURL() || !info.MaintenanceID.IsZero() {
		return startupFailure{}, errors.New("failed startup container does not carry the Started operation's identity")
	}
	terms := shared.OperationStartupFailureTerms{
		Reason: watch.surface.reason, Message: watch.surface.callback, Detail: watch.surface.cause.Error(),
		InstanceID: info.ContainerID, Service: watch.container.service, Degraded: launch.degraded(),
	}
	if watch.verdict == startupVerdictExited {
		terms.Termination = failurecause.Exited()
		terms.ExitCode, terms.OOMKilled = info.ExitCode, info.OOMKilled
		if death, observed := b.liveDeaths.awaitLiveDeath(ctx, info.ContainerID, liveDeathAwait); observed {
			terms.Provenance = death
		}
	}
	sealed, err := shared.NewOperationStartupFailure(terms)
	if err != nil {
		return startupFailure{}, err
	}
	return startupFailure{state: &startupFailureState{
		mutations: mutations, launch: launch, container: watch.container,
		surface: watch.surface, sealed: sealed,
	}}, nil
}

// present reports whether the workflow reported a finding at all. Whether it
// is valid for a subject is boundTo's question.
func (f startupFailure) present() bool { return f.state != nil }

// boundTo reports whether the finding was minted for this exact execution.
func (f startupFailure) boundTo(mutations *storageMutations) bool {
	return f.state != nil && mutations != nil && f.state.mutations == mutations &&
		f.state.launch.boundTo(mutations) && f.state.sealed.Valid() && f.state.surface != nil
}

// rollbackStartupFailure undoes a provision whose startup failed definitely,
// in the worker that observed it, on the worker's context: a Deprovision that
// preempts the attempt cancels it, every Step then fails, and the outcome
// stays Ambiguous for close to own. It removes exactly the attempt's cohort,
// proves that no container of the lease remains, and destroys only the
// volumes this launch's Create reported as created, minus the predecessor's
// names, through the ownership choke point (ENG-658). Anything it cannot
// prove returns an error, which leaves the attempt Ambiguous.
func (b *Backend) rollbackStartupFailure(
	ctx context.Context,
	mutations *storageMutations,
	finding startupFailure,
	logger *slog.Logger,
) error {
	if !finding.boundTo(mutations) {
		return errors.New("startup rollback requires this execution's finding")
	}
	subject := mutations.operationSubject
	intent := subject.Intent()
	// Capture the attempt's diagnostics under its curated surface, then remove
	// the captured cohort; each removal is a Step that re-checks identity.
	if err := b.cleanupFailedOperationTargets(ctx, mutations, subject, finding.state.surface); err != nil {
		return err
	}
	for observation := range 3 {
		fresh, err := b.strictIdentityBoundOperationInventory(ctx)
		if err != nil {
			return fmt.Errorf("confirm startup rollback: %w", err)
		}
		remaining, err := b.exactRecoveredOperationCleanupIDs(ctx, intent, fresh)
		if err != nil {
			return fmt.Errorf("derive remaining startup rollback targets: %w", err)
		}
		if len(remaining) == 0 {
			break
		}
		if observation == 2 {
			return errors.New("startup rollback substrate remained after two exact cleanup passes")
		}
		if err := b.cleanupFailedOperationTargets(ctx, mutations, subject, finding.state.surface); err != nil {
			return err
		}
	}
	preserve := make(map[string]struct{})
	if predecessor, ok := subject.PredecessorRelease(); ok {
		preserve = releaseCanonicalVolumeNames(subject.LeaseUUID(), predecessor)
	}
	var destroy []string
	for _, name := range finding.state.launch.createdVolumes() {
		if _, kept := preserve[name]; !kept {
			destroy = append(destroy, name)
		}
	}
	report := b.volumeOp(subject.LeaseUUID(), logger).destroy(mutations, ctx, destroySiteStartupRollback, destroy...)
	if report.leftOnDisk() {
		return fmt.Errorf("startup rollback could not destroy the attempt's volumes: %w",
			errors.Join(report.err(), fmt.Errorf("%d volume(s) refused", report.refused())))
	}
	return nil
}

// confirmStartupFailure is the classifier's only path from a finding to
// evidence, and the only caller of shared.NewOperationStartupFailed
// (internal/testutil). It re-reads, positively and in this order: the strict
// identity-bound inventory shows no container of the lease; the launch-debt
// journal holds nothing for the lease, the fact that rules out a late Docker
// Create (close relies on the same one); and the volume namespace holds
// nothing of the attempt. Any failed read is an error, which keeps the attempt
// Ambiguous. It never reports the target ready.
func (b *Backend) confirmStartupFailure(
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
	finding startupFailure,
) (shared.OperationPhysicalEvidence, error) {
	if finding.state == nil || finding.state.mutations == nil ||
		finding.state.mutations.operationSubject != subject || !finding.boundTo(finding.state.mutations) {
		return shared.OperationPhysicalEvidence{}, errors.New("startup failure finding belongs to another execution")
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return shared.OperationPhysicalEvidence{}, err
	}
	if slices.ContainsFunc(all, func(container ContainerInfo) bool {
		return container.LeaseUUID == subject.LeaseUUID()
	}) {
		return shared.OperationPhysicalEvidence{}, errors.New("startup failure substrate remains after rollback")
	}
	if err := b.volumeLaunches.checkNamespace(subject.LeaseUUID()); err != nil {
		return shared.OperationPhysicalEvidence{}, fmt.Errorf("startup failure launch debt: %w", err)
	}
	absent, err := b.operationStorageExactlyAbsent(ctx, subject)
	if err != nil {
		return shared.OperationPhysicalEvidence{}, err
	}
	if !absent {
		return shared.OperationPhysicalEvidence{}, errors.New("startup failure volume state remains after rollback")
	}
	return shared.NewOperationStartupFailed(subject, finding.state.sealed)
}

// restoreFailedProvisionRuntime returns the projection of a provision whose
// startup failed definitely to the lease's durable runtime before the actor
// publishes Failed: the inverse of prepareProvisionProjection, which moved it
// to the candidate for the attempt. Like that preparation, the worker writes
// it under the projection lock while the actor awaits this exact operation;
// a projection that awaits another operation, or none, is left alone.
func (b *Backend) restoreFailedProvisionRuntime(claim shared.OperationIntentClaim) error {
	if b.releaseStore == nil {
		return errors.New("release store is required to restore a failed provision's runtime")
	}
	active, err := b.releaseStore.LatestActive(claim.LeaseUUID())
	if err != nil {
		return fmt.Errorf("read failed provision predecessor release: %w", err)
	}
	b.provisionsMu.Lock()
	defer b.provisionsMu.Unlock()
	current := b.provisions[claim.LeaseUUID()]
	if current == nil || current.Status != backend.ProvisionStatusProvisioning ||
		!current.PendingOperation.Names(claim.OperationID()) {
		return nil
	}
	return restoreDurableRuntime(&current.ProvisionState, claim, active)
}

// classifyOperationFinding is the operation classifier as the Guard binds it:
// a workflow's finding goes to confirmStartupFailure, and everything else,
// recovery included, to the shared exhaustive classifier.
func (b *Backend) classifyOperationFinding(
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
	finding startupFailure,
) (shared.OperationPhysicalEvidence, error) {
	if finding.present() {
		return b.confirmStartupFailure(ctx, subject, finding)
	}
	return b.classifyOperationPhysical(ctx, subject)
}
