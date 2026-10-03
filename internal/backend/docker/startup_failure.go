package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// startupFailure is the provision workflow's typed finding (ENG-1125): a
// container of a settled launch's cohort positively failed startup. It is the
// Finding the substratemutation Guard hands to the classifier from a wholly
// successful session; the workflow reports it only as an
// acceptedStartupFailure, after its session accepted it and the attempt was
// rolled back exactly. The classifier alone decides whether it becomes
// evidence.
//
// Only newStartupFailure mints one, and internal/testutil confines its calls
// to the two startup observations. The zero value is never valid.
type startupFailure struct{ state *startupFailureState }

type startupFailureState struct {
	mutations *storageMutations
	launch    settledLaunch
	container startupContainer
	surface   *physicalOperationError
	sealed    shared.OperationStartupFailure
}

// acceptedStartupFailure is a startup finding this execution's session
// accepted: the only form the provision workflow reports, and the only input
// from which a live rollback may be admitted.
type acceptedStartupFailure = substratemutation.Accepted[startupFailure]

// newStartupFailure proves a failed startup watch into a finding. Every input
// must be positive: a receipt that this exact execution's launch exchange
// settled, a failed container that belongs to the launch's PS cohort, and that
// container's own inspection carrying the Started operation's identity. A
// refused start must also be one the receipt recorded, on a container that is
// still only created. An exit's termination is the substrate adapter's own
// (containerInfoToInstanceState), and its death's live provenance is taken
// from the event ledger, waiting briefly for the die event; a missing event
// only leaves the death unattributed.
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
		return startupFailure{}, errors.New("failed startup container is not in the launch's cohort")
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
	switch watch.verdict {
	case startupVerdictExited:
		// One adapter decides termination for every counting site.
		terms.Termination = containerInfoToInstanceState(info).Termination
		terms.ExitCode, terms.OOMKilled = info.ExitCode, info.OOMKilled
		if death, observed := b.liveDeaths.awaitLiveDeath(ctx, info.ContainerID, liveDeathAwait); observed {
			terms.Provenance = death
		}
	case startupVerdictStartRefused:
		if !launch.startRefused(info.ContainerID) || !strings.EqualFold(info.Status, "created") {
			return startupFailure{}, errors.New("a refused start requires the daemon's refusal of a container that never ran")
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

// boundTo reports whether the finding was minted for this exact execution.
func (f startupFailure) boundTo(mutations *storageMutations) bool {
	return f.state != nil && mutations != nil && f.state.mutations == mutations &&
		f.state.launch.boundTo(mutations) && f.state.sealed.Valid() && f.state.surface != nil
}

// concludeStartupFailure is the provision workflow's only path from a startup
// finding to its report, and it keeps the order that recovery depends on: the
// execution's session must accept the finding, then the rollback's
// preconditions must hold, and only then is anything removed. When either
// refuses, the finding could not have settled live, so nothing of the attempt
// is touched: the cohort stays, and operation recovery settles the failure
// from it, in one pass when a container of it exited or reported unhealthy,
// instead of waiting out provision_timeout behind an empty inventory.
func (b *Backend) concludeStartupFailure(
	ctx context.Context,
	mutations *storageMutations,
	failure startupFailure,
	logger *slog.Logger,
) (acceptedStartupFailure, error) {
	accepted, err := mutations.acceptStartupFailure(failure)
	if err != nil {
		return acceptedStartupFailure{}, fmt.Errorf("startup failure cannot settle live; its cohort is kept for recovery: %w", err)
	}
	rollback, err := b.admitStartupRollback(ctx, mutations, accepted)
	if err != nil {
		return acceptedStartupFailure{}, fmt.Errorf("startup failure cannot settle live; its cohort is kept for recovery: %w", err)
	}
	if err := b.rollbackStartupFailure(ctx, mutations, rollback, logger); err != nil {
		return acceptedStartupFailure{}, err
	}
	return accepted, nil
}

// startupRollback is an admitted plan to undo one accepted startup failure:
// the accepted finding, and the volumes its launch reported created that the
// predecessor does not name. Only admitStartupRollback mints one, after
// checking every precondition of the live definite outcome that the rollback
// itself cannot repair. The zero value is invalid.
type startupRollback struct{ state *startupRollbackState }

type startupRollbackState struct {
	mutations *storageMutations
	failure   startupFailure
	destroy   []string
}

// admitStartupRollback admits the rollback of an accepted finding only when
// every precondition of the classifier's confirmation that the rollback
// cannot repair holds, read positively before anything is removed: the
// launch-debt journal holds nothing for the lease (this launch's own row was
// cleared with its receipt), no retained volume of the lease exists, and every
// canonical volume of the lease was either created by this launch or named by
// the predecessor Release. A volume outside both can be destroyed by nothing
// the live path owns, so the classifier's absence proof could never hold; the
// attempt is then left as it is for recovery. A failed read is no fact and
// refuses too.
func (b *Backend) admitStartupRollback(
	ctx context.Context,
	mutations *storageMutations,
	accepted acceptedStartupFailure,
) (startupRollback, error) {
	failure, present := accepted.Finding()
	if !present || !failure.boundTo(mutations) {
		return startupRollback{}, errors.New("startup rollback requires this execution's accepted finding")
	}
	subject := mutations.operationSubject
	if err := b.volumeLaunches.checkNamespace(subject.LeaseUUID()); err != nil {
		return startupRollback{}, fmt.Errorf("launch debt of the lease remains: %w", err)
	}
	preserve := make(map[string]struct{})
	if predecessor, ok := subject.PredecessorRelease(); ok {
		preserve = releaseCanonicalVolumeNames(subject.LeaseUUID(), predecessor)
	}
	created := failure.state.launch.createdVolumes()
	volumes, err := b.volumes.ListForProof(ctx)
	if err != nil {
		return startupRollback{}, fmt.Errorf("list managed volumes before startup rollback: %w", err)
	}
	canonicalPrefix := leaseVolumePrefix(subject.LeaseUUID())
	retainedPrefix := retainedVolumePrefix + subject.LeaseUUID() + "-"
	for _, name := range volumes {
		if strings.HasPrefix(name, retainedPrefix) {
			return startupRollback{}, fmt.Errorf("retained volume %q of the lease exists", name)
		}
		if !strings.HasPrefix(name, canonicalPrefix) {
			continue
		}
		if _, kept := preserve[name]; kept {
			continue
		}
		if !slices.Contains(created, name) {
			return startupRollback{}, fmt.Errorf("volume %q of the lease is neither this launch's nor the predecessor's", name)
		}
	}
	var destroy []string
	for _, name := range created {
		if _, kept := preserve[name]; !kept {
			destroy = append(destroy, name)
		}
	}
	return startupRollback{state: &startupRollbackState{mutations: mutations, failure: failure, destroy: destroy}}, nil
}

// rollbackStartupFailure undoes an admitted startup rollback, in the worker
// that observed the failure, on the worker's context: a Deprovision that
// preempts the attempt cancels it, every Step then fails, and the outcome
// stays Ambiguous for close to own. It removes exactly the attempt's cohort,
// proves that no container of the lease remains, and destroys only the
// admitted volumes through the ownership choke point (ENG-658). Anything it
// cannot prove returns an error, which leaves the attempt Ambiguous.
func (b *Backend) rollbackStartupFailure(
	ctx context.Context,
	mutations *storageMutations,
	rollback startupRollback,
	logger *slog.Logger,
) error {
	if rollback.state == nil || rollback.state.mutations != mutations || !rollback.state.failure.boundTo(mutations) {
		return errors.New("startup rollback requires this execution's admitted plan")
	}
	surface := rollback.state.failure.state.surface
	subject := mutations.operationSubject
	intent := subject.Intent()
	// Capture the attempt's diagnostics under its curated surface, then remove
	// the captured cohort; each removal is a Step that re-checks identity.
	if err := b.cleanupFailedOperationTargets(ctx, mutations, subject, surface); err != nil {
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
		if err := b.cleanupFailedOperationTargets(ctx, mutations, subject, surface); err != nil {
			return err
		}
	}
	report := b.volumeOp(subject.LeaseUUID(), logger).destroy(mutations, ctx, destroySiteStartupRollback, rollback.state.destroy...)
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

// failedProvisionDurableRuntime derives the runtime the lease returns to when
// claim's startup failed definitely (ENG-1125): its active Release, or the
// claim's own callback pair for a first provision. The worker only derives it;
// the lease actor applies it while it publishes Failed, so no worker writes the
// actor-owned projection.
func (b *Backend) failedProvisionDurableRuntime(claim shared.OperationIntentClaim) (leasesm.DurableRuntime, error) {
	if b.releaseStore == nil {
		return leasesm.DurableRuntime{}, errors.New("release store is required to derive a failed provision's runtime")
	}
	active, err := b.releaseStore.LatestActive(claim.LeaseUUID())
	if err != nil {
		return leasesm.DurableRuntime{}, fmt.Errorf("read failed provision predecessor release: %w", err)
	}
	return leasesm.NewDurableRuntime(claim, active)
}

// classifyOperationFinding is the operation classifier as the Guard binds it:
// an accepted finding goes to confirmStartupFailure, and everything else,
// recovery included, to the shared exhaustive classifier.
func (b *Backend) classifyOperationFinding(
	ctx context.Context,
	subject shared.OperationPhysicalSubject,
	accepted acceptedStartupFailure,
) (shared.OperationPhysicalEvidence, error) {
	if finding, present := accepted.Finding(); present {
		return b.confirmStartupFailure(ctx, subject, finding)
	}
	return b.classifyOperationPhysical(ctx, subject)
}
