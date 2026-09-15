package docker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const interruptedMaintenanceFailure = "backend interrupted maintenance before completion"

// maintenanceAdmission keeps replay acknowledgements structurally separate
// from the mutation capabilities returned only for a newly-created command.
// Existing, completed, and superseded requests can therefore never reach
// release append, actor routing, or Compose by accidentally using zero claims.
type maintenanceAdmission struct {
	disposition shared.MaintenanceIntentAdmissionDisposition
	intent      shared.MaintenanceIntentClaim
	target      shared.MaintenanceReleaseClaim
}

func (admission maintenanceAdmission) created() bool {
	return admission.disposition == shared.MaintenanceIntentAdmissionCreated
}

// maintenanceReplayResult keeps replay-only dispositions out of the mutation
// path. A superseded update is a definitive invalid-state refusal: its backend
// work remains deduplicated, and the provider must not persist its older
// payload over a later accepted generation.
func maintenanceReplayResult(
	disposition shared.MaintenanceIntentAdmissionDisposition,
) (replayed bool, err error) {
	switch disposition {
	case shared.MaintenanceIntentAdmissionExisting,
		shared.MaintenanceIntentAdmissionCompleted:
		return true, nil
	case shared.MaintenanceIntentAdmissionCompletedSuperseded:
		return true, fmt.Errorf(
			"%w: completed update was superseded by a later update generation",
			backend.ErrInvalidState,
		)
	case shared.MaintenanceIntentAdmissionNone,
		shared.MaintenanceIntentAdmissionCreated:
		return false, nil
	default:
		return true, errors.New("unknown maintenance replay disposition")
	}
}

func (b *Backend) admitMaintenance(
	request shared.MaintenanceRequestAuthority,
	source shared.MaintenanceSourceClaim,
	target shared.Release,
) (maintenanceAdmission, error) {
	if b.callbackStore == nil || b.releaseStore == nil {
		return maintenanceAdmission{},
			errors.New("durable callback and release stores are required for maintenance")
	}
	candidate, err := b.maintenanceSettlement.NewMaintenanceIntentCandidate(request, source, target)
	if err != nil {
		return maintenanceAdmission{},
			fmt.Errorf("construct %s maintenance authority: %w", request.Kind(), err)
	}
	admission, err := b.maintenanceSettlement.BeginMaintenanceIntent(candidate)
	if err != nil {
		return maintenanceAdmission{},
			fmt.Errorf("publish durable %s maintenance intent: %w", request.Kind(), err)
	}
	if replayed, replayErr := maintenanceReplayResult(admission.Disposition()); replayed {
		return maintenanceAdmission{disposition: admission.Disposition()}, replayErr
	}
	dispatch, created := admission.CreatedDispatch()
	if !created {
		return maintenanceAdmission{}, errors.New("maintenance admission returned no authority")
	}
	if err := b.maintenanceSettlement.CheckAppendMaintenanceCapacity(dispatch); err != nil {
		if cancelErr := b.maintenanceSettlement.CancelMaintenanceIntent(dispatch); cancelErr != nil {
			return maintenanceAdmission{},
				fmt.Errorf("%s maintenance refused but intent cancellation failed: %w",
					request.Kind(), errors.Join(err, cancelErr))
		}
		return maintenanceAdmission{}, err
	}
	appendClaim, err := b.maintenanceSettlement.StartMaintenanceAppend(dispatch)
	if err != nil {
		return maintenanceAdmission{},
			fmt.Errorf("start %s maintenance append (intent preserved): %w", request.Kind(), err)
	}
	targetClaim, err := b.maintenanceSettlement.AppendMaintenance(appendClaim)
	if err != nil {
		// The stores are independent. An append error after Begin is not proof
		// that the release transaction did not commit, so preserve the intent.
		return maintenanceAdmission{},
			fmt.Errorf("append %s maintenance release (intent preserved): %w", request.Kind(), err)
	}
	boundTarget, err := b.maintenanceSettlement.BindMaintenanceIntentTarget(targetClaim)
	if err != nil {
		return maintenanceAdmission{},
			fmt.Errorf("bind %s maintenance release (intent preserved): %w", request.Kind(), err)
	}
	return maintenanceAdmission{
		disposition: shared.MaintenanceIntentAdmissionCreated,
		intent:      boundTarget.Intent(), target: boundTarget,
	}, nil
}

func (b *Backend) failUnacceptedMaintenance(
	intent shared.MaintenanceIntentClaim,
	target shared.MaintenanceReleaseClaim,
	cause error,
) error {
	reason := replaceOpReason(string(intent.Kind()))
	refused, refuseErr := b.maintenanceSettlement.RefuseMaintenanceExecution(target)
	if refuseErr != nil {
		return fmt.Errorf("maintenance routing outcome is not a pre-effect refusal: %w", errors.Join(cause, refuseErr))
	}
	failed, err := b.maintenanceSettlement.FailMaintenance(refused, reason, string(intent.Kind())+" failed")
	if err != nil {
		return fmt.Errorf("maintenance routing failed and exact release settlement is ambiguous: %w",
			errors.Join(cause, err))
	}
	if err := b.resolveMaintenanceFailure(
		failed, interruptedMaintenanceFailure,
	); err != nil {
		return fmt.Errorf("maintenance routing failed and callback settlement is pending: %w",
			errors.Join(cause, err))
	}
	return cause
}

func (b *Backend) resolveMaintenanceSuccess(
	active shared.MaintenanceReleaseActive,
) error {
	return b.callbackPublisher.PublishMaintenanceSuccessContext(b.stopCtx, active)
}

func (b *Backend) resolveMaintenanceFailure(
	failed shared.MaintenanceReleaseFailure,
	errMsg string,
) error {
	return b.callbackPublisher.PublishMaintenanceFailureContext(b.stopCtx, failed, errMsg)
}

func (b *Backend) tryResolveMaintenanceSuccess(
	ctx context.Context,
	active shared.MaintenanceReleaseActive,
) error {
	acquired, err := b.callbackPublisher.TryPublishMaintenanceSuccessContext(ctx, active)
	if err != nil {
		return err
	}
	if !acquired {
		return errors.New("maintenance callback journal is busy; retry exact settlement")
	}
	return nil
}

func (b *Backend) tryResolveMaintenanceFailure(
	ctx context.Context,
	failed shared.MaintenanceReleaseFailure,
	errMsg string,
) error {
	acquired, err := b.callbackPublisher.TryPublishMaintenanceFailureContext(ctx, failed, errMsg)
	if err != nil {
		return err
	}
	if !acquired {
		return errors.New("maintenance callback journal is busy; retry exact settlement")
	}
	return nil
}

// settleMaintenanceBeforeClose prevents BeginCloseIntent from classifying an
// already-committed target as preempted failure. The command fence serializes
// this cross-store classification with new maintenance admission.
func (b *Backend) settleMaintenanceBeforeClose(leaseUUID string) error {
	if b.callbackStore == nil || b.releaseStore == nil {
		return nil
	}
	intent, found, err := b.maintenanceSettlement.GetMaintenanceIntent(leaseUUID)
	if err != nil || !found {
		return err
	}
	release, target, targetFound, err := b.maintenanceSettlement.FindMaintenanceRelease(
		leaseUUID, intent.MaintenanceID(),
	)
	if err != nil {
		return fmt.Errorf("inspect maintenance before close: %w", err)
	}
	if targetFound {
		target, err = b.maintenanceSettlement.BindMaintenanceIntentTarget(target)
		if err != nil {
			return fmt.Errorf("bind maintenance before close: %w", err)
		}
		intent = target.Intent()
		if release.Status == "active" {
			active, err := b.maintenanceSettlement.ProveMaintenanceActive(intent)
			if err != nil {
				return err
			}
			return b.resolveMaintenanceSuccess(active)
		}
		if release.Status == "deploying" {
			refused, refuseErr := b.maintenanceSettlement.RefuseMaintenanceExecution(target)
			if refuseErr != nil {
				return fmt.Errorf("preempted maintenance has crossed its physical boundary: %w", refuseErr)
			}
			if _, err := b.maintenanceSettlement.FailMaintenance(
				refused, replaceOpReason(string(intent.Kind())), string(intent.Kind())+" failed",
			); err != nil {
				return fmt.Errorf("fail preempted maintenance release: %w", err)
			}
		}
	}
	// Failed or never-appended work is converted atomically by BeginCloseIntent,
	// preserving its FIFO position ahead of deprovision completion.
	return nil
}

// recoverMaintenanceIntents closes every restart/update crash window before
// ordinary inventory recovery interprets a potentially mixed source/target
// cohort. It never re-runs Compose. The exact MaintenanceID shared by the WAL,
// Release and target labels is the only cleanup/activation authority.
func (b *Backend) recoverMaintenanceIntents(ctx context.Context) error {
	if b.callbackStore == nil || b.releaseStore == nil || b.recoveryCoordinator == nil {
		return nil
	}
	if err := b.recoverFailedMaintenanceReceipts(ctx); err != nil {
		return err
	}
	intents, err := b.maintenanceSettlement.ListMaintenanceIntents()
	if err != nil {
		return fmt.Errorf("list maintenance intents: %w", err)
	}
	for _, snapshot := range intents {
		if snapshot.Backend() != b.Name() || snapshot.BackendStorageID() != b.storageIdentity {
			return fmt.Errorf(
				"maintenance intent for lease %q belongs to backend %q storage %s, not backend %q storage %s",
				snapshot.LeaseUUID(), snapshot.Backend(), snapshot.BackendStorageID(), b.Name(), b.storageIdentity,
			)
		}

		_, recoveryErr := b.recoveryCoordinator.WithLease(
			ctx, snapshot.LeaseUUID(),
			func(recoveryScope shared.LeaseRecoveryScope) error {
				intent, found, readErr := b.maintenanceSettlement.GetMaintenanceIntent(snapshot.LeaseUUID())
				if readErr != nil || !found {
					return readErr
				}
				if intent.Backend() != b.Name() || intent.BackendStorageID() != b.storageIdentity {
					return fmt.Errorf("maintenance authority changed before recovery")
				}

				targetRelease, targetClaim, targetFound, findErr := b.maintenanceSettlement.FindMaintenanceRelease(
					intent.LeaseUUID(), intent.MaintenanceID(),
				)
				if findErr != nil {
					return fmt.Errorf("find exact maintenance release: %w", findErr)
				}
				if targetFound {
					var acquired bool
					targetClaim, acquired, findErr = b.maintenanceSettlement.TryBindMaintenanceIntentTarget(targetClaim)
					if findErr != nil {
						return fmt.Errorf("bind recovered maintenance target: %w", findErr)
					}
					if !acquired {
						return errors.New("maintenance callback journal is busy; retry exact target binding")
					}
					intent = targetClaim.Intent()
				}

				switch {
				case targetFound && targetRelease.Status == "active":
					active, proofErr := b.maintenanceSettlement.ProveMaintenanceActive(intent)
					if proofErr != nil {
						return fmt.Errorf("prove committed maintenance target: %w", proofErr)
					}
					containers, listErr := b.listManagedContainersStrictForRecovery(ctx)
					if listErr != nil {
						return fmt.Errorf("strict committed maintenance inventory: %w", listErr)
					}
					targetContainers, leaseContainers, selectErr := maintenanceTargetContainers(intent, containers)
					if selectErr != nil {
						return selectErr
					}
					cohortHealthy := len(targetContainers) == len(leaseContainers) &&
						validateRecoveredReleaseCohort(&targetRelease, targetContainers) == nil
					runtimeDiverged := !cohortHealthy
					if cohortHealthy {
						readiness, readinessErr := b.classifyRecoveredMaintenanceReadiness(
							ctx, targetRelease, targetContainers,
						)
						if readinessErr != nil {
							// The Release proves substrate commit, but not current runtime
							// health. Preserve the WAL and actor projection until a bounded
							// read can classify healthy versus definitively divergent.
							return fmt.Errorf("committed maintenance readiness is indeterminate: %w", readinessErr)
						}
						runtimeDiverged = readiness == maintenanceReadinessUnready
					}
					if runtimeDiverged {
						if _, divergenceErr := b.convergeMaintenanceRuntimeFailure(
							ctx, active, targetRelease, targetContainers,
						); divergenceErr != nil {
							return fmt.Errorf("converge committed maintenance runtime loss: %w", divergenceErr)
						}
						acquired, settleErr := b.callbackPublisher.TryPublishMaintenanceRuntimeFailureContext(
							ctx, active, leasesm.ErrMsgCohortDiverged,
						)
						if settleErr != nil {
							return fmt.Errorf("atomically settle committed maintenance runtime loss: %w", settleErr)
						}
						if !acquired {
							return errors.New("maintenance callback journal is busy; retry committed runtime-loss settlement")
						}
						return nil
					}
					_, convergeErr := b.convergeMaintenanceSuccess(
						ctx, active, targetRelease, targetContainers,
					)
					if convergeErr != nil {
						return convergeErr
					}
					if findErr = b.tryResolveMaintenanceSuccess(ctx, active); findErr != nil {
						return fmt.Errorf("settle committed maintenance success: %w", findErr)
					}
					return nil

				case targetFound && (targetRelease.Status == "deploying" || targetRelease.Status == "failed"):
					if verifyErr := b.verifyMaintenanceSourceActive(intent); verifyErr != nil {
						return verifyErr
					}
					containers, listErr := b.listManagedContainersStrictForRecovery(ctx)
					if listErr != nil {
						return fmt.Errorf("strict maintenance inventory: %w", listErr)
					}
					targetContainers, leaseContainers, selectErr := maintenanceTargetContainers(intent, containers)
					if selectErr != nil {
						return selectErr
					}
					compensationStarted, compensationErr := b.maintenanceSettlement.CompensationStarted(intent)
					if compensationErr != nil {
						return compensationErr
					}
					cohortErr := validateRecoveredReleaseCohort(&targetRelease, targetContainers)
					if targetRelease.Status == "deploying" &&
						!compensationStarted && len(targetContainers) == len(leaseContainers) && cohortErr == nil {
						readiness, readinessErr := b.classifyRecoveredMaintenanceReadiness(
							ctx, targetRelease, targetContainers,
						)
						if readinessErr != nil {
							return fmt.Errorf("maintenance target readiness is indeterminate: %w", readinessErr)
						}
						if readiness != maintenanceReadinessReady {
							cohortErr = errors.New("maintenance target is definitively unready")
						}
					}
					if targetRelease.Status == "deploying" &&
						!compensationStarted && len(targetContainers) == len(leaseContainers) && cohortErr == nil {
						physical, physicalErr := b.maintenanceSettlement.RecoverMaintenanceExecution(
							ctx, recoveryScope, intent,
						)
						if physicalErr != nil {
							return fmt.Errorf("classify recovered maintenance target: %w", physicalErr)
						}
						ready, ok := physical.(shared.MaintenanceExecutionSuccess)
						if !ok {
							if failed, failedOK := physical.(shared.MaintenanceExecutionFailure); failedOK {
								return fmt.Errorf(
									"recovered maintenance target is not exactly Ready (%T, source_recovered=%t)",
									physical, failed.SourceRecovered(),
								)
							}
							return fmt.Errorf("recovered maintenance target is not exactly Ready (%T)", physical)
						}
						active, activateErr := b.maintenanceSettlement.ActivateMaintenance(ready)
						if activateErr != nil {
							return fmt.Errorf("activate recovered maintenance target: %w", activateErr)
						}
						if _, convergeErr := b.convergeMaintenanceSuccess(
							ctx, active, targetRelease, targetContainers,
						); convergeErr != nil {
							return convergeErr
						}
						resolveErr := b.tryResolveMaintenanceSuccess(ctx, active)
						if resolveErr != nil {
							// Activation is irrevocably committed. Preserve the intent and let
							// the next periodic recovery retry only exact callback settlement.
							return fmt.Errorf("maintenance active but success settlement remains pending: %w", resolveErr)
						}
						return nil
					}
					compensationPending, compensationErr := b.maintenanceSettlement.CompensationPending(intent)
					if compensationErr != nil {
						return compensationErr
					}
					if !compensationPending && targetRelease.Status == "deploying" &&
						intent.ExecutionPhase() == shared.MaintenanceExecutionStarted &&
						time.Now().Before(b.maintenanceRecoveryDeadline(intent.CreatedAt())) {
						// StartMaintenanceExecution proves Compose may have accepted work.
						// Empty, source-only, partial, and merely-starting inventories are
						// therefore observations to retry, not failure authority. Startup
						// remains bounded because we defer this lease to the level-triggered
						// recovery loop instead of sleeping out the visibility window.
						return nil
					}

					var physical shared.MaintenanceExecutionOutcome
					var cleanupErr error
					if compensationPending && targetRelease.Status == "deploying" {
						physical, cleanupErr = b.maintenanceSettlement.RecoverMaintenanceCompensation(ctx, recoveryScope, intent)
					} else {
						physical, cleanupErr = b.maintenanceSettlement.CleanupRecoveredMaintenance(ctx, recoveryScope, intent)
					}
					if cleanupErr != nil {
						return fmt.Errorf("clean exact maintenance target: %w", cleanupErr)
					}
					failure, ok := physical.(shared.MaintenanceExecutionFailure)
					if !ok {
						if ambiguous, isAmbiguous := physical.(shared.MaintenanceExecutionAmbiguous); isAmbiguous {
							return fmt.Errorf("maintenance cleanup remains ambiguous: %w", ambiguous.Cause())
						}
						return fmt.Errorf("maintenance cleanup lacks definitive failure evidence (%T)", physical)
					}
					if targetRelease.Status == "deploying" {
						if _, failErr := b.maintenanceSettlement.FailMaintenance(
							failure, maintenanceFailureReason(intent.Kind()), string(intent.Kind())+" interrupted",
						); failErr != nil {
							return fmt.Errorf("fail interrupted maintenance target: %w", failErr)
						}
					}
					failed, proofErr := b.maintenanceSettlement.ProveMaintenanceFailure(intent)
					if proofErr != nil {
						return fmt.Errorf("prove failed maintenance target: %w", proofErr)
					}
					after, listErr := b.listManagedContainersStrictForRecovery(ctx)
					if listErr != nil {
						return fmt.Errorf("strict source inventory after maintenance cleanup: %w", listErr)
					}
					sourceRelease, sourceContainers, sourceReady, readyErr := b.maintenanceSourceState(ctx, intent, after)
					if readyErr != nil {
						return readyErr
					}
					failureInfo := recoveredMaintenanceFailureInfo(intent, &targetRelease)
					if convergeErr := b.convergeMaintenanceFailure(
						ctx, intent, sourceRelease, sourceContainers, sourceReady, failureInfo,
					); convergeErr != nil {
						return convergeErr
					}
					findErr = b.tryResolveMaintenanceFailure(ctx, failed, failureInfo.CallbackError())
					if findErr != nil {
						return fmt.Errorf("settle interrupted maintenance failure: %w", findErr)
					}
					return nil

				case !targetFound:
					if verifyErr := b.verifyMaintenanceSourceActive(intent); verifyErr != nil {
						return verifyErr
					}
					containers, listErr := b.listManagedContainersStrictForRecovery(ctx)
					if listErr != nil {
						return fmt.Errorf("strict maintenance inventory: %w", listErr)
					}
					for _, container := range containers {
						if container.MaintenanceID == intent.MaintenanceID() {
							return fmt.Errorf(
								"maintenance target %s has substrate but no durable target release",
								intent.MaintenanceID(),
							)
						}
					}
					sourceRelease, sourceContainers, sourceReady, readyErr := b.maintenanceSourceState(ctx, intent, containers)
					if readyErr != nil {
						return readyErr
					}
					if convergeErr := b.convergeMaintenanceFailure(
						ctx, intent, sourceRelease, sourceContainers, sourceReady,
						recoveredMaintenanceFailureInfo(intent, nil),
					); convergeErr != nil {
						return convergeErr
					}
					failed, proofErr := b.maintenanceSettlement.ProveMaintenanceFailure(intent)
					if proofErr != nil {
						return fmt.Errorf("prove absent maintenance target: %w", proofErr)
					}
					findErr = b.tryResolveMaintenanceFailure(ctx, failed, interruptedMaintenanceFailure)
					if findErr != nil {
						return fmt.Errorf("settle pre-append maintenance failure: %w", findErr)
					}
					return nil
				default:
					return fmt.Errorf("maintenance target has unsupported status %q", targetRelease.Status)
				}
			},
		)
		if recoveryErr != nil {
			return fmt.Errorf("recover maintenance for lease %q: %w", snapshot.LeaseUUID(), recoveryErr)
		}
	}
	return nil
}

func (b *Backend) maintenanceRecoveryDeadline(createdAt time.Time) time.Time {
	timeout := b.cfg.ProvisionTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	if createdAt.IsZero() {
		return time.Time{}
	}
	return createdAt.Add(timeout)
}

func (b *Backend) recoverFailedMaintenanceReceipts(ctx context.Context) error {
	receipts, err := b.maintenanceSettlement.ListFailedMaintenanceReceipts()
	if err != nil {
		return fmt.Errorf("list failed maintenance receipts: %w", err)
	}
	for _, receipt := range receipts {
		if receipt.Backend() != b.Name() || receipt.BackendStorageID() != b.storageIdentity {
			return fmt.Errorf(
				"failed maintenance receipt for lease %q belongs to backend %q storage %s",
				receipt.LeaseUUID(), receipt.Backend(), receipt.BackendStorageID(),
			)
		}
		_, cleanupErr := b.recoveryCoordinator.WithLease(
			ctx, receipt.LeaseUUID(),
			func(scope shared.LeaseRecoveryScope) error {
				return b.maintenanceSettlement.CleanupFailedMaintenanceReceipt(
					ctx, scope, receipt,
				)
			},
		)
		if cleanupErr != nil {
			return fmt.Errorf("clean late failed maintenance for lease %q: %w",
				receipt.LeaseUUID(), cleanupErr)
		}
	}
	return nil
}

func (b *Backend) convergeMaintenanceSuccess(
	ctx context.Context,
	active shared.MaintenanceReleaseActive,
	target shared.Release,
	containers []ContainerInfo,
) (bool, error) {
	intent := active.Intent()
	_ = ctx
	return b.applyMaintenanceProjectionWithoutActor(
		intent, target, containers, backend.ProvisionStatusReady, nil,
	)
}

func (b *Backend) convergeMaintenanceFailure(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	source shared.Release,
	containers []ContainerInfo,
	sourceReady bool,
	info leasesm.ReplaceFailureInfo,
) error {
	info = info.WithOldStopped(sourceReady)
	_, err := b.convergeMaintenanceFailureWithInfo(
		ctx, intent, source, containers, sourceReady, info,
	)
	return err
}

func recoveredMaintenanceFailureInfo(
	intent shared.MaintenanceIntentClaim,
	target *shared.Release,
) leasesm.ReplaceFailureInfo {
	details := leasesm.ReplaceFailureDetails{
		CallbackErr: interruptedMaintenanceFailure,
		Reason:      maintenanceFailureReason(intent.Kind()),
		LastError:   interruptedMaintenanceFailure,
	}
	if target == nil || target.Status != "failed" {
		info, _ := leasesm.NewMaintenanceRecoveryFailureInfo(intent, details)
		return info
	}
	if target.Reason != "" {
		details.Reason = target.Reason
	}
	if target.Message != "" {
		details.CallbackErr = target.Message
		details.LastError = target.Message
	}
	info, _ := leasesm.NewMaintenanceRecoveryFailureInfo(intent, details)
	return info
}

func (b *Backend) convergeMaintenanceRuntimeFailure(
	ctx context.Context,
	active shared.MaintenanceReleaseActive,
	target shared.Release,
	containers []ContainerInfo,
) (bool, error) {
	intent := active.Intent()
	_ = ctx
	failure, failureErr := leasesm.NewMaintenanceRecoveryFailureInfo(intent, leasesm.ReplaceFailureDetails{
		CallbackErr: leasesm.ErrMsgCohortDiverged,
		Reason:      backend.ReasonInternal,
		LastError:   leasesm.ErrMsgCohortDiverged,
	})
	if failureErr != nil {
		return false, failureErr
	}
	return b.applyMaintenanceProjectionWithoutActor(
		intent, target, containers, backend.ProvisionStatusFailed, &failure,
	)
}

func (b *Backend) convergeMaintenanceFailureWithInfo(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	release shared.Release,
	containers []ContainerInfo,
	sourceReady bool,
	info leasesm.ReplaceFailureInfo,
) (bool, error) {
	_ = ctx
	status := backend.ProvisionStatusFailed
	if sourceReady {
		status = backend.ProvisionStatusReady
	}
	return b.applyMaintenanceProjectionWithoutActor(
		intent, release, containers, status, &info,
	)
}

// applyMaintenanceProjectionWithoutActor closes the live dropped-terminal
// window without constructing an actor. Holding actorsMu while updating the
// provision map follows actor creation's lock order (actorsMu -> provisionsMu)
// and proves no serial owner can appear between the absence check and the
// projection rewrite. Cold recovery has no projection and simply returns;
// ordinary inventory recovery then builds it from the same terminal Release.
func (b *Backend) applyMaintenanceProjectionWithoutActor(
	intent shared.MaintenanceIntentClaim,
	release shared.Release,
	containers []ContainerInfo,
	status backend.ProvisionStatus,
	failure *leasesm.ReplaceFailureInfo,
) (bool, error) {
	authority, ok := runtimeIdentityForRelease(&release)
	if !ok {
		return false, errors.New("maintenance projection release has no runtime authority")
	}
	target := intent.TargetRelease()
	targetAuthority, ok := runtimeIdentityForRelease(&target)
	if !ok || release.OperationID != target.OperationID ||
		authority.Class() != targetAuthority.Class() ||
		authority.Tenant() != intent.Tenant() ||
		authority.ProviderUUID() != intent.ProviderUUID() ||
		authority.Tenant() != targetAuthority.Tenant() ||
		authority.ProviderUUID() != targetAuthority.ProviderUUID() {
		return false, errors.New("maintenance projection release changes durable runtime identity")
	}
	if release.MaintenanceID == intent.MaintenanceID() &&
		(authority.CallbackURL() != intent.CallbackURL() ||
			authority.LifecycleCallbackURL() != intent.LifecycleCallbackURL()) {
		return false, errors.New("maintenance target projection route differs from durable intent")
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return false, fmt.Errorf("parse maintenance projection manifest: %w", err)
	}
	containerIDs := make([]string, 0, len(containers))
	serviceContainers := make(map[string][]string)
	for _, container := range containers {
		containerIDs = append(containerIDs, container.ContainerID)
		serviceContainers[container.ServiceName] = append(
			serviceContainers[container.ServiceName], container.ContainerID,
		)
	}
	slices.Sort(containerIDs)
	for service := range serviceContainers {
		slices.Sort(serviceContainers[service])
	}

	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.actors[intent.LeaseUUID()] != nil {
		return false, errors.New("maintenance actor appeared before projection convergence; retry")
	}
	b.provisionsMu.Lock()
	defer b.provisionsMu.Unlock()
	provision := b.provisions[intent.LeaseUUID()]
	if provision == nil {
		return false, nil
	}
	provision.Tenant = authority.Tenant()
	provision.ProviderUUID = authority.ProviderUUID()
	provision.CallbackURL = authority.CallbackURL()
	provision.LifecycleCallbackURL = authority.LifecycleCallbackURL()
	provision.Items = slices.Clone(release.Items)
	provision.ResourceProfiles = shared.CloneSKUResourceSnapshot(release.ResourceProfiles)
	provision.ActiveReleaseVersion = release.Version
	provision.ActiveOperationID = authority.OperationID()
	provision.StackManifest = stack
	provision.ContainerIDs = containerIDs
	provision.ServiceContainers = serviceContainers
	provision.Status = status
	if failure == nil {
		provision.LastError = ""
		provision.Reason = ""
		provision.Message = ""
	} else {
		provision.LastError = failure.LastError()
		provision.Reason = failure.Reason()
		provision.Message = failure.CallbackError()
	}
	return true, nil
}

func (b *Backend) maintenanceSourceState(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	containers []ContainerInfo,
) (shared.Release, []ContainerInfo, bool, error) {
	source, current, err := b.maintenanceSettlement.ClaimLatestActive(intent.LeaseUUID())
	if err != nil {
		return shared.Release{}, nil, false, fmt.Errorf("inspect exact maintenance source: %w", err)
	}
	expected := intent.SourceRelease()
	if current.Version() != expected.Version() || current.Digest() != expected.Digest() {
		return shared.Release{}, nil, false, errors.New("maintenance source release changed before projection recovery")
	}
	leaseContainers := make([]ContainerInfo, 0, len(containers))
	for _, container := range containers {
		if container.LeaseUUID == intent.LeaseUUID() &&
			container.MaintenanceID == source.MaintenanceID {
			leaseContainers = append(leaseContainers, container)
		}
	}
	// A cohort mismatch is terminal substrate evidence, not an indeterminate
	// inspection. Project the source as failed; only bounded read errors preserve
	// the intent for a later recovery sweep.
	cohortValid := validateRecoveredReleaseCohort(&source, leaseContainers) == nil
	if !cohortValid {
		return source, leaseContainers, false, nil
	}
	readiness, err := b.classifyRecoveredMaintenanceReadiness(ctx, source, leaseContainers)
	if err != nil {
		return shared.Release{}, nil, false, fmt.Errorf("maintenance source readiness is indeterminate: %w", err)
	}
	return source, leaseContainers, readiness == maintenanceReadinessReady, nil
}

type maintenanceReadiness uint8

const (
	maintenanceReadinessReady maintenanceReadiness = iota + 1
	maintenanceReadinessUnready
)

// classifyRecoveredMaintenanceReadiness distinguishes observed terminal
// workload evidence from transport ambiguity. Every inspection uses the
// recovery read budget; this function never polls or sleeps while recoverMu and
// commandFence are held. A young no-healthcheck container or a healthcheck that
// is still starting is retried on the next sweep rather than destroyed.
func (b *Backend) classifyRecoveredMaintenanceReadiness(
	ctx context.Context,
	target shared.Release,
	containers []ContainerInfo,
) (maintenanceReadiness, error) {
	stack, err := manifest.ParsePayload(target.Manifest)
	if err != nil {
		return 0, fmt.Errorf("parse recovered maintenance manifest: %w", err)
	}
	for _, container := range containers {
		if container.Status != "running" {
			return maintenanceReadinessUnready, nil
		}
		if container.Health == HealthStatusUnhealthy {
			return maintenanceReadinessUnready, nil
		}
		service := stack.Services[container.ServiceName]
		if service == nil {
			return 0, fmt.Errorf("maintenance target service %q is absent from manifest", container.ServiceName)
		}
		if !service.HasActiveHealthCheck() {
			minimumAge := b.cfg.StartupVerifyDuration
			if minimumAge <= 0 {
				minimumAge = 5 * time.Second
			}
			if container.CreatedAt.IsZero() || time.Since(container.CreatedAt) < minimumAge {
				return 0, fmt.Errorf("maintenance target %q has not reached its startup verification age", container.ContainerID)
			}
		}

		inspected, err := b.inspectContainerForRecovery(ctx, container.ContainerID)
		if err != nil {
			return 0, fmt.Errorf("inspect recovered maintenance target %q: %w", container.ContainerID, err)
		}
		if inspected.Status != "running" {
			return maintenanceReadinessUnready, nil
		}
		if inspected.Health == HealthStatusUnhealthy {
			return maintenanceReadinessUnready, nil
		}
		if service.HasActiveHealthCheck() {
			switch inspected.Health {
			case HealthStatusHealthy:
			case HealthStatusUnhealthy:
				return maintenanceReadinessUnready, nil
			default:
				return 0, fmt.Errorf("maintenance target %q health check is not terminal", container.ContainerID)
			}
		}
	}
	return maintenanceReadinessReady, nil
}

func maintenanceFailureReason(kind shared.MaintenanceIntentKind) backend.Reason {
	if kind == shared.MaintenanceIntentUpdate {
		return backend.ReasonUpdateFailed
	}
	return backend.ReasonRestartFailed
}

func (b *Backend) verifyMaintenanceSourceActive(intent shared.MaintenanceIntentClaim) error {
	_, current, err := b.maintenanceSettlement.ClaimLatestActive(intent.LeaseUUID())
	if err != nil {
		return fmt.Errorf("verify maintenance source release: %w", err)
	}
	source := intent.SourceRelease()
	if current.Version() != source.Version() || current.Digest() != source.Digest() {
		return errors.New("maintenance source release is no longer the exact active generation")
	}
	return nil
}

func maintenanceTargetContainers(
	intent shared.MaintenanceIntentClaim,
	containers []ContainerInfo,
) ([]ContainerInfo, []ContainerInfo, error) {
	target := make([]ContainerInfo, 0)
	lease := make([]ContainerInfo, 0)
	for _, container := range containers {
		if container.MaintenanceID == intent.MaintenanceID() && container.LeaseUUID != intent.LeaseUUID() {
			return nil, nil, fmt.Errorf(
				"maintenance ID %s is attached to foreign lease %q",
				intent.MaintenanceID(), container.LeaseUUID,
			)
		}
		if container.LeaseUUID != intent.LeaseUUID() {
			continue
		}
		lease = append(lease, container)
		if container.MaintenanceID == intent.MaintenanceID() {
			target = append(target, container)
		}
	}
	slices.SortFunc(target, compareContainerIdentity)
	return target, lease, nil
}

func compareContainerIdentity(left, right ContainerInfo) int {
	if left.ContainerID < right.ContainerID {
		return -1
	}
	if left.ContainerID > right.ContainerID {
		return 1
	}
	return 0
}

func (b *Backend) validateMaintenanceTargetContainer(
	intent shared.MaintenanceIntentClaim,
	target shared.Release,
	container ContainerInfo,
) error {
	return validateMaintenanceGenerationContainer(
		intent.LeaseUUID(), intent.MaintenanceID(), b.Name(), target, container,
	)
}

func validateMaintenanceGenerationContainer(
	leaseUUID string,
	maintenanceID shared.MaintenanceID,
	backendName string,
	target shared.Release,
	container ContainerInfo,
) error {
	if container.ContainerID == "" || container.LeaseUUID != leaseUUID ||
		container.MaintenanceID != maintenanceID || container.BackendName != backendName {
		return fmt.Errorf("container %q lacks exact maintenance identity", container.ContainerID)
	}
	authority, ok := runtimeIdentityForRelease(&target)
	if !ok ||
		container.Tenant != authority.Tenant() ||
		container.ProviderUUID != authority.ProviderUUID() ||
		container.CallbackURL != authority.CallbackURL() ||
		container.LifecycleCallbackURL != authority.LifecycleCallbackURL() {
		return fmt.Errorf("container %q diverges from target runtime authority", container.ContainerID)
	}
	stack, err := manifest.ParsePayload(target.Manifest)
	if err != nil {
		return fmt.Errorf("parse target manifest: %w", err)
	}
	for _, item := range target.Items {
		if item.ServiceName != container.ServiceName || item.SKU != container.SKU ||
			container.InstanceIndex < 0 || container.InstanceIndex >= item.Quantity {
			continue
		}
		service := stack.Services[item.ServiceName]
		if item.CustomDomain != container.CustomDomain || service == nil || service.Image != container.Image {
			return fmt.Errorf("container %q diverges from target instance authority", container.ContainerID)
		}
		return nil
	}
	return fmt.Errorf("container %q is outside the target instance set", container.ContainerID)
}
