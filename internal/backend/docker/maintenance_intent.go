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

func (b *Backend) admitMaintenance(
	kind shared.MaintenanceIntentKind,
	source shared.ReleaseClaim,
	target shared.Release,
) (shared.MaintenanceIntentClaim, shared.MaintenanceReleaseClaim, error) {
	if b.callbackStore == nil || b.releaseStore == nil {
		return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{},
			errors.New("durable callback and release stores are required for maintenance")
	}
	admission, err := b.callbackStore.BeginMaintenanceIntent(shared.MaintenanceIntentSpec{
		Kind:             kind,
		SourceRelease:    source,
		TargetRelease:    target,
		Backend:          b.Name(),
		BackendStorageID: b.storageIdentity,
	})
	if err != nil {
		return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{},
			fmt.Errorf("publish durable %s maintenance intent: %w", kind, err)
	}
	if err := b.releaseStore.CheckAppendMaintenanceCapacity(admission); err != nil {
		if cancelErr := b.callbackStore.CancelMaintenanceIntent(admission); cancelErr != nil {
			return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{},
				fmt.Errorf("%s maintenance refused but intent cancellation failed: %w",
					kind, errors.Join(err, cancelErr))
		}
		return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{}, err
	}
	appendClaim, err := b.callbackStore.StartMaintenanceAppend(admission)
	if err != nil {
		return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{},
			fmt.Errorf("start %s maintenance append (intent preserved): %w", kind, err)
	}
	intent := appendClaim.Intent()
	targetClaim, err := b.releaseStore.AppendMaintenance(appendClaim)
	if err != nil {
		// The stores are independent. An append error after Begin is not proof
		// that the release transaction did not commit, so preserve the intent.
		return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{},
			fmt.Errorf("append %s maintenance release (intent preserved): %w", kind, err)
	}
	bound, err := b.callbackStore.BindMaintenanceIntentTarget(intent, targetClaim)
	if err != nil {
		return shared.MaintenanceIntentClaim{}, shared.MaintenanceReleaseClaim{},
			fmt.Errorf("bind %s maintenance release (intent preserved): %w", kind, err)
	}
	return bound, targetClaim, nil
}

func (b *Backend) failUnacceptedMaintenance(
	intent shared.MaintenanceIntentClaim,
	target shared.MaintenanceReleaseClaim,
	cause error,
) error {
	reason := replaceOpReason(string(intent.Kind()))
	if err := b.releaseStore.FailMaintenance(target, reason, string(intent.Kind())+" failed"); err != nil {
		return fmt.Errorf("maintenance routing failed and exact release settlement is ambiguous: %w",
			errors.Join(cause, err))
	}
	if err := b.resolveMaintenanceIntent(
		intent, backend.CallbackStatusFailed, interruptedMaintenanceFailure,
	); err != nil {
		return fmt.Errorf("maintenance routing failed and callback settlement is pending: %w",
			errors.Join(cause, err))
	}
	return cause
}

func (b *Backend) resolveMaintenanceIntent(
	intent shared.MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) error {
	if _, err := b.callbackStore.ResolveMaintenanceIntent(intent, status, errMsg); err != nil {
		return err
	}
	if b.callbackSender != nil {
		b.callbackSender.NotifyPendingCallbacks()
	}
	return nil
}

func (b *Backend) tryResolveMaintenanceIntent(
	intent shared.MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) error {
	_, acquired, err := b.callbackStore.TryResolveMaintenanceIntent(intent, status, errMsg)
	if err != nil {
		return err
	}
	if !acquired {
		return errors.New("maintenance callback journal is busy; retry exact settlement")
	}
	if b.callbackSender != nil {
		b.callbackSender.NotifyPendingCallbacks()
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
	intent, found, err := b.callbackStore.GetMaintenanceIntent(leaseUUID)
	if err != nil || !found {
		return err
	}
	release, target, targetFound, err := b.releaseStore.FindMaintenanceRelease(
		leaseUUID, intent.MaintenanceID(),
	)
	if err != nil {
		return fmt.Errorf("inspect maintenance before close: %w", err)
	}
	if targetFound {
		if _, bound := intent.TargetReleaseClaim(); !bound {
			intent, err = b.callbackStore.BindMaintenanceIntentTarget(intent, target)
			if err != nil {
				return fmt.Errorf("bind maintenance before close: %w", err)
			}
		}
		if release.Status == "active" {
			return b.resolveMaintenanceIntent(intent, backend.CallbackStatusSuccess, "")
		}
		if release.Status == "deploying" {
			if err := b.releaseStore.FailMaintenance(
				target, replaceOpReason(string(intent.Kind())), string(intent.Kind())+" failed",
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
	if b.callbackStore == nil || b.releaseStore == nil {
		return nil
	}
	intents, err := b.callbackStore.ListMaintenanceIntents()
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

		unlock := b.commandFence.Lock(snapshot.LeaseUUID())
		recoveryErr := func() error {
			intent, found, readErr := b.callbackStore.GetMaintenanceIntent(snapshot.LeaseUUID())
			if readErr != nil || !found {
				return readErr
			}
			if intent.Backend() != b.Name() || intent.BackendStorageID() != b.storageIdentity {
				return fmt.Errorf("maintenance authority changed before recovery")
			}

			// Admission holds commandFence through the actor ack. Therefore a live
			// replacement observed here as Restarting/Updating owns all substrate
			// movement; recovery must neither classify its in-progress snapshot nor
			// race it to target cleanup.
			if b.actorOwnsMaintenance(intent.LeaseUUID(), intent.MaintenanceID()) {
				return nil
			}

			targetRelease, targetClaim, targetFound, findErr := b.releaseStore.FindMaintenanceRelease(
				intent.LeaseUUID(), intent.MaintenanceID(),
			)
			if findErr != nil {
				return fmt.Errorf("find exact maintenance release: %w", findErr)
			}
			if targetFound {
				if bound, ok := intent.TargetReleaseClaim(); ok {
					if bound.Version() != targetClaim.Version() || bound.Digest() != targetClaim.Digest() {
						return errors.New("bound maintenance target differs from exact release")
					}
				} else {
					var acquired bool
					intent, acquired, findErr = b.callbackStore.TryBindMaintenanceIntentTarget(intent, targetClaim)
					if findErr != nil {
						return fmt.Errorf("bind recovered maintenance target: %w", findErr)
					}
					if !acquired {
						return errors.New("maintenance callback journal is busy; retry exact target binding")
					}
				}
			}

			switch {
			case targetFound && targetRelease.Status == "active":
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
						ctx, intent, targetRelease, targetContainers,
					); divergenceErr != nil {
						return fmt.Errorf("converge committed maintenance runtime loss: %w", divergenceErr)
					}
					acquired, settleErr := b.callbackStore.TryResolveMaintenanceIntentWithRuntimeFailure(
						intent, leasesm.ErrMsgCohortDiverged,
					)
					if settleErr != nil {
						return fmt.Errorf("atomically settle committed maintenance runtime loss: %w", settleErr)
					}
					if !acquired {
						return errors.New("maintenance callback journal is busy; retry committed runtime-loss settlement")
					}
					if b.callbackSender != nil {
						b.callbackSender.NotifyPendingCallbacks()
					}
					return nil
				}
				_, convergeErr := b.convergeMaintenanceSuccess(
					ctx, intent, targetRelease, targetContainers,
				)
				if convergeErr != nil {
					return convergeErr
				}
				if findErr = b.tryResolveMaintenanceIntent(intent, backend.CallbackStatusSuccess, ""); findErr != nil {
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
				cohortErr := validateRecoveredReleaseCohort(&targetRelease, targetContainers)
				if targetRelease.Status == "deploying" &&
					len(targetContainers) == len(leaseContainers) && cohortErr == nil {
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
					len(targetContainers) == len(leaseContainers) && cohortErr == nil {
					if activateErr := b.releaseStore.ActivateMaintenance(targetClaim); activateErr != nil {
						return fmt.Errorf("activate recovered maintenance target: %w", activateErr)
					}
					if _, convergeErr := b.convergeMaintenanceSuccess(
						ctx, intent, targetRelease, targetContainers,
					); convergeErr != nil {
						return convergeErr
					}
					resolveErr := b.tryResolveMaintenanceIntent(
						intent, backend.CallbackStatusSuccess, "",
					)
					if resolveErr != nil {
						// Activation is irrevocably committed. Preserve the intent and let
						// the next periodic recovery retry only exact callback settlement.
						return fmt.Errorf("maintenance active but success settlement remains pending: %w", resolveErr)
					}
					return nil
				}

				if cleanupErr := b.removeExactMaintenanceTargets(
					ctx, intent, targetRelease, targetContainers,
				); cleanupErr != nil {
					return cleanupErr
				}
				if targetRelease.Status == "deploying" {
					if failErr := b.releaseStore.FailMaintenance(
						targetClaim, maintenanceFailureReason(intent.Kind()), string(intent.Kind())+" interrupted",
					); failErr != nil {
						return fmt.Errorf("fail interrupted maintenance target: %w", failErr)
					}
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
				if intent.Kind() == shared.MaintenanceIntentUpdate &&
					targetRelease.Reason == backend.ReasonImagePullFailed {
					// Live Update deliberately lands Failed on a pre-substrate image
					// pull refusal even though its untouched source is still healthy.
					// Preserve that exact terminal policy across this crash boundary.
					sourceReady = false
				}
				if convergeErr := b.convergeMaintenanceFailure(
					ctx, intent, sourceRelease, sourceContainers, sourceReady, failureInfo,
				); convergeErr != nil {
					return convergeErr
				}
				findErr = b.tryResolveMaintenanceIntent(
					intent, backend.CallbackStatusFailed, failureInfo.CallbackErr,
				)
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
				findErr = b.tryResolveMaintenanceIntent(
					intent, backend.CallbackStatusFailed, interruptedMaintenanceFailure,
				)
				if findErr != nil {
					return fmt.Errorf("settle pre-append maintenance failure: %w", findErr)
				}
				return nil
			default:
				return fmt.Errorf("maintenance target has unsupported status %q", targetRelease.Status)
			}
		}()
		unlock()
		if recoveryErr != nil {
			return fmt.Errorf("recover maintenance for lease %q: %w", snapshot.LeaseUUID(), recoveryErr)
		}
	}
	return nil
}

func (b *Backend) actorOwnsMaintenance(leaseUUID string, id shared.MaintenanceID) bool {
	b.actorsMu.Lock()
	actor := b.actors[leaseUUID]
	b.actorsMu.Unlock()
	return actor != nil && actor.OwnsMaintenance(id)
}

// routeToExistingLeaseBlocking is the recovery-only counterpart to
// routeToLeaseBlocking. It deliberately never constructs an actor: cold
// recovery needs only durable projection rebuild, while a live stale actor must
// be converged before the MaintenanceIntent can be consumed.
func (b *Backend) routeToExistingLeaseBlocking(
	ctx context.Context,
	leaseUUID string,
	msg leasesm.LeaseMessage,
) (bool, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if b.stopCtx.Err() != nil {
			return false, errors.New("backend shutting down")
		}

		b.actorsMu.Lock()
		actor := b.actors[leaseUUID]
		if actor == nil {
			b.actorsMu.Unlock()
			return false, nil
		}
		enqueued := actor.TryEnqueue(msg)
		b.actorsMu.Unlock()
		if enqueued {
			return true, nil
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-b.stopCtx.Done():
			return false, errors.New("backend shutting down")
		case <-time.After(routeToLeaseRetryInterval):
		}
	}
}

func (b *Backend) convergeMaintenanceSuccess(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	target shared.Release,
	containers []ContainerInfo,
) (bool, error) {
	projection := maintenanceRecoveryProjection(containers)
	reply := make(chan error, 1)
	msg, err := leasesm.NewMaintenanceRecoveredSuccessMsg(intent, projection, reply)
	if err != nil {
		return false, err
	}
	routed, err := b.routeToExistingLeaseBlocking(ctx, intent.LeaseUUID(), msg)
	if err != nil {
		return routed, err
	}
	if !routed {
		return b.applyMaintenanceProjectionWithoutActor(
			intent, target, containers, backend.ProvisionStatusReady, nil,
		)
	}
	if err := b.waitForReply(ctx, reply); err != nil {
		return true, fmt.Errorf("apply committed maintenance projection: %w", err)
	}
	return true, nil
}

func maintenanceRecoveryProjection(containers []ContainerInfo) leasesm.MaintenanceRecoveryProjection {
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
	return leasesm.MaintenanceRecoveryProjection{
		ContainerIDs:      containerIDs,
		ServiceContainers: serviceContainers,
	}
}

func (b *Backend) convergeMaintenanceFailure(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	source shared.Release,
	containers []ContainerInfo,
	sourceReady bool,
	info leasesm.ReplaceFailureInfo,
) error {
	info.OldStopped = sourceReady
	projection := maintenanceRecoveryProjection(containers)
	_, err := b.convergeMaintenanceFailureWithInfo(
		ctx, intent, source, containers, projection, sourceReady, info,
	)
	return err
}

func recoveredMaintenanceFailureInfo(
	intent shared.MaintenanceIntentClaim,
	target *shared.Release,
) leasesm.ReplaceFailureInfo {
	info := leasesm.ReplaceFailureInfo{
		Operation:   string(intent.Kind()),
		CallbackErr: interruptedMaintenanceFailure,
		Reason:      maintenanceFailureReason(intent.Kind()),
		LastError:   interruptedMaintenanceFailure,
	}
	if target == nil || target.Status != "failed" {
		return info
	}
	if target.Reason != "" {
		info.Reason = target.Reason
	}
	if target.Message != "" {
		info.CallbackErr = target.Message
		info.LastError = target.Message
	}
	return info
}

func (b *Backend) convergeMaintenanceRuntimeFailure(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	target shared.Release,
	containers []ContainerInfo,
) (bool, error) {
	projection := maintenanceRecoveryProjection(containers)
	reply := make(chan error, 1)
	msg, err := leasesm.NewMaintenanceRecoveredRuntimeFailureMsg(intent, projection, reply)
	if err != nil {
		return false, err
	}
	routed, err := b.routeToExistingLeaseBlocking(ctx, intent.LeaseUUID(), msg)
	if err != nil {
		return routed, err
	}
	if !routed {
		failure := leasesm.ReplaceFailureInfo{
			Operation:   string(intent.Kind()),
			CallbackErr: leasesm.ErrMsgCohortDiverged,
			Reason:      backend.ReasonInternal,
			LastError:   leasesm.ErrMsgCohortDiverged,
		}
		return b.applyMaintenanceProjectionWithoutActor(
			intent, target, containers, backend.ProvisionStatusFailed, &failure,
		)
	}
	if err := b.waitForReply(ctx, reply); err != nil {
		return true, fmt.Errorf("apply committed maintenance runtime failure: %w", err)
	}
	return true, nil
}

func (b *Backend) convergeMaintenanceFailureWithInfo(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	release shared.Release,
	containers []ContainerInfo,
	projection leasesm.MaintenanceRecoveryProjection,
	sourceReady bool,
	info leasesm.ReplaceFailureInfo,
) (bool, error) {
	reply := make(chan error, 1)
	var (
		msg leasesm.LeaseMessage
		err error
	)
	if sourceReady {
		msg, err = leasesm.NewMaintenanceRecoveredFailureReadyMsg(
			intent, projection, info, reply,
		)
	} else {
		msg, err = leasesm.NewMaintenanceRecoveredFailureFailedMsg(
			intent, projection, info, reply,
		)
	}
	if err != nil {
		return false, err
	}
	routed, err := b.routeToExistingLeaseBlocking(ctx, intent.LeaseUUID(), msg)
	if err != nil {
		return routed, err
	}
	if !routed {
		status := backend.ProvisionStatusFailed
		if sourceReady {
			status = backend.ProvisionStatusReady
		}
		return b.applyMaintenanceProjectionWithoutActor(
			intent, release, containers, status, &info,
		)
	}
	if err := b.waitForReply(ctx, reply); err != nil {
		return true, fmt.Errorf("apply interrupted maintenance projection: %w", err)
	}
	return true, nil
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
	if release.RuntimeAuthority == nil {
		return false, errors.New("maintenance projection release has no runtime authority")
	}
	authority := release.RuntimeAuthority
	if release.OperationID != intent.TargetRelease().OperationID ||
		authority.OperationID() != release.OperationID ||
		authority.Tenant() != intent.Tenant() ||
		authority.ProviderUUID() != intent.ProviderUUID() {
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
	provision.ProvisionState.ResourceProfiles = shared.CloneSKUResourceSnapshot(release.ResourceProfiles)
	provision.StackManifest = stack
	provision.ContainerIDs = containerIDs
	provision.ServiceContainers = serviceContainers
	provision.Status = status
	if failure == nil {
		provision.LastError = ""
		provision.Reason = ""
		provision.Message = ""
	} else {
		provision.LastError = failure.LastError
		provision.Reason = failure.Reason
		provision.Message = failure.CallbackErr
	}
	return true, nil
}

func (b *Backend) maintenanceSourceState(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	containers []ContainerInfo,
) (shared.Release, []ContainerInfo, bool, error) {
	source, current, err := b.releaseStore.ClaimLatestActive(intent.LeaseUUID())
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
			!isLegacyRollbackRemnant(container) &&
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

// verifyRecoveredMaintenanceReadiness is the live rollback readiness gate.
// Unlike cold/periodic WAL classification above, the caller owns a bounded
// mutation context and must wait through the normal startup contract before it
// may claim that Compose restored the source generation.
func (b *Backend) verifyRecoveredMaintenanceReadiness(
	ctx context.Context,
	release shared.Release,
	containers []ContainerInfo,
) error {
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return fmt.Errorf("parse maintenance release manifest: %w", err)
	}
	byService := make(map[string][]string, len(stack.Services))
	for _, container := range containers {
		if container.Status != "running" {
			return fmt.Errorf("maintenance container %q is %s", container.ContainerID, container.Status)
		}
		if container.Health == HealthStatusUnhealthy {
			return fmt.Errorf("maintenance container %q is unhealthy", container.ContainerID)
		}
		byService[container.ServiceName] = append(byService[container.ServiceName], container.ContainerID)
	}
	for serviceName, service := range stack.Services {
		if err := b.verifyStartup(
			ctx, service, byService[serviceName],
			b.logger.With("maintenance_id", release.MaintenanceID, "service", serviceName),
		); err != nil {
			return fmt.Errorf("verify maintenance service %q: %w", serviceName, err)
		}
	}
	return nil
}

func maintenanceFailureReason(kind shared.MaintenanceIntentKind) backend.Reason {
	if kind == shared.MaintenanceIntentUpdate {
		return backend.ReasonUpdateFailed
	}
	return backend.ReasonRestartFailed
}

func (b *Backend) verifyMaintenanceSourceActive(intent shared.MaintenanceIntentClaim) error {
	_, current, err := b.releaseStore.ClaimLatestActive(intent.LeaseUUID())
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
		if container.LeaseUUID != intent.LeaseUUID() || isLegacyRollbackRemnant(container) {
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

func (b *Backend) removeExactMaintenanceTargets(
	ctx context.Context,
	intent shared.MaintenanceIntentClaim,
	target shared.Release,
	candidates []ContainerInfo,
) error {
	for _, candidate := range candidates {
		if err := b.validateMaintenanceTargetContainer(intent, target, candidate); err != nil {
			return fmt.Errorf("refuse ambiguous maintenance cleanup: %w", err)
		}
		inspected, err := b.inspectContainerForRecovery(ctx, candidate.ContainerID)
		if err != nil {
			return fmt.Errorf("inspect exact maintenance target %q before removal: %w", candidate.ContainerID, err)
		}
		if inspected.ContainerID != candidate.ContainerID {
			return fmt.Errorf("maintenance target inspect changed container ID %q to %q", candidate.ContainerID, inspected.ContainerID)
		}
		if err := b.validateMaintenanceTargetContainer(intent, target, *inspected); err != nil {
			return fmt.Errorf("refuse changed maintenance target %q: %w", candidate.ContainerID, err)
		}
		if err := b.mutationAdapter().removeContainer(ctx, candidate.ContainerID); err != nil {
			return fmt.Errorf("remove exact maintenance target %q: %w", candidate.ContainerID, err)
		}
	}
	after, err := b.listManagedContainersStrictForRecovery(ctx)
	if err != nil {
		return fmt.Errorf("confirm maintenance target cleanup: %w", err)
	}
	for _, container := range after {
		if container.MaintenanceID == intent.MaintenanceID() {
			return fmt.Errorf("maintenance target %q remains after cleanup", container.ContainerID)
		}
	}
	return nil
}

func (b *Backend) validateMaintenanceTargetContainer(
	intent shared.MaintenanceIntentClaim,
	target shared.Release,
	container ContainerInfo,
) error {
	if container.ContainerID == "" || container.LeaseUUID != intent.LeaseUUID() ||
		container.MaintenanceID != intent.MaintenanceID() || container.BackendName != b.Name() {
		return fmt.Errorf("container %q lacks exact maintenance identity", container.ContainerID)
	}
	if target.RuntimeAuthority == nil ||
		container.Tenant != target.RuntimeAuthority.Tenant() ||
		container.ProviderUUID != target.RuntimeAuthority.ProviderUUID() ||
		container.CallbackURL != target.RuntimeAuthority.CallbackURL() ||
		container.LifecycleCallbackURL != target.RuntimeAuthority.LifecycleCallbackURL() {
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
