package docker

import (
	"cmp"
	"context"
	"errors"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func (b *Backend) cleanupCloseContainers(ctx context.Context, mutations *storageMutations, subject shared.ClosePhysicalSubject, recordedIDs []string) ([]string, error) {
	if b == nil || b.failureDiagnostics == nil || mutations == nil || mutations.closeSubject != subject || !subject.Valid() {
		return recordedIDs, errors.New("close cleanup requires its exact bound subject and diagnostics service")
	}
	attempt, present, err := b.failureDiagnostics.CloseAttempt(subject)
	if err != nil {
		return recordedIDs, err
	}
	if !present {
		return b.teardownLeaseContainersWith(mutations, ctx, subject.LeaseUUID(), recordedIDs,
			cmp.Or(b.cfg.ContainerStopTimeout, 30*time.Second), teardownOpDeprovision, b.logger.With("lease_uuid", subject.LeaseUUID()))
	}
	all, err := b.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return recordedIDs, err
	}
	var targets, failed []ContainerInfo
	for _, container := range all {
		if container.LeaseUUID != subject.LeaseUUID() {
			continue
		}
		if container.BackendName != b.Name() || container.Tenant != attempt.Tenant() || container.ProviderUUID != attempt.ProviderUUID() {
			return recordedIDs, errors.New("interrupted close cohort has divergent principal authority")
		}
		targets = append(targets, container)
		if attempt.MatchesTarget(container.CallbackURL, container.LifecycleCallbackURL, container.MaintenanceID) {
			failed = append(failed, container)
		}
	}
	observation := b.captureFailureObservation(ctx, failed, nil)
	capture, err := attempt.Capture(observation)
	if err != nil {
		return recordedIDs, err
	}
	// This private cohort seals the complete close set alongside the persisted
	// interrupted subset. A late target is classified by the next close attempt;
	// no Compose label sweep can delete evidence that this pass never observed.
	captured := capturedCloseCohort{mutations: mutations, capture: capture, containers: targets}
	return captured.remove(ctx)
}

type capturedCloseCohort struct {
	mutations  *storageMutations
	capture    shared.FailureDiagnosticCapture
	containers []ContainerInfo
}
