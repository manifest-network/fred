package docker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// failureCleanup owns both capture and removal for one exact physical subject.
// Raw container IDs and the remover never escape its capture boundary. The
// only removal method consumes a durable capturedFailureCohort minted here.
type failureCleanup struct {
	backend     *Backend
	mutations   *storageMutations
	operation   shared.OperationPhysicalSubject
	maintenance shared.MaintenancePhysicalSubject
}

type capturedFailureCohort struct {
	owner      *failureCleanup
	capture    shared.FailureDiagnosticCapture
	containers []ContainerInfo
}

func newOperationFailureCleanup(b *Backend, mutations *storageMutations, subject shared.OperationPhysicalSubject) (*failureCleanup, error) {
	if b == nil || b.failureDiagnostics == nil || mutations == nil || !subject.Valid() ||
		mutations.operationSubject != subject || mutations.ops.backend != b {
		return nil, errors.New("operation failure cleanup requires its exact bound subject and diagnostics store")
	}
	return &failureCleanup{backend: b, mutations: mutations, operation: subject}, nil
}

func newMaintenanceFailureCleanup(b *Backend, mutations *storageMutations, subject shared.MaintenancePhysicalSubject) (*failureCleanup, error) {
	if b == nil || b.failureDiagnostics == nil || mutations == nil || !subject.Valid() ||
		mutations.maintenanceSubject != subject || mutations.ops.backend != b {
		return nil, errors.New("maintenance failure cleanup requires its exact bound subject and diagnostics store")
	}
	return &failureCleanup{backend: b, mutations: mutations, maintenance: subject}, nil
}

func (b *Backend) cleanupFailedOperationTargets(ctx context.Context, mutations *storageMutations, subject shared.OperationPhysicalSubject, cause error) error {
	cleanup, err := newOperationFailureCleanup(b, mutations, subject)
	if err != nil {
		return err
	}
	return cleanup.run(ctx, cause)
}

func (b *Backend) cleanupFailedMaintenanceTargets(ctx context.Context, mutations *storageMutations, subject shared.MaintenancePhysicalSubject, cause error) error {
	cleanup, err := newMaintenanceFailureCleanup(b, mutations, subject)
	if err != nil {
		return err
	}
	return cleanup.run(ctx, cause)
}

func (cleanup *failureCleanup) run(ctx context.Context, cause error) error {
	captured, err := cleanup.capture(ctx, cause)
	if err != nil {
		return err
	}
	if err := captured.remove(ctx); err != nil {
		return err
	}
	return cleanup.backend.failureDiagnostics.RetireHistoricalCapture(captured.capture)
}

func (cleanup *failureCleanup) capture(ctx context.Context, cause error) (capturedFailureCohort, error) {
	containers, err := cleanup.targets(ctx)
	if err != nil {
		observation := failureObservation(cause)
		observation.Status = shared.DiagnosticCaptureUnavailable
		var persistErr error
		if cleanup.operation.Valid() {
			_, persistErr = cleanup.backend.failureDiagnostics.CaptureOperation(cleanup.operation, observation)
		} else {
			_, persistErr = cleanup.backend.failureDiagnostics.CaptureMaintenance(cleanup.maintenance, observation)
		}
		return capturedFailureCohort{}, errors.Join(err, persistErr)
	}
	observation := cleanup.backend.captureFailureObservation(ctx, containers, cause)
	var captured shared.FailureDiagnosticCapture
	if cleanup.operation.Valid() {
		captured, err = cleanup.backend.failureDiagnostics.CaptureOperation(cleanup.operation, observation)
	} else {
		captured, err = cleanup.backend.failureDiagnostics.CaptureMaintenance(cleanup.maintenance, observation)
	}
	if err != nil {
		return capturedFailureCohort{}, fmt.Errorf("persist exact failure diagnostics before cleanup: %w", err)
	}
	return capturedFailureCohort{owner: cleanup, capture: captured, containers: slices.Clone(containers)}, nil
}

func (captured capturedFailureCohort) remove(ctx context.Context) error {
	if captured.owner == nil || !captured.capture.Valid() || captured.owner.mutations == nil {
		return errors.New("failed target removal requires a durable exact-cohort diagnostic capture")
	}
	return captured.capture.RetainDuring(func() error {
		for _, target := range captured.containers {
			if err := captured.owner.mutations.removeContainer(ctx, target.ContainerID); err != nil {
				return fmt.Errorf("remove captured failed target %q: %w", target.ContainerID, err)
			}
		}
		return nil
	})
}

func (cleanup *failureCleanup) targets(ctx context.Context) ([]ContainerInfo, error) {
	all, err := cleanup.backend.strictIdentityBoundOperationInventory(ctx)
	if err != nil {
		return nil, err
	}
	if cleanup.operation.Valid() {
		var ids []string
		if receipt, historical := cleanup.operation.FailedReceiptCleanup(); historical {
			fence, err := newFailedOperationRecoveryFence(nil, []shared.FailedOperationReceipt{receipt})
			if err != nil {
				return nil, err
			}
			targets, err := fence.targets(all)
			if err != nil {
				return nil, err
			}
			for _, target := range targets {
				ids = append(ids, target.containerID)
			}
		} else {
			ids, err = cleanup.backend.exactRecoveredOperationCleanupIDs(ctx, cleanup.operation.Intent(), all)
			if err != nil {
				return nil, err
			}
		}
		selected := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			selected[id] = struct{}{}
		}
		var targets []ContainerInfo
		for _, container := range all {
			if _, ok := selected[container.ContainerID]; ok {
				targets = append(targets, container)
			}
		}
		slices.SortFunc(targets, compareContainerIdentity)
		return targets, nil
	}
	if receipt, historical := cleanup.maintenance.FailedReceiptCleanup(); historical {
		return cleanup.backend.failedMaintenanceReceiptTargets(receipt, all)
	}
	intent := cleanup.maintenance.Intent()
	target, ok := cleanup.maintenance.TargetRelease()
	if !ok {
		return nil, errors.New("maintenance failure cleanup has no target release")
	}
	targets, _, err := maintenanceTargetContainers(intent, all)
	if err != nil {
		return nil, err
	}
	for _, container := range targets {
		if err := cleanup.backend.validateMaintenanceTargetContainer(intent, target, container); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

func (b *Backend) captureFailureObservation(ctx context.Context, containers []ContainerInfo, cause error) shared.FailureDiagnosticObservation {
	observation := failureObservation(cause)
	observation.Logs = make(map[string]string)
	observation.ContainerIDs = make([]string, 0, len(containers))
	if len(containers) == 0 {
		observation.Status = shared.DiagnosticCaptureUnavailable
		return observation
	}
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	remaining := maxTotalLogBytes
	readFailures := 0
	for _, container := range containers {
		observation.ContainerIDs = append(observation.ContainerIDs, container.ContainerID)
		if remaining <= 0 {
			observation.Status = shared.DiagnosticCaptureTruncated
			continue
		}
		output, err := b.docker.ContainerLogs(readCtx, container.ContainerID, persistedLogTail)
		if err != nil {
			readFailures++
			continue
		}
		key := fmt.Sprintf("%s/%d", container.ServiceName, container.InstanceIndex)
		trimmed, consumed := trimLogToBudget(output, remaining)
		remaining -= consumed
		if len(trimmed) != len(output) || strings.Contains(output, "[log truncated: exceeded ") {
			observation.Status = shared.DiagnosticCaptureTruncated
		}
		if _, duplicate := observation.Logs[key]; duplicate {
			// Multiple exact late targets for one instance exceed the singular
			// API view. Preserve the first captured output and report truncation.
			observation.Status = shared.DiagnosticCaptureTruncated
			continue
		}
		observation.Logs[key] = trimmed
	}
	if len(observation.Logs) == 0 && readFailures > 0 {
		observation.Status = shared.DiagnosticCaptureUnavailable
	} else if readFailures > 0 && observation.Status != shared.DiagnosticCaptureTruncated {
		observation.Status = shared.DiagnosticCapturePartial
	}
	return observation
}

func failureObservation(cause error) shared.FailureDiagnosticObservation {
	observation := shared.FailureDiagnosticObservation{Status: shared.DiagnosticCaptureComplete}
	if cause == nil {
		return observation
	}
	observation.Error = cause.Error()
	observation.Message, observation.Reason = operationFailureDetails(cause)
	return observation
}

// observe*Failure records a known physical cause before returning ambiguity.
// It does not remove anything or grant terminal publication authority.
func (b *Backend) observeOperationFailure(ctx context.Context, mutations *storageMutations, subject shared.OperationPhysicalSubject, cause error) error {
	cleanup, err := newOperationFailureCleanup(b, mutations, subject)
	if err != nil {
		return err
	}
	_, err = cleanup.capture(ctx, cause)
	return err
}

func (b *Backend) observeMaintenanceFailure(ctx context.Context, mutations *storageMutations, subject shared.MaintenancePhysicalSubject, cause error) error {
	cleanup, err := newMaintenanceFailureCleanup(b, mutations, subject)
	if err != nil {
		return err
	}
	_, err = cleanup.capture(ctx, cause)
	return err
}
