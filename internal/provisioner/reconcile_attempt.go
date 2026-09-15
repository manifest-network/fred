package provisioner

import (
	"context"
	"errors"
	"log/slog"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type attemptRedeliveryOutcome uint8

const (
	attemptRedeliveryDeferred attemptRedeliveryOutcome = iota
	attemptRedeliveryAccepted
	attemptRedeliveryRefused
)

type attemptRedeliveryResult struct {
	outcome attemptRedeliveryOutcome
	err     error
}

// attemptRecoveryCoordinator is a construction-bound, high-level authority:
// exact lease exclusion, durable-attempt joining, backend invocation and
// panic containment, result classification, and settlement all live here.
// Its zero value is unusable because no method can acquire an attempt without
// every bound dependency.
type attemptRecoveryCoordinator struct {
	operations *placement.ReconciliationCoordinator
	recovery   *placement.AttemptRecoveryCoordinator
}

func newAttemptRecoveryCoordinator(
	operations *placement.ReconciliationCoordinator,
) *attemptRecoveryCoordinator {
	return &attemptRecoveryCoordinator{operations: operations}
}

// redeliverPlacementAttempt supplies the recovery half of the durable
// write-ahead protocol. An unresolved attempt is not merely a reason to wait:
// it is enough exact authority to retry the same operation against the same
// backend. Every uncertainty retains that authority for a later sweep.
func (c *attemptRecoveryCoordinator) Redeliver(
	ctx context.Context,
	leaseUUID string,
	projected *placement.ProjectedReconciliationSweep,
) attemptRedeliveryResult {
	if projected == nil || !projected.Valid() {
		return attemptRedeliveryResult{err: errors.New("invalid durable redelivery authority")}
	}
	record := projected.Record(leaseUUID)
	metadata := record.AttemptMetadata()
	if record.Attempt == "" || !metadata.Valid() {
		return attemptRedeliveryResult{err: errors.New("invalid durable redelivery authority")}
	}
	if record.State() == placement.StateUnusable || record.Conflict {
		return attemptRedeliveryResult{err: errors.New("unusable placement cannot authorize redelivery")}
	}
	settlement := projected.RecoverAttempt(ctx, leaseUUID)
	if settlement.Refused() {
		slog.Info("reconcile: backend definitively refused exact durable operation redelivery",
			"lease_uuid", leaseUUID,
			"backend", record.Attempt,
			"operation_fingerprint", metadata.OperationID(),
			"operation_kind", metadata.Kind(),
			"error", settlement.CallErr(),
		)
		return attemptRedeliveryResult{outcome: attemptRedeliveryRefused}
	}
	if !settlement.Accepted() {
		return attemptRedeliveryResult{err: settlement.Err()}
	}
	slog.Info("reconcile: recovered exact durable backend operation",
		"lease_uuid", leaseUUID,
		"backend", record.Attempt,
		"operation_fingerprint", metadata.OperationID(),
		"operation_kind", metadata.Kind(),
	)
	return attemptRedeliveryResult{outcome: attemptRedeliveryAccepted}
}

// convergeTerminalPlacementAttempt closes the other half of exact redelivery:
// once the chain positively says the target is terminal, replaying provision or
// restore would be wrong, but leaving a request-never-received Attempt forever
// would wedge placement and topology retirement. Under the same Registry ->
// placement claim order as callbacks and live redelivery, synchronously tear
// down every exact durable candidate and promote the attempted backend to
// conservative closed-lease affinity. Promotion is intentional: Deprovision may
// have retained data, and the bundled backends durably enqueue the exact failed
// operation followed by the lifecycle teardown observation before returning.
// A true no-op therefore leaves only a ghost owner that normal terminal/absent
// inventory pruning removes on a later sweep.
func (c *attemptRecoveryCoordinator) ConvergeTerminal(
	ctx context.Context,
	leaseUUID string,
	projected *placement.ProjectedReconciliationSweep,
) (bool, error) {
	if projected == nil || !projected.Valid() {
		return false, errors.New("invalid terminal durable-attempt authority")
	}
	record := projected.Record(leaseUUID)
	metadata := record.AttemptMetadata()
	if record.Attempt == "" || !metadata.Valid() {
		return false, errors.New("invalid terminal durable-attempt authority")
	}
	if record.State() == placement.StateUnusable || record.Conflict {
		return false, errors.New("unusable placement cannot authorize terminal teardown")
	}
	settlement := projected.RecoverAttempt(ctx, leaseUUID)
	if !settlement.Accepted() {
		return false, settlement.Err()
	}
	slog.Info("reconcile: converged terminal durable operation by exact teardown",
		"lease_uuid", leaseUUID,
		"backend", record.Attempt,
		"operation_fingerprint", metadata.OperationID(),
		"operation_kind", metadata.Kind(),
	)
	return true, nil
}
