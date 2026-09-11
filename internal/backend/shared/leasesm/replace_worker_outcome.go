package leasesm

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// ReplaceWorkOutcome is the closed result of a restore, restart, or update
// worker. A plain error is intentionally not representable: uncertainty must
// remain distinct from a failure backed by exact terminal journal evidence.
type ReplaceWorkOutcome interface {
	replaceWorkOutcome()
}

type replaceWorkTerminal struct{ result ReplaceResult }

type replaceWorkAmbiguous struct {
	err         error
	operation   shared.OperationIntentClaim
	maintenance shared.MaintenanceIntentClaim
}

func (replaceWorkTerminal) replaceWorkOutcome()  {}
func (replaceWorkAmbiguous) replaceWorkOutcome() {}

func NewRestoreWorkSuccess(
	proof shared.OperationReleaseCommitted,
) (ReplaceWorkOutcome, error) {
	ready, ok := proof.TargetReady()
	if !ok {
		return nil, errors.New("restore work success requires exact ready evidence")
	}
	release, ids, services := ready.Projection()
	stack, err := validateCompleteReleaseProjection(release, ids, services)
	if err != nil {
		return nil, fmt.Errorf("restore success evidence: %w", err)
	}
	success := newReplaceSuccessProjection(ReplaceSuccessProjection{
		ContainerIDs: ids, ServiceContainers: services,
	})
	success.authorityKind = replaceAuthorityRestoreCommitted
	success.operationRelease = proof
	success.release = &release
	success.stackManifest = stack
	return replaceWorkTerminal{result: ReplaceResult{success: success}}, nil
}

// NewRestoreWorkResult wraps only a definitive failure carrying its fresh
// exact uncommitted-operation proof. Success must use NewRestoreWorkSuccess so
// its actor projection is derived from classifier-sealed ready evidence.
func NewRestoreWorkResult(result ReplaceResult) (ReplaceWorkOutcome, error) {
	if result.err == nil || !result.failure.operationRelease.Valid() {
		return nil, errors.New("restore failure requires an exact operation failure proof")
	}
	return replaceWorkTerminal{result: result}, nil
}

func NewAmbiguousRestoreWork(
	err error,
	operation shared.OperationIntentClaim,
) (ReplaceWorkOutcome, error) {
	if err == nil || !operation.Valid() || operation.Kind() != shared.OperationIntentRestore {
		return nil, errors.New("ambiguous restore requires an error and exact pending restore")
	}
	return replaceWorkAmbiguous{err: err, operation: operation}, nil
}

func NewMaintenanceWorkSuccess(
	proof shared.MaintenanceReleaseActive,
) (ReplaceWorkOutcome, error) {
	ready, ok := proof.TargetReady()
	if !ok {
		return nil, errors.New("maintenance work success requires exact ready evidence")
	}
	_, ids, services := ready.Projection()
	release, ok := proof.TargetRelease()
	if !ok || release.Version <= 0 {
		return nil, errors.New("maintenance work success has no exact committed target")
	}
	stack, err := validateCompleteReleaseProjection(release, ids, services)
	if err != nil {
		return nil, fmt.Errorf("maintenance success evidence: %w", err)
	}
	success := newReplaceSuccessProjection(ReplaceSuccessProjection{
		ContainerIDs: ids, ServiceContainers: services,
	})
	success.authorityKind = replaceAuthorityMaintenance
	success.maintenanceRelease = proof
	success.maintenance = proof.Intent()
	success.release = &release
	success.stackManifest = stack
	return replaceWorkTerminal{result: ReplaceResult{success: success}}, nil
}

// NewMaintenanceWorkResult wraps only a definitive failed release. Success
// must use NewMaintenanceWorkSuccess to derive its projection from evidence.
func NewMaintenanceWorkResult(result ReplaceResult) (ReplaceWorkOutcome, error) {
	if result.err == nil || !result.failure.maintenanceRelease.Valid() {
		return nil, errors.New("maintenance failure requires an exact failed release proof")
	}
	return replaceWorkTerminal{result: result}, nil
}

func NewAmbiguousMaintenanceWork(
	err error,
	maintenance shared.MaintenanceIntentClaim,
) (ReplaceWorkOutcome, error) {
	if err == nil || !maintenance.Valid() ||
		(maintenance.Kind() != shared.MaintenanceIntentRestart &&
			maintenance.Kind() != shared.MaintenanceIntentUpdate &&
			maintenance.Kind() != shared.MaintenanceIntentCustomDomain) {
		return nil, errors.New("ambiguous maintenance requires an error and exact pending maintenance")
	}
	return replaceWorkAmbiguous{err: err, maintenance: maintenance}, nil
}

func (r ReplaceResult) validForRestoreClaim(claim shared.OperationIntentClaim) bool {
	if !claim.Valid() || claim.Kind() != shared.OperationIntentRestore {
		return false
	}
	if r.err == nil {
		return r.success.authorityKind == replaceAuthorityRestoreCommitted &&
			r.success.operationRelease.MatchesIntent(claim)
	}
	return r.failure.authorityKind == replaceAuthorityRestoreCommitted &&
		r.failure.operationRelease.MatchesIntent(claim)
}

func validateReplaceWorkOutcome(
	outcome ReplaceWorkOutcome,
	maintenance shared.MaintenanceIntentClaim,
	operation shared.OperationIntentClaim,
) error {
	if outcome == nil {
		return errors.New("replace worker returned no typed outcome")
	}
	switch typed := outcome.(type) {
	case replaceWorkTerminal:
		valid := typed.result.validForRestoreClaim(operation)
		if maintenance.Valid() {
			valid = typed.result.validForMaintenance(maintenance)
		}
		if !valid {
			return errors.New("replace terminal outcome belongs to another authority")
		}
	case replaceWorkAmbiguous:
		if typed.err == nil {
			return errors.New("ambiguous replace outcome has no cause")
		}
		if maintenance.Valid() {
			if !typed.maintenance.MatchesIntent(maintenance) {
				return errors.New("ambiguous maintenance outcome belongs to another generation")
			}
		} else if !operation.Valid() || !typed.operation.Valid() ||
			operation.BackendStorageID() != typed.operation.BackendStorageID() ||
			operation.Backend() != typed.operation.Backend() ||
			operation.OperationID() != typed.operation.OperationID() ||
			operation.LeaseUUID() != typed.operation.LeaseUUID() ||
			operation.CallbackURL() != typed.operation.CallbackURL() {
			return errors.New("ambiguous restore outcome belongs to another operation")
		}
	default:
		return fmt.Errorf("unknown replace work outcome %T", outcome)
	}
	return nil
}
