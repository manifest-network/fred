package k3s

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type operationSettlementService interface {
	NewOperationIntentProbe(string, string) (shared.OperationIntentProbe, error)
	ProbeOperationIntent(shared.OperationIntentProbe) (shared.OperationIntentAdmissionDisposition, error)
	NewOperationIntentCandidate(shared.OperationIntentSpec) (shared.OperationIntentCandidate, error)
	BeginOperationIntent(shared.OperationIntentCandidate) (shared.OperationIntentAdmission, error)
	ListOperationIntents() ([]shared.OperationIntentClaim, error)
	PrepareOperationRelease(shared.OperationIntentClaim) (shared.OperationReleaseCandidate, error)
	RefuseOperationExecution(shared.OperationReleaseCandidate) (shared.OperationExecutionFailure, error)
	RecoverOperationExecution(context.Context, shared.LeaseRecoveryScope, shared.OperationIntentClaim) (shared.OperationExecutionOutcome, error)
	CommitOperationFailure(shared.OperationExecutionFailure) (shared.OperationReleaseUncommitted, error)
}

// bindK3sStubOperationExecutor fixes the scaffold's complete physical behavior
// at construction. The stub has no mutating operation at all, and its strict
// recovery classifier can therefore attest only exact absence. If K3s gains a
// real provisioner, construction must instead bind its narrow mutation facade
// and exhaustive Kubernetes inventory classifier here.
func bindK3sStubOperationExecutor(b *Backend, settlement *shared.OperationSettlement) error {
	if b == nil || settlement == nil {
		return errors.New("K3s operation substrate dependencies are required")
	}
	authorize := func(ctx context.Context, _ string) (context.Context, func(), error) {
		if err := b.VerifyStorageIdentity(ctx); err != nil {
			return nil, nil, err
		}
		return ctx, func() {}, nil
	}
	complete := func(ctx context.Context, _ string, _ error) error {
		return b.VerifyStorageIdentity(ctx)
	}
	return shared.BindOperationSubstrateExecutor(
		settlement,
		authorize,
		complete,
		func(substratemutation.Runner, shared.OperationPhysicalSubject) struct{} { return struct{}{} },
		func(context.Context, struct{}, shared.OperationPhysicalSubject) error { return nil },
		func(
			_ context.Context,
			subject shared.OperationPhysicalSubject,
		) (shared.OperationPhysicalEvidence, error) {
			return shared.NewOperationExactAbsent(subject)
		},
	)
}

func (b *Backend) publishOperationFailure(
	ctx context.Context,
	failure shared.OperationExecutionFailure,
	errMsg string,
) error {
	uncommitted, err := b.operationSettlement.CommitOperationFailure(failure)
	if err != nil {
		return err
	}
	if b.callbackPublisher == nil {
		return errors.New("callback publisher is required")
	}
	return b.callbackPublisher.PublishOperationFailureContext(ctx, uncommitted, errMsg)
}

// resolvePreEffectOperationRefusal is deliberately limited to the scaffold's
// NotStarted phase. A future real K3s mutator must use guarded execution and
// classifier-issued physical evidence after crossing Started.
func (b *Backend) resolvePreEffectOperationRefusal(
	claim shared.OperationIntentClaim,
	errMsg string,
) error {
	if b.operationSettlement == nil {
		return errors.New("operation settlement journals are required")
	}
	candidate, err := b.operationSettlement.PrepareOperationRelease(claim)
	if err != nil {
		return err
	}
	failure, err := b.operationSettlement.RefuseOperationExecution(candidate)
	if err != nil {
		return err
	}
	return b.publishOperationFailure(b.stopCtx, failure, errMsg)
}

func (b *Backend) resolveRecoveredOperation(
	ctx context.Context,
	scope shared.LeaseRecoveryScope,
	claim shared.OperationIntentClaim,
	errMsg string,
) error {
	outcome, err := b.operationSettlement.RecoverOperationExecution(ctx, scope, claim)
	if err != nil {
		return err
	}
	switch outcome := outcome.(type) {
	case shared.OperationExecutionFailure:
		return b.publishOperationFailure(ctx, outcome, errMsg)
	case shared.OperationExecutionAmbiguous:
		return fmt.Errorf("%w: K3s operation recovery is ambiguous: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, outcome.Cause())
	case shared.OperationExecutionSuccess:
		return errors.New("K3s scaffold recovery unexpectedly found a ready substrate")
	default:
		return fmt.Errorf("K3s operation recovery returned unsupported outcome %T", outcome)
	}
}

func (b *Backend) probeProvisionIntent(req backend.ProvisionRequest) (bool, error) {
	if b.operationSettlement == nil {
		return false, errors.New("durable callback store is required for asynchronous operation")
	}
	probe, err := b.operationSettlement.NewOperationIntentProbe(req.LeaseUUID, req.CallbackURL)
	if err != nil {
		return false, fmt.Errorf("construct exact provision redelivery probe: %w", err)
	}
	disposition, err := b.operationSettlement.ProbeOperationIntent(probe)
	if err != nil {
		return false, fmt.Errorf("probe exact provision redelivery: %w", err)
	}
	return disposition == shared.OperationIntentAdmissionExisting ||
		disposition == shared.OperationIntentAdmissionCompleted, nil
}

func (b *Backend) beginProvisionIntent(req backend.ProvisionRequest) (shared.OperationIntentClaim, bool, error) {
	if b.operationSettlement == nil {
		return shared.OperationIntentClaim{}, false, errors.New("durable callback store is required for asynchronous operation")
	}
	resourceProfiles, err := shared.BuildSKUResourceSnapshot(req.Items, b.cfg.GetSKUProfile)
	if err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("snapshot provision resource profiles: %w", err)
	}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            req.LeaseUUID,
		CallbackURL:          req.CallbackURL,
		LifecycleCallbackURL: req.LifecycleCallbackURL,
		Tenant:               req.Tenant,
		ProviderUUID:         req.ProviderUUID,
		Items:                req.Items,
		ResourceProfiles:     resourceProfiles,
		EffectiveItems:       req.Items,
		Manifest:             req.Payload,
	})
	if err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("construct provision operation intent: %w", err)
	}
	admission, err := b.operationSettlement.BeginOperationIntent(candidate)
	if err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("persist provision operation intent: %w", err)
	}
	claim, created := admission.CreatedClaim()
	if !created {
		return shared.OperationIntentClaim{}, false, nil
	}
	return claim, true, nil
}

func (b *Backend) refuseProvisionIntent(claim shared.OperationIntentClaim, cause error) error {
	if !claim.Valid() {
		return cause
	}
	authorityErr := b.terminalStorageAuthorityError()
	if errors.Is(authorityErr, backendidentity.ErrIdentityDrift) ||
		errors.Is(authorityErr, backendidentity.ErrMutationOutcomeAmbiguous) ||
		errors.Is(cause, backendidentity.ErrIdentityDrift) ||
		errors.Is(cause, backendidentity.ErrMutationOutcomeAmbiguous) {
		if authorityErr == nil {
			authorityErr = cause
		}
		return errors.Join(cause, fmt.Errorf(
			"%w: preserve provision operation intent for restart recovery: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, authorityErr,
		))
	}
	if err := b.resolvePreEffectOperationRefusal(
		claim, "backend refused operation before asynchronous acceptance",
	); err != nil {
		b.logger.Error("failed to settle refused provision intent",
			"error", err,
			"lease_uuid", claim.LeaseUUID(),
		)
		return fmt.Errorf("provision refused but durable intent settlement failed: %s: %w", cause.Error(), err)
	}
	return cause
}

// recoverOperationIntents is exact for the current K3s scaffold: every
// accepted operation deterministically produces no cluster objects and ends
// with the canonical not-implemented failure. A future real provisioner must
// replace this with strict substrate classification before it can accept work.
func (b *Backend) recoverOperationIntents(ctx context.Context) error {
	if b.operationSettlement == nil {
		return nil
	}
	claims, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return fmt.Errorf("list callback operation intents: %w", err)
	}
	for _, claim := range claims {
		if claim.Backend() != b.cfg.Name || claim.BackendStorageID() != b.storageIdentity {
			return fmt.Errorf("%s operation intent for lease %q belongs to backend %q storage %q",
				claim.Kind(), claim.LeaseUUID(), claim.Backend(), claim.BackendStorageID().String())
		}
		if claim.Kind() != shared.OperationIntentProvision {
			return fmt.Errorf("unsupported K3s operation intent kind %q for lease %q",
				claim.Kind(), claim.LeaseUUID())
		}
		if b.recoveryCoordinator == nil {
			return errors.New("K3s operation recovery coordinator is required")
		}
		_, recoveryErr := b.recoveryCoordinator.WithLease(
			ctx, claim.LeaseUUID(),
			func(scope shared.LeaseRecoveryScope) error {
				return b.resolveRecoveredOperation(ctx, scope, claim, stubProvisionerErrMsg)
			},
		)
		if recoveryErr != nil {
			return fmt.Errorf("resolve interrupted K3s provision %q: %w", claim.LeaseUUID(), recoveryErr)
		}
	}
	return nil
}
