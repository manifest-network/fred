package k3s

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type operationIntentJournal interface {
	ProbeOperationIntent(shared.OperationIntentProbe) (shared.OperationIntentAdmissionDisposition, error)
	BeginOperationIntent(shared.OperationIntentSpec) (shared.OperationIntentAdmission, error)
	ResolveOperationIntent(shared.OperationIntentClaim, backend.CallbackStatus, string) (shared.CallbackEntry, error)
}

func (b *Backend) probeProvisionIntent(req backend.ProvisionRequest) (bool, error) {
	if b.operationIntents == nil {
		return false, errors.New("durable callback store is required for asynchronous operation")
	}
	disposition, err := b.operationIntents.ProbeOperationIntent(shared.OperationIntentProbe{
		LeaseUUID:        req.LeaseUUID,
		CallbackURL:      req.CallbackURL,
		Backend:          b.cfg.Name,
		BackendStorageID: b.storageIdentity,
	})
	if err != nil {
		return false, fmt.Errorf("probe exact provision redelivery: %w", err)
	}
	return disposition == shared.OperationIntentAdmissionExisting ||
		disposition == shared.OperationIntentAdmissionCompleted, nil
}

func (b *Backend) beginProvisionIntent(req backend.ProvisionRequest) (*shared.OperationIntentClaim, bool, error) {
	if b.operationIntents == nil {
		return nil, false, errors.New("durable callback store is required for asynchronous operation")
	}
	resourceProfiles, err := shared.BuildSKUResourceSnapshot(req.Items, b.cfg.GetSKUProfile)
	if err != nil {
		return nil, false, fmt.Errorf("snapshot provision resource profiles: %w", err)
	}
	admission, err := b.operationIntents.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            req.LeaseUUID,
		CallbackURL:          req.CallbackURL,
		LifecycleCallbackURL: req.LifecycleCallbackURL,
		Backend:              b.cfg.Name,
		BackendStorageID:     b.storageIdentity,
		Tenant:               req.Tenant,
		ProviderUUID:         req.ProviderUUID,
		Items:                req.Items,
		ResourceProfiles:     resourceProfiles,
		EffectiveItems:       req.Items,
		Manifest:             req.Payload,
	})
	if err != nil {
		return nil, false, fmt.Errorf("persist provision operation intent: %w", err)
	}
	if admission.Disposition != shared.OperationIntentAdmissionCreated {
		return nil, false, nil
	}
	return &admission.Claim, true, nil
}

func (b *Backend) refuseProvisionIntent(claim *shared.OperationIntentClaim, cause error) error {
	if claim == nil {
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
	if _, err := b.operationIntents.ResolveOperationIntent(
		*claim, backend.CallbackStatusFailed, "backend refused operation before asynchronous acceptance",
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
func (b *Backend) recoverOperationIntents() error {
	if b.callbackStore == nil {
		return nil
	}
	claims, err := b.callbackStore.ListOperationIntents()
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
		if _, err := b.callbackStore.ResolveOperationIntent(
			claim, backend.CallbackStatusFailed, stubProvisionerErrMsg,
		); err != nil {
			return fmt.Errorf("resolve interrupted K3s provision %q: %w", claim.LeaseUUID(), err)
		}
	}
	return nil
}
