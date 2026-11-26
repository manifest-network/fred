package docker

import (
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// preparedProvisionOperation is the private admission phase after validating
// the predecessor. It owns the detached candidate
// and exact issuing settlement; callers cannot splice another request into its
// eventual acceptance. It grants no physical execution or failure authority.
type preparedProvisionOperation struct {
	settlement operationSettlementService
	candidate  shared.OperationIntentCandidate
}

func (b *Backend) prepareProvisionOperation(
	request provisionOperationInput,
	desiredItems []backend.LeaseItem,
	ingress effectiveIngressPlan,
	profiles []shared.SKUResourceSnapshot,
	healthCheckServices []string,
	manifestAdmission shared.ProvisionManifestAdmission,
) (preparedProvisionOperation, error) {
	if b.operationSettlement == nil {
		return preparedProvisionOperation{}, errors.New("provision admission requires the operation settlement")
	}
	if err := validateDockerResourceProfiles(desiredItems, profiles); err != nil {
		return preparedProvisionOperation{}, fmt.Errorf("validate provision operation resource profiles: %w", err)
	}
	candidate, err := b.operationSettlement.NewOperationIntentCandidate(shared.OperationIntentSpec{
		Kind: shared.OperationIntentProvision, LeaseUUID: request.LeaseUUID,
		CallbackURL: request.CallbackURL, LifecycleCallbackURL: request.LifecycleCallbackURL,
		Tenant: request.Tenant, ProviderUUID: request.ProviderUUID,
		Items: desiredItems, ResourceProfiles: profiles, EffectiveItems: ingress.effectiveItems(),
		HealthCheckServices: healthCheckServices, Manifest: request.Payload,
	})
	if err != nil {
		return preparedProvisionOperation{}, fmt.Errorf("construct provision operation intent: %w", err)
	}
	candidate, err = manifestAdmission.Bind(candidate)
	if err != nil {
		return preparedProvisionOperation{}, err
	}
	return preparedProvisionOperation{settlement: b.operationSettlement, candidate: candidate}, nil
}

// begin is called inside the recovery publication bridge. Slow predecessor
// observation has already finished; only acceptance and projection publication
// need to exclude recovery. Exact redelivery retains the journal's Existing or
// Completed result and never grants a second dispatch.
func (prepared preparedProvisionOperation) begin() (shared.OperationIntentClaim, bool, error) {
	if prepared.settlement == nil {
		return shared.OperationIntentClaim{}, false, errors.New("provision admission has no prepared predecessor")
	}
	admission, err := prepared.settlement.BeginOperationIntent(prepared.candidate)
	if err != nil {
		return shared.OperationIntentClaim{}, false, fmt.Errorf("persist provision operation intent: %w", err)
	}
	claim, created := admission.CreatedClaim()
	return claim, created, nil
}

// prepareProvisionPredecessor finishes the read-only admission prerequisite
// before a replacement operation is durable: an active predecessor must record
// its runtime authority and belong to the requesting tenant and provider.
// The request remains caller-owned work until that acceptance boundary. This
// observation grants no teardown authority: the Started executor still requires
// its claim-bound classification and resource admission.
func (b *Backend) prepareProvisionPredecessor(request provisionOperationInput) error {
	if b.releaseStore == nil {
		return fmt.Errorf("provision predecessor requires the release journal")
	}
	active, err := b.releaseStore.LatestActive(request.LeaseUUID)
	if err != nil {
		return fmt.Errorf("read provision admission predecessor: %w", err)
	}
	if active == nil {
		return nil
	}
	if identity, ok := active.RuntimeIdentity(); ok {
		if identity.Tenant() != request.Tenant || identity.ProviderUUID() != request.ProviderUUID {
			return fmt.Errorf("%w: provision predecessor belongs to a different tenant or provider", backend.ErrInvalidState)
		}
		return nil
	}
	return fmt.Errorf("%w: provision predecessor has incomplete runtime authority", backend.ErrInvalidState)
}
