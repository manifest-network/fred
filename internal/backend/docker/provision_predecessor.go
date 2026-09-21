package docker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// preparedProvisionOperation is the private admission phase after validating
// and, when necessary, freezing the predecessor. It owns the detached candidate
// and exact issuing settlement; callers cannot splice another request into its
// eventual acceptance. It grants no physical execution or failure authority.
type preparedProvisionOperation struct {
	settlement operationSettlementService
	candidate  shared.OperationIntentCandidate
}

func (b *Backend) prepareProvisionOperation(
	ctx context.Context,
	request provisionOperationInput,
	desiredItems []backend.LeaseItem,
	profiles []shared.SKUResourceSnapshot,
	healthCheckServices []string,
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
		Items: desiredItems, ResourceProfiles: profiles, EffectiveItems: request.Items,
		HealthCheckServices: healthCheckServices, Manifest: request.Payload,
	})
	if err != nil {
		return preparedProvisionOperation{}, fmt.Errorf("construct provision operation intent: %w", err)
	}
	if err := b.prepareProvisionPredecessor(ctx, request); err != nil {
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

// prepareProvisionPredecessor finishes the read-only admission prerequisite and
// the exact legacy identity backfill before a replacement operation is durable.
// The request remains caller-owned work until that acceptance boundary. This
// observation grants no teardown authority: the Started executor still requires
// its claim-bound classification and resource admission.
func (b *Backend) prepareProvisionPredecessor(ctx context.Context, request provisionOperationInput) error {
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
	if !active.OperationID.IsZero() || active.RuntimeAuthority != nil || active.LegacyRuntimeAuthority != nil {
		return fmt.Errorf("%w: provision predecessor has incomplete runtime authority", backend.ErrInvalidState)
	}
	if len(active.Items) == 0 || validateDockerResourceProfiles(active.Items, active.ResourceProfiles) != nil {
		return fmt.Errorf("%w: legacy provision predecessor has incomplete topology or resource authority", backend.ErrInvalidState)
	}
	inventoryCtx, cancel := b.recoveryDockerReadContext(ctx)
	defer cancel()
	all, err := b.listManagedContainersStrictForRecovery(inventoryCtx)
	if err != nil {
		return fmt.Errorf("inspect legacy provision admission predecessor: %w", err)
	}
	var cohort []ContainerInfo
	for _, listed := range all {
		if listed.LeaseUUID != request.LeaseUUID || strings.HasSuffix(listed.Name, "-prev") {
			continue
		}
		inspected, err := b.inspectContainerForRecovery(inventoryCtx, listed.ContainerID)
		if err != nil {
			return fmt.Errorf("inspect legacy admission container %q: %w", listed.ContainerID, err)
		}
		if inspected == nil || inspected.ContainerID != listed.ContainerID ||
			inspected.LeaseUUID != request.LeaseUUID || inspected.BackendName != b.Name() {
			return fmt.Errorf("%w: legacy admission container identity changed", backend.ErrInvalidState)
		}
		cohort = append(cohort, *inspected)
	}
	if err := validateRecoveredReleaseCohort(active, cohort); err != nil {
		return fmt.Errorf("%w: legacy provision predecessor cannot freeze its complete cohort: %w", backend.ErrInvalidState, err)
	}
	// Complete topology establishes a nonempty cohort. Freezing observed identity
	// before checking every member would allow one peer's callbacks to authorize
	// the others, so revalidate against the candidate identity before its CAS.
	first := cohort[0]
	if first.Tenant != request.Tenant || first.ProviderUUID != request.ProviderUUID {
		return fmt.Errorf("%w: legacy provision predecessor belongs to a different tenant or provider", backend.ErrInvalidState)
	}
	authority, err := shared.NewLegacyRuntimeAuthority(first.Tenant, first.ProviderUUID, first.CallbackURL, first.LifecycleCallbackURL)
	if err != nil {
		return fmt.Errorf("%w: legacy provision predecessor has no complete callback identity: %w", backend.ErrInvalidState, err)
	}
	frozen := *active
	frozen.LegacyRuntimeAuthority = &authority
	if err := validateRecoveredReleaseCohort(&frozen, cohort); err != nil {
		return fmt.Errorf("%w: legacy provision predecessor identity is inconsistent: %w", backend.ErrInvalidState, err)
	}
	if b.releaseBackfiller == nil {
		return fmt.Errorf("legacy provision admission requires the release backfiller")
	}
	if err := b.releaseBackfiller.BackfillLegacyRuntimeAuthorityContext(inventoryCtx, request.LeaseUUID, *active, authority); err != nil {
		return fmt.Errorf("freeze legacy provision admission predecessor: %w", err)
	}
	return nil
}
