package leasesm

import (
	"errors"
	"fmt"
	"slices"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// DurableRuntime is the runtime a projection returns to once the provision or
// restore it awaited failed with nothing of it left on the substrate
// (ENG-1125). For a re-provision of a lease with an active Release it is that
// predecessor: its exact runtime identity and topology. Provision admission
// refuses to replace a lease whose projection's callback pair differs from its
// active Release, and recovery keeps a Failed projection with no containers
// whole, so a Failed projection left carrying the failed candidate's pair
// would refuse every later re-provision. Without a predecessor (a first
// provision, or a restore destination) it is the failed claim's own callback
// pair. In both cases no container is published.
//
// Every fallible step (parsing the predecessor's manifest, validating its
// quantities) happens in NewDurableRuntime, before any projection is touched,
// so applying one cannot fail. The lease actor applies it inside its own
// Provisioning -> Failed transition for a definite startup failure, and
// operation recovery applies it while it is the projection's only writer. The
// zero value is invalid and applies nothing.
type DurableRuntime struct{ state *durableRuntimeState }

type durableRuntimeState struct {
	operation shared.OperationID
	lease     string
	// predecessor selects between the two durable runtimes; when it is false,
	// only the callback pair below is restored.
	predecessor          bool
	tenant               string
	providerUUID         string
	callbackURL          string
	lifecycleCallbackURL string
	releaseVersion       int
	activeOperation      shared.OperationID
	sku                  string
	quantity             int
	items                []backend.LeaseItem
	profiles             []shared.SKUResourceSnapshot
	stack                *manifest.StackManifest
}

// NewDurableRuntime derives the durable runtime for the failed claim from the
// lease's active Release, nil when it has none.
func NewDurableRuntime(claim shared.OperationIntentClaim, active *shared.Release) (DurableRuntime, error) {
	if !claim.Valid() || !claim.OperationID().Valid() {
		return DurableRuntime{}, errors.New("durable runtime requires a valid operation claim")
	}
	state := &durableRuntimeState{
		operation: claim.OperationID(), lease: claim.LeaseUUID(),
		callbackURL: claim.CallbackURL(), lifecycleCallbackURL: claim.LifecycleCallbackURL(),
	}
	if active == nil || len(active.Items) == 0 {
		return DurableRuntime{state: state}, nil
	}
	identity, ok := active.RuntimeIdentity()
	if !ok {
		return DurableRuntime{state: state}, nil
	}
	stack, err := manifest.ParseStoredPayload(active.Manifest)
	if err != nil {
		return DurableRuntime{}, fmt.Errorf("parse failed provision predecessor manifest: %w", err)
	}
	quantity, err := backend.ValidateOperationQuantities(active.Items)
	if err != nil {
		return DurableRuntime{}, fmt.Errorf("validate failed provision predecessor quantities: %w", err)
	}
	state.predecessor = true
	state.tenant, state.providerUUID = identity.Tenant(), identity.ProviderUUID()
	state.callbackURL, state.lifecycleCallbackURL = identity.CallbackURL(), identity.LifecycleCallbackURL()
	state.releaseVersion, state.activeOperation = active.Version, identity.OperationID()
	state.sku, state.quantity = active.Items[0].SKU, quantity
	state.items = slices.Clone(active.Items)
	state.profiles = shared.CloneSKUResourceSnapshot(active.ResourceProfiles)
	state.stack = stack
	return DurableRuntime{state: state}, nil
}

// Valid reports whether r was derived by NewDurableRuntime.
func (r DurableRuntime) Valid() bool { return r.state != nil }

// awaitedBy reports whether r was derived for operationID.
func (r DurableRuntime) awaitedBy(operationID shared.OperationID) bool {
	return r.state != nil && operationID.Valid() && r.state.operation == operationID
}

// Apply reduces p's runtime identity and topology to r and publishes no
// container. It leaves status, diagnostics and the terminal budget to the
// caller's own transition. A projection of another lease, and the zero value,
// are left untouched.
func (r DurableRuntime) Apply(p *ProvisionState) {
	if r.state == nil || p == nil || p.LeaseUUID != r.state.lease {
		return
	}
	s := r.state
	if s.predecessor {
		p.Tenant, p.ProviderUUID = s.tenant, s.providerUUID
		p.ActiveReleaseVersion, p.ActiveOperationID = s.releaseVersion, s.activeOperation
		p.SKU, p.Quantity = s.sku, s.quantity
		// Clones: the projection never aliases this value's slices.
		p.Items = slices.Clone(s.items)
		p.ResourceProfiles = shared.CloneSKUResourceSnapshot(s.profiles)
		p.StackManifest = s.stack
	}
	p.CallbackURL, p.LifecycleCallbackURL = s.callbackURL, s.lifecycleCallbackURL
	p.ContainerIDs = nil
	p.ServiceContainers = nil
}
