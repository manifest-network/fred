package placement

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

var errInventoryProjectorConflict = errors.New("placement inventory projector is already bound")

type inventoryProjectorMarker struct{ _ byte }

// inventoryProjector is the single construction boundary joining an inventory
// collector to the exact Store that consumes its snapshots. Its zero value is
// invalid, and a Store can bind only one topology-exact projector.
type inventoryProjector struct {
	store     *Store
	collector *inventory.Collector
	marker    *inventoryProjectorMarker
}

// bindInventoryProjector creates or returns the Store's one topology-bound
// inventory authority. Repeating the exact topology is idempotent; attempting
// to splice another topology into the same Store is rejected.
func (s *Store) bindInventoryProjector(backendNames []string) (*inventoryProjector, error) {
	if s == nil {
		return nil, errors.New("placement Store is required")
	}
	collector, err := inventory.NewCollector(backendNames)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inventoryProjector != nil {
		if s.inventoryProjector.valid() &&
			s.inventoryProjector.collector.Binding().MatchesTopology(backendNames) {
			return s.inventoryProjector, nil
		}
		return nil, errInventoryProjectorConflict
	}
	if !collector.Binding().MatchesTopology(s.backendTopology) {
		return nil, fmt.Errorf(
			"%w: collector topology does not match the configured Store topology",
			errInventoryProjectorConflict,
		)
	}
	projector := &inventoryProjector{
		store: s, collector: collector, marker: &inventoryProjectorMarker{},
	}
	s.inventoryProjector = projector
	s.inventoryEvidence = collector.Binding()
	return projector, nil
}

// Valid reports whether projector is the exact instance retained by its Store.
func (projector *inventoryProjector) valid() bool {
	return projector != nil && projector.store != nil &&
		projector.collector != nil && projector.marker != nil &&
		projector.collector.Binding().Valid()
}

// beginCollection invalidates all older snapshots and returns the only mutable
// sink capable of issuing evidence accepted by this projector.
func (projector *inventoryProjector) beginCollection() *inventory.Session {
	if !projector.valid() {
		return nil
	}
	return projector.collector.Begin()
}

// complete classifies snapshot only against this exact Store-bound collector.
func (projector *inventoryProjector) complete(snapshot inventory.Snapshot) bool {
	return projector.valid() && snapshot.Complete(projector.collector.Binding())
}

// project consumes a projection only through the Store-bound collector
// identity. The private projection carries observations; its sealed Snapshot is
// the sole source of positive/negative reporter authority and completeness.
func (projector *inventoryProjector) project(
	fence inventoryFence,
	projection inventoryProjection,
) (projectionResult, error) {
	if !projector.valid() {
		return projectionResult{}, errInventoryProjectorConflict
	}
	projector.deriveProvisionAuthority(fence, &projection)
	return projector.store.projectInventory(fence, projection)
}

// deriveProvisionAuthority joins lifecycle and runtime-principal identity to
// the exact provision row sealed in the collector Snapshot. These maps are
// private Store inputs: API/reconciler callers can select placement policy, but
// cannot independently select the generation or principal that settles it.
func (projector *inventoryProjector) deriveProvisionAuthority(
	fence inventoryFence,
	projection *inventoryProjection,
) {
	if projector == nil || !projector.valid() || projection == nil ||
		!projection.AbsenceEvidence.ValidFor(projector.collector.Binding()) {
		return
	}

	projection.lifecycles = make(map[string]LifecycleObservation, len(projection.Placements))
	projection.runtimePrincipals = make(
		map[string]RuntimePrincipalObservation, len(projection.Placements),
	)
	complete := projection.AbsenceEvidence.Complete(projector.collector.Binding())
	storageIDs := projection.AbsenceEvidence.StorageIdentities(projector.collector.Binding())
	for leaseUUID, backendName := range projection.Placements {
		row, provisioned := projection.AbsenceEvidence.Provision(
			projector.collector.Binding(), backendName, leaseUUID,
		)
		if !provisioned {
			// A retention-derived placement intentionally carries no live lifecycle
			// generation or runtime maintenance principal.
			continue
		}
		generation := sealedLifecycleObservation(row.LifecycleGeneration())
		projection.lifecycles[leaseUUID] = generation
		if !complete {
			continue
		}
		principal, err := projector.store.mintRuntimePrincipalObservation(
			fence,
			storageIDs[backendName],
			backend.ProvisionInfo{
				LeaseUUID: row.LeaseUUID(), BackendName: row.BackendName(),
				ProviderUUID: row.ProviderUUID(), Tenant: row.Tenant(),
				LifecycleGeneration: row.LifecycleGeneration(),
			},
		)
		if err != nil {
			// A malformed/missing identity cannot gain maintenance authority, but
			// it must not discard conservative placement membership.
			slog.Warn("inventory provision cannot establish runtime principal",
				"lease_uuid", leaseUUID,
				"backend", backendName,
				"error", err,
			)
			continue
		}
		projection.runtimePrincipals[leaseUUID] = principal
	}
}

func sealedLifecycleObservation(
	observation *backend.LifecycleGenerationObservation,
) LifecycleObservation {
	if observation == nil {
		return LifecycleObservation{Kind: LifecycleObservationUnknown}
	}
	switch observation.Kind {
	case backend.LifecycleGenerationUnknown:
		if observation.ID == "" {
			return LifecycleObservation{Kind: LifecycleObservationUnknown}
		}
	case backend.LifecycleGenerationLegacy:
		if observation.ID == "" {
			return LifecycleObservation{Kind: LifecycleObservationLegacy}
		}
	case backend.LifecycleGenerationTyped:
		id, err := lifecycle.ParseID(observation.ID)
		if err == nil {
			return LifecycleObservation{Kind: LifecycleObservationTyped, ID: id}
		}
	case backend.LifecycleGenerationUnusable:
		if observation.ID == "" {
			return LifecycleObservation{Kind: LifecycleObservationUnusable}
		}
	}
	// A malformed upgraded observation is itself evidence that lifecycle
	// authority cannot be interpreted safely for this lease.
	return LifecycleObservation{Kind: LifecycleObservationUnusable}
}

func (projector *inventoryProjector) matchesStore(store *Store) bool {
	return projector.valid() && store != nil && projector.store == store &&
		store.inventoryProjector == projector
}
