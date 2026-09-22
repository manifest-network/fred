package shared

import (
	"context"
	"errors"
	"sync"
)

// RecoveryLeaseExclusion is the construction seam which proves that live work
// for one lease cannot overlap recovery. The implementation must hold its
// exclusion until run returns. acquired=false is a normal level-triggered
// deferral, not an error.
type RecoveryLeaseExclusion func(
	context.Context,
	string,
	func() error,
) (acquired bool, err error)

// RecoveryCoordinatorConfig binds every recovery authority for one backend
// lineage to the same lease-exclusion implementation. Nil settlements are
// permitted for substrates which implement only a subset of the protocols.
type RecoveryCoordinatorConfig struct {
	Operations   *OperationSettlement
	Maintenance  *MaintenanceSettlement
	Close        *CloseSettlement
	ExcludeLease RecoveryLeaseExclusion
	// ValidateActorClose is installed by the actor-owning substrate. Its input
	// is intentionally opaque here to avoid a shared <-> leasesm import cycle;
	// the implementation must accept only leasesm's unforgeable active scope for
	// the exact registered actor and coordinator lineage.
	ValidateActorClose func(ActorCloseScope, RecoveryLineage) (leaseUUID string, valid bool)
}

// ActorCloseScope is the narrow observation surface accepted at the shared
// package boundary. Implementing it does not grant authority: the
// construction-bound validator must still prove leasesm's exact active actor
// value and coordinator lineage before a recovery scope is minted.
type ActorCloseScope interface {
	LeaseUUID() string
}

// RecoveryCoordinator is the sole issuer of callback-lifetime recovery
// authority. Durable claims describe what may be recovered; they do not prove
// that concurrent live work is excluded.
type RecoveryCoordinator struct {
	operations         *OperationSettlement
	maintenance        *MaintenanceSettlement
	close              *CloseSettlement
	exclude            RecoveryLeaseExclusion
	lineage            RecoveryLineage
	validateActorClose func(ActorCloseScope, RecoveryLineage) (string, bool)
}

// RecoveryLineage is an opaque comparable identity passed into a lease actor
// at construction. Possessing it does not mint recovery authority; it only lets
// the coordinator reject an active scope from another backend construction.
type RecoveryLineage struct{ identity *recoveryLineageIdentity }

// Non-zero size is load-bearing: Go permits distinct zero-sized allocations
// to have equal addresses, which would collapse two coordinator lineages.
type recoveryLineageIdentity struct{ _ byte }

// LeaseRecoveryScope is an opaque, exact-lease capability valid only while a
// RecoveryCoordinator callback is running. Copies share revocation state, so a
// retained copy becomes invalid as soon as the callback returns or panics.
type LeaseRecoveryScope struct{ state *leaseRecoveryScopeState }

type leaseRecoveryScopeState struct {
	coordinator *RecoveryCoordinator
	leaseUUID   string

	mu       sync.Mutex
	active   bool
	inFlight int
	drained  *sync.Cond
}

func NewRecoveryCoordinator(config RecoveryCoordinatorConfig) (*RecoveryCoordinator, error) {
	if config.ExcludeLease == nil {
		return nil, errors.New("recovery coordinator requires lease exclusion")
	}
	if (config.Close == nil) != (config.ValidateActorClose == nil) {
		return nil, errors.New("close settlement and actor-close validator must be configured together")
	}
	if config.Close != nil && !config.Close.valid() {
		return nil, errors.New("recovery coordinator requires an open close settlement")
	}
	if !recoverySettlementsShareJournalPair(
		config.Operations, config.Maintenance, config.Close,
	) {
		return nil, errors.New("recovery coordinator settlements belong to different journal pairs")
	}
	coordinator := &RecoveryCoordinator{
		operations: config.Operations, maintenance: config.Maintenance,
		close: config.Close, exclude: config.ExcludeLease,
		lineage:            RecoveryLineage{identity: &recoveryLineageIdentity{}},
		validateActorClose: config.ValidateActorClose,
	}
	if config.Operations != nil && config.Operations.recoveryCoordinator != nil {
		return nil, errors.New("operation settlement already belongs to a recovery coordinator")
	}
	if config.Maintenance != nil && config.Maintenance.recoveryCoordinator != nil {
		return nil, errors.New("maintenance settlement already belongs to a recovery coordinator")
	}
	if config.Close != nil && config.Close.recoveryCoordinator != nil {
		return nil, errors.New("close settlement already belongs to a recovery coordinator")
	}
	if config.Operations != nil {
		config.Operations.recoveryCoordinator = coordinator
	}
	if config.Maintenance != nil {
		config.Maintenance.recoveryCoordinator = coordinator
	}
	if config.Close != nil {
		config.Close.recoveryCoordinator = coordinator
	}
	return coordinator, nil
}

func recoverySettlementsShareJournalPair(
	operations *OperationSettlement,
	maintenance *MaintenanceSettlement,
	close *CloseSettlement,
) bool {
	var expected *journalPair
	for _, pair := range []*journalPair{
		journalPairOfOperationSettlement(operations),
		journalPairOfMaintenanceSettlement(maintenance),
		journalPairOfCloseSettlement(close),
	} {
		if pair == nil {
			continue
		}
		if !pair.valid() {
			return false
		}
		if expected == nil {
			expected = pair
		} else if pair.callbacks != expected.callbacks || pair.releases != expected.releases {
			return false
		}
	}
	return true
}

func journalPairOfOperationSettlement(settlement *OperationSettlement) *journalPair {
	if settlement == nil {
		return nil
	}
	return &settlement.journalPair
}

func journalPairOfMaintenanceSettlement(settlement *MaintenanceSettlement) *journalPair {
	if settlement == nil {
		return nil
	}
	return &settlement.journalPair
}

func journalPairOfCloseSettlement(settlement *CloseSettlement) *journalPair {
	if settlement == nil {
		return nil
	}
	return &settlement.journalPair
}

// WithLease holds the configured live-work exclusion for the complete
// callback. A scope cannot escape: its shared state is revoked by defer even
// when work panics.
func (c *RecoveryCoordinator) WithLease(
	ctx context.Context,
	leaseUUID string,
	work func(LeaseRecoveryScope) error,
) (bool, error) {
	if c == nil || c.exclude == nil {
		return false, errors.New("recovery coordinator is not bound")
	}
	if ctx == nil {
		return false, errors.New("recovery context is required")
	}
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return false, err
	}
	if work == nil {
		return false, errors.New("recovery callback is required")
	}
	return c.exclude(ctx, leaseUUID, func() error {
		return c.withScope(leaseUUID, work)
	})
}

func (c *RecoveryCoordinator) withScope(
	leaseUUID string,
	work func(LeaseRecoveryScope) error,
) error {
	state := &leaseRecoveryScopeState{
		coordinator: c, leaseUUID: leaseUUID, active: true,
	}
	state.drained = sync.NewCond(&state.mu)
	// Revocation joins every consumer which entered while the callback owned
	// exclusion. A goroutine cannot pass validation, escape the callback, and
	// keep mutating after the live-work gate is released.
	defer state.revokeAndWait()
	return work(LeaseRecoveryScope{state: state})
}

// WithActorClose exchanges an active exact-actor close scope for the same
// short-lived recovery authority used by background convergence. No command or
// actor lock is reacquired: the actor already owns serial execution and has
// drained its workers before minting the supplied scope.
func (c *RecoveryCoordinator) WithActorClose(
	actorScope ActorCloseScope,
	work func(LeaseRecoveryScope) error,
) error {
	if c == nil || work == nil || c.validateActorClose == nil {
		return errors.New("actor close scope does not belong to this recovery coordinator")
	}
	leaseUUID, valid := c.validateActorClose(actorScope, c.lineage)
	if !valid || validateCanonicalLeaseUUID(leaseUUID) != nil {
		return errors.New("actor close scope does not belong to this recovery coordinator")
	}
	return c.withScope(leaseUUID, work)
}

func (c *RecoveryCoordinator) Lineage() RecoveryLineage {
	if c == nil {
		return RecoveryLineage{}
	}
	return c.lineage
}

// BoundTo reports whether this coordinator owns the exact settlement objects.
// It supports composition roots which install optional protocols in stages;
// it does not expose or mint a recovery scope.
func (c *RecoveryCoordinator) BoundTo(
	operations *OperationSettlement,
	maintenance *MaintenanceSettlement,
	close *CloseSettlement,
) bool {
	return c != nil && c.operations == operations && c.maintenance == maintenance && c.close == close
}

func (scope LeaseRecoveryScope) validFor(
	coordinator *RecoveryCoordinator,
	leaseUUID string,
) bool {
	state := scope.state
	if state == nil || state.coordinator != coordinator || state.leaseUUID != leaseUUID {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.active
}

// enter pins the callback-lifetime exclusion for one complete authority
// consumer call. Validation and refcount acquisition are one mutex operation;
// revocation either observes this use and joins it or makes the copied scope
// permanently ineligible.
func (scope LeaseRecoveryScope) enter(
	coordinator *RecoveryCoordinator,
	leaseUUID string,
) (func(), bool) {
	state := scope.state
	if state == nil || state.coordinator != coordinator || state.leaseUUID != leaseUUID {
		return nil, false
	}
	state.mu.Lock()
	if !state.active {
		state.mu.Unlock()
		return nil, false
	}
	state.inFlight++
	state.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			state.mu.Lock()
			state.inFlight--
			if state.inFlight == 0 {
				state.drained.Broadcast()
			}
			state.mu.Unlock()
		})
	}, true
}

func (state *leaseRecoveryScopeState) revokeAndWait() {
	if state == nil {
		return
	}
	state.mu.Lock()
	state.active = false
	for state.inFlight != 0 {
		state.drained.Wait()
	}
	state.mu.Unlock()
}
