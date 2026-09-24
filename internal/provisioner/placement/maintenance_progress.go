package placement

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	bolt "go.etcd.io/bbolt"
)

// The wire discriminant records durable knowledge. It is not mutation
// authority: only the distinct work capabilities below reach transitions.
type maintenanceJournalPhase uint8

const (
	maintenancePhaseInvalid maintenanceJournalPhase = iota
	maintenanceDeliveryOutstanding
	maintenanceCompletionOutstanding
	maintenancePayloadConfirmed
	maintenanceCompleted
)

func (phase maintenanceJournalPhase) String() string {
	switch phase {
	case maintenanceDeliveryOutstanding:
		return "delivery_outstanding"
	case maintenanceCompletionOutstanding:
		return "completion_outstanding"
	case maintenancePayloadConfirmed:
		return "confirmed_payload_outstanding"
	default:
		return ""
	}
}

func (phase maintenanceJournalPhase) validFor(kind MaintenanceCommandKind, terminal bool) bool {
	if terminal {
		return phase == maintenanceCompleted
	}
	return phase == maintenanceDeliveryOutstanding ||
		((phase == maintenanceCompletionOutstanding || phase == maintenancePayloadConfirmed) && kind == MaintenanceCommandUpdate)
}

func decodeMaintenancePhase(raw string, outcome MaintenanceCommandOutcome) (maintenanceJournalPhase, error) {
	if outcome != MaintenanceOutcomePending {
		if raw != "" {
			return maintenancePhaseInvalid, errors.New("terminal maintenance receipt carries pending phase")
		}
		return maintenanceCompleted, nil
	}
	switch raw {
	case "", "delivery_outstanding":
		// Legacy Pending was written before dispatch. It cannot prove that an
		// earlier process never sent the request, so reopen preserves ambiguity.
		return maintenanceDeliveryOutstanding, nil
	case "payload_outstanding", "completion_outstanding":
		// Old payload_outstanding proved HTTP acceptance only, never execution
		// success. Reopen must retain the last committed payload until a receipt.
		return maintenanceCompletionOutstanding, nil
	case "confirmed_payload_outstanding":
		return maintenancePayloadConfirmed, nil
	default:
		return maintenancePhaseInvalid, errors.New("unknown maintenance journal phase")
	}
}

type maintenanceWork interface{ maintenanceWork() }

// Only delivery work may be reauthorized for a physical backend call.
type maintenanceDelivery struct{ claim MaintenanceCommandClaim }

func (maintenanceDelivery) maintenanceWork() {}
func (work maintenanceDelivery) valid() bool {
	return work.claim.Valid() && work.claim.command.phase == maintenanceDeliveryOutstanding
}

// Accepted update work waits for an exact completion before local payload
// persistence, or retires after exact terminal chain evidence. It cannot
// become backend dispatch work.
type acceptedMaintenanceUpdate struct{ claim MaintenanceCommandClaim }

func (acceptedMaintenanceUpdate) maintenanceWork() {}
func (work acceptedMaintenanceUpdate) valid() bool {
	return work.claim.Valid() && work.claim.command.phase == maintenanceCompletionOutstanding &&
		work.claim.command.kind == MaintenanceCommandUpdate
}

// confirmedMaintenanceUpdate is minted only by loading a durable exact
// successful completion. It alone may cross the payload persistence boundary.
type confirmedMaintenanceUpdate struct{ claim MaintenanceCommandClaim }

func (confirmedMaintenanceUpdate) maintenanceWork() {}
func (work confirmedMaintenanceUpdate) valid() bool {
	return work.claim.Valid() && work.claim.command.phase == maintenancePayloadConfirmed &&
		work.claim.command.kind == MaintenanceCommandUpdate
}

type maintenanceUpdateWork interface {
	maintenanceWork
	valid() bool
	updateClaim() MaintenanceCommandClaim
}

func (work acceptedMaintenanceUpdate) updateClaim() MaintenanceCommandClaim  { return work.claim }
func (work confirmedMaintenanceUpdate) updateClaim() MaintenanceCommandClaim { return work.claim }

// endedMaintenanceUpdate proves that the exact accepted command's lease is
// terminal on the construction-bound control plane. It permits only the
// pending-update -> LeaseEnded transition, never backend dispatch or an
// arbitrary caller-selected settlement. A decoded command alone cannot mint it.
type endedMaintenanceUpdate struct {
	issuer   *MaintenanceCoordinator
	work     maintenanceUpdateWork
	consumed *atomic.Bool
}

func (ended endedMaintenanceUpdate) valid() bool {
	return ended.issuer != nil && ended.issuer.Valid() && ended.work != nil && ended.work.valid() &&
		ended.work.updateClaim().issuer == ended.issuer.coordinator.store && ended.consumed != nil
}

func (authority *MaintenanceCoordinator) observeEndedMaintenanceUpdate(
	ctx context.Context, work maintenanceUpdateWork,
) endedMaintenanceUpdate {
	if !authority.Valid() || work == nil || !work.valid() || work.updateClaim().issuer != authority.coordinator.store {
		return endedMaintenanceUpdate{}
	}
	command := work.updateClaim().command
	readCtx, cancel := context.WithTimeout(ctx, maintenanceChainReadTimeout)
	defer cancel()
	exact, ok := authority.controlPlane.observeLease(readCtx, command.LeaseUUID(), command.Tenant()).(observedExactLease)
	if !ok {
		return endedMaintenanceUpdate{}
	}
	switch exact.lease.State {
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED:
		return endedMaintenanceUpdate{issuer: authority, work: work, consumed: &atomic.Bool{}}
	default:
		return endedMaintenanceUpdate{}
	}
}

func (s *Store) endAcceptedMaintenanceUpdate(ended endedMaintenanceUpdate) error {
	if !ended.valid() || ended.work.updateClaim().issuer != s || !ended.consumed.CompareAndSwap(false, true) {
		return ErrInvalidMaintenanceCommand
	}
	return s.settleMaintenancePhase(ended.work.updateClaim(),
		maintenanceSettlement{outcome: MaintenanceOutcomeLeaseEnded}, ended.work.updateClaim().command.phase)
}

func (s *Store) maintenanceWork(claim MaintenanceCommandClaim) (maintenanceWork, error) {
	if s == nil || !claim.Valid() || claim.issuer != s {
		return nil, ErrInvalidMaintenanceCommand
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result maintenanceWork
	err := s.viewRuntimeAuthority(func(tx *bolt.Tx) error {
		command, _, err := pendingMaintenanceCommandTx(tx, claim)
		if err != nil {
			return err
		}
		current := MaintenanceCommandClaim{issuer: s, command: command}
		switch command.phase {
		case maintenanceDeliveryOutstanding:
			result = maintenanceDelivery{claim: current}
		case maintenanceCompletionOutstanding:
			result = acceptedMaintenanceUpdate{claim: current}
		case maintenancePayloadConfirmed:
			result = confirmedMaintenanceUpdate{claim: current}
		default:
			return ErrMaintenanceCommandNotPending
		}
		return nil
	})
	return result, err
}

// Caller holds s.mu. Both live and reopened capabilities must still name the
// exact pending journal head; an earlier delivery capability cannot survive
// the durable transition to local finalization.
func (s *Store) requireMaintenancePhaseLocked(claim MaintenanceCommandClaim, phase maintenanceJournalPhase) error {
	return s.viewRuntimeAuthority(func(tx *bolt.Tx) error {
		command, _, err := pendingMaintenanceCommandTx(tx, claim)
		if err != nil {
			return err
		}
		if command.phase != phase {
			return ErrMaintenanceCommandNotPending
		}
		return nil
	})
}

func pendingMaintenanceCommandTx(tx *bolt.Tx, claim MaintenanceCommandClaim) (MaintenanceCommand, []byte, error) {
	pending, records, err := maintenanceCommandBuckets(tx)
	if err != nil {
		return MaintenanceCommand{}, nil, err
	}
	command := claim.command
	if string(pending.Get([]byte(command.leaseUUID))) != command.id.String() {
		return MaintenanceCommand{}, nil, ErrMaintenanceCommandNotPending
	}
	encoded := records.Get(maintenanceReceiptKey(command.leaseUUID, command.id))
	stored, outcome, _, _, _, err := decodeMaintenanceCommand(encoded)
	if err != nil {
		return MaintenanceCommand{}, nil, fmt.Errorf("%w: decode pending command: %w", ErrMaintenanceJournalCorrupt, err)
	}
	if outcome != MaintenanceOutcomePending || !stored.equal(command) {
		return MaintenanceCommand{}, nil, ErrMaintenanceCommandNotPending
	}
	return stored, encoded, nil
}

func (s *Store) acceptMaintenanceUpdate(delivery maintenanceDelivery) (maintenanceWork, error) {
	if !delivery.valid() || delivery.claim.issuer != s || delivery.claim.command.kind != MaintenanceCommandUpdate {
		return nil, ErrInvalidMaintenanceCommand
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var accepted maintenanceWork
	err := s.updateMaintenanceAuthority(func(journal *maintenanceJournalTransaction) error {
		tx := journal.tx
		command, encoded, err := pendingMaintenanceCommandTx(tx, delivery.claim)
		if err != nil {
			return err
		}
		if command.phase == maintenancePayloadConfirmed {
			// The exact callback may beat the HTTP acceptance response.
			accepted = confirmedMaintenanceUpdate{claim: MaintenanceCommandClaim{issuer: s, command: command}}
			return nil
		}
		if command.phase != maintenanceDeliveryOutstanding {
			return ErrMaintenanceCommandNotPending
		}
		_, _, createdAt, settledAt, _, err := decodeMaintenanceCommand(encoded)
		if err != nil {
			return err
		}
		command.phase = maintenanceCompletionOutstanding
		value, err := encodeMaintenanceCommand(command, MaintenanceOutcomePending, createdAt, settledAt)
		if err != nil {
			return err
		}
		if err := journal.write(value); err != nil {
			return err
		}
		accepted = acceptedMaintenanceUpdate{claim: MaintenanceCommandClaim{issuer: s, command: command}}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return accepted, nil
}

// maintenancePayloadCommit is minted only after the construction-bound
// persister acknowledges the exact successfully completed bytes. It cannot be forged from
// a transport result, a decoded row or a caller-selected outcome.
type maintenancePayloadCommit struct {
	issuer    *MaintenanceCoordinator
	confirmed confirmedMaintenanceUpdate
	consumed  *atomic.Bool
}

func (s *Store) completeMaintenanceUpdate(commit maintenancePayloadCommit) error {
	if commit.issuer == nil || !commit.issuer.Valid() || commit.issuer.coordinator.store != s ||
		!commit.confirmed.valid() || commit.confirmed.claim.issuer != s ||
		commit.consumed == nil || !commit.consumed.CompareAndSwap(false, true) {
		return ErrInvalidMaintenanceCommand
	}
	return s.settleMaintenancePhase(commit.confirmed.claim, maintenanceSettlement{outcome: MaintenanceOutcomeAccepted}, maintenancePayloadConfirmed)
}
