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
	maintenancePayloadOutstanding
	maintenanceCompleted
)

func (phase maintenanceJournalPhase) String() string {
	switch phase {
	case maintenanceDeliveryOutstanding:
		return "delivery_outstanding"
	case maintenancePayloadOutstanding:
		return "payload_outstanding"
	default:
		return ""
	}
}

func (phase maintenanceJournalPhase) validFor(kind MaintenanceCommandKind, terminal bool) bool {
	if terminal {
		return phase == maintenanceCompleted
	}
	return phase == maintenanceDeliveryOutstanding ||
		(phase == maintenancePayloadOutstanding && kind == MaintenanceCommandUpdate)
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
	case "payload_outstanding":
		return maintenancePayloadOutstanding, nil
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

// Accepted update work can only finish local payload persistence or retire
// after exact terminal chain evidence. It cannot become backend dispatch work.
type acceptedMaintenanceUpdate struct{ claim MaintenanceCommandClaim }

func (acceptedMaintenanceUpdate) maintenanceWork() {}
func (work acceptedMaintenanceUpdate) valid() bool {
	return work.claim.Valid() && work.claim.command.phase == maintenancePayloadOutstanding &&
		work.claim.command.kind == MaintenanceCommandUpdate
}

// endedMaintenanceUpdate proves that the exact accepted command's lease is
// terminal on the construction-bound control plane. It permits only the
// payload-outstanding -> LeaseEnded transition, never backend dispatch or an
// arbitrary caller-selected settlement. A decoded command alone cannot mint it.
type endedMaintenanceUpdate struct {
	issuer   *MaintenanceCoordinator
	accepted acceptedMaintenanceUpdate
	consumed *atomic.Bool
}

func (ended endedMaintenanceUpdate) valid() bool {
	return ended.issuer != nil && ended.issuer.Valid() && ended.accepted.valid() &&
		ended.accepted.claim.issuer == ended.issuer.coordinator.store && ended.consumed != nil
}

func (authority *MaintenanceCoordinator) observeEndedMaintenanceUpdate(
	ctx context.Context, accepted acceptedMaintenanceUpdate,
) endedMaintenanceUpdate {
	if !authority.Valid() || !accepted.valid() || accepted.claim.issuer != authority.coordinator.store {
		return endedMaintenanceUpdate{}
	}
	command := accepted.claim.command
	readCtx, cancel := context.WithTimeout(ctx, maintenanceChainReadTimeout)
	defer cancel()
	exact, ok := authority.controlPlane.observeLease(readCtx, command.LeaseUUID(), command.Tenant()).(observedExactLease)
	if !ok {
		return endedMaintenanceUpdate{}
	}
	switch exact.lease.State {
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED:
		return endedMaintenanceUpdate{issuer: authority, accepted: accepted, consumed: &atomic.Bool{}}
	default:
		return endedMaintenanceUpdate{}
	}
}

func (s *Store) endAcceptedMaintenanceUpdate(ended endedMaintenanceUpdate) error {
	if !ended.valid() || ended.accepted.claim.issuer != s || !ended.consumed.CompareAndSwap(false, true) {
		return ErrInvalidMaintenanceCommand
	}
	return s.settleMaintenancePhase(ended.accepted.claim,
		maintenanceSettlement{outcome: MaintenanceOutcomeLeaseEnded}, maintenancePayloadOutstanding)
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
		case maintenancePayloadOutstanding:
			result = acceptedMaintenanceUpdate{claim: current}
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

func (s *Store) acceptMaintenanceUpdate(delivery maintenanceDelivery) (acceptedMaintenanceUpdate, error) {
	if !delivery.valid() || delivery.claim.issuer != s || delivery.claim.command.kind != MaintenanceCommandUpdate {
		return acceptedMaintenanceUpdate{}, ErrInvalidMaintenanceCommand
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var accepted acceptedMaintenanceUpdate
	err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		command, encoded, err := pendingMaintenanceCommandTx(tx, delivery.claim)
		if err != nil {
			return err
		}
		if command.phase != maintenanceDeliveryOutstanding {
			return ErrMaintenanceCommandNotPending
		}
		_, _, createdAt, settledAt, _, err := decodeMaintenanceCommand(encoded)
		if err != nil {
			return err
		}
		command.phase = maintenancePayloadOutstanding
		value, err := encodeMaintenanceCommand(command, MaintenanceOutcomePending, createdAt, settledAt)
		if err != nil {
			return err
		}
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		if err := records.Put(maintenanceReceiptKey(command.leaseUUID, command.id), value); err != nil {
			return err
		}
		accepted = acceptedMaintenanceUpdate{claim: MaintenanceCommandClaim{issuer: s, command: command}}
		return nil
	})
	if err != nil {
		return acceptedMaintenanceUpdate{}, err
	}
	return accepted, nil
}

// maintenancePayloadCommit is minted only after the construction-bound
// persister acknowledges the exact accepted bytes. It cannot be forged from
// a transport result, a decoded row or a caller-selected outcome.
type maintenancePayloadCommit struct {
	issuer   *MaintenanceCoordinator
	accepted acceptedMaintenanceUpdate
	consumed *atomic.Bool
}

func (s *Store) completeMaintenanceUpdate(commit maintenancePayloadCommit) error {
	if commit.issuer == nil || !commit.issuer.Valid() || commit.issuer.coordinator.store != s ||
		!commit.accepted.valid() || commit.accepted.claim.issuer != s ||
		commit.consumed == nil || !commit.consumed.CompareAndSwap(false, true) {
		return ErrInvalidMaintenanceCommand
	}
	return s.settleMaintenancePhase(commit.accepted.claim, maintenanceSettlement{outcome: MaintenanceOutcomeAccepted}, maintenancePayloadOutstanding)
}
