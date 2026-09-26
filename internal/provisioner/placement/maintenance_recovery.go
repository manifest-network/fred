package placement

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/maintenanceid"
)

// A candidate is a scheduling hint copied from the committed projection. It
// carries no payload and grants no mutation authority. Only a selected exact
// durable receipt can produce a recovery claim.
type maintenanceRecoveryCandidate struct {
	lease   string
	id      maintenanceid.ID
	backend string
	phase   maintenanceJournalPhase
}

func (candidate maintenanceRecoveryCandidate) key() string {
	return candidate.lease + "\x00" + candidate.id.String()
}

func (s *Store) maintenanceRecoverySnapshot() (map[string]maintenanceRecoveryCandidate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.reattestRuntimeAuthority(); err != nil {
		return nil, err
	}
	result := make(map[string]maintenanceRecoveryCandidate, len(s.maintenanceAccounting.leases))
	for lease, entry := range s.maintenanceAccounting.leases {
		result[lease] = maintenanceRecoveryCandidate{
			lease: lease, id: entry.id, backend: entry.backend, phase: entry.phase,
		}
	}
	return result, nil
}

func (s *Store) pendingMaintenanceClaim(lease string, id maintenanceid.ID) (MaintenanceCommandClaim, bool, error) {
	if s == nil || !canonicalLeaseUUID(lease) || !id.Valid() {
		return MaintenanceCommandClaim{}, false, ErrInvalidMaintenanceCommand
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result MaintenanceCommandClaim
	pendingCommand := false
	err := s.viewRuntimeAuthority(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		encoded := records.Get(maintenanceReceiptKey(lease, id))
		command, outcome, _, _, _, err := decodeMaintenanceCommand(encoded)
		if err != nil || command.leaseUUID != lease || command.id != id {
			return fmt.Errorf("%w: invalid recovery receipt for %q", ErrMaintenanceJournalCorrupt, lease)
		}
		headMatches := string(pending.Get([]byte(lease))) == id.String()
		if outcome != MaintenanceOutcomePending {
			if headMatches {
				return fmt.Errorf("%w: terminal receipt remains pending", ErrMaintenanceJournalCorrupt)
			}
			return nil
		}
		if !headMatches {
			return fmt.Errorf("%w: recovery receipt has no exact head", ErrMaintenanceJournalCorrupt)
		}
		result = MaintenanceCommandClaim{issuer: s, command: command}
		pendingCommand = true
		return nil
	})
	return result, pendingCommand, err
}
