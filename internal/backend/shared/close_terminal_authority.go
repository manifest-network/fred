package shared

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

// closeTerminalAuthority joins a physical observation with the exact close
// journal's proof that no earlier Docker workload launch can still act on its
// bind namespace.
// The close head prevents originating operation/maintenance subjects from
// creating new launch debt; completion re-attests this fact before retiring any
// Release history. Empty inventory alone can never issue this authority.
type closeTerminalAuthority struct{ subject ClosePhysicalSubject }

func (authority closeTerminalAuthority) validFor(s *CloseSettlement, subject ClosePhysicalSubject) bool {
	return authority.subject == subject && subject.validFor(s)
}

func (s *CloseSettlement) proveCloseTerminal(subject ClosePhysicalSubject) (closeTerminalAuthority, error) {
	if !s.valid() || !subject.validFor(s) {
		return closeTerminalAuthority{}, errors.New("close terminal authority requires its exact settlement subject")
	}
	if err := s.callbacks.view(func(tx *bolt.Tx) error {
		return verifyCloseTerminalTx(tx, subject.state.claim)
	}); err != nil {
		return closeTerminalAuthority{}, err
	}
	return closeTerminalAuthority{subject: subject}, nil
}

func verifyCloseTerminalTx(tx *bolt.Tx, claim CloseIntentClaim) error {
	if err := verifyCloseIntentTx(tx, claim); err != nil {
		return err
	}
	// The optional bucket is absent for backends which never issue Docker
	// launch debt. Any present row is strictly decoded and lineage-checked.
	return checkVolumeLaunchNamespaceTx(tx, claim.Backend(), claim.BackendStorageID(), claim.LeaseUUID())
}
