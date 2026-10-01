package placement

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// ErrBackendRetired means a configured backend name was retired as lost. A
// retired name can never rejoin the topology: its leases were closed as lost,
// and its storage identity stays pinned so no other name can claim it either.
var ErrBackendRetired = errors.New("placement backend was retired as lost")

// ErrPlacementLost means the lease lived on a backend an operator retired as
// irrecoverably lost. It is terminal: nothing can restart, update, restore,
// or read the lease's workload again.
var ErrPlacementLost = errors.New("placement is lost with its retired backend's storage")

// retiredBackend records one operator-attested retirement of a backend whose
// storage was irrecoverably lost. The name and its storage pin stay in
// KnownBackends and KnownBackendStorageIDs as history; this record is what
// makes the retirement permanent.
type retiredBackend struct {
	RetiredAt time.Time `json:"retired_at"`
	// AttestationSHA256 is the digest of the exact confirmation the operator
	// applied, so a later audit can bind the record to its plan.
	AttestationSHA256 string `json:"attestation_sha256"`
	// RecordlessUnproven records that the database carried no current
	// admission baseline when the backend was retired, so a live lease with no
	// placement row may have lived there. The reconciler then closes such a
	// lease as lost instead of provisioning it empty on a survivor.
	RecordlessUnproven bool `json:"recordless_unproven,omitempty"`
}

// validateRetiredBackends requires every retired name to be historical,
// inactive, and pinned, with a complete record.
func validateRetiredBackends(metadata topologyMetadata) error {
	if len(metadata.RetiredBackends) == 0 {
		if metadata.RetiredBackends != nil {
			return errors.New("placement retired backends must be omitted when empty")
		}
		return nil
	}
	known := make(map[string]struct{}, len(metadata.KnownBackends))
	for _, backendName := range metadata.KnownBackends {
		known[backendName] = struct{}{}
	}
	active := make(map[string]struct{}, len(metadata.Topology))
	for _, backendName := range metadata.Topology {
		active[backendName] = struct{}{}
	}
	for _, backendName := range slices.Sorted(maps.Keys(metadata.RetiredBackends)) {
		record := metadata.RetiredBackends[backendName]
		if _, historical := known[backendName]; !historical {
			return fmt.Errorf("retired backend %q is not a known backend", backendName)
		}
		if _, still := active[backendName]; still {
			return fmt.Errorf("retired backend %q is still in the active topology", backendName)
		}
		if metadata.KnownBackendStorageIDs[backendName] == "" {
			return fmt.Errorf("retired backend %q has no storage pin", backendName)
		}
		if record.RetiredAt.IsZero() {
			return fmt.Errorf("retired backend %q has no retirement time", backendName)
		}
		digest, err := hex.DecodeString(record.AttestationSHA256)
		if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != record.AttestationSHA256 {
			return fmt.Errorf("retired backend %q has a malformed attestation digest", backendName)
		}
	}
	return nil
}

func cloneRetiredBackends(retired map[string]retiredBackend) map[string]retiredBackend {
	if len(retired) == 0 {
		return nil
	}
	return maps.Clone(retired)
}

// recordlessLeasesUnprovenLocked reports whether any retirement could not
// prove that every live lease on the retired backend had a placement row.
// Caller holds s.mu.
func (s *Store) recordlessLeasesUnprovenLocked() bool {
	for _, record := range s.retiredBackends {
		if record.RecordlessUnproven {
			return true
		}
	}
	return false
}

// RecordlessLeasesUnproven reports whether a live lease with no placement row
// may have lived on a retired backend. The reconciler closes such a lease as
// lost rather than provisioning it empty on a survivor.
func (s *Store) RecordlessLeasesUnproven() bool {
	if err := s.reattestRuntimeAuthority(); err != nil {
		// Without authority nothing is admitted; answer conservatively.
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recordlessLeasesUnprovenLocked()
}

// refuseRetiredBackendsLocked keeps every retired name out of a proposed
// topology. Caller holds s.mu.
func (s *Store) refuseRetiredBackendsLocked(names []string) error {
	for _, backendName := range names {
		if _, retired := s.retiredBackends[backendName]; retired {
			return fmt.Errorf("%w: %q", ErrBackendRetired, backendName)
		}
	}
	return nil
}
