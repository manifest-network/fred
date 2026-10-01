package placementprobe

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
)

var (
	// ErrRetirementTargetAlive means the backend proposed for retirement
	// answered with its pinned storage identity, so its storage is not lost.
	ErrRetirementTargetAlive = errors.New("backend proposed for retirement still serves its pinned storage")

	// ErrRetirementTargetMisrouted means the address configured for the backend
	// proposed for retirement answered with another backend's pinned storage,
	// so the probe never reached the backend being retired.
	ErrRetirementTargetMisrouted = errors.New("backend proposed for retirement answered with another backend's storage")
)

// RetirementTargetLiveness is what one bounded probe of a backend proposed
// for retirement observed. The probe guards against retiring the wrong name.
// Only an identity answer proves anything; silence can mean down, fenced, or
// failing, which is why retirement also needs the operator's attestation.
type RetirementTargetLiveness uint8

const (
	retirementTargetLivenessInvalid RetirementTargetLiveness = iota
	// RetirementTargetNoIdentity: the target returned no storage identity,
	// because it is unreachable or its storage identity did not verify.
	RetirementTargetNoIdentity
	// RetirementTargetOtherStorage: the target answered with a storage
	// identity no backend is pinned to, such as a host rebuilt on new disks.
	RetirementTargetOtherStorage
)

func (liveness RetirementTargetLiveness) String() string {
	switch liveness {
	case RetirementTargetNoIdentity:
		return "no_identity"
	case RetirementTargetOtherStorage:
		return "answered_with_other_storage"
	default:
		return "invalid"
	}
}

// MarshalText renders the liveness for the operator's JSON plan.
func (liveness RetirementTargetLiveness) MarshalText() ([]byte, error) {
	if liveness == retirementTargetLivenessInvalid {
		return nil, errors.New("retirement target liveness is invalid")
	}
	return []byte(liveness.String()), nil
}

// ProbeRetirementTarget asks the configured backend once, within its
// configured request timeout, which storage it serves. It refuses when the
// answer is the target's own pin (the storage is not lost) or another pinned
// backend's storage (the configured address reaches a different backend).
// pins maps every pinned backend name, the target included, to its storage.
func ProbeRetirementTarget(
	ctx context.Context,
	cfg *config.Config,
	backendName string,
	pins map[string]backendidentity.ID,
) (RetirementTargetLiveness, error) {
	pin := pins[backendName]
	if !pin.Valid() {
		return retirementTargetLivenessInvalid, fmt.Errorf("backend %q has no storage pin", backendName)
	}
	if cfg == nil {
		return retirementTargetLivenessInvalid, errors.New("provider config is required")
	}
	policy, err := cfg.BackendConnectionPolicy(backendName)
	if err != nil {
		return retirementTargetLivenessInvalid, fmt.Errorf("backend %q: compose connection policy: %w", backendName, err)
	}
	observed, err := backend.ProbeStorageIdentity(ctx, policy)
	if err != nil {
		return RetirementTargetNoIdentity, nil //nolint:nilerr // an unanswered probe proves nothing either way
	}
	for name, other := range pins {
		if other != observed {
			continue
		}
		if name == backendName {
			return retirementTargetLivenessInvalid, fmt.Errorf(
				"%w: backend %q answered with storage identity %s", ErrRetirementTargetAlive, backendName, pin,
			)
		}
		return retirementTargetLivenessInvalid, fmt.Errorf(
			"%w: backend %q answered with the storage of backend %q", ErrRetirementTargetMisrouted, backendName, name,
		)
	}
	return RetirementTargetOtherStorage, nil
}
