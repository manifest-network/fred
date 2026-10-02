package placementprobe

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

var (
	// ErrRetirementTargetAlive means the backend proposed for retirement
	// answered with its pinned storage identity, so its storage is not lost.
	ErrRetirementTargetAlive = errors.New("backend proposed for retirement still serves its pinned storage")

	// ErrRetirementTargetMisrouted means the address configured for the backend
	// proposed for retirement answered with the pinned storage of another
	// backend, active, removed, or retired, so the probe never reached the
	// backend being retired.
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
	// identity no backend has ever been pinned to, such as a host rebuilt on
	// new disks.
	RetirementTargetOtherStorage
	// RetirementTargetFenced: the target is fenced, so it was not asked.
	RetirementTargetFenced
)

func (liveness RetirementTargetLiveness) String() string {
	switch liveness {
	case RetirementTargetNoIdentity:
		return "no_identity"
	case RetirementTargetOtherStorage:
		return "answered_with_other_storage"
	case RetirementTargetFenced:
		return "fenced"
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
// answer is the target's own pin (the storage is not lost) or any other pin
// the database has ever recorded (the configured address reaches a different
// backend, even one that has since left the topology).
func ProbeRetirementTarget(
	ctx context.Context,
	cfg *config.Config,
	target placement.RetirementProbeTarget,
) (RetirementTargetLiveness, error) {
	backendName, pin := target.BackendName(), target.Pin()
	if backendName == "" || !pin.Valid() {
		return retirementTargetLivenessInvalid, errors.New("retirement probe target is invalid")
	}
	if cfg == nil {
		return retirementTargetLivenessInvalid, errors.New("provider config is required")
	}
	policy, err := cfg.BackendConnectionPolicy(backendName)
	if err != nil {
		return retirementTargetLivenessInvalid, fmt.Errorf("backend %q: compose connection policy: %w", backendName, err)
	}
	if policy.Fenced() {
		return RetirementTargetFenced, nil
	}
	observed, err := backend.ProbeStorageIdentity(ctx, policy)
	if err != nil {
		return RetirementTargetNoIdentity, nil //nolint:nilerr // an unanswered probe proves nothing either way
	}
	owner, known := target.StorageOwner(observed)
	switch {
	case !known:
		return RetirementTargetOtherStorage, nil
	case owner == backendName:
		return retirementTargetLivenessInvalid, fmt.Errorf(
			"%w: backend %q answered with storage identity %s", ErrRetirementTargetAlive, backendName, pin,
		)
	default:
		return retirementTargetLivenessInvalid, fmt.Errorf(
			"%w: backend %q answered with the storage of backend %q", ErrRetirementTargetMisrouted, backendName, owner,
		)
	}
}
