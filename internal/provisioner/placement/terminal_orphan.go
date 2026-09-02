package placement

import (
	"context"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendname"
)

var (
	// ErrTerminalOrphanProof means the stopped authority or height-pinned chain
	// snapshot did not positively prove that one legacy rollback remnant belongs
	// to a terminal lease whose v0.13 placement has already been retired.
	ErrTerminalOrphanProof = errors.New("terminal orphan proof failed")

	// ErrTerminalOrphanResidualPlacement distinguishes a safe, terminal chain
	// observation from a still-present v0.13 routing row. The old reconciler can
	// retire that row from complete backend absence; an offline proof must not.
	ErrTerminalOrphanResidualPlacement = errors.New("terminal orphan has a residual v0.13 placement")
)

// TerminalOrphanChainSnapshot extends the complete signer-free provider
// snapshot with the identities behind its blocking counter. Keeping states out
// of this interface is deliberate: the chain reader has already classified
// every PENDING, ACTIVE, unknown, and future state as blocking.
type TerminalOrphanChainSnapshot interface {
	ProviderLeaseMembershipSnapshot
	BlockingLeaseUUIDs() []string
}

// TerminalOrphanChainProof is immutable positive evidence that one exact lease
// belongs to the configured provider and was terminal at one pinned height. Its
// zero value is invalid and callers cannot manufacture its fields.
type TerminalOrphanChainProof struct {
	providerUUID string
	leaseUUID    string
	blockHeight  int64
}

// NewTerminalOrphanChainProof validates a complete provider snapshot and then
// selects one exact terminal member. Absence is not terminal evidence: it may
// mean the wrong provider was queried or that the index was incomplete.
func NewTerminalOrphanChainProof(
	snapshot TerminalOrphanChainSnapshot,
	providerUUID string,
	leaseUUID string,
) (TerminalOrphanChainProof, error) {
	if !canonicalLeaseUUID(providerUUID) {
		return TerminalOrphanChainProof{}, fmt.Errorf(
			"%w: configured provider UUID %q is not canonical",
			ErrTerminalOrphanProof,
			providerUUID,
		)
	}
	if !canonicalLeaseUUID(leaseUUID) {
		return TerminalOrphanChainProof{}, fmt.Errorf(
			"%w: target lease UUID %q is not canonical",
			ErrTerminalOrphanProof,
			leaseUUID,
		)
	}
	providerProof, err := NewLegacyUpgradeChainProof(snapshot)
	if err != nil {
		return TerminalOrphanChainProof{}, fmt.Errorf("%w: %w", ErrTerminalOrphanProof, err)
	}
	if providerProof.providerUUID != providerUUID {
		return TerminalOrphanChainProof{}, fmt.Errorf(
			"%w: chain snapshot belongs to provider %q, configured provider is %q",
			ErrTerminalOrphanProof,
			providerProof.providerUUID,
			providerUUID,
		)
	}
	if _, present := providerProof.leaseUUIDs[leaseUUID]; !present {
		return TerminalOrphanChainProof{}, fmt.Errorf(
			"%w: lease %q is absent from the height-%d all-state chain snapshot for provider %q",
			ErrTerminalOrphanProof,
			leaseUUID,
			providerProof.blockHeight,
			providerProof.providerUUID,
		)
	}

	blockingLeaseUUIDs := snapshot.BlockingLeaseUUIDs()
	if len(blockingLeaseUUIDs) != snapshot.BlockingLeaseCount() {
		return TerminalOrphanChainProof{}, fmt.Errorf(
			"%w: chain snapshot blocking identities (%d) differ from its blocking count (%d)",
			ErrTerminalOrphanProof,
			len(blockingLeaseUUIDs),
			snapshot.BlockingLeaseCount(),
		)
	}
	seenBlocking := make(map[string]struct{}, len(blockingLeaseUUIDs))
	for _, blockingLeaseUUID := range blockingLeaseUUIDs {
		if !canonicalLeaseUUID(blockingLeaseUUID) {
			return TerminalOrphanChainProof{}, fmt.Errorf(
				"%w: chain snapshot contains non-canonical blocking lease UUID %q",
				ErrTerminalOrphanProof,
				blockingLeaseUUID,
			)
		}
		if _, member := providerProof.leaseUUIDs[blockingLeaseUUID]; !member {
			return TerminalOrphanChainProof{}, fmt.Errorf(
				"%w: chain snapshot blocking lease %q is absent from all-state membership",
				ErrTerminalOrphanProof,
				blockingLeaseUUID,
			)
		}
		if _, duplicate := seenBlocking[blockingLeaseUUID]; duplicate {
			return TerminalOrphanChainProof{}, fmt.Errorf(
				"%w: chain snapshot contains duplicate blocking lease %q",
				ErrTerminalOrphanProof,
				blockingLeaseUUID,
			)
		}
		seenBlocking[blockingLeaseUUID] = struct{}{}
	}
	if _, blocking := seenBlocking[leaseUUID]; blocking {
		return TerminalOrphanChainProof{}, fmt.Errorf(
			"%w: lease %q is non-terminal or unknown at height %d",
			ErrTerminalOrphanProof,
			leaseUUID,
			providerProof.blockHeight,
		)
	}
	return TerminalOrphanChainProof{
		providerUUID: providerProof.providerUUID,
		leaseUUID:    leaseUUID,
		blockHeight:  providerProof.blockHeight,
	}, nil
}

func (proof TerminalOrphanChainProof) valid() bool {
	return canonicalLeaseUUID(proof.providerUUID) &&
		canonicalLeaseUUID(proof.leaseUUID) && proof.blockHeight > 0
}

// TerminalOrphanProofSummary is the bounded, non-secret fact printed by the
// offline command after both the chain and stopped placement file prove the
// target safe for the separate immutable-container cleanup procedure.
type TerminalOrphanProofSummary struct {
	LeaseUUID       string
	ExpectedBackend string
	ProviderUUID    string
	ChainHeight     int64
}

// ProveTerminalOrphanContext holds the inspector's shared stopped-database lock
// while proving the complete file is still pristine v0.13 authority. Success is
// possible only when the exact target row is absent. A valid residual owner is
// returned as a distinct actionable error; malformed, current, or foreign
// authority remains a generic proof failure.
func (inspector *LegacyUpgradeInspector) ProveTerminalOrphanContext(
	ctx context.Context,
	expectedBackend string,
	chainProof TerminalOrphanChainProof,
) (TerminalOrphanProofSummary, error) {
	if inspector == nil || inspector.db == nil {
		return TerminalOrphanProofSummary{}, errors.New("legacy upgrade inspector is not open")
	}
	if ctx == nil {
		return TerminalOrphanProofSummary{}, errors.New("terminal orphan proof context is required")
	}
	if err := ctx.Err(); err != nil {
		return TerminalOrphanProofSummary{}, fmt.Errorf("terminal orphan proof interrupted: %w", err)
	}
	if len(expectedBackend) > maxAuthorityRenderedIdentity {
		return TerminalOrphanProofSummary{}, fmt.Errorf(
			"%w: expected backend exceeds the %d-byte diagnostic bound",
			ErrTerminalOrphanProof,
			maxAuthorityRenderedIdentity,
		)
	}
	if err := backendname.Validate(expectedBackend); err != nil {
		return TerminalOrphanProofSummary{}, fmt.Errorf(
			"%w: expected backend: %w", ErrTerminalOrphanProof, err,
		)
	}
	if !chainProof.valid() {
		return TerminalOrphanProofSummary{}, fmt.Errorf(
			"%w: chain proof is absent or invalid", ErrTerminalOrphanProof,
		)
	}
	if inspector.authority != nil {
		if err := inspector.authority.verify(); err != nil {
			return TerminalOrphanProofSummary{}, fmt.Errorf(
				"verify stopped placement authority: %w", err,
			)
		}
	}
	if err := verifyBoltPhysicalConsistency(inspector.db); err != nil {
		return TerminalOrphanProofSummary{}, fmt.Errorf(
			"validate placement db physical consistency: %w", err,
		)
	}

	var targetOwner string
	err := inspector.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		topLevelBuckets := 0
		if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			topLevelBuckets++
			if string(name) != string(bucketName) {
				return fmt.Errorf("unexpected top-level bucket %q", name)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("inspect top-level buckets: %w", err)
		}
		if topLevelBuckets != 1 {
			return fmt.Errorf(
				"stopped placement authority has %d top-level buckets, expected the one pristine v0.13 placements bucket",
				topLevelBuckets,
			)
		}
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return errors.New("placements bucket is missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			leaseUUID := string(key)
			if value == nil || !canonicalLeaseUUID(leaseUUID) {
				return errors.New("v0.13 placement row has an invalid key or nested value")
			}
			if len(value) > maxAuthorityRowValueBytes {
				return fmt.Errorf("v0.13 placement %q exceeds the safe decoder bound", leaseUUID)
			}
			if revision, object, headerErr := preflightRevisionHeader(value); object {
				switch {
				case headerErr != nil:
					return fmt.Errorf("v0.13 placement %q has a malformed revision header: %w", leaseUUID, headerErr)
				case revision != 0:
					return fmt.Errorf("placement %q is already revisioned (revision=%d)", leaseUUID, revision)
				}
			}
			owner, err := decodeV013PreflightPlacement(value)
			if err != nil {
				return fmt.Errorf("placement %q is not an unambiguous v0.13 confirmed owner: %w", leaseUUID, err)
			}
			if len(owner) > maxAuthorityRenderedIdentity {
				return fmt.Errorf(
					"placement %q owner exceeds the %d-byte diagnostic bound",
					leaseUUID,
					maxAuthorityRenderedIdentity,
				)
			}
			if leaseUUID == chainProof.leaseUUID {
				targetOwner = owner
			}
			return nil
		})
	})
	if err != nil {
		return TerminalOrphanProofSummary{}, fmt.Errorf(
			"%w: require pristine v0.13 placement authority: %w",
			ErrTerminalOrphanProof,
			err,
		)
	}
	if inspector.authority != nil {
		if err := inspector.authority.verify(); err != nil {
			return TerminalOrphanProofSummary{}, fmt.Errorf(
				"reverify stopped placement authority: %w", err,
			)
		}
	}
	if targetOwner != "" {
		if targetOwner != expectedBackend {
			return TerminalOrphanProofSummary{}, fmt.Errorf(
				"%w: terminal lease %q has residual v0.13 owner %q, expected %q",
				ErrTerminalOrphanProof,
				chainProof.leaseUUID,
				targetOwner,
				expectedBackend,
			)
		}
		return TerminalOrphanProofSummary{}, fmt.Errorf(
			"%w: terminal lease %q still has confirmed owner %q; restart the complete matching v0.13 fleet and providerd in isolation, let cleanupOrphanedPlacements observe this backend absent from both provisions and retentions and prune the row, then stop and drain everything and rerun this proof",
			ErrTerminalOrphanResidualPlacement,
			chainProof.leaseUUID,
			targetOwner,
		)
	}
	return TerminalOrphanProofSummary{
		LeaseUUID:       chainProof.leaseUUID,
		ExpectedBackend: expectedBackend,
		ProviderUUID:    chainProof.providerUUID,
		ChainHeight:     chainProof.blockHeight,
	}, nil
}
