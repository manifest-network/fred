package shared

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// RetentionOrphanVolumeInventory is an opaque, purpose-specific read capability.
// Its zero value is invalid; callers must bind the complete substrate inventory
// once with BindRetentionOrphanVolumeInventory. Keeping the callable unexported
// prevents swapping the evidence source per sweep; the sweep additionally turns
// a dependency panic (including a bound method with a nil receiver) into a
// fail-closed inventory error before selecting or deleting any row.
type RetentionOrphanVolumeInventory struct {
	listForProof func(context.Context) ([]string, error)
}

// BindRetentionOrphanVolumeInventory turns the backend's complete physical
// inventory operation into the only input a pruner can retain. The operation
// must enumerate every entry in fred's reserved volume namespace and fail on
// uncertain root identity.
func BindRetentionOrphanVolumeInventory(
	listForProof func(context.Context) ([]string, error),
) (RetentionOrphanVolumeInventory, error) {
	if listForProof == nil {
		return RetentionOrphanVolumeInventory{}, errors.New("retention orphan inventory requires a complete listing operation")
	}
	return RetentionOrphanVolumeInventory{listForProof: listForProof}, nil
}

func (i RetentionOrphanVolumeInventory) valid() bool { return i.listForProof != nil }

func (i RetentionOrphanVolumeInventory) list(ctx context.Context) (names []string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			names = nil
			err = fmt.Errorf("retention orphan inventory panicked: %v", recovered)
		}
	}()
	return i.listForProof(ctx)
}

// RetentionOrphanSkipReason is a closed observation vocabulary for a sweep that
// deliberately performed no orphan deletion.
type RetentionOrphanSkipReason uint8

const (
	RetentionOrphanSkipNone RetentionOrphanSkipReason = iota
	RetentionOrphanSkipDisabled
	RetentionOrphanSkipInventoryError
	RetentionOrphanSkipStoreError
)

// RetentionOrphanSweepResult contains aggregate observations only. It exposes
// no candidate, proof, lease key, or caller-selected deletion target.
type RetentionOrphanSweepResult struct {
	Pruned         int
	Raced          int
	DeleteFailures int
	SkipReason     RetentionOrphanSkipReason
}

// RetentionOrphanPruner is the sole owner of orphan candidate selection,
// complete-volume inventory, consecutive confirmation state, and ACTIVE-row
// deletion. It is safe for concurrent callers, though production binds it once
// to the single cleanup loop.
type RetentionOrphanPruner struct {
	mu             sync.Mutex
	store          *RetentionStore
	confirmations  int
	rootConfigured bool
	inventory      RetentionOrphanVolumeInventory
	// A row revision, not a lease UUID, earns consecutive absence. Replacing an
	// ACTIVE row therefore starts at observation one even when the UUID is reused.
	streaks map[[sha256.Size]byte]int
}

// NewRetentionOrphanPruner binds all authority required for orphan cleanup.
// The inventory capability is fixed for the pruner's lifetime; Sweep accepts
// only a context and cannot be supplied per-record evidence.
func NewRetentionOrphanPruner(
	store *RetentionStore,
	confirmations int,
	rootConfigured bool,
	inventory RetentionOrphanVolumeInventory,
) (*RetentionOrphanPruner, error) {
	if !retentionStoreIsOpen(store) {
		return nil, errors.New("retention orphan pruner requires an open identity-bound retention store")
	}
	if confirmations < 0 {
		return nil, errors.New("retention orphan confirmations must be non-negative")
	}
	if !inventory.valid() {
		return nil, errors.New("retention orphan pruner requires a complete inventory")
	}
	return &RetentionOrphanPruner{
		store: store, confirmations: confirmations, rootConfigured: rootConfigured,
		inventory: inventory, streaks: make(map[[sha256.Size]byte]int),
	}, nil
}

// Sweep inventories the entire volume namespace, selects every current ACTIVE
// row itself, and consumes only a row that remained byte-identical for the
// configured number of consecutive complete observations.
func (p *RetentionOrphanPruner) Sweep(ctx context.Context) (RetentionOrphanSweepResult, error) {
	if p == nil || !retentionStoreIsOpen(p.store) {
		return RetentionOrphanSweepResult{}, errors.New("retention orphan pruner is invalid or its store is closed")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.confirmations == 0 {
		return RetentionOrphanSweepResult{SkipReason: RetentionOrphanSkipDisabled}, nil
	}
	physicalNames, err := p.inventory.list(ctx)
	if err != nil {
		p.streaks = make(map[[sha256.Size]byte]int)
		return RetentionOrphanSweepResult{SkipReason: RetentionOrphanSkipInventoryError}, err
	}
	present := make(map[string]struct{}, len(physicalNames)*2)
	for _, name := range physicalNames {
		present[name] = struct{}{}
		if strings.HasPrefix(name, "fred-") && !strings.HasPrefix(name, "fred-retained-") {
			present["fred-retained-"+strings.TrimPrefix(name, "fred-")] = struct{}{}
		}
	}

	candidates, err := p.store.ListActiveCandidates()
	if err != nil {
		p.streaks = make(map[[sha256.Size]byte]int)
		return RetentionOrphanSweepResult{SkipReason: RetentionOrphanSkipStoreError}, err
	}

	next := make(map[[sha256.Size]byte]int, len(p.streaks))
	result := RetentionOrphanSweepResult{}
	var deleteErrors []error
	for _, candidate := range candidates {
		entry := candidate.Entry()
		if !p.rootConfigured && len(entry.RetainedVolumeNames) != 0 {
			continue
		}
		presentVolume := false
		for _, name := range entry.RetainedVolumeNames {
			if _, ok := present[name]; ok {
				presentVolume = true
				break
			}
		}
		if presentVolume {
			continue
		}
		streak := p.streaks[candidate.digest] + 1
		if streak < p.confirmations {
			next[candidate.digest] = streak
			continue
		}
		deleted, deleteErr := p.store.pruneOrphanedActive(candidate)
		switch {
		case deleteErr != nil:
			result.DeleteFailures++
			next[candidate.digest] = streak
			deleteErrors = append(deleteErrors, fmt.Errorf("delete confirmed orphan: %w", deleteErr))
		case !deleted:
			result.Raced++
		default:
			result.Pruned++
		}
	}
	p.streaks = next
	return result, errors.Join(deleteErrors...)
}
